//! sqlite3's `.import`, done by the daemon: it reads the client's file (a
//! descriptor the shell passed) with the shell's rules and inserts the rows in one
//! transaction. A new table takes its columns from the first row; an existing one
//! takes every row as data. Short and long rows are filled or cut with sqlite3's
//! messages, and a row the database refuses is reported and skipped.

use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{AssertSqlSafe, SqliteConnection};

/// How the input is split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImportMode {
    /// `--csv` / `.mode csv`: commas, RFC 4180 quoting.
    Csv,
    /// `--ascii`: unit (0x1F) and record (0x1E) separators, no quoting.
    Ascii,
    /// Any other mode: its column separator, with CSV quoting.
    List { separator: String },
}

pub struct ImportRequest<'a> {
    pub table: String,
    pub schema: Option<String>,
    pub mode: ImportMode,
    pub skip: u64,
    /// The file's name, for messages.
    pub file: String,
    pub in_transaction: bool,
    pub src: OwnedFd,
    pub cancelled: &'a AtomicBool,
    pub peer: Option<OwnedFd>,
}


/// Records as they are read: (line where the record starts, fields).
type Lines = Vec<(u64, Vec<String>)>;

/// Read and split the input on a blocking thread, handing records over in batches.
fn reader(mode: ImportMode, src: OwnedFd, tx: tokio::sync::mpsc::Sender<Result<Lines>>) {
    let mut b = csv::ReaderBuilder::new();
    b.has_headers(false).flexible(true);
    match &mode {
        ImportMode::Csv => {}
        ImportMode::Ascii => {
            b.delimiter(0x1f).terminator(csv::Terminator::Any(0x1e)).quoting(false);
        }
        ImportMode::List { separator } => {
            b.delimiter(separator.bytes().next().unwrap_or(b'|'));
        }
    }
    let mut r = b.from_reader(std::io::BufReader::with_capacity(1 << 20, std::fs::File::from(src)));
    let mut batch = Vec::new();
    let mut rec = csv::ByteRecord::new();
    loop {
        match r.read_byte_record(&mut rec) {
            Ok(false) => break,
            Ok(true) => {
                let line = rec.position().map_or(0, |p| p.line());
                let fields = rec.iter().map(|f| String::from_utf8_lossy(f).into_owned()).collect();
                batch.push((line, fields));
                if batch.len() >= 1000 && tx.blocking_send(Ok(std::mem::take(&mut batch))).is_err() {
                    return;
                }
            }
            Err(e) => {
                tx.blocking_send(Err(anyhow::anyhow!("{e}"))).ok();
                return;
            }
        }
    }
    if !batch.is_empty() {
        tx.blocking_send(Ok(batch)).ok();
    }
}

/// Bound parameters per INSERT stay under SQLite's limit (32766).
const MAX_PARAMS: usize = 32_000;

async fn insert(c: &mut SqliteConnection, target: &str, ncols: usize, rows: &[(u64, Vec<Option<String>>)]) -> Result<u64> {
    let one = format!("({})", vec!["?"; ncols].join(", "));
    let sql = format!("INSERT INTO {target} VALUES {}", vec![one; rows.len()].join(", "));
    let mut q = sqlx::query(AssertSqlSafe(sql)).persistent(false);
    for (_, row) in rows {
        for v in row {
            q = q.bind(v.clone());
        }
    }
    Ok(q.execute(&mut *c).await?.rows_affected())
}

/// Run `.import`. Returns `{"rows", "errors", "lines", "messages": [...]}`.
pub async fn import(c: &mut SqliteConnection, req: ImportRequest<'_>) -> Result<Value> {
    let schema = req.schema.clone().unwrap_or_else(|| "main".into());
    let target = format!("{}.{}", crate::db::sql::quote_ident(&schema), crate::db::sql::quote_ident(&req.table));
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM pragma_table_info(?1, ?2)")
        .bind(&req.table)
        .bind(&schema)
        .fetch_one(&mut *c)
        .await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let mode = req.mode.clone();
    let src = req.src;
    let reading = tokio::task::spawn_blocking(move || reader(mode, src, tx));

    let begin = if req.in_transaction { "SAVEPOINT vareto_import" } else { "BEGIN" };
    sqlx::raw_sql(begin).execute(&mut *c).await?;
    let mut messages = Vec::new();
    let (mut rows, mut errors, mut lines, mut skipped) = (0u64, 0u64, 0u64, 0u64);
    let mut ncols = (existing > 0).then_some(existing as usize);
    let res: Result<()> = async {
        let mut pending: Vec<(u64, Vec<Option<String>>)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            for (line, fields) in batch? {
                if req.cancelled.load(Ordering::SeqCst) {
                    bail!("interrupted");
                }
                lines = line;
                if skipped < req.skip {
                    skipped += 1;
                    continue;
                }
                let n = match ncols {
                    Some(n) => n,
                    None => {
                        // A new table: its columns are the first row.
                        let cols: Vec<String> = fields.iter().map(|f| crate::db::sql::quote_ident(f)).collect();
                        let create = match &req.schema {
                            Some(s) => format!("CREATE TABLE {}.{}(\n{})", crate::db::sql::quote_ident(s), crate::db::sql::quote_ident(&req.table), cols.join(", ")),
                            None => format!("CREATE TABLE {}(\n{})", crate::db::sql::quote_ident(&req.table), cols.join(", ")),
                        };
                        sqlx::raw_sql(AssertSqlSafe(create)).execute(&mut *c).await?;
                        ncols = Some(fields.len());
                        continue;
                    }
                };
                let found = fields.len();
                let mut row: Vec<Option<String>> = fields.into_iter().map(Some).collect();
                if found < n {
                    messages.push((line, format!("{}:{line}: expected {n} columns but found {found} - filling the rest with NULL", req.file)));
                    row.resize(n, None);
                } else if found > n {
                    messages.push((line, format!("{}:{line}: expected {n} columns but found {found} - extras ignored", req.file)));
                    row.truncate(n);
                }
                pending.push((line, row));
                if pending.len() * n >= MAX_PARAMS || pending.len() >= 500 {
                    flush(c, &target, n, &mut pending, &req.file, &mut rows, &mut errors, &mut messages).await?;
                }
            }
        }
        if let Some(n) = ncols {
            flush(c, &target, n, &mut pending, &req.file, &mut rows, &mut errors, &mut messages).await?;
        }
        if req.cancelled.load(Ordering::SeqCst) {
            bail!("interrupted");
        }
        if req.peer.as_ref().is_some_and(|p| crate::ipc::peer_closed(std::os::fd::AsFd::as_fd(p))) {
            bail!("the client went away during .import");
        }
        Ok(())
    }
    .await;
    drop(rx);
    reading.await.ok();
    // In file order, as sqlite3 prints them while it goes (a refused row's message
    // comes after that row's column message).
    messages.sort_by_key(|(line, _)| *line);
    let messages: Vec<String> = messages.into_iter().map(|(_, m)| m).collect();
    let end = match (&res, req.in_transaction) {
        (Ok(()), true) => "RELEASE vareto_import",
        (Ok(()), false) => "COMMIT",
        (Err(_), true) => "ROLLBACK TO vareto_import",
        (Err(_), false) => "ROLLBACK",
    };
    let ended = sqlx::raw_sql(end).execute(&mut *c).await;
    if req.in_transaction && res.is_err() {
        sqlx::raw_sql("RELEASE vareto_import").execute(&mut *c).await.ok();
    }
    res.with_context(|| format!("{}: import failed", req.file))?;
    ended?;
    Ok(json!({ "rows": rows, "errors": errors, "lines": lines, "messages": messages }))
}

/// Insert the pending rows. A batch the database refuses goes again row by row:
/// as in sqlite3, a refused row is reported and the others still go in.
#[allow(clippy::too_many_arguments)]
async fn flush(
    c: &mut SqliteConnection,
    target: &str,
    ncols: usize,
    pending: &mut Vec<(u64, Vec<Option<String>>)>,
    file: &str,
    rows: &mut u64,
    errors: &mut u64,
    messages: &mut Vec<(u64, String)>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    if let Ok(n) = insert(c, target, ncols, pending).await {
        *rows += n;
        pending.clear();
        return Ok(());
    }
    for (line, row) in pending.drain(..) {
        match insert(c, target, ncols, std::slice::from_ref(&(line, row))).await {
            Ok(n) => *rows += n,
            Err(e) => {
                *errors += 1;
                let msg = match e.downcast_ref::<sqlx::Error>() {
                    Some(sqlx::Error::Database(d)) => d.message().to_string(),
                    _ => e.to_string(),
                };
                messages.push((line, format!("{file}:{line}: INSERT failed: {msg}")));
            }
        }
    }
    Ok(())
}
