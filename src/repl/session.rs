//! The shell's side of a daemon session: blocking calls on the shell thread.

use std::future::Future;
use std::os::fd::{AsFd as _, BorrowedFd};
use std::pin::Pin;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::runtime::Handle;

use crate::client::Daemon;
use crate::config::Kind;
use crate::ipc::{Client, QuerySpec, Request};

/// Transaction state, tracked from the statements the shell sends (psql shows it
/// in the prompt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tx {
    Idle,
    Active,
    /// Postgres: an error inside the transaction; everything fails until ROLLBACK.
    Failed,
}

pub struct Session {
    rt: Handle,
    /// Away while a streaming request is in flight ([`Session::start_with_fd`]).
    client: Option<Client>,
    pending: Option<tokio::task::JoinHandle<(Client, Result<Value>)>>,
    force_readonly: bool,
    pub id: u64,
    pub connection: String,
    pub kind: Kind,
    pub readonly: bool,
    pub server_version: String,
    pub user: Option<String>,
    pub host: String,
    pub database: Option<String>,
    /// The server's id for this connection (Postgres backend pid, MySQL connection id).
    pub backend_id: Option<i64>,
    pub tx: Tx,
    /// Row cap per statement (`--limit`); the daemon's default when unset.
    pub limit: Option<usize>,
    /// MySQL `warnings` / `\W`: fetch each statement's warnings, not just their count.
    pub show_warnings: bool,
}

impl Session {
    pub fn open(rt: Handle, mut client: Client, force_readonly: bool, connection: &str, database: Option<String>, limit: Option<usize>) -> Result<Self> {
        let req = Request::SessionOpen { connection: connection.into(), database };
        let v = rt.block_on(client.call(&req))?;
        let kind: Kind = serde_json::from_value(v["kind"].clone()).context("session kind")?;
        Ok(Self {
            rt,
            client: Some(client),
            pending: None,
            force_readonly,
            id: v["session"].as_u64().context("session id")?,
            connection: connection.into(),
            kind,
            readonly: v["readonly"].as_bool() == Some(true),
            server_version: v["server_version"].as_str().unwrap_or("").into(),
            user: v["user"].as_str().map(str::to_string),
            host: v["host"].as_str().unwrap_or("").into(),
            database: v["database"].as_str().map(str::to_string),
            backend_id: v["backend_id"].as_i64(),
            tx: Tx::Idle,
            limit,
            show_warnings: false,
        })
    }

    /// A request on the session's socket. Ctrl-C while it runs asks the daemon
    /// (over a second socket) to cancel the session's query.
    pub fn call(&mut self, req: Request) -> Result<Value> {
        self.interruptible(|c| Box::pin(async move { c.call(&req).await }))
    }

    /// [`Self::call`] for a request that streams through `fd` (COPY).
    pub fn call_with_fd(&mut self, req: Request, fd: BorrowedFd<'_>) -> Result<Value> {
        self.interruptible(|c| Box::pin(async move { c.call_with_fd(&req, fd).await }))
    }

    /// Run a request; Ctrl-C meanwhile asks the daemon (over a second socket) to
    /// cancel the session's query.
    fn interruptible<'a>(&'a mut self, f: impl FnOnce(&'a mut Client) -> Pin<Box<dyn Future<Output = Result<Value>> + 'a>>) -> Result<Value> {
        let (id, ro) = (self.id, self.force_readonly);
        let client = self.client.as_mut().context("a streaming request is still in progress")?;
        self.rt.block_on(super::interrupt::during(f(client), move || cancel(id, ro)))
    }

    /// Start a request that reads from a pipe, and return the pipe's write end. The
    /// caller writes the data and drops it, then calls [`Self::finish_streaming`].
    /// Writes fail with a broken pipe if the daemon gives up early.
    pub fn start_with_fd(&mut self, req: Request) -> Result<std::fs::File> {
        let mut client = self.client.take().context("a streaming request is still in progress")?;
        let (read, write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)?;
        self.pending = Some(self.rt.spawn(async move {
            let res = match client.send_with_fd(&req, read.as_fd()).await {
                Ok(()) => {
                    // Only the daemon may hold the read end, so its exit breaks the pipe.
                    drop(read);
                    client.response().await
                }
                Err(e) => Err(e),
            };
            (client, res)
        }));
        Ok(std::fs::File::from(write))
    }

    /// The result of the request started by [`Self::start_with_fd`]; Ctrl-C while
    /// waiting cancels it.
    pub fn finish_streaming(&mut self) -> Result<Value> {
        let task = self.pending.take().context("no streaming request in progress")?;
        let (id, ro) = (self.id, self.force_readonly);
        let joined = self.rt.block_on(super::interrupt::during(
            async move { task.await.map_err(anyhow::Error::from) },
            move || cancel(id, ro),
        ));
        let (client, res) = joined?;
        self.client = Some(client);
        res
    }

    /// Ask the daemon to cancel the session's running request.
    pub fn cancel(&mut self) {
        self.rt.block_on(cancel(self.id, self.force_readonly));
    }

    pub fn query(&mut self, query: QuerySpec) -> Result<Value> {
        self.query_limit(query, self.limit)
    }

    pub fn query_limit(&mut self, query: QuerySpec, limit: Option<usize>) -> Result<Value> {
        let sql = match &query {
            QuerySpec::Sql { sql } => Some(sql.clone()),
            _ => None,
        };
        let res = self.call(Request::SessionQuery { session: self.id, query, limit, warnings: self.show_warnings });
        if let Some(sql) = sql {
            self.tx = next_tx(self.kind, self.tx, &sql, res.is_ok());
        }
        let v = res?;
        if v["reconnected"].as_bool() == Some(true) {
            eprintln!("(the connection was re-established; session state such as transactions or SET was reset)");
            self.tx = Tx::Idle;
        }
        if let Some(db) = v.get("database") {
            self.database = db.as_str().map(str::to_string);
        }
        Ok(v)
    }

    pub fn sql(&mut self, sql: &str) -> Result<Value> {
        self.query(QuerySpec::Sql { sql: sql.into() })
    }

    /// Postgres `COPY … FROM STDIN`, the daemon reading `src` to its end.
    /// Returns `{"rows": n}`.
    pub fn copy_in(&mut self, sql: &str, src: BorrowedFd<'_>) -> Result<Value> {
        let res = self.call_with_fd(Request::SessionCopyIn { session: self.id, sql: sql.into(), framed: false }, src);
        self.track(sql, res.is_ok());
        res
    }

    /// `COPY … FROM STDIN` with data the caller writes into the returned pipe as
    /// `ipc::copy_frame`s, ending with an empty one ([`Self::start_with_fd`]); then
    /// [`Self::finish_streaming`] and [`Self::track`]. Without the empty frame (the
    /// shell died), the daemon aborts the COPY.
    pub fn start_copy_in(&mut self, sql: &str) -> Result<std::fs::File> {
        self.start_with_fd(Request::SessionCopyIn { session: self.id, sql: sql.into(), framed: true })
    }

    /// Postgres `COPY … TO STDOUT`, the daemon writing to `dst`. Returns `{"rows": n}`.
    pub fn copy_out(&mut self, sql: &str, dst: BorrowedFd<'_>) -> Result<Value> {
        let res = self.call_with_fd(Request::SessionCopyOut { session: self.id, sql: sql.into() }, dst);
        self.track(sql, res.is_ok());
        res
    }

    /// Run `sql`, the daemon writing its rows in `format` to `dst` with no row
    /// limit. Returns `{"rows": n}` (and MySQL `warnings`).
    pub fn stream(&mut self, sql: &str, format: crate::textfmt::StreamFormat, dst: BorrowedFd<'_>) -> Result<Value> {
        let req = Request::SessionStream { session: self.id, sql: sql.into(), format, warnings: self.show_warnings };
        let res = self.call_with_fd(req, dst);
        self.track(sql, res.is_ok());
        res
    }

    /// Update the transaction state after `sql` ran.
    pub fn track(&mut self, sql: &str, ok: bool) {
        self.tx = next_tx(self.kind, self.tx, sql, ok);
    }

    /// A statement's result columns without running it: `{"columns": [{name, type, oid}]}`.
    pub fn describe(&mut self, sql: &str) -> Result<Value> {
        self.call(Request::SessionDescribe { session: self.id, sql: sql.into() })
    }

    pub fn use_db(&mut self, database: &str) -> Result<()> {
        let v = self.call(Request::SessionUse { session: self.id, database: database.into() })?;
        self.database = v["database"].as_str().map(str::to_string);
        self.backend_id = v["backend_id"].as_i64().or(self.backend_id);
        self.tx = Tx::Idle;
        Ok(())
    }

    /// A fresh connection for the session (mysql `connect`, `resetconnection`).
    pub fn reset(&mut self) -> Result<()> {
        let v = self.call(Request::SessionReset { session: self.id })?;
        self.database = v["database"].as_str().map(str::to_string);
        self.backend_id = v["backend_id"].as_i64();
        self.tx = Tx::Idle;
        Ok(())
    }

    pub fn list_databases(&mut self) -> Result<Value> {
        self.call(Request::ListDatabases { connection: self.connection.clone() })
    }

    pub fn list_tables(&mut self, database: Option<String>, pattern: Option<String>) -> Result<Value> {
        self.call(Request::ListTables { connection: self.connection.clone(), database, pattern })
    }

    pub fn close(&mut self) {
        if let Some(task) = self.pending.take() {
            self.cancel();
            if let Ok((client, _)) = self.rt.block_on(task) {
                self.client = Some(client);
            }
        }
        let req = Request::SessionClose { session: self.id };
        if let Some(c) = self.client.as_mut() {
            self.rt.block_on(c.call(&req)).ok();
        }
    }
}

async fn cancel(id: u64, force_readonly: bool) {
    let d = Daemon { force_readonly };
    if let Ok(mut c) = d.connect().await {
        c.call(&Request::SessionCancel { session: id }).await.ok();
    }
}

/// The transaction state after running `sql`.
fn next_tx(kind: Kind, tx: Tx, sql: &str, ok: bool) -> Tx {
    let words: Vec<String> = sql.split_whitespace().take(2).map(|w| w.trim_end_matches(';').to_ascii_uppercase()).collect();
    let first = words.first().map(String::as_str).unwrap_or("");
    let second = words.get(1).map(String::as_str).unwrap_or("");
    match first {
        "BEGIN" if ok => Tx::Active,
        "START" if ok && second == "TRANSACTION" => Tx::Active,
        "COMMIT" | "END" | "ABORT" => Tx::Idle,
        "ROLLBACK" if second != "TO" => Tx::Idle,
        // MySQL commits implicitly before DDL and locking statements.
        "CREATE" | "ALTER" | "DROP" | "RENAME" | "TRUNCATE" | "LOCK" | "UNLOCK" if kind == Kind::Mysql && ok => Tx::Idle,
        _ if !ok && tx == Tx::Active && kind == Kind::Postgres => Tx::Failed,
        _ => tx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_tracking() {
        let pg = |tx, sql, ok| next_tx(Kind::Postgres, tx, sql, ok);
        assert_eq!(pg(Tx::Idle, "begin", true), Tx::Active);
        assert_eq!(pg(Tx::Active, "select 1", true), Tx::Active);
        assert_eq!(pg(Tx::Active, "select x", false), Tx::Failed);
        assert_eq!(pg(Tx::Failed, "rollback to savepoint a", true), Tx::Failed);
        assert_eq!(pg(Tx::Failed, "ROLLBACK;", true), Tx::Idle);
        assert_eq!(pg(Tx::Idle, "select x", false), Tx::Idle);
        let my = |tx, sql, ok| next_tx(Kind::Mysql, tx, sql, ok);
        assert_eq!(my(Tx::Idle, "START TRANSACTION", true), Tx::Active);
        assert_eq!(my(Tx::Active, "select x", false), Tx::Active);
        assert_eq!(my(Tx::Active, "create table t (a int)", true), Tx::Idle);
    }
}
