//! MySQL/MariaDB, PostgreSQL and SQLite via sqlx.
//!
//! Everything runs through `raw_sql` (simple/text protocol), so every value arrives
//! as text and is converted to JSON based on the column type name.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, ConnectOptions, Connection as _, Either, Executor, Row, TypeInfo, ValueRef};

use crate::config::{Connection, Kind};
use crate::readonly::SqlDialect;
use crate::render::{Collector, Column, Output, Sink};

pub enum SqlPool {
    Mysql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub struct Endpoint<'a> {
    pub host: &'a str,
    pub port: u16,
    /// A local server's unix socket, instead of host/port.
    pub socket: Option<&'a std::path::Path>,
    pub password: Option<&'a str>,
}

pub async fn connect(conn: &Connection, ep: &Endpoint<'_>) -> Result<SqlPool> {
    let cfg = &conn.cfg;
    let pool_opts = |n| (n, Duration::from_secs(15));
    match cfg.kind {
        Kind::Postgres => {
            let mut o = PgConnectOptions::new_without_pgpass().host(ep.host).port(ep.port);
            if let Some(sock) = ep.socket {
                o = o.socket(sock);
            }
            if let Some(u) = &cfg.user {
                o = o.username(u);
            }
            if let Some(p) = ep.password {
                o = o.password(p);
            }
            if let Some(db) = &cfg.database {
                o = o.database(db);
            }
            o = o.application_name("vareto").disable_statement_logging();
            let mut server_opts: Vec<(String, String)> = Vec::new();
            for (k, v) in &cfg.params {
                match k.as_str() {
                    "sslmode" | "ssl_mode" => o = o.ssl_mode(v.parse::<PgSslMode>()?),
                    "sslrootcert" => o = o.ssl_root_cert(v.as_str()),
                    "application_name" => o = o.application_name(v),
                    // Anything else is passed as a server run-time parameter (`-c k=v`).
                    _ => server_opts.push((k.clone(), v.clone())),
                }
            }
            if conn.readonly {
                server_opts.push(("default_transaction_read_only".into(), "on".into()));
            }
            o = o.options(server_opts);
            let (max, acquire) = pool_opts(4);
            let pool = PgPoolOptions::new()
                .max_connections(max)
                .acquire_timeout(acquire)
                .idle_timeout(Duration::from_secs(300))
                .connect_with(o)
                .await
                .context("connecting to PostgreSQL")?;
            Ok(SqlPool::Postgres(pool))
        }
        Kind::Mysql => {
            let mut o = MySqlConnectOptions::new().host(ep.host).port(ep.port).disable_statement_logging();
            if let Some(sock) = ep.socket {
                o = o.socket(sock);
            }
            if let Some(u) = &cfg.user {
                o = o.username(u);
            }
            if let Some(p) = ep.password {
                o = o.password(p);
            }
            if let Some(db) = &cfg.database {
                o = o.database(db);
            }
            for (k, v) in &cfg.params {
                match k.as_str() {
                    "ssl-mode" | "ssl_mode" | "sslmode" => o = o.ssl_mode(v.parse::<MySqlSslMode>()?),
                    "ssl-ca" | "ssl_ca" => o = o.ssl_ca(v.as_str()),
                    "charset" => o = o.charset(v),
                    "timezone" => o = o.timezone(Some(v.clone())),
                    _ => bail!("unsupported MySQL param {k:?} (supported: ssl-mode, ssl-ca, charset, timezone)"),
                }
            }
            let readonly = conn.readonly;
            let (max, acquire) = pool_opts(4);
            let pool = MySqlPoolOptions::new()
                .max_connections(max)
                .acquire_timeout(acquire)
                .idle_timeout(Duration::from_secs(300))
                .after_connect(move |c, _| {
                    Box::pin(async move {
                        if readonly {
                            c.execute("SET SESSION TRANSACTION READ ONLY").await?;
                        }
                        Ok(())
                    })
                })
                .connect_with(o)
                .await
                .context("connecting to MySQL")?;
            Ok(SqlPool::Mysql(pool))
        }
        Kind::Sqlite => {
            let path = match conn.route() {
                crate::config::Route::File(p) => p,
                _ => unreachable!("validated: sqlite has a path"),
            };
            if let Some(k) = cfg.params.keys().next() {
                bail!("unsupported SQLite param {k:?}");
            }
            // Read-only connections open the file read-only (SQLITE_OPEN_READONLY),
            // on top of the statement checks.
            let o = SqliteConnectOptions::new()
                .filename(&path)
                .read_only(conn.readonly)
                .create_if_missing(false)
                .busy_timeout(Duration::from_secs(5))
                .disable_statement_logging();
            let pool = SqlitePoolOptions::new()
                .max_connections(4)
                .acquire_timeout(Duration::from_secs(15))
                .idle_timeout(Duration::from_secs(300))
                .connect_with(o)
                .await
                .with_context(|| format!("opening SQLite database {}", path.display()))?;
            Ok(SqlPool::Sqlite(pool))
        }
        _ => unreachable!("not a SQL connection"),
    }
}

fn json_from_text(type_name: &str, text: &str) -> Value {
    let t = type_name.to_ascii_uppercase();
    let base = t.trim_end_matches(" UNSIGNED");
    match base {
        "INT2" | "INT4" | "INT8" | "OID" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER"
        | "BIGINT" | "YEAR" => text.parse::<i64>().map(Value::from).or_else(|_| text.parse::<u64>().map(Value::from)).unwrap_or_else(|_| text.into()),
        "FLOAT4" | "FLOAT8" | "FLOAT" | "DOUBLE" | "REAL" => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| text.into()),
        "BOOL" | "BOOLEAN" => match text {
            "t" | "true" | "1" => Value::Bool(true),
            "f" | "false" | "0" => Value::Bool(false),
            _ => text.into(),
        },
        "JSON" | "JSONB" => serde_json::from_str(text).unwrap_or_else(|_| text.into()),
        _ => text.into(),
    }
}

fn cell<R: Row>(row: &R, i: usize, type_name: &str) -> Result<Value>
where
    usize: sqlx::ColumnIndex<R>,
    for<'r> &'r str: sqlx::Decode<'r, R::Database>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database>,
{
    let raw = row.try_get_raw(i)?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    // SQLite expression columns have no declared type ("NULL"); use the value's.
    let dynamic = (type_name == "NULL").then(|| raw.type_info().name().to_string());
    let type_name = dynamic.as_deref().unwrap_or(type_name);
    if let Ok(s) = row.try_get_unchecked::<&str, _>(i) {
        return Ok(json_from_text(type_name, s));
    }
    let bytes: Vec<u8> = row.try_get_unchecked(i)?;
    use base64::Engine;
    Ok(Value::String(format!("base64:{}", base64::engine::general_purpose::STANDARD.encode(bytes))))
}

/// Feed a `fetch_many` stream into a sink, one statement at a time, stopping early
/// when it has enough. Returns whether any row arrived.
async fn drain<'e, DB, R>(
    mut stream: futures::stream::BoxStream<'e, Result<Either<DB::QueryResult, R>, sqlx::Error>>,
    sink: &mut dyn Sink,
    affected_of: fn(&DB::QueryResult) -> u64,
) -> Result<bool>
where
    DB: sqlx::Database,
    R: Row<Database = DB>,
    usize: sqlx::ColumnIndex<R>,
    for<'r> &'r str: sqlx::Decode<'r, R::Database>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database>,
{
    // Column types of the current statement's result set.
    let mut types: Option<Vec<String>> = None;
    let mut any_rows = false;
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(res) => {
                sink.end_statement(affected_of(&res))?;
                types = None;
            }
            Either::Right(row) => {
                any_rows = true;
                if types.is_none() {
                    let cols = sink_columns(row.columns());
                    types = Some(cols.iter().map(|c| c.type_name.clone().unwrap_or_default()).collect());
                    sink.columns(cols)?;
                }
                let types = types.as_ref().unwrap();
                let mut out = Vec::with_capacity(types.len());
                for (i, t) in types.iter().enumerate() {
                    out.push(cell(&row, i, t)?);
                }
                if !sink.row(out)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(any_rows)
}

/// Column metadata as the sink wants it.
fn sink_columns<C: sqlx::Column>(cols: &[C]) -> Vec<Column> {
    use sqlx::TypeInfo as _;
    cols.iter()
        .map(|c| {
            let info = c.type_info();
            let name = mysql_text_type(info.name(), &format!("{info:?}")).unwrap_or(info.name()).to_string();
            Column { name: c.name().to_string(), type_name: Some(name), table: None }
        })
        .collect()
}

/// sqlx names a MySQL column BINARY/VARBINARY/BLOB from its BINARY flag, which the
/// server also sets on text with a `_bin` collation (`SHOW DATABASES`, information
/// schema). Only the `binary` character set (collation 63) holds bytes, as the mysql
/// client decides: for anything else, the text type. `debug` is the type info's
/// `Debug` text, the only place sqlx shows the collation.
fn mysql_text_type(name: &str, debug: &str) -> Option<&'static str> {
    let text = match name {
        "BINARY" => "CHAR",
        "VARBINARY" => "VARCHAR",
        "TINYBLOB" => "TINYTEXT",
        "BLOB" => "TEXT",
        "MEDIUMBLOB" => "MEDIUMTEXT",
        "LONGBLOB" => "LONGTEXT",
        _ => return None,
    };
    let collation: u16 = debug.split("Collation(").nth(1)?.split(')').next()?.trim().parse().ok()?;
    (collation != 63).then_some(text)
}

/// The columns of `sql` without running it, for a query that returned no rows
/// (rows are where the column names come from otherwise). A macro rather than a
/// generic function: `Executor` on concrete connections keeps the futures `Send`.
macro_rules! describe_empty {
    ($conn:expr, $sql:expr, $sink:expr) => {{
        use sqlx::{SqlSafeStr as _, Statement as _};
        match $conn.prepare(AssertSqlSafe($sql.to_string()).into_sql_str()).await {
            Ok(stmt) => $sink.empty_result(sink_columns(stmt.columns())),
            Err(_) => Ok(()),
        }
    }};
}

impl SqlPool {
    /// Run user SQL, collecting up to `limit` rows.
    pub async fn run(&self, sql: &str, limit: usize, readonly: bool, timeout: Duration) -> Result<Output> {
        let mut c = Collector::new(limit, None);
        self.stream(sql, &mut c, readonly, timeout).await?;
        Ok(c.finish())
    }

    /// Run user SQL, sending rows to `sink` as they arrive. `timeout` is enforced by
    /// the server (statement_timeout / max_execution_time) and locally. In read-only
    /// mode it runs inside a READ ONLY transaction that is always rolled back.
    pub async fn stream(&self, sql: &str, sink: &mut dyn Sink, readonly: bool, timeout: Duration) -> Result<()> {
        let describe = crate::readonly::returns_rows(self.dialect(), sql);
        let fut = async {
            match self {
                SqlPool::Postgres(pool) => pg_stream(&mut *pool.acquire().await?, sql, sink, readonly, Some(timeout), describe).await,
                SqlPool::Mysql(pool) => mysql_stream(&mut *pool.acquire().await?, sql, sink, readonly, Some(timeout), describe, None).await,
                SqlPool::Sqlite(pool) => sqlite_stream(&mut *pool.acquire().await?, sql, sink, describe).await,
            }
        };
        tokio::time::timeout(timeout + Duration::from_secs(5), fut)
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {}s", timeout.as_secs()))?
            .map_err(server_message)
    }

    /// A dedicated connection taken out of the pool (for an interactive session).
    /// The statement timeout is set once, at the session level.
    pub async fn detach(&self, timeout: Duration) -> Result<SqlConn> {
        let mut c = match self {
            SqlPool::Postgres(pool) => SqlConn::Postgres(pool.acquire().await?.detach()),
            SqlPool::Mysql(pool) => SqlConn::Mysql(pool.acquire().await?.detach()),
            SqlPool::Sqlite(pool) => SqlConn::Sqlite(pool.acquire().await?.detach()),
        };
        c.set_timeout(timeout).await?;
        Ok(c)
    }

    /// A new dedicated Postgres connection to another database (psql `\c`).
    pub async fn connect_database(&self, database: &str, timeout: Duration) -> Result<SqlConn> {
        let mut c = match self {
            SqlPool::Postgres(pool) => {
                let opts = (*pool.connect_options()).clone().database(database);
                SqlConn::Postgres(sqlx::PgConnection::connect_with(&opts).await.with_context(|| format!("connecting to database {database:?}"))?)
            }
            SqlPool::Mysql(_) => bail!("MySQL switches databases with USE"),
            SqlPool::Sqlite(_) => bail!("a SQLite connection is one database file"),
        };
        c.set_timeout(timeout).await?;
        Ok(c)
    }

    pub fn dialect(&self) -> SqlDialect {
        match self {
            SqlPool::Postgres(_) => SqlDialect::Postgres,
            SqlPool::Mysql(_) => SqlDialect::Mysql,
            SqlPool::Sqlite(_) => SqlDialect::Sqlite,
        }
    }

    pub async fn close(&self) {
        match self {
            SqlPool::Mysql(p) => p.close().await,
            SqlPool::Postgres(p) => p.close().await,
            SqlPool::Sqlite(p) => p.close().await,
        }
    }

    // ------------------------------------------------------------ introspection

    pub async fn list_databases(&self, t: Duration) -> Result<Output> {
        let sql = match self {
            SqlPool::Postgres(_) => "SELECT datname AS database FROM pg_database WHERE NOT datistemplate ORDER BY 1",
            SqlPool::Mysql(_) => "SELECT schema_name AS `database` FROM information_schema.schemata ORDER BY 1",
            SqlPool::Sqlite(_) => "SELECT name AS database, file FROM pragma_database_list ORDER BY seq",
        };
        self.run(sql, 10_000, true, t).await
    }

    /// A string literal in this database's dialect.
    fn lit(&self, s: &str) -> String {
        match self {
            SqlPool::Mysql(_) => quote_lit_mysql(s),
            SqlPool::Postgres(_) | SqlPool::Sqlite(_) => quote_lit(s),
        }
    }

    /// `schema` = Postgres schema / MySQL database.
    pub async fn list_tables(&self, schema: Option<&str>, pattern: Option<&str>, t: Duration) -> Result<Output> {
        let like = pattern.map(|p| format!(" AND table_name LIKE {}", self.lit(&p.replace('*', "%"))));
        let sql = match self {
            SqlPool::Postgres(_) => format!(
                "SELECT table_schema AS schema, table_name AS name, table_type AS type \
                 FROM information_schema.tables WHERE {}{} ORDER BY 1, 2",
                match schema {
                    Some(s) => format!("table_schema = {}", quote_lit(s)),
                    None => "table_schema NOT IN ('pg_catalog', 'information_schema')".into(),
                },
                like.unwrap_or_default()
            ),
            SqlPool::Mysql(_) => format!(
                "SELECT table_name AS name, table_type AS type, table_rows AS approx_rows \
                 FROM information_schema.tables WHERE table_schema = {}{} ORDER BY 1",
                schema.map(quote_lit_mysql).unwrap_or_else(|| "DATABASE()".into()),
                like.unwrap_or_default()
            ),
            SqlPool::Sqlite(_) => format!(
                "SELECT name, type FROM {}.sqlite_schema WHERE type IN ('table', 'view') \
                 AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'{} ORDER BY 1",
                quote_ident(schema.unwrap_or("main")),
                like.unwrap_or_default()
            ),
        };
        self.run(&sql, 10_000, true, t).await
    }

    pub async fn describe(&self, schema: Option<&str>, table: &str, t: Duration) -> Result<Value> {
        let (schema, table) = match (schema, table.split_once('.')) {
            (None, Some((s, tb))) => (Some(s.to_string()), tb.to_string()),
            _ => (schema.map(str::to_string), table.to_string()),
        };
        let tl = self.lit(&table);
        let (cols, idx, fks) = match self {
            SqlPool::Postgres(_) => {
                let sl = quote_lit(schema.as_deref().unwrap_or("public"));
                (
                    format!(
                        "SELECT column_name AS name, data_type AS type, is_nullable AS nullable, column_default AS default \
                         FROM information_schema.columns WHERE table_schema = {sl} AND table_name = {tl} ORDER BY ordinal_position"
                    ),
                    format!("SELECT indexname AS name, indexdef AS definition FROM pg_indexes WHERE schemaname = {sl} AND tablename = {tl}"),
                    format!(
                        "SELECT conname AS name, pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c \
                         JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace \
                         WHERE c.contype = 'f' AND n.nspname = {sl} AND t.relname = {tl}"
                    ),
                )
            }
            SqlPool::Mysql(_) => {
                let sl = schema.as_deref().map(quote_lit_mysql).unwrap_or_else(|| "DATABASE()".into());
                (
                    format!(
                        "SELECT column_name AS name, column_type AS type, is_nullable AS nullable, column_default AS `default`, \
                         column_key AS `key`, extra FROM information_schema.columns \
                         WHERE table_schema = {sl} AND table_name = {tl} ORDER BY ordinal_position"
                    ),
                    format!(
                        "SELECT index_name AS name, non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns \
                         FROM information_schema.statistics WHERE table_schema = {sl} AND table_name = {tl} \
                         GROUP BY index_name, non_unique"
                    ),
                    format!(
                        "SELECT constraint_name AS name, column_name AS `column`, referenced_table_name AS ref_table, \
                         referenced_column_name AS ref_column FROM information_schema.key_column_usage \
                         WHERE table_schema = {sl} AND table_name = {tl} AND referenced_table_name IS NOT NULL"
                    ),
                )
            }
            SqlPool::Sqlite(_) => {
                let sl = quote_lit(schema.as_deref().unwrap_or("main"));
                (
                    format!(
                        "SELECT name, type, CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS nullable, \
                         dflt_value AS \"default\", pk FROM pragma_table_info({tl}, {sl}) ORDER BY cid"
                    ),
                    format!("SELECT name, \"unique\", origin FROM pragma_index_list({tl}, {sl})"),
                    format!(
                        "SELECT \"from\" AS \"column\", \"table\" AS ref_table, \"to\" AS ref_column \
                         FROM pragma_foreign_key_list({tl}, {sl})"
                    ),
                )
            }
        };
        let cols = self.run(&cols, 10_000, true, t).await?;
        if matches!(&cols, Output::Table { rows, .. } if rows.is_empty()) || matches!(cols, Output::Affected { .. }) {
            bail!("table {table:?} not found");
        }
        let idx = self.run(&idx, 10_000, true, t).await?;
        let fks = self.run(&fks, 10_000, true, t).await?;
        Ok(serde_json::json!({
            "table": table,
            "columns": cols.to_json(10_000),
            "indexes": idx.to_json(10_000),
            "foreign_keys": fks.to_json(10_000),
        }))
    }
}

async fn pg_stream(c: &mut sqlx::PgConnection, sql: &str, sink: &mut dyn Sink, readonly: bool, timeout: Option<Duration>, describe: bool) -> Result<()> {
    let affected = |r: &sqlx::postgres::PgQueryResult| r.rows_affected();
    if readonly {
        let mut tx = c.begin_with("BEGIN READ ONLY").await?;
        if let Some(t) = timeout {
            sqlx::raw_sql(AssertSqlSafe(format!("SET LOCAL statement_timeout = {}", t.as_millis()))).execute(&mut *tx).await?;
        }
        let mut out = drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), sink, affected).await;
        if describe && matches!(out, Ok(false)) {
            out = describe_empty!(&mut *tx, sql, sink).map(|()| false);
        }
        tx.rollback().await.ok();
        out.map(drop)
    } else {
        if let Some(t) = timeout {
            sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {}", t.as_millis()))).execute(&mut *c).await?;
        }
        if !drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, affected).await? && describe {
            describe_empty!(&mut *c, sql, sink)?;
        }
        Ok(())
    }
}

/// What a MySQL session wants to know about warnings after each statement.
pub struct Warnings {
    /// Also fetch the warnings themselves (`SHOW WARNINGS`), not only their count.
    pub list: bool,
    /// `{"count": n, "list": [{level, code, message}]}` once fetched.
    pub found: Option<Value>,
}

/// The statement's warnings, read on the same connection (and transaction) right
/// after it: anything else in between would reset them.
async fn mysql_warnings(c: &mut sqlx::MySqlConnection, list: bool) -> Option<Value> {
    use sqlx::Row as _;
    // SHOW WARNINGS first: it leaves the warnings alone, while any other statement
    // (even `SELECT @@warning_count`) clears them.
    let listed = if list {
        let rows = sqlx::raw_sql("SHOW WARNINGS").fetch_all(&mut *c).await.ok()?;
        let items: Vec<Value> = rows
            .iter()
            .map(|r| {
                let text = |i: usize| r.try_get_unchecked::<&str, _>(i).unwrap_or("").to_string();
                serde_json::json!({ "level": text(0), "code": text(1).parse::<u64>().unwrap_or(0), "message": text(2) })
            })
            .collect();
        Some(items)
    } else {
        None
    };
    let row = sqlx::raw_sql("SELECT @@warning_count").fetch_one(&mut *c).await.ok()?;
    let count: u64 = row.try_get_unchecked::<&str, _>(0).ok()?.parse().ok()?;
    let mut v = serde_json::json!({ "count": count });
    if let Some(items) = listed.filter(|l| !l.is_empty()) {
        v["list"] = Value::Array(items);
    }
    Some(v)
}

async fn mysql_stream(
    c: &mut sqlx::MySqlConnection,
    sql: &str,
    sink: &mut dyn Sink,
    readonly: bool,
    timeout: Option<Duration>,
    describe: bool,
    mut warnings: Option<&mut Warnings>,
) -> Result<()> {
    let affected = |r: &sqlx::mysql::MySqlQueryResult| r.rows_affected();
    let set_timeout = timeout.map(|t| format!("SET SESSION max_execution_time = {}", t.as_millis()));
    if readonly {
        let mut tx = c.begin_with("START TRANSACTION READ ONLY").await?;
        if let Some(s) = set_timeout {
            sqlx::raw_sql(AssertSqlSafe(s)).execute(&mut *tx).await.ok();
        }
        let mut out = drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), sink, affected).await;
        if let (Ok(_), Some(w)) = (&out, warnings.as_deref_mut()) {
            w.found = mysql_warnings(&mut tx, w.list).await;
        }
        if describe && matches!(out, Ok(false)) {
            out = describe_empty!(&mut *tx, sql, sink).map(|()| false);
        }
        tx.rollback().await.ok();
        out.map(drop)
    } else {
        if let Some(s) = set_timeout {
            sqlx::raw_sql(AssertSqlSafe(s)).execute(&mut *c).await.ok();
        }
        let rows = drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, affected).await?;
        if let Some(w) = warnings {
            w.found = mysql_warnings(c, w.list).await;
        }
        if !rows && describe {
            describe_empty!(&mut *c, sql, sink)?;
        }
        Ok(())
    }
}

/// A database error as the server words it, without sqlx's wrapper (and without
/// Postgres's `at line N`, which is a line in the server's own C source), with
/// its structured fields. MySQL keeps its code: `1146 (42S02): Table 'x' doesn't exist`.
pub fn server_error(e: anyhow::Error) -> anyhow::Error {
    server_message(e)
}

fn server_message(e: anyhow::Error) -> anyhow::Error {
    let Some(sqlx::Error::Database(d)) = e.downcast_ref::<sqlx::Error>() else { return e };
    crate::db::DbError::from_sqlx(d.as_ref()).into()
}

async fn sqlite_stream(c: &mut sqlx::SqliteConnection, sql: &str, sink: &mut dyn Sink, describe: bool) -> Result<()> {
    let affected = |r: &sqlx::sqlite::SqliteQueryResult| r.rows_affected();
    if !drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, affected).await? && describe {
        describe_empty!(&mut *c, sql, sink)?;
    }
    Ok(())
}

/// Where a COPY FROM's data comes from (see `ipc::Request::SessionCopyIn`).
pub struct CopySource {
    pub fd: OwnedFd,
    /// The data is in `ipc::copy_frame`s, ended by an empty one.
    pub framed: bool,
    /// The client's socket: a COPY whose client went away is aborted, not committed.
    pub peer: Option<OwnedFd>,
}

/// Decodes `ipc::copy_frame`s from a byte stream.
#[derive(Default)]
struct Unframer {
    /// Bytes of an unfinished length prefix.
    len_buf: Vec<u8>,
    /// Bytes still to come of the current chunk.
    left: usize,
    ended: bool,
}

impl Unframer {
    /// The data bytes in `input` (which may hold several chunks, or parts of them).
    fn feed(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(input.len());
        while !input.is_empty() {
            if self.ended {
                anyhow::bail!("unexpected COPY data after its end");
            }
            if self.left > 0 {
                let n = self.left.min(input.len());
                out.extend_from_slice(&input[..n]);
                self.left -= n;
                input = &input[n..];
                continue;
            }
            let n = (4 - self.len_buf.len()).min(input.len());
            self.len_buf.extend_from_slice(&input[..n]);
            input = &input[n..];
            if self.len_buf.len() == 4 {
                let len = u32::from_be_bytes([self.len_buf[0], self.len_buf[1], self.len_buf[2], self.len_buf[3]]) as usize;
                self.len_buf.clear();
                if len == 0 {
                    self.ended = true;
                } else {
                    self.left = len;
                }
            }
        }
        Ok(out)
    }
}

/// COPY data moves in chunks of this size.
const COPY_CHUNK: usize = 1 << 20;

/// Raise `statement_timeout` to `timeout` for a COPY (which may legitimately run as
/// long as an export); returns the setting to restore.
async fn pg_long_timeout(c: &mut sqlx::PgConnection, timeout: Duration) -> Result<String> {
    let old: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')").fetch_one(&mut *c).await?;
    sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {}", timeout.as_millis()))).execute(&mut *c).await?;
    Ok(old)
}

async fn restore_timeout(c: &mut sqlx::PgConnection, old: String) {
    sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {}", quote_lit(&old)))).execute(&mut *c).await.ok();
}

/// COPY's `QUOTE 'c'` option, if any.
fn copy_quote(sql: &str) -> Option<u8> {
    let upper = sql.to_ascii_uppercase();
    let at = upper.find("QUOTE")?;
    let rest = sql[at + 5..].trim_start();
    let rest = rest.strip_prefix("AS").or_else(|| rest.strip_prefix("as")).unwrap_or(rest).trim_start();
    let b = rest.as_bytes();
    (b.len() >= 3 && b[0] == b'\'' && b[2] == b'\'').then(|| b[1])
}

/// Counts the rows in COPY output as it streams by: lines for text, records for
/// CSV (less a header; a quoted field may hold newlines), tuples for the binary
/// format (recognised by its signature).
pub struct RowCounter {
    csv_header: bool,
    /// CSV format: records are counted by a CSV reader (a quoted field may hold
    /// newlines), not by lines.
    csv: Option<csv_core::Reader>,
    seen: u64,
    /// The first bytes, until there are enough to tell the format.
    head: Option<Vec<u8>>,
    binary: Option<Binary>,
}

/// Where the binary-format parser is: the bytes it still needs, and what they are.
struct Binary {
    pending: Vec<u8>,
    need: usize,
    part: Part,
    fields_left: u16,
    done: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Part {
    /// Signature (11), flags (4), header extension length (4).
    Header,
    HeaderExt,
    FieldCount,
    FieldLen,
    FieldData,
}

const PG_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

impl RowCounter {
    /// `sql`: the COPY statement, for its format options (CSV, HEADER, QUOTE).
    pub fn new(sql: &str) -> Self {
        let upper = sql.to_ascii_uppercase();
        let csv = upper.contains("CSV");
        let csv_header = csv && upper.contains("HEADER");
        let csv = csv.then(|| csv_core::ReaderBuilder::new().quote(copy_quote(sql).unwrap_or(b'"')).build());
        Self { csv_header, csv, seen: 0, head: Some(Vec::new()), binary: None }
    }

    /// Count the CSV records in `data` (`None`: the end of the data).
    fn csv_feed(&mut self, data: Option<&[u8]>) {
        let Some(r) = self.csv.as_mut() else { return };
        let (mut out, mut ends) = ([0u8; 4096], [0usize; 256]);
        let mut input = data.unwrap_or(&[]);
        loop {
            let (res, nin, _, _) = r.read_record(input, &mut out, &mut ends);
            input = &input[nin..];
            match res {
                csv_core::ReadRecordResult::Record => self.seen += 1,
                // Field bytes don't matter, only record ends: keep going.
                csv_core::ReadRecordResult::OutputFull | csv_core::ReadRecordResult::OutputEndsFull => {}
                csv_core::ReadRecordResult::InputEmpty if data.is_some() => return,
                csv_core::ReadRecordResult::InputEmpty => {}
                csv_core::ReadRecordResult::End => return,
            }
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        let Some(head) = self.head.as_mut() else { return self.count_in(data) };
        head.extend_from_slice(data);
        if head.len() < PG_BINARY_SIGNATURE.len() {
            return;
        }
        let head = self.head.take().expect("checked");
        if head.starts_with(PG_BINARY_SIGNATURE) {
            self.binary = Some(Binary { pending: Vec::new(), need: 19, part: Part::Header, fields_left: 0, done: false });
        }
        self.count_in(&head);
    }

    fn count_in(&mut self, mut data: &[u8]) {
        if self.binary.is_none() && self.csv.is_some() {
            return self.csv_feed(Some(data));
        }
        let Some(b) = self.binary.as_mut() else {
            // Text format: escapes keep newlines in data off the line ends.
            self.seen += data.iter().filter(|&&c| c == b'\n').count() as u64;
            return;
        };
        while !data.is_empty() && !b.done {
            let take = (b.need - b.pending.len()).min(data.len());
            if b.part == Part::FieldData || b.part == Part::HeaderExt {
                // Skipped bytes needn't be kept.
                b.need -= take;
            } else {
                b.pending.extend_from_slice(&data[..take]);
            }
            data = &data[take..];
            let complete = match b.part {
                Part::FieldData | Part::HeaderExt => b.need == 0,
                _ => b.pending.len() == b.need,
            };
            if !complete {
                continue;
            }
            let p = std::mem::take(&mut b.pending);
            match b.part {
                Part::Header => {
                    let ext = u32::from_be_bytes([p[15], p[16], p[17], p[18]]) as usize;
                    (b.part, b.need) = if ext > 0 { (Part::HeaderExt, ext) } else { (Part::FieldCount, 2) };
                }
                Part::HeaderExt => (b.part, b.need) = (Part::FieldCount, 2),
                Part::FieldCount => {
                    let n = i16::from_be_bytes([p[0], p[1]]);
                    if n < 0 {
                        b.done = true;
                    } else {
                        self.seen += 1;
                        b.fields_left = n as u16;
                        (b.part, b.need) = if n > 0 { (Part::FieldLen, 4) } else { (Part::FieldCount, 2) };
                    }
                }
                Part::FieldLen => {
                    let len = i32::from_be_bytes([p[0], p[1], p[2], p[3]]);
                    b.fields_left -= 1;
                    let next = if b.fields_left > 0 { (Part::FieldLen, 4) } else { (Part::FieldCount, 2) };
                    (b.part, b.need) = if len > 0 { (Part::FieldData, len as usize) } else { next };
                }
                Part::FieldData => {
                    (b.part, b.need) = if b.fields_left > 0 { (Part::FieldLen, 4) } else { (Part::FieldCount, 2) };
                }
            }
        }
    }

    /// The rows, once all the data was fed.
    pub fn count(&mut self) -> u64 {
        // Output shorter than the signature is text or CSV.
        if let Some(head) = self.head.take() {
            self.count_in(&head);
        }
        if self.binary.is_some() {
            return self.seen;
        }
        // A last record without a line end.
        self.csv_feed(None);
        self.csv.take();
        if self.csv_header { self.seen.saturating_sub(1) } else { self.seen }
    }
}

/// A single connection pinned to an interactive session, so session state (USE,
/// SET, transactions) carries over between statements.
pub enum SqlConn {
    Mysql(sqlx::MySqlConnection),
    Postgres(sqlx::PgConnection),
    Sqlite(sqlx::SqliteConnection),
}

impl SqlConn {
    pub async fn set_timeout(&mut self, t: Duration) -> Result<()> {
        let ms = t.as_millis();
        match self {
            SqlConn::Postgres(c) => {
                sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {ms}"))).execute(c).await?;
            }
            SqlConn::Mysql(c) => {
                // MariaDB calls it max_statement_time (in seconds). Setting the wrong one
                // would leave an "unknown variable" warning that table-less statements
                // (`SELECT 1`) keep reporting.
                let version: String = sqlx::query_scalar("SELECT VERSION()").fetch_one(&mut *c).await?;
                let set = if version.to_lowercase().contains("mariadb") {
                    format!("SET SESSION max_statement_time = {}", ms as f64 / 1000.0)
                } else {
                    format!("SET SESSION max_execution_time = {ms}")
                };
                sqlx::raw_sql(AssertSqlSafe(set)).execute(c).await.ok();
            }
            // No server-side statement timeout; the local one applies.
            SqlConn::Sqlite(_) => {}
        }
        Ok(())
    }

    /// Run user SQL on this connection. `readonly` wraps it in a READ ONLY
    /// transaction that is rolled back. MySQL also reports the statement's warnings
    /// into `warnings`.
    pub async fn stream(&mut self, sql: &str, sink: &mut dyn Sink, readonly: bool, warnings: Option<&mut Warnings>) -> Result<()> {
        let dialect = match self {
            SqlConn::Postgres(_) => SqlDialect::Postgres,
            SqlConn::Mysql(_) => SqlDialect::Mysql,
            SqlConn::Sqlite(_) => SqlDialect::Sqlite,
        };
        let describe = crate::readonly::returns_rows(dialect, sql);
        let res = match self {
            SqlConn::Postgres(c) => pg_stream(c, sql, sink, readonly, None, describe).await,
            SqlConn::Mysql(c) => mysql_stream(c, sql, sink, readonly, None, describe, warnings).await,
            // Read-only connections open the file read-only.
            SqlConn::Sqlite(c) => sqlite_stream(c, sql, sink, describe).await,
        };
        res.map_err(server_message)
    }

    pub async fn run(&mut self, sql: &str, limit: usize, readonly: bool) -> Result<Output> {
        let mut c = Collector::new(limit, None);
        self.stream(sql, &mut c, readonly, None).await?;
        Ok(c.finish())
    }

    /// Postgres `COPY … FROM STDIN`, fed from `src` (the client's file or pipe) until
    /// it ends. `cancelled` aborts the copy instead of committing it, even when the
    /// client closes its end first. `timeout` replaces the statement timeout for the
    /// copy. Returns the rows copied.
    pub async fn copy_in(&mut self, sql: &str, src: CopySource, cancelled: &AtomicBool, timeout: Duration) -> Result<u64> {
        use tokio::io::AsyncReadExt as _;
        let SqlConn::Postgres(c) = self else { anyhow::bail!("COPY FROM STDIN is Postgres-only") };
        let restore = pg_long_timeout(c, timeout).await?;
        let res = async {
            let mut w = c.copy_in_raw(sql).await?;
            let mut file = tokio::fs::File::from_std(std::fs::File::from(src.fd));
            let mut buf = vec![0u8; COPY_CHUNK];
            let mut frames = src.framed.then(Unframer::default);
            // Why the data ended badly, if it did; the COPY is then aborted.
            let failure = loop {
                if cancelled.load(Ordering::SeqCst) {
                    break Some("canceling statement due to user request".to_string());
                }
                let n = match file.read(&mut buf).await {
                    Ok(0) => {
                        break match &frames {
                            Some(f) if !f.ended => Some("the client went away during COPY".into()),
                            _ => None,
                        };
                    }
                    Ok(n) => n,
                    Err(e) => break Some(format!("could not read COPY data: {e}")),
                };
                match frames.as_mut() {
                    None => {
                        w.send(&buf[..n]).await?;
                    }
                    Some(f) => {
                        let data = match f.feed(&buf[..n]) {
                            Ok(d) => d,
                            Err(e) => break Some(e.to_string()),
                        };
                        if !data.is_empty() {
                            w.send(data.as_slice()).await?;
                        }
                        if f.ended {
                            break None;
                        }
                    }
                }
            };
            let failure = failure
                .or_else(|| cancelled.load(Ordering::SeqCst).then(|| "canceling statement due to user request".to_string()))
                .or_else(|| {
                    src.peer
                        .as_ref()
                        .is_some_and(|p| crate::ipc::peer_closed(std::os::fd::AsFd::as_fd(p)))
                        .then(|| "the client went away during COPY".to_string())
                });
            if let Some(why) = failure {
                w.abort(why.clone()).await.ok();
                anyhow::bail!(why);
            }
            Ok(w.finish().await?)
        }
        .await;
        restore_timeout(c, restore).await;
        res.map_err(server_message)
    }

    /// Postgres `COPY … TO STDOUT`, written to `dst` (the client's file, pipe or
    /// terminal) as it arrives. `readonly` runs it in a READ ONLY transaction.
    /// Returns the rows copied.
    pub async fn copy_out(&mut self, sql: &str, readonly: bool, dst: OwnedFd, timeout: Duration) -> Result<u64> {
        let SqlConn::Postgres(c) = self else { anyhow::bail!("COPY TO STDOUT is Postgres-only") };
        async fn write(c: &mut sqlx::PgConnection, sql: &str, dst: OwnedFd) -> Result<u64> {
            use tokio::io::AsyncWriteExt as _;
            let mut dst = tokio::fs::File::from_std(std::fs::File::from(dst));
            let mut stream = c.copy_out_raw(sql).await?;
            let mut rows = RowCounter::new(sql);
            let mut failed = None;
            while let Some(chunk) = stream.try_next().await? {
                rows.feed(&chunk);
                // On a write error (the reader went away) keep draining, so the
                // connection stays usable.
                if failed.is_none() {
                    if let Err(e) = dst.write_all(&chunk).await {
                        failed = Some(e);
                    }
                }
            }
            if failed.is_none() {
                if let Err(e) = dst.flush().await {
                    failed = Some(e);
                }
            }
            match failed {
                Some(e) if e.kind() != std::io::ErrorKind::BrokenPipe => anyhow::bail!("could not write COPY data: {e}"),
                _ => Ok(rows.count()),
            }
        }
        let restore = pg_long_timeout(c, timeout).await?;
        let res = if readonly {
            match c.begin_with("BEGIN READ ONLY").await {
                Ok(mut tx) => {
                    let r = write(&mut tx, sql, dst).await;
                    tx.rollback().await.ok();
                    r
                }
                Err(e) => Err(e.into()),
            }
        } else {
            write(c, sql, dst).await
        };
        restore_timeout(c, restore).await;
        res.map_err(server_message)
    }

    /// The result columns of `sql` without running it: `[{name, type, oid}]`
    /// (`oid` on Postgres only).
    pub async fn describe(&mut self, sql: &str) -> Result<Vec<Value>> {
        use sqlx::{Column as _, SqlSafeStr as _, Statement as _, TypeInfo as _};
        let stmt = AssertSqlSafe(sql.to_string()).into_sql_str();
        let cols = match self {
            SqlConn::Postgres(c) => c
                .prepare(stmt)
                .await
                .map(|s| {
                    s.columns()
                        .iter()
                        .map(|c| serde_json::json!({ "name": c.name(), "type": c.type_info().name(), "oid": c.type_info().oid().map(|o| o.0) }))
                        .collect()
                }),
            SqlConn::Mysql(c) => c
                .prepare(stmt)
                .await
                .map(|s| s.columns().iter().map(|c| serde_json::json!({ "name": c.name(), "type": c.type_info().name() })).collect()),
            SqlConn::Sqlite(c) => c
                .prepare(stmt)
                .await
                .map(|s| s.columns().iter().map(|c| serde_json::json!({ "name": c.name(), "type": c.type_info().name() })).collect()),
        };
        cols.map_err(|e| server_message(e.into()))
    }

    /// A single scalar from an internal query.
    pub async fn scalar(&mut self, sql: &str) -> Result<Value> {
        match self.run(sql, 1, false).await? {
            Output::Table { rows, .. } => Ok(rows.into_iter().next().and_then(|r| r.into_iter().next()).unwrap_or(Value::Null)),
            _ => Ok(Value::Null),
        }
    }

    pub async fn close(self) {
        use sqlx::Connection as _;
        match self {
            SqlConn::Postgres(c) => c.close().await.ok(),
            SqlConn::Mysql(c) => c.close().await.ok(),
            SqlConn::Sqlite(c) => c.close().await.ok(),
        };
    }
}

/// A double-quoted identifier.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// A standard-SQL string literal (Postgres, SQLite): quotes doubled; a backslash
/// is an ordinary character.
pub fn quote_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A MySQL string literal: backslashes escaped too (the default sql_mode reads
/// them as escapes).
pub fn quote_lit_mysql(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// A MySQL identifier: backquoted.
pub fn quote_ident_mysql(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mysql_bin_collations_are_text() {
        let dbg = |n: u16| format!("MySqlTypeInfo {{ type: VarString, flags: ColumnFlags(BINARY), collation: Collation({n}), max_size: Some(256) }}");
        assert_eq!(mysql_text_type("VARBINARY", &dbg(46)), Some("VARCHAR"));
        assert_eq!(mysql_text_type("BLOB", &dbg(83)), Some("TEXT"));
        assert_eq!(mysql_text_type("VARBINARY", &dbg(63)), None);
        assert_eq!(mysql_text_type("VARCHAR", &dbg(46)), None);
        // The Debug text really carries the collation.
        assert!(format!("{:?}", sqlx::mysql::MySqlTypeInfo::__enum()).contains("Collation("));
    }

    #[test]
    fn copy_frames() {
        let mut stream = crate::ipc::copy_frame(b"1\tone\n");
        stream.extend(crate::ipc::copy_frame(b"2\ttwo\n"));
        stream.extend(crate::ipc::copy_frame(b""));
        // Any split of the stream decodes the same.
        for size in [1, 3, 5, 64] {
            let mut u = Unframer::default();
            let data: Vec<u8> = stream.chunks(size).flat_map(|c| u.feed(c).unwrap()).collect();
            assert_eq!(data, b"1\tone\n2\ttwo\n");
            assert!(u.ended);
        }
        // Without the empty frame the data hasn't ended (the client went away).
        let mut u = Unframer::default();
        u.feed(&crate::ipc::copy_frame(b"x")).unwrap();
        assert!(!u.ended);
    }

    #[test]
    fn copy_row_counts() {
        let mut c = RowCounter::new("COPY t TO STDOUT");
        c.feed(b"1\tone\n2\t");
        c.feed(b"two\n");
        assert_eq!(c.count(), 2);
        let mut c = RowCounter::new("COPY t TO STDOUT WITH (FORMAT csv, HEADER)");
        c.feed(b"id,name\n1,one\n");
        assert_eq!(c.count(), 1);
        // CSV: a quoted newline is inside a record, and the last record may lack
        // its line end; split across chunks.
        let mut c = RowCounter::new("copy t to stdout csv");
        c.feed(b"1,\"multi\nli");
        c.feed(b"ne\"\n2,\"x\"\"\n\"\n3,last");
        assert_eq!(c.count(), 3);
        // A custom quote character.
        let mut c = RowCounter::new("COPY t TO STDOUT (FORMAT csv, QUOTE '|')");
        c.feed(b"1,|a\nb|\n");
        assert_eq!(c.count(), 1);

        // Binary format: header, then (int2 fields, [int4 len, bytes]...) per tuple,
        // then -1. Three tuples: (int4 7, NULL), (text "hi", text "") and ().
        let mut data = PG_BINARY_SIGNATURE.to_vec();
        data.extend_from_slice(&0u32.to_be_bytes()); // flags
        data.extend_from_slice(&0u32.to_be_bytes()); // no header extension
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&7i32.to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(b"hi");
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&0i16.to_be_bytes()); // a tuple with no columns
        data.extend_from_slice(&(-1i16).to_be_bytes());
        // Fed byte by byte, so every state crosses a chunk boundary; a `\n` byte
        // inside the binary data must not count.
        let mut c = RowCounter::new("COPY t TO STDOUT (FORMAT binary)");
        data.chunks(1).for_each(|b| c.feed(b));
        assert_eq!(c.count(), 3);
        let mut c = RowCounter::new("COPY t TO STDOUT (FORMAT binary)");
        c.feed(&data);
        assert_eq!(c.count(), 3);
    }

    #[test]
    fn text_conversion() {
        assert_eq!(json_from_text("INT4", "42"), json!(42));
        assert_eq!(json_from_text("BIGINT UNSIGNED", "18446744073709551615"), json!(18446744073709551615u64));
        assert_eq!(json_from_text("BOOL", "t"), json!(true));
        assert_eq!(json_from_text("JSONB", r#"{"a":1}"#), json!({"a": 1}));
        assert_eq!(json_from_text("NUMERIC", "1.10"), json!("1.10"));
        assert_eq!(json_from_text("FLOAT8", "NaN"), json!("NaN"));
        assert_eq!(quote_lit("it's"), "'it''s'");
        // Postgres and SQLite read a backslash as itself; MySQL as an escape.
        assert_eq!(quote_lit("a\\b"), "'a\\b'");
        assert_eq!(quote_lit_mysql("a\\b'c"), "'a\\\\b''c'");
        assert_eq!(quote_ident_mysql("we`ird"), "`we``ird`");
    }
}
