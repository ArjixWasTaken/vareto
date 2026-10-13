//! MySQL/MariaDB, PostgreSQL and SQLite via sqlx.
//!
//! Everything runs through `raw_sql` (simple/text protocol), so every value arrives
//! as text and is converted to JSON based on the column type name.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, Column as _, ConnectOptions, Connection as _, Either, Executor, Row, TypeInfo, ValueRef};

use crate::config::{Connection, Kind};
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

/// Feed a `fetch_many` stream into a sink, stopping early when it has enough.
async fn drain<'e, DB, R>(
    mut stream: futures::stream::BoxStream<'e, Result<Either<DB::QueryResult, R>, sqlx::Error>>,
    sink: &mut dyn Sink,
    affected_of: fn(&DB::QueryResult) -> u64,
) -> Result<()>
where
    DB: sqlx::Database,
    R: Row<Database = DB>,
    usize: sqlx::ColumnIndex<R>,
    for<'r> &'r str: sqlx::Decode<'r, R::Database>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database>,
{
    let mut types: Option<Vec<String>> = None;
    let mut affected = 0u64;
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(res) => affected += affected_of(&res),
            Either::Right(row) => {
                if types.is_none() {
                    let cols: Vec<Column> = row
                        .columns()
                        .iter()
                        .map(|c| Column {
                            name: c.name().to_string(),
                            type_name: Some(c.type_info().name().to_string()),
                            table: None,
                        })
                        .collect();
                    types = Some(cols.iter().map(|c| c.type_name.clone().unwrap_or_default()).collect());
                    sink.columns(cols)?;
                }
                let types = types.as_ref().unwrap();
                let mut out = Vec::with_capacity(types.len());
                for (i, t) in types.iter().enumerate() {
                    out.push(cell(&row, i, t)?);
                }
                if !sink.row(out)? {
                    return Ok(());
                }
            }
        }
    }
    if types.is_none() {
        sink.affected(affected, None)?;
    }
    Ok(())
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
        let fut = async {
            match self {
                SqlPool::Postgres(pool) => pg_stream(&mut *pool.acquire().await?, sql, sink, readonly, Some(timeout)).await,
                SqlPool::Mysql(pool) => mysql_stream(&mut *pool.acquire().await?, sql, sink, readonly, Some(timeout)).await,
                SqlPool::Sqlite(pool) => sqlite_stream(&mut *pool.acquire().await?, sql, sink).await,
            }
        };
        tokio::time::timeout(timeout + Duration::from_secs(5), fut)
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {}s", timeout.as_secs()))?
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

    /// `schema` = Postgres schema / MySQL database.
    pub async fn list_tables(&self, schema: Option<&str>, pattern: Option<&str>, t: Duration) -> Result<Output> {
        let like = pattern.map(|p| format!(" AND table_name LIKE {}", quote_lit(&p.replace('*', "%"))));
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
                schema.map(quote_lit).unwrap_or_else(|| "DATABASE()".into()),
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
        let tl = quote_lit(&table);
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
                let sl = schema.as_deref().map(quote_lit).unwrap_or_else(|| "DATABASE()".into());
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

async fn pg_stream(c: &mut sqlx::PgConnection, sql: &str, sink: &mut dyn Sink, readonly: bool, timeout: Option<Duration>) -> Result<()> {
    let affected = |r: &sqlx::postgres::PgQueryResult| r.rows_affected();
    if readonly {
        let mut tx = c.begin_with("BEGIN READ ONLY").await?;
        if let Some(t) = timeout {
            sqlx::raw_sql(AssertSqlSafe(format!("SET LOCAL statement_timeout = {}", t.as_millis()))).execute(&mut *tx).await?;
        }
        let out = drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), sink, affected).await;
        tx.rollback().await.ok();
        out
    } else {
        if let Some(t) = timeout {
            sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {}", t.as_millis()))).execute(&mut *c).await?;
        }
        drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, affected).await
    }
}

async fn mysql_stream(c: &mut sqlx::MySqlConnection, sql: &str, sink: &mut dyn Sink, readonly: bool, timeout: Option<Duration>) -> Result<()> {
    let affected = |r: &sqlx::mysql::MySqlQueryResult| r.rows_affected();
    let set_timeout = timeout.map(|t| format!("SET SESSION max_execution_time = {}", t.as_millis()));
    if readonly {
        let mut tx = c.begin_with("START TRANSACTION READ ONLY").await?;
        if let Some(s) = set_timeout {
            sqlx::raw_sql(AssertSqlSafe(s)).execute(&mut *tx).await.ok();
        }
        let out = drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), sink, affected).await;
        tx.rollback().await.ok();
        out
    } else {
        if let Some(s) = set_timeout {
            sqlx::raw_sql(AssertSqlSafe(s)).execute(&mut *c).await.ok();
        }
        drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, affected).await
    }
}

async fn sqlite_stream(c: &mut sqlx::SqliteConnection, sql: &str, sink: &mut dyn Sink) -> Result<()> {
    drain(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), sink, |r: &sqlx::sqlite::SqliteQueryResult| r.rows_affected()).await
}

/// A single connection pinned to an interactive session, so session state (USE,
/// SET, transactions) carries over between statements.
pub enum SqlConn {
    Mysql(sqlx::MySqlConnection),
    Postgres(sqlx::PgConnection),
    Sqlite(sqlx::SqliteConnection),
}

impl SqlConn {
    async fn set_timeout(&mut self, t: Duration) -> Result<()> {
        let ms = t.as_millis();
        match self {
            SqlConn::Postgres(c) => {
                sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {ms}"))).execute(c).await?;
            }
            SqlConn::Mysql(c) => {
                // Not supported by MariaDB; the local timeout still applies.
                sqlx::raw_sql(AssertSqlSafe(format!("SET SESSION max_execution_time = {ms}"))).execute(c).await.ok();
            }
            // No server-side statement timeout; the local one applies.
            SqlConn::Sqlite(_) => {}
        }
        Ok(())
    }

    /// Run user SQL on this connection. `readonly` wraps it in a READ ONLY
    /// transaction that is rolled back.
    pub async fn stream(&mut self, sql: &str, sink: &mut dyn Sink, readonly: bool) -> Result<()> {
        match self {
            SqlConn::Postgres(c) => pg_stream(c, sql, sink, readonly, None).await,
            SqlConn::Mysql(c) => mysql_stream(c, sql, sink, readonly, None).await,
            // Read-only connections open the file read-only.
            SqlConn::Sqlite(c) => sqlite_stream(c, sql, sink).await,
        }
    }

    pub async fn run(&mut self, sql: &str, limit: usize, readonly: bool) -> Result<Output> {
        let mut c = Collector::new(limit, None);
        self.stream(sql, &mut c, readonly).await?;
        Ok(c.finish())
    }

    /// A single scalar from an internal query.
    pub async fn scalar(&mut self, sql: &str) -> Result<Value> {
        match self.run(sql, 1, false).await? {
            Output::Table { rows, .. } => Ok(rows.into_iter().next().and_then(|r| r.into_iter().next()).unwrap_or(Value::Null)),
            _ => Ok(Value::Null),
        }
    }

    pub fn in_transaction(&self) -> bool {
        use sqlx::Connection as _;
        match self {
            SqlConn::Postgres(c) => c.is_in_transaction(),
            SqlConn::Mysql(c) => c.is_in_transaction(),
            SqlConn::Sqlite(c) => c.is_in_transaction(),
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

/// Standard-SQL string literal (also valid for MySQL with default sql_mode when
/// backslashes are escaped).
pub fn quote_lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_conversion() {
        assert_eq!(json_from_text("INT4", "42"), json!(42));
        assert_eq!(json_from_text("BIGINT UNSIGNED", "18446744073709551615"), json!(18446744073709551615u64));
        assert_eq!(json_from_text("BOOL", "t"), json!(true));
        assert_eq!(json_from_text("JSONB", r#"{"a":1}"#), json!({"a": 1}));
        assert_eq!(json_from_text("NUMERIC", "1.10"), json!("1.10"));
        assert_eq!(json_from_text("FLOAT8", "NaN"), json!("NaN"));
        assert_eq!(quote_lit("it's"), "'it''s'");
    }
}
