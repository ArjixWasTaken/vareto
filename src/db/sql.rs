//! MySQL/MariaDB and PostgreSQL via sqlx.
//!
//! Everything runs through `raw_sql` (simple/text protocol), so every value arrives
//! as text and is converted to JSON based on the column type name.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::{AssertSqlSafe, Column as _, ConnectOptions, Either, Executor, Row, TypeInfo, ValueRef};

use crate::config::{Connection, Kind};
use crate::render::{Column, Output};

pub enum SqlPool {
    Mysql(MySqlPool),
    Postgres(PgPool),
}

pub struct Endpoint<'a> {
    pub host: &'a str,
    pub port: u16,
    pub password: Option<&'a str>,
}

pub async fn connect(conn: &Connection, ep: &Endpoint<'_>) -> Result<SqlPool> {
    let cfg = &conn.cfg;
    let pool_opts = |n| (n, Duration::from_secs(15));
    match cfg.kind {
        Kind::Postgres => {
            let mut o = PgConnectOptions::new_without_pgpass().host(ep.host).port(ep.port);
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
    if row.try_get_raw(i)?.is_null() {
        return Ok(Value::Null);
    }
    if let Ok(s) = row.try_get_unchecked::<&str, _>(i) {
        return Ok(json_from_text(type_name, s));
    }
    let bytes: Vec<u8> = row.try_get_unchecked(i)?;
    use base64::Engine;
    Ok(Value::String(format!("base64:{}", base64::engine::general_purpose::STANDARD.encode(bytes))))
}

/// Collect a `fetch_many` stream into an [`Output`].
async fn collect<'e, DB, R>(
    mut stream: futures::stream::BoxStream<'e, Result<Either<DB::QueryResult, R>, sqlx::Error>>,
    limit: usize,
    affected_of: fn(&DB::QueryResult) -> u64,
) -> Result<Output>
where
    DB: sqlx::Database,
    R: Row<Database = DB>,
    usize: sqlx::ColumnIndex<R>,
    for<'r> &'r str: sqlx::Decode<'r, R::Database>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database>,
{
    let mut columns: Option<Vec<Column>> = None;
    let mut rows = Vec::new();
    let mut affected = 0u64;
    let mut truncated = false;
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(res) => affected += affected_of(&res),
            Either::Right(row) => {
                if columns.is_none() {
                    columns = Some(
                        row.columns()
                            .iter()
                            .map(|c| Column {
                                name: c.name().to_string(),
                                type_name: Some(c.type_info().name().to_string()),
                                table: None,
                            })
                            .collect(),
                    );
                }
                if rows.len() >= limit {
                    truncated = true;
                    break;
                }
                let cols = columns.as_ref().unwrap();
                let mut out = Vec::with_capacity(cols.len());
                for (i, c) in cols.iter().enumerate() {
                    out.push(cell(&row, i, c.type_name.as_deref().unwrap_or(""))?);
                }
                rows.push(out);
            }
        }
    }
    Ok(match columns {
        Some(columns) => Output::Table { columns, rows, truncated },
        None => Output::Affected { rows: affected, detail: None },
    })
}

impl SqlPool {
    /// Run user SQL. In read-only mode it runs inside a READ ONLY transaction that is
    /// always rolled back.
    pub async fn run(&self, sql: &str, limit: usize, readonly: bool, timeout: Duration) -> Result<Output> {
        let ms = timeout.as_millis();
        let fut = async {
            match self {
                SqlPool::Postgres(pool) => {
                    if readonly {
                        let mut tx = pool.begin_with("BEGIN READ ONLY").await?;
                        sqlx::raw_sql(AssertSqlSafe(format!("SET LOCAL statement_timeout = {ms}"))).execute(&mut *tx).await?;
                        let out = collect(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), limit, |r: &sqlx::postgres::PgQueryResult| r.rows_affected()).await;
                        tx.rollback().await.ok();
                        out
                    } else {
                        let mut c = pool.acquire().await?;
                        sqlx::raw_sql(AssertSqlSafe(format!("SET statement_timeout = {ms}"))).execute(&mut *c).await?;
                        collect(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), limit, |r: &sqlx::postgres::PgQueryResult| r.rows_affected()).await
                    }
                }
                SqlPool::Mysql(pool) => {
                    if readonly {
                        let mut tx = pool.begin_with("START TRANSACTION READ ONLY").await?;
                        sqlx::raw_sql(AssertSqlSafe(format!("SET SESSION max_execution_time = {ms}"))).execute(&mut *tx).await.ok();
                        let out = collect(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *tx), limit, |r: &sqlx::mysql::MySqlQueryResult| r.rows_affected()).await;
                        tx.rollback().await.ok();
                        out
                    } else {
                        let mut c = pool.acquire().await?;
                        sqlx::raw_sql(AssertSqlSafe(format!("SET SESSION max_execution_time = {ms}"))).execute(&mut *c).await.ok();
                        collect(sqlx::raw_sql(AssertSqlSafe(sql)).fetch_many(&mut *c), limit, |r: &sqlx::mysql::MySqlQueryResult| r.rows_affected()).await
                    }
                }
            }
        };
        tokio::time::timeout(timeout + Duration::from_secs(5), fut)
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {}s", timeout.as_secs()))?
    }

    pub async fn close(&self) {
        match self {
            SqlPool::Mysql(p) => p.close().await,
            SqlPool::Postgres(p) => p.close().await,
        }
    }

    // ------------------------------------------------------------ introspection

    pub async fn list_databases(&self, t: Duration) -> Result<Output> {
        let sql = match self {
            SqlPool::Postgres(_) => "SELECT datname AS database FROM pg_database WHERE NOT datistemplate ORDER BY 1",
            SqlPool::Mysql(_) => "SELECT schema_name AS `database` FROM information_schema.schemata ORDER BY 1",
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
