//! The shell's side of a daemon session: blocking calls on the shell thread.

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
    client: Client,
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
            client,
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
        let (id, ro) = (self.id, self.force_readonly);
        let client = &mut self.client;
        self.rt.block_on(super::interrupt::during(client.call(&req), || async move {
            let d = Daemon { force_readonly: ro };
            if let Ok(mut c) = d.connect().await {
                c.call(&Request::SessionCancel { session: id }).await.ok();
            }
        }))
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

    /// Postgres `COPY … FROM STDIN` with `data`. Returns `{"rows": n}`.
    pub fn copy_in(&mut self, sql: &str, data: String) -> Result<Value> {
        let res = self.call(Request::SessionCopyIn { session: self.id, sql: sql.into(), data });
        self.tx = next_tx(self.kind, self.tx, sql, res.is_ok());
        res
    }

    /// Postgres `COPY … TO STDOUT`. Returns `{"data": text}`.
    pub fn copy_out(&mut self, sql: &str) -> Result<Value> {
        let res = self.call(Request::SessionCopyOut { session: self.id, sql: sql.into() });
        self.tx = next_tx(self.kind, self.tx, sql, res.is_ok());
        res
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
        let req = Request::SessionClose { session: self.id };
        self.rt.block_on(self.client.call(&req)).ok();
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
