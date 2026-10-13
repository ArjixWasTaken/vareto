//! The shell's side of a daemon session: blocking calls on the shell thread.

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::runtime::Handle;

use crate::client::Daemon;
use crate::config::Kind;
use crate::ipc::{Client, QuerySpec, Request};

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
    pub in_transaction: bool,
    /// Row cap per statement (`--limit`); the daemon's default when unset.
    pub limit: Option<usize>,
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
            in_transaction: false,
            limit,
        })
    }

    /// A request on the session's socket. Ctrl-C while it runs asks the daemon
    /// (over a second socket) to cancel the session's query.
    pub fn call(&mut self, req: Request) -> Result<Value> {
        let (id, ro) = (self.id, self.force_readonly);
        let client = &mut self.client;
        self.rt.block_on(async move {
            let call = client.call(&req);
            tokio::pin!(call);
            loop {
                tokio::select! {
                    r = &mut call => return r,
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("^C");
                        let d = Daemon { force_readonly: ro };
                        if let Ok(mut c) = d.connect().await {
                            c.call(&Request::SessionCancel { session: id }).await.ok();
                        }
                    }
                }
            }
        })
    }

    pub fn query(&mut self, query: QuerySpec) -> Result<Value> {
        self.query_limit(query, self.limit)
    }

    pub fn query_limit(&mut self, query: QuerySpec, limit: Option<usize>) -> Result<Value> {
        let v = self.call(Request::SessionQuery { session: self.id, query, limit })?;
        if v["reconnected"].as_bool() == Some(true) {
            eprintln!("(the connection was re-established; session state such as transactions or SET was reset)");
        }
        if let Some(db) = v.get("database") {
            self.database = db.as_str().map(str::to_string);
        }
        self.in_transaction = v["in_transaction"].as_bool() == Some(true);
        Ok(v)
    }

    pub fn sql(&mut self, sql: &str) -> Result<Value> {
        self.query(QuerySpec::Sql { sql: sql.into() })
    }

    pub fn use_db(&mut self, database: &str) -> Result<()> {
        let v = self.call(Request::SessionUse { session: self.id, database: database.into() })?;
        self.database = v["database"].as_str().map(str::to_string);
        self.in_transaction = false;
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
