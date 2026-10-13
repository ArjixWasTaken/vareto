//! Daemon protocol: one length-delimited JSON request, one response, per frame.

use std::path::Path;

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const VERSION: &str = env!("VARETO_VERSION");
const MAX_FRAME: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// First frame on every connection. `force_readonly` applies to the rest of it.
    Hello { version: String, #[serde(default)] force_readonly: bool },
    Status,
    Shutdown,
    ListConnections,
    TunnelList,
    TunnelOpen { name: String },
    TunnelClose { name: String },
    SaveConnection(SaveConnection),
    DeleteSavedConnection { name: String },
    Query { connection: String, query: QuerySpec, limit: Option<usize> },
    Export { connection: String, query: QuerySpec, path: String, format: ExportFormat },
    ListDatabases { connection: String },
    ListTables { connection: String, database: Option<String>, pattern: Option<String> },
    DescribeTable { connection: String, database: Option<String>, table: String },
    /// Pin a dedicated connection for an interactive shell. Sessions belong to the
    /// socket that opened them and close with it.
    SessionOpen { connection: String, #[serde(default)] database: Option<String> },
    SessionQuery { session: u64, query: QuerySpec, #[serde(default)] limit: Option<usize> },
    /// Switch the session's database (Postgres reconnects, MySQL `USE`, Mongo's
    /// current db, Redis `SELECT`).
    SessionUse { session: u64, database: String },
    SessionClose { session: u64 },
    /// Cancel the query a session is running. Sent on a second socket, since the
    /// session's own socket is waiting for that query.
    SessionCancel { session: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok { result: Value },
    Err { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SaveConnection {
    /// New connection name (must not already exist).
    pub name: String,
    pub kind: crate::config::Kind,
    /// Name of an existing tunnel (SSH hop) to connect through; `host`/`port` are then
    /// the database address as seen from that SSH server.
    #[serde(default)]
    pub tunnel: Option<String>,
    /// SSH host to tunnel through (alternative to `tunnel`); host/port are then resolved on that server.
    #[serde(default)]
    pub ssh: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub user: Option<String>,
    /// Stored in the OS keyring, never in the config file.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub params: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// What to run. One variant per backend family.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QuerySpec {
    Sql { sql: String },
    Mongo(MongoQuery),
    Redis { args: Vec<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MongoOp {
    Find,
    Aggregate,
    Count,
    Distinct,
    InsertOne,
    InsertMany,
    UpdateOne,
    UpdateMany,
    DeleteOne,
    DeleteMany,
}

impl MongoOp {
    pub fn is_read(self) -> bool {
        matches!(self, MongoOp::Find | MongoOp::Aggregate | MongoOp::Count | MongoOp::Distinct)
    }
}

/// A structured MongoDB operation. JSON values are MongoDB Extended JSON
/// (e.g. `{"$oid": "..."}`, `{"$date": "..."}`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MongoQuery {
    /// Database; defaults to the connection's database.
    #[serde(default)]
    pub database: Option<String>,
    pub collection: String,
    pub op: MongoOp,
    /// Query filter (find, count, distinct, update_*, delete_*).
    #[serde(default)]
    pub filter: Option<Value>,
    /// Projection (find).
    #[serde(default)]
    pub projection: Option<Value>,
    /// Sort (find).
    #[serde(default)]
    pub sort: Option<Value>,
    /// Documents to skip (find).
    #[serde(default)]
    pub skip: Option<u64>,
    /// Aggregation pipeline (aggregate).
    #[serde(default)]
    pub pipeline: Option<Vec<Value>>,
    /// Field name (distinct).
    #[serde(default)]
    pub field: Option<String>,
    /// Documents (insert_one uses the first, insert_many all).
    #[serde(default)]
    pub documents: Option<Vec<Value>>,
    /// Update document or pipeline (update_*).
    #[serde(default)]
    pub update: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Csv,
    Jsonl,
}

pub type Conn = Framed<UnixStream, LengthDelimitedCodec>;

pub fn framed(stream: UnixStream) -> Conn {
    let codec = LengthDelimitedCodec::builder().max_frame_length(MAX_FRAME).new_codec();
    Framed::new(stream, codec)
}

pub async fn send<T: Serialize>(conn: &mut Conn, msg: &T) -> Result<()> {
    conn.send(bytes::Bytes::from(serde_json::to_vec(msg)?)).await?;
    Ok(())
}

pub async fn recv<T: DeserializeOwned>(conn: &mut Conn) -> Result<Option<T>> {
    match conn.next().await {
        None => Ok(None),
        Some(frame) => Ok(Some(serde_json::from_slice(&frame?).context("decoding message")?)),
    }
}

/// A client connection to the daemon.
pub struct Client {
    conn: Conn,
}

impl Client {
    pub async fn connect(path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { conn: framed(stream) })
    }

    pub async fn call(&mut self, req: &Request) -> Result<Value> {
        send(&mut self.conn, req).await?;
        match recv::<Response>(&mut self.conn).await? {
            Some(Response::Ok { result }) => Ok(result),
            Some(Response::Err { error }) => bail!(error),
            None => bail!("daemon closed the connection"),
        }
    }
}
