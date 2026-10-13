//! MCP stdio server. Thin: every tool call is forwarded to the daemon.

use std::sync::Arc;

use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler, ServiceExt};
use serde::Deserialize;

use crate::client::Daemon;
use crate::ipc::{ExportFormat, MongoQuery, QuerySpec, Request, SaveConnection};

const INSTRUCTIONS: &str = "\
vareto gives access to databases (MySQL/MariaDB, PostgreSQL, MongoDB, Redis), opening SSH tunnels \
automatically when a connection needs one. Start with `list_connections`. Explore with \
`list_databases`, `list_tables`, `describe_table`, then query with `sql_query`, `mongo_query` or \
`redis_command` depending on the connection kind. Connections marked readonly reject writes. \
Results are capped (see `limit`); use `export_query` to write large results to a file instead of \
reading them. Values may be masked by configuration.";

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct NameArg {
    /// Tunnel or connection name.
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ConnArg {
    /// Connection name (see list_connections).
    pub connection: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesArgs {
    /// Connection name.
    pub connection: String,
    /// MySQL: database. Postgres: schema (default: all non-system schemas). MongoDB: database. Redis: ignored.
    #[serde(default)]
    pub database: Option<String>,
    /// Name filter. SQL: LIKE pattern (`*` allowed). MongoDB: glob. Redis: SCAN MATCH pattern.
    #[serde(default)]
    pub pattern: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeArgs {
    /// Connection name.
    pub connection: String,
    /// Table (`schema.table` allowed for Postgres), collection, or Redis key.
    pub table: String,
    /// MySQL database / Postgres schema / MongoDB database.
    #[serde(default)]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SqlArgs {
    /// Connection name (a mysql or postgres connection).
    pub connection: String,
    /// One SQL statement (several are allowed only on writable connections).
    pub sql: String,
    /// Max rows to return (default from config, usually 200).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MongoArgs {
    /// Connection name (a mongodb connection).
    pub connection: String,
    #[serde(flatten)]
    pub query: MongoQuery,
    /// Max documents to return (default from config, usually 200).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RedisArgs {
    /// Connection name (a redis connection).
    pub connection: String,
    /// Command and arguments, e.g. ["HGETALL", "user:1"].
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExportArgs {
    /// Connection name.
    pub connection: String,
    /// SQL query (mysql/postgres connections).
    #[serde(default)]
    pub sql: Option<String>,
    /// MongoDB query (mongodb connections).
    #[serde(default)]
    pub mongo: Option<MongoQuery>,
    /// Redis command (redis connections).
    #[serde(default)]
    pub redis: Option<Vec<String>>,
    /// Output file; relative paths are resolved against the MCP server's working directory.
    pub path: String,
    pub format: ExportFormat,
}

#[derive(Clone)]
pub struct Server {
    daemon: Arc<Daemon>,
    tool_router: ToolRouter<Self>,
}

fn ok_json(v: serde_json::Value) -> CallToolResult {
    let text = serde_json::to_string(&v).unwrap_or_else(|e| e.to_string());
    CallToolResult::success(vec![ContentBlock::text(text)])
}

fn err(e: anyhow::Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(format!("{e:#}"))])
}

impl Server {
    async fn forward(&self, req: Request) -> Result<CallToolResult, McpError> {
        Ok(match self.daemon.call(req).await {
            Ok(v) => ok_json(v),
            Err(e) => err(e),
        })
    }
}

#[tool_router]
impl Server {
    pub fn new(force_readonly: bool) -> Self {
        Self { daemon: Arc::new(Daemon { force_readonly }), tool_router: Self::tool_router() }
    }

    #[tool(description = "List configured database connections: kind, readonly flag, route (direct or SSH tunnel), and whether currently connected.")]
    async fn list_connections(&self) -> Result<CallToolResult, McpError> {
        self.forward(Request::ListConnections).await
    }

    #[tool(description = "List SSH tunnels and their state (open, local port, active connections).")]
    async fn tunnel_list(&self) -> Result<CallToolResult, McpError> {
        self.forward(Request::TunnelList).await
    }

    #[tool(description = "Open a configured SSH tunnel and keep it open until closed. Not needed before querying: tunnels open on demand.")]
    async fn tunnel_open(&self, Parameters(a): Parameters<NameArg>) -> Result<CallToolResult, McpError> {
        self.forward(Request::TunnelOpen { name: a.name }).await
    }

    #[tool(description = "Close an open SSH tunnel and the database connections using it.")]
    async fn tunnel_close(&self, Parameters(a): Parameters<NameArg>) -> Result<CallToolResult, McpError> {
        self.forward(Request::TunnelClose { name: a.name }).await
    }

    #[tool(description = "List databases on a connection (Redis: keyspaces).")]
    async fn list_databases(&self, Parameters(a): Parameters<ConnArg>) -> Result<CallToolResult, McpError> {
        self.forward(Request::ListDatabases { connection: a.connection }).await
    }

    #[tool(description = "List tables (SQL), collections (MongoDB) or keys with types (Redis, max 1000).")]
    async fn list_tables(&self, Parameters(a): Parameters<ListTablesArgs>) -> Result<CallToolResult, McpError> {
        self.forward(Request::ListTables { connection: a.connection, database: a.database, pattern: a.pattern }).await
    }

    #[tool(description = "Describe a table (columns, indexes, foreign keys), a MongoDB collection (indexes, count, field types from a sample), or a Redis key (type, TTL, size).")]
    async fn describe_table(&self, Parameters(a): Parameters<DescribeArgs>) -> Result<CallToolResult, McpError> {
        self.forward(Request::DescribeTable { connection: a.connection, database: a.database, table: a.table }).await
    }

    #[tool(description = "Run SQL on a MySQL/MariaDB or PostgreSQL connection. On readonly connections only a single read statement (SELECT/WITH/EXPLAIN/SHOW/DESCRIBE) is allowed and it runs in a READ ONLY transaction.")]
    async fn sql_query(&self, Parameters(a): Parameters<SqlArgs>) -> Result<CallToolResult, McpError> {
        self.forward(Request::Query { connection: a.connection, query: QuerySpec::Sql { sql: a.sql }, limit: a.limit }).await
    }

    #[tool(description = "Run a structured MongoDB operation (find, aggregate, count, distinct; insert/update/delete on writable connections). JSON values are MongoDB Extended JSON, e.g. {\"_id\": {\"$oid\": \"...\"}}.")]
    async fn mongo_query(&self, Parameters(a): Parameters<MongoArgs>) -> Result<CallToolResult, McpError> {
        self.forward(Request::Query { connection: a.connection, query: QuerySpec::Mongo(a.query), limit: a.limit }).await
    }

    #[tool(description = "Run a Redis command. Readonly connections only allow read commands; blocking and connection-state commands (SUBSCRIBE, MONITOR, SELECT, MULTI, BLPOP...) are never allowed.")]
    async fn redis_command(&self, Parameters(a): Parameters<RedisArgs>) -> Result<CallToolResult, McpError> {
        self.forward(Request::Query { connection: a.connection, query: QuerySpec::Redis { args: a.args }, limit: None }).await
    }

    #[tool(description = "Run a query and write the full result (up to 1,000,000 rows) to a CSV or JSONL file instead of returning it. Give exactly one of sql, mongo, redis.")]
    async fn export_query(&self, Parameters(a): Parameters<ExportArgs>) -> Result<CallToolResult, McpError> {
        let query = match (a.sql, a.mongo, a.redis) {
            (Some(sql), None, None) => QuerySpec::Sql { sql },
            (None, Some(m), None) => QuerySpec::Mongo(m),
            (None, None, Some(args)) => QuerySpec::Redis { args },
            _ => return Ok(err(anyhow::anyhow!("give exactly one of `sql`, `mongo`, `redis`"))),
        };
        let path = std::path::Path::new(&a.path);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().map_err(|e| McpError::internal_error(e.to_string(), None))?.join(path)
        };
        self.forward(Request::Export {
            connection: a.connection,
            query,
            path: path.to_string_lossy().into_owned(),
            format: a.format,
        })
        .await
    }

    #[tool(description = "Save a new connection for future use. Saved connections are always read-only; the password (if any) goes to the OS keyring, never to a file. Use `ssh` to tunnel through an SSH host (then host/port are resolved on that server, default 127.0.0.1 and the kind's default port).")]
    async fn save_connection(&self, Parameters(a): Parameters<SaveConnection>) -> Result<CallToolResult, McpError> {
        self.forward(Request::SaveConnection(a)).await
    }

    #[tool(description = "Delete a connection previously created with save_connection (connections from the user's config file cannot be deleted).")]
    async fn delete_saved_connection(&self, Parameters(a): Parameters<NameArg>) -> Result<CallToolResult, McpError> {
        self.forward(Request::DeleteSavedConnection { name: a.name }).await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("vareto", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

pub async fn serve(force_readonly: bool) -> anyhow::Result<()> {
    // Start the daemon early so the first tool call isn't slowed down by it.
    let server = Server::new(force_readonly);
    if let Err(e) = server.daemon.connect().await {
        tracing::warn!("daemon not reachable yet: {e:#}");
    }
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}
