mod client;
mod config;
mod daemon;
mod db;
mod ipc;
mod mask;
mod mcp;
mod paths;
mod readonly;
mod render;
mod secrets;
mod ssh;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;

use crate::client::Daemon;
use crate::ipc::{MongoQuery, QuerySpec, Request};

/// vareto (βαρετό, "boring"): SSH-tunnelling database MCP server.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Force every connection to read-only for this client.
    #[arg(long, global = true, env = "VARETO_READONLY")]
    readonly: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the MCP server on stdio (what MCP clients launch).
    Mcp,
    /// Run the daemon in the foreground (normally auto-started).
    Daemon,
    /// Show daemon status.
    Status,
    /// Stop the daemon, closing all tunnels.
    Stop,
    /// List connections and tunnels.
    #[command(alias = "ls")]
    List,
    /// Open a tunnel and keep it open until `close`.
    Open { tunnel: String },
    /// Close a tunnel.
    Close { tunnel: String },
    /// Run SQL on a connection and print JSON.
    Sql { connection: String, sql: String, #[arg(long)] limit: Option<usize> },
    /// Run a MongoDB operation given as JSON (fields of the mongo_query tool).
    Mongo { connection: String, query: String, #[arg(long)] limit: Option<usize> },
    /// Run a Redis command.
    Redis { connection: String, #[arg(trailing_var_arg = true, required = true)] args: Vec<String> },
    /// Config file helpers.
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Manage passwords in the OS keyring (for connections with `keyring = true`).
    Secret {
        #[command(subcommand)]
        cmd: SecretCmd,
    },
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Print config file locations.
    Path,
    /// Validate the config files.
    Check,
    /// Write an example config.toml if none exists.
    Init,
}

#[derive(Subcommand)]
enum SecretCmd {
    /// Store a password (read from the terminal, or stdin when piped).
    Set { connection: String },
    /// Remove a stored password.
    Delete { connection: String },
}

const EXAMPLE_CONFIG: &str = include_str!("../config.example.toml");

fn init_tracing(default: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("VARETO_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).with_ansi(false).init();
}

fn print(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap());
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let daemon = Daemon { force_readonly: cli.readonly };
    match cli.cmd {
        Cmd::Daemon => {
            init_tracing("info");
            daemon::run().await
        }
        Cmd::Mcp => {
            init_tracing("warn");
            mcp::serve(cli.readonly).await
        }
        cmd => {
            init_tracing("warn");
            run_cli(&daemon, cmd).await
        }
    }
}

async fn run_cli(daemon: &Daemon, cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Status => match client::connect_existing().await {
            Some(mut c) => print(&c.call(&Request::Status).await?),
            None => println!("daemon not running"),
        },
        Cmd::Stop => match client::connect_existing().await {
            Some(mut c) => {
                c.call(&Request::Shutdown).await?;
                println!("daemon stopped");
            }
            None => println!("daemon not running"),
        },
        Cmd::List => {
            let conns = daemon.call(Request::ListConnections).await?;
            let tunnels = daemon.call(Request::TunnelList).await?;
            println!("CONNECTIONS");
            for c in conns.as_array().into_iter().flatten() {
                let route = &c["route"];
                let via = route["direct"].as_str().map(str::to_string).unwrap_or_else(|| {
                    format!("tunnel {} ({})", route["tunnel"].as_str().unwrap_or("?"), route["ssh"].as_str().unwrap_or("?"))
                });
                println!(
                    "  {:<20} {:<9} {:<3} {} {}",
                    c["name"].as_str().unwrap_or(""),
                    c["kind"].as_str().unwrap_or(""),
                    if c["readonly"].as_bool() == Some(true) { "ro" } else { "rw" },
                    via,
                    if c["connected"].as_bool() == Some(true) { "[connected]" } else { "" }
                );
            }
            println!("TUNNELS");
            for t in tunnels.as_array().into_iter().flatten() {
                let state = if t.get("local").is_some() {
                    format!("open on {} ({} conns)", t["local"].as_str().unwrap_or(""), t["active_connections"])
                } else {
                    "closed".into()
                };
                println!(
                    "  {:<20} {} -> {}  {}",
                    t["name"].as_str().unwrap_or(""),
                    t["ssh"].as_str().unwrap_or(""),
                    t["remote"].as_str().unwrap_or(""),
                    state
                );
            }
        }
        Cmd::Open { tunnel } => {
            let t = daemon.call(Request::TunnelOpen { name: tunnel }).await?;
            println!("{} open: {} -> {} via {}", t["name"].as_str().unwrap_or(""), t["local"].as_str().unwrap_or(""), t["remote"].as_str().unwrap_or(""), t["ssh"].as_str().unwrap_or(""));
        }
        Cmd::Close { tunnel } => {
            daemon.call(Request::TunnelClose { name: tunnel.clone() }).await?;
            println!("{tunnel} closed");
        }
        Cmd::Sql { connection, sql, limit } => {
            print(&daemon.call(Request::Query { connection, query: QuerySpec::Sql { sql }, limit }).await?)
        }
        Cmd::Mongo { connection, query, limit } => {
            let q: MongoQuery = serde_json::from_str(&query).context("parsing mongo query JSON")?;
            print(&daemon.call(Request::Query { connection, query: QuerySpec::Mongo(q), limit }).await?)
        }
        Cmd::Redis { connection, args } => {
            print(&daemon.call(Request::Query { connection, query: QuerySpec::Redis { args }, limit: None }).await?)
        }
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Path => {
                println!("config: {}", paths::config_file().display());
                println!("saved:  {}", paths::saved_file().display());
                println!("socket: {}", paths::socket_path().display());
                println!("log:    {}", paths::log_file().display());
            }
            ConfigCmd::Check => {
                let r = config::resolve(
                    config::load_file(&paths::config_file())?,
                    config::load_file(&paths::saved_file())?,
                    false,
                )?;
                for c in r.connections.values() {
                    mask::Masker::new(&[&r.defaults.mask, &c.cfg.mask])
                        .with_context(|| format!("connection {:?}: invalid mask rules", c.name))?;
                }
                println!("ok: {} connections, {} tunnels", r.connections.len(), r.tunnels.len());
            }
            ConfigCmd::Init => {
                let p = paths::config_file();
                if p.exists() {
                    bail!("{} already exists", p.display());
                }
                std::fs::create_dir_all(p.parent().unwrap())?;
                std::fs::write(&p, EXAMPLE_CONFIG)?;
                println!("wrote {}", p.display());
            }
        },
        Cmd::Secret { cmd } => match cmd {
            SecretCmd::Set { connection } => {
                let pw = if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
                    rpassword::prompt_password(format!("password for {connection}: "))?
                } else {
                    let mut s = String::new();
                    std::io::stdin().read_line(&mut s)?;
                    s.trim_end_matches(['\r', '\n']).to_string()
                };
                secrets::keyring_set(&connection, &pw)?;
                println!("stored; set `keyring = true` on connection {connection:?}");
            }
            SecretCmd::Delete { connection } => {
                secrets::keyring_delete(&connection)?;
                println!("deleted");
            }
        },
        Cmd::Mcp | Cmd::Daemon => unreachable!(),
    }
    Ok(())
}
