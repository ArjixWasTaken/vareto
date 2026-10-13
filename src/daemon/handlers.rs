//! Request dispatch.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{session, Backend, State};
use crate::config::{ConfigStore, Connection, ConnectionConfig, Kind, Route, Source};
use crate::ipc::{ExportFormat, QuerySpec, Request, SaveConnection};
use crate::readonly::{self, SqlDialect};
use crate::render::{ExportWriter, Output, Sink};

/// Hard cap on rows returned to the model in one response.
const MAX_ROW_LIMIT: usize = 5_000;

pub async fn handle(
    state: &State,
    req: Request,
    client: u64,
    force_readonly: bool,
    fd: Option<std::os::fd::OwnedFd>,
    peer: Option<std::os::fd::OwnedFd>,
) -> Result<Value> {
    match req {
        Request::Status => status(state).await,
        Request::ListConnections => list_connections(state, force_readonly).await,
        Request::TunnelList => tunnel_list(state).await,
        Request::TunnelOpen { name } => {
            let t = state.tunnel(&name, true).await?;
            Ok(serde_json::to_value(t.status().await)?)
        }
        Request::TunnelClose { name } => {
            state.close_tunnel(&name).await?;
            Ok(json!({ "closed": name }))
        }
        Request::SaveConnection(s) => save_connection(state, s).await,
        Request::DeleteSavedConnection { name } => delete_saved(state, &name).await,
        Request::Query { connection, query, limit } => {
            let cfg = state.config()?;
            let limit = limit.unwrap_or(cfg.defaults.row_limit).clamp(1, MAX_ROW_LIMIT);
            let out = run_query(state, &connection, &query, limit, force_readonly).await?;
            Ok(out.to_json(cfg.defaults.max_cell_chars))
        }
        Request::Export { connection, query, path, format } => export(state, &connection, &query, &path, format, force_readonly).await,
        Request::ListDatabases { connection } => {
            let (conn, live, t) = setup(state, &connection, force_readonly).await?;
            let out = match &live.backend {
                Backend::Sql(p) => p.list_databases(t).await?,
                Backend::Mongo(m) => m.list_databases().await?,
                Backend::Redis(r) => r.list_databases(t).await?,
            };
            let _ = conn;
            Ok(out.to_json(10_000))
        }
        Request::ListTables { connection, database, pattern } => {
            let (_, live, t) = setup(state, &connection, force_readonly).await?;
            let out = match &live.backend {
                Backend::Sql(p) => p.list_tables(database.as_deref(), pattern.as_deref(), t).await?,
                Backend::Mongo(m) => m.list_collections(database.as_deref(), pattern.as_deref()).await?,
                Backend::Redis(r) => r.list_keys(pattern.as_deref(), t).await?,
            };
            Ok(out.to_json(10_000))
        }
        Request::DescribeTable { connection, database, table } => {
            let (_, live, t) = setup(state, &connection, force_readonly).await?;
            match &live.backend {
                Backend::Sql(p) => p.describe(database.as_deref(), &table, t).await,
                Backend::Mongo(m) => m.describe(database.as_deref(), &table).await,
                Backend::Redis(r) => r.describe(&table, t).await,
            }
        }
        Request::SessionOpen { connection, database } => session::open(state, client, &connection, database, force_readonly).await,
        Request::SessionQuery { session, query, limit, warnings } => {
            let limit = limit.unwrap_or(state.config()?.defaults.row_limit).clamp(1, MAX_ROW_LIMIT);
            session::query(state, client, session, session::Op::Query { query, limit, warnings }).await
        }
        Request::SessionCopyIn { session, sql, framed } => {
            let fd = fd.context("SessionCopyIn needs a file descriptor")?;
            session::query(state, client, session, session::Op::CopyIn { sql, fd, framed, peer }).await
        }
        Request::SessionStream { session, sql, format, warnings } => {
            let fd = fd.context("SessionStream needs a file descriptor")?;
            session::query(state, client, session, session::Op::Stream { sql, format, fd, warnings }).await
        }
        Request::SessionCopyOut { session, sql } => {
            let fd = fd.context("SessionCopyOut needs a file descriptor")?;
            session::query(state, client, session, session::Op::CopyOut { sql, fd }).await
        }
        Request::SessionDescribe { session, sql } => session::query(state, client, session, session::Op::Describe { sql }).await,
        Request::SessionReset { session } => session::reset(state, client, session).await,
        Request::SessionUse { session, database } => session::use_db(state, client, session, &database).await,
        Request::SessionClose { session } => session::close(state, Some(client), session).await,
        Request::SessionCancel { session } => session::cancel(state, session).await,
        Request::Hello { .. } | Request::Shutdown => unreachable!("handled by the connection loop"),
    }
}

pub(super) async fn setup(state: &State, name: &str, force_readonly: bool) -> Result<(Connection, std::sync::Arc<super::Live>, Duration)> {
    let cfg = state.config()?;
    let mut conn = cfg.connection(name)?.clone();
    conn.readonly |= force_readonly;
    let live = state.live(&conn).await?;
    Ok((conn, live, cfg.defaults.query_timeout))
}

async fn run_query(state: &State, name: &str, q: &QuerySpec, limit: usize, force_readonly: bool) -> Result<Output> {
    let (conn, live, timeout) = {
        let cfg = state.config()?;
        let mut conn = cfg.connection(name)?.clone();
        conn.readonly |= force_readonly;
        // Validate before connecting, so rejected queries never open a tunnel.
        check(&conn, q, false)?;
        let live = state.live(&conn).await?;
        (conn, live, cfg.defaults.query_timeout)
    };
    let mut out = match (&live.backend, q) {
        (Backend::Sql(p), QuerySpec::Sql { sql }) => {
            let mut out = p.run(sql, limit, conn.readonly, timeout).await?;
            if let Some(t) = readonly::single_table(dialect(conn.cfg.kind), sql) {
                out.set_table(&t);
            }
            out
        }
        (Backend::Mongo(m), QuerySpec::Mongo(mq)) => m.run(mq, limit, timeout).await?,
        (Backend::Redis(r), QuerySpec::Redis { args }) => r.command(args, timeout).await?,
        _ => unreachable!("checked above"),
    };
    out.mask(&live.masker);
    Ok(out)
}

pub(super) fn dialect(kind: Kind) -> SqlDialect {
    match kind {
        Kind::Postgres => SqlDialect::Postgres,
        Kind::Sqlite => SqlDialect::Sqlite,
        _ => SqlDialect::Mysql,
    }
}

/// Kind/query compatibility and read-only guards. `session`: an interactive
/// session's dedicated connection, which may also change harmless session state.
pub(super) fn check(conn: &Connection, q: &QuerySpec, session: bool) -> Result<()> {
    let kind = conn.cfg.kind;
    match q {
        QuerySpec::Sql { sql } => {
            if !matches!(kind, Kind::Mysql | Kind::Postgres | Kind::Sqlite) {
                bail!("connection {:?} is {}, not SQL", conn.name, kind.as_str());
            }
            if conn.readonly && !(session && readonly::session_statement(dialect(kind), sql)) {
                readonly::check_sql(dialect(kind), sql)?;
            }
        }
        QuerySpec::Mongo(mq) => {
            if kind != Kind::Mongodb {
                bail!("connection {:?} is {}, not MongoDB", conn.name, kind.as_str());
            }
            if conn.readonly {
                readonly::check_mongo(mq)?;
            }
        }
        QuerySpec::Redis { args } => {
            if kind != Kind::Redis {
                bail!("connection {:?} is {}, not Redis", conn.name, kind.as_str());
            }
            if session {
                readonly::check_redis_session(args, conn.readonly)?;
            } else {
                readonly::check_redis(args, conn.readonly)?;
            }
        }
    }
    Ok(())
}

/// Stream a query's full result to a file. No row cap; bounded by `export_timeout`.
async fn export(state: &State, name: &str, q: &QuerySpec, path: &str, format: ExportFormat, force_readonly: bool) -> Result<Value> {
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        bail!("export path must be absolute");
    }
    let cfg = state.config()?;
    let timeout = cfg.defaults.export_timeout;
    let mut conn = cfg.connection(name)?.clone();
    conn.readonly |= force_readonly;
    check(&conn, q, false)?;
    let live = state.live(&conn).await?;

    let started = std::time::Instant::now();
    let (table, collection) = match q {
        QuerySpec::Sql { sql } => (readonly::single_table(dialect(conn.cfg.kind), sql), None),
        QuerySpec::Mongo(mq) => (None, Some(mq.collection.clone())),
        QuerySpec::Redis { .. } => (None, None),
    };
    let mut w = ExportWriter::create(&path, format, &live.masker, table, collection)?;
    let res = match (&live.backend, q) {
        (Backend::Sql(p), QuerySpec::Sql { sql }) => p.stream(sql, &mut w, conn.readonly, timeout).await,
        (Backend::Mongo(m), QuerySpec::Mongo(mq)) => m.stream(mq, None, &mut w, timeout).await,
        (Backend::Redis(r), QuerySpec::Redis { args }) => match r.command(args, timeout).await {
            Ok(Output::Value(v)) => w.value(v),
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        },
        _ => unreachable!("checked above"),
    };
    let summary = res.and_then(|()| w.finish());
    match summary {
        Ok(mut v) => {
            v["path"] = json!(path);
            v["elapsed_secs"] = json!((started.elapsed().as_secs_f64() * 10.0).round() / 10.0);
            Ok(v)
        }
        Err(e) => {
            // Don't leave a partial file that looks like a complete export.
            std::fs::remove_file(&path).ok();
            Err(e.context(format!("export to {} failed (export_timeout is {}s)", path.display(), timeout.as_secs())))
        }
    }
}

async fn status(state: &State) -> Result<Value> {
    let conns: Vec<String> = state.conns.lock().await.keys().cloned().collect();
    let tunnels = tunnel_list(state).await?;
    let config = state.config().map(|_| "ok".to_string()).unwrap_or_else(|e| format!("{e:#}"));
    let (main, saved) = ConfigStore::paths();
    Ok(json!({
        "version": crate::ipc::VERSION,
        "pid": std::process::id(),
        "uptime_secs": state.started.elapsed().as_secs(),
        "config": config,
        "config_files": [main, saved],
        "tunnels": tunnels,
        "ssh_sessions": state.ssh.len(),
        "shell_sessions": state.sessions.lock().await.len(),
        "connected": conns,
    }))
}

async fn tunnel_list(state: &State) -> Result<Value> {
    let cfg = state.config()?;
    let open = state.tunnels.lock().await.clone();
    let mut out = Vec::new();
    for (name, t) in &cfg.forwards {
        match open.get(name) {
            Some(live) => out.push(serde_json::to_value(live.status().await)?),
            None => out.push(json!({ "name": name, "ssh": t.host, "remote": t.remote, "open": false })),
        }
    }
    Ok(Value::Array(out))
}

async fn list_connections(state: &State, force_readonly: bool) -> Result<Value> {
    let cfg = state.config()?;
    let live = state.conns.lock().await;
    let list: Vec<Value> = cfg
        .connections
        .values()
        .map(|c| {
            let route = match c.route() {
                Route::Direct { host, port } => json!({ "direct": crate::config::join_host_port(&host, port) }),
                Route::Tunnel(t) => json!({ "tunnel": t, "ssh": cfg.forwards.get(&t).map(|t| t.host.clone()) }),
                Route::Socket(p) => json!({ "socket": p }),
                Route::File(p) => json!({ "file": p }),
            };
            let mut v = json!({
                "name": c.name,
                "kind": c.cfg.kind,
                "readonly": c.readonly || force_readonly,
                "database": c.cfg.database,
                "route": route,
                "connected": live.contains_key(&c.name),
                "source": c.source,
            });
            if let Some(d) = &c.cfg.description {
                v["description"] = json!(d);
            }
            if !c.cfg.mask.is_empty() || !cfg.defaults.mask.is_empty() {
                v["masked"] = json!(true);
            }
            v
        })
        .collect();
    Ok(Value::Array(list))
}

fn valid_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

async fn save_connection(state: &State, s: SaveConnection) -> Result<Value> {
    if !valid_name(&s.name) {
        bail!("connection name must be 1-64 chars of [A-Za-z0-9._-]");
    }
    let cfg = state.config()?;
    if cfg.connections.contains_key(&s.name) {
        bail!("connection {:?} already exists", s.name);
    }
    if let Some(t) = &s.tunnel {
        // Only tunnels from config.toml: saved connections must survive on their own.
        match cfg.tunnels.get(t) {
            Some(tc) if !t.starts_with('@') => {
                if tc.remote.is_none() && s.host.is_none() && s.port.is_none() {
                    bail!("tunnel {t:?} is an SSH hop without a fixed remote: set `host` (and `port`) to the database address as seen from that SSH server");
                }
            }
            _ => bail!("unknown tunnel {t:?}; use `ssh` to tunnel through a host directly"),
        }
    }
    if s.tunnel.is_some() && s.ssh.is_some() {
        bail!("set either `tunnel` or `ssh`, not both");
    }
    let has_password = s.password.is_some();
    let cc = ConnectionConfig {
        kind: s.kind,
        tunnel: s.tunnel,
        ssh: s.ssh,
        host: s.host,
        port: s.port,
        socket: s.socket,
        path: s.path,
        user: s.user,
        password: None,
        password_cmd: None,
        keyring: has_password,
        database: s.database,
        readonly: Some(true),
        params: s.params,
        description: s.description,
        mask: Default::default(),
    };
    if let Some(pw) = s.password {
        let name = s.name.clone();
        tokio::task::spawn_blocking(move || crate::secrets::keyring_set(&name, &pw))
            .await?
            .context("storing the password in the OS keyring")?;
    }
    let name = s.name.clone();
    tokio::task::spawn_blocking(move || write_saved(|doc| {
        let rendered = toml::to_string(&cc)?;
        let table: toml_edit::DocumentMut = rendered.parse()?;
        let conns = doc.entry("connections").or_insert(toml_edit::table());
        let conns = conns.as_table_mut().context("`connections` in saved.toml is not a table")?;
        conns.set_implicit(true);
        conns.insert(&name, toml_edit::Item::Table(table.as_table().clone()));
        Ok(())
    }))
    .await??;
    // Validate the result parses together with config.toml.
    state.config().context("saved connection made the config invalid")?;
    Ok(json!({ "saved": s.name, "readonly": true, "password_in_keyring": has_password }))
}

async fn delete_saved(state: &State, name: &str) -> Result<Value> {
    let cfg = state.config()?;
    match cfg.connections.get(name) {
        Some(c) if c.source == Source::Saved => {}
        Some(_) => bail!("{name:?} is defined in config.toml; only connections saved via save_connection can be deleted"),
        None => bail!("unknown connection {name:?}"),
    }
    let n = name.to_string();
    tokio::task::spawn_blocking(move || {
        write_saved(|doc| {
            if let Some(t) = doc.get_mut("connections").and_then(|c| c.as_table_mut()) {
                t.remove(&n);
            }
            Ok(())
        })?;
        crate::secrets::keyring_delete(&n).ok();
        anyhow::Ok(())
    })
    .await??;
    state.drop_conn(name).await;
    Ok(json!({ "deleted": name }))
}

fn write_saved(edit: impl FnOnce(&mut toml_edit::DocumentMut) -> Result<()>) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let path = crate::paths::saved_file();
    let src = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            "# Managed by vareto (connections saved by the assistant). Always read-only.\n".into()
        }
        Err(e) => return Err(e.into()),
    };
    let mut doc: toml_edit::DocumentMut = src.parse().context("parsing saved.toml")?;
    edit(&mut doc)?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("toml.tmp");
    let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).mode(0o600).open(&tmp)?;
    f.write_all(doc.to_string().as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}
