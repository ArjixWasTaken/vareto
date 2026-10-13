//! Interactive sessions (`vareto shell`): one dedicated connection per shell, so
//! session state (USE, SET, transactions, Redis SELECT) carries over between
//! statements.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use super::handlers::{check, dialect, setup};
use super::{Backend, Live, State};
use crate::config::{Connection, Kind};
use crate::db::sql::SqlConn;
use crate::ipc::QuerySpec;
use crate::readonly;
use crate::render::{Collector, Output};

pub struct Session {
    pub id: u64,
    /// The client socket that opened it; the session closes with that socket.
    pub owner: u64,
    name: String,
    force_readonly: bool,
    inner: AsyncMutex<Inner>,
    /// What `SessionCancel` needs while `inner` is locked by the running query.
    cancel: Mutex<Cancel>,
}

#[derive(Default)]
struct Cancel {
    abort: Option<tokio::task::AbortHandle>,
    /// The pool to send `pg_cancel_backend` / `KILL QUERY` through, and the backend id.
    backend: Option<(Arc<Live>, i64)>,
}

struct Inner {
    live: Arc<Live>,
    pinned: Pinned,
    database: Option<String>,
}

enum Pinned {
    Sql(SqlConn),
    Mongo,
    Redis(redis::aio::MultiplexedConnection),
    /// Lost mid-query (cancelled or timed out); reconnected on next use.
    Broken,
}

impl Session {
    pub fn tunnel(&self) -> Option<String> {
        self.inner.try_lock().ok().and_then(|i| i.live.tunnel.clone())
    }

    fn set_backend(&self, live: &Arc<Live>, id: Option<i64>) {
        self.cancel.lock().unwrap().backend = id.map(|id| (live.clone(), id));
    }
}

fn mysql_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Pin a connection on `live`, switched to `database` when given. Returns the
/// connection, its current database and its server-side id (for cancelling).
async fn pin(live: &Live, kind: Kind, cfg_db: Option<&str>, database: Option<&str>, timeout: Duration) -> Result<(Pinned, Option<String>, Option<i64>)> {
    Ok(match &live.backend {
        Backend::Sql(p) => {
            let mut c = match (kind, database) {
                (Kind::Postgres, Some(db)) if Some(db) != cfg_db => p.connect_database(db, timeout).await?,
                _ => p.detach(timeout).await?,
            };
            if let (Kind::Mysql, Some(db)) = (kind, database) {
                c.run(&format!("USE {}", mysql_ident(db)), 1, false).await?;
            }
            let (db, id) = match kind {
                Kind::Postgres => (c.scalar("SELECT current_database()").await?, c.scalar("SELECT pg_backend_pid()").await?),
                Kind::Mysql => (c.scalar("SELECT DATABASE()").await?, c.scalar("SELECT CONNECTION_ID()").await?),
                _ => (Value::from("main"), Value::Null),
            };
            (Pinned::Sql(c), db.as_str().map(str::to_string), id.as_i64())
        }
        Backend::Mongo(_) => (Pinned::Mongo, database.or(cfg_db).map(str::to_string), None),
        Backend::Redis(r) => {
            let mut con = r.session().await?;
            let db = database.or(cfg_db).unwrap_or("0").to_string();
            if db != "0" {
                crate::db::redis::session_command(&mut con, &["SELECT".into(), db.clone()], timeout).await?;
            }
            (Pinned::Redis(con), Some(db), None)
        }
    })
}

async fn unpin(p: Pinned) {
    if let Pinned::Sql(c) = p {
        c.close().await;
    }
}

async fn server_version(live: &Live, pinned: &mut Pinned, timeout: Duration) -> Option<String> {
    match (pinned, &live.backend) {
        (Pinned::Sql(c), Backend::Sql(crate::db::sql::SqlPool::Sqlite(_))) => {
            c.scalar("SELECT sqlite_version()").await.ok()?.as_str().map(str::to_string)
        }
        (Pinned::Sql(c), _) => c.scalar("SELECT version()").await.ok()?.as_str().map(str::to_string),
        (Pinned::Mongo, Backend::Mongo(m)) => m.server_version().await.ok(),
        (Pinned::Redis(con), _) => {
            let (info, _) = crate::db::redis::session_command(con, &["INFO".into(), "server".into()], timeout).await.ok()?;
            info.as_str()?.lines().find_map(|l| l.strip_prefix("redis_version:")).map(|v| v.trim().to_string())
        }
        _ => None,
    }
}

pub async fn open(state: &State, owner: u64, connection: &str, database: Option<String>, force_readonly: bool) -> Result<Value> {
    let (conn, live, timeout) = setup(state, connection, force_readonly).await?;
    let kind = conn.cfg.kind;
    let (mut pinned, database, backend_id) = pin(&live, kind, conn.cfg.database.as_deref(), database.as_deref(), timeout).await?;
    let version = server_version(&live, &mut pinned, timeout).await;
    let id = state.next_session.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let s = Arc::new(Session {
        id,
        owner,
        name: conn.name.clone(),
        force_readonly,
        inner: AsyncMutex::new(Inner { live: live.clone(), pinned, database: database.clone() }),
        cancel: Mutex::new(Cancel::default()),
    });
    s.set_backend(&live, backend_id);
    state.sessions.lock().await.insert(id, s);
    Ok(json!({
        "session": id,
        "connection": conn.name,
        "kind": kind,
        "readonly": conn.readonly,
        "database": database,
        "server_version": version,
        "user": conn.cfg.user,
        "host": conn.route().describe(),
    }))
}

async fn get(state: &State, id: u64, owner: Option<u64>) -> Result<Arc<Session>> {
    let s = state.sessions.lock().await.get(&id).cloned().with_context(|| format!("unknown session {id}"))?;
    if owner.is_some_and(|o| o != s.owner) {
        bail!("session {id} belongs to another client");
    }
    Ok(s)
}

/// Re-pin when the backend was reconnected (config change, tunnel reopened) or the
/// pinned connection was lost. Returns true when it did, i.e. session state is gone.
async fn refresh(s: &Session, inner: &mut Inner, conn: &Connection, live: Arc<Live>, timeout: Duration) -> Result<bool> {
    if Arc::ptr_eq(&live, &inner.live) && !matches!(inner.pinned, Pinned::Broken) {
        return Ok(false);
    }
    let old = std::mem::replace(&mut inner.pinned, Pinned::Broken);
    unpin(old).await;
    let (pinned, db, id) = pin(&live, conn.cfg.kind, conn.cfg.database.as_deref(), inner.database.as_deref(), timeout).await?;
    inner.pinned = pinned;
    inner.database = db;
    inner.live = live.clone();
    s.set_backend(&live, id);
    Ok(true)
}

pub async fn query(state: &State, owner: u64, id: u64, q: QuerySpec, limit: usize) -> Result<Value> {
    let s = get(state, id, Some(owner)).await?;
    let (conn, live, timeout) = setup(state, &s.name, s.force_readonly).await?;
    check(&conn, &q, true)?;
    let task = {
        let s = s.clone();
        tokio::spawn(async move {
            let mut inner = s.inner.lock().await;
            let reconnected = refresh(&s, &mut inner, &conn, live, timeout).await?;
            let mut v = execute(&mut inner, &conn, &q, limit, timeout).await?;
            if reconnected {
                v["reconnected"] = json!(true);
            }
            Ok(v)
        })
    };
    s.cancel.lock().unwrap().abort = Some(task.abort_handle());
    let res = task.await;
    s.cancel.lock().unwrap().abort = None;
    match res {
        Ok(r) => r,
        Err(e) if e.is_cancelled() => {
            let mut inner = s.inner.lock().await;
            if matches!(inner.pinned, Pinned::Sql(_)) {
                // The connection was dropped mid-protocol; reconnect next time.
                let old = std::mem::replace(&mut inner.pinned, Pinned::Broken);
                unpin(old).await;
            }
            bail!("query cancelled")
        }
        Err(e) => Err(e.into()),
    }
}

async fn execute(inner: &mut Inner, conn: &Connection, q: &QuerySpec, limit: usize, timeout: Duration) -> Result<Value> {
    let started = Instant::now();
    let kind = conn.cfg.kind;
    let mut extra = serde_json::Map::new();
    let mut out = match (&mut inner.pinned, &inner.live.backend, q) {
        (Pinned::Sql(c), _, QuerySpec::Sql { sql }) => {
            let d = dialect(kind);
            let wrap = conn.readonly && !readonly::session_statement(d, sql);
            let mut col = Collector::new(limit, None);
            match tokio::time::timeout(timeout + Duration::from_secs(5), c.stream(sql, &mut col, wrap)).await {
                Ok(r) => r?,
                Err(_) => {
                    let old = std::mem::replace(&mut inner.pinned, Pinned::Broken);
                    unpin(old).await;
                    bail!("query timed out after {}s; the session was reset", timeout.as_secs());
                }
            }
            let mut out = col.finish();
            if let Output::Table { columns, .. } = &mut out {
                if let Some(t) = readonly::single_table(d, sql) {
                    columns.iter_mut().for_each(|c| c.table = Some(t.clone()));
                }
            }
            if kind == Kind::Mysql && sql.trim_start().get(..3).is_some_and(|w| w.eq_ignore_ascii_case("use")) {
                inner.database = c.scalar("SELECT DATABASE()").await?.as_str().map(str::to_string);
            }
            extra.insert("in_transaction".into(), json!(c.in_transaction()));
            out
        }
        (Pinned::Mongo, Backend::Mongo(m), QuerySpec::Mongo(mq)) => {
            let mut mq = mq.clone();
            if mq.database.is_none() {
                mq.database = inner.database.clone();
            }
            m.run(&mq, limit, timeout).await?
        }
        (Pinned::Redis(con), _, QuerySpec::Redis { args }) => {
            let (v, kinds) = crate::db::redis::session_command(con, args, timeout).await?;
            if args[0].eq_ignore_ascii_case("select") {
                inner.database = args.get(1).cloned();
            }
            extra.insert("kinds".into(), kinds);
            Output::Value(v)
        }
        _ => bail!("this session's connection cannot run that query"),
    };
    out.mask(&inner.live.masker);
    let mut v = out.to_json(usize::MAX);
    v["elapsed_ms"] = json!(started.elapsed().as_millis() as u64);
    v["database"] = json!(inner.database);
    for (k, x) in extra {
        v[k] = x;
    }
    Ok(v)
}

pub async fn use_db(state: &State, owner: u64, id: u64, database: &str) -> Result<Value> {
    let s = get(state, id, Some(owner)).await?;
    let (conn, live, timeout) = setup(state, &s.name, s.force_readonly).await?;
    let mut inner = s.inner.lock().await;
    refresh(&s, &mut inner, &conn, live, timeout).await?;
    match conn.cfg.kind {
        Kind::Postgres => {
            let (pinned, db, backend) = pin(&inner.live, Kind::Postgres, None, Some(database), timeout).await?;
            let old = std::mem::replace(&mut inner.pinned, pinned);
            unpin(old).await;
            inner.database = db;
            s.set_backend(&inner.live.clone(), backend);
        }
        Kind::Mysql => {
            let Pinned::Sql(c) = &mut inner.pinned else { unreachable!() };
            c.run(&format!("USE {}", mysql_ident(database)), 1, false).await?;
            inner.database = c.scalar("SELECT DATABASE()").await?.as_str().map(str::to_string);
        }
        Kind::Mongodb => inner.database = Some(database.to_string()),
        Kind::Sqlite => bail!("a SQLite connection is one database file"),
        Kind::Redis => {
            let Pinned::Redis(con) = &mut inner.pinned else { unreachable!() };
            database.parse::<u32>().with_context(|| format!("redis database must be a number, got {database:?}"))?;
            crate::db::redis::session_command(con, &["SELECT".into(), database.into()], timeout).await?;
            inner.database = Some(database.to_string());
        }
    }
    Ok(json!({ "database": inner.database }))
}

/// Cancel the running query. Postgres and MySQL cancel it server-side and keep the
/// session; other backends drop it (SQLite then reconnects).
pub async fn cancel(state: &State, id: u64) -> Result<Value> {
    let s = get(state, id, None).await?;
    let (abort, backend) = {
        let c = s.cancel.lock().unwrap();
        (c.abort.clone(), c.backend.clone())
    };
    if abort.is_none() {
        return Ok(json!({ "cancelled": false }));
    }
    if let Some((live, pid)) = backend {
        if let Backend::Sql(p) = &live.backend {
            let sql = match p {
                crate::db::sql::SqlPool::Postgres(_) => format!("SELECT pg_cancel_backend({pid})"),
                crate::db::sql::SqlPool::Mysql(_) => format!("KILL QUERY {pid}"),
                crate::db::sql::SqlPool::Sqlite(_) => unreachable!("no backend id"),
            };
            if p.run(&sql, 1, false, Duration::from_secs(10)).await.is_ok() {
                return Ok(json!({ "cancelled": true }));
            }
        }
    }
    if let Some(a) = abort {
        a.abort();
    }
    Ok(json!({ "cancelled": true }))
}

pub async fn close(state: &State, owner: Option<u64>, id: u64) -> Result<Value> {
    get(state, id, owner).await?;
    let s = state.sessions.lock().await.remove(&id);
    if let Some(s) = s {
        shutdown(s).await;
    }
    Ok(json!({ "closed": id }))
}

/// Close every session a client socket opened.
pub async fn close_owned(state: &State, owner: u64) {
    let gone: Vec<Arc<Session>> = {
        let mut map = state.sessions.lock().await;
        let ids: Vec<u64> = map.values().filter(|s| s.owner == owner).map(|s| s.id).collect();
        ids.iter().filter_map(|id| map.remove(id)).collect()
    };
    for s in gone {
        shutdown(s).await;
    }
}

async fn shutdown(s: Arc<Session>) {
    if let Some(a) = s.cancel.lock().unwrap().abort.take() {
        a.abort();
    }
    let mut inner = s.inner.lock().await;
    let old = std::mem::replace(&mut inner.pinned, Pinned::Broken);
    unpin(old).await;
}
