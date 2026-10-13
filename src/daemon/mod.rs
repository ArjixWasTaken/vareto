//! The long-lived daemon: owns tunnels and DB pools, serves the unix socket.

mod handlers;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::net::UnixListener;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::config::{ConfigStore, Connection, Kind, Resolved, Route};
use crate::db::{mongo::Mongo, redis::Redis, sql::SqlPool};
use crate::ipc::{self, Request, Response};
use crate::mask::Masker;
use crate::paths;
use crate::ssh::pool::SessionPool;
use crate::ssh::tunnel::Tunnel;

pub enum Backend {
    Sql(SqlPool),
    Mongo(Mongo),
    Redis(Redis),
}

/// A connected backend plus what it was built from.
pub struct Live {
    pub backend: Backend,
    fingerprint: String,
    pub tunnel: Option<String>,
    pub masker: Masker,
}

pub struct State {
    store: Mutex<ConfigStore>,
    pub tunnels: AsyncMutex<HashMap<String, Arc<Tunnel>>>,
    pub conns: AsyncMutex<HashMap<String, Arc<Live>>>,
    /// SSH sessions shared by all forwards to the same SSH destination.
    pub ssh: Arc<SessionPool>,
    last_activity: Mutex<Instant>,
    pub started: Instant,
    shutdown: Notify,
}

impl State {
    pub fn config(&self) -> Result<Arc<Resolved>> {
        self.store.lock().unwrap().get()
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    /// Get (or lazily open) a tunnel. `pin` marks it as explicitly opened.
    pub async fn tunnel(&self, name: &str, pin: bool) -> Result<Arc<Tunnel>> {
        let cfg = self.config()?;
        let tcfg = cfg.forwards.get(name).cloned().with_context(|| match cfg.tunnels.get(name) {
            Some(_) => format!(
                "tunnel {name:?} has no fixed `remote`; it is an SSH hop for connections (open a forward listed by tunnel_list instead)"
            ),
            None => format!("unknown tunnel {name:?}"),
        })?;
        let mut tunnels = self.tunnels.lock().await;
        if let Some(t) = tunnels.get(name) {
            if t.cfg == tcfg && (!pin || t.pinned) {
                t.touch();
                return Ok(t.clone());
            }
            // Config changed, or a lazy tunnel is being pinned: reopen.
            let old = tunnels.remove(name).unwrap();
            self.drop_conns_for_tunnel(name).await;
            old.close();
            self.release_unused_session(&tunnels, &old.target).await;
        }
        let t = Tunnel::open(name, &tcfg, pin, self.ssh.clone()).await?;
        tunnels.insert(name.to_string(), t.clone());
        Ok(t)
    }

    pub async fn close_tunnel(&self, name: &str) -> Result<()> {
        let mut tunnels = self.tunnels.lock().await;
        let Some(t) = tunnels.remove(name) else { bail!("tunnel {name:?} is not open") };
        self.drop_conns_for_tunnel(name).await;
        t.close();
        self.release_unused_session(&tunnels, &t.target).await;
        Ok(())
    }

    /// Disconnect a shared SSH session once no open forward uses it.
    async fn release_unused_session(&self, open: &HashMap<String, Arc<Tunnel>>, target: &crate::ssh::Target) {
        if !open.values().any(|t| &t.target == target) {
            self.ssh.release(target).await;
        }
    }

    async fn drop_conns_for_tunnel(&self, tunnel: &str) {
        let mut conns = self.conns.lock().await;
        let names: Vec<String> =
            conns.iter().filter(|(_, l)| l.tunnel.as_deref() == Some(tunnel)).map(|(n, _)| n.clone()).collect();
        for n in names {
            if let Some(l) = conns.remove(&n) {
                close_live(&l).await;
            }
        }
    }

    pub async fn drop_conn(&self, name: &str) {
        if let Some(l) = self.conns.lock().await.remove(name) {
            close_live(&l).await;
        }
    }

    /// Get (or lazily connect) a backend for a connection.
    pub async fn live(&self, conn: &Connection) -> Result<Arc<Live>> {
        let cfg = self.config()?;
        let fingerprint = format!("{:?}|{}", conn.cfg, conn.readonly);
        let route = conn.route();
        let tunnel_name = match &route {
            Route::Tunnel(t) => Some(t.clone()),
            Route::Direct { .. } => None,
        };
        let existing = self.conns.lock().await.get(&conn.name).cloned();
        if let Some(l) = existing {
            if l.fingerprint == fingerprint {
                if let Some(t) = &tunnel_name {
                    // Make sure the tunnel is (still) up; this also refreshes its idle timer.
                    self.tunnel(t, false).await?;
                }
                return Ok(l.clone());
            }
        }
        self.drop_conn(&conn.name).await;

        let (host, port) = match &route {
            Route::Direct { host, port } => (host.clone(), *port),
            Route::Tunnel(t) => {
                let t = self.tunnel(t, false).await?;
                (t.local.ip().to_string(), t.local.port())
            }
        };
        let c2 = conn.clone();
        let password = tokio::task::spawn_blocking(move || crate::secrets::password(&c2)).await??;
        let backend = self.connect_backend(conn, &host, port, password.as_deref(), tunnel_name.is_some()).await;
        let backend = match (backend, &tunnel_name) {
            (Ok(b), _) => b,
            (Err(e), Some(t)) => {
                let hint = self.tunnels.lock().await.get(t).and_then(|t| t.recent_error());
                return Err(match hint {
                    Some(h) => e.context(format!("tunnel {t:?}: {h}")),
                    None => e,
                });
            }
            (Err(e), None) => return Err(e),
        };
        let masker = Masker::new(&[&cfg.defaults.mask, &conn.cfg.mask])?;
        let live = Arc::new(Live { backend, fingerprint, tunnel: tunnel_name, masker });
        self.conns.lock().await.insert(conn.name.clone(), live.clone());
        Ok(live)
    }

    async fn connect_backend(&self, conn: &Connection, host: &str, port: u16, password: Option<&str>, tunneled: bool) -> Result<Backend> {
        Ok(match conn.cfg.kind {
            Kind::Mysql | Kind::Postgres => {
                let ep = crate::db::sql::Endpoint { host, port, password };
                Backend::Sql(crate::db::sql::connect(conn, &ep).await?)
            }
            Kind::Mongodb => {
                let uri = crate::db::mongo::build_uri(conn, host, port, password, tunneled);
                Backend::Mongo(Mongo::connect(&uri, conn.cfg.database.clone()).await?)
            }
            Kind::Redis => {
                let url = crate::db::redis::build_url(conn, host, port, password)?;
                Backend::Redis(Redis::connect(&url).await?)
            }
        })
    }

    /// Close idle lazy tunnels; returns true when the daemon itself should exit.
    async fn reap(&self) -> bool {
        let Ok(cfg) = self.config() else { return false };
        let idle: Vec<String> = self
            .tunnels
            .lock()
            .await
            .iter()
            .filter(|(_, t)| !t.pinned && t.idle_for() > cfg.defaults.idle_timeout)
            .map(|(n, _)| n.clone())
            .collect();
        for name in idle {
            tracing::info!(tunnel = %name, "closing idle tunnel");
            self.close_tunnel(&name).await.ok();
        }
        let no_tunnels = self.tunnels.lock().await.is_empty();
        no_tunnels && self.last_activity.lock().unwrap().elapsed() > cfg.defaults.daemon_idle_timeout
    }

    async fn close_all(&self) {
        for (_, l) in self.conns.lock().await.drain() {
            close_live(&l).await;
        }
        for (_, t) in self.tunnels.lock().await.drain() {
            t.close();
            self.ssh.release(&t.target).await;
        }
    }
}

async fn close_live(l: &Live) {
    if let Backend::Sql(p) = &l.backend {
        p.close().await;
    }
}

/// Run the daemon in the foreground (the client spawns it detached).
pub async fn run() -> Result<()> {
    let dir = paths::runtime_dir();
    paths::ensure_private_dir(&dir)?;
    paths::ensure_private_dir(&paths::state_dir()).ok();

    // Single instance: hold an exclusive lock for our whole lifetime.
    let lock_file = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(paths::lock_path())?;
    let _lock = match nix::fcntl::Flock::lock(lock_file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
        Ok(l) => l,
        Err(_) => bail!("another vareto daemon is already running"),
    };

    let sock = paths::socket_path();
    let _ = std::fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock).with_context(|| format!("binding {}", sock.display()))?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
    }

    let state = Arc::new(State {
        store: Mutex::new(ConfigStore::new(false)),
        tunnels: Default::default(),
        conns: Default::default(),
        ssh: Default::default(),
        last_activity: Mutex::new(Instant::now()),
        started: Instant::now(),
        shutdown: Notify::new(),
    });
    if let Err(e) = state.config() {
        tracing::error!("config error (will retry on each request): {e:#}");
    }
    tracing::info!(pid = std::process::id(), socket = %sock.display(), "vareto daemon {} started", ipc::VERSION);

    let reaper = {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if state.reap().await {
                    tracing::info!("idle; exiting");
                    state.shutdown.notify_one();
                    return;
                }
            }
        })
    };

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(x) => x,
                    Err(e) => { tracing::warn!("accept: {e}"); continue; }
                };
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_client(state, stream).await {
                        tracing::debug!("client error: {e:#}");
                    }
                });
            }
            _ = state.shutdown.notified() => break,
            _ = sigterm.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    reaper.abort();
    state.close_all().await;
    let _ = std::fs::remove_file(&sock);
    tracing::info!("daemon stopped");
    Ok(())
}

async fn serve_client(state: Arc<State>, stream: tokio::net::UnixStream) -> Result<()> {
    let mut conn = ipc::framed(stream);
    let mut force_readonly = false;
    let mut greeted = false;
    while let Some(req) = ipc::recv::<Request>(&mut conn).await? {
        state.touch();
        let resp = match req {
            Request::Hello { version, force_readonly: f } => {
                greeted = true;
                force_readonly = f;
                Ok(serde_json::json!({ "version": ipc::VERSION, "pid": std::process::id(), "compatible": version == ipc::VERSION }))
            }
            _ if !greeted => Err(anyhow::anyhow!("protocol error: expected hello")),
            Request::Shutdown => {
                ipc::send(&mut conn, &Response::Ok { result: serde_json::json!("stopping") }).await?;
                state.shutdown.notify_one();
                return Ok(());
            }
            req => handlers::handle(&state, req, force_readonly).await,
        };
        let resp = match resp {
            Ok(result) => Response::Ok { result },
            Err(e) => Response::Err { error: format!("{e:#}") },
        };
        ipc::send(&mut conn, &resp).await?;
        state.touch();
    }
    Ok(())
}
