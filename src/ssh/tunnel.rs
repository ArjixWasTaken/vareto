//! Local port forwards over an SSH session, with transparent reconnect.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use super::{connect, Session, Target};
use crate::config::TunnelConfig;

pub struct Tunnel {
    pub name: String,
    pub cfg: TunnelConfig,
    pub target: Target,
    pub local: SocketAddr,
    /// Opened explicitly (CLI / tool) rather than lazily: exempt from idle close.
    pub pinned: bool,
    session: AsyncMutex<Option<Arc<Session>>>,
    last_used: Mutex<Instant>,
    active: AtomicUsize,
    last_error: Mutex<Option<(Instant, String)>>,
    accept_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Serialize)]
pub struct TunnelStatus {
    pub name: String,
    pub ssh: String,
    pub remote: String,
    pub local: String,
    pub connected: bool,
    pub pinned: bool,
    pub active_connections: usize,
    pub idle_secs: u64,
}

impl Tunnel {
    /// Connect the SSH session and start listening locally.
    pub async fn open(name: &str, cfg: &TunnelConfig, pinned: bool) -> Result<Arc<Tunnel>> {
        let target = super::resolve_tunnel(cfg);
        let session = connect(&target).await?;
        let bind = cfg.local.clone().unwrap_or_else(|| "127.0.0.1:0".into());
        let listener = TcpListener::bind(&bind).await.with_context(|| format!("binding {bind}"))?;
        let local = listener.local_addr()?;
        let tunnel = Arc::new(Tunnel {
            name: name.to_string(),
            cfg: cfg.clone(),
            target,
            local,
            pinned,
            session: AsyncMutex::new(Some(Arc::new(session))),
            last_used: Mutex::new(Instant::now()),
            active: AtomicUsize::new(0),
            last_error: Mutex::new(None),
            accept_task: Mutex::new(None),
        });
        let task = tokio::spawn(accept_loop(Arc::downgrade(&tunnel), listener));
        *tunnel.accept_task.lock().unwrap() = Some(task);
        tracing::info!(tunnel = name, %local, remote = %cfg.remote, "tunnel open");
        Ok(tunnel)
    }

    pub fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    /// Time since the tunnel was last used (accepts and daemon-side queries touch it).
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_used.lock().unwrap().elapsed()
    }

    /// Current session, reconnecting if it dropped.
    async fn session(&self) -> Result<Arc<Session>> {
        let mut guard = self.session.lock().await;
        if let Some(s) = guard.as_ref() {
            if !s.is_closed() {
                return Ok(s.clone());
            }
            tracing::warn!(tunnel = %self.name, "ssh session dropped; reconnecting");
        }
        let mut delay = std::time::Duration::from_millis(250);
        let mut last_err = None;
        for _ in 0..4 {
            match connect(&self.target).await {
                Ok(s) => {
                    let s = Arc::new(s);
                    *guard = Some(s.clone());
                    return Ok(s);
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
        *guard = None;
        Err(last_err.unwrap())
    }

    /// A forwarding error from the last few seconds, to explain driver-level failures.
    pub fn recent_error(&self) -> Option<String> {
        let g = self.last_error.lock().unwrap();
        g.as_ref().filter(|(at, _)| at.elapsed().as_secs() < 30).map(|(_, e)| e.clone())
    }

    pub async fn is_connected(&self) -> bool {
        self.session.lock().await.as_ref().is_some_and(|s| !s.is_closed())
    }

    pub async fn status(&self) -> TunnelStatus {
        TunnelStatus {
            name: self.name.clone(),
            ssh: format!("{}@{}:{}", self.target.user, self.target.hostname, self.target.port),
            remote: self.cfg.remote.clone(),
            local: self.local.to_string(),
            connected: self.is_connected().await,
            pinned: self.pinned,
            active_connections: self.active.load(Ordering::Relaxed),
            idle_secs: self.last_used.lock().unwrap().elapsed().as_secs(),
        }
    }

    pub async fn close(&self) {
        if let Some(t) = self.accept_task.lock().unwrap().take() {
            t.abort();
        }
        if let Some(s) = self.session.lock().await.take() {
            s.close().await;
        }
        tracing::info!(tunnel = %self.name, "tunnel closed");
    }
}

async fn accept_loop(tunnel: std::sync::Weak<Tunnel>, listener: TcpListener) {
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let Some(t) = tunnel.upgrade() else { return };
        tokio::spawn(async move {
            t.touch();
            t.active.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = forward(&t, sock).await {
                tracing::warn!(tunnel = %t.name, %peer, "forward failed: {e:#}");
                *t.last_error.lock().unwrap() = Some((Instant::now(), format!("{e:#}")));
            }
            t.active.fetch_sub(1, Ordering::Relaxed);
            t.touch();
        });
    }
}

async fn forward(t: &Tunnel, mut sock: TcpStream) -> Result<()> {
    let session = t.session().await?;
    let mut remote = match session.open_stream(&t.cfg.remote).await {
        Ok(s) => s,
        // A stale session can look alive until a channel open fails; retry once fresh.
        Err(_) if session.is_closed() => t.session().await?.open_stream(&t.cfg.remote).await?,
        Err(e) => return Err(e).with_context(|| format!("opening channel to {}", t.cfg.remote)),
    };
    sock.set_nodelay(true).ok();
    tokio::io::copy_bidirectional(&mut sock, &mut remote).await.ok();
    Ok(())
}
