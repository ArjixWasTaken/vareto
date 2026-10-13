//! Local port forwards over an SSH session, with transparent reconnect.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::pool::SessionPool;
use super::Target;
use crate::config::TunnelConfig;

pub struct Tunnel {
    pub name: String,
    pub cfg: TunnelConfig,
    pub target: Target,
    pub local: SocketAddr,
    /// Opened explicitly (CLI / tool) rather than lazily: exempt from idle close.
    pub pinned: bool,
    pool: Arc<SessionPool>,
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
    /// Make sure the (possibly shared) SSH session is up and start listening locally.
    pub async fn open(name: &str, cfg: &TunnelConfig, pinned: bool, pool: Arc<SessionPool>) -> Result<Arc<Tunnel>> {
        let remote = cfg.remote.clone().with_context(|| format!("tunnel {name:?} has no remote"))?;
        let target = super::resolve_tunnel(cfg);
        pool.get(&target).await?;
        let bind = cfg.local.clone().unwrap_or_else(|| "127.0.0.1:0".into());
        let listener = TcpListener::bind(&bind).await.with_context(|| format!("binding {bind}"))?;
        let local = listener.local_addr()?;
        let tunnel = Arc::new(Tunnel {
            name: name.to_string(),
            cfg: cfg.clone(),
            target,
            local,
            pinned,
            pool,
            last_used: Mutex::new(Instant::now()),
            active: AtomicUsize::new(0),
            last_error: Mutex::new(None),
            accept_task: Mutex::new(None),
        });
        let task = tokio::spawn(accept_loop(Arc::downgrade(&tunnel), listener));
        *tunnel.accept_task.lock().unwrap() = Some(task);
        tracing::info!(tunnel = name, %local, %remote, "tunnel open");
        Ok(tunnel)
    }

    pub fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    /// Time since the tunnel was last used (accepts and daemon-side queries touch it).
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_used.lock().unwrap().elapsed()
    }

    /// A forwarding error from the last few seconds, to explain driver-level failures.
    pub fn recent_error(&self) -> Option<String> {
        let g = self.last_error.lock().unwrap();
        g.as_ref().filter(|(at, _)| at.elapsed().as_secs() < 30).map(|(_, e)| e.clone())
    }

    /// Destination address (always set: checked in `open`).
    pub fn remote(&self) -> &str {
        self.cfg.remote.as_deref().unwrap_or_default()
    }

    pub async fn is_connected(&self) -> bool {
        self.pool.is_connected(&self.target).await
    }

    pub async fn status(&self) -> TunnelStatus {
        TunnelStatus {
            name: self.name.clone(),
            ssh: format!("{}@{}:{}", self.target.user, self.target.hostname, self.target.port),
            remote: self.remote().to_string(),
            local: self.local.to_string(),
            connected: self.is_connected().await,
            pinned: self.pinned,
            active_connections: self.active.load(Ordering::Relaxed),
            idle_secs: self.last_used.lock().unwrap().elapsed().as_secs(),
        }
    }

    /// Stop listening. The SSH session is shared; the daemon releases it from the
    /// pool once no open tunnel uses it.
    pub fn close(&self) {
        if let Some(t) = self.accept_task.lock().unwrap().take() {
            t.abort();
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
    let session = t.pool.get(&t.target).await?;
    let mut remote = match session.open_stream(t.remote()).await {
        Ok(s) => s,
        // A stale session can look alive until a channel open fails; retry once fresh.
        Err(_) if session.is_closed() => t.pool.get(&t.target).await?.open_stream(t.remote()).await?,
        Err(e) => return Err(e).with_context(|| format!("opening channel to {}", t.remote())),
    };
    sock.set_nodelay(true).ok();
    tokio::io::copy_bidirectional(&mut sock, &mut remote).await.ok();
    Ok(())
}
