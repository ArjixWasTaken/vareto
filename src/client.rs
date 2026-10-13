//! Connecting to the daemon, spawning it (detached) when it isn't running.

use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::ipc::{Client, Request, PROTOCOL, VERSION};
use crate::paths;

pub struct Daemon {
    pub force_readonly: bool,
}

impl Daemon {
    /// Open a greeted connection, starting or upgrading the daemon as needed.
    pub async fn connect(&self) -> Result<Client> {
        let sock = paths::socket_path();
        let mut client = match Client::connect(&sock).await {
            Ok(c) => c,
            Err(_) => {
                spawn()?;
                wait_for_socket().await?
            }
        };
        let hello = client.call(&self.hello()).await?;
        // A daemon from before `protocol` existed doesn't report one.
        if hello["compatible"].as_bool() != Some(true) || hello["protocol"].as_u64() != Some(PROTOCOL.into()) {
            tracing::info!("daemon version {} (protocol {}) != {VERSION} (protocol {PROTOCOL}); restarting it", hello["version"], hello["protocol"]);
            client.call(&Request::Shutdown).await.ok();
            for _ in 0..50 {
                if Client::connect(&sock).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            spawn()?;
            client = wait_for_socket().await?;
            client.call(&self.hello()).await?;
        }
        Ok(client)
    }

    fn hello(&self) -> Request {
        Request::Hello { version: VERSION.into(), force_readonly: self.force_readonly }
    }

    pub async fn call(&self, req: Request) -> Result<Value> {
        self.connect().await?.call(&req).await
    }
}

/// Connect to a running daemon without starting one.
pub async fn connect_existing() -> Option<Client> {
    let mut c = Client::connect(&paths::socket_path()).await.ok()?;
    c.call(&Request::Hello { version: VERSION.into(), force_readonly: false }).await.ok()?;
    Some(c)
}

fn spawn() -> Result<()> {
    paths::ensure_private_dir(&paths::runtime_dir())?;
    paths::ensure_private_dir(&paths::state_dir())?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(paths::log_file())
        .context("opening daemon log")?;
    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("daemon")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .current_dir("/");
    // SAFETY: setsid is async-signal-safe; detaches from our session/terminal.
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid().map_err(std::io::Error::from)?;
            Ok(())
        });
    }
    let mut child = cmd.spawn().context("spawning daemon")?;
    // Reap it if it exits while we're still alive.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}

async fn wait_for_socket() -> Result<Client> {
    let sock = paths::socket_path();
    for _ in 0..100 {
        if let Ok(c) = Client::connect(&sock).await {
            return Ok(c);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("daemon did not start; see {}", paths::log_file().display())
}
