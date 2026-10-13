//! SSH sessions shared between tunnels: every tunnel whose destination resolves to
//! the same target (host, port, user, keys, jump hosts) multiplexes its channels
//! over one authenticated session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::Mutex as AsyncMutex;

use super::{connect, Session, Target};

#[derive(Default)]
pub struct SessionPool {
    slots: Mutex<HashMap<Target, Arc<AsyncMutex<Option<Arc<Session>>>>>>,
}

impl SessionPool {
    fn slot(&self, target: &Target) -> Arc<AsyncMutex<Option<Arc<Session>>>> {
        self.slots.lock().unwrap().entry(target.clone()).or_default().clone()
    }

    /// The live session for `target`, connecting (or reconnecting, with backoff)
    /// if needed. Concurrent callers for the same target wait on one attempt.
    pub async fn get(&self, target: &Target) -> Result<Arc<Session>> {
        let slot = self.slot(target);
        let mut guard = slot.lock().await;
        if let Some(s) = guard.as_ref() {
            if !s.is_closed() {
                return Ok(s.clone());
            }
            tracing::warn!(host = %target.hostname, "ssh session dropped; reconnecting");
        }
        let mut delay = std::time::Duration::from_millis(250);
        let mut last_err = None;
        for attempt in 0..4 {
            match connect(target).await {
                Ok(s) => {
                    tracing::info!(host = %target.hostname, user = %target.user, "ssh session established");
                    let s = Arc::new(s);
                    *guard = Some(s.clone());
                    return Ok(s);
                }
                // Host key / auth problems won't fix themselves; only retry the first time.
                Err(e) if attempt == 0 && guard.is_none() => {
                    *guard = None;
                    return Err(e);
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

    pub async fn is_connected(&self, target: &Target) -> bool {
        let slot = self.slots.lock().unwrap().get(target).cloned();
        match slot {
            Some(slot) => slot.lock().await.as_ref().is_some_and(|s| !s.is_closed()),
            None => false,
        }
    }

    /// Disconnect and forget the session for `target`.
    pub async fn release(&self, target: &Target) {
        let slot = self.slots.lock().unwrap().remove(target);
        if let Some(slot) = slot {
            if let Some(s) = slot.lock().await.take() {
                s.close().await;
                tracing::info!(host = %target.hostname, "ssh session closed");
            }
        }
    }

    /// Number of distinct live SSH sessions (for status output).
    pub fn len(&self) -> usize {
        self.slots.lock().unwrap().len()
    }
}
