//! SSH sessions: `~/.ssh/config` resolution, host key TOFU, agent/key auth, ProxyJump.

pub mod tunnel;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, PublicKeyOrCertificate};

use crate::config::TunnelConfig;
use crate::paths;

/// A fully resolved SSH destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub hostname: String,
    pub port: u16,
    pub user: String,
    pub identity_files: Vec<PathBuf>,
    /// Jump hosts, outermost first (each is itself resolved through ssh config).
    pub proxy_jump: Vec<String>,
}

/// Split `[user@]host[:port]`.
fn split_spec(spec: &str) -> (Option<String>, String, Option<u16>) {
    let (user, rest) = match spec.rsplit_once('@') {
        Some((u, r)) => (Some(u.to_string()), r),
        None => (None, spec),
    };
    if let Some(inner) = rest.strip_prefix('[') {
        if let Some((h, p)) = inner.split_once("]:") {
            return (user, h.to_string(), p.parse().ok());
        }
        return (user, inner.trim_end_matches(']').to_string(), None);
    }
    match rest.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => match p.parse() {
            Ok(port) => (user, h.to_string(), Some(port)),
            Err(_) => (user, rest.to_string(), None),
        },
        _ => (user, rest.to_string(), None),
    }
}

fn load_ssh_config() -> Option<ssh2_config::SshConfig> {
    let path = paths::ssh_dir().join("config");
    let file = std::fs::File::open(&path).ok()?;
    let mut reader = std::io::BufReader::new(file);
    let rules = ssh2_config::ParseRule::ALLOW_UNKNOWN_FIELDS | ssh2_config::ParseRule::ALLOW_UNSUPPORTED_FIELDS;
    match ssh2_config::SshConfig::default().parse(&mut reader, rules) {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!("ignoring unparsable {}: {e}", path.display());
            None
        }
    }
}

fn default_user() -> String {
    std::env::var("USER")
        .ok()
        .or_else(|| nix::unistd::User::from_uid(nix::unistd::getuid()).ok().flatten().map(|u| u.name))
        .unwrap_or_else(|| "root".into())
}

/// Resolve a host spec plus optional explicit overrides into a [`Target`].
pub fn resolve(spec: &str, user: Option<&str>, port: Option<u16>, identity: Option<&str>) -> Target {
    let (spec_user, alias, spec_port) = split_spec(spec);
    let params = load_ssh_config().map(|c| c.query(&alias));
    let p = params.as_ref();

    let mut identity_files: Vec<PathBuf> = identity.map(paths::expand_tilde).into_iter().collect();
    identity_files.extend(p.and_then(|p| p.identity_file.clone()).unwrap_or_default());
    if identity_files.is_empty() {
        for name in ["id_ed25519", "id_ecdsa", "id_rsa"] {
            let f = paths::ssh_dir().join(name);
            if f.exists() {
                identity_files.push(f);
            }
        }
    }

    Target {
        hostname: p.and_then(|p| p.host_name.clone()).unwrap_or(alias),
        port: port.or(spec_port).or(p.and_then(|p| p.port)).unwrap_or(22),
        user: user
            .map(str::to_string)
            .or(spec_user)
            .or(p.and_then(|p| p.user.clone()))
            .unwrap_or_else(default_user),
        identity_files,
        proxy_jump: p
            .and_then(|p| p.proxy_jump.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|j| !j.eq_ignore_ascii_case("none"))
            .collect(),
    }
}

pub fn resolve_tunnel(t: &TunnelConfig) -> Target {
    resolve(&t.host, t.user.as_deref(), t.port, t.identity.as_deref())
}

pub struct Handler {
    host: String,
    port: u16,
    known_hosts: PathBuf,
}

impl client::Handler for Handler {
    type Error = russh::Error;

    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        let pk = key.public_key();
        match russh::keys::check_known_hosts_path(&self.host, self.port, &pk, &self.known_hosts) {
            Ok(true) => Ok(true),
            Ok(false) => {
                // Trust on first use.
                tracing::info!(host = %self.host, port = self.port, "learning new host key ({})", pk.algorithm());
                russh::keys::known_hosts::learn_known_hosts_path(&self.host, self.port, &pk, &self.known_hosts)?;
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// An authenticated SSH session, keeping its jump-host sessions alive.
pub struct Session {
    pub handle: client::Handle<Handler>,
    _jumps: Vec<client::Handle<Handler>>,
}

impl Session {
    pub fn is_closed(&self) -> bool {
        self.handle.is_closed()
    }

    pub async fn open_stream(&self, remote: &str) -> Result<russh::ChannelStream<client::Msg>> {
        let ch = if remote.starts_with('/') {
            self.handle.channel_open_direct_streamlocal(remote).await?
        } else {
            let (host, port) = crate::config::split_host_port(remote)?;
            self.handle.channel_open_direct_tcpip(host, port as u32, "127.0.0.1", 0).await?
        };
        Ok(ch.into_stream())
    }

    pub async fn close(&self) {
        let _ = self.handle.disconnect(russh::Disconnect::ByApplication, "", "en").await;
        for j in &self._jumps {
            let _ = j.disconnect(russh::Disconnect::ByApplication, "", "en").await;
        }
    }
}

fn client_config() -> Arc<client::Config> {
    Arc::new(client::Config {
        keepalive_interval: Some(Duration::from_secs(15)),
        keepalive_max: 3,
        nodelay: true,
        ..Default::default()
    })
}

fn friendly_connect_error(t: &Target, e: russh::Error) -> anyhow::Error {
    match e {
        russh::Error::Keys(russh::keys::Error::KeyChanged { line }) => anyhow::anyhow!(
            "HOST KEY MISMATCH for {}:{} (known_hosts line {line}). Refusing to connect; \
             if the server key legitimately changed, remove the old entry with `ssh-keygen -R {}`",
            t.hostname,
            t.port,
            t.hostname
        ),
        e => anyhow::anyhow!("ssh connection to {}@{}:{} failed: {e}", t.user, t.hostname, t.port),
    }
}

/// Connect and authenticate, going through ProxyJump hosts if configured.
pub async fn connect(target: &Target) -> Result<Session> {
    let mut jumps: Vec<client::Handle<Handler>> = Vec::new();
    // Chain: each jump is reached through the previous one.
    let hops: Vec<Target> = target
        .proxy_jump
        .iter()
        .flat_map(|j| j.split(','))
        .map(|j| resolve(j.trim(), None, None, None))
        .chain(std::iter::once(target.clone()))
        .collect();

    let known_hosts = paths::ssh_dir().join("known_hosts");
    let mut final_handle = None;
    for (i, hop) in hops.iter().enumerate() {
        let handler = Handler { host: hop.hostname.clone(), port: hop.port, known_hosts: known_hosts.clone() };
        let mut handle = match jumps.last() {
            None => tokio::time::timeout(
                Duration::from_secs(20),
                client::connect(client_config(), (hop.hostname.as_str(), hop.port), handler),
            )
            .await
            .with_context(|| format!("timed out connecting to {}:{}", hop.hostname, hop.port))?
            .map_err(|e| friendly_connect_error(hop, e))?,
            Some(prev) => {
                let ch = prev
                    .channel_open_direct_tcpip(hop.hostname.clone(), hop.port as u32, "127.0.0.1", 0)
                    .await
                    .with_context(|| format!("jump host could not reach {}:{}", hop.hostname, hop.port))?;
                client::connect_stream(client_config(), ch.into_stream(), handler)
                    .await
                    .map_err(|e| friendly_connect_error(hop, e))?
            }
        };
        authenticate(&mut handle, hop).await?;
        if i + 1 == hops.len() {
            final_handle = Some(handle);
        } else {
            jumps.push(handle);
        }
    }
    Ok(Session { handle: final_handle.expect("at least one hop"), _jumps: jumps })
}

async fn authenticate(handle: &mut client::Handle<Handler>, t: &Target) -> Result<()> {
    let mut tried = Vec::new();

    if std::env::var_os("SSH_AUTH_SOCK").is_some() {
        match russh::keys::agent::client::AgentClient::connect_env().await {
            Ok(mut agent) => {
                let ids = agent.request_identities().await.unwrap_or_default();
                for id in ids {
                    let pk = id.public_key().into_owned();
                    let hash = handle.best_supported_rsa_hash().await?.flatten();
                    match handle.authenticate_publickey_with(&t.user, pk, hash, &mut agent).await {
                        Ok(r) if r.success() => return Ok(()),
                        Ok(_) => {}
                        Err(e) => tracing::debug!("agent auth error: {e:?}"),
                    }
                }
                tried.push("ssh-agent".to_string());
            }
            Err(e) => tracing::debug!("ssh-agent unavailable: {e}"),
        }
    }

    for path in &t.identity_files {
        match load_key(path) {
            Ok(key) => {
                let hash = handle.best_supported_rsa_hash().await?.flatten();
                let r = handle.authenticate_publickey(&t.user, PrivateKeyWithHashAlg::new(Arc::new(key), hash)).await?;
                if r.success() {
                    return Ok(());
                }
                tried.push(path.display().to_string());
            }
            Err(e) => tried.push(format!("{} ({e})", path.display())),
        }
    }

    bail!(
        "ssh authentication failed for {}@{}:{}; tried: {}. Encrypted keys must be loaded into ssh-agent.",
        t.user,
        t.hostname,
        t.port,
        if tried.is_empty() { "nothing (no agent, no identity files)".into() } else { tried.join(", ") }
    )
}

fn load_key(path: &Path) -> Result<russh::keys::PrivateKey> {
    russh::keys::load_secret_key(path, None).map_err(|e| match e {
        russh::keys::Error::KeyIsEncrypted => anyhow::anyhow!("encrypted, not in agent"),
        e => e.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::split_spec;

    #[test]
    fn specs() {
        assert_eq!(split_spec("bastion"), (None, "bastion".into(), None));
        assert_eq!(split_spec("me@bastion:2222"), (Some("me".into()), "bastion".into(), Some(2222)));
        assert_eq!(split_spec("[::1]:22"), (None, "::1".into(), Some(22)));
        assert_eq!(split_spec("::1"), (None, "::1".into(), None));
    }
}
