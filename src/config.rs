//! Configuration: the human-edited `config.toml` plus the LLM-managed `saved.toml`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::mask::MaskConfig;
use crate::paths;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub tunnels: BTreeMap<String, TunnelConfig>,
    #[serde(default)]
    pub connections: BTreeMap<String, ConnectionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Defaults {
    /// Max rows/documents returned to the model per query.
    pub row_limit: usize,
    /// Max characters per returned cell before it is truncated.
    pub max_cell_chars: usize,
    /// Close tunnels (and their DB pools) after this long without use.
    #[serde(with = "duration_str")]
    pub idle_timeout: Duration,
    /// The daemon exits after this long with no open tunnels and no requests.
    #[serde(with = "duration_str")]
    pub daemon_idle_timeout: Duration,
    /// Readonly default for connections that don't specify it.
    pub readonly: bool,
    /// Statement timeout applied to queries.
    #[serde(with = "duration_str")]
    pub query_timeout: Duration,
    /// Timeout for `export_query`, which streams whole results to a file.
    #[serde(with = "duration_str")]
    pub export_timeout: Duration,
    /// Masking applied to every connection (merged with per-connection rules).
    pub mask: MaskConfig,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            row_limit: 200,
            max_cell_chars: 2000,
            idle_timeout: Duration::from_secs(15 * 60),
            daemon_idle_timeout: Duration::from_secs(30 * 60),
            readonly: true,
            query_timeout: Duration::from_secs(60),
            export_timeout: Duration::from_secs(10 * 60),
            mask: MaskConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TunnelConfig {
    /// `~/.ssh/config` alias or hostname, optionally `user@host:port`.
    pub host: String,
    /// Optional fixed destination, as seen from the SSH server, e.g. `127.0.0.1:5432`.
    /// Usually omitted: connections name their own `host`/`port`, and every
    /// destination reached through the same SSH host shares one SSH session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    /// Fixed local bind address (only with `remote`); an ephemeral `127.0.0.1` port when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    #[serde(alias = "mariadb")]
    Mysql,
    #[serde(alias = "postgresql", alias = "pg")]
    Postgres,
    #[serde(alias = "mongo")]
    Mongodb,
    Redis,
    /// A local SQLite file (`path`); never tunneled.
    #[serde(alias = "sqlite3")]
    Sqlite,
}

impl Kind {
    pub fn default_port(self) -> u16 {
        match self {
            Kind::Mysql => 3306,
            Kind::Postgres => 5432,
            Kind::Mongodb => 27017,
            Kind::Redis => 6379,
            Kind::Sqlite => 0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Mysql => "mysql",
            Kind::Postgres => "postgres",
            Kind::Mongodb => "mongodb",
            Kind::Redis => "redis",
            Kind::Sqlite => "sqlite",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    pub kind: Kind,
    /// Name of a `[tunnels.*]` entry (SSH hop) to connect through. `host`/`port` are
    /// then the destination as seen from that SSH server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<String>,
    /// Shorthand: SSH host to tunnel through to `host:port` (an implicit tunnel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh: Option<String>,
    /// DB host. With `ssh`, it is resolved on the SSH server (default 127.0.0.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Unix socket of a local server, instead of `host`/`port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<String>,
    /// SQLite database file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Plaintext, or `${env:VAR}` references.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Shell command whose trimmed stdout is the password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_cmd: Option<String>,
    /// Read the password from the OS keyring (service `vareto`, account = connection name).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub keyring: bool,
    /// Database (SQL), default database (Mongo) or db index (Redis).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readonly: Option<bool>,
    /// Extra driver parameters (e.g. `sslmode`, `authSource`, `tls`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "MaskConfig::is_empty")]
    pub mask: MaskConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Config,
    Saved,
}

/// A connection after merging both files.
#[derive(Debug, Clone)]
pub struct Connection {
    pub name: String,
    pub cfg: ConnectionConfig,
    pub source: Source,
    pub readonly: bool,
    /// The forward (key into [`Resolved::forwards`]) this connection goes through.
    pub forward: Option<String>,
}

/// Where a connection's traffic goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    Direct { host: String, port: u16 },
    Tunnel(String),
    /// A local server's unix socket.
    Socket(PathBuf),
    /// A SQLite file.
    File(PathBuf),
}

impl Route {
    /// How `list` shows it.
    pub fn describe(&self) -> String {
        match self {
            Route::Direct { host, port } => join_host_port(host, *port),
            Route::Tunnel(t) => format!("tunnel {t}"),
            Route::Socket(p) => format!("socket {}", p.display()),
            Route::File(p) => format!("file {}", p.display()),
        }
    }
}

impl Connection {
    pub fn route(&self) -> Route {
        if let Some(f) = &self.forward {
            Route::Tunnel(f.clone())
        } else if let Some(p) = &self.cfg.path {
            Route::File(expand_home(p))
        } else if let Some(s) = &self.cfg.socket {
            Route::Socket(expand_home(s))
        } else {
            Route::Direct {
                host: self.cfg.host.clone().unwrap_or_else(|| "127.0.0.1".into()),
                port: self.cfg.port.unwrap_or(self.cfg.kind.default_port()),
            }
        }
    }
}

/// `~` and `~/x` to the home directory.
pub fn expand_home(p: &str) -> PathBuf {
    crate::paths::expand_tilde(p)
}

/// Settings that only make sense for some connection kinds or routes.
fn validate_target(name: &str, cfg: &ConnectionConfig) -> Result<()> {
    let networked = cfg.tunnel.is_some() || cfg.ssh.is_some() || cfg.host.is_some() || cfg.port.is_some();
    if cfg.kind == Kind::Sqlite {
        let Some(path) = &cfg.path else { bail!("connection {name:?}: sqlite needs `path` (the database file)") };
        if networked || cfg.socket.is_some() {
            bail!("connection {name:?}: sqlite opens a local file; `tunnel`, `ssh`, `host`, `port` and `socket` don't apply");
        }
        if cfg.user.is_some() || cfg.password.is_some() || cfg.password_cmd.is_some() || cfg.keyring {
            bail!("connection {name:?}: sqlite has no users or passwords");
        }
        if !expand_home(path).is_absolute() {
            bail!("connection {name:?}: sqlite `path` must be absolute (or start with ~/)");
        }
        return Ok(());
    }
    if cfg.path.is_some() {
        bail!("connection {name:?}: `path` is only for sqlite connections");
    }
    // `port` may stay: Postgres names its socket file after it.
    if cfg.socket.is_some() && (cfg.tunnel.is_some() || cfg.ssh.is_some() || cfg.host.is_some()) {
        bail!("connection {name:?}: `socket` is for local servers; drop `tunnel`, `ssh` and `host`");
    }
    Ok(())
}

pub fn implicit_tunnel_name(conn: &str) -> String {
    format!("@{conn}")
}

/// Fully merged, validated configuration.
#[derive(Debug, Clone, Default)]
pub struct Resolved {
    pub defaults: Defaults,
    /// Tunnels as configured (SSH hops, optionally with a fixed `remote`).
    pub tunnels: BTreeMap<String, TunnelConfig>,
    /// Concrete port forwards (always with `remote`): configured tunnels that have a
    /// `remote`, plus one per distinct (tunnel, destination) that connections use.
    pub forwards: BTreeMap<String, TunnelConfig>,
    pub connections: BTreeMap<String, Connection>,
}

impl Resolved {
    pub fn connection(&self, name: &str) -> Result<&Connection> {
        self.connections.get(name).with_context(|| {
            let known: Vec<_> = self.connections.keys().map(String::as_str).collect();
            format!("unknown connection {name:?} (known: {})", known.join(", "))
        })
    }
}

pub fn load_file(path: &Path) -> Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(s) => toml::from_str(&s).with_context(|| format!("parsing {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Merge `config.toml` and `saved.toml`. `force_readonly` comes from `--readonly`.
pub fn resolve(main: Config, saved: Config, force_readonly: bool) -> Result<Resolved> {
    let mut tunnels = main.tunnels;
    for (name, t) in saved.tunnels {
        if tunnels.contains_key(&name) {
            bail!("tunnel {name:?} is defined in both config.toml and saved.toml");
        }
        tunnels.insert(name, t);
    }

    let mut forwards: BTreeMap<String, TunnelConfig> =
        tunnels.iter().filter(|(_, t)| t.remote.is_some()).map(|(n, t)| (n.clone(), t.clone())).collect();
    for (name, t) in &tunnels {
        if t.local.is_some() && t.remote.is_none() {
            bail!("tunnel {name:?}: `local` needs `remote`");
        }
    }

    let mut connections = BTreeMap::new();
    let all = main
        .connections
        .into_iter()
        .map(|c| (c, Source::Config))
        .chain(saved.connections.into_iter().map(|c| (c, Source::Saved)));
    for ((name, cfg), source) in all {
        if connections.contains_key(&name) {
            bail!("connection {name:?} is defined in both config.toml and saved.toml");
        }
        if cfg.tunnel.is_some() && cfg.ssh.is_some() {
            bail!("connection {name:?}: set either `tunnel` or `ssh`, not both");
        }
        validate_target(&name, &cfg)?;
        // Readonly can only be relaxed by a human, in config.toml.
        let readonly = force_readonly
            || source == Source::Saved
            || cfg.readonly.unwrap_or(main.defaults.readonly);
        let dest = || {
            join_host_port(
                cfg.host.as_deref().unwrap_or("127.0.0.1"),
                cfg.port.unwrap_or(cfg.kind.default_port()),
            )
        };
        let forward = if let Some(tname) = &cfg.tunnel {
            let t = tunnels
                .get(tname)
                .with_context(|| format!("connection {name:?} references unknown tunnel {tname:?}"))?;
            if cfg.host.is_some() || cfg.port.is_some() {
                // Destination from the connection, reached through the tunnel's SSH host.
                let remote = dest();
                let key = format!("{tname}/{remote}");
                forwards.entry(key.clone()).or_insert_with(|| TunnelConfig {
                    remote: Some(remote),
                    local: None,
                    ..t.clone()
                });
                Some(key)
            } else if t.remote.is_some() {
                Some(tname.clone())
            } else {
                bail!("connection {name:?}: tunnel {tname:?} has no `remote`, so set `host`/`port` on the connection");
            }
        } else if let Some(ssh) = &cfg.ssh {
            let key = implicit_tunnel_name(&name);
            forwards.insert(
                key.clone(),
                TunnelConfig { host: ssh.clone(), remote: Some(dest()), local: None, user: None, port: None, identity: None },
            );
            Some(key)
        } else {
            None
        };
        connections.insert(name.clone(), Connection { name, cfg, source, readonly, forward });
    }

    Ok(Resolved { defaults: main.defaults, tunnels, forwards, connections })
}

pub fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Split `host:port` / `[v6]:port`.
pub fn split_host_port(s: &str) -> Result<(String, u16)> {
    let (host, port) = s.rsplit_once(':').with_context(|| format!("{s:?} is not host:port"))?;
    let port = port.parse().with_context(|| format!("invalid port in {s:?}"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Ok((host.to_string(), port))
}

/// Loads config files and reloads them when their mtime changes.
pub struct ConfigStore {
    force_readonly: bool,
    stamp: Option<(Option<SystemTime>, Option<SystemTime>)>,
    current: std::sync::Arc<Resolved>,
}

fn mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

impl ConfigStore {
    pub fn new(force_readonly: bool) -> Self {
        Self { force_readonly, stamp: None, current: Default::default() }
    }

    pub fn paths() -> (PathBuf, PathBuf) {
        (paths::config_file(), paths::saved_file())
    }

    /// Current config, re-read from disk if either file changed.
    pub fn get(&mut self) -> Result<std::sync::Arc<Resolved>> {
        let (main_p, saved_p) = Self::paths();
        let stamp = (mtime(&main_p), mtime(&saved_p));
        if self.stamp != Some(stamp) {
            let resolved = resolve(load_file(&main_p)?, load_file(&saved_p)?, self.force_readonly)?;
            self.current = std::sync::Arc::new(resolved);
            self.stamp = Some(stamp);
        }
        Ok(self.current.clone())
    }
}

/// Resolve `${env:VAR}` references inside a string.
pub fn interpolate_env(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${env:") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 6..];
        let end = after.find('}').with_context(|| format!("unterminated ${{env:...}} in {s:?}"))?;
        let var = &after[..end];
        let val = std::env::var(var).with_context(|| format!("environment variable {var} is not set"))?;
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Parse durations like `90s`, `15m`, `1h`, `1h30m`, or a bare number of seconds.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u64>() {
        return Ok(Duration::from_secs(n));
    }
    let mut total = 0u64;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: u64 = num.parse().with_context(|| format!("invalid duration {s:?}"))?;
        num.clear();
        total += n * match c {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            _ => bail!("invalid duration unit {c:?} in {s:?}"),
        };
    }
    if !num.is_empty() {
        bail!("invalid duration {s:?} (missing unit)");
    }
    Ok(Duration::from_secs(total))
}

mod duration_str {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Secs(u64),
            Str(String),
        }
        match Raw::deserialize(d)? {
            Raw::Secs(n) => Ok(Duration::from_secs(n)),
            Raw::Str(s) => super::parse_duration(&s).map_err(serde::de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Config {
        toml::from_str(s).unwrap()
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("1h30").is_err());
    }

    #[test]
    fn env_interpolation() {
        // SAFETY: tests in this module don't read this variable concurrently.
        unsafe { std::env::set_var("VARETO_TEST_PW", "s3cret") };
        assert_eq!(interpolate_env("${env:VARETO_TEST_PW}").unwrap(), "s3cret");
        assert_eq!(interpolate_env("a-${env:VARETO_TEST_PW}-b").unwrap(), "a-s3cret-b");
        assert_eq!(interpolate_env("plain").unwrap(), "plain");
        assert!(interpolate_env("${env:VARETO_DEFINITELY_UNSET}").is_err());
    }

    #[test]
    fn readonly_rules() {
        let main = parse(
            r#"
            [defaults]
            readonly = false
            idle_timeout = "5m"
            [connections.rw]
            kind = "postgres"
            [connections.ro]
            kind = "mysql"
            readonly = true
            "#,
        );
        let saved = parse(
            r#"
            [connections.llm]
            kind = "redis"
            readonly = false
            "#,
        );
        let r = resolve(main.clone(), saved.clone(), false).unwrap();
        assert!(!r.connections["rw"].readonly);
        assert!(r.connections["ro"].readonly);
        assert!(r.connections["llm"].readonly, "saved connections are always readonly");
        assert_eq!(r.defaults.idle_timeout, Duration::from_secs(300));

        let forced = resolve(main, saved, true).unwrap();
        assert!(forced.connections.values().all(|c| c.readonly));
    }

    #[test]
    fn local_targets() {
        let main = parse(
            r#"
            [connections.lite]
            kind = "sqlite"
            path = "/data/app.db"
            [connections.sock]
            kind = "postgres"
            socket = "/run/postgresql"
            [connections.tcp]
            kind = "mysql"
            "#,
        );
        let r = resolve(main, Config::default(), false).unwrap();
        assert_eq!(r.connections["lite"].route(), Route::File("/data/app.db".into()));
        assert_eq!(r.connections["sock"].route(), Route::Socket("/run/postgresql".into()));
        assert_eq!(r.connections["tcp"].route(), Route::Direct { host: "127.0.0.1".into(), port: 3306 });

        for bad in [
            "kind = \"sqlite\"",
            "kind = \"sqlite\"\npath = \"rel.db\"",
            "kind = \"sqlite\"\npath = \"/a.db\"\nssh = \"h\"",
            "kind = \"sqlite\"\npath = \"/a.db\"\npassword = \"x\"",
            "kind = \"postgres\"\npath = \"/a.db\"",
            "kind = \"postgres\"\nsocket = \"/s\"\nssh = \"h\"",
            "kind = \"redis\"\nsocket = \"/s\"\nhost = \"h\"",
        ] {
            let cfg: Config = toml::from_str(&format!("[connections.x]\n{bad}")).unwrap();
            assert!(resolve(cfg, Config::default(), false).is_err(), "should reject: {bad}");
        }
    }

    #[test]
    fn implicit_tunnel() {
        let main = parse(
            r#"
            [connections.prod]
            kind = "postgres"
            ssh = "bastion"
            "#,
        );
        let r = resolve(main, Config::default(), false).unwrap();
        let c = &r.connections["prod"];
        assert_eq!(c.route(), Route::Tunnel("@prod".into()));
        assert_eq!(r.forwards["@prod"].remote.as_deref(), Some("127.0.0.1:5432"));
        assert_eq!(r.forwards["@prod"].host, "bastion");
    }

    #[test]
    fn shared_tunnel_destinations() {
        let main = parse(
            r#"
            [tunnels.dc]
            host = "bastion"
            user = "ops"
            [tunnels.legacy]
            host = "bastion"
            remote = "10.0.0.9:6379"
            [connections.a]
            kind = "mongodb"
            tunnel = "dc"
            host = "10.0.0.1"
            [connections.b]
            kind = "mongodb"
            tunnel = "dc"
            host = "10.0.0.2"
            port = 27018
            [connections.c]
            kind = "redis"
            tunnel = "legacy"
            "#,
        );
        let r = resolve(main, Config::default(), false).unwrap();
        assert_eq!(r.connections["a"].route(), Route::Tunnel("dc/10.0.0.1:27017".into()));
        assert_eq!(r.connections["b"].route(), Route::Tunnel("dc/10.0.0.2:27018".into()));
        assert_eq!(r.connections["c"].route(), Route::Tunnel("legacy".into()));
        let fa = &r.forwards["dc/10.0.0.1:27017"];
        assert_eq!((fa.host.as_str(), fa.user.as_deref()), ("bastion", Some("ops")));
        assert!(!r.forwards.contains_key("dc"), "remote-less tunnels are only SSH hops");

        let bad = parse("[tunnels.dc]\nhost = \"b\"\n[connections.x]\nkind = \"redis\"\ntunnel = \"dc\"\n");
        assert!(resolve(bad, Config::default(), false).is_err());
    }

    #[test]
    fn duplicates_and_unknown_tunnels_rejected() {
        let main = parse("[connections.a]\nkind = \"redis\"\n");
        let saved = parse("[connections.a]\nkind = \"redis\"\n");
        assert!(resolve(main, saved, false).is_err());
        let bad = parse("[connections.a]\nkind = \"redis\"\ntunnel = \"nope\"\n");
        assert!(resolve(bad, Config::default(), false).is_err());
    }

    #[test]
    fn host_port() {
        assert_eq!(split_host_port("db:5432").unwrap(), ("db".into(), 5432));
        assert_eq!(split_host_port("[::1]:6379").unwrap(), ("::1".into(), 6379));
        assert_eq!(join_host_port("::1", 1), "[::1]:1");
    }
}
