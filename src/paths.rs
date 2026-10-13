//! Filesystem locations. `VARETO_CONFIG_DIR` / `VARETO_RUNTIME_DIR` / `VARETO_STATE_DIR` / `VARETO_SSH_DIR`
//! override the XDG defaults (used by tests and for running several instances).

use std::path::PathBuf;

fn env_dir(var: &str) -> Option<PathBuf> {
    std::env::var_os(var).filter(|v| !v.is_empty()).map(PathBuf::from)
}

fn home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn config_dir() -> PathBuf {
    env_dir("VARETO_CONFIG_DIR")
        .or_else(|| directories::BaseDirs::new().map(|b| b.config_dir().join("vareto")))
        .unwrap_or_else(|| home().join(".config/vareto"))
}

pub fn config_file() -> PathBuf {
    config_dir().join("config.toml")
}

/// Connections saved by the LLM through the `save_connection` tool live here,
/// separate from the human-edited config.
pub fn saved_file() -> PathBuf {
    config_dir().join("saved.toml")
}

pub fn runtime_dir() -> PathBuf {
    env_dir("VARETO_RUNTIME_DIR")
        .or_else(|| env_dir("XDG_RUNTIME_DIR").map(|d| d.join("vareto")))
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/vareto-{}", nix::unistd::getuid())))
}

pub fn socket_path() -> PathBuf {
    runtime_dir().join("daemon.sock")
}

pub fn lock_path() -> PathBuf {
    runtime_dir().join("daemon.lock")
}

pub fn state_dir() -> PathBuf {
    env_dir("VARETO_STATE_DIR")
        .or_else(|| directories::BaseDirs::new().and_then(|b| b.state_dir().map(|d| d.join("vareto"))))
        .unwrap_or_else(|| home().join(".local/state/vareto"))
}

pub fn log_file() -> PathBuf {
    state_dir().join("daemon.log")
}

/// `~/.ssh` (overridable with `VARETO_SSH_DIR`, used by the e2e tests).
pub fn ssh_dir() -> PathBuf {
    env_dir("VARETO_SSH_DIR").unwrap_or_else(|| home().join(".ssh"))
}

pub fn expand_tilde(p: &str) -> PathBuf {
    if p == "~" {
        home()
    } else if let Some(rest) = p.strip_prefix("~/") {
        home().join(rest)
    } else {
        PathBuf::from(p)
    }
}

/// Create a directory (and parents) readable only by the current user.
pub fn ensure_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}
