//! Password resolution: plaintext / `${env:VAR}`, `password_cmd`, OS keyring.

use anyhow::{bail, Context, Result};

use crate::config::{interpolate_env, Connection};

pub const KEYRING_SERVICE: &str = "vareto";

/// Resolve the password for a connection. Blocking (runs a command / talks to D-Bus).
pub fn password(conn: &Connection) -> Result<Option<String>> {
    let cfg = &conn.cfg;
    let set = [cfg.password.is_some(), cfg.password_cmd.is_some(), cfg.keyring].iter().filter(|b| **b).count();
    if set > 1 {
        bail!("connection {:?}: set only one of password, password_cmd, keyring", conn.name);
    }
    if let Some(p) = &cfg.password {
        return interpolate_env(p).map(Some);
    }
    if let Some(cmd) = &cfg.password_cmd {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .stdin(std::process::Stdio::null())
            .output()
            .with_context(|| format!("running password_cmd for {:?}", conn.name))?;
        if !out.status.success() {
            bail!(
                "password_cmd for {:?} failed ({}): {}",
                conn.name,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let s = String::from_utf8(out.stdout).context("password_cmd output is not UTF-8")?;
        return Ok(Some(s.trim_end_matches(['\r', '\n']).to_string()));
    }
    if cfg.keyring {
        return keyring_get(&conn.name)?
            .map(Some)
            .with_context(|| format!("no keyring entry for connection {:?} (run `vareto secret set {}`)", conn.name, conn.name));
    }
    Ok(None)
}

fn entry(account: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, account).map_err(|e| match e {
        keyring::Error::NoDefaultStore => match keyring::Entry::store_status() {
            Err(inner) => anyhow::anyhow!("OS keyring unavailable: {inner}"),
            Ok(()) => anyhow::anyhow!("OS keyring unavailable"),
        },
        e => e.into(),
    })
}

pub fn keyring_get(account: &str) -> Result<Option<String>> {
    match entry(account)?.get_password() {
        Ok(p) => Ok(Some(p)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn keyring_set(account: &str, password: &str) -> Result<()> {
    entry(account)?.set_password(password)?;
    Ok(())
}

pub fn keyring_delete(account: &str) -> Result<()> {
    match entry(account)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e.into()),
    }
}
