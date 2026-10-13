// Version reported by `--version` and used in the client/daemon handshake (a
// mismatch makes the client restart an outdated daemon).
// - CI release builds set VARETO_BUILD to the workflow run id: `0.1.0.1234567890`.
// - Builds from a git checkout add the commit: `0.1.0+ea3e9b6`, and with
//   uncommitted changes a hash of them, `0.1.0+ea3e9b6.dirty-1a2b3c4d`, so every
//   distinct local build has its own version.
// - Anything else (no git, e.g. a published crate) is the package version.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// FNV-1a: a short, stable fingerprint of the uncommitted changes.
fn fnv(data: &[u8]) -> u32 {
    data.iter().fold(0x811c_9dc5u32, |h, &b| (h ^ u32::from(b)).wrapping_mul(0x0100_0193))
}

fn main() {
    println!("cargo:rerun-if-env-changed=VARETO_BUILD");
    let base = std::env::var("CARGO_PKG_VERSION").unwrap();
    if let Some(b) = std::env::var("VARETO_BUILD").ok().map(|b| b.trim().to_string()).filter(|b| !b.is_empty()) {
        println!("cargo:rustc-env=VARETO_VERSION={base}.{b}");
        return;
    }
    let version = match git(&["rev-parse", "--short", "HEAD"]).map(|h| h.trim().to_string()).filter(|h| !h.is_empty()) {
        Some(commit) => {
            // Re-run when the commit or the working tree changes.
            if let Some(dir) = git(&["rev-parse", "--git-dir"]) {
                let dir = dir.trim();
                for f in ["HEAD", "index", "packed-refs", "refs"] {
                    println!("cargo:rerun-if-changed={dir}/{f}");
                }
            }
            for f in ["src", "build.rs", "Cargo.toml", "Cargo.lock"] {
                println!("cargo:rerun-if-changed={f}");
            }
            // Tracked changes, plus the names of new files.
            let diff = git(&["diff", "HEAD"]).unwrap_or_default();
            let untracked = git(&["ls-files", "--others", "--exclude-standard"]).unwrap_or_default();
            if diff.is_empty() && untracked.is_empty() {
                format!("{base}+{commit}")
            } else {
                format!("{base}+{commit}.dirty-{:08x}", fnv(format!("{diff}\0{untracked}").as_bytes()))
            }
        }
        None => base,
    };
    println!("cargo:rustc-env=VARETO_VERSION={version}");
}
