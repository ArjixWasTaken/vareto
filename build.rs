// Version reported by `--version` and used in the client/daemon handshake (a
// mismatch makes the client restart an outdated daemon). CI release builds set
// VARETO_BUILD to the workflow run id, giving e.g. `0.1.0.1234567890`.
fn main() {
    println!("cargo:rerun-if-env-changed=VARETO_BUILD");
    let base = std::env::var("CARGO_PKG_VERSION").unwrap();
    let version = match std::env::var("VARETO_BUILD") {
        Ok(b) if !b.is_empty() => format!("{base}.{b}"),
        _ => base,
    };
    println!("cargo:rustc-env=VARETO_VERSION={version}");
}
