//! Ctrl-C for the shell. One listener for the whole run: the first `ctrl_c()`
//! replaces the default SIGINT handler, so a signal that arrives while nobody is
//! listening would otherwise be lost (the prompt itself is unaffected: the line
//! editor reads ^C as a key).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::LazyLock;

use tokio::sync::Notify;

struct State {
    /// Ctrl-C presses so far.
    count: AtomicU64,
    /// A daemon call is in flight (it gets cancelled instead).
    in_call: AtomicBool,
    notify: Notify,
}

static STATE: LazyLock<State> =
    LazyLock::new(|| State { count: AtomicU64::new(0), in_call: AtomicBool::new(false), notify: Notify::new() });

/// Start listening. In a script (`interactive == false`), Ctrl-C outside a daemon
/// call ends the shell with status 130, as `mysql < file` or `psql -f` do.
pub fn install(rt: &tokio::runtime::Handle, interactive: bool) {
    rt.spawn(async move {
        while tokio::signal::ctrl_c().await.is_ok() {
            STATE.count.fetch_add(1, Ordering::SeqCst);
            STATE.notify.notify_waiters();
            if !interactive && !STATE.in_call.load(Ordering::SeqCst) {
                eprintln!("^C");
                std::process::exit(130);
            }
        }
    });
}

/// Ctrl-C presses so far; compare two readings to see whether one happened between.
pub fn count() -> u64 {
    STATE.count.load(Ordering::SeqCst)
}

/// Run `fut` (a daemon call); `on_interrupt` runs on each Ctrl-C until it finishes.
pub async fn during<T, F, C>(fut: F, mut on_interrupt: impl FnMut() -> C) -> T
where
    F: Future<Output = T>,
    C: Future<Output = ()>,
{
    STATE.in_call.store(true, Ordering::SeqCst);
    tokio::pin!(fut);
    let out = loop {
        // Registered before anything else runs, so no Ctrl-C slips between.
        let interrupted = STATE.notify.notified();
        tokio::pin!(interrupted);
        interrupted.as_mut().enable();
        tokio::select! {
            r = &mut fut => break r,
            _ = interrupted => on_interrupt().await,
        }
    };
    STATE.in_call.store(false, Ordering::SeqCst);
    out
}
