//! Where the SQL shells' output goes: stdout, a file or a pipe (psql `\o`), a tee
//! copy (mysql `tee`), and the pager.
//!
//! Output for one input line is collected and written by [`flush`] when the line
//! is done, so the pager sees the whole result and errors (on stderr) can be
//! ordered after it.

use std::fs::File;
use std::io::Write;
use std::process::{Child, Stdio};
use std::sync::{LazyLock, Mutex};

use anyhow::{Context, Result};

/// When results go through the pager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pager {
    Off,
    /// psql's default: when the output is taller than the terminal.
    Auto,
    /// mysql `pager [cmd]` / psql `\pset pager always`: every result.
    Always,
}

enum Target {
    Stdout,
    File(File),
    Pipe(Child),
}

struct State {
    pending: String,
    target: Target,
    tee: Option<File>,
    pager: Pager,
    /// The pager command; `$PAGER` (or `less -S`) when unset.
    pager_cmd: Option<String>,
    interactive: bool,
    width: usize,
    height: usize,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        pending: String::new(),
        target: Target::Stdout,
        tee: None,
        pager: Pager::Off,
        pager_cmd: None,
        interactive: false,
        width: 0,
        height: 0,
    })
});

fn state() -> std::sync::MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Queue a line of output.
pub fn line(text: &str) {
    let mut s = state();
    s.pending.push_str(text);
    s.pending.push('\n');
}

/// Queue output without adding a newline.
pub fn text(text: &str) {
    state().pending.push_str(text);
}

/// `println!` into the shell's output.
macro_rules! outln {
    () => { $crate::repl::out::line("") };
    ($($t:tt)*) => { $crate::repl::out::line(&format!($($t)*)) };
}
pub(crate) use outln;

pub fn set_interactive(on: bool) {
    state().interactive = on;
}

/// The terminal size, refreshed before each prompt (0 when unknown).
pub fn set_size(width: usize, height: usize) {
    let mut s = state();
    s.width = width;
    s.height = height;
}

/// Terminal width, or `$COLUMNS`, or 80.
pub fn width() -> usize {
    let w = state().width;
    if w > 0 {
        return w;
    }
    std::env::var("COLUMNS").ok().and_then(|c| c.parse().ok()).unwrap_or(80)
}

pub fn set_pager(pager: Pager, cmd: Option<String>) {
    let mut s = state();
    s.pager = pager;
    if cmd.is_some() {
        s.pager_cmd = cmd;
    }
}

pub fn pager() -> Pager {
    state().pager.clone()
}

/// psql `\o`: `None` back to stdout, `|cmd` a pipe, otherwise a file.
pub fn redirect(to: Option<&str>) -> Result<()> {
    flush();
    let target = match to.map(str::trim) {
        None | Some("") => Target::Stdout,
        Some(t) => open_target(t)?,
    };
    let old = std::mem::replace(&mut state().target, target);
    close(old);
    Ok(())
}

/// Run `f` with output going to `to` (a file or `|cmd`), then back to the current
/// target (psql `\g file`).
pub fn with_target<T>(to: &str, f: impl FnOnce() -> T) -> Result<T> {
    flush();
    let target = open_target(to)?;
    let old = std::mem::replace(&mut state().target, target);
    let out = f();
    flush();
    let temp = std::mem::replace(&mut state().target, old);
    close(temp);
    Ok(out)
}

fn open_target(to: &str) -> Result<Target> {
    let t = to.trim();
    Ok(match t.strip_prefix('|') {
        Some(cmd) => Target::Pipe(super::shell_command(cmd.trim()).stdin(Stdio::piped()).spawn().with_context(|| format!("could not run {cmd}"))?),
        None => Target::File(File::create(super::expand_home(t)).with_context(|| t.to_string())?),
    })
}

/// mysql `tee [file]` / `notee`: append a copy of all output to a file.
pub fn tee(to: Option<&str>) -> Result<()> {
    flush();
    let file = match to {
        None => None,
        Some(path) => Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(super::expand_home(path))
                .with_context(|| format!("{path}"))?,
        ),
    };
    state().tee = file;
    Ok(())
}

fn close(t: Target) {
    if let Target::Pipe(mut child) = t {
        drop(child.stdin.take());
        child.wait().ok();
    }
}

/// Write what's queued: through the pager when it applies, to the current target,
/// and to the tee file.
pub fn flush() {
    let mut s = state();
    if s.pending.is_empty() {
        return;
    }
    let text = std::mem::take(&mut s.pending);
    if let Some(tee) = s.tee.as_mut() {
        tee.write_all(text.as_bytes()).ok();
        tee.flush().ok();
    }
    let paged = match (&s.target, &s.pager, s.interactive) {
        (Target::Stdout, Pager::Always, true) => true,
        (Target::Stdout, Pager::Auto, true) => s.height > 0 && text.lines().count() >= s.height,
        _ => false,
    };
    if paged {
        let cmd = s.pager_cmd.clone().or_else(|| super::get_env("PAGER")).unwrap_or_else(|| "less -S".into());
        drop(s);
        if page(&cmd, &text).is_ok() {
            return;
        }
        s = state();
    }
    match &mut s.target {
        Target::Stdout => {
            let mut out = std::io::stdout().lock();
            out.write_all(text.as_bytes()).ok();
            out.flush().ok();
        }
        Target::File(f) => {
            f.write_all(text.as_bytes()).ok();
        }
        Target::Pipe(child) => {
            if let Some(stdin) = child.stdin.as_mut() {
                stdin.write_all(text.as_bytes()).ok();
            }
        }
    }
}

fn page(cmd: &str, text: &str) -> Result<()> {
    let mut child = super::shell_command(cmd).stdin(Stdio::piped()).spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        // The pager may quit before reading everything; that's fine.
        stdin.write_all(text.as_bytes()).ok();
    }
    child.wait()?;
    Ok(())
}

/// Close a `\o` pipe or file at exit, so a pipe's command finishes its output.
pub fn finish() {
    flush();
    let old = std::mem::replace(&mut state().target, Target::Stdout);
    close(old);
}
