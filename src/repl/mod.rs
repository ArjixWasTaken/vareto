//! `vareto shell`: an interactive shell that behaves like each database's own
//! client (mysql, psql, sqlite3, redis-cli, mongosh), on a pinned daemon session.

mod edit;
mod interrupt;
mod mongo;
mod mysql;
mod out;
mod psql;
mod psql_catalog;
mod psql_print;
mod psql_vars;
mod redis;
mod session;
mod sqlite;
mod sqlbuf;
mod table;

use std::cell::RefCell;
use std::io::{BufRead, IsTerminal};
use std::rc::Rc;

use anyhow::Result;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::error::ReadlineError;

use crate::client::Daemon;
use crate::config::Kind;
use session::Session;

pub struct Options {
    pub connection: String,
    pub database: Option<String>,
    pub limit: Option<usize>,
    /// Run these commands and exit instead of reading input.
    pub execute: Option<String>,
    /// mysql `--force`: keep going after errors in a script.
    pub force: bool,
    /// psql `-v name=value`.
    pub set: Vec<String>,
    // mysql client options.
    pub safe_updates: bool,
    pub auto_vertical: bool,
    pub vertical: bool,
    pub batch: bool,
    pub raw: bool,
    pub table: bool,
    pub skip_column_names: bool,
    pub column_type_info: bool,
    pub show_warnings: bool,
}

impl Options {
    fn mysql(&self) -> mysql::MysqlOptions {
        mysql::MysqlOptions {
            force: self.force,
            safe_updates: self.safe_updates,
            auto_vertical: self.auto_vertical,
            vertical: self.vertical,
            batch: self.batch,
            raw: self.raw,
            table: self.table,
            skip_column_names: self.skip_column_names,
            column_type_info: self.column_type_info,
            show_warnings: self.show_warnings,
        }
    }

    /// The mysql-only options that were given.
    fn mysql_flags(&self) -> Vec<&'static str> {
        [
            (self.force, "--force"),
            (self.safe_updates, "--safe-updates"),
            (self.auto_vertical, "--auto-vertical-output"),
            (self.vertical, "--vertical"),
            (self.batch, "--batch"),
            (self.raw, "--raw"),
            (self.table, "--table"),
            (self.skip_column_names, "--skip-column-names"),
            (self.column_type_info, "--column-type-info"),
            (self.show_warnings, "--show-warnings"),
        ]
        .into_iter()
        .filter_map(|(on, name)| on.then_some(name))
        .collect()
    }
}

pub enum Flow {
    Continue,
    Quit,
    /// psql `\s [file]`: the driver prints (or writes) the line history.
    History(Option<String>),
}

/// psql `\setenv`: variables for the commands the shell starts (`\!`, the editor,
/// the pager). The process environment itself is left alone; changing it while
/// other threads run is unsound.
static ENV_OVERLAY: std::sync::LazyLock<std::sync::Mutex<std::collections::BTreeMap<String, Option<String>>>> =
    std::sync::LazyLock::new(Default::default);

pub fn set_env(name: &str, value: Option<&str>) {
    ENV_OVERLAY.lock().unwrap_or_else(|e| e.into_inner()).insert(name.to_string(), value.map(str::to_string));
}

/// An environment variable as the shell's child processes see it.
pub fn get_env(name: &str) -> Option<String> {
    match ENV_OVERLAY.lock().unwrap_or_else(|e| e.into_inner()).get(name) {
        Some(v) => v.clone(),
        None => std::env::var(name).ok(),
    }
}

/// `sh -c <cmd>` with the `\setenv` overlay applied.
pub fn shell_command(cmd: &str) -> std::process::Command {
    let mut c = std::process::Command::new("sh");
    c.arg("-c").arg(cmd);
    for (k, v) in ENV_OVERLAY.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        match v {
            Some(v) => c.env(k, v),
            None => c.env_remove(k),
        };
    }
    c
}

/// One database's shell dialect.
pub trait Flavor {
    fn banner(&self, s: &Session) -> String;
    fn prompt(&self, s: &Session) -> String;
    /// Handle one line of input, printing whatever it produces.
    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow>;
    /// End of a script: run whatever is still buffered.
    fn finish(&mut self, _s: &mut Session) -> Result<()> {
        Ok(())
    }
    /// Unfinished input is buffered.
    fn pending(&self) -> bool;
    /// Drop buffered input (Ctrl-C).
    fn reset(&mut self);
    /// An error, worded the way the native client prints it.
    fn error(&self, e: &anyhow::Error) -> String;
    /// Reload completion words (table names, ...) after connecting or switching databases.
    fn refresh(&mut self, _s: &mut Session) {}
    /// Words for tab completion.
    fn words(&self) -> Vec<String>;
    /// Whether a script stops at its first error (mysql), or reports it and goes on
    /// (psql unless `ON_ERROR_STOP`).
    fn stop_on_error(&self) -> bool {
        true
    }
    /// Exit status when a script stopped at an error (psql: 3).
    fn stop_status(&self) -> i32 {
        1
    }
    /// Exit status for a script that ran to the end with `errors` errors.
    fn finish_status(&self, errors: usize) -> i32 {
        i32::from(errors > 0)
    }
}

/// Errors a script reported and went past (see [`Flavor::stop_on_error`]).
static SCRIPT_ERRORS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Completion words shared by the SQL shells.
pub const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES",
    "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX", "VIEW", "DROP", "ALTER", "ADD", "COLUMN", "JOIN",
    "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON", "USING", "AND", "OR", "NOT", "NULL", "IS", "IN", "EXISTS",
    "BETWEEN", "LIKE", "DISTINCT", "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "UNION", "ALL", "WITH",
    "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "EXPLAIN", "ANALYZE", "SHOW", "DESCRIBE", "COUNT", "RETURNING",
    "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "DEFAULT", "UNIQUE", "TRUNCATE",
];

/// The error text without driver boilerplate.
/// The database's structured error fields, when the daemon sent them.
pub fn error_info(e: &anyhow::Error) -> Option<&serde_json::Value> {
    e.chain().find_map(|c| c.downcast_ref::<crate::ipc::ServerError>()).and_then(|s| s.info.as_ref())
}

pub fn error_text(e: &anyhow::Error) -> String {
    let s = format!("{e:#}");
    s.strip_prefix("error returned from database: ").unwrap_or(&s).to_string()
}

pub fn elapsed(v: &serde_json::Value) -> f64 {
    v["elapsed_ms"].as_f64().unwrap_or(0.0) / 1000.0
}

/// Names from a `ListTables` response (SQL tables, Mongo collections).
pub fn names(v: &serde_json::Value) -> Vec<String> {
    match table::QueryResult::from_json(v) {
        table::QueryResult::Table { columns, rows, .. } => {
            let i = columns.iter().position(|c| c == "name").unwrap_or(0);
            rows.iter().filter_map(|r| r.get(i).and_then(|v| v.as_str()).map(str::to_string)).collect()
        }
        table::QueryResult::Value(serde_json::Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    }
}

/// Read a file for `source` / `\i` / `.read`.
pub fn read_script(path: &str) -> Result<String> {
    let p = expand_home(path.trim().trim_matches(|c| c == '\'' || c == '"'));
    std::fs::read_to_string(&p).map_err(|e| anyhow::anyhow!("{p}: {e}"))
}

pub fn expand_home(p: &str) -> String {
    match (p.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}/{rest}"),
        _ => p.to_string(),
    }
}

/// Feed a whole script through a flavor, line by line. An error ends it, or is
/// reported and skipped, as the flavor's client does.
pub fn run_script(f: &mut dyn Flavor, s: &mut Session, text: &str) -> Result<Flow> {
    run_lines(f, s, text.lines().map(|l| Ok(l.to_string())))
}

/// [`run_script`] over lines as they arrive (piped stdin runs statement by
/// statement, like `mysql < file`, instead of waiting for the end of input).
fn run_lines(f: &mut dyn Flavor, s: &mut Session, lines: impl Iterator<Item = std::io::Result<String>>) -> Result<Flow> {
    let handle = |f: &mut dyn Flavor, r: Result<Flow>| -> Result<Option<Flow>> {
        match r {
            Ok(Flow::Quit) => Ok(Some(Flow::Quit)),
            // No line history in a script.
            Ok(Flow::Continue | Flow::History(_)) => Ok(None),
            Err(e) if f.stop_on_error() => Err(e),
            Err(e) => {
                out::flush();
                eprintln!("{}", f.error(&e));
                SCRIPT_ERRORS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(None)
            }
        }
    };
    for line in lines {
        let r = f.line(&line?, s);
        out::flush();
        if let Some(flow) = handle(f, r)? {
            return Ok(flow);
        }
    }
    let r = f.finish(s).map(|()| Flow::Continue);
    out::flush();
    handle(f, r)?;
    Ok(Flow::Continue)
}

/// Runs the shell; returns the exit status.
pub async fn run(daemon: &Daemon, opts: Options) -> Result<i32> {
    let client = daemon.connect().await?;
    let rt = tokio::runtime::Handle::current();
    let ro = daemon.force_readonly;
    let interactive = opts.execute.is_none() && std::io::stdin().is_terminal();
    interrupt::install(&rt, interactive);
    out::set_interactive(interactive);
    tokio::task::spawn_blocking(move || {
        let mut s = Session::open(rt, client, ro, &opts.connection, opts.database.clone(), opts.limit)?;
        if s.kind != Kind::Mysql && !opts.mysql_flags().is_empty() {
            anyhow::bail!("{} only apply to mysql connections", opts.mysql_flags().join(", "));
        }
        let mut f: Box<dyn Flavor> = match s.kind {
            Kind::Mysql => Box::new(mysql::Mysql::new(&mut s, opts.mysql())?),
            Kind::Postgres => Box::new(psql::Psql::new(&s, &opts, interactive)?),
            Kind::Redis => Box::new(redis::Redis::new()),
            Kind::Sqlite => Box::new(sqlite::Sqlite::new()),
            Kind::Mongodb => Box::new(mongo::Mongo::new(interactive)?),
        };
        if !opts.set.is_empty() && s.kind != Kind::Postgres {
            anyhow::bail!("-v/--set is for postgres connections");
        }
        let code = if let Some(cmds) = &opts.execute {
            script(f.as_mut(), &mut s, cmds)
        } else if interactive {
            run_interactive(f.as_mut(), &mut s, &opts.connection)
        } else {
            let stdin = std::io::stdin();
            let r = run_lines(f.as_mut(), &mut s, stdin.lock().lines());
            finish_script(f.as_mut(), r)
        };
        out::finish();
        s.close();
        Ok(code)
    })
    .await?
}

fn script(f: &mut dyn Flavor, s: &mut Session, text: &str) -> i32 {
    let r = run_script(f, s, text);
    finish_script(f, r)
}

/// The exit status of a script run, printing the error that stopped it.
fn finish_script(f: &mut dyn Flavor, r: Result<Flow>) -> i32 {
    match r {
        Ok(_) => f.finish_status(SCRIPT_ERRORS.load(std::sync::atomic::Ordering::SeqCst)),
        Err(e) => {
            out::flush();
            eprintln!("{}", f.error(&e));
            f.stop_status()
        }
    }
}

struct Helper {
    words: Rc<RefCell<Vec<String>>>,
}

impl Completer for Helper {
    type Candidate = String;

    fn complete(&self, line: &str, pos: usize, _: &rustyline::Context<'_>) -> rustyline::Result<(usize, Vec<String>)> {
        let start = line[..pos]
            .rfind(|c: char| !(c.is_alphanumeric() || matches!(c, '_' | '.' | '\\' | '$')))
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &line[start..pos];
        if word.is_empty() {
            return Ok((pos, Vec::new()));
        }
        let lower = word.to_lowercase();
        let upper = word.chars().next().is_some_and(char::is_uppercase);
        let mut out: Vec<String> = self
            .words
            .borrow()
            .iter()
            .filter(|w| w.to_lowercase().starts_with(&lower) && w.len() > word.len())
            .map(|w| {
                // Keywords follow the case being typed.
                let kw = w.chars().all(|c| c.is_ascii_uppercase() || c == ' ');
                if kw && !upper { w.to_lowercase() } else { w.clone() }
            })
            .collect();
        out.sort();
        out.dedup();
        Ok((start, out))
    }
}

impl Hinter for Helper {
    type Hint = String;
}
impl Highlighter for Helper {}
impl Validator for Helper {}
impl rustyline::Helper for Helper {}

fn run_interactive(f: &mut dyn Flavor, s: &mut Session, connection: &str) -> i32 {
    let config = rustyline::Config::builder().auto_add_history(false).max_history_size(5000).map(|b| b.build());
    let mut ed: rustyline::Editor<Helper, DefaultHistory> = match config.and_then(rustyline::Editor::with_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("cannot start the line editor: {e}");
            return 1;
        }
    };
    let words = Rc::new(RefCell::new(Vec::new()));
    ed.set_helper(Some(Helper { words: words.clone() }));
    let history = crate::paths::state_dir().join("history").join(connection);
    if let Some(dir) = history.parent() {
        crate::paths::ensure_private_dir(dir).ok();
    }
    ed.load_history(&history).ok();

    println!("{}", f.banner(s));
    f.refresh(s);
    *words.borrow_mut() = f.words();
    let mut entry: Vec<String> = Vec::new();
    loop {
        let db = s.database.clone();
        if let Some((w, h)) = ed.dimensions() {
            out::set_size(w as usize, h as usize);
        }
        match ed.readline(&f.prompt(s)) {
            Ok(line) => {
                entry.push(line.clone());
                let flow = f.line(&line, s);
                out::flush();
                if !f.pending() {
                    let text = entry.join("\n");
                    if !text.trim().is_empty() {
                        ed.add_history_entry(text).ok();
                    }
                    entry.clear();
                }
                match flow {
                    Ok(Flow::Quit) => break,
                    Ok(Flow::Continue) => {}
                    Ok(Flow::History(to)) => {
                        let lines: Vec<String> = ed.history().iter().map(|h| h.to_string()).collect();
                        match to {
                            Some(path) => match std::fs::write(expand_home(&path), lines.join("\n") + "\n") {
                                Ok(()) => println!("Wrote history to file \"{path}\"."),
                                Err(e) => eprintln!("{path}: {e}"),
                            },
                            None => println!("{}", lines.join("\n")),
                        }
                    }
                    Err(e) => eprintln!("{}", f.error(&e)),
                }
                if s.database != db {
                    f.refresh(s);
                }
                // Cheap, and picks up `rehash`.
                *words.borrow_mut() = f.words();
            }
            Err(ReadlineError::Interrupted) => {
                f.reset();
                entry.clear();
            }
            Err(ReadlineError::Eof) => {
                if !f.pending() {
                    break;
                }
                f.reset();
                entry.clear();
            }
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }
    ed.save_history(&history).ok();
    0
}
