//! `vareto shell`: an interactive shell that behaves like each database's own
//! client (mysql, psql, sqlite3, redis-cli, mongosh), on a pinned daemon session.

mod interrupt;
mod mongo;
mod mysql;
mod psql;
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
}

pub enum Flow {
    Continue,
    Quit,
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
}

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
    let p = shellexpand_home(path.trim().trim_matches(|c| c == '\'' || c == '"'));
    std::fs::read_to_string(&p).map_err(|e| anyhow::anyhow!("{p}: {e}"))
}

fn shellexpand_home(p: &str) -> String {
    match (p.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}/{rest}"),
        _ => p.to_string(),
    }
}

/// Feed a whole script through a flavor, line by line. Stops at the first error.
pub fn run_script(f: &mut dyn Flavor, s: &mut Session, text: &str) -> Result<Flow> {
    for line in text.lines() {
        if let Flow::Quit = f.line(line, s)? {
            return Ok(Flow::Quit);
        }
    }
    f.finish(s)?;
    Ok(Flow::Continue)
}

pub async fn run(daemon: &Daemon, opts: Options) -> Result<bool> {
    let client = daemon.connect().await?;
    let rt = tokio::runtime::Handle::current();
    let ro = daemon.force_readonly;
    let interactive = opts.execute.is_none() && std::io::stdin().is_terminal();
    interrupt::install(&rt, interactive);
    tokio::task::spawn_blocking(move || {
        let mut s = Session::open(rt, client, ro, &opts.connection, opts.database.clone(), opts.limit)?;
        let mut f: Box<dyn Flavor> = match s.kind {
            Kind::Mysql => Box::new(mysql::Mysql::new(&s)),
            Kind::Postgres => Box::new(psql::Psql::new()),
            Kind::Redis => Box::new(redis::Redis::new()),
            Kind::Sqlite => Box::new(sqlite::Sqlite::new()),
            Kind::Mongodb => Box::new(mongo::Mongo::new(interactive)?),
        };
        let ok = if let Some(cmds) = &opts.execute {
            script(f.as_mut(), &mut s, cmds)
        } else if interactive {
            run_interactive(f.as_mut(), &mut s, &opts.connection)
        } else {
            let mut text = String::new();
            for l in std::io::stdin().lock().lines() {
                text.push_str(&l?);
                text.push('\n');
            }
            script(f.as_mut(), &mut s, &text)
        };
        s.close();
        Ok(ok)
    })
    .await?
}

fn script(f: &mut dyn Flavor, s: &mut Session, text: &str) -> bool {
    match run_script(f, s, text) {
        Ok(_) => true,
        Err(e) => {
            eprintln!("{}", f.error(&e));
            false
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

fn run_interactive(f: &mut dyn Flavor, s: &mut Session, connection: &str) -> bool {
    let config = rustyline::Config::builder().auto_add_history(false).max_history_size(5000).map(|b| b.build());
    let mut ed: rustyline::Editor<Helper, DefaultHistory> = match config.and_then(rustyline::Editor::with_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("cannot start the line editor: {e}");
            return false;
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
        match ed.readline(&f.prompt(s)) {
            Ok(line) => {
                entry.push(line.clone());
                let flow = f.line(&line, s);
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
                    Err(e) => eprintln!("{}", f.error(&e)),
                }
                if s.database != db {
                    f.refresh(s);
                    *words.borrow_mut() = f.words();
                }
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
    true
}
