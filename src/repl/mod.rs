//! `vareto shell`: an interactive shell that behaves like each database's own
//! client (mysql, psql, sqlite3, redis-cli, mongosh), on a pinned daemon session.

mod complete;
mod edit;
mod interrupt;
mod mongo;
mod mysql;
mod out;
mod psql;
mod psql_catalog;
mod psql_copy;
mod psql_print;
mod psql_vars;
mod redis;
mod session;
mod sqlite;
mod sqlbuf;
mod table;

use std::borrow::Cow;
use std::io::{BufRead, IsTerminal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::client::Daemon;
use crate::config::Kind;
use complete::Completions;
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
    pub local_infile: Option<bool>,
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
            local_infile: self.local_infile,
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
            (self.local_infile.is_some(), "--local-infile"),
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
    fn reset(&mut self, s: &mut Session);
    /// An error, worded the way the native client prints it.
    fn error(&self, e: &anyhow::Error) -> String;
    /// Reload completion words (table names, ...) after connecting or switching databases.
    fn refresh(&mut self, _s: &mut Session) {}
    /// What tab completion offers.
    fn completions(&self) -> Completions;
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
/// Most `(table, column)` pairs loaded for completion.
pub const COLUMN_LIMIT: usize = 5000;

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
/// (table, column) pairs from the first two columns of a result, for completion.
pub fn pairs(v: &serde_json::Value) -> Vec<(String, String)> {
    match table::QueryResult::from_json(v) {
        table::QueryResult::Table { rows, .. } => rows
            .iter()
            .filter_map(|r| Some((r.first()?.as_str()?.to_string(), r.get(1)?.as_str()?.to_string())))
            .collect(),
        _ => Vec::new(),
    }
}

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
            Kind::Sqlite => Box::new(sqlite::Sqlite::new(interactive)),
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

/// Our candidates, reedline's popup: each one shows its kind as a second column,
/// with the characters that matched highlighted.
struct ShellCompleter {
    words: Arc<Mutex<Completions>>,
}

impl reedline::Completer for ShellCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> reedline::CompletionResult {
        let (start, cands) = complete::complete(&self.words.lock().unwrap(), line, pos);
        let width = cands.iter().map(|c| c.text.chars().count()).max().unwrap_or(0).min(48);
        let suggestions: Vec<reedline::Suggestion> = cands
            .into_iter()
            .map(|c| {
                let note = c.note();
                reedline::Suggestion {
                    display_override: Some(format!("{:<width$}  {note}", c.text)),
                    value: c.text,
                    span: reedline::Span::new(start, pos),
                    append_whitespace: false,
                    match_indices: Some(c.indices),
                    ..Default::default()
                }
            })
            .collect();
        reedline::CompletionResult::fresh(suggestions)
    }
}

/// The flavor's prompt, shown as it is.
struct ShellPrompt(String);

impl reedline::Prompt for ShellPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }
    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
    fn render_prompt_indicator(&self, _: reedline::PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
    fn render_prompt_history_search_indicator(&self, h: reedline::PromptHistorySearch) -> Cow<'_, str> {
        Cow::Owned(format!("(history search: {}) ", h.term))
    }
    fn get_prompt_color(&self) -> reedline::Color {
        reedline::Color::Default
    }
}

/// reedline's file history, holding whole statements: the shells read a line at a
/// time, so reedline's own save of each submitted line is skipped, and the loop
/// saves a statement once it's complete.
struct StatementHistory {
    inner: reedline::FileBackedHistory,
    accept: Arc<AtomicBool>,
}

impl reedline::History for StatementHistory {
    fn save(&mut self, h: reedline::HistoryItem) -> reedline::Result<reedline::HistoryItem> {
        if self.accept.load(Ordering::Relaxed) { self.inner.save(h) } else { Ok(h) }
    }
    fn load(&self, id: reedline::HistoryItemId) -> reedline::Result<reedline::HistoryItem> {
        self.inner.load(id)
    }
    fn count(&self, query: reedline::SearchQuery) -> reedline::Result<i64> {
        self.inner.count(query)
    }
    fn search(&self, query: reedline::SearchQuery) -> reedline::Result<Vec<reedline::HistoryItem>> {
        self.inner.search(query)
    }
    fn update(&mut self, id: reedline::HistoryItemId, updater: &dyn Fn(reedline::HistoryItem) -> reedline::HistoryItem) -> reedline::Result<()> {
        self.inner.update(id, updater)
    }
    fn clear(&mut self) -> reedline::Result<()> {
        self.inner.clear()
    }
    fn delete(&mut self, h: reedline::HistoryItemId) -> reedline::Result<()> {
        self.inner.delete(h)
    }
    fn sync(&mut self) -> std::io::Result<()> {
        self.inner.sync()
    }
    fn session(&self) -> Option<reedline::HistorySessionId> {
        self.inner.session()
    }
}

/// Entries of a rustyline history file (`#V2`: `\n` and `\\` escaped), or `None`
/// when `text` isn't one.
fn rustyline_history(text: &str) -> Option<Vec<String>> {
    let mut lines = text.lines();
    if lines.next()? != "#V2" {
        return None;
    }
    Some(
        lines
            .filter(|l| !l.is_empty())
            .map(|l| {
                let mut out = String::with_capacity(l.len());
                let mut chars = l.chars();
                while let Some(c) = chars.next() {
                    match (c, c == '\\') {
                        (_, true) => match chars.next() {
                            Some('n') => out.push('\n'),
                            Some(other) => out.push(other),
                            None => out.push('\\'),
                        },
                        (c, false) => out.push(c),
                    }
                }
                out
            })
            .collect(),
    )
}

/// How reedline's history file writes a newline inside an entry.
const REEDLINE_NEWLINE: &str = "<\\n>";

/// History files written by the rustyline-based shell become reedline's format
/// (one entry per line, newlines as `<\n>`); the original is kept beside it.
fn migrate_history(path: &std::path::Path) {
    let Ok(text) = std::fs::read_to_string(path) else { return };
    let Some(entries) = rustyline_history(&text) else { return };
    let backup = std::path::PathBuf::from(format!("{}.rustyline", path.display()));
    if std::fs::copy(path, &backup).is_err() {
        return;
    }
    let body: String = entries.iter().map(|e| e.replace('\n', REEDLINE_NEWLINE) + "\n").collect();
    std::fs::write(path, body).ok();
}

/// The host commands Ctrl-T and Ctrl-R send: they open the fzf finder (skim).
const FIND_COMPLETION: &str = "vareto:find-completion";
const FIND_HISTORY: &str = "vareto:find-history";

/// One entry of the fzf finder: matched on `text`, shown with `note`, inserts `value`.
struct Pick {
    text: String,
    value: String,
    note: String,
    /// Width the text is padded to, so the notes line up.
    width: usize,
}

impl skim::SkimItem for Pick {
    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.text)
    }

    fn display(&self, context: skim::DisplayContext) -> ratatui_core::text::Line<'_> {
        let mut line = context.to_line(Cow::Borrowed(&self.text));
        if !self.note.is_empty() {
            let dim = ratatui_core::style::Style::default().add_modifier(ratatui_core::style::Modifier::DIM);
            let pad = self.width.saturating_sub(self.text.chars().count()) + 2;
            line.push_span(ratatui_core::text::Span::styled(format!("{:pad$}{}", "", self.note), dim));
        }
        line
    }

    fn output(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.value)
    }
}

/// Run the fzf finder (skim) below the prompt; the chosen entry's value, or
/// `None` when it was left with Esc.
fn finder(picks: Vec<Pick>, query: &str) -> Option<String> {
    // Open below the line being edited, as fzf does.
    print!("\r\n");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    let mut o = skim::SkimOptions::default();
    o.height = "40%".into();
    o.reverse = true;
    o.query = (!query.is_empty()).then(|| query.to_string());
    let out = skim::Skim::run_items(o.build(), picks).ok()?;
    if out.is_abort {
        return None;
    }
    out.selected_items.first().map(|m| m.item.output().into_owned())
}

/// Ctrl-T / Ctrl-R: open the finder on completions for the word at the cursor, or
/// on the history, then put its pick into the buffer reedline kept.
fn run_finder(ed: &mut reedline::Reedline, which: &str, words: &Mutex<Completions>) {
    use reedline::EditCommand;
    let buffer = ed.current_buffer_contents().to_string();
    let pos = ed.current_insertion_point();
    let (new, cursor) = if which == FIND_COMPLETION {
        let (start, typed, all) = complete::complete_all(&words.lock().unwrap(), &buffer, pos);
        let width = all.iter().map(|c| c.text.chars().count()).max().unwrap_or(0).min(48);
        let picks = all.into_iter().map(|c| Pick { note: c.note(), value: c.text.clone(), text: c.text, width }).collect();
        let Some(v) = finder(picks, typed) else { return };
        (format!("{}{v}{}", &buffer[..start], &buffer[pos..]), start + v.len())
    } else {
        let query = reedline::SearchQuery::everything(reedline::SearchDirection::Backward, None);
        let mut seen = std::collections::HashSet::new();
        let picks: Vec<Pick> = ed
            .history()
            .search(query)
            .unwrap_or_default()
            .into_iter()
            .map(|h| h.command_line)
            .filter(|c| seen.insert(c.clone()))
            .map(|c| Pick { text: c.replace('\n', " ↵ "), value: c, note: String::new(), width: 0 })
            .collect();
        let Some(v) = finder(picks, buffer.trim()) else { return };
        let len = v.len();
        (v, len)
    };
    ed.run_edit_commands(&[EditCommand::Clear, EditCommand::InsertString(new), EditCommand::MoveToPosition { position: cursor, select: false }]);
}

fn run_interactive(f: &mut dyn Flavor, s: &mut Session, connection: &str) -> i32 {
    use reedline::{default_emacs_keybindings, Emacs, IdeMenu, KeyCode, KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal};
    let history_path = crate::paths::state_dir().join("history").join(connection);
    if let Some(dir) = history_path.parent() {
        crate::paths::ensure_private_dir(dir).ok();
    }
    migrate_history(&history_path);
    let accept = Arc::new(AtomicBool::new(false));
    let history = match reedline::FileBackedHistory::with_file(5000, history_path.clone()) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("cannot open the history file {}: {e}", history_path.display());
            return 1;
        }
    };
    let words = Arc::new(Mutex::new(Completions::default()));
    let mut keys = default_emacs_keybindings();
    keys.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![ReedlineEvent::Menu("completion_menu".into()), ReedlineEvent::MenuNext]),
    );
    keys.add_binding(KeyModifiers::SHIFT, KeyCode::BackTab, ReedlineEvent::MenuPrevious);
    // fzf's bindings: Ctrl-T finds a completion, Ctrl-R a history entry.
    keys.add_binding(KeyModifiers::CONTROL, KeyCode::Char('t'), ReedlineEvent::ExecuteHostCommand(FIND_COMPLETION.into()));
    keys.add_binding(KeyModifiers::CONTROL, KeyCode::Char('r'), ReedlineEvent::ExecuteHostCommand(FIND_HISTORY.into()));
    // The whole line, not just up to the cursor: `select | from users u` needs the FROM.
    let menu = IdeMenu::default()
        .with_name("completion_menu")
        .with_marker("")
        .with_default_border()
        .with_input_mode(reedline::InputMode::FullBuffer);
    let mut ed = Reedline::create()
        .with_history(Box::new(StatementHistory { inner: history, accept: accept.clone() }))
        .with_completer(Box::new(ShellCompleter { words: words.clone() }))
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
        .with_quick_completions(true)
        .with_edit_mode(Box::new(Emacs::new(keys)));

    println!("{}", f.banner(s));
    f.refresh(s);
    *words.lock().unwrap() = f.completions();
    let mut entry: Vec<String> = Vec::new();
    'session: loop {
        let db = s.database.clone();
        if let Ok((w, h)) = crossterm::terminal::size() {
            out::set_size(w as usize, h as usize);
        }
        let signal = match ed.read_line(&ShellPrompt(f.prompt(s))) {
            Ok(sig) => sig,
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        };
        match signal {
            // A recalled or pasted multi-line statement arrives at once; the
            // shells take it a line at a time.
            Signal::Success(buffer) => {
                for line in buffer.split('\n') {
                    entry.push(line.to_string());
                    let flow = f.line(line, s);
                    out::flush();
                    if !f.pending() {
                        let text = entry.join("\n");
                        if !text.trim().is_empty() {
                            accept.store(true, Ordering::Relaxed);
                            ed.history_mut().save(reedline::HistoryItem::from_command_line(text)).ok();
                            accept.store(false, Ordering::Relaxed);
                            ed.sync_history().ok();
                        }
                        entry.clear();
                    }
                    match flow {
                        Ok(Flow::Quit) => break 'session,
                        Ok(Flow::Continue) => {}
                        Ok(Flow::History(to)) => {
                            let query = reedline::SearchQuery::everything(reedline::SearchDirection::Forward, None);
                            let lines: Vec<String> = ed.history().search(query).unwrap_or_default().into_iter().map(|h| h.command_line).collect();
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
                }
                if s.database != db {
                    f.refresh(s);
                }
                // Cheap, and picks up `rehash`.
                *words.lock().unwrap() = f.completions();
            }
            // The editor is suspended with its buffer kept; the next read_line resumes it.
            Signal::HostCommand(which) => run_finder(&mut ed, &which, &words),
            Signal::CtrlC => {
                f.reset(s);
                entry.clear();
            }
            Signal::CtrlD => {
                if !f.pending() {
                    break;
                }
                f.reset(s);
                entry.clear();
            }
            _ => {}
        }
    }
    ed.sync_history().ok();
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rustyline_history_converts() {
        let dir = std::env::temp_dir().join(format!("vareto-hist-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("conn");
        std::fs::write(&path, "#V2\nselect 1;\nselect\\n  2;\na\\\\b\n").unwrap();
        migrate_history(&path);
        assert!(dir.join("conn.rustyline").exists());
        // reedline's own reader sees the same entries, multi-line one included.
        use reedline::History as _;
        let h = reedline::FileBackedHistory::with_file(100, path.clone()).unwrap();
        let q = reedline::SearchQuery::everything(reedline::SearchDirection::Forward, None);
        let entries: Vec<String> = h.search(q).unwrap().into_iter().map(|i| i.command_line).collect();
        assert_eq!(entries, vec!["select 1;", "select\n  2;", "a\\b"]);
        // A second run leaves the converted file alone.
        migrate_history(&path);
        assert!(!std::fs::read_to_string(&path).unwrap().starts_with("#V2"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
