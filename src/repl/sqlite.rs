//! sqlite3.

use anyhow::{bail, Context as _, Result};
use serde_json::Value;

use super::out::outln;
use crate::textfmt::StreamFormat;
use super::session::Session;
use super::sqlbuf::{Dialect, SqlBuf};
use super::table::{self, QueryResult, Style};
use super::complete::{rule, Offer, Rule};
use super::{elapsed, error_text, names, pairs, read_script, run_script, Flavor, Flow, Completions, COLUMN_LIMIT, SQL_KEYWORDS};
use crate::db::sql::quote_lit;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    List,
    Csv,
    Column,
    Line,
    Box,
    Table,
    Json,
}

const MODES: &[(&str, Mode)] = &[
    ("list", Mode::List),
    ("csv", Mode::Csv),
    ("column", Mode::Column),
    ("line", Mode::Line),
    ("box", Mode::Box),
    ("table", Mode::Table),
    ("json", Mode::Json),
];

pub struct Sqlite {
    buf: SqlBuf,
    mode: Mode,
    headers: bool,
    timer: bool,
    null: String,
    tables: Vec<String>,
    columns: Vec<(String, String)>,
    /// `.once FILE`: where the next statement's output goes.
    once: Option<String>,
    /// `.headers` was given (`.mode column` then keeps that choice).
    headers_set: bool,
}

const ON_OFF: &[&str] = &["on", "off"];

/// Completion rules of sqlite3's dot commands (on top of the SQL ones).
const SQLITE_RULES: &[Rule] = &[
    rule(&[".SCHEMA"], Offer::Tables),
    rule(&[".INDEXES"], Offer::Tables),
    rule(&[".INDICES"], Offer::Tables),
    rule(&[".TABLES"], Offer::Tables),
    rule(&[".MODE"], Offer::Values(&["box", "column", "csv", "json", "line", "list", "table"])),
    rule(&[".HEADERS"], Offer::Values(ON_OFF)),
    rule(&[".HEADER"], Offer::Values(ON_OFF)),
    rule(&[".TIMER"], Offer::Values(ON_OFF)),
    rule(&[".IMPORT"], Offer::Nothing),
    rule(&[".IMPORT", "*"], Offer::Tables),
    rule(&[".READ"], Offer::Nothing),
    rule(&[".OUTPUT"], Offer::Nothing),
    rule(&[".ONCE"], Offer::Nothing),
    rule(&[".NULLVALUE"], Offer::Nothing),
];

const HELP: &str = "\
.databases               List names and files of attached databases
.exit, .quit             Exit this program
.headers on|off          Turn display of headers on or off
.help                    Show this message
.import FILE TABLE       Import data from FILE into TABLE (--csv, --ascii, --skip N, --schema S, -v)
.indexes ?TABLE?         Show names of indexes
.mode MODE               Set output mode: box column csv json line list table
.nullvalue STRING        Use STRING in place of NULL values
.once FILE               Output for the next SQL command only to FILE ('|cmd' for a pipe)
.output ?FILE?           Send output to FILE ('|cmd' for a pipe), or to stdout
.read FILE               Read input from FILE
.schema ?PATTERN?        Show the CREATE statements matching PATTERN
.tables ?PATTERN?        List names of tables matching LIKE pattern
.timer on|off            Turn SQL timer on or off";

fn on_off(arg: &str) -> Result<bool> {
    match arg.to_ascii_lowercase().as_str() {
        "on" | "yes" | "1" | "true" => Ok(true),
        "off" | "no" | "0" | "false" => Ok(false),
        _ => bail!("ERROR: Not a boolean value: \"{arg}\". Assuming \"no\"."),
    }
}

/// A dot command's arguments: words, with '…' or "…" grouping.
fn dot_args(arg: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = arg.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        let mut w = String::new();
        if c == '\'' || c == '"' {
            chars.next();
            for ch in chars.by_ref() {
                if ch == c {
                    break;
                }
                w.push(ch);
            }
        } else {
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() {
                    break;
                }
                w.push(ch);
                chars.next();
            }
        }
        out.push(w);
    }
    out
}

/// A string as sqlite3 shows it in messages: `\n`, `\t`, `\r`, other control
/// characters in octal.
fn c_string(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\n' => "\\n".to_string(),
            '\t' => "\\t".to_string(),
            '\r' => "\\r".to_string(),
            '"' => "\\\"".to_string(),
            '\\' => "\\\\".to_string(),
            c if (c as u32) < 0x20 => format!("\\{:03o}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

fn v_strings(v: &Value) -> Vec<String> {
    v.as_array().into_iter().flatten().filter_map(|m| m.as_str().map(str::to_string)).collect()
}

impl Sqlite {
    /// sqlite3 starts a terminal session in a table layout, and a script in `list`
    /// mode without headers.
    pub fn new(interactive: bool) -> Self {
        Self {
            buf: SqlBuf::new(Dialect::Sqlite),
            mode: if interactive { Mode::Box } else { Mode::List },
            headers: interactive,
            timer: false,
            null: String::new(),
            tables: Vec::new(),
            columns: Vec::new(),
            once: None,
            headers_set: false,
        }
    }

    /// The current mode, when it can be written a row at a time.
    fn stream_format(&self) -> Option<StreamFormat> {
        Some(match self.mode {
            Mode::List => StreamFormat::SqliteList { separator: "|".into(), null: self.null.clone(), header: self.headers },
            Mode::Csv => StreamFormat::SqliteCsv { header: self.headers },
            Mode::Line => StreamFormat::SqliteLine,
            Mode::Json => StreamFormat::SqliteJson,
            Mode::Column | Mode::Box | Mode::Table => return None,
        })
    }

    fn print(&self, columns: &[String], rows: &[Vec<Value>]) {
        if let Some(f) = self.stream_format() {
            super::out::text(&crate::textfmt::render(&f, columns, &[], rows));
            return;
        }
        let null = self.null.as_str();
        let fmt = |v: &Value| table::cell(v, null);
        let out = match self.mode {
            Mode::Box => table::table(columns, rows, Style::Box, true, &fmt),
            Mode::Table => table::table(columns, rows, Style::Mysql, true, &fmt),
            _ => table::table(columns, rows, Style::Column, self.headers, &fmt),
        };
        if !out.is_empty() {
            outln!("{out}");
        }
    }

    fn execute(&mut self, sql: &str, s: &mut Session) -> Result<()> {
        match self.once.take() {
            Some(to) => super::out::with_target(&to, || self.run(sql, s))?,
            None => self.run(sql, s),
        }
    }

    /// `.import [OPTIONS] FILE TABLE`, with sqlite3's options.
    fn import(&mut self, arg: &str, s: &mut Session) -> Result<()> {
        use crate::db::import::ImportMode;
        let usage = "Usage: .import [--csv|--ascii] [--skip N] [--schema S] [-v] FILE TABLE";
        let words = dot_args(arg);
        let (mut mode, mut skip, mut schema, mut verbose) = (None, 0u64, None, false);
        let mut rest = Vec::new();
        let mut it = words.into_iter();
        while let Some(w) = it.next() {
            match w.as_str() {
                "--csv" => mode = Some(ImportMode::Csv),
                "--ascii" => mode = Some(ImportMode::Ascii),
                "--skip" => skip = it.next().and_then(|n| n.parse().ok()).context(usage)?,
                "--schema" => schema = Some(it.next().context(usage)?),
                "-v" => verbose = true,
                o if o.starts_with('-') && o.len() > 1 => bail!("ERROR: unknown option: \"{o}\".  {usage}"),
                _ => rest.push(w),
            }
        }
        let [file, table] = rest.as_slice() else { bail!("ERROR: missing FILE or TABLE argument.  {usage}") };
        let mode = mode.unwrap_or(if self.mode == Mode::Csv { ImportMode::Csv } else { ImportMode::List { separator: "|".into() } });
        let mode_shown = mode.clone();
        let res = match file.strip_prefix('|') {
            Some(cmd) => {
                let mut child = super::shell_command(cmd).stdout(std::process::Stdio::piped()).spawn().with_context(|| format!("Error: cannot open \"{cmd}\""))?;
                let stdout = child.stdout.take().expect("piped");
                let res = s.import(table, schema, mode, skip, file, std::os::fd::AsFd::as_fd(&stdout));
                drop(stdout);
                child.wait().ok();
                res
            }
            None => {
                let f = std::fs::File::open(super::expand_home(file)).map_err(|_| anyhow::anyhow!("Error: cannot open \"{file}\""))?;
                s.import(table, schema, mode, skip, file, std::os::fd::AsFd::as_fd(&f))
            }
        }?;
        super::out::flush();
        for m in v_strings(&res["messages"]) {
            eprintln!("{m}");
        }
        if verbose {
            let (col, row) = match &mode_shown {
                ImportMode::Csv => (",".to_string(), "\n".to_string()),
                ImportMode::Ascii => ("\x1f".to_string(), "\x1e".to_string()),
                ImportMode::List { separator } => (separator.clone(), "\n".to_string()),
            };
            outln!("Column separator \"{}\", row separator \"{}\"", c_string(&col), c_string(&row));
            outln!(
                "Added {} rows with {} errors using {} lines of input",
                res["rows"].as_u64().unwrap_or(0),
                res["errors"].as_u64().unwrap_or(0),
                res["lines"].as_u64().unwrap_or(0)
            );
        }
        self.refresh(s);
        Ok(())
    }

    fn run(&mut self, sql: &str, s: &mut Session) -> Result<()> {
        // Streamable modes going to a file or pipe: unlimited rows.
        if let Some(format) = self.stream_format() {
            if crate::readonly::returns_rows(crate::readonly::SqlDialect::Sqlite, sql) {
                if let Some(dst) = super::out::bulk_fd()? {
                    let v = s.stream(sql, format, std::os::fd::AsFd::as_fd(&dst))?;
                    if self.timer {
                        outln!("Run Time: real {:.3}", elapsed(&v));
                    }
                    return Ok(());
                }
            }
        }
        let v = s.sql(sql)?;
        for part in QueryResult::from_json(&v).into_parts() {
            match part {
                QueryResult::Table { columns, rows, truncated, .. } => {
                    if !rows.is_empty() {
                        self.print(&columns, &rows);
                    }
                    if truncated {
                        eprintln!("(more rows exist; only the first {} are shown, see --limit)", rows.len());
                    }
                }
                QueryResult::Affected { .. } | QueryResult::Multi(_) => {}
                QueryResult::Docs { docs, .. } => outln!("{}", serde_json::to_string_pretty(&Value::Array(docs))?),
                QueryResult::Value(v) => outln!("{}", serde_json::to_string_pretty(&v)?),
            }
        }
        if self.timer {
            outln!("Run Time: real {:.3}", elapsed(&v));
        }
        Ok(())
    }

    /// Rows of an internal query (5000 max).
    fn rows(&self, s: &mut Session, sql: &str) -> Result<Vec<Vec<Value>>> {
        let v = s.query_limit(crate::ipc::QuerySpec::Sql { sql: sql.into() }, Some(5000))?;
        Ok(match QueryResult::from_json(&v) {
            QueryResult::Table { rows, .. } => rows,
            _ => Vec::new(),
        })
    }

    fn dot(&mut self, cmd: &str, arg: &str, s: &mut Session) -> Result<Flow> {
        let like = |col: &str| if arg.is_empty() { String::new() } else { format!(" AND {col} LIKE {}", quote_lit(arg)) };
        match cmd {
            "quit" | "exit" | "q" => return Ok(Flow::Quit),
            "help" => outln!("{HELP}"),
            "tables" => {
                let sql = format!(
                    "SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'{} ORDER BY 1",
                    like("name")
                );
                let names: Vec<String> = self.rows(s, &sql)?.iter().map(|r| table::cell(&r[0], "")).collect();
                let w = names.iter().map(|n| crate::textfmt::width(n)).max().unwrap_or(0) + 2;
                for chunk in names.chunks((80 / w.max(1)).max(1)) {
                    outln!("{}", chunk.iter().map(|n| crate::textfmt::pad_right(n, w)).collect::<String>().trim_end());
                }
            }
            "schema" => {
                let sql = format!(
                    "SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'{} \
                     ORDER BY tbl_name, type DESC, name",
                    like("tbl_name")
                );
                for r in self.rows(s, &sql)? {
                    outln!("{};", table::cell(&r[0], ""));
                }
            }
            "indexes" | "indices" => {
                let sql = format!("SELECT name FROM sqlite_schema WHERE type = 'index'{} ORDER BY 1", like("tbl_name"));
                for r in self.rows(s, &sql)? {
                    outln!("{}", table::cell(&r[0], ""));
                }
            }
            "databases" => {
                let mode = if s.readonly { "r/o" } else { "r/w" };
                for r in self.rows(s, "SELECT name, file FROM pragma_database_list ORDER BY seq")? {
                    outln!("{}: {} {mode}", table::cell(&r[0], ""), table::cell(&r[1], ""));
                }
            }
            "mode" => match MODES.iter().find(|(n, _)| *n == arg) {
                Some((_, m)) => {
                    self.mode = *m;
                    // As in sqlite3: column mode shows titles unless `.headers off` was given.
                    if self.mode == Mode::Column && !self.headers_set {
                        self.headers = true;
                    }
                }
                None if arg.is_empty() => {
                    let cur = MODES.iter().find(|(_, m)| *m == self.mode).map(|(n, _)| *n).unwrap_or("");
                    outln!("current output mode: {cur}");
                }
                None => bail!("Error: mode should be one of: box column csv json line list table"),
            },
            "headers" | "header" => {
                self.headers = on_off(arg)?;
                self.headers_set = true;
            }
            "timer" => self.timer = on_off(arg)?,
            "nullvalue" => self.null = arg.trim_matches('"').to_string(),
            "read" => {
                let text = read_script(arg)?;
                return run_script(self, s, &text);
            }
            "import" => self.import(arg, s)?,
            "output" | "once" => {
                let words = dot_args(arg);
                if let Some(o) = words.iter().find(|w| w.starts_with('-')) {
                    bail!("ERROR: option \"{o}\" is not available in the vareto shell");
                }
                let to = words.first().cloned();
                if cmd == "once" {
                    self.once = Some(to.context("ERROR: missing FILE argument on .once")?);
                } else {
                    super::out::redirect(to.as_deref().filter(|t| *t != "stdout"))?;
                }
            }
            other => bail!("Error: unknown command or invalid arguments:  \"{other}\". Enter \".help\" for help"),
        }
        Ok(Flow::Continue)
    }
}

impl Flavor for Sqlite {
    fn banner(&self, s: &Session) -> String {
        format!(
            "SQLite version {} (vareto shell){}\nEnter \".help\" for usage hints.\nConnected to {}",
            s.server_version,
            if s.readonly { ", read-only" } else { "" },
            s.host
        )
    }

    fn prompt(&self, _s: &Session) -> String {
        if self.buf.is_empty() { "sqlite> ".into() } else { "   ...> ".into() }
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.buf.is_empty() {
            if let Some(rest) = line.trim_start().strip_prefix('.') {
                let (cmd, arg) = rest.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((rest.trim(), ""));
                return self.dot(cmd, arg, s);
            }
        }
        for (sql, _) in self.buf.push_line(line) {
            self.execute(&sql, s)?;
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        let rest = self.buf.take();
        if !rest.is_empty() {
            self.execute(&rest, s)?;
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn reset(&mut self, _s: &mut Session) {
        self.buf.clear();
    }

    fn error(&self, e: &anyhow::Error) -> String {
        let t = error_text(e);
        if t.starts_with("Error") || t.starts_with("ERROR") { t } else { format!("Error: {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        self.tables = s.list_tables(None, None).map(|v| names(&v)).unwrap_or_default();
        self.columns = s
            .query_limit(
                crate::ipc::QuerySpec::Sql {
                    sql: "SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p \
                          WHERE m.type IN ('table', 'view') ORDER BY m.name, p.cid"
                        .into(),
                },
                Some(COLUMN_LIMIT),
            )
            .map(|v| pairs(&v))
            .unwrap_or_default();
    }

    fn completions(&self) -> Completions {
        let dots = [".tables", ".schema", ".indexes", ".databases", ".mode", ".headers", ".timer", ".read", ".help", ".quit", ".exit", ".nullvalue", ".import", ".output", ".once"];
        Completions {
            sql: true,
            rules: SQLITE_RULES.to_vec(),
            keywords: SQL_KEYWORDS.iter().map(|w| w.to_string()).collect(),
            commands: dots.iter().map(|w| w.to_string()).collect(),
            tables: self.tables.clone(),
            columns: self.columns.clone(),
            ..Completions::default()
        }
    }
}
