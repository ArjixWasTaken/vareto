//! sqlite3.

use anyhow::{bail, Result};
use serde_json::Value;

use super::out::outln;
use super::session::Session;
use super::sqlbuf::{Dialect, SqlBuf};
use super::table::{self, QueryResult, Style};
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
}

const HELP: &str = "\
.databases               List names and files of attached databases
.exit, .quit             Exit this program
.headers on|off          Turn display of headers on or off
.help                    Show this message
.indexes ?TABLE?         Show names of indexes
.mode MODE               Set output mode: box column csv json line list table
.nullvalue STRING        Use STRING in place of NULL values
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

impl Sqlite {
    pub fn new() -> Self {
        Self { buf: SqlBuf::new(Dialect::Sqlite), mode: Mode::Box, headers: true, timer: false, null: String::new(), tables: Vec::new(), columns: Vec::new() }
    }

    fn print(&self, columns: &[String], rows: &[Vec<Value>]) {
        let null = self.null.as_str();
        let fmt = |v: &Value| table::cell(v, null);
        let out = match self.mode {
            Mode::List => {
                let mut lines = Vec::new();
                if self.headers {
                    lines.push(columns.join("|"));
                }
                lines.extend(rows.iter().map(|r| r.iter().map(fmt).collect::<Vec<_>>().join("|")));
                lines.join("\n")
            }
            Mode::Csv => table::csv(columns, rows, self.headers),
            Mode::Column => table::table(columns, rows, Style::Column, self.headers, &fmt),
            Mode::Line => table::line_sqlite(columns, rows),
            Mode::Box => table::table(columns, rows, Style::Box, true, &fmt),
            Mode::Table => table::table(columns, rows, Style::Mysql, true, &fmt),
            Mode::Json => table::json(columns, rows),
        };
        if !out.is_empty() {
            outln!("{out}");
        }
    }

    fn execute(&mut self, sql: &str, s: &mut Session) -> Result<()> {
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
                let w = names.iter().map(|n| n.chars().count()).max().unwrap_or(0) + 2;
                for chunk in names.chunks((80 / w.max(1)).max(1)) {
                    outln!("{}", chunk.iter().map(|n| format!("{n:<w$}")).collect::<String>().trim_end());
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
                Some((_, m)) => self.mode = *m,
                None if arg.is_empty() => {
                    let cur = MODES.iter().find(|(_, m)| *m == self.mode).map(|(n, _)| *n).unwrap_or("");
                    outln!("current output mode: {cur}");
                }
                None => bail!("Error: mode should be one of: box column csv json line list table"),
            },
            "headers" | "header" => self.headers = on_off(arg)?,
            "timer" => self.timer = on_off(arg)?,
            "nullvalue" => self.null = arg.trim_matches('"').to_string(),
            "read" => {
                let text = read_script(arg)?;
                return run_script(self, s, &text);
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

    fn reset(&mut self) {
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
        let dots = [".tables", ".schema", ".indexes", ".databases", ".mode", ".headers", ".timer", ".read", ".help", ".quit", ".exit", ".nullvalue"];
        Completions {
            keywords: SQL_KEYWORDS.iter().map(|w| w.to_string()).collect(),
            commands: dots.iter().map(|w| w.to_string()).collect(),
            tables: self.tables.clone(),
            columns: self.columns.clone(),
            other: Vec::new(),
        }
    }
}
