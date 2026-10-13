//! The mysql / mariadb client.

use anyhow::Result;
use serde_json::Value;

use super::out::outln;
use super::session::Session;
use super::sqlbuf::{Dialect, End, Open, SqlBuf};
use super::table::{self, QueryResult, Style};
use super::{elapsed, error_text, names, read_script, run_script, Flavor, Flow, SQL_KEYWORDS};

pub struct Mysql {
    buf: SqlBuf,
    /// `--force`: keep going after errors in a script.
    force: bool,
    mariadb: bool,
    tables: Vec<String>,
}

const HELP: &str = "\
List of all client commands:
Note that all text commands must be first on line and end with ';'
?         (\\?) Synonym for `help'.
clear     (\\c) Clear the current input statement.
delimiter (\\d) Set statement delimiter.
ego       (\\G) Send command to server. Display result vertically.
exit      (\\q) Exit. Same as quit.
go        (\\g) Send command to server.
help      (\\h) Display this help.
quit      (\\q) Quit.
source    (\\.) Execute an SQL script file. Takes a file name as an argument.
status    (\\s) Get status information from the server.
use       (\\u) Use another database. Takes database name as argument.";

fn plural(n: usize, one: &str) -> String {
    if n == 1 { format!("1 {one}") } else { format!("{n} {one}s") }
}

impl Mysql {
    pub fn new(s: &Session, opts: &super::Options) -> Self {
        Self { buf: SqlBuf::new(Dialect::Mysql), force: opts.force, mariadb: s.server_version.to_lowercase().contains("mariadb"), tables: Vec::new() }
    }

    /// A client command at the start of a line, if it is one.
    fn command(&mut self, line: &str, s: &mut Session) -> Option<Result<Flow>> {
        let t = line.trim().trim_end_matches(';').trim();
        let (cmd, arg) = t.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((t, ""));
        let out = match cmd.to_lowercase().as_str() {
            "quit" | "exit" | "\\q" => Ok(Flow::Quit),
            "help" | "\\h" | "?" | "\\?" => {
                outln!("{HELP}\n");
                Ok(Flow::Continue)
            }
            "\\u" | "use" if !arg.is_empty() => {
                let db = arg.trim_matches('`');
                s.use_db(db).map(|()| {
                    outln!("Database changed");
                    Flow::Continue
                })
            }
            "status" | "\\s" => {
                self.status(s);
                Ok(Flow::Continue)
            }
            "source" | "\\." if !arg.is_empty() => read_script(arg).and_then(|text| run_script(self, s, &text)),
            "delimiter" | "\\d" if !arg.is_empty() => {
                self.buf.delimiter = arg.to_string();
                Ok(Flow::Continue)
            }
            _ => return None,
        };
        Some(out)
    }

    fn status(&self, s: &Session) {
        let line = "--------------";
        outln!("{line}");
        outln!("Connection:\t\t{} (vareto connection {:?})", s.host, s.connection);
        outln!("Current database:\t{}", s.database.as_deref().unwrap_or(""));
        outln!("Current user:\t\t{}", s.user.as_deref().unwrap_or(""));
        outln!("Server version:\t\t{}", s.server_version);
        outln!("Read-only:\t\t{}", if s.readonly { "yes" } else { "no" });
        outln!("Statement delimiter:\t{}", self.buf.delimiter);
        outln!("{line}\n");
    }

    fn execute(&mut self, sql: &str, end: End, s: &mut Session) -> Result<()> {
        let v = s.sql(sql)?;
        let secs = elapsed(&v);
        // A CALL (or several statements at once) prints one result per statement.
        for part in QueryResult::from_json(&v).into_parts() {
            self.print_result(part, end, secs)?;
        }
        Ok(())
    }

    /// Binary values as `0x…` hex. MariaDB's client prints the raw bytes; text that
    /// is valid UTF-8 stays text there, and anything else is shown as hex too.
    fn binary_cells(&self, types: &[Option<String>], rows: &mut [Vec<Value>]) {
        for (i, t) in types.iter().enumerate() {
            if !t.as_deref().is_some_and(table::is_binary_type) {
                continue;
            }
            for row in rows.iter_mut() {
                let Some(cell) = row.get_mut(i) else { continue };
                if self.mariadb && cell.as_str().is_some_and(|s| !s.starts_with("base64:")) {
                    continue;
                }
                if let Some(bytes) = table::value_bytes(cell) {
                    *cell = Value::String(table::hex_bytes(&bytes));
                }
            }
        }
    }

    fn print_result(&self, r: QueryResult, end: End, secs: f64) -> Result<()> {
        match r {
            QueryResult::Table { columns, types, mut rows, truncated } => {
                if rows.is_empty() {
                    outln!("Empty set ({secs:.2} sec)\n");
                    return Ok(());
                }
                self.binary_cells(&types, &mut rows);
                let out = match end {
                    End::Vertical => table::vertical_mysql(&columns, &rows),
                    End::Normal => table::table(&columns, &rows, Style::Mysql, true, &|v| table::cell(v, "NULL")),
                };
                outln!("{out}");
                outln!("{} in set ({secs:.2} sec)", plural(rows.len(), "row"));
                if truncated {
                    outln!("(more rows exist; only the first {} are shown, see --limit)", rows.len());
                }
                outln!();
            }
            QueryResult::Affected { rows, .. } => outln!("Query OK, {} affected ({secs:.2} sec)\n", plural(rows as usize, "row")),
            QueryResult::Docs { docs, .. } => outln!("{}\n", serde_json::to_string_pretty(&Value::Array(docs))?),
            QueryResult::Value(v) => outln!("{}\n", serde_json::to_string_pretty(&v)?),
            QueryResult::Multi(parts) => {
                for p in parts {
                    self.print_result(p, end, secs)?;
                }
            }
        }
        Ok(())
    }
}

impl Flavor for Mysql {
    fn banner(&self, s: &Session) -> String {
        format!(
            "Welcome to the {} monitor (vareto shell). Commands end with ; or \\g.\nServer version: {}{}\n\nType 'help;' or '\\h' for help. Type '\\c' to clear the current input statement.\n",
            if self.mariadb { "MariaDB" } else { "MySQL" },
            s.server_version,
            if s.readonly { "  [read-only]" } else { "" },
        )
    }

    fn prompt(&self, s: &Session) -> String {
        match self.buf.open() {
            Some(Open::Quote(q)) => format!("    {q}> "),
            Some(Open::Comment) => "   /*> ".into(),
            Some(_) => "    -> ".into(),
            None if self.mariadb => format!("MariaDB [{}]> ", s.database.as_deref().unwrap_or("(none)")),
            None => "mysql> ".into(),
        }
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.buf.is_empty() {
            if let Some(r) = self.command(line, s) {
                return r;
            }
        }
        if matches!(self.buf.open(), None | Some(Open::Statement)) && line.trim_end().ends_with("\\c") {
            self.buf.clear();
            return Ok(Flow::Continue);
        }
        for (sql, end) in self.buf.push_line(line) {
            self.execute(&sql, end, s)?;
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        // In batch mode the last statement doesn't need a delimiter.
        let rest = self.buf.take();
        if !rest.is_empty() {
            self.execute(&rest, End::Normal, s)?;
        }
        Ok(())
    }

    fn stop_on_error(&self) -> bool {
        !self.force
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn reset(&mut self) {
        self.buf.clear();
    }

    fn error(&self, e: &anyhow::Error) -> String {
        let t = error_text(e);
        // Server errors carry their code: `ERROR 1146 (42S02): Table ...`.
        if t.starts_with(|c: char| c.is_ascii_digit()) { format!("ERROR {t}") } else { format!("ERROR: {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        self.tables = s.list_tables(s.database.clone(), None).map(|v| names(&v)).unwrap_or_default();
    }

    fn words(&self) -> Vec<String> {
        let cmds = ["quit", "exit", "help", "status", "source", "delimiter", "use"];
        SQL_KEYWORDS.iter().chain(cmds.iter()).map(|w| w.to_string()).chain(self.tables.iter().cloned()).collect()
    }
}
