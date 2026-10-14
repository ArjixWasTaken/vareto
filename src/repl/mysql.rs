//! The mysql / mariadb client.

use anyhow::{bail, Context as _, Result};
use serde_json::Value;

use super::out::{self, outln};
use crate::textfmt::StreamFormat;
use super::session::{Session, Tx};
use super::sqlbuf::{Dialect, End, Mode, Open, SqlBuf};
use super::table::{self, QueryResult, Style};
use super::complete::{rule, Offer, Rule};
use super::{elapsed, error_text, names, pairs, read_script, run_script, Flavor, Flow, Completions, COLUMN_LIMIT, SQL_KEYWORDS};

/// Command-line options of the real client that the shell supports.
#[derive(Clone, Debug, Default)]
pub struct MysqlOptions {
    pub force: bool,
    pub safe_updates: bool,
    pub auto_vertical: bool,
    pub vertical: bool,
    pub batch: bool,
    pub raw: bool,
    pub table: bool,
    pub skip_column_names: bool,
    pub column_type_info: bool,
    pub show_warnings: bool,
    /// `--local-infile[=0|1]`; unset: the client's default (MySQL off, MariaDB on).
    pub local_infile: Option<bool>,
}

/// Character sets `charset` / `SET NAMES` complete to.
const CHARSETS: &[&str] = &["utf8mb4", "utf8mb3", "utf8", "latin1", "ascii", "binary", "ucs2", "utf16", "utf32", "cp1251", "gbk", "big5", "sjis"];

/// Completion rules of the mysql client (on top of the SQL ones).
const MYSQL_RULES: &[Rule] = &[
    rule(&["USE"], Offer::Databases),
    rule(&["\\u"], Offer::Databases),
    rule(&["CONNECT"], Offer::Databases),
    rule(&["\\r"], Offer::Databases),
    rule(&["DATABASE"], Offer::Databases),
    rule(&["SCHEMA"], Offer::Databases),
    rule(&["SHOW", "TABLES", "FROM"], Offer::Databases),
    rule(&["SHOW", "TABLES", "IN"], Offer::Databases),
    rule(&["SHOW", "FULL", "TABLES", "FROM"], Offer::Databases),
    rule(&["SHOW", "FULL", "TABLES", "IN"], Offer::Databases),
    rule(&["SHOW", "TABLE", "STATUS", "FROM"], Offer::Databases),
    rule(&["SHOW", "COLUMNS", "FROM"], Offer::Tables),
    rule(&["SHOW", "FULL", "COLUMNS", "FROM"], Offer::Tables),
    rule(&["SHOW", "FIELDS", "FROM"], Offer::Tables),
    rule(&["SHOW", "INDEX", "FROM"], Offer::Tables),
    rule(&["SHOW", "INDEXES", "FROM"], Offer::Tables),
    rule(&["SHOW", "KEYS", "FROM"], Offer::Tables),
    rule(&["SHOW", "CREATE", "TABLE"], Offer::Tables),
    rule(&["SHOW", "CREATE", "DATABASE"], Offer::Databases),
    rule(&["CHARSET"], Offer::Values(CHARSETS)),
    rule(&["\\C"], Offer::Values(CHARSETS)),
    rule(&["SET", "NAMES"], Offer::Values(CHARSETS)),
    rule(&["SET", "CHARACTER", "SET"], Offer::Values(CHARSETS)),
];

/// How a statement's result is shown.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Table,
    /// `\G` / `ego` / `--vertical`.
    Vertical,
    /// MariaDB's `\S`: no headers or borders.
    Silent,
}

pub struct Mysql {
    buf: SqlBuf,
    opts: MysqlOptions,
    mariadb: bool,
    /// `prompt` / `\R`; the client's default when unset.
    prompt: Option<String>,
    /// Statements run so far (the `\c` prompt escape).
    statements: u64,
    /// The last statement, for `edit` on an empty buffer.
    last: String,
    tee_file: Option<String>,
    pager_cmd: Option<String>,
    tables: Vec<String>,
    columns: Vec<(String, String)>,
    databases: Vec<String>,
}

const HELP: &str = "\
List of all client commands:
Note that all text commands must be first on line and end with ';'
?               (\\?) Synonym for `help'.
charset         (\\C) Switch to another charset.
clear           (\\c) Clear the current input statement.
connect         (\\r) Reconnect to the server. Optional argument is db.
delimiter       (\\d) Set statement delimiter.
edit            (\\e) Edit command with $EDITOR.
ego             (\\G) Send command to server. Display result vertically.
exit            (\\q) Exit. Same as quit.
go              (\\g) Send command to server.
help            (\\h) Display this help.
nopager         (\\n) Disable pager, print to stdout.
notee           (\\t) Don't write into outfile.
nowarning       (\\w) Don't show warnings after every statement.
pager           (\\P) Set PAGER [to_pager]. Print the query results via PAGER.
print           (\\p) Print current command.
prompt          (\\R) Change your mysql prompt.
quit            (\\q) Quit.
rehash          (\\#) Rebuild completion hash.
resetconnection (\\x) Clean session context.
silent          (\\S) Send command to server. Display result without column names or borders.
source          (\\.) Execute an SQL script file. Takes a file name as an argument.
status          (\\s) Get status information from the server.
system          (\\!) Execute a system shell command.
tee             (\\T) Set outfile [to_outfile]. Append everything into given outfile.
use             (\\u) Use another database. Takes database name as argument.
warnings        (\\W) Show warnings after every statement.";

fn plural(n: u64, one: &str) -> String {
    if n == 1 { format!("1 {one}") } else { format!("{n} {one}s") }
}

/// The byte index of the first backslash command in `line` outside quotes and
/// comments, given the context the line starts in (MySQL rules: backslash escapes
/// in '…' and "…", `#` and `-- ` comments, `/* */`).
fn find_command(line: &str, start: &Mode) -> Option<usize> {
    #[derive(PartialEq)]
    enum St {
        Code,
        Quote(char),
        Block,
    }
    let mut st = match start {
        Mode::Quote(q) => St::Quote(*q),
        Mode::BlockComment => St::Block,
        _ => St::Code,
    };
    let b = line.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match st {
            St::Code => match c {
                b'\\' => return Some(i),
                b'\'' | b'"' | b'`' => st = St::Quote(c as char),
                b'#' => return None,
                b'-' if line[i..].starts_with("-- ") || line[i..].trim_end() == "--" => return None,
                b'/' if line[i..].starts_with("/*") => {
                    st = St::Block;
                    i += 1;
                }
                _ => {}
            },
            St::Quote(q) => {
                if c == b'\\' && q != '`' {
                    i += 1;
                } else if c == q as u8 {
                    st = St::Code;
                }
            }
            St::Block => {
                if line[i..].starts_with("*/") {
                    st = St::Code;
                    i += 1;
                }
            }
        }
        i += 1;
    }
    None
}

/// Batch mode escaping: `\n`, `\t`, `\0` and `\` as escapes (unless `--raw`).

impl Mysql {
    pub fn new(s: &mut Session, opts: MysqlOptions) -> Result<Self> {
        if opts.safe_updates {
            s.sql("SET sql_safe_updates=1, sql_select_limit=1000, max_join_size=1000000")?;
        }
        s.show_warnings = opts.show_warnings;
        let mariadb = s.server_version.to_lowercase().contains("mariadb");
        Ok(Self {
            buf: SqlBuf::new(Dialect::Mysql),
            opts,
            mariadb,
            prompt: super::get_env("MYSQL_PS1"),
            statements: 0,
            last: String::new(),
            tee_file: None,
            pager_cmd: None,
            tables: Vec::new(),
            columns: Vec::new(),
            databases: Vec::new(),
        })
    }

    fn default_prompt(&self) -> &'static str {
        if self.mariadb { "\\N [\\d]> " } else { "mysql> " }
    }

    /// The prompt escapes of mysql and mariadb (`\d`, `\u`, `\h`, `\T`, `\N`, ...).
    fn expand_prompt(&self, p: &str, s: &Session) -> String {
        use chrono::{Datelike, Timelike};
        let now = chrono::Local::now();
        let host = s.host.split(" (").next().unwrap_or(&s.host).to_string();
        let (hostname, port) = match host.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => (h.to_string(), p.to_string()),
            _ => (host.clone(), String::new()),
        };
        let user = s.user.clone().unwrap_or_default();
        let mut out = String::new();
        let mut chars = p.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            let Some(e) = chars.next() else {
                out.push('\\');
                break;
            };
            match e {
                'C' => out.push_str(&s.backend_id.map(|i| i.to_string()).unwrap_or_default()),
                'c' => out.push_str(&self.statements.to_string()),
                'D' => out.push_str(&now.format("%a %b %e %H:%M:%S %Y").to_string()),
                'd' => out.push_str(s.database.as_deref().unwrap_or("(none)")),
                'h' => out.push_str(&hostname),
                'l' => out.push_str(&self.buf.delimiter),
                'm' => out.push_str(&format!("{:02}", now.minute())),
                'N' => out.push_str(if self.mariadb { "MariaDB" } else { "mysql" }),
                'n' => out.push('\n'),
                'O' => out.push_str(&now.format("%b").to_string()),
                'o' => out.push_str(&now.month().to_string()),
                'P' => out.push_str(&now.format("%P").to_string()),
                'p' => out.push_str(&port),
                'R' => out.push_str(&now.hour().to_string()),
                'r' => out.push_str(&format!("{:02}", now.hour12().1)),
                'S' => out.push(';'),
                's' => out.push_str(&format!("{:02}", now.second())),
                'T' => out.push_str(if s.tx == Tx::Idle { "" } else { "*" }),
                't' => out.push('\t'),
                'U' => out.push_str(&format!("{user}@{hostname}")),
                'u' => out.push_str(&user),
                'v' => out.push_str(&s.server_version),
                'w' => out.push_str(&now.format("%a").to_string()),
                'Y' => out.push_str(&now.year().to_string()),
                'y' => out.push_str(&format!("{:02}", now.year() % 100)),
                '_' | ' ' => out.push(' '),
                other => out.push(other),
            }
        }
        out
    }

    /// Run the buffered statement (`go`, `\g`, `\G`, `\S`).
    fn send_buffer(&mut self, shape: Shape, s: &mut Session) -> Result<()> {
        let sql = self.buf.take();
        if sql.is_empty() {
            bail!("No query specified");
        }
        self.execute(&sql, shape, s)
    }

    /// A long-form command (`pager less`), which must start the line.
    fn long_command(&mut self, line: &str, s: &mut Session) -> Option<Result<Flow>> {
        let t = line.trim().trim_end_matches(';').trim();
        let (cmd, arg) = t.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((t, ""));
        // `delimiter`'s argument is taken literally: `delimiter ;` sets `;`.
        let raw_arg = line.trim().split_once(char::is_whitespace).map(|(_, a)| a.trim()).unwrap_or("");
        let short = match cmd.to_lowercase().as_str() {
            "quit" | "exit" => 'q',
            "help" | "?" => 'h',
            "use" if !arg.is_empty() => 'u',
            "status" => 's',
            "source" if !arg.is_empty() => '.',
            "delimiter" if !raw_arg.is_empty() => 'd',
            "clear" => 'c',
            "connect" => 'r',
            "edit" => 'e',
            "ego" => 'G',
            "go" => 'g',
            "nopager" => 'n',
            "notee" => 't',
            "nowarning" => 'w',
            "pager" => 'P',
            "print" => 'p',
            "prompt" => 'R',
            "rehash" => '#',
            "resetconnection" => 'x',
            "silent" => 'S',
            "system" => '!',
            "tee" => 'T',
            "warnings" => 'W',
            "charset" if !arg.is_empty() => 'C',
            _ => return None,
        };
        let arg = if short == 'd' { raw_arg } else { arg };
        Some(self.command(short, arg, s))
    }

    /// A client command by its short letter (`\G`, `\u db`, ...).
    fn command(&mut self, c: char, arg: &str, s: &mut Session) -> Result<Flow> {
        match c {
            'q' => return Ok(Flow::Quit),
            'h' | '?' => outln!("{HELP}\n"),
            'g' => self.send_buffer(Shape::Table, s)?,
            'G' => self.send_buffer(Shape::Vertical, s)?,
            'S' => self.send_buffer(Shape::Silent, s)?,
            'c' => self.buf.clear(),
            'p' => {
                let b = self.buf.take();
                outln!("--------------\n{}\n--------------\n", if b.is_empty() { &self.last } else { &b });
                self.buf.push_line(&b);
            }
            'u' => {
                let db = arg.split_whitespace().next().context("USE must be followed by a database name")?.trim_matches('`');
                s.use_db(db)?;
                outln!("Database changed");
            }
            's' => self.status(s)?,
            '.' => {
                let text = read_script(arg)?;
                return run_script(self, s, &text);
            }
            'd' => {
                if arg.is_empty() || arg.contains('\\') {
                    bail!("DELIMITER must be followed by a 'delimiter' character or string");
                }
                self.buf.delimiter = arg.trim_matches(|c| c == '\'' || c == '"' || c == '`').to_string();
            }
            'e' => {
                let b = self.buf.take();
                let text = super::edit::edit_text(if b.is_empty() { &self.last } else { &b }, "sql")?;
                self.buf.clear();
                for line in text.trim_end().lines() {
                    self.buf.push_line(line);
                }
            }
            'P' => {
                let cmd = if arg.is_empty() { super::get_env("PAGER").unwrap_or_else(|| "less".into()) } else { arg.to_string() };
                outln!("PAGER set to '{cmd}'");
                out::set_pager(out::Pager::Always, Some(cmd.clone()));
                self.pager_cmd = Some(cmd);
            }
            'n' => {
                out::set_pager(out::Pager::Off, None);
                self.pager_cmd = None;
                outln!("PAGER set to stdout");
            }
            'T' => {
                let file = if arg.is_empty() { self.tee_file.clone().context("No previous outfile available, you must give a filename!")? } else { arg.to_string() };
                out::tee(Some(&file))?;
                outln!("Logging to file '{file}'");
                self.tee_file = Some(file);
            }
            't' => {
                out::tee(None)?;
                outln!("Outfile disabled.");
            }
            'W' => {
                s.show_warnings = true;
                outln!("Show warnings enabled.");
            }
            'w' => {
                s.show_warnings = false;
                outln!("Show warnings disabled.");
            }
            '!' => {
                out::flush();
                let cmd = if arg.is_empty() { super::get_env("SHELL").unwrap_or_else(|| "/bin/sh".into()) } else { arg.to_string() };
                super::shell_command(&cmd).status().with_context(|| format!("could not run {cmd}"))?;
            }
            'C' => {
                let cs = arg.split_whitespace().next().context("Charset must be given")?;
                s.sql(&format!("SET NAMES {}", cs.trim_matches(|c| c == '\'' || c == '"' || c == '`')))?;
                outln!("Charset changed");
            }
            'r' => {
                let mut words = arg.split_whitespace();
                let db = words.next();
                if words.next().is_some() {
                    bail!("the host is part of the vareto connection settings; use another connection instead");
                }
                s.reset()?;
                if let Some(db) = db {
                    s.use_db(db.trim_matches('`'))?;
                }
                outln!(
                    "Connection id:    {}\nCurrent database: {}\n",
                    s.backend_id.map(|i| i.to_string()).unwrap_or_default(),
                    s.database.as_deref().unwrap_or("*** NONE ***")
                );
            }
            'x' => s.reset()?,
            '#' => self.refresh(s),
            'R' => {
                if arg.is_empty() {
                    self.prompt = None;
                    outln!("Returning to default PROMPT of {}", self.default_prompt());
                } else {
                    self.prompt = Some(arg.to_string());
                    outln!("PROMPT set to '{arg}'");
                }
            }
            other => bail!("Unknown command '\\{other}'."),
        }
        Ok(Flow::Continue)
    }

    fn status(&mut self, s: &mut Session) -> Result<()> {
        let one = |s: &mut Session, sql: &str| -> Result<Vec<Value>> {
            let v = s.sql(sql)?;
            Ok(v["rows"][0].as_array().cloned().unwrap_or_default())
        };
        let r = one(
            s,
            "SELECT CONNECTION_ID(), DATABASE(), CURRENT_USER(), @@version, @@version_comment, @@protocol_version, \
             @@character_set_server, @@character_set_database, @@character_set_client, @@character_set_connection, @@port",
        )?;
        let txt = |i: usize| r.get(i).map(|v| table::cell(v, "")).unwrap_or_default();
        let ssl = one(s, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").ok().and_then(|r| r.get(1).map(|v| table::cell(v, ""))).unwrap_or_default();
        let global = s.sql(
            "SHOW GLOBAL STATUS WHERE Variable_name IN \
             ('Uptime', 'Threads_connected', 'Questions', 'Slow_queries', 'Opened_tables', 'Open_tables')",
        )?;
        let stat = |name: &str| {
            global["rows"].as_array().into_iter().flatten().find(|r| r[0] == name).map(|r| table::cell(&r[1], "")).unwrap_or_default()
        };
        let uptime: u64 = stat("Uptime").parse().unwrap_or(0);
        let questions: f64 = stat("Questions").parse().unwrap_or(0.0);
        let (d, h, m, sec) = (uptime / 86400, uptime % 86400 / 3600, uptime % 3600 / 60, uptime % 60);
        let mut up = String::new();
        for (n, unit) in [(d, "day"), (h, "hour"), (m, "min")] {
            if n > 0 || !up.is_empty() {
                up.push_str(&format!("{n} {}{} ", unit, if n != 1 && unit != "min" { "s" } else { "" }));
            }
        }
        up.push_str(&format!("{sec} sec"));
        let line = "--------------";
        let name = if self.mariadb { "MariaDB" } else { "mysql" };
        outln!("{line}");
        outln!("{name}  vareto shell {}\n", crate::ipc::VERSION);
        outln!("Connection id:\t\t{}", txt(0));
        outln!("Current database:\t{}", txt(1));
        outln!("Current user:\t\t{}", txt(2));
        outln!("SSL:\t\t\t{}", if ssl.is_empty() { "Not in use".into() } else { format!("Cipher in use is {ssl}") });
        outln!("Current pager:\t\t{}", self.pager_cmd.as_deref().unwrap_or("stdout"));
        outln!("Using outfile:\t\t'{}'", self.tee_file.as_deref().unwrap_or(""));
        outln!("Using delimiter:\t{}", self.buf.delimiter);
        outln!("Server:\t\t\t{}", if self.mariadb { "MariaDB" } else { "MySQL" });
        outln!("Server version:\t\t{} {}", txt(3), txt(4));
        outln!("Protocol version:\t{}", txt(5));
        outln!("Connection:\t\t{} (vareto connection \"{}\")", s.host, s.connection);
        outln!("Server characterset:\t{}", txt(6));
        outln!("Db     characterset:\t{}", txt(7));
        outln!("Client characterset:\t{}", txt(8));
        outln!("Conn.  characterset:\t{}", txt(9));
        outln!("TCP port:\t\t{}", txt(10));
        outln!("Read-only:\t\t{}", if s.readonly { "yes" } else { "no" });
        outln!("Uptime:\t\t\t{up}\n");
        outln!(
            "Threads: {}  Questions: {}  Slow queries: {}  Opens: {}  Open tables: {}  Queries per second avg: {:.3}",
            stat("Threads_connected"),
            stat("Questions"),
            stat("Slow_queries"),
            stat("Opened_tables"),
            stat("Open_tables"),
            if uptime > 0 { questions / uptime as f64 } else { 0.0 }
        );
        outln!("{line}\n");
        Ok(())
    }

    fn execute(&mut self, sql: &str, shape: Shape, s: &mut Session) -> Result<()> {
        self.last = sql.to_string();
        self.statements += 1;
        if let Some(Ok(load)) = crate::db::load_data::parse(sql) {
            if load.local {
                return self.load_data(sql, &load.file, s);
            }
        }
        if let Some(v) = self.stream_to_output(sql, shape, s)? {
            // The rows went straight to the output.
            self.print_warning_list(&v);
            return Ok(());
        }
        let v = s.sql(sql)?;
        let secs = elapsed(&v);
        let warnings = v["warnings"]["count"].as_u64().unwrap_or(0);
        // A CALL (or several statements at once) prints one result per statement;
        // the warnings belong to the last one.
        let parts = QueryResult::from_json(&v).into_parts();
        let n = parts.len();
        for (i, part) in parts.into_iter().enumerate() {
            self.print_result(part, shape, secs, if i + 1 == n { warnings } else { 0 })?;
        }
        self.print_warning_list(&v);
        Ok(())
    }

    fn print_warning_list(&self, v: &Value) {
        if let Some(list) = v["warnings"]["list"].as_array().filter(|l| !l.is_empty()) {
            for w in list {
                outln!("{} (Code {}): {}", w["level"].as_str().unwrap_or("Warning"), w["code"], w["message"].as_str().unwrap_or(""));
            }
            outln!();
        }
    }

    /// `LOAD DATA LOCAL INFILE`: the daemon reads the file this process opens and
    /// inserts its records ([`crate::db::load_data`]).
    fn load_data(&mut self, sql: &str, path: &str, s: &mut Session) -> Result<()> {
        // A file that can't be opened is reported after the server's checks, as the
        // real client does; the daemon gets an empty stand-in.
        let (file, file_error) = match std::fs::File::open(path) {
            Ok(f) => (f, None),
            Err(e) => {
                let desc = e.to_string();
                let desc = desc.split(" (os error").next().unwrap_or(&desc).to_string();
                let msg = format!("2 (HY000): File '{path}' not found (OS errno {} - {desc})", e.raw_os_error().unwrap_or(0));
                (std::fs::File::open("/dev/null")?, Some(msg))
            }
        };
        let client_allowed = self.opts.local_infile.unwrap_or(self.mariadb);
        let v = s.load_data(sql, std::os::fd::AsFd::as_fd(&file), client_allowed, file_error)?;
        let secs = elapsed(&v);
        let warnings = v["warnings"]["count"].as_u64().unwrap_or(0);
        if !(self.opts.batch && !self.opts.table) {
            let warn = if warnings > 0 { format!(", {}", plural(warnings, "warning")) } else { String::new() };
            outln!("Query OK, {} affected{warn} ({secs:.2} sec)", plural(v["affected"].as_u64().unwrap_or(0), "row"));
            outln!(
                "Records: {}  Deleted: {}  Skipped: {}  Warnings: {warnings}\n",
                v["records"].as_u64().unwrap_or(0),
                v["deleted"].as_u64().unwrap_or(0),
                v["skipped"].as_u64().unwrap_or(0)
            );
        }
        self.print_warning_list(&v);
        Ok(())
    }

    /// `--batch` output to a file or pipe: the daemon writes the rows there, with no
    /// row limit, as `mysql -B` does. `None` when the statement or the settings
    /// don't allow it.
    fn stream_to_output(&self, sql: &str, shape: Shape, s: &mut Session) -> Result<Option<Value>> {
        let o = &self.opts;
        let batch = o.batch && !o.table && !o.vertical && !o.auto_vertical && !o.column_type_info;
        if !batch || shape != Shape::Table || !crate::readonly::returns_rows(crate::readonly::SqlDialect::Mysql, sql) {
            return Ok(None);
        }
        let Some(dst) = out::bulk_fd()? else { return Ok(None) };
        let format = StreamFormat::MysqlBatch { raw: o.raw, header: !o.skip_column_names, mariadb: self.mariadb };
        s.stream(sql, format, std::os::fd::AsFd::as_fd(&dst)).map(Some)
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
                if let Some(hex) = crate::textfmt::mysql_binary(cell, self.mariadb) {
                    *cell = hex;
                }
            }
        }
    }

    fn print_result(&self, r: QueryResult, shape: Shape, secs: f64, warnings: u64) -> Result<()> {
        let warn = if warnings > 0 { format!(", {}", plural(warnings, "warning")) } else { String::new() };
        // Batch mode (-B, without -t) prints rows only: tab-separated, no footers.
        let batch = self.opts.batch && !self.opts.table;
        match r {
            QueryResult::Table { columns, types, mut rows, truncated } => {
                self.binary_cells(&types, &mut rows);
                if self.opts.column_type_info {
                    for (i, (c, t)) in columns.iter().zip(&types).enumerate() {
                        outln!("Field {:>3}:  `{c}`\nType:       {}\n", i + 1, t.as_deref().unwrap_or("?"));
                    }
                }
                if rows.is_empty() {
                    if !batch {
                        outln!("Empty set{warn} ({secs:.2} sec)\n");
                    }
                    return Ok(());
                }
                let headers = !self.opts.skip_column_names;
                let vertical = shape == Shape::Vertical
                    || self.opts.vertical
                    || (self.opts.auto_vertical && {
                        let t = table::table(&columns, &rows, Style::Mysql, headers, &|v| table::cell(v, "NULL"));
                        t.lines().next().map(crate::textfmt::width).unwrap_or(0) > out::width()
                    });
                let text = if vertical {
                    table::vertical_mysql(&columns, &rows)
                } else if batch || shape == Shape::Silent {
                    // Binary cells are hex already.
                    let f = StreamFormat::MysqlBatch { raw: self.opts.raw, header: headers && shape != Shape::Silent, mariadb: self.mariadb };
                    let mut text = crate::textfmt::render(&f, &columns, &[], &rows);
                    text.pop();
                    text
                } else {
                    table::table(&columns, &rows, Style::Mysql, headers, &|v| table::cell(v, "NULL"))
                };
                outln!("{text}");
                if !batch && shape != Shape::Silent {
                    outln!("{} in set{warn} ({secs:.2} sec)", plural(rows.len() as u64, "row"));
                    if truncated {
                        outln!("(more rows exist; only the first {} are shown, see --limit)", rows.len());
                    }
                    outln!();
                }
            }
            QueryResult::Affected { rows, .. } => {
                if !batch {
                    outln!("Query OK, {} affected{warn} ({secs:.2} sec)\n", plural(rows, "row"));
                }
            }
            QueryResult::Docs { docs, .. } => outln!("{}\n", serde_json::to_string_pretty(&Value::Array(docs))?),
            QueryResult::Value(v) => outln!("{}\n", serde_json::to_string_pretty(&v)?),
            QueryResult::Multi(parts) => {
                for p in parts {
                    self.print_result(p, shape, secs, 0)?;
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
            None => self.expand_prompt(self.prompt.as_deref().unwrap_or(self.default_prompt()), s),
        }
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.buf.is_empty() {
            if let Some(r) = self.long_command(line, s) {
                return r;
            }
        }
        // Short commands (`\G`, `\p`, `\u db`, ...) can come anywhere outside
        // quotes; those with an argument take the rest of the line.
        let mut rest = line;
        loop {
            let Some(at) = find_command(rest, &self.buf.mode()) else {
                for (sql, end) in self.buf.push_line(rest) {
                    let shape = if end == End::Vertical { Shape::Vertical } else { Shape::Table };
                    self.execute(&sql, shape, s)?;
                }
                return Ok(Flow::Continue);
            };
            let (before, after) = (&rest[..at], &rest[at + 1..]);
            if !before.trim().is_empty() {
                for (sql, end) in self.buf.push_line(before) {
                    let shape = if end == End::Vertical { Shape::Vertical } else { Shape::Table };
                    self.execute(&sql, shape, s)?;
                }
            }
            let Some(c) = after.chars().next() else { return Ok(Flow::Continue) };
            let tail = &after[c.len_utf8()..];
            if matches!(c, 'u' | '.' | 'P' | 'T' | 'C' | 'r' | 'R' | '!' | 'd') {
                return self.command(c, tail.trim().trim_end_matches(';').trim(), s);
            }
            if let Flow::Quit = self.command(c, "", s)? {
                return Ok(Flow::Quit);
            }
            rest = tail;
        }
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        // In batch mode the last statement doesn't need a delimiter.
        let rest = self.buf.take();
        if !rest.is_empty() {
            self.execute(&rest, Shape::Table, s)?;
        }
        Ok(())
    }

    fn stop_on_error(&self) -> bool {
        !self.opts.force
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn reset(&mut self, _s: &mut Session) {
        self.buf.clear();
    }

    fn error(&self, e: &anyhow::Error) -> String {
        let t = error_text(e);
        // Server errors carry their code: `ERROR 1146 (42S02): Table ...`.
        if t.starts_with(|c: char| c.is_ascii_digit()) { format!("ERROR {t}") } else { format!("ERROR: {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        self.tables = s.list_tables(s.database.clone(), None).map(|v| names(&v)).unwrap_or_default();
        self.columns = if s.database.is_some() {
            s.query_limit(
                crate::ipc::QuerySpec::Sql {
                    sql: "SELECT table_name, column_name FROM information_schema.columns \
                          WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position"
                        .into(),
                },
                Some(COLUMN_LIMIT),
            )
            .map(|v| pairs(&v))
            .unwrap_or_default()
        } else {
            Vec::new()
        };
        self.databases = s.list_databases().map(|v| names(&v)).unwrap_or_default();
    }

    fn completions(&self) -> Completions {
        let cmds = [
            "quit", "exit", "help", "status", "source", "delimiter", "use", "clear", "connect", "edit", "ego", "go", "nopager",
            "notee", "nowarning", "pager", "print", "prompt", "rehash", "resetconnection", "system", "tee", "warnings", "charset",
        ];
        Completions {
            sql: true,
            dialect: Some(crate::readonly::SqlDialect::Mysql),
            rules: MYSQL_RULES.to_vec(),
            keywords: SQL_KEYWORDS.iter().map(|w| w.to_string()).collect(),
            commands: cmds.iter().map(|w| w.to_string()).collect(),
            tables: self.tables.clone(),
            columns: self.columns.clone(),
            databases: self.databases.clone(),
            ..Completions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_in_lines() {
        assert_eq!(find_command("select 1 \\G", &Mode::Code), Some(9));
        assert_eq!(find_command("select '\\G' \\g", &Mode::Code), Some(12));
        assert_eq!(find_command("select \"a\\\"b\" # \\G", &Mode::Code), None);
        assert_eq!(find_command("x' \\p", &Mode::Quote('\'')), Some(3));
        assert_eq!(find_command("select 1 -- \\G", &Mode::Code), None);
        assert_eq!(find_command("select 1 /* \\G */ \\p", &Mode::Code), Some(18));
    }

    #[test]
    fn batch_escaping() {
        use crate::textfmt::batch_cell;
        assert_eq!(batch_cell(&Value::String("a\tb\nc\\".into()), false), "a\\tb\\nc\\\\");
        assert_eq!(batch_cell(&Value::String("a\tb".into()), true), "a\tb");
        assert_eq!(batch_cell(&Value::Null, false), "NULL");
    }
}
