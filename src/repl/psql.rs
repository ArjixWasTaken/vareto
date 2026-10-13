//! psql.

use anyhow::{bail, Context as _, Result};
use serde_json::Value;

use super::out::outln;
use super::session::{Session, Tx};
use super::sqlbuf::{Dialect, End, Open, SqlBuf};
use super::table::{self, QueryResult, Style};
use super::{elapsed, error_info, error_text, names, read_script, run_script, Flavor, Flow, SQL_KEYWORDS};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Expanded {
    Off,
    On,
    Auto,
}

pub struct Psql {
    buf: SqlBuf,
    /// psql variables (`\set`), including the special ones like ON_ERROR_STOP.
    vars: std::collections::BTreeMap<String, String>,
    /// The statement an error's position refers to (empty for internal queries).
    stmt: String,
    /// The last statement sent, for `\g` on an empty buffer.
    last: String,
    expanded: Expanded,
    timing: bool,
    tables: Vec<String>,
}

const HELP: &str = "\
General
  \\q                     quit psql
  \\g [\\gx]               execute query (with expanded output)
  \\timing [on|off]       toggle timing of commands
  \\x [on|off|auto]       toggle expanded output

Query Buffer
  \\p                     show the contents of the query buffer
  \\r                     reset (clear) the query buffer

Input/Output
  \\echo [STRING]         write string to standard output
  \\i FILE                execute commands from file

Informational
  \\d[S+]                 list tables, views, and sequences
  \\d NAME                describe table, view, sequence, or index
  \\df [PATTERN]          list functions
  \\di [PATTERN]          list indexes
  \\dm [PATTERN]          list materialized views
  \\dn [PATTERN]          list schemas
  \\ds [PATTERN]          list sequences
  \\dt [PATTERN]          list tables
  \\du [PATTERN]          list roles
  \\dv [PATTERN]          list views
  \\l [PATTERN]           list databases

Connection
  \\c [DBNAME]            connect to another database
  \\conninfo              display information about current connection";

/// A string literal (standard_conforming_strings: backslashes are literal).
fn quote_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A psql-style pattern (`*`, `?`, optional `schema.`) as SQL conditions on the
/// given schema and name columns.
fn pattern_sql(pattern: &str, schema_col: &str, name_col: &str, visible: &str) -> String {
    let re = |p: &str| format!("^({})$", regex::escape(p).replace("\\*", ".*").replace("\\?", "."));
    match pattern.split_once('.') {
        Some((sc, name)) => format!("{schema_col} ~ {} AND {name_col} ~ {}", quote_lit(&re(sc)), quote_lit(&re(name))),
        None if pattern.is_empty() => visible.to_string(),
        None => format!("{name_col} ~ {} AND {visible}", quote_lit(&re(pattern))),
    }
}

fn relkinds(cmd: &str) -> Option<(&'static str, &'static str)> {
    // (relkinds, what psql calls them when none are found)
    Some(match cmd {
        "" => ("'r','p','v','m','S','f'", "relations"),
        "t" => ("'r','p'", "tables"),
        "v" => ("'v'", "views"),
        "m" => ("'m'", "materialized views"),
        "i" => ("'i','I'", "indexes"),
        "s" => ("'S'", "sequences"),
        _ => return None,
    })
}

/// A server error the way psql prints it: severity and message, the statement
/// line with a caret under the error position, then DETAIL / HINT / CONTEXT.
pub fn format_error(info: &Value, stmt: &str) -> String {
    let mut out = format!("{}:  {}", info["severity"].as_str().unwrap_or("ERROR"), info["message"].as_str().unwrap_or(""));
    if let (Some(pos), false) = (info["position"].as_u64(), stmt.is_empty()) {
        // 1-based, in characters.
        let before: String = stmt.chars().take(pos.saturating_sub(1) as usize).collect();
        let line_no = before.matches('\n').count() + 1;
        let col = before.rsplit('\n').next().unwrap_or("").chars().count();
        let line = stmt.lines().nth(line_no - 1).unwrap_or("");
        let prefix = format!("LINE {line_no}: ");
        out.push_str(&format!("\n{prefix}{line}\n{}^", " ".repeat(prefix.chars().count() + col)));
    }
    for (key, label) in [("detail", "DETAIL"), ("hint", "HINT"), ("where", "CONTEXT")] {
        if let Some(v) = info[key].as_str() {
            out.push_str(&format!("\n{label}:  {v}"));
        }
    }
    out
}

/// psql's boolean values: on/off, true/false, yes/no, 1/0, case-insensitive, and
/// unique prefixes of the words.
pub fn bool_value(v: &str) -> Option<bool> {
    let v = v.trim().to_ascii_lowercase();
    if v.is_empty() {
        return None;
    }
    let word = |w: &str| w.starts_with(&v);
    if v == "1" || word("true") || word("yes") || (v.len() > 1 && word("on")) {
        Some(true)
    } else if v == "0" || word("false") || word("no") || (v.len() > 1 && word("off")) {
        Some(false)
    } else {
        None
    }
}

/// Meta-command arguments: whitespace-separated, with '…' quoting (`''` and
/// backslash escapes inside).
pub fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.peek().is_none() {
            return out;
        }
        let mut cur = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                break;
            }
            chars.next();
            if c != '\'' {
                cur.push(c);
                continue;
            }
            while let Some(c) = chars.next() {
                match c {
                    '\'' if chars.peek() == Some(&'\'') => {
                        chars.next();
                        cur.push('\'');
                    }
                    '\'' => break,
                    '\\' => match chars.next() {
                        Some('n') => cur.push('\n'),
                        Some('t') => cur.push('\t'),
                        Some(c) => cur.push(c),
                        None => {}
                    },
                    c => cur.push(c),
                }
            }
        }
        out.push(cur);
    }
}

/// The tag psql prints for a statement without rows.
fn command_tag(sql: &str, rows: u64) -> String {
    let words: Vec<String> = sql.split_whitespace().take(2).map(|w| w.trim_end_matches(';').to_uppercase()).collect();
    let first = words.first().map(String::as_str).unwrap_or("");
    match first {
        "INSERT" => format!("INSERT 0 {rows}"),
        "UPDATE" | "DELETE" | "MERGE" | "COPY" | "MOVE" | "FETCH" | "SELECT" => format!("{first} {rows}"),
        "CREATE" | "DROP" | "ALTER" => {
            let second = words.get(1).map(String::as_str).unwrap_or("");
            match second {
                "UNIQUE" => format!("{first} INDEX"),
                "OR" | "TEMP" | "TEMPORARY" | "UNLOGGED" => format!("{first} TABLE"),
                _ => format!("{first} {second}"),
            }
        }
        "START" => "START TRANSACTION".into(),
        "END" => "COMMIT".into(),
        "ABORT" => "ROLLBACK".into(),
        w => w.to_string(),
    }
}

fn term_width() -> usize {
    super::out::width()
}

impl Psql {
    pub fn new(opts: &super::Options) -> Result<Self> {
        let mut vars = std::collections::BTreeMap::new();
        for kv in &opts.set {
            let (k, v) = kv.split_once('=').unwrap_or((kv.as_str(), ""));
            if k.is_empty() {
                bail!("invalid variable assignment {kv:?} (expected NAME=VALUE)");
            }
            vars.insert(k.to_string(), v.to_string());
        }
        // psql pages results taller than the terminal by default.
        super::out::set_pager(super::out::Pager::Auto, std::env::var("PSQL_PAGER").ok());
        Ok(Self::with_vars(vars))
    }

    fn with_vars(vars: std::collections::BTreeMap<String, String>) -> Self {
        Self { buf: SqlBuf::new(Dialect::Postgres), vars, stmt: String::new(), last: String::new(), expanded: Expanded::Off, timing: false, tables: Vec::new() }
    }

    /// A boolean variable, read the way psql does (on/off, true/false, yes/no, 1/0).
    fn flag(&self, name: &str) -> bool {
        self.vars.get(name).is_some_and(|v| bool_value(v).unwrap_or(false))
    }

    fn print_rows(&self, title: Option<&str>, columns: &[String], rows: &[Vec<Value>], vertical: bool) {
        let expanded = vertical
            || self.expanded == Expanded::On
            || (self.expanded == Expanded::Auto && {
                let t = table::table(columns, rows, Style::Psql, true, &table::psql_cell);
                t.lines().map(|l| l.chars().count()).max().unwrap_or(0) > term_width()
            });
        if expanded {
            if rows.is_empty() {
                outln!("(0 rows)");
            } else {
                outln!("{}", table::expanded_psql(columns, rows));
            }
        } else {
            let t = table::table(columns, rows, Style::Psql, true, &table::psql_cell);
            if let Some(title) = title {
                let w = t.lines().map(|l| l.chars().count()).max().unwrap_or(0);
                outln!("{}", format!("{title:^w$}").trim_end());
            }
            outln!("{t}");
            outln!("({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
        }
        outln!();
    }

    fn execute(&mut self, sql: &str, end: End, s: &mut Session) -> Result<()> {
        self.last = sql.to_string();
        self.stmt = sql.to_string();
        let v = s.sql(sql)?;
        for part in QueryResult::from_json(&v).into_parts() {
            match part {
                QueryResult::Table { columns, rows, truncated, .. } => {
                    self.print_rows(None, &columns, &rows, end == End::Vertical);
                    if truncated {
                        outln!("(more rows exist; only the first {} are shown, see --limit)\n", rows.len());
                    }
                }
                QueryResult::Affected { rows, .. } => outln!("{}", command_tag(sql, rows)),
                QueryResult::Docs { docs, .. } => outln!("{}", serde_json::to_string_pretty(&Value::Array(docs))?),
                QueryResult::Value(v) => outln!("{}", serde_json::to_string_pretty(&v)?),
                QueryResult::Multi(_) => {}
            }
        }
        if self.timing {
            outln!("Time: {:.3} ms", elapsed(&v) * 1000.0);
        }
        Ok(())
    }

    /// Run an internal catalog query and print it under `title`.
    fn listing(&mut self, s: &mut Session, title: &str, sql: &str, none: &str) -> Result<()> {
        self.stmt.clear();
        let v = s.query_limit(crate::ipc::QuerySpec::Sql { sql: sql.into() }, Some(5000))?;
        if let QueryResult::Table { columns, rows, .. } = QueryResult::from_json(&v) {
            if rows.is_empty() && !none.is_empty() {
                outln!("{none}");
            } else {
                self.print_rows(Some(title), &columns, &rows, false);
            }
        }
        Ok(())
    }

    fn list_relations(&mut self, s: &mut Session, kind: &str, pattern: &str) -> Result<()> {
        let (kinds, what) = relkinds(kind).expect("checked by caller");
        let index = kind == "i";
        let sql = format!(
            "SELECT n.nspname AS \"Schema\", c.relname AS \"Name\", \
             CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' \
             WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' \
             WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END AS \"Type\", \
             pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\"{} \
             FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace{} \
             WHERE c.relkind IN ({kinds}) AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' \
             AND n.nspname <> 'information_schema' AND {} ORDER BY 1, 2",
            if index { ", c2.relname AS \"Table\"" } else { "" },
            if index {
                " LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid"
            } else {
                ""
            },
            pattern_sql(pattern, "n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)"),
        );
        let none = if pattern.is_empty() { format!("Did not find any {what}.") } else { format!("Did not find any {what} named \"{pattern}\".") };
        self.listing(s, "List of relations", &sql, &none)
    }

    fn describe(&mut self, s: &mut Session, name: &str) -> Result<()> {
        self.stmt.clear();
        let reg = format!("{}::regclass", quote_lit(name));
        let head = s.sql(&format!(
            "SELECT n.nspname, c.relname, c.relkind::text FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = {reg}"
        ))?;
        let QueryResult::Table { rows, .. } = QueryResult::from_json(&head) else { bail!("Did not find any relation named \"{name}\".") };
        let Some(row) = rows.first() else { bail!("Did not find any relation named \"{name}\".") };
        let (schema, rel, kind) = (table::cell(&row[0], ""), table::cell(&row[1], ""), table::cell(&row[2], ""));
        let what = match kind.as_str() {
            "v" => "View",
            "m" => "Materialized view",
            "i" | "I" => "Index",
            "S" => "Sequence",
            "f" => "Foreign table",
            "p" => "Partitioned table",
            _ => "Table",
        };
        let cols = s.sql(&format!(
            "SELECT a.attname AS \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"Type\", \
             (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
              WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS \"Collation\", \
             CASE WHEN a.attnotnull THEN 'not null' ELSE '' END AS \"Nullable\", \
             pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS \"Default\" \
             FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             WHERE a.attrelid = {reg} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        ))?;
        let QueryResult::Table { columns, rows, .. } = QueryResult::from_json(&cols) else { return Ok(()) };
        let t = table::table(&columns, &rows, Style::Psql, true, &table::psql_cell);
        let w = t.lines().map(|l| l.chars().count()).max().unwrap_or(0);
        outln!("{}", format!("{:^w$}", format!("{what} \"{schema}.{rel}\"")).trim_end());
        outln!("{t}");

        let section = |s: &mut Session, title: &str, sql: String, fmt: &dyn Fn(&[Value]) -> String| -> Result<()> {
            if let QueryResult::Table { rows, .. } = QueryResult::from_json(&s.sql(&sql)?) {
                if !rows.is_empty() {
                    outln!("{title}:");
                    for r in &rows {
                        outln!("    {}", fmt(r));
                    }
                }
            }
            Ok(())
        };
        section(
            s,
            "Indexes",
            format!(
                "SELECT c2.relname, i.indisprimary, i.indisunique, pg_catalog.pg_get_indexdef(i.indexrelid) \
                 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid \
                 WHERE i.indrelid = {reg} ORDER BY i.indisprimary DESC, c2.relname"
            ),
            &|r| {
                let def = table::cell(&r[3], "");
                let using = def.split_once(" USING ").map(|(_, u)| u.to_string()).unwrap_or(def);
                let kind = if r[1] == Value::Bool(true) {
                    "PRIMARY KEY, "
                } else if r[2] == Value::Bool(true) {
                    "UNIQUE, "
                } else {
                    ""
                };
                format!("\"{}\" {kind}{using}", table::cell(&r[0], ""))
            },
        )?;
        for (title, kind) in [("Check constraints", "c"), ("Foreign-key constraints", "f")] {
            section(
                s,
                title,
                format!(
                    "SELECT conname, pg_catalog.pg_get_constraintdef(oid, true) FROM pg_catalog.pg_constraint \
                     WHERE conrelid = {reg} AND contype = '{kind}' ORDER BY 1"
                ),
                &|r| format!("\"{}\" {}", table::cell(&r[0], ""), table::cell(&r[1], "")),
            )?;
        }
        section(
            s,
            "Referenced by",
            format!(
                "SELECT conrelid::regclass::text, conname, pg_catalog.pg_get_constraintdef(oid, true) \
                 FROM pg_catalog.pg_constraint WHERE confrelid = {reg} AND contype = 'f' ORDER BY 1, 2"
            ),
            &|r| format!("TABLE \"{}\" CONSTRAINT \"{}\" {}", table::cell(&r[0], ""), table::cell(&r[1], ""), table::cell(&r[2], "")),
        )?;
        outln!();
        Ok(())
    }

    fn meta(&mut self, cmd: &str, arg: &str, s: &mut Session) -> Result<Flow> {
        match cmd {
            "q" | "quit" => return Ok(Flow::Quit),
            "?" => outln!("{HELP}"),
            "h" | "help" => outln!("SQL syntax help is not available in vareto; see https://www.postgresql.org/docs/current/sql-commands.html"),
            "g" | "gx" => {
                let sql = if self.buf.is_empty() { self.last.clone() } else { self.buf.take() };
                if !sql.is_empty() {
                    self.execute(&sql, if cmd == "gx" { End::Vertical } else { End::Normal }, s)?;
                }
            }
            "r" | "reset" => {
                self.buf.clear();
                outln!("Query buffer reset (cleared).");
            }
            "p" | "print" => {
                let b = self.buf.take();
                outln!("{}", if b.is_empty() { &self.last } else { &b });
                self.buf.push_line(&b);
            }
            "echo" => outln!("{arg}"),
            "set" => {
                if arg.is_empty() {
                    for (k, v) in &self.vars {
                        outln!("{k} = '{v}'");
                    }
                } else {
                    let mut words = split_args(arg).into_iter();
                    let name = words.next().unwrap_or_default();
                    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        bail!("invalid variable name: \"{name}\"");
                    }
                    self.vars.insert(name, words.collect::<String>());
                }
            }
            "unset" => {
                let name = arg.split_whitespace().next().context("\\unset: missing required argument")?;
                self.vars.remove(name);
            }
            "i" | "include" => {
                let text = read_script(arg)?;
                return run_script(self, s, &text);
            }
            "x" => {
                self.expanded = match arg {
                    "on" => Expanded::On,
                    "off" => Expanded::Off,
                    "auto" => Expanded::Auto,
                    _ if self.expanded == Expanded::Off => Expanded::On,
                    _ => Expanded::Off,
                };
                let state = match self.expanded {
                    Expanded::On => "on",
                    Expanded::Off => "off",
                    Expanded::Auto => "auto",
                };
                outln!("Expanded display is {state}.");
            }
            "timing" => {
                self.timing = match arg {
                    "on" => true,
                    "off" => false,
                    _ => !self.timing,
                };
                outln!("Timing is {}.", if self.timing { "on" } else { "off" });
            }
            "c" | "connect" => {
                if let Some(db) = arg.split_whitespace().next().filter(|d| *d != "-") {
                    s.use_db(db)?;
                }
                outln!(
                    "You are now connected to database \"{}\" as user \"{}\".",
                    s.database.as_deref().unwrap_or(""),
                    s.user.as_deref().unwrap_or("")
                );
            }
            "conninfo" => outln!(
                "You are connected to database \"{}\" as user \"{}\" on {} (vareto connection \"{}\"{}).",
                s.database.as_deref().unwrap_or(""),
                s.user.as_deref().unwrap_or(""),
                s.host,
                s.connection,
                if s.readonly { ", read-only" } else { "" }
            ),
            "l" | "list" => {
                let cond = if arg.is_empty() { "TRUE".into() } else { pattern_sql(arg, "''", "d.datname", "TRUE") };
                let sql = format!(
                    "SELECT d.datname AS \"Name\", pg_catalog.pg_get_userbyid(d.datdba) AS \"Owner\", \
                     pg_catalog.pg_encoding_to_char(d.encoding) AS \"Encoding\", d.datcollate AS \"Collate\", \
                     d.datctype AS \"Ctype\" FROM pg_catalog.pg_database d WHERE NOT d.datistemplate AND {cond} ORDER BY 1"
                );
                self.listing(s, "List of databases", &sql, "")?;
            }
            "dn" => {
                let cond = pattern_sql(arg, "''", "n.nspname", "n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
                let sql = format!(
                    "SELECT n.nspname AS \"Name\", pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\" \
                     FROM pg_catalog.pg_namespace n WHERE {cond} ORDER BY 1"
                );
                self.listing(s, "List of schemas", &sql, "")?;
            }
            "df" => {
                let cond = pattern_sql(arg, "n.nspname", "p.proname", "pg_catalog.pg_function_is_visible(p.oid)");
                let sql = format!(
                    "SELECT n.nspname AS \"Schema\", p.proname AS \"Name\", \
                     pg_catalog.pg_get_function_result(p.oid) AS \"Result data type\", \
                     pg_catalog.pg_get_function_arguments(p.oid) AS \"Argument data types\", \
                     CASE p.prokind WHEN 'a' THEN 'agg' WHEN 'w' THEN 'window' WHEN 'p' THEN 'proc' ELSE 'func' END AS \"Type\" \
                     FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
                     WHERE n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND {cond} ORDER BY 1, 2, 4"
                );
                self.listing(s, "List of functions", &sql, "")?;
            }
            "du" | "dg" => {
                let cond = pattern_sql(arg, "''", "r.rolname", "r.rolname !~ '^pg_'");
                let sql = format!(
                    "SELECT r.rolname AS \"Role name\", concat_ws(', ', \
                     CASE WHEN r.rolsuper THEN 'Superuser' END, CASE WHEN r.rolcreaterole THEN 'Create role' END, \
                     CASE WHEN r.rolcreatedb THEN 'Create DB' END, CASE WHEN NOT r.rolcanlogin THEN 'Cannot login' END, \
                     CASE WHEN r.rolreplication THEN 'Replication' END, CASE WHEN r.rolbypassrls THEN 'Bypass RLS' END) AS \"Attributes\" \
                     FROM pg_catalog.pg_roles r WHERE {cond} ORDER BY 1"
                );
                self.listing(s, "List of roles", &sql, "")?;
            }
            "d" if !arg.is_empty() && !arg.contains(['*', '?']) => self.describe(s, arg)?,
            d if d.starts_with('d') && relkinds(d.trim_start_matches('d').trim_end_matches(['S', '+'])).is_some() => {
                let kind = d.trim_start_matches('d').trim_end_matches(['S', '+']);
                self.list_relations(s, kind, arg)?;
            }
            other => bail!("invalid command \\{other}\nTry \\? for help."),
        }
        Ok(Flow::Continue)
    }
}

impl Flavor for Psql {
    fn banner(&self, s: &Session) -> String {
        let v = s.server_version.split_whitespace().nth(1).unwrap_or(&s.server_version).to_string();
        format!(
            "psql-compatible vareto shell (server {v}){}\nType \"\\?\" for help.\n",
            if s.readonly { ", read-only" } else { "" }
        )
    }

    fn prompt(&self, s: &Session) -> String {
        let db = s.database.as_deref().unwrap_or("");
        let tx = match s.tx {
            Tx::Idle => "",
            Tx::Active => "*",
            Tx::Failed => "!",
        };
        match self.buf.open() {
            None => format!("{db}={tx}> "),
            Some(Open::Statement) => format!("{db}-{tx}> "),
            Some(Open::Quote(q)) => format!("{db}{q}{tx}> "),
            Some(Open::Comment) => format!("{db}*{tx}> "),
            Some(Open::Dollar) => format!("{db}${tx}> "),
        }
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        // Backslash commands: on their own line, or ending one (`select 1 \g`).
        let (sql, meta) = match self.buf.open() {
            None | Some(Open::Statement) => match line.find('\\') {
                Some(i) if !line[..i].contains(['\'', '"']) => (&line[..i], Some(line[i + 1..].trim())),
                _ => (line, None),
            },
            _ => (line, None),
        };
        for (stmt, end) in self.buf.push_line(sql) {
            self.execute(&stmt, end, s)?;
        }
        if let Some(m) = meta {
            let (cmd, arg) = m.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((m, ""));
            return self.meta(cmd, arg, s);
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        let rest = self.buf.take();
        if !rest.is_empty() {
            self.execute(&rest, End::Normal, s)?;
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn stop_on_error(&self) -> bool {
        self.flag("ON_ERROR_STOP")
    }

    fn stop_status(&self) -> i32 {
        3
    }

    fn finish_status(&self, _errors: usize) -> i32 {
        0
    }

    fn reset(&mut self) {
        self.buf.clear();
    }

    fn error(&self, e: &anyhow::Error) -> String {
        if let Some(info) = error_info(e).filter(|i| i.get("severity").is_some()) {
            return format_error(info, &self.stmt);
        }
        let t = error_text(e);
        if t.starts_with("invalid command") || t.starts_with("Did not find") { t } else { format!("ERROR:  {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        self.tables = s.list_tables(None, None).map(|v| names(&v)).unwrap_or_default();
    }

    fn words(&self) -> Vec<String> {
        let metas = [
            "\\q", "\\?", "\\l", "\\c", "\\conninfo", "\\d", "\\dt", "\\dv", "\\di", "\\dn", "\\df", "\\du", "\\ds", "\\dm",
            "\\x", "\\timing", "\\g", "\\gx", "\\i", "\\echo", "\\p", "\\r",
        ];
        SQL_KEYWORDS.iter().chain(metas.iter()).map(|w| w.to_string()).chain(self.tables.iter().cloned()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variables() {
        assert_eq!(split_args("x 'a b' 'it''s' c'd'e '\\t'"), vec!["x", "a b", "it's", "cde", "\t"]);
        for (v, b) in [("on", Some(true)), ("OFF", Some(false)), ("t", Some(true)), ("1", Some(true)), ("n", Some(false)), ("o", None), ("maybe", None)] {
            assert_eq!(bool_value(v), b, "{v}");
        }
    }

    #[test]
    fn errors() {
        let info = serde_json::json!({
            "severity": "ERROR", "message": "column \"nope\" does not exist", "position": 14,
            "hint": "Perhaps you meant to reference the column \"t.note\".",
        });
        assert_eq!(
            format_error(&info, "select id,\n  nope from t"),
            "ERROR:  column \"nope\" does not exist\nLINE 2:   nope from t\n          ^\nHINT:  Perhaps you meant to reference the column \"t.note\"."
        );
        assert_eq!(format_error(&serde_json::json!({"severity": "FATAL", "message": "x"}), ""), "FATAL:  x");
    }

    #[test]
    fn tags() {
        assert_eq!(command_tag("insert into t values (1)", 1), "INSERT 0 1");
        assert_eq!(command_tag("UPDATE t SET a = 1", 3), "UPDATE 3");
        assert_eq!(command_tag("create table t (a int)", 0), "CREATE TABLE");
        assert_eq!(command_tag("create unique index i on t (a)", 0), "CREATE INDEX");
        assert_eq!(command_tag("begin", 0), "BEGIN");
    }

    #[test]
    fn patterns() {
        assert_eq!(pattern_sql("", "s", "n", "vis"), "vis");
        assert_eq!(pattern_sql("us*", "s", "n", "vis"), "n ~ '^(us.*)$' AND vis");
        assert_eq!(pattern_sql("app.u?", "s", "n", "vis"), "s ~ '^(app)$' AND n ~ '^(u.)$'");
        assert_eq!(pattern_sql("a_b.c", "s", "n", "vis"), "s ~ '^(a_b)$' AND n ~ '^(c)$'");
        assert_eq!(pattern_sql("it's", "s", "n", "vis"), "n ~ '^(it''s)$' AND vis");
    }
}
