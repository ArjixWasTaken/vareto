//! psql.

use std::io::Write as _;

use anyhow::{bail, Context as _, Result};
use serde_json::Value;

use super::out::{self, outln};
use super::psql_print::{crosstab, PrintOpts};
use super::psql_vars::{bool_value, interpolate, split_args, split_meta, Vars};
use super::session::{Session, Tx};
use super::sqlbuf::{Dialect, End, Mode, Open, SqlBuf};
use super::table::{self, QueryResult, Style};
use super::{elapsed, error_info, error_text, names, read_script, run_script, Flavor, Flow, SQL_KEYWORDS};

/// One level of `\if`.
struct Cond {
    /// This branch runs.
    active: bool,
    /// A branch of this `\if` has already run (or the whole `\if` is skipped).
    taken: bool,
    in_else: bool,
}

pub struct Psql {
    buf: SqlBuf,
    /// psql variables (`\set`), including the special ones (ON_ERROR_STOP, ECHO, ...).
    vars: Vars,
    /// The statement an error's position refers to (empty for internal queries).
    stmt: String,
    /// The last statement sent, for `\g` on an empty buffer.
    last: String,
    print: PrintOpts,
    timing: bool,
    cond: Vec<Cond>,
    superuser: bool,
    interactive: bool,
    /// Directories of the scripts being run, innermost last (for `\ir`).
    script_dirs: Vec<std::path::PathBuf>,
    tables: Vec<String>,
}

const HELP: &str = "\
General
  \\q                     quit psql
  \\crosstabview [COLUMNS] execute query and display result in crosstab
  \\g [(OPTIONS)] [FILE]  execute query (and send result to file or |pipe)
  \\gexec                 execute query, then execute each value in its result
  \\gset [PREFIX]         execute query and store result in psql variables
  \\gx [(OPTIONS)] [FILE] as \\g, but forces expanded output mode
  \\watch [[i=]SEC] [c=N] execute query every SEC seconds, up to N times

Help
  \\?                     show help on backslash commands
  \\h                     help on syntax of SQL commands

Query Buffer
  \\e [FILE] [LINE]       edit the query buffer (or file) with external editor
  \\p                     show the contents of the query buffer
  \\r                     reset (clear) the query buffer
  \\s [FILE]              display history or save it to file
  \\w FILE                write query buffer to file

Input/Output
  \\echo [-n] [STRING]    write string to standard output (-n for no newline)
  \\i FILE                execute commands from file
  \\ir FILE               as \\i, but relative to location of current script
  \\o [FILE]              send all query results to file or |pipe
  \\qecho [-n] [STRING]   write string to \\o output stream
  \\warn [-n] [STRING]    write string to standard error

Conditional
  \\if EXPR               begin conditional block
  \\elif EXPR             alternative within current conditional block
  \\else                  final alternative within current conditional block
  \\endif                 end conditional block

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

Formatting
  \\a                     toggle between unaligned and aligned output mode
  \\C [STRING]            set table title, or unset if none
  \\f [STRING]            show or set field separator for unaligned query output
  \\H                     toggle HTML output mode
  \\pset [NAME [VALUE]]   set table output option
                         (border|csv_fieldsep|expanded|fieldsep|footer|format|linestyle|
                         null|recordsep|tableattr|title|tuples_only)
  \\t [on|off]            show only rows
  \\T [STRING]            set HTML <table> tag attributes, or unset if none
  \\x [on|off|auto]       toggle expanded output

Connection
  \\c [DBNAME]            connect to another database
  \\conninfo              display information about current connection

Operating System
  \\cd [DIR]              change the current working directory
  \\getenv PSQLVAR ENVVAR fetch environment variable
  \\setenv NAME [VALUE]   set or unset environment variable (for commands run from here)
  \\timing [on|off]       toggle timing of commands
  \\! [COMMAND]           execute command in shell or start interactive shell

Variables
  \\prompt [TEXT] NAME    prompt user to set internal variable
  \\set [NAME [VALUE]]    set internal variable, or list all if no parameters
  \\unset NAME            unset (delete) internal variable";

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
/// `verbosity` is psql's VERBOSITY (terse, default, verbose) and `context` its
/// SHOW_CONTEXT (never, errors, always).
pub fn format_error_with(info: &Value, stmt: &str, verbosity: &str, context: &str) -> String {
    let code = if verbosity == "verbose" { info["sqlstate"].as_str().map(|c| format!("{c}: ")).unwrap_or_default() } else { String::new() };
    let mut out = format!("{}:  {code}{}", info["severity"].as_str().unwrap_or("ERROR"), info["message"].as_str().unwrap_or(""));
    if verbosity == "terse" {
        return out;
    }
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
        if key == "where" && context == "never" {
            continue;
        }
        if let Some(v) = info[key].as_str() {
            out.push_str(&format!("\n{label}:  {v}"));
        }
    }
    out
}

#[cfg(test)]
pub fn format_error(info: &Value, stmt: &str) -> String {
    format_error_with(info, stmt, "default", "errors")
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

/// `18.6 (Debian …)` → 180006, the way `server_version_num` counts.
fn version_num(version: &str) -> Option<u32> {
    let v = version.split_whitespace().find(|w| w.starts_with(|c: char| c.is_ascii_digit()))?;
    let mut parts = v.split(|c: char| !c.is_ascii_digit()).filter(|p| !p.is_empty()).map(|p| p.parse::<u32>().ok());
    let major = parts.next()??;
    let minor = parts.next().flatten().unwrap_or(0);
    Some(if major >= 10 { major * 10000 + minor } else { major * 10000 + minor * 100 + parts.next().flatten().unwrap_or(0) })
}

/// What `\g` options and an output target ask for: `\g (format=csv) file`.
fn parse_g_args(arg: &str) -> Result<(Vec<(String, Option<String>)>, Option<String>)> {
    let arg = arg.trim();
    let (opts, rest) = match arg.strip_prefix('(') {
        Some(inner) => {
            let end = inner.find(')').context("\\g: missing right parenthesis")?;
            (&inner[..end], inner[end + 1..].trim())
        }
        None => ("", arg),
    };
    let opts = split_args(opts)
        .into_iter()
        .map(|o| match o.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (o, None),
        })
        .collect();
    Ok((opts, (!rest.is_empty()).then(|| rest.to_string())))
}

impl Psql {
    pub fn new(s: &Session, opts: &super::Options, interactive: bool) -> Result<Self> {
        let mut vars: Vars = [
            ("AUTOCOMMIT", "on"),
            ("ECHO", "none"),
            ("ECHO_HIDDEN", "off"),
            ("LAST_ERROR_MESSAGE", ""),
            ("LAST_ERROR_SQLSTATE", "00000"),
            ("ON_ERROR_ROLLBACK", "off"),
            ("ON_ERROR_STOP", "off"),
            ("PROMPT1", "%/%R%x%# "),
            ("PROMPT2", "%/%R%x%# "),
            ("PROMPT3", ">> "),
            ("QUIET", "off"),
            ("SHOW_CONTEXT", "errors"),
            ("VERBOSITY", "default"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        vars.insert("VERSION_NAME".into(), crate::ipc::VERSION.into());
        vars.insert("VERSION".into(), format!("vareto shell {}", crate::ipc::VERSION));
        for kv in &opts.set {
            let (k, v) = kv.split_once('=').unwrap_or((kv.as_str(), ""));
            if k.is_empty() {
                bail!("invalid variable assignment {kv:?} (expected NAME=VALUE)");
            }
            vars.insert(k.to_string(), v.to_string());
        }
        // psql pages results taller than the terminal by default.
        out::set_pager(out::Pager::Auto, super::get_env("PSQL_PAGER"));
        let mut p = Self {
            buf: SqlBuf::new(Dialect::Postgres),
            vars,
            stmt: String::new(),
            last: String::new(),
            print: PrintOpts::default(),
            timing: false,
            cond: Vec::new(),
            superuser: false,
            interactive,
            script_dirs: Vec::new(),
            tables: Vec::new(),
        };
        p.sync_conn_vars(s);
        Ok(p)
    }

    /// The variables psql keeps up to date about the connection.
    fn sync_conn_vars(&mut self, s: &Session) {
        let set = |vars: &mut Vars, k: &str, v: String| {
            vars.insert(k.to_string(), v);
        };
        set(&mut self.vars, "DBNAME", s.database.clone().unwrap_or_default());
        set(&mut self.vars, "USER", s.user.clone().unwrap_or_default());
        set(&mut self.vars, "HOST", s.host.clone());
        let name = s.server_version.split_whitespace().nth(1).unwrap_or(&s.server_version).to_string();
        set(&mut self.vars, "SERVER_VERSION_NAME", name);
        if let Some(n) = version_num(&s.server_version) {
            set(&mut self.vars, "SERVER_VERSION_NUM", n.to_string());
        }
    }

    /// A boolean variable, read the way psql does (on/off, true/false, yes/no, 1/0).
    fn flag(&self, name: &str) -> bool {
        self.vars.get(name).is_some_and(|v| bool_value(v).unwrap_or(false))
    }

    fn var(&self, name: &str) -> &str {
        self.vars.get(name).map(String::as_str).unwrap_or("")
    }

    /// An informational message, unless QUIET.
    fn note(&self, msg: &str) {
        if !self.flag("QUIET") {
            outln!("{msg}");
        }
    }

    /// Whether the current `\if` branch runs.
    fn active(&self) -> bool {
        self.cond.last().is_none_or(|c| c.active)
    }

    fn print_rows(&self, opts: &PrintOpts, columns: &[String], rows: &[Vec<Value>], vertical: bool) {
        let text = opts.render(columns, rows, vertical, out::width());
        if !text.is_empty() {
            outln!("{text}");
        }
        // psql leaves a blank line after aligned output.
        if matches!(opts.format, super::psql_print::Format::Aligned | super::psql_print::Format::Wrapped) {
            outln!();
        }
    }

    /// Send one statement, keeping psql's result variables up to date.
    fn query(&mut self, sql: &str, s: &mut Session) -> Result<Value> {
        self.last = sql.to_string();
        self.stmt = sql.to_string();
        if self.var("ECHO") == "queries" || (self.var("ECHO") == "all" && self.interactive) {
            outln!("{sql}");
        }
        match s.sql(sql) {
            Ok(v) => {
                let rows: u64 = QueryResult::from_json(&v)
                    .into_parts()
                    .iter()
                    .map(|p| match p {
                        QueryResult::Table { rows, .. } => rows.len() as u64,
                        QueryResult::Affected { rows } => *rows,
                        _ => 0,
                    })
                    .sum();
                self.vars.insert("ROW_COUNT".into(), rows.to_string());
                self.vars.insert("ERROR".into(), "false".into());
                self.vars.insert("SQLSTATE".into(), "00000".into());
                Ok(v)
            }
            Err(e) => {
                let state = error_info(&e).and_then(|i| i["sqlstate"].as_str()).unwrap_or("XX000").to_string();
                let msg = error_info(&e).and_then(|i| i["message"].as_str()).map(str::to_string).unwrap_or_else(|| error_text(&e));
                self.vars.insert("ROW_COUNT".into(), "0".into());
                self.vars.insert("ERROR".into(), "true".into());
                self.vars.insert("SQLSTATE".into(), state.clone());
                self.vars.insert("LAST_ERROR_SQLSTATE".into(), state);
                self.vars.insert("LAST_ERROR_MESSAGE".into(), msg);
                if self.var("ECHO") == "errors" {
                    eprintln!("STATEMENT:  {sql}");
                }
                Err(e)
            }
        }
    }

    fn execute(&mut self, sql: &str, end: End, s: &mut Session) -> Result<()> {
        let opts = self.print.clone();
        self.execute_with(sql, end == End::Vertical, &opts, s)
    }

    fn execute_with(&mut self, sql: &str, vertical: bool, opts: &PrintOpts, s: &mut Session) -> Result<()> {
        let v = self.query(sql, s)?;
        for part in QueryResult::from_json(&v).into_parts() {
            match part {
                QueryResult::Table { columns, rows, truncated, .. } => {
                    self.print_rows(opts, &columns, &rows, vertical);
                    if truncated {
                        outln!("(more rows exist; only the first {} are shown, see --limit)\n", rows.len());
                    }
                }
                QueryResult::Affected { rows, .. } => self.note(&command_tag(sql, rows)),
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

    /// The query a `\g`-family command runs: the buffer, else the last one.
    fn take_query(&mut self) -> Result<String> {
        let sql = if self.buf.is_empty() { self.last.clone() } else { self.buf.take() };
        if sql.is_empty() {
            bail!("\\g: no query buffer");
        }
        Ok(sql)
    }

    /// A one-row, all-columns result of `sql` (for `\gset`), or the first result set.
    fn result_table(&mut self, sql: &str, s: &mut Session) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let v = self.query(sql, s)?;
        for part in QueryResult::from_json(&v).into_parts() {
            if let QueryResult::Table { columns, rows, .. } = part {
                return Ok((columns, rows));
            }
        }
        Ok((Vec::new(), Vec::new()))
    }

    /// Internal catalog queries go through here: ECHO_HIDDEN shows them.
    fn hidden(&mut self, sql: &str, s: &mut Session) -> Result<Option<Value>> {
        self.stmt.clear();
        match self.var("ECHO_HIDDEN").to_ascii_lowercase().as_str() {
            "noexec" => {
                outln!("/******** QUERY *********/\n{sql}\n/************************/\n");
                return Ok(None);
            }
            v if bool_value(v) == Some(true) => outln!("/******** QUERY *********/\n{sql}\n/************************/\n"),
            _ => {}
        }
        Ok(Some(s.query_limit(crate::ipc::QuerySpec::Sql { sql: sql.into() }, Some(5000))?))
    }

    /// Run an internal catalog query and print it under `title`.
    fn listing(&mut self, s: &mut Session, title: &str, sql: &str, none: &str) -> Result<()> {
        let Some(v) = self.hidden(sql, s)? else { return Ok(()) };
        if let QueryResult::Table { columns, rows, .. } = QueryResult::from_json(&v) {
            if rows.is_empty() && !none.is_empty() {
                outln!("{none}");
            } else {
                let mut opts = self.print.clone();
                opts.title = Some(title.to_string());
                self.print_rows(&opts, &columns, &rows, false);
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

    /// Feed text (an edited buffer, a script) through the line handler.
    fn feed(&mut self, text: &str, s: &mut Session) -> Result<Flow> {
        for line in text.lines() {
            if let Flow::Quit = self.line(line, s)? {
                return Ok(Flow::Quit);
            }
        }
        Ok(Flow::Continue)
    }

    /// `\if` / `\elif` / `\else` / `\endif`.
    fn conditional(&mut self, cmd: &str, arg: &str) -> Result<()> {
        let eval = |arg: &str| -> bool {
            let expr = arg.trim();
            match bool_value(expr) {
                Some(b) => b,
                None => {
                    eprintln!("unrecognized value \"{expr}\" for \"\\{cmd} expression\": Boolean expected");
                    false
                }
            }
        };
        let parent_active = || self.cond.len() < 2 || self.cond[self.cond.len() - 2].active;
        match cmd {
            "if" => {
                if self.active() {
                    let b = eval(arg);
                    self.cond.push(Cond { active: b, taken: b, in_else: false });
                } else {
                    self.cond.push(Cond { active: false, taken: true, in_else: false });
                }
            }
            "elif" => {
                let parent = parent_active();
                let top = self.cond.last_mut().context("\\elif: no matching \\if")?;
                if top.in_else {
                    bail!("\\elif: cannot occur after \\else");
                }
                if parent && !top.taken {
                    let b = eval(arg);
                    let top = self.cond.last_mut().unwrap();
                    top.active = b;
                    top.taken = b;
                } else {
                    top.active = false;
                }
            }
            "else" => {
                let parent = parent_active();
                let top = self.cond.last_mut().context("\\else: no matching \\if")?;
                if top.in_else {
                    bail!("\\else: cannot occur after \\else");
                }
                top.in_else = true;
                top.active = parent && !top.taken;
                top.taken = true;
            }
            _ => {
                self.cond.pop().context("\\endif: no matching \\if")?;
            }
        }
        Ok(())
    }

    /// psql's `\watch [i=]SECONDS [c=]COUNT [m=]MIN_ROWS`.
    fn watch(&mut self, arg: &str, s: &mut Session) -> Result<()> {
        let sql = self.take_query()?;
        let (mut every, mut count, mut min_rows) = (2.0_f64, None::<u64>, None::<usize>);
        for a in split_args(arg) {
            let (k, v) = a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())).unwrap_or(("i".into(), a.clone()));
            let bad = || anyhow::anyhow!("\\watch: incorrect {k} value \"{v}\"");
            match k.as_str() {
                "i" | "interval" => every = v.parse().map_err(|_| bad())?,
                "c" | "count" => count = Some(v.parse().map_err(|_| bad())?),
                "m" | "min_rows" => min_rows = Some(v.parse().map_err(|_| bad())?),
                _ => bail!("\\watch: unrecognized parameter \"{k}\""),
            }
        }
        let interrupts = super::interrupt::count();
        let mut runs = 0u64;
        loop {
            let now = chrono::Local::now().format("%a %d %b %Y %I:%M:%S %p");
            let mut opts = self.print.clone();
            opts.title = Some(match &self.print.title {
                Some(t) => format!("{t}  {now} (every {every}s)"),
                None => format!("{now} (every {every}s)"),
            });
            let (columns, rows) = self.result_table(&sql, s)?;
            self.print_rows(&opts, &columns, &rows, false);
            out::flush();
            runs += 1;
            if count.is_some_and(|c| runs >= c) || min_rows.is_some_and(|m| rows.len() < m) {
                return Ok(());
            }
            let until = std::time::Instant::now() + std::time::Duration::from_secs_f64(every.max(0.0));
            while std::time::Instant::now() < until {
                if super::interrupt::count() != interrupts {
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    fn meta(&mut self, cmd: &str, arg: &str, s: &mut Session) -> Result<Flow> {
        if matches!(cmd, "if" | "elif" | "else" | "endif") {
            self.conditional(cmd, arg)?;
            return Ok(Flow::Continue);
        }
        if !self.active() {
            if self.interactive {
                eprintln!("\\{cmd} command ignored; use \\endif or Ctrl-C to exit current \\if block");
            }
            return Ok(Flow::Continue);
        }
        let args = split_args(arg);
        let first = args.first().map(String::as_str);
        match cmd {
            "q" | "quit" => return Ok(Flow::Quit),
            "?" => outln!("{HELP}"),
            "h" | "help" => outln!("SQL syntax help is not available in vareto; see https://www.postgresql.org/docs/current/sql-commands.html"),
            "g" | "gx" => {
                let sql = self.take_query()?;
                let (opts, target) = parse_g_args(arg)?;
                let mut print = self.print.clone();
                for (k, v) in &opts {
                    print.set(k, v.as_deref())?;
                }
                let vertical = cmd == "gx";
                match target {
                    Some(t) => out::with_target(&t, || self.execute_with(&sql, vertical, &print, s))??,
                    None => self.execute_with(&sql, vertical, &print, s)?,
                }
            }
            "gset" => {
                let sql = self.take_query()?;
                let (columns, rows) = self.result_table(&sql, s)?;
                match rows.as_slice() {
                    [] => bail!("no rows returned for \\gset"),
                    [row] => {
                        let prefix = first.unwrap_or("");
                        for (c, v) in columns.iter().zip(row) {
                            let name = format!("{prefix}{c}");
                            match v {
                                Value::Null => self.vars.remove(&name),
                                v => self.vars.insert(name, table::psql_cell(v)),
                            };
                        }
                    }
                    _ => bail!("more than one row returned for \\gset"),
                }
            }
            "gexec" => {
                let sql = self.take_query()?;
                let (_, rows) = self.result_table(&sql, s)?;
                for row in rows {
                    for v in row.iter().filter(|v| !v.is_null()) {
                        let stmt = table::psql_cell(v);
                        self.execute(&stmt, End::Normal, s)?;
                    }
                }
            }
            "watch" => self.watch(arg, s)?,
            "crosstabview" => {
                let sql = self.take_query()?;
                let (columns, rows) = self.result_table(&sql, s)?;
                let (header, grid) = crosstab(&columns, &rows, &args)?;
                let opts = self.print.clone();
                self.print_rows(&opts, &header, &grid, false);
            }
            "r" | "reset" => {
                self.buf.clear();
                self.note("Query buffer reset (cleared).");
            }
            "p" | "print" => {
                let b = self.buf.take();
                let shown = if b.is_empty() { self.last.clone() } else { b.clone() };
                if shown.is_empty() {
                    outln!("Query buffer is empty.");
                } else {
                    outln!("{shown}");
                }
                self.buf.push_line(&b);
            }
            "w" | "write" => {
                let path = first.context("\\w: missing required argument")?;
                let b = self.buf.take();
                let text = if b.is_empty() { self.last.clone() } else { b.clone() };
                self.buf.push_line(&b);
                std::fs::write(super::expand_home(path), text + "\n").with_context(|| path.to_string())?;
            }
            "e" | "edit" => {
                let text = match first {
                    Some(path) => {
                        let line = args.get(1).and_then(|l| l.parse().ok());
                        let path = super::expand_home(path);
                        super::edit::edit_file(&path, line)?;
                        std::fs::read_to_string(&path).with_context(|| path.clone())?
                    }
                    None => {
                        let b = self.buf.take();
                        super::edit::edit_text(if b.is_empty() { &self.last } else { &b }, "sql")?
                    }
                };
                // Complete statements run; the rest stays in the buffer.
                self.buf.clear();
                return self.feed(&text, s);
            }
            "s" => return Ok(Flow::History(first.map(str::to_string))),
            "echo" | "qecho" | "warn" => {
                let (newline, words) = match args.first().map(String::as_str) {
                    Some("-n") => (false, &args[1..]),
                    _ => (true, &args[..]),
                };
                let text = words.join(" ");
                match cmd {
                    "qecho" => {
                        if newline { outln!("{text}") } else { out::text(&text) }
                    }
                    "warn" => {
                        out::flush();
                        if newline { eprintln!("{text}") } else { eprint!("{text}") }
                    }
                    _ => {
                        out::flush();
                        let mut stdout = std::io::stdout().lock();
                        write!(stdout, "{text}{}", if newline { "\n" } else { "" }).ok();
                        stdout.flush().ok();
                    }
                }
            }
            "o" | "out" => out::redirect(first)?,
            "set" => {
                if args.is_empty() {
                    for (k, v) in &self.vars {
                        outln!("{k} = '{v}'");
                    }
                } else {
                    let name = &args[0];
                    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        bail!("invalid variable name: \"{name}\"");
                    }
                    self.vars.insert(name.clone(), args[1..].concat());
                }
            }
            "unset" => {
                let name = first.context("\\unset: missing required argument")?;
                self.vars.remove(name);
            }
            "getenv" => {
                let (var, env) = match args.as_slice() {
                    [v, e] => (v.clone(), e.clone()),
                    _ => bail!("\\getenv: missing required argument"),
                };
                match super::get_env(&env) {
                    Some(v) => self.vars.insert(var, v),
                    None => self.vars.remove(&var),
                };
            }
            "setenv" => {
                let name = first.context("\\setenv: missing required argument")?;
                if name.contains('=') {
                    bail!("\\setenv: environment variable name must not contain \"=\"");
                }
                super::set_env(name, args.get(1).map(String::as_str));
            }
            "prompt" => {
                let (text, name) = match args.as_slice() {
                    [name] => (String::new(), name.clone()),
                    [text @ .., name] => (text.join(" "), name.clone()),
                    [] => bail!("\\prompt: missing required argument"),
                };
                out::flush();
                print!("{text}");
                std::io::stdout().flush().ok();
                let mut line = String::new();
                // From the terminal even when commands come from a script.
                match std::fs::File::open("/dev/tty") {
                    Ok(tty) => std::io::BufRead::read_line(&mut std::io::BufReader::new(tty), &mut line)?,
                    Err(_) => std::io::stdin().read_line(&mut line)?,
                };
                self.vars.insert(name, line.trim_end_matches(['\n', '\r']).to_string());
            }
            "i" | "include" | "ir" | "include_relative" => {
                let path = first.context(format!("\\{cmd}: missing required argument"))?;
                let mut p = std::path::PathBuf::from(super::expand_home(path));
                if matches!(cmd, "ir" | "include_relative") && p.is_relative() {
                    if let Some(dir) = self.script_dirs.last() {
                        p = dir.join(p);
                    }
                }
                let text = read_script(&p.to_string_lossy())?;
                self.script_dirs.push(p.parent().map(|d| d.to_path_buf()).unwrap_or_default());
                let r = run_script(self, s, &text);
                self.script_dirs.pop();
                return r;
            }
            "pset" => match first {
                None => outln!("{}", self.print.show_all()),
                Some("pager") => {
                    let mode = match args.get(1).map(|v| v.to_ascii_lowercase()) {
                        Some(v) if v == "always" => out::Pager::Always,
                        Some(v) => if bool_value(&v) == Some(true) { out::Pager::Auto } else { out::Pager::Off },
                        None => if out::pager() == out::Pager::Off { out::Pager::Auto } else { out::Pager::Off },
                    };
                    let msg = match mode {
                        out::Pager::Off => "Pager usage is off.",
                        out::Pager::Auto => "Pager is used for long output.",
                        out::Pager::Always => "Pager is always used.",
                    };
                    out::set_pager(mode, None);
                    self.note(msg);
                }
                Some(name) => {
                    let msg = self.print.set(name, args.get(1).map(String::as_str))?;
                    self.note(&msg);
                }
            },
            "x" => {
                let msg = self.print.set("expanded", first)?;
                self.note(&msg);
            }
            "a" => {
                let to = if self.print.format == super::psql_print::Format::Aligned { "unaligned" } else { "aligned" };
                let msg = self.print.set("format", Some(to))?;
                self.note(&msg);
            }
            "t" => {
                let msg = self.print.set("tuples_only", first)?;
                self.note(&msg);
            }
            "f" => {
                let msg = self.print.set("fieldsep", first)?;
                self.note(&msg);
            }
            "C" => {
                let msg = self.print.set("title", Some(&args.join(" ")))?;
                self.note(&msg);
            }
            "H" | "html" => {
                let to = if self.print.format == super::psql_print::Format::Html { "aligned" } else { "html" };
                let msg = self.print.set("format", Some(to))?;
                self.note(&msg);
            }
            "T" => {
                let msg = self.print.set("tableattr", Some(&args.join(" ")))?;
                self.note(&msg);
            }
            "timing" => {
                self.timing = match first {
                    Some(v) => bool_value(v).context(format!("unrecognized value \"{v}\" for \"\\timing\": Boolean expected"))?,
                    None => !self.timing,
                };
                self.note(&format!("Timing is {}.", if self.timing { "on" } else { "off" }));
            }
            "cd" => {
                let dir = first.map(super::expand_home).or_else(|| super::get_env("HOME")).context("\\cd: could not get home directory")?;
                std::env::set_current_dir(&dir).with_context(|| format!("\\cd: could not change directory to \"{dir}\""))?;
            }
            "!" => {
                out::flush();
                let cmd = arg.trim();
                let cmd = if cmd.is_empty() { super::get_env("SHELL").unwrap_or_else(|| "/bin/sh".into()) } else { cmd.to_string() };
                super::shell_command(&cmd).status().with_context(|| format!("could not run {cmd}"))?;
            }
            "c" | "connect" => {
                if let Some(db) = first.filter(|d| *d != "-") {
                    s.use_db(db)?;
                }
                self.sync_conn_vars(s);
                self.note(&format!(
                    "You are now connected to database \"{}\" as user \"{}\".",
                    s.database.as_deref().unwrap_or(""),
                    s.user.as_deref().unwrap_or("")
                ));
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

    /// psql's prompt escapes (`%/`, `%R`, `%x`, `%#`, ...).
    fn expand_prompt(&self, p: &str, s: &Session) -> String {
        let db = s.database.clone().unwrap_or_default();
        let r = if !self.active() {
            '@'
        } else {
            match self.buf.open() {
                None => '=',
                Some(Open::Statement) => '-',
                Some(Open::Quote(q)) => q,
                Some(Open::Comment) => '*',
                Some(Open::Dollar) => '$',
            }
        };
        let mut out = String::new();
        let mut chars = p.chars().peekable();
        while let Some(c) = chars.next() {
            if c != '%' {
                out.push(c);
                continue;
            }
            match chars.next() {
                Some('/') => out.push_str(&db),
                Some('~') => out.push_str(if s.user.as_deref() == Some(db.as_str()) { "~" } else { &db }),
                Some('n') => out.push_str(s.user.as_deref().unwrap_or("")),
                Some('M') => out.push_str(&s.host),
                Some('m') => out.push_str(s.host.split(['.', ' ']).next().unwrap_or("")),
                Some('>') => out.push_str(self.var("PORT")),
                Some('#') => out.push(if self.superuser { '#' } else { '>' }),
                Some('R') => out.push(r),
                Some('x') => out.push_str(match s.tx {
                    Tx::Idle => "",
                    Tx::Active => "*",
                    Tx::Failed => "!",
                }),
                Some('p') => out.push_str(&s.backend_id.map(|p| p.to_string()).unwrap_or_default()),
                Some('l') => out.push_str(&(self.buf.lines() + 1).to_string()),
                Some('w') => out.push_str(&" ".repeat(self.expand_prompt(self.var("PROMPT1"), s).chars().count())),
                Some('%') => out.push('%'),
                Some('[') | Some(']') => {}
                Some(':') => {
                    let name: String = chars.by_ref().take_while(|&c| c != ':').collect();
                    out.push_str(self.var(&name));
                }
                Some(other) => {
                    out.push('%');
                    out.push(other);
                }
                None => out.push('%'),
            }
        }
        out
    }
}

impl Flavor for Psql {
    fn banner(&self, s: &Session) -> String {
        if self.flag("QUIET") {
            return String::new();
        }
        let v = s.server_version.split_whitespace().nth(1).unwrap_or(&s.server_version).to_string();
        format!(
            "psql-compatible vareto shell (server {v}){}\nType \"\\?\" for help.\n",
            if s.readonly { ", read-only" } else { "" }
        )
    }

    fn prompt(&self, s: &Session) -> String {
        let which = if self.buf.is_empty() { "PROMPT1" } else { "PROMPT2" };
        self.expand_prompt(self.var(which), s)
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.var("ECHO") == "all" && !self.interactive {
            outln!("{line}");
        }
        let mode = self.buf.mode();
        // Backslash commands: on their own line, or ending one (`select 1 \g`).
        let (sql, meta) = split_meta(line, &mode);
        if self.active() {
            let sql = interpolate(sql, &mode, &self.vars);
            for (stmt, end) in self.buf.push_line(&sql) {
                self.execute(&stmt, end, s)?;
            }
        }
        if let Some(m) = meta {
            let m = m.trim();
            let (cmd, arg) = m.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((m, ""));
            // `\!` takes the rest of the line verbatim.
            let arg = if cmd == "!" { arg.to_string() } else { interpolate(arg, &Mode::Code, &self.vars) };
            return self.meta(cmd, &arg, s);
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        if !self.cond.is_empty() {
            self.cond.clear();
            eprintln!("reached EOF without finding closing \\endif(s)");
        }
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
            return format_error_with(info, &self.stmt, self.var("VERBOSITY"), self.var("SHOW_CONTEXT"));
        }
        let t = error_text(e);
        let local = ["invalid command", "Did not find", "\\", "no rows returned", "more than one row", "unrecognized value", "invalid variable"];
        if local.iter().any(|p| t.starts_with(p)) { t } else { format!("ERROR:  {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        self.tables = s.list_tables(None, None).map(|v| names(&v)).unwrap_or_default();
        self.superuser = s
            .sql("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")
            .ok()
            .and_then(|v| v["rows"][0][0].as_bool())
            .unwrap_or(false);
        self.sync_conn_vars(s);
    }

    fn words(&self) -> Vec<String> {
        let metas = [
            "\\q", "\\?", "\\l", "\\c", "\\conninfo", "\\d", "\\dt", "\\dv", "\\di", "\\dn", "\\df", "\\du", "\\ds", "\\dm",
            "\\x", "\\timing", "\\g", "\\gx", "\\gset", "\\gexec", "\\watch", "\\crosstabview", "\\i", "\\ir", "\\echo",
            "\\qecho", "\\warn", "\\p", "\\r", "\\e", "\\w", "\\s", "\\o", "\\set", "\\unset", "\\pset", "\\a", "\\t", "\\f",
            "\\C", "\\H", "\\T", "\\if", "\\elif", "\\else", "\\endif", "\\prompt", "\\getenv", "\\setenv", "\\cd", "\\!",
        ];
        SQL_KEYWORDS.iter().chain(metas.iter()).map(|w| w.to_string()).chain(self.tables.iter().cloned()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(version_num("PostgreSQL 18.6 (Debian 18.6-1.pgdg)"), Some(180006));
        assert_eq!(version_num("PostgreSQL 9.6.24 on x86_64"), Some(90624));
    }

    #[test]
    fn g_arguments() {
        let (opts, target) = parse_g_args("(format=csv tuples_only) |sort").unwrap();
        assert_eq!(opts, vec![("format".to_string(), Some("csv".to_string())), ("tuples_only".to_string(), None)]);
        assert_eq!(target.as_deref(), Some("|sort"));
        assert_eq!(parse_g_args("").unwrap(), (vec![], None));
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
