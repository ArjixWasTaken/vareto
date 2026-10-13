//! psql.

use std::io::Write as _;
use std::os::fd::{AsFd as _, OwnedFd};

use anyhow::{bail, Context as _, Result};
use serde_json::Value;

use super::out::{self, outln};
use super::psql_catalog::{self as catalog, lit, Listing, Mods};
use super::psql_copy::{self as copy, Direction, Endpoint};
use super::psql_print::{crosstab, PrintOpts};
use super::psql_vars::{bool_value, interpolate, split_args, split_meta, Vars};
use super::session::{Session, Tx};
use super::sqlbuf::{Dialect, End, Mode, Open, SqlBuf};
use super::table::{self, QueryResult};
use super::{elapsed, error_info, error_text, names, pairs, read_script, run_script, Flavor, Flow, Completions, COLUMN_LIMIT, SQL_KEYWORDS};

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
    /// The last error and the statement it came from (`\errverbose`).
    last_error: Option<(Value, String)>,
    warned_readonly_tx: bool,
    /// Directories of the scripts being run, innermost last (for `\ir`).
    script_dirs: Vec<std::path::PathBuf>,
    tables: Vec<String>,
    columns: Vec<(String, String)>,
    /// A `COPY … FROM STDIN` taking its data from the input lines, up to `\.`.
    copy_in: Option<InlineCopy>,
    /// `\restrict KEY` (as pg_dump writes): meta-commands are refused until `\unrestrict KEY`.
    restricted: Option<String>,
}

/// An inline `COPY … FROM STDIN` in progress: the data lines go into `pipe`, which
/// the daemon reads.
struct InlineCopy {
    sql: String,
    /// `None` once the daemon stopped reading.
    pipe: Option<std::io::BufWriter<std::fs::File>>,
    savepoint: bool,
}

const HELP: &str = "\
General
  \\q                     quit psql
  \\crosstabview [COLUMNS] execute query and display result in crosstab
  \\g [(OPTIONS)] [FILE]  execute query (and send result to file or |pipe)
  \\gdesc                 describe result of query, without executing it
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
  \\copy ...              perform SQL COPY with data stream to the client host
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
  (options: S = show system objects, + = additional detail, x = expanded output)
  \\d[S+]                 list tables, views, and sequences
  \\d[S+]  NAME           describe table, view, sequence, or index
  \\da[S]  [PATTERN]      list aggregates
  \\dA[+]  [PATTERN]      list access methods
  \\dAc / \\dAf / \\dAo / \\dAp [AMPTRN [...]]  list operator classes, families, operators, support functions
  \\db[+]  [PATTERN]      list tablespaces
  \\dc[S+] [PATTERN]      list conversions
  \\dconfig[+] [PATTERN]  list configuration parameters
  \\dC[+]  [PATTERN]      list casts
  \\dd[S]  [PATTERN]      show object descriptions not displayed elsewhere
  \\dD[S+] [PATTERN]      list domains
  \\ddp    [PATTERN]      list default privileges
  \\dE[S+] [PATTERN]      list foreign tables
  \\des[+] [PATTERN]      list foreign servers
  \\det[+] [PATTERN]      list foreign tables
  \\deu[+] [PATTERN]      list user mappings
  \\dew[+] [PATTERN]      list foreign-data wrappers
  \\df[anptw][S+] [PATTERN]  list [only agg/normal/procedure/trigger/window] functions
  \\dF[+]  [PATTERN]      list text search configurations
  \\dFd[+] / \\dFp[+] / \\dFt[+] [PATTERN]  list text search dictionaries, parsers, templates
  \\dg[S+] [PATTERN]      list roles
  \\di[S+] [PATTERN]      list indexes
  \\dl[+]                 list large objects, same as \\lo_list
  \\dL[S+] [PATTERN]      list procedural languages
  \\dm[S+] [PATTERN]      list materialized views
  \\dn[S+] [PATTERN]      list schemas
  \\do[S+] [OPPTRN [TYPEPTRN [TYPEPTRN]]]  list operators
  \\dO[S+] [PATTERN]      list collations
  \\dp[S]  [PATTERN]      list table, view, and sequence access privileges
  \\dP[itn+] [PATTERN]    list [only index/table] partitioned relations [n=nested]
  \\drds [ROLEPTRN [DBPTRN]] list per-database role settings
  \\drg[S] [PATTERN]      list role grants
  \\dRp[+] [PATTERN]      list replication publications
  \\dRs[+] [PATTERN]      list replication subscriptions
  \\ds[S+] [PATTERN]      list sequences
  \\dt[S+] [PATTERN]      list tables
  \\dT[S+] [PATTERN]      list data types
  \\du[S+] [PATTERN]      list roles
  \\dv[S+] [PATTERN]      list views
  \\dx[+]  [PATTERN]      list extensions
  \\dX     [PATTERN]      list extended statistics
  \\dy[+]  [PATTERN]      list event triggers
  \\l[+]   [PATTERN]      list databases
  \\sf[+]  FUNCNAME       show a function's definition
  \\sv[+]  VIEWNAME       show a view's definition
  \\z[S]   [PATTERN]      same as \\dp
  \\ef [FUNCNAME]         edit function definition with external editor
  \\ev [VIEWNAME]         edit view definition with external editor
  \\errverbose            show most recent error message at maximum verbosity

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
  \\password [USERNAME]   securely change the password for a user

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
        // CREATE TABLE … AS and materialized views report the rows they stored.
        "CREATE" if sql.to_uppercase().split_whitespace().nth(1) == Some("MATERIALIZED")
            || (sql.to_uppercase().contains(" AS ") && sql.to_uppercase().split_whitespace().any(|w| w == "TABLE")
                && !sql.to_uppercase().contains("FUNCTION") && !sql.to_uppercase().contains("PROCEDURE")) =>
        {
            format!("SELECT {rows}")
        }
        "CREATE" | "DROP" | "ALTER" => {
            let upper = sql.to_uppercase();
            let mut rest = upper.split_whitespace().skip(1).peekable();
            // CREATE OR REPLACE VIEW → CREATE VIEW; CREATE TEMP TABLE → CREATE TABLE.
            while rest.next_if(|w| matches!(*w, "OR" | "REPLACE" | "TEMP" | "TEMPORARY" | "UNLOGGED" | "GLOBAL" | "LOCAL" | "RECURSIVE" | "TRUSTED" | "PROCEDURAL")).is_some() {}
            let words: Vec<&str> = rest.collect();
            let w = |i: usize| words.get(i).copied().unwrap_or_default();
            // Object types of more than one word keep them all.
            let object = match w(0) {
                "UNIQUE" => "INDEX".to_string(),
                "DEFAULT" | "CONSTRAINT" => w(1).to_string(),
                "MATERIALIZED" | "EVENT" | "ACCESS" | "USER" => format!("{} {}", w(0), w(1)),
                "FOREIGN" if w(1) == "DATA" => format!("FOREIGN DATA {}", w(2)),
                "FOREIGN" => format!("FOREIGN {}", w(1)),
                "TEXT" => format!("TEXT SEARCH {}", w(2)),
                "OPERATOR" if matches!(w(1), "CLASS" | "FAMILY") => format!("OPERATOR {}", w(1)),
                other => other.to_string(),
            };
            format!("{first} {object}")
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
            last_error: None,
            warned_readonly_tx: false,
            script_dirs: Vec::new(),
            tables: Vec::new(),
            columns: Vec::new(),
            copy_in: None,
            restricted: None,
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
        self.query_with(sql, s, &mut |s| s.sql(sql))
    }

    /// [`Self::query`], with `run` sending the statement (COPY has its own requests).
    fn query_with(&mut self, sql: &str, s: &mut Session, run: &mut dyn FnMut(&mut Session) -> Result<Value>) -> Result<Value> {
        let savepoint = self.before_query(sql, s)?;
        let res = run(s);
        self.after_query(sql, s, res, savepoint)
    }

    /// What psql does before sending a statement: ECHO, the implicit BEGIN of
    /// `AUTOCOMMIT off`, the `ON_ERROR_ROLLBACK` savepoint. Returns whether it set
    /// the savepoint.
    fn before_query(&mut self, sql: &str, s: &mut Session) -> Result<bool> {
        self.last = sql.to_string();
        self.stmt = sql.to_string();
        if self.var("ECHO") == "queries" || (self.var("ECHO") == "all" && self.interactive) {
            outln!("{sql}");
        }
        // AUTOCOMMIT off: an implicit BEGIN. ON_ERROR_ROLLBACK: a savepoint around
        // each statement inside a transaction, so an error doesn't abort it.
        let autocommit = !matches!(bool_value(self.var("AUTOCOMMIT")), Some(false));
        let rollback_mode = self.var("ON_ERROR_ROLLBACK").to_ascii_lowercase();
        let wants_rollback = rollback_mode == "interactive" && self.interactive || bool_value(&rollback_mode) == Some(true);
        let control = Self::needs_no_begin(sql);
        if s.readonly && (!autocommit || wants_rollback) {
            if !self.warned_readonly_tx {
                eprintln!("(AUTOCOMMIT off / ON_ERROR_ROLLBACK have no effect on a read-only connection)");
                self.warned_readonly_tx = true;
            }
        } else if !autocommit && s.tx == Tx::Idle && !control {
            s.sql("BEGIN")?;
        }
        let savepoint = !s.readonly && wants_rollback && s.tx == Tx::Active && !control;
        if savepoint {
            s.sql("SAVEPOINT pg_psql_temporary_savepoint")?;
        }
        Ok(savepoint)
    }

    /// What psql does after a statement: release or roll back to the savepoint,
    /// print notices, set `ROW_COUNT`, `ERROR`, `SQLSTATE` and friends.
    fn after_query(&mut self, sql: &str, s: &mut Session, res: Result<Value>, savepoint: bool) -> Result<Value> {
        if savepoint {
            match &res {
                Ok(_) => {
                    s.sql("RELEASE pg_psql_temporary_savepoint").ok();
                }
                Err(_) => {
                    if s.sql("ROLLBACK TO pg_psql_temporary_savepoint").is_ok() {
                        s.tx = Tx::Active;
                    }
                }
            }
        }
        match res {
            Ok(v) => {
                for n in v["notices"].as_array().into_iter().flatten() {
                    out::flush();
                    eprintln!("{}:  {}", n["severity"].as_str().unwrap_or("NOTICE"), n["message"].as_str().unwrap_or(""));
                }
                let rows: u64 = if let Some(n) = v["rows"].as_u64() {
                    n
                } else {
                    QueryResult::from_json(&v)
                    .into_parts()
                    .iter()
                    .map(|p| match p {
                        QueryResult::Table { rows, .. } => rows.len() as u64,
                        QueryResult::Affected { rows } => *rows,
                        _ => 0,
                    })
                    .sum()
                };
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
                if let Some(info) = error_info(&e) {
                    self.last_error = Some((info.clone(), sql.to_string()));
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
        match copy::direction(sql) {
            Some(Direction::In) => return self.start_copy_in(sql, s),
            Some(Direction::Out) => return self.copy_to_output(sql, s),
            None => {}
        }
        // csv / unaligned output to a file or pipe streams, unlimited.
        if let Some(format) = opts.stream_format(vertical) {
            if crate::readonly::returns_rows(crate::readonly::SqlDialect::Postgres, sql) {
                if let Some(dst) = out::bulk_fd()? {
                    let v = self.query_with(sql, s, &mut |s| s.stream(sql, format.clone(), dst.as_fd()))?;
                    if self.timing {
                        outln!("Time: {:.3} ms", elapsed(&v) * 1000.0);
                    }
                    return Ok(());
                }
            }
        }
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

    /// Start a `COPY … FROM STDIN` whose data is the input lines that follow, up to
    /// `\.` or the end of input. They stream to the daemon through a pipe as they
    /// are read.
    fn start_copy_in(&mut self, sql: &str, s: &mut Session) -> Result<()> {
        let savepoint = self.before_query(sql, s)?;
        let pipe = match s.start_copy_in(sql) {
            Ok(p) => p,
            Err(e) => return self.after_query(sql, s, Err(e), savepoint).map(drop),
        };
        if self.interactive && !self.flag("QUIET") {
            eprintln!("Enter data to be copied followed by a newline.\nEnd with a backslash and a period on a line by itself, or an EOF signal.");
        }
        self.copy_in = Some(InlineCopy { sql: sql.to_string(), pipe: Some(std::io::BufWriter::with_capacity(1 << 16, pipe)), savepoint });
        Ok(())
    }

    /// One data line of an inline COPY.
    fn copy_line(&mut self, line: &str) {
        let Some(c) = self.copy_in.as_mut() else { return };
        if let Some(pipe) = c.pipe.as_mut() {
            // A broken pipe: the daemon stopped reading (the COPY failed). Keep
            // swallowing data lines; its error is reported at the end.
            let mut data = Vec::with_capacity(line.len() + 1);
            data.extend_from_slice(line.as_bytes());
            data.push(b'\n');
            if pipe.write_all(&crate::ipc::copy_frame(&data)).is_err() {
                c.pipe = None;
            }
        }
    }

    /// End an inline COPY's data and report the result.
    fn end_copy_in(&mut self, s: &mut Session) -> Result<()> {
        let Some(c) = self.copy_in.take() else { return Ok(()) };
        if let Some(mut pipe) = c.pipe {
            // The empty frame: the data is complete.
            pipe.write_all(&crate::ipc::copy_frame(&[])).and_then(|()| pipe.flush()).ok();
        }
        let res = s.finish_streaming();
        s.track(&c.sql, res.is_ok());
        let v = self.after_query(&c.sql, s, res, c.savepoint)?;
        self.note(&format!("COPY {}", v["rows"].as_u64().unwrap_or(0)));
        Ok(())
    }

    /// Ctrl-C during an inline COPY: abort it.
    fn cancel_copy_in(&mut self, s: &mut Session) {
        if let Some(c) = self.copy_in.take() {
            // Cancel before closing the pipe, so the daemon aborts instead of
            // taking the end of the data as the end of the COPY.
            s.cancel();
            drop(c.pipe);
            let res = s.finish_streaming();
            s.track(&c.sql, res.is_ok());
            if let Err(e) = self.after_query(&c.sql, s, res, c.savepoint) {
                eprintln!("{}", self.error(&e));
            }
        }
    }

    /// `COPY … TO STDOUT` into the current output (the terminal, `\o`, `\g file`).
    fn copy_to_output(&mut self, sql: &str, s: &mut Session) -> Result<()> {
        let dst = match out::stream_fd()? {
            Some(fd) => fd,
            None => std::io::stdout().as_fd().try_clone_to_owned()?,
        };
        let v = self.query_with(sql, s, &mut |s| s.copy_out(sql, dst.as_fd()))?;
        self.note(&format!("COPY {}", v["rows"].as_u64().unwrap_or(0)));
        Ok(())
    }

    /// `\copy`: COPY with the file on this side. The daemon reads or writes the
    /// file, program or output this process opened.
    fn meta_copy(&mut self, arg: &str, s: &mut Session) -> Result<()> {
        use std::process::{Command, Stdio};
        let m = copy::parse_meta(arg)?;
        let sql = m.sql.clone();
        match (m.direction, &m.endpoint) {
            (Direction::In, Endpoint::Std | Endpoint::PStd) => self.start_copy_in(&sql, s),
            (Direction::In, Endpoint::File(f)) => {
                let file = std::fs::File::open(f).with_context(|| f.clone())?;
                let v = self.query_with(&sql, s, &mut |s| s.copy_in(&sql, file.as_fd()))?;
                self.note(&format!("COPY {}", v["rows"].as_u64().unwrap_or(0)));
                Ok(())
            }
            (Direction::In, Endpoint::Program(cmd)) => {
                let mut child = Command::new("sh").arg("-c").arg(cmd).stdout(Stdio::piped()).spawn()?;
                let stdout = child.stdout.take().expect("piped");
                let res = self.query_with(&sql, s, &mut |s| s.copy_in(&sql, stdout.as_fd()));
                drop(stdout);
                let status = child.wait()?;
                let v = res?;
                if !status.success() {
                    bail!("program \"{cmd}\" failed: {status}");
                }
                self.note(&format!("COPY {}", v["rows"].as_u64().unwrap_or(0)));
                Ok(())
            }
            (Direction::Out, Endpoint::Std) => self.copy_to_output(&sql, s),
            (Direction::Out, endpoint) => {
                let mut child = None;
                let dst: OwnedFd = match endpoint {
                    Endpoint::File(f) => std::fs::File::create(f).with_context(|| f.clone())?.into(),
                    Endpoint::Program(cmd) => {
                        let mut c = Command::new("sh").arg("-c").arg(cmd).stdin(Stdio::piped()).spawn()?;
                        let stdin = c.stdin.take().expect("piped");
                        child = Some(c);
                        stdin.into()
                    }
                    _ => {
                        out::flush();
                        std::io::stdout().flush().ok();
                        std::io::stdout().as_fd().try_clone_to_owned()?
                    }
                };
                let res = self.query_with(&sql, s, &mut |s| s.copy_out(&sql, dst.as_fd()));
                drop(dst);
                if let Some(mut c) = child {
                    c.wait()?;
                }
                self.note(&format!("COPY {}", res?["rows"].as_u64().unwrap_or(0)));
                Ok(())
            }
        }
    }

    /// `\gdesc`: the result columns of the query buffer, without running it.
    fn gdesc(&mut self, sql: &str, s: &mut Session) -> Result<()> {
        self.stmt = sql.to_string();
        let v = s.describe(sql)?;
        let cols = v["columns"].as_array().cloned().unwrap_or_default();
        if cols.is_empty() {
            outln!("The command has no result, or the result has no columns.");
            return Ok(());
        }
        let values: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let oid = c["oid"].as_u64().map(|o| format!("{o}::pg_catalog.oid")).unwrap_or_else(|| "NULL::pg_catalog.oid".into());
                format!("({}, {oid}, {i})", lit(c["name"].as_str().unwrap_or("")))
            })
            .collect();
        let q = format!(
            "SELECT n AS \"Column\", pg_catalog.format_type(o, NULL) AS \"Type\" FROM (VALUES {}) v(n, o, i) ORDER BY i",
            values.join(", ")
        );
        if let Some((columns, rows)) = self.catalog(s, &q)? {
            let opts = self.print.clone();
            self.print_rows(&opts, &columns, &rows, false);
        }
        Ok(())
    }

    /// `\password [user]`: a SCRAM verifier computed here, so the password never
    /// leaves this process.
    fn password(&mut self, user: Option<&str>, s: &mut Session) -> Result<()> {
        let user = match user {
            Some(u) => u.to_string(),
            None => s.sql("SELECT current_user")?["rows"][0][0].as_str().unwrap_or_default().to_string(),
        };
        let pw = rpassword::prompt_password(format!("Enter new password for user \"{user}\": "))?;
        let again = rpassword::prompt_password("Enter it again: ")?;
        if pw != again {
            bail!("Passwords didn't match.");
        }
        let mut salt = [0u8; 16];
        std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut salt)?;
        let verifier = copy::scram_verifier(&pw, &salt, 4096);
        s.sql(&format!("ALTER USER {} PASSWORD {}", super::psql_vars::quote_ident(&user), lit(&verifier)))?;
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

    /// Rows of an internal catalog query (shown first when ECHO_HIDDEN is set).
    fn catalog(&mut self, s: &mut Session, sql: &str) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        let Some(v) = self.hidden(sql, s)? else { return Ok(None) };
        Ok(Some(match QueryResult::from_json(&v) {
            QueryResult::Table { columns, rows, .. } => (columns, rows),
            _ => (Vec::new(), Vec::new()),
        }))
    }

    /// Run a catalog listing and print it under its title.
    fn listing(&mut self, s: &mut Session, l: &Listing, expanded: bool) -> Result<()> {
        let Some((columns, rows)) = self.catalog(s, &l.sql)? else { return Ok(()) };
        if rows.is_empty() {
            if let Some(none) = &l.none {
                outln!("{none}");
                return Ok(());
            }
        }
        let mut opts = self.print.clone();
        opts.title = Some(l.title.clone());
        self.print_rows(&opts, &columns, &rows, expanded);
        Ok(())
    }

    fn server_version_num(&self) -> u32 {
        self.var("SERVER_VERSION_NUM").parse().unwrap_or(180000)
    }

    /// `\d PATTERN`: describe each matching relation.
    fn describe_matching(&mut self, s: &mut Session, pat: &str, m: Mods) -> Result<()> {
        let sql = format!(
            "SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r','p','v','m','S','f','i','I','c') AND {} ORDER BY n.nspname, c.relname",
            catalog::pattern(Some(pat), "n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)", m),
        );
        let Some((_, rows)) = self.catalog(s, &sql)? else { return Ok(()) };
        if rows.is_empty() {
            bail!("Did not find any relation named \"{pat}\".");
        }
        for r in rows {
            self.describe(s, &table::cell(&r[0], ""), m)?;
        }
        Ok(())
    }

    /// A section after the columns of `\d` ("Indexes:", "Triggers:", ...).
    fn section(&mut self, s: &mut Session, title: &str, sql: &str, fmt: &dyn Fn(&[Value]) -> String) -> Result<()> {
        if let Some((_, rows)) = self.catalog(s, sql)? {
            if !rows.is_empty() {
                outln!("{title}:");
                for r in &rows {
                    outln!("    {}", fmt(r));
                }
            }
        }
        Ok(())
    }

    /// `\d NAME` for one relation, by OID.
    fn describe(&mut self, s: &mut Session, oid: &str, m: Mods) -> Result<()> {
        let ver = self.server_version_num();
        let am = if ver >= 120000 { "am.amname" } else { "NULL" };
        let am_join = if ver >= 120000 { "LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam" } else { "" };
        let Some((_, head)) = self.catalog(
            s,
            &format!(
                "SELECT n.nspname, c.relname, c.relkind::text, c.relpersistence::text, c.relrowsecurity, c.relforcerowsecurity, \
                 {}, {am}, pg_catalog.array_to_string(c.reloptions, ', ') \
                 FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace {am_join} WHERE c.oid = {oid}",
                if ver >= 100000 { "c.relispartition" } else { "false" },
            ),
        )?
        else {
            return Ok(());
        };
        let Some(h) = head.first() else { bail!("Did not find any relation with OID {oid}.") };
        let txt = |i: usize| table::cell(&h[i], "");
        let (schema, rel, kind, persistence) = (txt(0), txt(1), txt(2), txt(3));
        let (rls, forced, is_partition) = (h[4] == Value::Bool(true), h[5] == Value::Bool(true), h[6] == Value::Bool(true));
        let (access_method, options) = (txt(7), txt(8));
        let unlogged = if persistence == "u" { "Unlogged " } else { "" };
        let what = match kind.as_str() {
            "v" => "View".to_string(),
            "m" => format!("{unlogged}Materialized view"),
            "i" => format!("{unlogged}Index"),
            "I" => format!("{unlogged}Partitioned index"),
            "S" => format!("{unlogged}Sequence"),
            "f" => "Foreign table".into(),
            "p" => format!("{unlogged}Partitioned table"),
            "c" => "Composite type".into(),
            _ => if unlogged.is_empty() { "Table".into() } else { "Unlogged table".into() },
        };
        let title = format!("{what} \"{schema}.{rel}\"");
        let mut opts = self.print.clone();
        opts.footer = false;
        opts.title = Some(title);

        if kind == "S" {
            if let Some((cols, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT pg_catalog.format_type(seqtypid, NULL) AS \"Type\", seqstart AS \"Start\", seqmin AS \"Minimum\", \
                     seqmax AS \"Maximum\", seqincrement AS \"Increment\", CASE WHEN seqcycle THEN 'yes' ELSE 'no' END AS \"Cycles?\", \
                     seqcache AS \"Cache\" FROM pg_catalog.pg_sequence WHERE seqrelid = {oid}"
                ),
            )? {
                outln!("{}", opts.render(&cols, &rows, m.expanded, out::width()));
            }
            let owned = format!(
                "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) || '.' || pg_catalog.quote_ident(a.attname), d.deptype::text \
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_depend d ON c.oid = d.refobjid \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid \
                 WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass \
                 AND d.objid = {oid} AND d.deptype IN ('a', 'i')"
            );
            if let Some((_, rows)) = self.catalog(s, &owned)? {
                for r in rows {
                    let col = table::cell(&r[0], "");
                    if table::cell(&r[1], "") == "i" {
                        outln!("Sequence for identity column: {col}");
                    } else {
                        outln!("Owned by: {col}");
                    }
                }
            }
            outln!();
            return Ok(());
        }

        if kind == "i" || kind == "I" {
            if let Some((cols, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT a.attname AS \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"Type\", \
                     CASE WHEN a.attnum <= (SELECT i.indnkeyatts FROM pg_catalog.pg_index i WHERE i.indexrelid = {oid}) THEN 'yes' ELSE 'no' END AS \"Key?\", \
                     pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, true) AS \"Definition\"{} \
                     FROM pg_catalog.pg_attribute a WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                    if m.verbose {
                        ", CASE a.attstorage WHEN 'p' THEN 'plain' WHEN 'm' THEN 'main' WHEN 'x' THEN 'extended' WHEN 'e' THEN 'external' END AS \"Storage\""
                    } else {
                        ""
                    },
                ),
            )? {
                outln!("{}", opts.render(&cols, &rows, m.expanded, out::width()));
            }
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT i.indisprimary, i.indisunique, am.amname, pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c2.relname), \
                     pg_catalog.pg_get_expr(i.indpred, i.indrelid, true), i.indisvalid \
                     FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid JOIN pg_catalog.pg_am am ON am.oid = c.relam \
                     JOIN pg_catalog.pg_class c2 ON c2.oid = i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid = c2.relnamespace \
                     WHERE i.indexrelid = {oid}"
                ),
            )? {
                if let Some(r) = rows.first() {
                    let mut f = String::new();
                    if r[0] == Value::Bool(true) {
                        f.push_str("primary key, ");
                    } else if r[1] == Value::Bool(true) {
                        f.push_str("unique, ");
                    }
                    f.push_str(&format!("{}, for table \"{}\"", table::cell(&r[2], ""), table::cell(&r[3], "").replace('"', "")));
                    if !r[4].is_null() {
                        f.push_str(&format!(", predicate ({})", table::cell(&r[4], "")));
                    }
                    if r[5] == Value::Bool(false) {
                        f.push_str(", invalid");
                    }
                    outln!("{f}");
                }
            }
            outln!();
            return Ok(());
        }

        // Tables, views, materialized views, foreign tables, composite types.
        let default = if ver >= 120000 {
            "CASE WHEN a.attidentity = 'a' THEN 'generated always as identity' \
             WHEN a.attidentity = 'd' THEN 'generated by default as identity' \
             WHEN a.attgenerated = 's' THEN 'generated always as (' || pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) || ') stored' \
             ELSE pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) END"
        } else {
            "pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)"
        };
        let mut extra = String::new();
        if m.verbose {
            extra.push_str(", CASE a.attstorage WHEN 'p' THEN 'plain' WHEN 'm' THEN 'main' WHEN 'x' THEN 'extended' WHEN 'e' THEN 'external' END AS \"Storage\"");
            if ver >= 140000 && matches!(kind.as_str(), "r" | "p" | "m") {
                extra.push_str(", CASE a.attcompression WHEN 'p' THEN 'pglz' WHEN 'l' THEN 'lz4' END AS \"Compression\"");
            }
            if matches!(kind.as_str(), "r" | "p" | "m" | "f") {
                extra.push_str(", NULLIF(a.attstattarget, -1) AS \"Stats target\"");
            }
            extra.push_str(", pg_catalog.col_description(a.attrelid, a.attnum) AS \"Description\"");
        }
        if let Some((cols, rows)) = self.catalog(
            s,
            &format!(
                "SELECT a.attname AS \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"Type\", \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                  WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS \"Collation\", \
                 CASE WHEN a.attnotnull THEN 'not null' ELSE '' END AS \"Nullable\", {default} AS \"Default\"{extra} \
                 FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                 WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
        )? {
            outln!("{}", opts.render(&cols, &rows, m.expanded, out::width()));
        }

        if is_partition {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT inhparent::pg_catalog.regclass::pg_catalog.text || ' ' || pg_catalog.pg_get_expr(c.relpartbound, c.oid), \
                     pg_catalog.pg_get_partition_constraintdef(c.oid) \
                     FROM pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid WHERE c.oid = {oid}"
                ),
            )? {
                if let Some(r) = rows.first() {
                    outln!("Partition of: {}", table::cell(&r[0], ""));
                    if m.verbose && !r[1].is_null() {
                        outln!("Partition constraint: {}", table::cell(&r[1], ""));
                    }
                }
            }
        }
        if kind == "p" {
            if let Some((_, rows)) = self.catalog(s, &format!("SELECT pg_catalog.pg_get_partkeydef({oid})"))? {
                if let Some(r) = rows.first() {
                    outln!("Partition key: {}", table::cell(&r[0], ""));
                }
            }
        }
        if kind == "f" {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT s.srvname, pg_catalog.array_to_string(ft.ftoptions, ', ') FROM pg_catalog.pg_foreign_table ft \
                     JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver WHERE ft.ftrelid = {oid}"
                ),
            )? {
                if let Some(r) = rows.first() {
                    outln!("Server: {}", table::cell(&r[0], ""));
                    if !r[1].is_null() && table::cell(&r[1], "") != "" {
                        outln!("FDW options: ({})", table::cell(&r[1], ""));
                    }
                }
            }
        }
        if matches!(kind.as_str(), "r" | "p" | "m") {
            self.section(
                s,
                "Indexes",
                &format!(
                    "SELECT c2.relname, i.indisprimary, i.indisunique, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), \
                     con.contype::text, i.indisvalid, i.indisclustered \
                     FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid \
                     LEFT JOIN pg_catalog.pg_constraint con ON con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN ('p','u','x') \
                     WHERE i.indrelid = {oid} ORDER BY i.indisprimary DESC, c2.relname"
                ),
                &|r| {
                    let def = table::cell(&r[3], "");
                    let using = def.split_once(" USING ").map(|(_, u)| u.to_string()).unwrap_or(def);
                    let kind = match (r[1] == Value::Bool(true), r[2] == Value::Bool(true), table::cell(&r[4], "").as_str()) {
                        (true, _, _) => "PRIMARY KEY, ",
                        (_, true, "u") => "UNIQUE CONSTRAINT, ",
                        (_, true, _) => "UNIQUE, ",
                        (_, _, "x") => "EXCLUDE CONSTRAINT, ",
                        _ => "",
                    };
                    let mut line = format!("\"{}\" {kind}{using}", table::cell(&r[0], ""));
                    if r[6] == Value::Bool(true) {
                        line.push_str(" CLUSTER");
                    }
                    if r[5] == Value::Bool(false) {
                        line.push_str(" INVALID");
                    }
                    line
                },
            )?;
        }
        if matches!(kind.as_str(), "r" | "p" | "f") {
            for (title, contype) in [("Check constraints", "c"), ("Foreign-key constraints", "f")] {
                self.section(
                    s,
                    title,
                    &format!(
                        "SELECT conname, pg_catalog.pg_get_constraintdef(oid, true) FROM pg_catalog.pg_constraint \
                         WHERE conrelid = {oid} AND contype = '{contype}' ORDER BY 1"
                    ),
                    &|r| format!("\"{}\" {}", table::cell(&r[0], ""), table::cell(&r[1], "")),
                )?;
            }
            self.section(
                s,
                "Referenced by",
                &format!(
                    "SELECT conrelid::pg_catalog.regclass::pg_catalog.text, conname, pg_catalog.pg_get_constraintdef(oid, true) \
                     FROM pg_catalog.pg_constraint WHERE confrelid = {oid} AND contype = 'f' ORDER BY 1, 2"
                ),
                &|r| format!("TABLE \"{}\" CONSTRAINT \"{}\" {}", table::cell(&r[0], ""), table::cell(&r[1], ""), table::cell(&r[2], "")),
            )?;
        }
        if matches!(kind.as_str(), "r" | "p") {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT pol.polname, pol.polpermissive, \
                     CASE WHEN pol.polroles = '{{0}}' THEN NULL ELSE pg_catalog.array_to_string(array(SELECT rolname FROM pg_catalog.pg_roles \
                      WHERE oid = any (pol.polroles) ORDER BY 1), ',') END, \
                     pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), \
                     CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' END \
                     FROM pg_catalog.pg_policy pol WHERE pol.polrelid = {oid} ORDER BY 1"
                ),
            )? {
                let header = match (rls, forced) {
                    (true, true) => Some("Policies (forced row security enabled):"),
                    (true, false) => Some("Policies:"),
                    (false, _) if !rows.is_empty() => Some("Policies (row security disabled):"),
                    _ => None,
                };
                if let Some(h) = header {
                    outln!("{h}");
                    if rows.is_empty() {
                        outln!("    (none)");
                    }
                    for r in rows {
                        let mut line = format!("    POLICY \"{}\"", table::cell(&r[0], ""));
                        if r[1] == Value::Bool(false) {
                            line.push_str(" AS RESTRICTIVE");
                        }
                        if !r[5].is_null() {
                            line.push_str(&format!(" FOR {}", table::cell(&r[5], "")));
                        }
                        if !r[2].is_null() {
                            line.push_str(&format!("\n      TO {}", table::cell(&r[2], "")));
                        }
                        if !r[3].is_null() {
                            line.push_str(&format!("\n      USING ({})", table::cell(&r[3], "")));
                        }
                        if !r[4].is_null() {
                            line.push_str(&format!("\n      WITH CHECK ({})", table::cell(&r[4], "")));
                        }
                        outln!("{line}");
                    }
                }
            }
        }
        if matches!(kind.as_str(), "r" | "p" | "v" | "m" | "f") {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT pg_catalog.pg_get_triggerdef(t.oid, true), t.tgenabled::text FROM pg_catalog.pg_trigger t \
                     WHERE t.tgrelid = {oid} AND NOT t.tgisinternal ORDER BY t.tgname"
                ),
            )? {
                let def = |r: &Vec<Value>| {
                    let d = table::cell(&r[0], "");
                    d.strip_prefix("CREATE CONSTRAINT TRIGGER ").or_else(|| d.strip_prefix("CREATE TRIGGER ")).unwrap_or(&d).to_string()
                };
                let (enabled, disabled): (Vec<_>, Vec<_>) = rows.iter().partition(|r| table::cell(&r[1], "") != "D");
                for (title, list) in [("Triggers", enabled), ("Disabled user triggers", disabled)] {
                    if !list.is_empty() {
                        outln!("{title}:");
                        for r in list {
                            outln!("    {}", def(r));
                        }
                    }
                }
            }
        }
        if matches!(kind.as_str(), "r" | "p" | "f") && !is_partition {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT c.oid::pg_catalog.regclass::pg_catalog.text FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_inherits i ON c.oid = i.inhparent WHERE i.inhrelid = {oid} ORDER BY inhseqno"
                ),
            )? {
                let names: Vec<String> = rows.iter().map(|r| table::cell(&r[0], "")).collect();
                if !names.is_empty() {
                    outln!("Inherits: {}", names.join(",\n          "));
                }
            }
        }
        if matches!(kind.as_str(), "r" | "p" | "f") {
            if let Some((_, rows)) = self.catalog(
                s,
                &format!(
                    "SELECT c.oid::pg_catalog.regclass::pg_catalog.text, pg_catalog.pg_get_expr(c.relpartbound, c.oid) \
                     FROM pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid \
                     WHERE i.inhparent = {oid} ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text"
                ),
            )? {
                let n = rows.len();
                let (label, count) = if kind == "p" { ("Partitions", "partitions") } else { ("Child tables", "child tables") };
                if m.verbose && n > 0 {
                    let items: Vec<String> = rows
                        .iter()
                        .map(|r| if r[1].is_null() { table::cell(&r[0], "") } else { format!("{} {}", table::cell(&r[0], ""), table::cell(&r[1], "")) })
                        .collect();
                    outln!("{label}: {}", items.join(&format!(",\n{}", " ".repeat(label.len() + 2))));
                } else if n > 0 || kind == "p" {
                    outln!("Number of {count}: {n} (Use \\d+ to list them.)");
                }
            }
        }
        if matches!(kind.as_str(), "r" | "p") && ver >= 100000 {
            self.section(
                s,
                "Publications",
                &format!(
                    "SELECT pubname FROM pg_catalog.pg_publication WHERE puballtables \
                     UNION SELECT p.pubname FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid \
                     WHERE pr.prrelid = {oid} ORDER BY 1"
                ),
                &|r| format!("\"{}\"", table::cell(&r[0], "")),
            )?;
        }
        if m.verbose {
            if matches!(kind.as_str(), "v" | "m") {
                if let Some((_, rows)) = self.catalog(s, &format!("SELECT pg_catalog.pg_get_viewdef({oid}::pg_catalog.oid, true)"))? {
                    if let Some(r) = rows.first() {
                        outln!("View definition:\n{}", table::cell(&r[0], ""));
                    }
                }
            }
            if !access_method.is_empty() && matches!(kind.as_str(), "r" | "m") {
                outln!("Access method: {access_method}");
            }
            if !options.is_empty() {
                outln!("Options: {options}");
            }
        }
        outln!();
        Ok(())
    }

    /// `\sf` / `\sv`: a function's or view's definition.
    fn definition(&mut self, s: &mut Session, what: &str, name: &str) -> Result<Option<String>> {
        let sql = if what == "function" {
            format!("SELECT pg_catalog.pg_get_functiondef({})", catalog::function_oid(name)?)
        } else {
            format!(
                "SELECT CASE c.relkind WHEN 'm' THEN 'CREATE MATERIALIZED VIEW ' ELSE 'CREATE OR REPLACE VIEW ' END || \
                 pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) || ' AS' || E'\\n' || \
                 pg_catalog.rtrim(pg_catalog.pg_get_viewdef(c.oid, true), ';') \
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = {}::pg_catalog.regclass AND c.relkind IN ('v', 'm')",
                lit(name)
            )
        };
        let Some((_, rows)) = self.catalog(s, &sql)? else { return Ok(None) };
        match rows.first() {
            Some(r) => Ok(Some(table::cell(&r[0], ""))),
            None => bail!("\"{name}\" is not a view"),
        }
    }

    /// psql's command_no_begin: statements an implicit BEGIN must not precede.
    fn needs_no_begin(sql: &str) -> bool {
        let words: Vec<String> = sql.split_whitespace().take(3).map(|w| w.trim_end_matches(';').to_uppercase()).collect();
        let w = |i: usize| words.get(i).map(String::as_str).unwrap_or("");
        matches!(w(0), "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT" | "SAVEPOINT" | "RELEASE" | "VACUUM" | "DISCARD")
            || (w(0) == "PREPARE" && w(1) == "TRANSACTION")
            || ((w(0) == "COMMIT" || w(0) == "ROLLBACK") && w(1) == "PREPARED")
            || (matches!(w(0), "CREATE" | "DROP") && matches!(w(1), "DATABASE" | "TABLESPACE"))
            || (w(0) == "CREATE" && w(1) == "INDEX" && w(2) == "CONCURRENTLY")
            || (w(0) == "REINDEX" && matches!(w(1), "DATABASE" | "SYSTEM"))
            || (w(0) == "ALTER" && w(1) == "SYSTEM")
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
        if let Some(key) = &self.restricted {
            if cmd == "unrestrict" && arg.trim() == key {
                self.restricted = None;
                return Ok(Flow::Continue);
            }
            bail!("backslash commands are restricted; only \\unrestrict is allowed");
        }
        if cmd == "restrict" {
            if arg.trim().is_empty() {
                bail!("\\restrict: missing required argument");
            }
            self.restricted = Some(arg.trim().to_string());
            return Ok(Flow::Continue);
        }
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
            "copy" => self.meta_copy(arg, s)?,
            "gdesc" => {
                let sql = self.take_query()?;
                self.last = sql.clone();
                self.gdesc(&sql, s)?;
            }
            "password" => self.password(first, s)?,
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
            "errverbose" => match &self.last_error {
                None => outln!("There is no previous error."),
                Some((info, stmt)) => {
                    let mut t = format_error_with(info, stmt, "verbose", "always");
                    if let Some(r) = info["routine"].as_str() {
                        t.push_str(&format!("\nLOCATION:  {r}, {}:{}", info["file"].as_str().unwrap_or(""), info["line"]));
                    }
                    out::flush();
                    eprintln!("{t}");
                }
            },
            "sf" | "sf+" | "sv" | "sv+" => {
                let what = if cmd.starts_with("sf") { "function" } else { "view" };
                if let Some(def) = self.definition(s, what, arg.trim())? {
                    let text = if cmd.ends_with('+') { catalog::number_lines(&def, what == "function") } else { def };
                    outln!("{}", text.trim_end());
                }
            }
            "ef" | "ev" => {
                let what = if cmd == "ef" { "function" } else { "view" };
                let def = if arg.trim().is_empty() {
                    if what == "function" {
                        "CREATE FUNCTION ( )\n RETURNS \n LANGUAGE \n -- common options:  IMMUTABLE  STABLE  STRICT  SECURITY DEFINER\nAS $function$\n\n$function$\n".into()
                    } else {
                        "CREATE VIEW  AS\n SELECT \n  -- something...\n".into()
                    }
                } else {
                    self.definition(s, what, arg.trim())?.unwrap_or_default()
                };
                let text = super::edit::edit_text(&def, "sql")?;
                self.buf.clear();
                return self.feed(&text, s);
            }
            c if c.starts_with('d') || c.starts_with('l') || c == "z" => {
                let (base, mods) = catalog::parse_mods(c);
                let ver = self.server_version_num();
                if base == "d" && !args.is_empty() {
                    self.describe_matching(s, &args[0], mods)?;
                } else if base.starts_with('d') && base[1..].chars().all(|c| "tvmisE".contains(c)) {
                    let l = catalog::relations(&base[1..], first, mods, ver);
                    self.listing(s, &l, mods.expanded)?;
                } else if let Some(l) = catalog::listing(base, &args, mods, ver) {
                    let l = l?;
                    self.listing(s, &l, mods.expanded)?;
                } else {
                    bail!("invalid command \\{c}\nTry \\? for help.");
                }
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
        if self.copy_in.is_some() {
            return ">> ".into();
        }
        let which = if self.buf.is_empty() { "PROMPT1" } else { "PROMPT2" };
        self.expand_prompt(self.var(which), s)
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.copy_in.is_some() {
            if line == "\\." {
                self.end_copy_in(s)?;
            } else {
                self.copy_line(line);
            }
            return Ok(Flow::Continue);
        }
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
        // End of input ends a COPY's data.
        self.end_copy_in(s)?;
        let rest = self.buf.take();
        if !rest.is_empty() {
            self.execute(&rest, End::Normal, s)?;
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty() || self.copy_in.is_some()
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

    fn reset(&mut self, s: &mut Session) {
        self.cancel_copy_in(s);
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
        self.columns = s
            .query_limit(
                crate::ipc::QuerySpec::Sql {
                    sql: "SELECT c.relname::text, a.attname::text FROM pg_catalog.pg_attribute a \
                          JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                          WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND a.attnum > 0 AND NOT a.attisdropped \
                          AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                          AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1, a.attnum"
                        .into(),
                },
                Some(COLUMN_LIMIT),
            )
            .map(|v| pairs(&v))
            .unwrap_or_default();
        self.superuser = s
            .sql("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")
            .ok()
            .and_then(|v| v["rows"][0][0].as_bool())
            .unwrap_or(false);
        self.sync_conn_vars(s);
    }

    fn completions(&self) -> Completions {
        let metas = [
            "\\q", "\\?", "\\l", "\\c", "\\conninfo", "\\d", "\\dt", "\\dv", "\\di", "\\dn", "\\df", "\\du", "\\ds", "\\dm",
            "\\x", "\\timing", "\\g", "\\gx", "\\gset", "\\gexec", "\\watch", "\\crosstabview", "\\i", "\\ir", "\\echo",
            "\\qecho", "\\warn", "\\p", "\\r", "\\e", "\\w", "\\s", "\\o", "\\set", "\\unset", "\\pset", "\\a", "\\t", "\\f",
            "\\C", "\\H", "\\T", "\\if", "\\elif", "\\else", "\\endif", "\\prompt", "\\getenv", "\\setenv", "\\cd", "\\!",
            "\\copy", "\\gdesc", "\\password", "\\errverbose", "\\sf", "\\sv", "\\ef", "\\ev", "\\z", "\\dp", "\\dx", "\\dT", "\\dD", "\\dE", "\\dy", "\\dL",
            "\\dO", "\\dP", "\\dX", "\\db", "\\dconfig", "\\drds", "\\drg", "\\dRp", "\\dRs", "\\da", "\\dA", "\\dc",
            "\\dC", "\\dd", "\\ddp", "\\do", "\\dF", "\\des", "\\det", "\\deu", "\\dew", "\\dg", "\\dt+", "\\d+",
        ];
        Completions {
            keywords: SQL_KEYWORDS.iter().map(|w| w.to_string()).collect(),
            commands: metas.iter().map(|w| w.to_string()).collect(),
            tables: self.tables.clone(),
            columns: self.columns.clone(),
            other: Vec::new(),
            dotted: false,
        }
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
        assert_eq!(command_tag("create materialized view m as select 1", 1), "SELECT 1");
        assert_eq!(command_tag("create table t2 as select * from t", 4), "SELECT 4");
        assert_eq!(command_tag("create or replace view v as select 1", 0), "CREATE VIEW");
        assert_eq!(command_tag("drop materialized view mv", 0), "DROP MATERIALIZED VIEW");
        assert_eq!(command_tag("create text search configuration c (copy = english)", 0), "CREATE TEXT SEARCH CONFIGURATION");
        assert_eq!(command_tag("create or replace function f() returns int as $$ select 1 $$ language sql", 0), "CREATE FUNCTION");
    }

}
