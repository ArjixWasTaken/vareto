//! MySQL `LOAD DATA LOCAL INFILE`, done by the daemon. sqlx can't answer the
//! server's request for a local file, so the daemon reads the client's file itself
//! (a descriptor the shell passed) and inserts its records in batches, inside one
//! transaction, with the statement's own rules: field and line terminators,
//! enclosures and escapes, `IGNORE n LINES`, column and `@variable` lists, `SET`,
//! `REPLACE`/`IGNORE`, the character set, and the `Records: … Skipped: …` counts.

use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use sqlx::{AssertSqlSafe, MySqlConnection, Row as _};

/// What happens to rows that duplicate a unique key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dup {
    /// `LOCAL` without `REPLACE`: duplicates (and bad values) become warnings.
    Ignore,
    Replace,
}

/// Where an input field goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A column, as written (`a`, `` `a b` ``).
    Column(String),
    /// A user variable (`@x`), name without the `@`, for `SET`.
    Var(String),
}

/// A parsed `LOAD DATA` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadData {
    pub local: bool,
    pub file: String,
    pub dup: Dup,
    /// The table as written (`t`, `db.t`, `` `db`.`t` ``).
    pub table: String,
    /// `PARTITION (…)` as written.
    pub partition: Option<String>,
    pub charset: Option<String>,
    pub fields_terminated: Vec<u8>,
    pub enclosed: Option<u8>,
    pub escaped: Option<u8>,
    pub lines_starting: Vec<u8>,
    pub lines_terminated: Vec<u8>,
    pub ignore_lines: u64,
    /// Empty: every column of the table, in order.
    pub targets: Vec<Target>,
    /// `SET col = expr`, both as written.
    pub set: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A keyword or bare identifier (raw text).
    Word(String),
    /// A quoted string, unescaped.
    Str(Vec<u8>),
    /// A backquoted identifier (raw text, with the backquotes).
    Ident(String),
    /// `@name` or `` @`name` `` (raw text).
    Var(String),
    Number(u64),
    Punct(char),
}

/// Tokens with their byte spans in the statement.
fn tokenize(sql: &str) -> Result<Vec<(Tok, usize, usize)>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && b.get(i + 1) == Some(&b'-') || c == b'#' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        let tok = match c {
            b'\'' | b'"' => {
                let mut v = Vec::new();
                i += 1;
                loop {
                    let Some(&ch) = b.get(i) else { bail!("unterminated string in LOAD DATA") };
                    if ch == b'\\' {
                        let e = *b.get(i + 1).context("unterminated string in LOAD DATA")?;
                        v.push(match e {
                            b'0' => 0,
                            b'b' => 8,
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'Z' => 26,
                            other => other,
                        });
                        i += 2;
                    } else if ch == c && b.get(i + 1) == Some(&c) {
                        v.push(c);
                        i += 2;
                    } else if ch == c {
                        i += 1;
                        break;
                    } else {
                        v.push(ch);
                        i += 1;
                    }
                }
                Tok::Str(v)
            }
            b'`' => {
                i += 1;
                while i < b.len() && !(b[i] == b'`' && b.get(i + 1) != Some(&b'`')) {
                    i += if b[i] == b'`' { 2 } else { 1 };
                }
                i += 1;
                Tok::Ident(sql[start..i.min(b.len())].to_string())
            }
            b'@' => {
                i += 1;
                if b.get(i) == Some(&b'`') {
                    i += 1;
                    while i < b.len() && b[i] != b'`' {
                        i += 1;
                    }
                    i += 1;
                } else {
                    while i < b.len() && (b[i].is_ascii_alphanumeric() || matches!(b[i], b'_' | b'$' | b'.') || b[i] >= 0x80) {
                        i += 1;
                    }
                }
                Tok::Var(sql[start..i.min(b.len())].to_string())
            }
            c if c.is_ascii_digit() => {
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                // `1e3`, `12abc` identifiers are words.
                if i < b.len() && (b[i].is_ascii_alphabetic() || b[i] == b'_') {
                    while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                        i += 1;
                    }
                    Tok::Word(sql[start..i].to_string())
                } else {
                    Tok::Number(sql[start..i].parse()?)
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c == b'$' || c >= 0x80 => {
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80) {
                    i += 1;
                }
                Tok::Word(sql[start..i].to_string())
            }
            other => {
                i += 1;
                Tok::Punct(other as char)
            }
        };
        out.push((tok, start, i.min(b.len())));
    }
    Ok(out)
}

struct Parser<'a> {
    sql: &'a str,
    toks: Vec<(Tok, usize, usize)>,
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.0)
    }

    fn is_word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w))
    }

    fn eat_word(&mut self, w: &str) -> bool {
        let yes = self.is_word(w);
        if yes {
            self.pos += 1;
        }
        yes
    }

    fn expect_word(&mut self, w: &str) -> Result<()> {
        if !self.eat_word(w) {
            bail!("{}", self.syntax_error());
        }
        Ok(())
    }

    fn eat_punct(&mut self, p: char) -> bool {
        let yes = self.peek() == Some(&Tok::Punct(p));
        if yes {
            self.pos += 1;
        }
        yes
    }

    fn string(&mut self) -> Result<Vec<u8>> {
        match self.peek() {
            Some(Tok::Str(s)) => {
                let s = s.clone();
                self.pos += 1;
                Ok(s)
            }
            _ => bail!("{}", self.syntax_error()),
        }
    }

    /// One character (`ENCLOSED BY`, `ESCAPED BY`); empty is none.
    fn char_option(&mut self, what: &str) -> Result<Option<u8>> {
        let s = self.string()?;
        match s.as_slice() {
            [] => Ok(None),
            [c] => Ok(Some(*c)),
            _ => bail!("1083 (42000): Field separator argument is not what is expected; check the manual ({what} must be one character)"),
        }
    }

    /// An identifier, possibly qualified (`db.t`), as written.
    fn name(&mut self) -> Result<String> {
        let start = self.toks.get(self.pos).map(|t| t.1).context("expected a name")?;
        let mut end;
        loop {
            match self.peek() {
                Some(Tok::Word(_) | Tok::Ident(_)) => {
                    end = self.toks[self.pos].2;
                    self.pos += 1;
                }
                _ => bail!("{}", self.syntax_error()),
            }
            if !self.eat_punct('.') {
                break;
            }
        }
        Ok(self.sql[start..end].to_string())
    }

    /// The raw text from the current token up to a top-level `,` (or the end).
    fn expression(&mut self) -> Result<String> {
        let start = self.toks.get(self.pos).map(|t| t.1).context("expected an expression")?;
        let mut end = start;
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            match t {
                Tok::Punct('(') => depth += 1,
                Tok::Punct(')') => depth -= 1,
                Tok::Punct(',') | Tok::Punct(';') if depth == 0 => break,
                _ => {}
            }
            end = self.toks[self.pos].2;
            self.pos += 1;
        }
        if end == start {
            bail!("{}", self.syntax_error());
        }
        Ok(self.sql[start..end].to_string())
    }

    fn syntax_error(&self) -> String {
        let near = self.toks.get(self.pos).map(|t| &self.sql[t.1..]).unwrap_or("");
        let near: String = near.chars().take(80).collect();
        format!("1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{near}'")
    }
}

/// Parse a `LOAD DATA` statement. `None` when `sql` isn't one.
pub fn parse(sql: &str) -> Option<Result<LoadData>> {
    let toks = tokenize(sql).ok()?;
    let mut p = Parser { sql, toks, pos: 0 };
    if !(p.eat_word("LOAD") && p.eat_word("DATA")) {
        return None;
    }
    Some(parse_rest(&mut p))
}

fn parse_rest(p: &mut Parser<'_>) -> Result<LoadData> {
    let _ = p.eat_word("LOW_PRIORITY") || p.eat_word("CONCURRENT");
    let local = p.eat_word("LOCAL");
    p.expect_word("INFILE")?;
    let file = String::from_utf8(p.string()?).context("file name is not UTF-8")?;
    let dup = if p.eat_word("REPLACE") {
        Dup::Replace
    } else {
        p.eat_word("IGNORE");
        Dup::Ignore
    };
    p.expect_word("INTO")?;
    p.expect_word("TABLE")?;
    let table = p.name()?;
    let mut d = LoadData {
        local,
        file,
        dup,
        table,
        partition: None,
        charset: None,
        fields_terminated: b"\t".to_vec(),
        enclosed: None,
        escaped: Some(b'\\'),
        lines_starting: Vec::new(),
        lines_terminated: b"\n".to_vec(),
        ignore_lines: 0,
        targets: Vec::new(),
        set: Vec::new(),
    };
    if p.is_word("PARTITION") {
        let start = p.toks[p.pos].1;
        p.pos += 1;
        if !p.eat_punct('(') {
            bail!("{}", p.syntax_error());
        }
        while !p.eat_punct(')') {
            if p.peek().is_none() {
                bail!("{}", p.syntax_error());
            }
            p.pos += 1;
        }
        d.partition = Some(p.sql[start..p.toks[p.pos - 1].2].to_string());
    }
    if p.eat_word("CHARACTER") {
        p.expect_word("SET")?;
        d.charset = Some(match p.peek() {
            Some(Tok::Word(w)) => w.clone(),
            Some(Tok::Str(s)) => String::from_utf8_lossy(s).into_owned(),
            _ => bail!("{}", p.syntax_error()),
        });
        p.pos += 1;
    } else if p.eat_word("CHARSET") {
        d.charset = Some(match p.peek() {
            Some(Tok::Word(w)) => w.clone(),
            _ => bail!("{}", p.syntax_error()),
        });
        p.pos += 1;
    }
    if p.eat_word("FIELDS") || p.eat_word("COLUMNS") {
        loop {
            if p.eat_word("TERMINATED") {
                p.expect_word("BY")?;
                d.fields_terminated = p.string()?;
            } else if p.eat_word("OPTIONALLY") || p.is_word("ENCLOSED") {
                p.expect_word("ENCLOSED")?;
                p.expect_word("BY")?;
                d.enclosed = p.char_option("ENCLOSED BY")?;
            } else if p.eat_word("ESCAPED") {
                p.expect_word("BY")?;
                d.escaped = p.char_option("ESCAPED BY")?;
            } else {
                break;
            }
        }
    }
    if p.eat_word("LINES") {
        loop {
            if p.eat_word("STARTING") {
                p.expect_word("BY")?;
                d.lines_starting = p.string()?;
            } else if p.eat_word("TERMINATED") {
                p.expect_word("BY")?;
                d.lines_terminated = p.string()?;
            } else {
                break;
            }
        }
    }
    if p.eat_word("IGNORE") {
        match p.peek() {
            Some(Tok::Number(n)) => d.ignore_lines = *n,
            _ => bail!("{}", p.syntax_error()),
        }
        p.pos += 1;
        if !(p.eat_word("LINES") || p.eat_word("ROWS")) {
            bail!("{}", p.syntax_error());
        }
    }
    if p.eat_punct('(') {
        while !p.eat_punct(')') {
            match p.peek() {
                Some(Tok::Var(v)) => {
                    let name = v.trim_start_matches('@').trim_matches('`').to_string();
                    d.targets.push(Target::Var(name));
                    p.pos += 1;
                }
                Some(Tok::Word(_) | Tok::Ident(_)) => d.targets.push(Target::Column(p.name()?)),
                _ => bail!("{}", p.syntax_error()),
            }
            if !p.eat_punct(',') && p.peek() != Some(&Tok::Punct(')')) {
                bail!("{}", p.syntax_error());
            }
        }
    }
    if p.eat_word("SET") {
        loop {
            let col = p.name()?;
            if !p.eat_punct('=') {
                bail!("{}", p.syntax_error());
            }
            d.set.push((col, p.expression()?));
            if !p.eat_punct(',') {
                break;
            }
        }
    }
    p.eat_punct(';');
    if p.peek().is_some() {
        bail!("{}", p.syntax_error());
    }
    Ok(d)
}

/// One input record: its fields (`None` is NULL).
pub type Record = Vec<Option<Vec<u8>>>;

/// Splits input bytes into records with LOAD DATA's rules. Feed it the data as it
/// arrives; it parses what is complete and keeps the rest.
pub struct Splitter {
    fterm: Vec<u8>,
    lterm: Vec<u8>,
    start: Vec<u8>,
    enclosed: Option<u8>,
    escaped: Option<u8>,
    buf: Vec<u8>,
    pos: usize,
}

enum Step {
    Record(Record),
    /// A line without the `LINES STARTING BY` prefix: skipped.
    Skipped,
    NeedMore,
}

impl Splitter {
    pub fn new(d: &LoadData) -> Result<Self> {
        if d.lines_terminated.is_empty() {
            bail!("LOAD DATA with an empty LINES TERMINATED BY is not supported by the vareto shell");
        }
        if d.fields_terminated.is_empty() {
            bail!("fixed-width LOAD DATA (empty FIELDS TERMINATED BY) is not supported by the vareto shell");
        }
        Ok(Self {
            fterm: d.fields_terminated.clone(),
            lterm: d.lines_terminated.clone(),
            start: d.lines_starting.clone(),
            enclosed: d.enclosed,
            escaped: d.escaped,
            buf: Vec::new(),
            pos: 0,
        })
    }

    pub fn feed(&mut self, data: &[u8]) {
        if self.pos > 0 && self.pos >= self.buf.len() / 2 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
    }

    /// The next complete record. `eof`: no more data is coming, so whatever is left
    /// is the last record.
    pub fn next(&mut self, eof: bool) -> Option<Record> {
        loop {
            if self.pos >= self.buf.len() {
                return None;
            }
            match self.step(eof) {
                Step::Record(r) => return Some(r),
                Step::Skipped => continue,
                Step::NeedMore => return None,
            }
        }
    }

    /// Whether `buf[at..]` starts with `pat`: `None` when it can't tell yet (the
    /// buffer ends inside what might be `pat`).
    fn at(&self, at: usize, pat: &[u8], eof: bool) -> Option<bool> {
        let rest = &self.buf[at..];
        if rest.len() >= pat.len() {
            Some(rest.starts_with(pat))
        } else if !eof && pat.starts_with(rest) {
            None
        } else {
            Some(false)
        }
    }

    fn unescape(c: u8) -> u8 {
        match c {
            b'0' => 0,
            b'b' => 8,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'Z' => 26,
            other => other,
        }
    }

    fn step(&mut self, eof: bool) -> Step {
        let mut p = self.pos;
        // LINES STARTING BY: skip up to and including the prefix, or the whole line
        // when it doesn't have one.
        if !self.start.is_empty() {
            loop {
                if p >= self.buf.len() {
                    if eof {
                        self.pos = p;
                        return Step::Skipped;
                    }
                    return Step::NeedMore;
                }
                match (self.at(p, &self.start, eof), self.at(p, &self.lterm, eof)) {
                    (Some(true), _) => {
                        p += self.start.len();
                        break;
                    }
                    (_, Some(true)) => {
                        self.pos = p + self.lterm.len();
                        return Step::Skipped;
                    }
                    (None, _) | (_, None) => return Step::NeedMore,
                    _ => p += 1,
                }
            }
        }
        let mut fields: Record = Vec::new();
        loop {
            // One field.
            let mut val = Vec::new();
            let mut null = false;
            let enclosed = self.enclosed.is_some_and(|e| self.buf.get(p) == Some(&e));
            let mut record_done = false;
            if enclosed {
                let e = self.enclosed.unwrap();
                p += 1;
                loop {
                    let Some(&c) = self.buf.get(p) else {
                        if eof {
                            record_done = true;
                            break;
                        }
                        return Step::NeedMore;
                    };
                    if self.escaped == Some(c) {
                        let Some(&n) = self.buf.get(p + 1) else {
                            if eof {
                                val.push(c);
                                p += 1;
                                continue;
                            }
                            return Step::NeedMore;
                        };
                        val.push(Self::unescape(n));
                        p += 2;
                        continue;
                    }
                    if c == e {
                        match self.buf.get(p + 1) {
                            Some(&n) if n == e => {
                                val.push(e);
                                p += 2;
                                continue;
                            }
                            None if !eof => return Step::NeedMore,
                            None => {
                                p += 1;
                                record_done = true;
                                break;
                            }
                            Some(_) => {}
                        }
                        match (self.at(p + 1, &self.lterm, eof), self.at(p + 1, &self.fterm, eof)) {
                            (Some(true), _) => {
                                p += 1 + self.lterm.len();
                                record_done = true;
                                break;
                            }
                            (_, Some(true)) => {
                                p += 1 + self.fterm.len();
                                break;
                            }
                            (None, _) | (_, None) => return Step::NeedMore,
                            // An enclosing character inside the field is part of it.
                            _ => {
                                val.push(c);
                                p += 1;
                                continue;
                            }
                        }
                    }
                    val.push(c);
                    p += 1;
                }
            } else {
                let mut escapes = false;
                loop {
                    if p >= self.buf.len() {
                        if eof {
                            record_done = true;
                            break;
                        }
                        return Step::NeedMore;
                    }
                    match (self.at(p, &self.lterm, eof), self.at(p, &self.fterm, eof)) {
                        (Some(true), _) => {
                            p += self.lterm.len();
                            record_done = true;
                            break;
                        }
                        (_, Some(true)) => {
                            p += self.fterm.len();
                            break;
                        }
                        (None, _) | (_, None) => return Step::NeedMore,
                        _ => {}
                    }
                    let c = self.buf[p];
                    if self.escaped == Some(c) {
                        let Some(&n) = self.buf.get(p + 1) else {
                            if eof {
                                val.push(c);
                                p += 1;
                                continue;
                            }
                            return Step::NeedMore;
                        };
                        // `\N` alone is NULL.
                        if n == b'N' && val.is_empty() && !escapes {
                            null = true;
                        } else {
                            null = false;
                        }
                        escapes = true;
                        val.push(Self::unescape(n));
                        p += 2;
                        continue;
                    }
                    null = false;
                    val.push(c);
                    p += 1;
                }
                // `\N` must be the whole field.
                null = null && val == b"N";
                // The word NULL is NULL when fields may be enclosed, or nothing escapes.
                if !escapes && val == b"NULL" && (self.enclosed.is_some() || self.escaped.is_none()) {
                    null = true;
                }
            }
            fields.push(if null { None } else { Some(val) });
            if record_done {
                break;
            }
        }
        self.pos = p;
        Step::Record(fields)
    }
}

/// An identifier the server can't misread: letters, digits, `_`.
fn plain_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn backquote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Split `db.t` / `` `db`.`t` `` into its unquoted parts.
fn name_parts(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '`' if quoted && chars.peek() == Some(&'`') => {
                chars.next();
                cur.push('`');
            }
            '`' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    parts
}

/// A `SET` expression with references to input variables and input columns turned
/// into the staged values (`v.s0`, ...). Quoted text and function names stay.
fn rewrite_set(expr: &str, staged: &dyn Fn(&Target) -> Option<String>) -> Result<String> {
    let toks = tokenize(expr)?;
    let mut out = String::new();
    let mut last = 0;
    for (i, (t, s, e)) in toks.iter().enumerate() {
        let next_is_call = matches!(toks.get(i + 1), Some((Tok::Punct('('), _, _)));
        let prev_is_dot = i > 0 && matches!(toks[i - 1].0, Tok::Punct('.'));
        let replacement = match t {
            Tok::Var(v) => staged(&Target::Var(v.trim_start_matches('@').trim_matches('`').to_string())),
            Tok::Word(_) | Tok::Ident(_) if !next_is_call && !prev_is_dot => staged(&Target::Column(expr[*s..*e].to_string())),
            _ => None,
        };
        if let Some(r) = replacement {
            out.push_str(&expr[last..*s]);
            out.push_str(&r);
            last = *e;
        }
    }
    out.push_str(&expr[last..]);
    Ok(out)
}

/// The MySQL error a client shows when LOCAL INFILE is off on its side or the
/// server's.
fn disabled(mariadb: bool, server_side: bool) -> anyhow::Error {
    if mariadb {
        anyhow::anyhow!("4166 (HY000): The used command is not allowed because the MariaDB server or client has disabled the local infile capability")
    } else if server_side {
        anyhow::anyhow!("3948 (42000): Loading local data is disabled; this must be enabled on both the client and server sides")
    } else {
        anyhow::anyhow!("2068 (HY000): LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.")
    }
}

/// How the shell asks for the load.
pub struct LoadRequest<'a> {
    pub sql: &'a str,
    pub src: OwnedFd,
    /// `--local-infile` on the client side.
    pub client_allowed: bool,
    /// The shell has a transaction open: use a savepoint, don't commit.
    pub in_transaction: bool,
    /// The client couldn't open the file; reported after the server's checks, as
    /// the real client does.
    pub file_error: Option<String>,
    /// Also return the warnings themselves.
    pub list_warnings: bool,
    pub cancelled: &'a AtomicBool,
    /// The client's socket: a load whose client went away is rolled back.
    pub peer: Option<OwnedFd>,
}

/// Rows per INSERT, at most.
const BATCH_ROWS: usize = 1000;
/// Warnings kept for `warnings` / `\W` (MySQL's default `max_error_count`).
const MAX_WARNINGS: usize = 1024;

struct Batch {
    rows: Vec<Vec<Option<Vec<u8>>>>,
    /// Input fields present in these rows (rows with fewer fields go in their own
    /// INSERT, leaving the missing columns to their defaults).
    present: usize,
    bytes: usize,
}

struct Loader<'a> {
    d: &'a LoadData,
    charset: Option<String>,
    columns: Vec<Target>,
    verb: &'static str,
    table: String,
    records: u64,
    affected: u64,
    deleted: u64,
    warnings: u64,
    list: Vec<Value>,
    list_warnings: bool,
    max_bytes: usize,
    /// This batch's own warnings (missing or extra fields): (row, code, message).
    pending: Vec<(u64, u64, String)>,
    /// The row number of the batch's first row.
    batch_first: u64,
}

impl Loader<'_> {
    /// `CONVERT(? USING cs)`, so the file's bytes are read in the load's character set.
    fn param(&self) -> String {
        match &self.charset {
            Some(cs) => format!("CONVERT(? USING {cs})"),
            None => "?".into(),
        }
    }

    /// Staged value name of input position `i`.
    fn staged(i: usize) -> String {
        format!("v.`s{i}`")
    }

    fn insert_sql(&self, present: usize, rows: usize) -> Result<String> {
        let targets = &self.columns[..present.min(self.columns.len())];
        let partition = self.d.partition.as_deref().map(|p| format!(" {p}")).unwrap_or_default();
        let assigned: Vec<(usize, &String)> = targets
            .iter()
            .enumerate()
            .filter_map(|(i, t)| match t {
                Target::Column(c) => Some((i, c)),
                Target::Var(_) => None,
            })
            .collect();
        if self.d.set.is_empty() {
            let cols: Vec<&str> = assigned.iter().map(|(_, c)| c.as_str()).collect();
            let one = format!("({})", vec![self.param(); targets.len()].join(", "));
            // Variables without SET just drop their field: bind it, select only the columns.
            if assigned.len() == targets.len() {
                return Ok(format!("{} INTO {}{partition} ({}) VALUES {}", self.verb, self.table, cols.join(", "), vec![one; rows].join(", ")));
            }
        }
        // Staged rows, then INSERT … SELECT with the SET expressions over them.
        let staged_row = |first: bool| {
            (0..targets.len())
                .map(|i| if first { format!("{} AS `s{i}`", self.param()) } else { self.param() })
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut derived = format!("SELECT {}", staged_row(true));
        for _ in 1..rows {
            derived.push_str(" UNION ALL SELECT ");
            derived.push_str(&staged_row(false));
        }
        let lookup = |t: &Target| -> Option<String> {
            let i = targets.iter().position(|x| match (x, t) {
                (Target::Var(a), Target::Var(b)) => a.eq_ignore_ascii_case(b),
                (Target::Column(a), Target::Column(b)) => name_parts(a).last().map(|s| s.to_lowercase()) == name_parts(b).last().map(|s| s.to_lowercase()),
                _ => false,
            });
            match (i, t) {
                (Some(i), _) => Some(Self::staged(i)),
                // An input variable this row has no field for.
                (None, Target::Var(v)) if self.columns.iter().any(|x| matches!(x, Target::Var(a) if a.eq_ignore_ascii_case(v))) => Some("NULL".into()),
                _ => None,
            }
        };
        let mut cols: Vec<String> = assigned.iter().map(|(_, c)| (*c).clone()).collect();
        let mut exprs: Vec<String> = assigned.iter().map(|(i, _)| Self::staged(*i)).collect();
        for (c, e) in &self.d.set {
            cols.push(c.clone());
            exprs.push(rewrite_set(e, &lookup)?);
        }
        Ok(format!("{} INTO {}{partition} ({}) SELECT {} FROM ({derived}) AS v", self.verb, self.table, cols.join(", "), exprs.join(", ")))
    }

    async fn flush(&mut self, c: &mut MySqlConnection, b: &mut Batch) -> Result<()> {
        if b.rows.is_empty() {
            return Ok(());
        }
        let n = b.rows.len();
        let sql = self.insert_sql(b.present, n)?;
        let mut q = sqlx::query(AssertSqlSafe(sql)).persistent(false);
        for row in b.rows.drain(..) {
            for v in row {
                q = q.bind(v);
            }
        }
        let res = q.execute(&mut *c).await?;
        let affected = res.rows_affected();
        self.affected += affected;
        if self.d.dup == Dup::Replace {
            self.deleted += affected.saturating_sub(n as u64);
        }
        // SHOW WARNINGS before @@warning_count: any other statement clears them.
        let server = if self.list_warnings && self.list.len() < MAX_WARNINGS {
            sqlx::raw_sql("SHOW WARNINGS")
                .fetch_all(&mut *c)
                .await?
                .iter()
                .map(|r| {
                    let text = |i: usize| r.try_get_unchecked::<&str, _>(i).unwrap_or("").to_string();
                    (text(0), text(1).parse::<u64>().unwrap_or(0), text(2))
                })
                .collect()
        } else {
            Vec::new()
        };
        let row = sqlx::raw_sql("SELECT @@warning_count").fetch_one(&mut *c).await?;
        self.warnings += row.try_get_unchecked::<&str, _>(0).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        self.merge_warnings(server);
        b.bytes = 0;
        self.batch_first = self.records + 1;
        Ok(())
    }

    /// A warning of the load itself (missing or extra fields) for `row`.
    fn warn(&mut self, row: u64, code: u64, message: String) {
        self.warnings += 1;
        self.pending.push((row, code, message));
    }

    /// Add this batch's warnings to the list in the order the real statement gives
    /// them: by row; within a row, the server's own warnings (`at row k`, which is
    /// made the file's row) first, then missing or extra fields, then warnings
    /// without a row (duplicate keys).
    fn merge_warnings(&mut self, server: Vec<(String, u64, String)>) {
        let pending = std::mem::take(&mut self.pending);
        if !self.list_warnings {
            return;
        }
        let mut all: Vec<((u64, u8, usize), Value)> = Vec::new();
        let mut last_row = self.batch_first;
        for (i, (level, code, msg)) in server.into_iter().enumerate() {
            let (key, msg) = match msg.rfind(" at row ").and_then(|at| msg[at + 8..].parse::<u64>().ok().map(|k| (at, k))) {
                Some((at, k)) => {
                    let row = self.batch_first + k - 1;
                    last_row = row;
                    ((row, 0, i), format!("{} at row {row}", &msg[..at]))
                }
                None => ((last_row, 2, i), msg),
            };
            all.push((key, json!({ "level": level, "code": code, "message": msg })));
        }
        for (i, (row, code, msg)) in pending.into_iter().enumerate() {
            all.push(((row, 1, i), json!({ "level": "Warning", "code": code, "message": msg })));
        }
        all.sort_by_key(|(k, _)| *k);
        for (_, w) in all {
            if self.list.len() >= MAX_WARNINGS {
                break;
            }
            self.list.push(w);
        }
    }
}

async fn scalar(c: &mut MySqlConnection, sql: &str) -> Result<Option<String>> {
    let row = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_one(&mut *c).await?;
    Ok(row.try_get_unchecked::<Option<String>, _>(0).ok().flatten())
}

/// Run a `LOAD DATA LOCAL INFILE` on `c`. Returns
/// `{"affected", "records", "deleted", "skipped", "warnings": {"count", "list"}}`.
pub async fn load(c: &mut MySqlConnection, req: LoadRequest<'_>) -> Result<Value> {
    use tokio::io::AsyncReadExt as _;
    let d = match parse(req.sql) {
        Some(r) => r?,
        None => bail!("not a LOAD DATA statement"),
    };
    let version = scalar(c, "SELECT VERSION()").await?.unwrap_or_default();
    let mariadb = version.to_lowercase().contains("mariadb");
    // The real client's order: the server's setting, the client's, then the file.
    if scalar(c, "SELECT @@GLOBAL.local_infile").await?.as_deref() == Some("0") {
        return Err(disabled(mariadb, true));
    }
    if !req.client_allowed {
        return Err(disabled(mariadb, false));
    }
    if let Some(e) = req.file_error {
        bail!(e);
    }
    let mut split = Splitter::new(&d)?;
    // Fail on a missing table before reading anything.
    sqlx::raw_sql(AssertSqlSafe(format!("SELECT * FROM {} LIMIT 0", d.table))).execute(&mut *c).await?;
    let columns = if d.targets.is_empty() {
        let parts = name_parts(&d.table);
        let (schema, table) = match parts.as_slice() {
            [db, t] => (crate::db::sql::quote_lit(db), crate::db::sql::quote_lit(t)),
            [t] => ("DATABASE()".to_string(), crate::db::sql::quote_lit(t)),
            _ => bail!("unexpected table name {}", d.table),
        };
        let rows = sqlx::raw_sql(AssertSqlSafe(format!(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table} \
             AND extra NOT LIKE '%GENERATED%' ORDER BY ordinal_position"
        )))
        .fetch_all(&mut *c)
        .await?;
        rows.iter().map(|r| Target::Column(backquote(r.try_get_unchecked::<&str, _>(0).unwrap_or_default()))).collect()
    } else {
        d.targets.clone()
    };
    let charset = match &d.charset {
        Some(cs) => cs.clone(),
        None => scalar(c, "SELECT @@character_set_database").await?.unwrap_or_else(|| "utf8mb4".into()),
    };
    if !plain_ident(&charset) {
        bail!("1115 (42000): Unknown character set: '{charset}'");
    }
    let charset = (!charset.eq_ignore_ascii_case("binary")).then_some(charset);
    let max_packet: usize = scalar(c, "SELECT @@max_allowed_packet").await?.and_then(|s| s.parse().ok()).unwrap_or(16 << 20);

    // One transaction for the whole load (a savepoint inside the session's own).
    let in_tx = req.in_transaction
        || scalar(c, "SELECT @@autocommit").await?.as_deref() == Some("0")
        || (mariadb && scalar(c, "SELECT @@in_transaction").await.ok().flatten().as_deref() == Some("1"));
    let begin = if in_tx { "SAVEPOINT vareto_load_data" } else { "START TRANSACTION" };
    sqlx::raw_sql(begin).execute(&mut *c).await?;
    // REPLACE (no IGNORE) in strict mode: missing or extra fields are errors, as are
    // bad values (those the server reports itself).
    let strict = d.dup == Dup::Replace && scalar(c, "SELECT @@SESSION.sql_mode").await?.unwrap_or_default().contains("STRICT_");

    let mut loader = Loader {
        d: &d,
        charset,
        columns,
        verb: if d.dup == Dup::Replace { "REPLACE" } else { "INSERT IGNORE" },
        table: d.table.clone(),
        records: 0,
        affected: 0,
        deleted: 0,
        warnings: 0,
        list: Vec::new(),
        list_warnings: req.list_warnings,
        max_bytes: (max_packet / 2).max(64 << 10),
        pending: Vec::new(),
        batch_first: 1,
    };
    let res: Result<()> = async {
        let mut src = tokio::fs::File::from_std(std::fs::File::from(req.src));
        let mut buf = vec![0u8; 1 << 20];
        let mut batch = Batch { rows: Vec::new(), present: usize::MAX, bytes: 0 };
        let mut ignored = 0u64;
        let ncols = loader.columns.len();
        let per_batch = (65_535 / ncols.max(1)).clamp(1, BATCH_ROWS);
        let mut eof = false;
        loop {
            if req.cancelled.load(Ordering::SeqCst) {
                bail!("1317 (70100): Query execution was interrupted");
            }
            if !eof {
                let n = src.read(&mut buf).await.context("reading the file")?;
                if n == 0 {
                    eof = true;
                } else {
                    split.feed(&buf[..n]);
                }
            }
            while let Some(fields) = split.next(eof) {
                if ignored < d.ignore_lines {
                    ignored += 1;
                    continue;
                }
                let present = fields.len().min(ncols);
                let size: usize = fields.iter().take(ncols).map(|f| f.as_ref().map_or(0, Vec::len) + 8).sum();
                if !batch.rows.is_empty() && (present != batch.present || batch.rows.len() >= per_batch || batch.bytes + size > loader.max_bytes) {
                    loader.flush(c, &mut batch).await?;
                }
                loader.records += 1;
                let row_no = loader.records;
                let mut fields = fields;
                if fields.len() > ncols {
                    let msg = format!("Row {row_no} was truncated; it contained more data than there were input columns");
                    if strict {
                        bail!("1262 (01000): {msg}");
                    }
                    loader.warn(row_no, 1262, msg);
                    fields.truncate(ncols);
                } else if fields.len() < ncols {
                    let msg = format!("Row {row_no} doesn't contain data for all columns");
                    if strict {
                        bail!("1261 (01000): {msg}");
                    }
                    // One per missing column.
                    for _ in fields.len()..ncols {
                        loader.warn(row_no, 1261, msg.clone());
                    }
                }
                batch.present = present;
                batch.bytes += size;
                batch.rows.push(fields);
            }
            if eof {
                break;
            }
        }
        loader.flush(c, &mut batch).await?;
        if req.cancelled.load(Ordering::SeqCst) {
            bail!("1317 (70100): Query execution was interrupted");
        }
        if req.peer.as_ref().is_some_and(|p| crate::ipc::peer_closed(std::os::fd::AsFd::as_fd(p))) {
            bail!("the client went away during LOAD DATA");
        }
        Ok(())
    }
    .await;
    let end = match (&res, in_tx) {
        (Ok(()), true) => "RELEASE SAVEPOINT vareto_load_data",
        (Ok(()), false) => "COMMIT",
        (Err(_), true) => "ROLLBACK TO SAVEPOINT vareto_load_data",
        (Err(_), false) => "ROLLBACK",
    };
    let ended = sqlx::raw_sql(end).execute(&mut *c).await;
    res?;
    ended?;
    let skipped = if d.dup == Dup::Replace { 0 } else { loader.records.saturating_sub(loader.affected) };
    let mut warnings = json!({ "count": loader.warnings });
    if req.list_warnings && !loader.list.is_empty() {
        warnings["list"] = Value::Array(loader.list);
    }
    Ok(json!({
        "load_data": true,
        "affected": loader.affected,
        "records": loader.records,
        "deleted": loader.deleted,
        "skipped": skipped,
        "warnings": warnings,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(d: &LoadData, input: &[u8], chunk: usize) -> Vec<Record> {
        let mut s = Splitter::new(d).unwrap();
        let mut out = Vec::new();
        for c in input.chunks(chunk.max(1)) {
            s.feed(c);
            while let Some(r) = s.next(false) {
                out.push(r);
            }
        }
        while let Some(r) = s.next(true) {
            out.push(r);
        }
        out
    }

    fn f(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    #[test]
    fn statement() {
        let d = parse(
            "load data local infile '/tmp/a b.csv' replace into table `shop`.`t` character set latin1 \
             fields terminated by ',' optionally enclosed by '\"' escaped by '\\\\' \
             lines starting by 'x:' terminated by '\\r\\n' ignore 1 lines (id, @n, `full name`) \
             set note = upper(@n), created = now()",
        )
        .unwrap()
        .unwrap();
        assert!(d.local);
        assert_eq!(d.file, "/tmp/a b.csv");
        assert_eq!(d.dup, Dup::Replace);
        assert_eq!(d.table, "`shop`.`t`");
        assert_eq!(d.charset.as_deref(), Some("latin1"));
        assert_eq!((d.fields_terminated.as_slice(), d.enclosed, d.escaped), (b",".as_slice(), Some(b'"'), Some(b'\\')));
        assert_eq!((d.lines_starting.as_slice(), d.lines_terminated.as_slice()), (b"x:".as_slice(), b"\r\n".as_slice()));
        assert_eq!(d.ignore_lines, 1);
        assert_eq!(d.targets, vec![Target::Column("id".into()), Target::Var("n".into()), Target::Column("`full name`".into())]);
        assert_eq!(d.set, vec![("note".into(), "upper(@n)".into()), ("created".into(), "now()".into())]);
        assert!(parse("select 1").is_none());
        assert!(parse("load data infile 'x' into t").unwrap().is_err());
        let d = parse("LOAD DATA LOCAL INFILE 'x' INTO TABLE t").unwrap().unwrap();
        assert_eq!((d.dup, d.fields_terminated.as_slice(), d.escaped), (Dup::Ignore, b"\t".as_slice(), Some(b'\\')));
    }

    #[test]
    fn default_format() {
        let d = parse("load data local infile 'x' into table t").unwrap().unwrap();
        let input = b"1\ta\\tb\t\\N\n2\t\\\\N\tc\\\nd\n3\n";
        for chunk in [1, 2, 7, 100] {
            assert_eq!(
                records(&d, input, chunk),
                vec![vec![f("1"), f("a\tb"), None], vec![f("2"), f("\\N"), f("c\nd")], vec![f("3")]],
                "chunk {chunk}"
            );
        }
        // The last line needs no terminator.
        assert_eq!(records(&d, b"1\t2", 1), vec![vec![f("1"), f("2")]]);
    }

    #[test]
    fn csv_format() {
        let d = parse("load data local infile 'x' into table t fields terminated by ',' enclosed by '\"' lines terminated by '\\r\\n'")
            .unwrap()
            .unwrap();
        let input = b"1,\"a,b\",\"say \"\"hi\"\"\"\r\n2,NULL,\"NULL\"\r\n3,\"multi\r\nline\",x\"y\r\n";
        for chunk in [1, 3, 100] {
            assert_eq!(
                records(&d, input, chunk),
                vec![
                    vec![f("1"), f("a,b"), f("say \"hi\"")],
                    vec![f("2"), None, f("NULL")],
                    vec![f("3"), f("multi\r\nline"), f("x\"y")],
                ],
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn starting_by() {
        let d = parse("load data local infile 'x' into table t fields terminated by ',' lines starting by 'xxx'").unwrap().unwrap();
        let input = b"xxx1,a\nskip me\nyyyxxx2,b\n";
        assert_eq!(records(&d, input, 1), vec![vec![f("1"), f("a")], vec![f("2"), f("b")]]);
    }

    #[test]
    fn set_rewrite() {
        let targets = [Target::Column("id".into()), Target::Var("n".into())];
        let lookup = |t: &Target| targets.iter().position(|x| x == t).map(|i| format!("v.`s{i}`"));
        assert_eq!(rewrite_set("upper(@n) || id || 'id @n'", &lookup).unwrap(), "upper(v.`s1`) || v.`s0` || 'id @n'");
        assert_eq!(rewrite_set("id(@n)", &lookup).unwrap(), "id(v.`s1`)");
    }

    #[test]
    fn insert_statements() {
        let d = parse("load data local infile 'x' into table t (a, @skip, b)").unwrap().unwrap();
        let l = Loader {
            d: &d,
            charset: Some("utf8mb4".into()),
            columns: d.targets.clone(),
            verb: "INSERT IGNORE",
            table: d.table.clone(),
            records: 0,
            affected: 0,
            deleted: 0,
            warnings: 0,
            list: Vec::new(),
            list_warnings: false,
            max_bytes: 1 << 20,
            pending: Vec::new(),
            batch_first: 1,
        };
        // A dropped variable needs the staged form; short rows list fewer columns.
        assert_eq!(
            l.insert_sql(3, 2).unwrap(),
            "INSERT IGNORE INTO t (a, b) SELECT v.`s0`, v.`s2` FROM (SELECT CONVERT(? USING utf8mb4) AS `s0`, CONVERT(? USING utf8mb4) AS `s1`, \
             CONVERT(? USING utf8mb4) AS `s2` UNION ALL SELECT CONVERT(? USING utf8mb4), CONVERT(? USING utf8mb4), CONVERT(? USING utf8mb4)) AS v"
        );
        let d2 = parse("load data local infile 'x' replace into table t (a, b)").unwrap().unwrap();
        let l2 = Loader { d: &d2, columns: d2.targets.clone(), verb: "REPLACE", charset: None, ..l };
        assert_eq!(l2.insert_sql(1, 2).unwrap(), "REPLACE INTO t (a) VALUES (?), (?)");
    }
}
