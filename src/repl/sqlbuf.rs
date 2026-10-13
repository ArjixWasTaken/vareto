//! Splitting typed SQL into statements, the way the native clients do: a statement
//! ends at the delimiter (or MySQL `\G`/`\g`) outside quotes and comments.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Mysql,
    Postgres,
    Sqlite,
}

/// How a statement was terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// `;` (or the custom delimiter), or `\g`.
    Normal,
    /// MySQL `\G` / psql `\gx`: print vertically.
    Vertical,
}

/// What an unfinished buffer is waiting for (for continuation prompts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Open {
    Statement,
    Quote(char),
    Comment,
    Dollar,
}

/// Where scanning stands at the end of the buffered text, so the next line can be
/// read in the right context (inside a quote, a comment, a dollar-quote).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Code,
    Quote(char),
    BlockComment,
    /// Inside `$tag$ … $tag$`; the tag includes the dollars.
    Dollar(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Code,
    Quote(char),
    LineComment,
    BlockComment,
    Dollar,
}

pub struct SqlBuf {
    dialect: Dialect,
    pub delimiter: String,
    buf: String,
}

impl SqlBuf {
    pub fn new(dialect: Dialect) -> Self {
        Self { dialect, delimiter: ";".into(), buf: String::new() }
    }

    /// Lines buffered so far.
    pub fn lines(&self) -> usize {
        if self.buf.is_empty() { 0 } else { self.buf.lines().count() }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.trim().is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn take(&mut self) -> String {
        std::mem::take(&mut self.buf).trim().to_string()
    }

    /// Add a line; returns the statements it completed. The rest stays buffered.
    pub fn push_line(&mut self, line: &str) -> Vec<(String, End)> {
        if !self.buf.is_empty() {
            self.buf.push('\n');
        }
        self.buf.push_str(line);
        let mut out = Vec::new();
        loop {
            let (stmts, rest) = split_one(self.dialect, &self.delimiter, &self.buf);
            match stmts {
                Some((s, end)) => {
                    let s = s.trim().to_string();
                    self.buf = rest.to_string();
                    if !s.is_empty() {
                        out.push((s, end));
                    }
                }
                None => break,
            }
        }
        if self.buf.trim().is_empty() {
            self.buf.clear();
        }
        out
    }

    /// The scanning context the next line starts in.
    pub fn mode(&self) -> Mode {
        if self.buf.is_empty() {
            return Mode::Code;
        }
        // The newline a next line brings ends any `--` comment.
        let text = format!("{}\n", self.buf);
        match scan(self.dialect, None, &text) {
            Scan::Open(State::Quote(q), _) => Mode::Quote(q),
            Scan::Open(State::BlockComment, _) => Mode::BlockComment,
            Scan::Open(State::Dollar, tag) => Mode::Dollar(tag),
            _ => Mode::Code,
        }
    }

    /// What the buffered text is waiting for, if anything.
    pub fn open(&self) -> Option<Open> {
        if self.is_empty() {
            return None;
        }
        Some(match scan_state(self.dialect, &self.buf) {
            State::Quote(c) => Open::Quote(c),
            State::BlockComment => Open::Comment,
            State::Dollar => Open::Dollar,
            State::Code | State::LineComment => Open::Statement,
        })
    }
}

/// Find the first complete statement in `s`. Returns it with the remainder.
fn split_one<'a>(dialect: Dialect, delim: &str, s: &'a str) -> (Option<(&'a str, End)>, &'a str) {
    match scan(dialect, Some(delim), s) {
        Scan::Statement { at, len, end } => (Some((&s[..at], end)), &s[at + len..]),
        Scan::Open(..) => (None, s),
    }
}

fn scan_state(dialect: Dialect, s: &str) -> State {
    match scan(dialect, None, s) {
        Scan::Open(state, _) => state,
        Scan::Statement { .. } => State::Code,
    }
}

enum Scan {
    /// A terminator of `len` bytes at `at`.
    Statement { at: usize, len: usize, end: End },
    /// No terminator; the state at the end of the text (and the open dollar tag).
    Open(State, String),
}

/// Walk `s` tracking quotes, comments and dollar-quotes. With `delim`, stop at the
/// first terminator outside them.
fn scan(dialect: Dialect, delim: Option<&str>, s: &str) -> Scan {
    let b = s.as_bytes();
    let mut state = State::Code;
    let mut dollar_tag = String::new();
    let mut i = 0;
    while i < b.len() {
        if !s.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let c = b[i] as char;
        match state {
            State::Code => {
                if let Some(delim) = delim {
                    if s[i..].starts_with(delim) {
                        return Scan::Statement { at: i, len: delim.len(), end: End::Normal };
                    }
                    if dialect == Dialect::Mysql && (s[i..].starts_with("\\G") || s[i..].starts_with("\\g")) {
                        let end = if b[i + 1] == b'G' { End::Vertical } else { End::Normal };
                        return Scan::Statement { at: i, len: 2, end };
                    }
                }
                match c {
                    '\'' | '"' => state = State::Quote(c),
                    '`' if dialect != Dialect::Postgres => state = State::Quote(c),
                    '-' if s[i..].starts_with("--") => state = State::LineComment,
                    '#' if dialect == Dialect::Mysql => state = State::LineComment,
                    '/' if s[i..].starts_with("/*") => {
                        state = State::BlockComment;
                        i += 1;
                    }
                    '$' if dialect == Dialect::Postgres => {
                        if let Some(tag) = dollar_open(&s[i..]) {
                            i += tag.len() - 1;
                            dollar_tag = tag;
                            state = State::Dollar;
                        }
                    }
                    _ => {}
                }
            }
            State::Quote(q) => {
                if c == '\\' && dialect == Dialect::Mysql {
                    i += 1;
                } else if c == q {
                    // A doubled quote is an escaped quote.
                    if b.get(i + 1) == Some(&(q as u8)) {
                        i += 1;
                    } else {
                        state = State::Code;
                    }
                }
            }
            State::LineComment => {
                if c == '\n' {
                    state = State::Code;
                }
            }
            State::BlockComment => {
                if s[i..].starts_with("*/") {
                    state = State::Code;
                    i += 1;
                }
            }
            State::Dollar => {
                if s[i..].starts_with(&dollar_tag) {
                    i += dollar_tag.len() - 1;
                    state = State::Code;
                }
            }
        }
        i += 1;
    }
    Scan::Open(state, dollar_tag)
}

/// `$$` or `$tag$` at the start of `s`.
fn dollar_open(s: &str) -> Option<String> {
    let rest = &s[1..];
    let end = rest.find('$')?;
    let tag = &rest[..end];
    if tag.chars().all(|c| c.is_alphanumeric() || c == '_') && !tag.starts_with(|c: char| c.is_ascii_digit()) {
        Some(format!("${tag}$"))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(d: Dialect, lines: &[&str]) -> (Vec<(String, End)>, SqlBuf) {
        let mut b = SqlBuf::new(d);
        let mut out = Vec::new();
        for l in lines {
            out.extend(b.push_line(l));
        }
        (out, b)
    }

    #[test]
    fn simple_and_multiline() {
        let (out, b) = feed(Dialect::Postgres, &["select 1;", "select", "  2", ";"]);
        assert_eq!(out, vec![("select 1".into(), End::Normal), ("select\n  2".into(), End::Normal)]);
        assert!(b.is_empty());
        let (out, _) = feed(Dialect::Postgres, &["select 1; select 2;"]);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn quotes_and_comments() {
        let (out, b) = feed(Dialect::Postgres, &["select 'a;b', \"c;\" -- x;", "from t;"]);
        assert_eq!(out, vec![("select 'a;b', \"c;\" -- x;\nfrom t".into(), End::Normal)]);
        assert!(b.is_empty());
        let (out, b) = feed(Dialect::Postgres, &["select 'it''s", "fine;"]);
        assert!(out.is_empty());
        assert_eq!(b.open(), Some(Open::Quote('\'')));
        let (out, _) = feed(Dialect::Postgres, &["/* ; */ select 'Ünïcødé ✓';"]);
        assert_eq!(out.len(), 1);
        let (_, b) = feed(Dialect::Mysql, &["select 1 # comment;"]);
        assert_eq!(b.open(), Some(Open::Statement));
        let (out, _) = feed(Dialect::Mysql, &["select 'a\\';b';"]);
        assert_eq!(out, vec![("select 'a\\';b'".into(), End::Normal)]);
    }

    #[test]
    fn dollar_quotes() {
        let (out, b) = feed(Dialect::Postgres, &["create function f() returns int as $body$", "select 1;", "$body$ language sql;"]);
        assert_eq!(out.len(), 1);
        assert!(out[0].0.ends_with("language sql"));
        assert!(b.is_empty());
        let (_, b) = feed(Dialect::Postgres, &["do $$ begin"]);
        assert_eq!(b.open(), Some(Open::Dollar));
        // `$1` is a parameter, not a dollar quote.
        let (out, _) = feed(Dialect::Postgres, &["select $1;"]);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn modes() {
        let (_, b) = feed(Dialect::Postgres, &["select 'a"]);
        assert_eq!(b.mode(), Mode::Quote('\''));
        let (_, b) = feed(Dialect::Postgres, &["do $fn$ begin"]);
        assert_eq!(b.mode(), Mode::Dollar("$fn$".into()));
        let (_, b) = feed(Dialect::Postgres, &["select 1 -- comment"]);
        assert_eq!(b.mode(), Mode::Code);
        let (_, b) = feed(Dialect::Postgres, &["select /* x"]);
        assert_eq!(b.mode(), Mode::BlockComment);
    }

    #[test]
    fn mysql_terminators() {
        let (out, _) = feed(Dialect::Mysql, &["select * from t\\G"]);
        assert_eq!(out, vec![("select * from t".into(), End::Vertical)]);
        let (out, _) = feed(Dialect::Mysql, &["select 1\\g select 2;"]);
        assert_eq!(out.len(), 2);
        let mut b = SqlBuf::new(Dialect::Mysql);
        b.delimiter = "//".into();
        assert!(b.push_line("create procedure p() begin select 1; end").is_empty());
        assert_eq!(b.push_line("//").len(), 1);
    }
}
