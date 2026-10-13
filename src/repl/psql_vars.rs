//! psql variables: `:name` interpolation, splitting meta-commands off a line,
//! meta-command arguments and boolean values.

use std::collections::BTreeMap;

use super::sqlbuf::Mode;

pub type Vars = BTreeMap<String, String>;

/// For each byte of `text`, whether it is SQL code (not inside a quote, comment or
/// dollar-quote), starting in `start`. Postgres rules: '' and "" double up,
/// `--` comments, nesting `/* */`, `$tag$` quotes.
fn code_map(text: &str, start: &Mode) -> Vec<bool> {
    #[derive(PartialEq)]
    enum St {
        Code,
        Quote(char),
        LineComment,
        Block(u32),
        Dollar(String),
    }
    let mut st = match start {
        Mode::Code => St::Code,
        Mode::Quote(q) => St::Quote(*q),
        Mode::BlockComment => St::Block(1),
        Mode::Dollar(tag) => St::Dollar(tag.clone()),
    };
    let b = text.as_bytes();
    let mut code = vec![false; b.len()];
    let mut i = 0;
    while i < b.len() {
        if !text.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &text[i..];
        match &mut st {
            St::Code => {
                code[i] = true;
                if rest.starts_with("--") {
                    st = St::LineComment;
                } else if rest.starts_with("/*") {
                    st = St::Block(1);
                    i += 1;
                } else if b[i] == b'\'' || b[i] == b'"' {
                    st = St::Quote(b[i] as char);
                } else if b[i] == b'$' {
                    if let Some(end) = rest[1..].find('$') {
                        let tag = &rest[..end + 2];
                        if tag[1..tag.len() - 1].chars().all(|c| c.is_alphanumeric() || c == '_')
                            && !tag[1..].starts_with(|c: char| c.is_ascii_digit())
                        {
                            i += tag.len() - 1;
                            st = St::Dollar(tag.to_string());
                        }
                    }
                }
            }
            St::Quote(q) => {
                if b[i] == *q as u8 {
                    if b.get(i + 1) == Some(&(*q as u8)) {
                        i += 1;
                    } else {
                        st = St::Code;
                    }
                }
            }
            St::LineComment => {}
            St::Block(depth) => {
                if rest.starts_with("*/") {
                    *depth -= 1;
                    i += 1;
                    if *depth == 0 {
                        st = St::Code;
                    }
                } else if rest.starts_with("/*") {
                    *depth += 1;
                    i += 1;
                }
            }
            St::Dollar(tag) => {
                if rest.starts_with(tag.as_str()) {
                    i += tag.len() - 1;
                    st = St::Code;
                }
            }
        }
        i += 1;
    }
    code
}

/// Split a line at its first backslash in SQL code: the SQL before it and the
/// meta-command after it (`select 1 \g`, or `\dt` on its own).
pub fn split_meta<'a>(line: &'a str, start: &Mode) -> (&'a str, Option<&'a str>) {
    let code = code_map(line, start);
    match line.char_indices().find(|&(i, c)| c == '\\' && code[i]) {
        Some((i, _)) => (&line[..i], Some(&line[i + 1..])),
        None => (line, None),
    }
}

/// A value as an SQL string literal, the way libpq's PQescapeLiteral writes it.
pub fn quote_literal(v: &str) -> String {
    if v.contains('\\') {
        format!(" E'{}'", v.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

/// A value as an SQL identifier.
pub fn quote_ident(v: &str) -> String {
    format!("\"{}\"", v.replace('"', "\"\""))
}

fn is_name(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Replace `:name`, `:'name'` (literal), `:"name"` (identifier) and `:{?name}`
/// (defined?) in SQL code. Unknown names and `::` casts are left alone.
pub fn interpolate(text: &str, start: &Mode, vars: &Vars) -> String {
    if !text.contains(':') {
        return text.to_string();
    }
    let code = code_map(text, start);
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        let c = text[i..].chars().next().unwrap();
        if c != ':' || !code[i] {
            out.push(c);
            i += c.len_utf8();
            continue;
        }
        let rest = &text[i + 1..];
        // `::type` is a cast.
        if rest.starts_with(':') {
            out.push_str("::");
            i += 2;
            continue;
        }
        let quoted = |q: char| -> Option<(&str, usize)> {
            let inner = rest.strip_prefix(q)?;
            let end = inner.find(q)?;
            let name = &inner[..end];
            (!name.is_empty() && name.chars().all(is_name)).then_some((name, end + 2))
        };
        if let Some((name, len)) = quoted('\'') {
            if let Some(v) = vars.get(name) {
                out.push_str(&quote_literal(v));
                i += 1 + len;
                continue;
            }
        } else if let Some((name, len)) = quoted('"') {
            if let Some(v) = vars.get(name) {
                out.push_str(&quote_ident(v));
                i += 1 + len;
                continue;
            }
        } else if let Some(inner) = rest.strip_prefix("{?") {
            if let Some(end) = inner.find('}') {
                out.push_str(if vars.contains_key(&inner[..end]) { "TRUE" } else { "FALSE" });
                i += 1 + 2 + end + 1;
                continue;
            }
        } else {
            let len: usize = rest.chars().take_while(|&c| is_name(c)).map(char::len_utf8).sum();
            if let Some(v) = (len > 0).then(|| vars.get(&rest[..len])).flatten() {
                out.push_str(v);
                i += 1 + len;
                continue;
            }
        }
        out.push(':');
        i += 1;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> Vars {
        [("t", "users"), ("id", "42"), ("name", "O'Brien"), ("col", "Mixed Case")].into_iter().map(|(k, v)| (k.into(), v.into())).collect()
    }

    #[test]
    fn interpolation() {
        let v = vars();
        assert_eq!(interpolate("select * from :t where id = :id", &Mode::Code, &v), "select * from users where id = 42");
        assert_eq!(interpolate("select :'name', :\"col\"", &Mode::Code, &v), "select 'O''Brien', \"Mixed Case\"");
        assert_eq!(interpolate("select '1'::int, :nope, x::text", &Mode::Code, &v), "select '1'::int, :nope, x::text");
        assert_eq!(interpolate("select ':id', \":id\" -- :id", &Mode::Code, &v), "select ':id', \":id\" -- :id");
        assert_eq!(interpolate("select $$ :id $$, /* :id */ :id", &Mode::Code, &v), "select $$ :id $$, /* :id */ 42");
        assert_eq!(interpolate("still quoted :id' then :id", &Mode::Quote('\''), &v), "still quoted :id' then 42");
        assert_eq!(interpolate(":{?id} :{?nope}", &Mode::Code, &v), "TRUE FALSE");
        assert_eq!(quote_literal("a\\b"), " E'a\\\\b'");
    }

    #[test]
    fn meta_split() {
        assert_eq!(split_meta("select 1 \\g", &Mode::Code), ("select 1 ", Some("g")));
        assert_eq!(split_meta("select '\\x' \\gx", &Mode::Code), ("select '\\x' ", Some("gx")));
        assert_eq!(split_meta("\\dt", &Mode::Code), ("", Some("dt")));
        assert_eq!(split_meta("a\\b' \\g", &Mode::Quote('\'')), ("a\\b' ", Some("g")));
    }

    #[test]
    fn arguments() {
        assert_eq!(split_args("x 'a b' 'it''s' c'd'e '\\t'"), vec!["x", "a b", "it's", "cde", "\t"]);
        for (v, b) in [("on", Some(true)), ("OFF", Some(false)), ("t", Some(true)), ("1", Some(true)), ("n", Some(false)), ("o", None), ("maybe", None)] {
            assert_eq!(bool_value(v), b, "{v}");
        }
    }
}
