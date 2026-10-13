//! psql's COPY handling: recognising `COPY … FROM STDIN` / `TO STDOUT`, parsing
//! `\copy`, and the SCRAM verifier `\password` sends.

use anyhow::{bail, Result};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Direction {
    /// `COPY … FROM STDIN`: the client sends data.
    In,
    /// `COPY … TO STDOUT`: the server sends data.
    Out,
}

/// Whether `sql` is a COPY that exchanges data with the client.
pub fn direction(sql: &str) -> Option<Direction> {
    let words: Vec<String> = words(sql).into_iter().map(|w| w.trim_end_matches(';').to_ascii_uppercase()).collect();
    if words.first().map(String::as_str) != Some("COPY") {
        return None;
    }
    words.windows(2).find_map(|w| match (w[0].as_str(), w[1].as_str()) {
        ("FROM", "STDIN") => Some(Direction::In),
        ("TO", "STDOUT") => Some(Direction::Out),
        _ => None,
    })
}

/// Whitespace-separated words outside quotes and parentheses (a COPY's query is
/// in parentheses and may contain `FROM`).
fn words(sql: &str) -> Vec<String> {
    let (mut out, mut cur, mut depth, mut quote) = (Vec::new(), String::new(), 0usize, None::<char>);
    for c in sql.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                continue;
            }
            None if c == '\'' || c == '"' => {
                quote = Some(c);
                continue;
            }
            None => {}
        }
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            c if c.is_whitespace() && depth == 0 => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c if depth == 0 => cur.push(c),
            _ => {}
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Where `\copy` reads or writes on the client.
#[derive(Debug, PartialEq, Eq)]
pub enum Endpoint {
    File(String),
    Program(String),
    /// `stdin` / `stdout`: the script's input, or the query output channel (`\o`).
    Std,
    /// `pstdin` / `pstdout`: psql's own standard input / output.
    PStd,
}

/// A parsed `\copy`.
#[derive(Debug, PartialEq, Eq)]
pub struct MetaCopy {
    /// The COPY statement sent to the server (`… FROM STDIN` / `… TO STDOUT`).
    pub sql: String,
    pub direction: Direction,
    pub endpoint: Endpoint,
}

/// `\copy { table [(cols)] | (query) } { from | to } { 'file' | program 'cmd' | stdin | stdout | pstdin | pstdout } [[with] (options)]`
pub fn parse_meta(arg: &str) -> Result<MetaCopy> {
    let usage = "\\copy: arguments required: \\copy table [(columns)] from|to 'file'|program 'cmd'|stdin|stdout [with (options)]";
    // Find the top-level FROM / TO.
    let (mut depth, mut quote, mut at) = (0usize, None::<char>, None);
    let bytes: Vec<(usize, char)> = arg.char_indices().collect();
    for (i, &(pos, c)) in bytes.iter().enumerate() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            c if c.is_whitespace() && depth == 0 => {
                let rest = &arg[pos + c.len_utf8()..];
                let word: String = rest.chars().take_while(|c| !c.is_whitespace()).collect();
                if (word.eq_ignore_ascii_case("from") || word.eq_ignore_ascii_case("to")) && i > 0 {
                    at = Some((pos, word));
                    break;
                }
            }
            _ => {}
        }
    }
    let Some((pos, word)) = at else { bail!(usage) };
    let target = arg[..pos].trim();
    if target.is_empty() {
        bail!(usage);
    }
    let direction = if word.eq_ignore_ascii_case("from") { Direction::In } else { Direction::Out };
    let rest = arg[pos..].trim_start()[word.len()..].trim_start();
    let (name, quoted, mut rest) = take_name(rest)?;
    let endpoint = match (name.to_ascii_lowercase().as_str(), quoted) {
        ("program", false) => {
            let (cmd, _, r) = take_name(rest)?;
            rest = r;
            Endpoint::Program(cmd)
        }
        ("stdin", false) if direction == Direction::In => Endpoint::Std,
        ("stdout", false) if direction == Direction::Out => Endpoint::Std,
        ("pstdin", false) if direction == Direction::In => Endpoint::PStd,
        ("pstdout", false) if direction == Direction::Out => Endpoint::PStd,
        ("", _) => bail!(usage),
        _ => Endpoint::File(super::expand_home(&name)),
    };
    let rest = rest.trim().trim_end_matches(';').trim();
    let end = if direction == Direction::In { "FROM STDIN" } else { "TO STDOUT" };
    let sql = if rest.is_empty() { format!("COPY {target} {end}") } else { format!("COPY {target} {end} {rest}") };
    Ok(MetaCopy { sql, direction, endpoint })
}

/// A file name: `'quoted'` (with `''` for a quote) or a bare word. Returns the
/// name, whether it was quoted, and what follows.
fn take_name(s: &str) -> Result<(String, bool, &str)> {
    let s = s.trim_start();
    if let Some(body) = s.strip_prefix('\'') {
        let mut name = String::new();
        let mut chars = body.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c == '\'' {
                if chars.peek().map(|&(_, c)| c) == Some('\'') {
                    chars.next();
                    name.push('\'');
                    continue;
                }
                return Ok((name, true, &body[i + 1..]));
            }
            name.push(c);
        }
        bail!("unterminated quoted string");
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    Ok((s[..end].to_string(), false, &s[end..]))
}

/// The rows a COPY moved, from its text or CSV data (a CSV header isn't a row).
pub fn count_rows(sql: &str, data: &str) -> usize {
    let lines = data.lines().count();
    let upper = sql.to_ascii_uppercase();
    if upper.contains("HEADER") && upper.contains("CSV") { lines.saturating_sub(1) } else { lines }
}

/// A SCRAM-SHA-256 password verifier, in the form Postgres stores
/// (`SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey`), so `ALTER ROLE … PASSWORD`
/// never carries the password itself.
pub fn scram_verifier(password: &str, salt: &[u8], iterations: u32) -> String {
    use base64::Engine as _;
    use hmac::{KeyInit as _, Mac as _};
    use sha2::Digest as _;
    type Hmac = hmac::Hmac<sha2::Sha256>;
    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = Hmac::new_from_slice(key).expect("HMAC takes any key length");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    // PBKDF2-HMAC-SHA-256 with a single output block.
    let mut u = mac(password.as_bytes(), &[salt, &1u32.to_be_bytes()].concat());
    let mut salted = u.clone();
    for _ in 1..iterations {
        u = mac(password.as_bytes(), &u);
        salted.iter_mut().zip(&u).for_each(|(a, b)| *a ^= b);
    }
    let client_key = mac(&salted, b"Client Key");
    let stored_key = sha2::Sha256::digest(&client_key);
    let server_key = mac(&salted, b"Server Key");
    let b64 = base64::engine::general_purpose::STANDARD;
    format!("SCRAM-SHA-256${iterations}:{}${}:{}", b64.encode(salt), b64.encode(stored_key), b64.encode(server_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directions() {
        assert_eq!(direction("COPY public.t (a, b) FROM stdin;"), Some(Direction::In));
        assert_eq!(direction("copy t to stdout with (format csv)"), Some(Direction::Out));
        assert_eq!(direction("COPY (SELECT * FROM t) TO STDOUT"), Some(Direction::Out));
        assert_eq!(direction("COPY t TO '/tmp/x'"), None);
        assert_eq!(direction("SELECT 'COPY t FROM STDIN'"), None);
    }

    #[test]
    fn meta() {
        let m = parse_meta("users to 'out file.csv' with (format csv, header)").unwrap();
        assert_eq!(m.sql, "COPY users TO STDOUT with (format csv, header)");
        assert_eq!(m.endpoint, Endpoint::File("out file.csv".into()));
        let m = parse_meta("(select * from users where name = 'to') to stdout").unwrap();
        assert_eq!(m.sql, "COPY (select * from users where name = 'to') TO STDOUT");
        assert_eq!(m.endpoint, Endpoint::Std);
        let m = parse_meta("users (id, email) from stdin csv").unwrap();
        assert_eq!((m.sql.as_str(), m.direction), ("COPY users (id, email) FROM STDIN csv", Direction::In));
        let m = parse_meta("t from program 'gzip -dc x.gz'").unwrap();
        assert_eq!(m.endpoint, Endpoint::Program("gzip -dc x.gz".into()));
        assert!(parse_meta("users").is_err());
        assert!(parse_meta("to x").is_err());
    }

    #[test]
    fn rows() {
        assert_eq!(count_rows("COPY t TO STDOUT", "1\n2\n"), 2);
        assert_eq!(count_rows("COPY t TO STDOUT (FORMAT CSV, HEADER)", "a\n1\n"), 1);
    }

    #[test]
    fn scram() {
        // RFC 7677's example salt and password; keys checked with Python's
        // hashlib.pbkdf2_hmac / hmac.
        let salt = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, "W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        assert_eq!(
            scram_verifier("pencil", &salt, 4096),
            "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU="
        );
    }
}
