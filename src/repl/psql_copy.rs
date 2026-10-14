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
    let words = top_level_words(sql)?;
    if words.first().map(|w| w.0.as_str()) != Some("COPY") {
        return None;
    }
    words.windows(2).find_map(|w| match (w[0].0.as_str(), w[1].0.as_str()) {
        ("FROM", "STDIN") => Some(Direction::In),
        ("TO", "STDOUT") => Some(Direction::Out),
        _ => None,
    })
}

/// The unquoted words outside parentheses (a COPY's query is in parentheses and
/// may contain `FROM`), upper-case, with their byte spans. Postgres's tokenizer
/// sees strings, dollar quotes and comments. `None` when it can't read the text.
fn top_level_words(text: &str) -> Option<Vec<(String, usize, usize)>> {
    use sqlparser::tokenizer::Token;
    let toks = crate::sqltok::tokenize(crate::readonly::SqlDialect::Postgres, text).ok()?;
    let mut depth = 0usize;
    let mut out = Vec::new();
    for t in toks {
        match &t.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(w) if depth == 0 && w.quote_style.is_none() => out.push((w.value.to_ascii_uppercase(), t.start, t.end)),
            _ => {}
        }
    }
    Some(out)
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
    // The first top-level FROM / TO after the target.
    let words = top_level_words(arg).ok_or_else(|| anyhow::anyhow!("\\copy: parse error at \"{arg}\""))?;
    let Some((word, pos, end)) = words.into_iter().find(|(w, at, _)| (w == "FROM" || w == "TO") && !arg[..*at].trim().is_empty()) else {
        bail!(usage)
    };
    let target = arg[..pos].trim();
    if target.is_empty() {
        bail!(usage);
    }
    let direction = if word == "FROM" { Direction::In } else { Direction::Out };
    let rest = arg[end..].trim_start();
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

/// A SCRAM-SHA-256 password verifier, in the form Postgres stores
/// (`SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey`), so `ALTER ROLE … PASSWORD`
/// never carries the password itself.
pub fn scram_verifier(password: &str, salt: &[u8], iterations: u32) -> String {
    use base64::Engine as _;
    use hmac::{KeyInit as _, Mac as _};
    use sha2::Digest as _;
    type Hmac = hmac::Hmac<sha2::Sha256>;
    // SASLprep, as clients do at login (sqlx, libpq). Like libpq, a password it
    // rejects is used as it is.
    let prepared = stringprep::saslprep(password).unwrap_or(std::borrow::Cow::Borrowed(password));
    let salted: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(prepared.as_bytes(), salt, iterations);
    let mac = |data: &[u8]| -> Vec<u8> {
        let mut m = Hmac::new_from_slice(&salted).expect("HMAC takes any key length");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let stored_key = sha2::Sha256::digest(mac(b"Client Key"));
    let server_key = mac(b"Server Key");
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
        // FROM/TO inside strings, dollar quotes and comments of the query don't count.
        let m = parse_meta("(select $$ to $$ /* from */ as x) to stdout").unwrap();
        assert_eq!(m.sql, "COPY (select $$ to $$ /* from */ as x) TO STDOUT");
        assert_eq!(direction("COPY (select 'from stdin') TO STDOUT"), Some(Direction::Out));
        assert!(parse_meta("users").is_err());
        assert!(parse_meta("to x").is_err());
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
        // SASLprep first: a non-ASCII space maps to a space, so both give the
        // same verifier (what a client computes at login).
        assert_eq!(scram_verifier("pen\u{00A0}cil", &salt, 4096), scram_verifier("pen cil", &salt, 4096));
        // A password SASLprep rejects (a prohibited control character) is used as is.
        assert_ne!(scram_verifier("pen\u{0007}", &salt, 4096), scram_verifier("pen", &salt, 4096));
    }
}
