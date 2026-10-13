//! redis-cli.

use anyhow::{bail, Result};
use serde_json::Value;

use super::session::Session;
use super::{error_text, Flavor, Flow};
use crate::ipc::QuerySpec;

pub struct Redis {
    /// Inside MULTI.
    tx: bool,
    keys: Vec<String>,
}

const COMMANDS: &[&str] = &[
    "GET", "SET", "DEL", "EXISTS", "EXPIRE", "TTL", "TYPE", "KEYS", "SCAN", "MGET", "MSET", "INCR", "DECR", "INCRBY",
    "APPEND", "STRLEN", "HGET", "HSET", "HGETALL", "HDEL", "HKEYS", "HVALS", "HLEN", "HSCAN", "LPUSH", "RPUSH", "LPOP",
    "RPOP", "LRANGE", "LLEN", "SADD", "SREM", "SMEMBERS", "SCARD", "SSCAN", "ZADD", "ZREM", "ZRANGE", "ZSCORE",
    "ZCARD", "ZSCAN", "XADD", "XRANGE", "XLEN", "INFO", "DBSIZE", "PING", "ECHO", "SELECT", "MULTI", "EXEC",
    "DISCARD", "WATCH", "UNWATCH", "OBJECT", "MEMORY", "CONFIG", "TIME", "quit", "exit",
];

/// Split a line the way redis-cli does: whitespace-separated, `"..."` with
/// backslash escapes (`\n`, `\xHH`, ...), `'...'` with only `\'`.
pub fn split_args(line: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else { return Ok(out) };
        let mut cur: Vec<u8> = Vec::new();
        match first {
            '"' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => bail!("Invalid argument(s): unbalanced quotes"),
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => cur.push(b'\n'),
                            Some('r') => cur.push(b'\r'),
                            Some('t') => cur.push(b'\t'),
                            Some('b') => cur.push(8),
                            Some('a') => cur.push(7),
                            Some('x') => {
                                let h: String = chars.by_ref().take(2).collect();
                                match u8::from_str_radix(&h, 16) {
                                    Ok(b) if h.len() == 2 => cur.push(b),
                                    _ => cur.extend(format!("x{h}").bytes()),
                                }
                            }
                            Some(c) => cur.extend(c.to_string().bytes()),
                            None => bail!("Invalid argument(s): unbalanced quotes"),
                        },
                        Some(c) => cur.extend(c.to_string().bytes()),
                    }
                }
                if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                    bail!("Invalid argument(s): closing quote must be followed by a space");
                }
            }
            '\'' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => bail!("Invalid argument(s): unbalanced quotes"),
                        Some('\'') => break,
                        Some('\\') if chars.peek() == Some(&'\'') => {
                            chars.next();
                            cur.push(b'\'');
                        }
                        Some(c) => cur.extend(c.to_string().bytes()),
                    }
                }
                if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                    bail!("Invalid argument(s): closing quote must be followed by a space");
                }
            }
            _ => {
                while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                    cur.extend(c.to_string().bytes());
                }
            }
        }
        // Arguments travel as strings over the daemon protocol.
        out.push(String::from_utf8(cur).map_err(|_| anyhow::anyhow!("binary arguments are not supported"))?);
    }
}

/// A bulk string the way redis-cli quotes it.
fn quote(bytes: &[u8]) -> String {
    let mut s = String::from("\"");
    for &b in bytes {
        match b {
            b'\\' => s.push_str("\\\\"),
            b'"' => s.push_str("\\\""),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            7 => s.push_str("\\a"),
            8 => s.push_str("\\b"),
            0x20..=0x7e => s.push(b as char),
            _ => s.push_str(&format!("\\x{b:02x}")),
        }
    }
    s.push('"');
    s
}

fn bulk(v: &Value, kind: &str) -> String {
    match v {
        Value::String(s) if kind == "x" => {
            use base64::Engine;
            let raw = s.strip_prefix("base64:").and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok());
            quote(raw.as_deref().unwrap_or(s.as_bytes()))
        }
        // Keep UTF-8 readable (redis-cli prints \x escapes; that only obscures text).
        Value::String(s) => {
            let q = quote(s.as_bytes());
            if s.is_ascii() { q } else { format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")) }
        }
        other => quote(other.to_string().as_bytes()),
    }
}

/// Format a reply like redis-cli. `kind` is the daemon's reply-type skeleton.
pub fn format(v: &Value, kind: &Value) -> String {
    let k = kind.as_str().unwrap_or("");
    match v {
        Value::Null => "(nil)".into(),
        Value::Array(items) if items.is_empty() => "(empty array)".into(),
        Value::Array(items) => {
            let kinds = kind.as_array();
            let w = items.len().to_string().len();
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                let sub = format(item, kinds.and_then(|k| k.get(i)).unwrap_or(&Value::Null));
                let prefix = format!("{:>w$}) ", i + 1);
                let pad = " ".repeat(prefix.len());
                for (j, line) in sub.lines().enumerate() {
                    out.push(if j == 0 { format!("{prefix}{line}") } else { format!("{pad}{line}") });
                }
            }
            out.join("\n")
        }
        Value::Object(m) if k == "e" => format!("(error) {}", m.get("error").and_then(Value::as_str).unwrap_or("")),
        Value::Object(m) if m.is_empty() => "(empty hash)".into(),
        Value::Object(m) => {
            let w = m.len().to_string().len();
            let mut out = Vec::new();
            for (i, (key, val)) in m.iter().enumerate() {
                let sub = format(val, kind.get(key).unwrap_or(&Value::Null));
                let prefix = format!("{:>w$}# {} => ", i + 1, bulk(&Value::String(key.clone()), "b"));
                let pad = " ".repeat(prefix.chars().count());
                for (j, line) in sub.lines().enumerate() {
                    out.push(if j == 0 { format!("{prefix}{line}") } else { format!("{pad}{line}") });
                }
            }
            out.join("\n")
        }
        Value::Number(n) if k == "d" => format!("(double) {n}"),
        Value::Number(n) if k == "i" || k.is_empty() => format!("(integer) {n}"),
        Value::Bool(b) => format!("({b})"),
        Value::String(s) if k == "s" => s.clone(),
        v => bulk(v, k),
    }
}

impl Redis {
    pub fn new() -> Self {
        Self { tx: false, keys: Vec::new() }
    }
}

impl Flavor for Redis {
    fn banner(&self, s: &Session) -> String {
        format!("redis-cli-compatible vareto shell (server {}){}", s.server_version, if s.readonly { ", read-only" } else { "" })
    }

    fn prompt(&self, s: &Session) -> String {
        let host = s.host.strip_prefix("tunnel ").unwrap_or(&s.host);
        let db = match s.database.as_deref() {
            None | Some("0") => String::new(),
            Some(d) => format!("[{d}]"),
        };
        format!("{host}{db}{}> ", if self.tx { "(TX)" } else { "" })
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        let args = split_args(line)?;
        let Some(cmd) = args.first().map(|c| c.to_ascii_uppercase()) else { return Ok(Flow::Continue) };
        match cmd.as_str() {
            "QUIT" | "EXIT" => return Ok(Flow::Quit),
            "SELECT" if !self.tx && args.len() == 2 => {
                s.use_db(&args[1])?;
                println!("OK");
                return Ok(Flow::Continue);
            }
            "HELP" => {
                println!("vareto: redis-cli's built-in help is not available; see https://redis.io/docs/latest/commands/");
                return Ok(Flow::Continue);
            }
            _ => {}
        }
        let v = s.query(QuerySpec::Redis { args })?;
        match cmd.as_str() {
            "MULTI" => self.tx = true,
            "EXEC" | "DISCARD" => self.tx = false,
            _ => {}
        }
        println!("{}", format(&v["result"], &v["kinds"]));
        Ok(Flow::Continue)
    }

    fn pending(&self) -> bool {
        false
    }

    fn reset(&mut self) {}

    fn error(&self, e: &anyhow::Error) -> String {
        let t = error_text(e);
        if t.starts_with("Invalid argument") { t } else { format!("(error) {t}") }
    }

    fn refresh(&mut self, s: &mut Session) {
        let v = s.list_tables(None, Some("*".into())).unwrap_or_default();
        self.keys = v["documents"].as_array().into_iter().flatten().filter_map(|d| d["key"].as_str().map(str::to_string)).take(1000).collect();
    }

    fn words(&self) -> Vec<String> {
        COMMANDS.iter().map(|c| c.to_string()).chain(self.keys.iter().cloned()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn args() {
        assert_eq!(split_args(r#"set k "a b\n""#).unwrap(), vec!["set", "k", "a b\n"]);
        assert_eq!(split_args(r"set k 'it\'s'").unwrap(), vec!["set", "k", "it's"]);
        assert_eq!(split_args(r#"set k "\x41""#).unwrap(), vec!["set", "k", "A"]);
        assert!(split_args(r#"set k "a"b"#).is_err());
        assert!(split_args(r#"set k "a"#).is_err());
        assert!(split_args("   ").unwrap().is_empty());
    }

    #[test]
    fn replies() {
        assert_eq!(format(&json!("OK"), &json!("s")), "OK");
        assert_eq!(format(&json!("OK"), &json!("b")), "\"OK\"");
        assert_eq!(format(&json!(3), &json!("i")), "(integer) 3");
        assert_eq!(format(&Value::Null, &json!("n")), "(nil)");
        assert_eq!(format(&json!([]), &json!([])), "(empty array)");
        assert_eq!(format(&json!({"error": "ERR x"}), &json!("e")), "(error) ERR x");
        assert_eq!(format(&json!("base64:/w=="), &json!("x")), "\"\\xff\"");
        assert_eq!(
            format(&json!(["a", ["b", 2]]), &json!(["b", ["b", "i"]])),
            "1) \"a\"\n2) 1) \"b\"\n   2) (integer) 2"
        );
        let many: Vec<Value> = (0..10).map(|i| json!(i)).collect();
        assert!(format(&Value::Array(many), &json!([])).starts_with(" 1) (integer) 0"));
    }
}
