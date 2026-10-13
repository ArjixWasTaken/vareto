//! Redis via redis-rs with a reconnecting connection manager.

use std::time::Duration;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::Value as RV;
use serde_json::{json, Value};

use crate::config::Connection;
use crate::render::Output;

pub struct Redis {
    client: redis::Client,
    con: ConnectionManager,
}

fn enc(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

pub fn build_url(conn: &Connection, host: &str, port: u16, password: Option<&str>) -> Result<String> {
    let cfg = &conn.cfg;
    let auth = match (&cfg.user, password) {
        (Some(u), Some(p)) => format!("{}:{}@", enc(u), enc(p)),
        (None, Some(p)) => format!(":{}@", enc(p)),
        (Some(u), None) => format!("{}@", enc(u)),
        (None, None) => String::new(),
    };
    let db = match &cfg.database {
        Some(d) => d.parse::<u32>().with_context(|| format!("redis `database` must be a number, got {d:?}"))?,
        None => 0,
    };
    if let Some(k) = cfg.params.keys().next() {
        anyhow::bail!("unsupported Redis param {k:?}");
    }
    Ok(format!("redis://{auth}{}/{db}", crate::config::join_host_port(host, port)))
}

/// Run one command on a session connection. Returns the reply as JSON plus its
/// [`kinds`] skeleton.
pub async fn session_command(con: &mut redis::aio::MultiplexedConnection, args: &[String], timeout: Duration) -> Result<(Value, Value)> {
    let mut cmd = redis::cmd(&args[0]);
    for a in &args[1..] {
        cmd.arg(a);
    }
    let v: RV = tokio::time::timeout(timeout, cmd.query_async(con))
        .await
        .map_err(|_| anyhow::anyhow!("command timed out"))?
        // The server's own wording (`WRONGTYPE Operation against ...`), as redis-cli shows it.
        .map_err(|e| match (e.code(), e.detail()) {
            (Some(code), Some(detail)) => anyhow::anyhow!("{code} {detail}"),
            _ => anyhow::anyhow!("{e}"),
        })?;
    let kinds = kinds(&v);
    Ok((to_json(v), kinds))
}

/// The reply types of a [`to_json`] value, in the same shape: each leaf becomes a
/// one-letter code (`s` status, `b` bulk, `i` integer, `d` double, `n` nil, `e`
/// error, `t` boolean, `x` binary bulk) so a shell can format it like redis-cli
/// after the JSON itself has been masked.
pub fn kinds(v: &RV) -> Value {
    match v {
        RV::Nil => json!("n"),
        RV::Int(_) => json!("i"),
        RV::BulkString(b) => json!(if std::str::from_utf8(b).is_ok() { "b" } else { "x" }),
        RV::Array(items) | RV::Set(items) => Value::Array(items.iter().map(kinds).collect()),
        RV::SimpleString(_) | RV::Okay => json!("s"),
        RV::Map(pairs) => {
            let mut obj = serde_json::Map::new();
            let mut fallback = Vec::new();
            for (k, v) in pairs {
                match to_json(k.clone()) {
                    Value::String(k) => {
                        obj.insert(k, kinds(v));
                    }
                    _ => fallback.push(json!([kinds(k), kinds(v)])),
                }
            }
            if fallback.is_empty() { Value::Object(obj) } else { Value::Array(fallback) }
        }
        RV::Attribute { data, .. } => kinds(data),
        RV::Double(_) => json!("d"),
        RV::Boolean(_) => json!("t"),
        RV::VerbatimString { .. } => json!("b"),
        RV::ServerError(_) => json!("e"),
        _ => json!("b"),
    }
}

/// Convert a reply to JSON; binary-unsafe bulk strings become `base64:...`.
pub fn to_json(v: RV) -> Value {
    use base64::Engine;
    match v {
        RV::Nil => Value::Null,
        RV::Int(i) => json!(i),
        RV::BulkString(b) => match String::from_utf8(b) {
            Ok(s) => Value::String(s),
            Err(e) => Value::String(format!("base64:{}", base64::engine::general_purpose::STANDARD.encode(e.into_bytes()))),
        },
        RV::Array(items) | RV::Set(items) => Value::Array(items.into_iter().map(to_json).collect()),
        RV::SimpleString(s) => Value::String(s),
        RV::Okay => Value::String("OK".into()),
        RV::Map(pairs) => {
            // Maps with string keys become objects; anything else a list of pairs.
            let mut obj = serde_json::Map::new();
            let mut fallback = Vec::new();
            for (k, v) in pairs {
                match to_json(k) {
                    Value::String(k) => {
                        obj.insert(k, to_json(v));
                    }
                    k => fallback.push(json!([k, to_json(v)])),
                }
            }
            if fallback.is_empty() { Value::Object(obj) } else { Value::Array(fallback) }
        }
        RV::Attribute { data, .. } => to_json(*data),
        RV::Double(d) => serde_json::Number::from_f64(d).map(Value::Number).unwrap_or_else(|| d.to_string().into()),
        RV::Boolean(b) => Value::Bool(b),
        RV::VerbatimString { text, .. } => Value::String(text),
        RV::ServerError(e) => json!({ "error": e.to_string() }),
        other => Value::String(format!("{other:?}")),
    }
}

impl Redis {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        // No fixed response timeout: every call is bounded by the configured
        // query/export timeout instead.
        let cfg = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(10)))
            .set_response_timeout(None);
        let mut con = client.get_connection_manager_with_config(cfg).await.context("connecting to Redis")?;
        let _: RV = redis::cmd("PING").query_async(&mut con).await.context("connecting to Redis")?;
        Ok(Self { client, con })
    }

    /// A dedicated connection for an interactive session, so SELECT and MULTI stay
    /// local to it.
    pub async fn session(&self) -> Result<redis::aio::MultiplexedConnection> {
        let cfg = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(10)))
            .set_response_timeout(None);
        self.client.get_multiplexed_async_connection_with_config(&cfg).await.context("connecting to Redis")
    }

    pub async fn command(&self, args: &[String], timeout: Duration) -> Result<Output> {
        let mut con = self.con.clone();
        let mut cmd = redis::cmd(&args[0]);
        for a in &args[1..] {
            cmd.arg(a);
        }
        let v: RV = tokio::time::timeout(timeout, cmd.query_async(&mut con))
            .await
            .map_err(|_| anyhow::anyhow!("command timed out"))??;
        Ok(Output::Value(to_json(v)))
    }

    pub async fn list_databases(&self, t: Duration) -> Result<Output> {
        let info = self.command(&["INFO".into(), "keyspace".into()], t).await?;
        let Output::Value(Value::String(s)) = info else { return Ok(info) };
        let dbs: Vec<Value> = s
            .lines()
            .filter_map(|l| l.split_once(':'))
            .filter(|(k, _)| k.starts_with("db"))
            .map(|(k, v)| json!({ "database": k.trim_start_matches("db"), "stats": v }))
            .collect();
        Ok(Output::Value(Value::Array(dbs)))
    }

    /// SCAN keys matching `pattern` (max 1000) with their types.
    pub async fn list_keys(&self, pattern: Option<&str>, t: Duration) -> Result<Output> {
        let mut con = self.con.clone();
        let mut cursor: u64 = 0;
        let mut keys: Vec<String> = Vec::new();
        let fut = async {
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern.unwrap_or("*"))
                    .arg("COUNT")
                    .arg(500)
                    .query_async(&mut con)
                    .await?;
                keys.extend(batch);
                cursor = next;
                if cursor == 0 || keys.len() >= 1000 {
                    break;
                }
            }
            anyhow::Ok(())
        };
        tokio::time::timeout(t, fut).await.map_err(|_| anyhow::anyhow!("scan timed out"))??;
        let truncated = keys.len() >= 1000;
        keys.truncate(1000);
        keys.sort();
        let mut pipe = redis::pipe();
        for k in &keys {
            pipe.cmd("TYPE").arg(k);
        }
        let types: Vec<String> = if keys.is_empty() { vec![] } else { pipe.query_async(&mut con).await? };
        let docs = keys.into_iter().zip(types).map(|(k, t)| json!({ "key": k, "type": t })).collect();
        Ok(Output::Documents { collection: None, docs, truncated })
    }

    pub async fn describe(&self, key: &str, t: Duration) -> Result<Value> {
        let mut con = self.con.clone();
        let fut = async {
            let kind: String = redis::cmd("TYPE").arg(key).query_async(&mut con).await?;
            if kind == "none" {
                anyhow::bail!("key {key:?} does not exist");
            }
            let ttl: i64 = redis::cmd("TTL").arg(key).query_async(&mut con).await?;
            let mem: Option<i64> = redis::cmd("MEMORY").arg("USAGE").arg(key).query_async(&mut con).await.ok();
            let len_cmd = match kind.as_str() {
                "string" => Some("STRLEN"),
                "list" => Some("LLEN"),
                "set" => Some("SCARD"),
                "zset" => Some("ZCARD"),
                "hash" => Some("HLEN"),
                "stream" => Some("XLEN"),
                _ => None,
            };
            let len: Option<i64> = match len_cmd {
                Some(c) => redis::cmd(c).arg(key).query_async(&mut con).await.ok(),
                None => None,
            };
            Ok(json!({ "key": key, "type": kind, "ttl": ttl, "length": len, "memory_bytes": mem }))
        };
        tokio::time::timeout(t, fut).await.map_err(|_| anyhow::anyhow!("timed out"))?
    }
}
