//! Data masking applied to results before they leave the daemon.
//!
//! Two mechanisms:
//! - column rules: glob patterns matched against `column` or `table.column`
//!   (for MongoDB: `collection.field.path`), each with a strategy;
//! - value detectors: regexes for well-known PII shapes applied to every string value.
//!
//! The `fake` strategy replaces values with realistic, deterministic fakes (see [`fake`]).

pub mod fake;

use self::fake::{FakeKind, Faker};

use std::sync::LazyLock;

use globset::{GlobBuilder, GlobMatcher};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct MaskConfig {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnRule>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub detect: Vec<Detector>,
    /// How detected values are replaced: `partial` (default), `redact`, `hash` or `fake`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detect_strategy: Option<Strategy>,
    /// Secret mixed into `hash` and `fake` outputs (`${env:VAR}` allowed). Without it,
    /// anyone can check a guessed original by hashing/faking it themselves.
    /// A connection's seed overrides the default one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
}

impl MaskConfig {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.detect.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnRule {
    /// Glob, case-insensitive. Without a `.` it matches the column name only;
    /// with a `.` it matches `table.column`.
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default)]
    pub strategy: Strategy,
    /// For `strategy = "fake"`: what to generate. Guessed from the column name, then
    /// from the value, when omitted.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub fake_as: Option<FakeKind>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    /// Replace the whole value with `***`.
    #[default]
    Redact,
    /// Keep a few leading/trailing characters (`jo***@example.com`, `****1234`).
    Partial,
    /// Replace with a short stable hash, so equal values stay joinable.
    Hash,
    /// Replace with null.
    Null,
    /// Replace with a realistic fake of the same kind (email, name, phone, ...).
    /// Deterministic: equal inputs give equal fakes.
    Fake,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Detector {
    Email,
    Card,
    Phone,
    Iban,
}

struct CompiledRule {
    qualified: bool,
    glob: GlobMatcher,
    strategy: Strategy,
    fake_as: Option<FakeKind>,
}

/// A compiled masker for one connection (global defaults + connection rules).
pub struct Masker {
    rules: Vec<CompiledRule>,
    detectors: Vec<Detector>,
    detect_strategy: Strategy,
    seed: String,
    faker: Faker,
}

impl Masker {
    pub fn new(configs: &[&MaskConfig]) -> anyhow::Result<Self> {
        let mut rules = Vec::new();
        let mut detectors = Vec::new();
        let mut detect_strategy = Strategy::Partial;
        let mut seed = String::new();
        for cfg in configs {
            if let Some(s) = &cfg.seed {
                seed = crate::config::interpolate_env(s)?;
            }
            if let Some(s) = cfg.detect_strategy {
                if s == Strategy::Null {
                    anyhow::bail!("detect_strategy cannot be `null` (detectors replace substrings)");
                }
                detect_strategy = s;
            }
            for r in &cfg.columns {
                if r.fake_as.is_some() && r.strategy != Strategy::Fake {
                    anyhow::bail!("mask rule {:?}: `as` only applies to strategy = \"fake\"", r.pattern);
                }
                let glob = GlobBuilder::new(&r.pattern)
                    .case_insensitive(true)
                    .literal_separator(false)
                    .build()?
                    .compile_matcher();
                rules.push(CompiledRule {
                    qualified: r.pattern.contains('.'),
                    glob,
                    strategy: r.strategy,
                    fake_as: r.fake_as,
                });
            }
            for d in &cfg.detect {
                if !detectors.contains(d) {
                    detectors.push(*d);
                }
            }
        }
        let faker = Faker::new(Some(&seed));
        Ok(Self { rules, detectors, detect_strategy, seed, faker })
    }

    pub fn is_noop(&self) -> bool {
        self.rules.is_empty() && self.detectors.is_empty()
    }

    /// The first rule matching a column, if any. `table` may be unknown.
    fn rule(&self, table: Option<&str>, column: &str) -> Option<&CompiledRule> {
        let qualified = table.map(|t| format!("{t}.{column}"));
        self.rules.iter().find_map(|r| {
            let hit = if r.qualified {
                qualified.as_deref().is_some_and(|q| r.glob.is_match(q))
            } else {
                // Match the leaf name (`a.b.email` -> `email`) as well as the full path.
                let leaf = column.rsplit('.').next().unwrap_or(column);
                r.glob.is_match(column) || r.glob.is_match(leaf)
            };
            hit.then_some(r)
        })
    }

    fn apply(&self, r: &CompiledRule, column: &str, v: &Value) -> Value {
        match r.strategy {
            Strategy::Fake => self.faker.fake(r.fake_as, column, v),
            s => apply_strategy(s, v, &self.seed),
        }
    }

    /// Mask a single value that belongs to `table.column`.
    pub fn mask_cell(&self, table: Option<&str>, column: &str, v: &mut Value) {
        if v.is_null() {
            return;
        }
        if let Some(r) = self.rule(table, column) {
            *v = self.apply(r, column, v);
            return;
        }
        self.detect_in(v);
    }

    /// Mask a JSON document (MongoDB) recursively; field paths are dot-joined.
    pub fn mask_document(&self, collection: Option<&str>, doc: &mut Value) {
        if self.is_noop() {
            return;
        }
        self.walk(collection, String::new(), doc);
    }

    fn walk(&self, table: Option<&str>, path: String, v: &mut Value) {
        match v {
            Value::Object(map) => {
                for (k, child) in map.iter_mut() {
                    // Extended JSON wrappers ($oid, $date, ...) belong to their parent field.
                    if k.starts_with('$') {
                        continue;
                    }
                    let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                    if let Some(r) = self.rule(table, &p) {
                        if !child.is_null() {
                            *child = self.apply(r, &p, child);
                        }
                    } else {
                        self.walk(table, p, child);
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.walk(table, path.clone(), item);
                }
            }
            _ => self.detect_in(v),
        }
    }

    fn detect_in(&self, v: &mut Value) {
        if self.detectors.is_empty() {
            return;
        }
        if let Value::String(s) = v {
            let mut out = s.clone();
            for d in &self.detectors {
                out = self.detect(*d, &out);
            }
            if out != *s {
                *s = out;
            }
        }
    }
}

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Non-`fake` strategies. `seed` salts `hash`.
pub fn apply_strategy(s: Strategy, v: &Value, seed: &str) -> Value {
    match s {
        Strategy::Redact => Value::String("***".into()),
        Strategy::Null => Value::Null,
        Strategy::Hash => Value::String(hash(&value_text(v), seed)),
        Strategy::Partial => Value::String(partial(&value_text(v))),
        Strategy::Fake => unreachable!("handled by Masker::apply"),
    }
}

fn hash(text: &str, seed: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    if !seed.is_empty() {
        h.update(seed.as_bytes());
        h.update([0]);
    }
    h.update(text.as_bytes());
    let hex = hex::encode(&h.finalize()[..6]);
    format!("h:{hex}")
}

fn partial(s: &str) -> String {
    if let Some((local, domain)) = s.split_once('@') {
        let first: String = local.chars().take(1).collect();
        return format!("{first}***@{domain}");
    }
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    if n <= 4 {
        return "***".into();
    }
    let keep = if n >= 10 { 4 } else { 2 };
    let tail: String = chars[n - keep..].iter().collect();
    format!("{}{tail}", "*".repeat(n - keep))
}

static EMAIL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b").unwrap());
static CARD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(?:\d[ \-]?){12,18}\d\b").unwrap());
static PHONE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:\+|\b00)?\(?\d{1,4}\)?[\s.\-]?(?:\d[\s.\-]?){6,13}\d\b").unwrap());
static IBAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Z]{2}\d{2}(?:[ ]?[A-Z0-9]{4}){2,7}(?:[ ]?[A-Z0-9]{1,4})?\b").unwrap());

/// The Luhn sum of `digits` (check digit last): every second digit from the
/// right doubled, digits of the products added.
pub(crate) fn luhn_sum(digits: &[u32]) -> u32 {
    digits.iter().rev().enumerate().map(|(i, &d)| if i % 2 == 1 { let x = d * 2; if x > 9 { x - 9 } else { x } } else { d }).sum()
}

/// The Luhn check digit that completes `payload`.
pub(crate) fn luhn_check_digit(payload: &[u32]) -> u32 {
    let with_zero: Vec<u32> = payload.iter().copied().chain([0]).collect();
    (10 - luhn_sum(&with_zero) % 10) % 10
}

/// A card-number-length digit string with a valid Luhn check digit.
fn luhn(digits: &str) -> bool {
    let ds: Vec<u32> = digits.chars().filter_map(|c| c.to_digit(10)).collect();
    (13..=19).contains(&ds.len()) && luhn_sum(&ds) % 10 == 0
}

impl Masker {
    fn detect(&self, d: Detector, s: &str) -> String {
        let (re, kind) = match d {
            Detector::Email => (&*EMAIL, FakeKind::Email),
            Detector::Card => (&*CARD, FakeKind::Card),
            Detector::Phone => (&*PHONE, FakeKind::Phone),
            Detector::Iban => (&*IBAN, FakeKind::Iban),
        };
        re.replace_all(s, |c: &regex::Captures| {
            let m = &c[0];
            let real = match d {
                Detector::Card => luhn(m),
                Detector::Phone => (8..=15).contains(&m.chars().filter(char::is_ascii_digit).count()),
                _ => true,
            };
            if !real {
                return m.to_string();
            }
            match self.detect_strategy {
                Strategy::Fake => self.faker.fake_str(kind, m),
                Strategy::Redact => "***".to_string(),
                Strategy::Hash => hash(m, &self.seed),
                _ => match d {
                    Detector::Card => partial(&m.replace([' ', '-'], "")),
                    Detector::Iban => partial(&m.replace(' ', "")),
                    _ => partial(m),
                },
            }
        })
        .into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn masker(toml_src: &str) -> Masker {
        let cfg: MaskConfig = toml::from_str(toml_src).unwrap();
        Masker::new(&[&cfg]).unwrap()
    }

    #[test]
    fn column_rules() {
        let m = masker(
            r#"
            columns = [
              { match = "users.email", strategy = "partial" },
              { match = "*password*" },
              { match = "ssn", strategy = "null" },
            ]
            "#,
        );
        let mut v = json!("john@example.com");
        m.mask_cell(Some("users"), "email", &mut v);
        assert_eq!(v, json!("j***@example.com"));

        let mut v = json!("john@example.com");
        m.mask_cell(Some("orders"), "email", &mut v);
        assert_eq!(v, json!("john@example.com"), "qualified rule only hits its table");

        let mut v = json!("hunter2");
        m.mask_cell(None, "PasswordHash", &mut v);
        assert_eq!(v, json!("***"));

        let mut v = json!(123456789);
        m.mask_cell(None, "ssn", &mut v);
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn hash_is_stable() {
        let a = apply_strategy(Strategy::Hash, &json!("x"), "");
        let b = apply_strategy(Strategy::Hash, &json!("x"), "");
        assert_eq!(a, b);
        assert_ne!(a, apply_strategy(Strategy::Hash, &json!("y"), ""));
    }

    #[test]
    fn detectors() {
        let m = masker(r#"detect = ["email", "card", "iban"]"#);
        let mut v = json!("contact bob@corp.io, card 4111 1111 1111 1111, ref 1234567890123");
        m.mask_cell(None, "notes", &mut v);
        let s = v.as_str().unwrap();
        assert!(s.contains("b***@corp.io"), "{s}");
        assert!(s.contains("************1111"), "{s}");
        assert!(s.contains("1234567890123"), "non-Luhn numbers untouched: {s}");

        let mut v = json!("pay to DE89 3704 0044 0532 0130 00");
        m.mask_cell(None, "notes", &mut v);
        assert!(!v.as_str().unwrap().contains("3704"), "{v}");
    }

    #[test]
    fn hash_seed() {
        assert_ne!(hash("x", ""), hash("x", "secret"));
        assert_eq!(hash("x", "secret"), hash("x", "secret"));
    }

    #[test]
    fn fake_strategy() {
        let m = masker(
            r#"
            seed = "s3cret"
            columns = [
              { match = "*email*", strategy = "fake" },
              { match = "users.contact", strategy = "fake", as = "phone" },
              { match = "*.ssn", strategy = "fake" },
            ]
            detect = ["email"]
            detect_strategy = "fake"
            "#,
        );
        let mut a = json!("john@corp.com");
        let mut b = json!("john@corp.com");
        m.mask_cell(Some("users"), "email", &mut a);
        m.mask_cell(Some("orders"), "customer_email", &mut b);
        assert_eq!(a, b, "deterministic across tables/columns");
        assert_ne!(a, json!("john@corp.com"));
        assert!(a.as_str().unwrap().contains('@'));

        let mut c = json!("anything");
        m.mask_cell(Some("users"), "contact", &mut c);
        assert!(c.as_str().unwrap().chars().any(|ch| ch.is_ascii_digit()), "{c}");

        let mut ssn = json!("123-45-6789");
        m.mask_cell(Some("people"), "ssn", &mut ssn);
        assert!(Regex::new(r"^\d{3}-\d{2}-\d{4}$").unwrap().is_match(ssn.as_str().unwrap()), "{ssn}");
        assert_ne!(ssn, json!("123-45-6789"));

        let mut note = json!("mail bob@corp.io today");
        m.mask_cell(None, "notes", &mut note);
        let n = note.as_str().unwrap();
        assert!(n.starts_with("mail ") && n.ends_with(" today") && !n.contains("bob@corp.io"), "{n}");

        // A different seed gives different fakes.
        let other = masker(r#"columns = [{ match = "*email*", strategy = "fake" }]"#);
        let mut d = json!("john@corp.com");
        other.mask_cell(Some("users"), "email", &mut d);
        assert_ne!(a, d);

        let bad: MaskConfig = toml::from_str(r#"columns = [{ match = "x", strategy = "redact", as = "email" }]"#).unwrap();
        assert!(Masker::new(&[&bad]).is_err());
    }

    #[test]
    fn documents() {
        let m = masker(r#"columns = [{ match = "users.profile.email", strategy = "redact" }, { match = "token" }]"#);
        let mut doc = json!({
            "_id": {"$oid": "64b7f0000000000000000000"},
            "profile": {"email": "a@b.co", "name": "A"},
            "sessions": [{"token": "abc"}, {"token": "def"}],
        });
        m.mask_document(Some("users"), &mut doc);
        assert_eq!(doc["profile"]["email"], json!("***"));
        assert_eq!(doc["profile"]["name"], json!("A"));
        assert_eq!(doc["sessions"][1]["token"], json!("***"));
        assert_eq!(doc["_id"]["$oid"], json!("64b7f0000000000000000000"));
    }
}
