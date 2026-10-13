//! Deterministic fake data for the `fake` masking strategy.
//!
//! Every fake value is a pure function of (seed, kind, original value): the RNG is
//! seeded from SHA-256 of those, so the same input always produces the same output
//! across queries, tables, connections and daemon restarts, which keeps joins and
//! GROUP BYs on masked values meaningful. Set a secret `fake_seed` to stop anyone
//! from confirming a guessed original by faking it themselves.

use std::sync::LazyLock;

use fake::faker::address::en::{BuildingNumber, CityName, CountryName, StreetName, StreetSuffix, ZipCode};
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::{IPv4, IPv6, MACAddress, Username};
use fake::faker::lorem::en::Words;
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::Fake;
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// What kind of fake value to generate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FakeKind {
    Email,
    Name,
    FirstName,
    LastName,
    Username,
    Phone,
    Address,
    Street,
    City,
    Country,
    Zip,
    Company,
    Ip,
    Ipv6,
    Mac,
    Card,
    Iban,
    Uuid,
    Date,
    Text,
    Number,
    /// Format-preserving scramble: letters -> letters, digits -> digits, rest kept.
    Shape,
}

impl FakeKind {
    fn tag(self) -> &'static str {
        match self {
            FakeKind::Email => "email",
            FakeKind::Name => "name",
            FakeKind::FirstName => "first_name",
            FakeKind::LastName => "last_name",
            FakeKind::Username => "username",
            FakeKind::Phone => "phone",
            FakeKind::Address => "address",
            FakeKind::Street => "street",
            FakeKind::City => "city",
            FakeKind::Country => "country",
            FakeKind::Zip => "zip",
            FakeKind::Company => "company",
            FakeKind::Ip => "ip",
            FakeKind::Ipv6 => "ipv6",
            FakeKind::Mac => "mac",
            FakeKind::Card => "card",
            FakeKind::Iban => "iban",
            FakeKind::Uuid => "uuid",
            FakeKind::Date => "date",
            FakeKind::Text => "text",
            FakeKind::Number => "number",
            FakeKind::Shape => "shape",
        }
    }

    /// Guess from a column / field name (`users.email` -> Email).
    pub fn from_column(column: &str) -> Option<FakeKind> {
        let leaf = column.rsplit('.').next().unwrap_or(column).to_ascii_lowercase();
        let n = leaf.replace(['-', ' '], "_");
        let has = |s: &str| n.contains(s);
        let is = |s: &[&str]| s.contains(&n.as_str());
        Some(if has("email") || has("e_mail") || is(&["mail"]) {
            FakeKind::Email
        } else if is(&["first_name", "firstname", "given_name", "givenname", "forename"]) {
            FakeKind::FirstName
        } else if is(&["last_name", "lastname", "surname", "family_name", "familyname"]) {
            FakeKind::LastName
        } else if is(&["username", "user_name", "login", "handle", "nickname", "screen_name"]) {
            FakeKind::Username
        } else if has("company") || has("organization") || has("organisation") || has("employer") {
            FakeKind::Company
        } else if is(&["name", "full_name", "fullname", "display_name", "contact_name", "customer_name"]) {
            FakeKind::Name
        } else if has("phone") || has("mobile") || is(&["tel", "telephone", "fax", "msisdn"]) {
            FakeKind::Phone
        } else if has("street") {
            FakeKind::Street
        } else if has("address") && !has("ip") && !has("mac") && !has("email") {
            FakeKind::Address
        } else if is(&["city", "town"]) {
            FakeKind::City
        } else if is(&["country"]) {
            FakeKind::Country
        } else if has("zip") || has("postcode") || has("postal") {
            FakeKind::Zip
        } else if is(&["ip", "ip_address", "ipaddr", "ipv4", "remote_addr", "client_ip"]) {
            FakeKind::Ip
        } else if is(&["ipv6"]) {
            FakeKind::Ipv6
        } else if is(&["mac", "mac_address"]) {
            FakeKind::Mac
        } else if has("iban") {
            FakeKind::Iban
        } else if is(&["card", "card_number", "cc", "cc_number", "pan", "credit_card"]) {
            FakeKind::Card
        } else if has("uuid") || has("guid") {
            FakeKind::Uuid
        } else {
            return None;
        })
    }

    /// Guess from the value itself.
    fn from_value(s: &str) -> FakeKind {
        static EMAIL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^[^@\s]+@[^@\s]+\.[a-z]{2,}$").unwrap());
        static UUID: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$").unwrap());
        static DATE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}([ T].*)?$").unwrap());
        static IPV4: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{1,3}(\.\d{1,3}){3}$").unwrap());
        if EMAIL.is_match(s) {
            FakeKind::Email
        } else if UUID.is_match(s) {
            FakeKind::Uuid
        } else if DATE.is_match(s) && parse_date_prefix(s).is_some() {
            FakeKind::Date
        } else if IPV4.is_match(s) {
            FakeKind::Ip
        } else {
            FakeKind::Shape
        }
    }
}

pub struct Faker {
    seed: Vec<u8>,
}

impl Faker {
    pub fn new(seed: Option<&str>) -> Self {
        Self { seed: seed.unwrap_or_default().as_bytes().to_vec() }
    }

    fn rng(&self, kind: FakeKind, original: &str) -> Xoshiro256PlusPlus {
        Xoshiro256PlusPlus::from_seed(self.digest(kind, original))
    }

    fn digest(&self, kind: FakeKind, original: &str) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(b"vareto-fake-v1\0");
        h.update((self.seed.len() as u64).to_le_bytes());
        h.update(&self.seed);
        h.update(kind.tag().as_bytes());
        h.update([0]);
        h.update(original.as_bytes());
        h.finalize().into()
    }

    /// Fake a value. `kind`: explicit (from `as = "..."`), else guessed from the
    /// column name, else from the value's shape.
    pub fn fake(&self, kind: Option<FakeKind>, column: &str, v: &Value) -> Value {
        match v {
            Value::Null => Value::Null,
            // Extended JSON wrappers ({"$date": ...}, {"$oid": ...}): fake the payload, keep the wrapper.
            Value::Object(m) if m.len() == 1 && m.keys().all(|k| k.starts_with('$')) => {
                let (k, inner) = m.iter().next().unwrap();
                let inner_kind = match k.as_str() {
                    "$date" => Some(FakeKind::Date),
                    "$oid" => Some(FakeKind::Shape),
                    _ => kind,
                };
                let mut out = serde_json::Map::new();
                out.insert(k.clone(), self.fake(inner_kind, column, inner));
                Value::Object(out)
            }
            Value::Object(m) => {
                Value::Object(m.iter().map(|(k, x)| (k.clone(), self.fake(None, k, x))).collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(|x| self.fake(kind, column, x)).collect()),
            Value::Bool(b) => {
                let mut rng = self.rng(FakeKind::Shape, if *b { "true" } else { "false" });
                Value::Bool(rng.random())
            }
            Value::Number(n) => {
                let s = n.to_string();
                // Same sign and digit layout, e.g. 1234.5 -> 8071.2.
                let faked = shape(&mut self.rng(FakeKind::Number, &s), &s);
                serde_json::from_str::<Value>(&faked).ok().filter(Value::is_number).unwrap_or(Value::String(faked))
            }
            Value::String(s) => {
                let kind = kind.or_else(|| FakeKind::from_column(column)).unwrap_or_else(|| FakeKind::from_value(s));
                Value::String(self.fake_str(kind, s))
            }
        }
    }

    pub fn fake_str(&self, kind: FakeKind, s: &str) -> String {
        if kind == FakeKind::Email {
            return self.email(s);
        }
        let mut rng = self.rng(kind, s);
        let r = &mut rng;
        match kind {
            FakeKind::Email => unreachable!("handled above"),
            FakeKind::Name => Name().fake_with_rng(r),
            FakeKind::FirstName => FirstName().fake_with_rng(r),
            FakeKind::LastName => LastName().fake_with_rng(r),
            FakeKind::Username => {
                let u: String = Username().fake_with_rng(r);
                format!("{u}{}", r.random_range(10..1000))
            }
            FakeKind::Phone => PhoneNumber().fake_with_rng(r),
            FakeKind::Street => self.street(r),
            FakeKind::Address => {
                let street = self.street(r);
                let city: String = CityName().fake_with_rng(r);
                let zip: String = ZipCode().fake_with_rng(r);
                format!("{street}, {city} {zip}")
            }
            FakeKind::City => CityName().fake_with_rng(r),
            FakeKind::Country => CountryName().fake_with_rng(r),
            FakeKind::Zip => ZipCode().fake_with_rng(r),
            FakeKind::Company => CompanyName().fake_with_rng(r),
            FakeKind::Ip => IPv4().fake_with_rng(r),
            FakeKind::Ipv6 => IPv6().fake_with_rng(r),
            FakeKind::Mac => MACAddress().fake_with_rng(r),
            FakeKind::Card => card(r, s),
            FakeKind::Iban => iban(r, s),
            FakeKind::Uuid => {
                let b: [u8; 16] = r.random();
                let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
                // RFC 4122 version 4 layout.
                format!("{}-{}-4{}-{:x}{}-{}", &h[0..8], &h[8..12], &h[13..16], 8 + (b[8] & 3), &h[17..20], &h[20..32])
            }
            FakeKind::Date => match parse_date_prefix(s) {
                Some((date, rest)) => {
                    let shifted = date + chrono::Duration::days(r.random_range(-365..=365));
                    let rest = if rest.is_empty() { String::new() } else { shape(r, rest) };
                    format!("{}{rest}", shifted.format("%Y-%m-%d"))
                }
                None => shape(r, s),
            },
            FakeKind::Text => {
                let n = s.split_whitespace().count().clamp(1, 50);
                let words: Vec<String> = Words(n..n + 1).fake_with_rng(r);
                words.join(" ")
            }
            FakeKind::Number => shape(r, s),
            FakeKind::Shape => shape(r, s),
        }
    }

    /// `first.last.<tag>@example.{com,org,net}`, all lowercase.
    ///
    /// Emails are usually under a UNIQUE constraint, so distinct inputs must give
    /// distinct fakes. The tag is the first 80 bits of the seeded SHA-256 of the
    /// original, so two different emails collide only if those 80 bits do: for
    /// 100M distinct emails the chance of any collision is about 4 in a billion.
    /// The output is lowercase, so it is also unique under case-insensitive
    /// constraints (inputs differing only in case still get different fakes).
    fn email(&self, s: &str) -> String {
        const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
        let d = self.digest(FakeKind::Email, s);
        let bits = u128::from_be_bytes(d[..16].try_into().unwrap()) >> 48; // top 80 bits
        let tag: String = (0..16).rev().map(|i| B32[((bits >> (i * 5)) & 31) as usize] as char).collect();
        let r = &mut Xoshiro256PlusPlus::from_seed(d);
        let clean = |x: String| -> String { x.to_ascii_lowercase().chars().filter(|c| c.is_ascii_alphanumeric()).collect() };
        let first = clean(FirstName().fake_with_rng(r));
        let last = clean(LastName().fake_with_rng(r));
        let domain = ["example.com", "example.org", "example.net"][r.random_range(0..3)];
        format!("{first}.{last}.{tag}@{domain}")
    }

    fn street(&self, r: &mut Xoshiro256PlusPlus) -> String {
        let n: String = BuildingNumber().fake_with_rng(r);
        let name: String = StreetName().fake_with_rng(r);
        let suffix: String = StreetSuffix().fake_with_rng(r);
        format!("{n} {name} {suffix}")
    }
}

/// Letters -> random letters of the same case, digits -> random digits (a leading
/// digit stays non-zero), hex strings stay hex, everything else is kept.
fn shape(r: &mut Xoshiro256PlusPlus, s: &str) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let is_hex = s.len() >= 8 && s.chars().all(|c| c.is_ascii_hexdigit()) && s.chars().any(|c| c.is_ascii_alphabetic());
    let mut prev_digit = false;
    s.chars()
        .map(|c| {
            let out = if is_hex {
                let x = HEX[r.random_range(0..16)] as char;
                if c.is_ascii_uppercase() { x.to_ascii_uppercase() } else { x }
            } else if c.is_ascii_digit() {
                let lo = if !prev_digit && c != '0' { 1 } else { 0 };
                char::from(b'0' + r.random_range(lo..10u8))
            } else if c.is_ascii_lowercase() {
                char::from(b'a' + r.random_range(0..26u8))
            } else if c.is_ascii_uppercase() {
                char::from(b'A' + r.random_range(0..26u8))
            } else if c.is_alphabetic() {
                // Non-ASCII letters: replace with ASCII of the same case.
                if c.is_uppercase() { char::from(b'A' + r.random_range(0..26u8)) } else { char::from(b'a' + r.random_range(0..26u8)) }
            } else {
                c
            };
            prev_digit = c.is_ascii_digit();
            out
        })
        .collect()
}

fn parse_date_prefix(s: &str) -> Option<(chrono::NaiveDate, &str)> {
    let head = s.get(..10)?;
    let d = chrono::NaiveDate::parse_from_str(head, "%Y-%m-%d").ok()?;
    Some((d, &s[10..]))
}

/// Random digits in the original layout (separators kept), with a valid Luhn digit.
fn card(r: &mut Xoshiro256PlusPlus, s: &str) -> String {
    let n = s.chars().filter(char::is_ascii_digit).count();
    let n = if (12..=19).contains(&n) { n } else { 16 };
    let mut digits: Vec<u32> = (0..n - 1).map(|i| if i == 0 { r.random_range(3..7) } else { r.random_range(0..10) }).collect();
    // Luhn check digit for the payload.
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| if i % 2 == 0 { let x = d * 2; if x > 9 { x - 9 } else { x } } else { d })
        .sum();
    digits.push((10 - sum % 10) % 10);
    let mut it = digits.into_iter();
    if s.chars().filter(char::is_ascii_digit).count() == n {
        s.chars().map(|c| if c.is_ascii_digit() { char::from_digit(it.next().unwrap(), 10).unwrap() } else { c }).collect()
    } else {
        it.map(|d| char::from_digit(d, 10).unwrap()).collect()
    }
}

/// Same country code and length, random BBAN, valid ISO 13616 check digits.
fn iban(r: &mut Xoshiro256PlusPlus, s: &str) -> String {
    let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_ascii_uppercase();
    let country = compact.get(..2).filter(|c| c.chars().all(|c| c.is_ascii_uppercase())).unwrap_or("DE").to_string();
    let len = if (15..=34).contains(&compact.len()) { compact.len() } else { 22 };
    let bban: String = (0..len - 4).map(|_| char::from(b'0' + r.random_range(0..10u8))).collect();
    // Check digits: 98 - (BBAN + country + "00") mod 97, letters as A=10..Z=35.
    let rearranged = format!("{bban}{country}00");
    let mut rem: u32 = 0;
    for c in rearranged.chars() {
        let v = c.to_digit(36).unwrap();
        rem = if v >= 10 { (rem * 100 + v) % 97 } else { (rem * 10 + v) % 97 };
    }
    let compact_out = format!("{country}{:02}{bban}", 98 - rem);
    if s.contains(' ') {
        compact_out.as_bytes().chunks(4).map(|c| std::str::from_utf8(c).unwrap()).collect::<Vec<_>>().join(" ")
    } else {
        compact_out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn luhn_ok(s: &str) -> bool {
        let ds: Vec<u32> = s.chars().filter_map(|c| c.to_digit(10)).collect();
        let sum: u32 = ds
            .iter()
            .rev()
            .enumerate()
            .map(|(i, &d)| if i % 2 == 1 { let x = d * 2; if x > 9 { x - 9 } else { x } } else { d })
            .sum();
        sum % 10 == 0
    }

    fn iban_ok(s: &str) -> bool {
        let c: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let r = format!("{}{}", &c[4..], &c[..4]);
        let mut rem = 0u32;
        for ch in r.chars() {
            let v = ch.to_digit(36).unwrap();
            rem = if v >= 10 { (rem * 100 + v) % 97 } else { (rem * 10 + v) % 97 };
        }
        rem == 1
    }

    #[test]
    fn deterministic_and_seed_dependent() {
        let a = Faker::new(None);
        let b = Faker::new(None);
        let salted = Faker::new(Some("secret"));
        let v = json!("john.doe@corp.com");
        assert_eq!(a.fake(None, "email", &v), b.fake(None, "email", &v));
        assert_ne!(a.fake(None, "email", &v), a.fake(None, "email", &json!("jane@corp.com")));
        assert_ne!(a.fake(None, "email", &v), salted.fake(None, "email", &v));
        // Same value in a differently-named column of the same kind -> same fake (joins work).
        assert_eq!(a.fake(None, "users.email", &v), a.fake(None, "contact_email", &v));
    }

    #[test]
    fn emails_are_unique() {
        let f = Faker::new(Some("seed"));
        let shape = Regex::new(r"^[a-z]+\.[a-z]+\.[a-z2-7]{16}@example\.(com|org|net)$").unwrap();
        let mut seen = std::collections::HashSet::new();
        for i in 0..100_000 {
            let e = f.fake_str(FakeKind::Email, &format!("user{i}@corp.com"));
            assert!(shape.is_match(&e), "{e}");
            assert!(seen.insert(e), "collision at {i}");
        }
        // Case variants are different inputs and stay distinct after lowercasing.
        let a = f.fake_str(FakeKind::Email, "John@corp.com");
        let b = f.fake_str(FakeKind::Email, "john@corp.com");
        assert_ne!(a, b);
        assert_eq!(a, f.fake_str(FakeKind::Email, "John@corp.com"));
    }

    #[test]
    fn kinds_from_column_and_value() {
        let f = Faker::new(None);
        let e = f.fake(None, "notes", &json!("a@b.io"));
        assert!(e.as_str().unwrap().contains('@'), "{e}");
        let u = f.fake(None, "id", &json!("123e4567-e89b-12d3-a456-426614174000"));
        assert!(Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$").unwrap().is_match(u.as_str().unwrap()), "{u}");
        let n = f.fake(None, "first_name", &json!("Angelos"));
        assert!(!n.as_str().unwrap().is_empty() && n != json!("Angelos"));
        assert_eq!(FakeKind::from_column("customers.phone_number"), Some(FakeKind::Phone));
        assert_eq!(FakeKind::from_column("company_name"), Some(FakeKind::Company));
        assert_eq!(FakeKind::from_column("ip_address"), Some(FakeKind::Ip));
        assert_eq!(FakeKind::from_column("total"), None);
    }

    #[test]
    fn format_preserving() {
        let f = Faker::new(None);
        let s = f.fake_str(FakeKind::Shape, "AB-1234-xy");
        assert!(Regex::new(r"^[A-Z]{2}-[1-9]\d{3}-[a-z]{2}$").unwrap().is_match(&s), "{s}");
        let oid = f.fake(None, "_id", &json!({"$oid": "64b7f0a1c2d3e4f5a6b7c8d9"}));
        assert!(Regex::new(r"^[0-9a-f]{24}$").unwrap().is_match(oid["$oid"].as_str().unwrap()), "{oid}");
        let d = f.fake(None, "joined", &json!({"$date": "2024-01-02T10:00:00Z"}));
        assert!(parse_date_prefix(d["$date"].as_str().unwrap()).is_some(), "{d}");
        let num = f.fake(None, "balance", &json!(1234.5));
        assert!(num.is_number() && num.to_string().len() == 6, "{num}");
        let int = f.fake(None, "age", &json!(42));
        assert!(int.as_i64().is_some_and(|i| (10..100).contains(&i)), "{int}");
    }

    #[test]
    fn valid_cards_and_ibans() {
        let f = Faker::new(None);
        for input in ["4111 1111 1111 1111", "5500-0000-0000-0004", "378282246310005"] {
            let c = f.fake_str(FakeKind::Card, input);
            assert!(luhn_ok(&c), "{c}");
            assert_eq!(c.len(), input.len(), "{c}");
            assert_ne!(c, input);
        }
        for input in ["DE89 3704 0044 0532 0130 00", "GR1601101250000000012300695"] {
            let i = f.fake_str(FakeKind::Iban, input);
            assert!(iban_ok(&i), "{i}");
            assert_eq!(&i[..2], &input[..2]);
            assert_eq!(i.len(), input.len(), "{i}");
        }
    }
}
