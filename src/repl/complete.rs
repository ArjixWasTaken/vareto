//! Completion candidates: which words fit where the cursor is, and how well they
//! match what was typed. SQL context decides the lists (tables after
//! FROM/JOIN/INTO/UPDATE/…, columns after SELECT/WHERE/ON/BY/SET/…, `table.column`
//! after a table name, client commands after `\` or `.`); fzf's matching (nucleo)
//! ranks them. Showing and picking them is the line editor's job (reedline).

use nucleo_matcher::pattern::{Atom, AtomKind, CaseMatching, Normalization};
use nucleo_matcher::{Config, Matcher, Utf32Str};

/// What a candidate is, shown next to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Keyword,
    Command,
    Table,
    Column,
    Method,
    Collection,
    Key,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Keyword => "keyword",
            Kind::Command => "command",
            Kind::Table => "table",
            Kind::Column => "column",
            Kind::Method => "method",
            Kind::Collection => "collection",
            Kind::Key => "key",
        }
    }
}

/// What a flavor offers for completion.
#[derive(Clone, Debug, Default)]
pub struct Completions {
    /// Keywords; completed in the case being typed.
    pub keywords: Vec<String>,
    /// Client commands (`\dt`, `.tables`, `pager`, ...), offered at the start of a
    /// line or after their prefix character.
    pub commands: Vec<String>,
    pub tables: Vec<String>,
    /// (table, column)
    pub columns: Vec<(String, String)>,
    /// Everything else (Redis commands and keys, mongosh paths): offered everywhere.
    pub other: Vec<(String, Kind)>,
    /// `other` holds dotted paths (`db.coll.find(`): offer one level at a time, so
    /// `db.pro` lists collections and `db.product.` that collection's methods.
    pub dotted: bool,
}

/// A ranked candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub text: String,
    pub kind: Kind,
    /// Positions (in chars) of `text` that matched what was typed.
    pub indices: Vec<usize>,
}

#[derive(PartialEq, Debug)]
enum Context {
    /// The first word: a keyword or a client command.
    Start,
    Tables,
    Columns,
    Anything,
}

const TABLE_KEYWORDS: &[&str] = &["FROM", "JOIN", "INTO", "UPDATE", "TABLE", "DESCRIBE", "DESC", "TRUNCATE", "\\D", "\\D+", "\\DT", "\\DT+"];
const COLUMN_KEYWORDS: &[&str] = &[
    "SELECT", "WHERE", "AND", "OR", "ON", "BY", "SET", "HAVING", "DISTINCT", "RETURNING", "NOT", "WHEN", "THEN", "ELSE", "CASE",
];

/// Where the word being typed sits, from the text before it.
fn context(before: &str) -> Context {
    let tokens: Vec<String> = before
        .split(|c: char| c.is_whitespace() || c == '(' || c == ')')
        .filter(|t| !t.is_empty())
        .map(str::to_uppercase)
        .collect();
    let classify = |t: &str| {
        if TABLE_KEYWORDS.contains(&t) {
            Some(Context::Tables)
        } else if COLUMN_KEYWORDS.contains(&t) {
            Some(Context::Columns)
        } else {
            None
        }
    };
    let Some(last) = tokens.last() else { return Context::Anything };
    if last.ends_with(',') {
        // `a, b, |` continues the list of the nearest clause keyword.
        return tokens.iter().rev().find_map(|t| classify(t.trim_end_matches(','))).unwrap_or(Context::Anything);
    }
    // Right after FROM: a table. After `FROM t`: a keyword (WHERE, JOIN, ...).
    classify(last).unwrap_or(Context::Anything)
}

/// The word at `pos`: (where it starts, the word).
pub fn word_at(line: &str, pos: usize) -> (usize, &str) {
    let start = line[..pos]
        .rfind(|ch: char| !(ch.is_alphanumeric() || matches!(ch, '_' | '.' | '\\' | '$' | '+')))
        .map(|i| i + 1)
        .unwrap_or(0);
    (start, &line[start..pos])
}

/// Dots outside parentheses (`db.c.find(a.b` has 2): a dotted path's depth.
fn depth(s: &str) -> usize {
    let mut level = 0i32;
    s.chars()
        .filter(|&c| {
            match c {
                '(' => level += 1,
                ')' => level -= 1,
                _ => {}
            }
            c == '.' && level == 0
        })
        .count()
}

/// Every candidate that fits the context of the word at `pos`, unranked: (start of
/// the word, candidates).
pub fn candidates(c: &Completions, line: &str, pos: usize) -> (usize, Vec<(String, Kind)>) {
    let (start, word) = word_at(line, pos);
    let before = &line[..start];
    let typed_upper = word.chars().next().is_some_and(char::is_uppercase);
    let mut out: Vec<(String, Kind)> = Vec::new();
    if word.starts_with('\\') || (word.starts_with('.') && before.trim().is_empty() && !c.commands.is_empty()) {
        out.extend(c.commands.iter().map(|w| (w.clone(), Kind::Command)));
    } else if let Some((table, _)) = word.split_once('.').filter(|(t, _)| c.tables.iter().any(|x| x.eq_ignore_ascii_case(t))) {
        out.extend(c.columns.iter().filter(|(t, _)| t.eq_ignore_ascii_case(table)).map(|(_, col)| (format!("{table}.{col}"), Kind::Column)));
    } else {
        let ctx = if before.trim().is_empty() { Context::Start } else { context(before) };
        if matches!(ctx, Context::Tables | Context::Anything) {
            out.extend(c.tables.iter().map(|w| (w.clone(), Kind::Table)));
        }
        if matches!(ctx, Context::Columns | Context::Anything) {
            out.extend(c.columns.iter().map(|(_, col)| (col.clone(), Kind::Column)));
        }
        if ctx != Context::Tables {
            out.extend(c.keywords.iter().map(|w| (if typed_upper { w.clone() } else { w.to_lowercase() }, Kind::Keyword)));
        }
        if ctx == Context::Start {
            out.extend(c.commands.iter().map(|w| (w.clone(), Kind::Command)));
        }
        if c.dotted {
            // One level: the same parent path as the word, and no deeper.
            let parent = word.rfind('.').map(|i| &word[..=i]).unwrap_or("");
            let d = depth(word);
            out.extend(
                c.other
                    .iter()
                    .filter(|(w, _)| depth(w) == d && w.len() >= parent.len() && w[..parent.len()].eq_ignore_ascii_case(parent))
                    .cloned(),
            );
        } else {
            out.extend(c.other.iter().cloned());
        }
    }
    out.sort();
    out.dedup_by(|a, b| a.0 == b.0);
    (start, out)
}

/// Candidates for the word at `pos`, best first: fzf-style fuzzy matches, with
/// those that start with the word ahead. Completion is per word: in a dotted path
/// (`db.product.fi`, `users.em`) only the part after the last dot is completed and
/// replaced, and candidates are the names at that level (`find(`, `email`). An
/// empty word gets every candidate of its context, in order. Returns where the
/// replaced word starts.
pub fn complete(c: &Completions, line: &str, pos: usize) -> (usize, Vec<Candidate>) {
    let (start, all) = candidates(c, line, pos);
    let word = &line[start..pos];
    // The word proper: after the last dot of a path.
    let cut = word.rfind('.').map(|i| i + 1).unwrap_or(0);
    let parent = &word[..cut];
    let needle = &word[cut..];
    let mut seen = std::collections::HashSet::new();
    let words: Vec<(String, Kind)> = all
        .into_iter()
        .filter(|(text, _)| text.len() > cut && text.is_char_boundary(cut) && text[..cut].eq_ignore_ascii_case(parent))
        .map(|(text, kind)| (text[cut..].to_string(), kind))
        .filter(|(text, _)| seen.insert(text.clone()))
        .collect();
    let start = start + cut;
    if needle.is_empty() {
        return (start, words.into_iter().map(|(text, kind)| Candidate { text, kind, indices: Vec::new() }).collect());
    }
    let mut config = Config::DEFAULT;
    config.prefer_prefix = true;
    let mut matcher = Matcher::new(config);
    let atom = Atom::new(needle, CaseMatching::Ignore, Normalization::Smart, AtomKind::Fuzzy, false);
    let lower = needle.to_lowercase();
    let mut buf = Vec::new();
    let mut ranked: Vec<(bool, u16, Candidate)> = words
        .into_iter()
        .filter(|(text, _)| !text.eq_ignore_ascii_case(needle))
        .filter_map(|(text, kind)| {
            let mut idx = Vec::new();
            let score = atom.indices(Utf32Str::new(&text, &mut buf), &mut matcher, &mut idx)?;
            idx.sort_unstable();
            idx.dedup();
            let prefix = text.to_lowercase().starts_with(&lower);
            Some((prefix, score, Candidate { text, kind, indices: idx.into_iter().map(|i| i as usize).collect() }))
        })
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.text.len().cmp(&b.2.text.len())).then(a.2.text.cmp(&b.2.text)));
    (start, ranked.into_iter().map(|(_, _, c)| c).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql() -> Completions {
        Completions {
            keywords: ["SELECT", "FROM", "WHERE", "ORDER BY"].iter().map(|s| s.to_string()).collect(),
            commands: ["\\dt", "\\d", "\\x"].iter().map(|s| s.to_string()).collect(),
            tables: vec!["users".into(), "orders".into(), "productcategory".into(), "purchaseorder".into()],
            columns: vec![("users".into(), "id".into()), ("users".into(), "email".into()), ("orders".into(), "user_id".into())],
            other: Vec::new(),
            dotted: false,
        }
    }

    fn ranked(line: &str) -> Vec<Candidate> {
        complete(&sql(), line, line.len()).1
    }

    fn texts(line: &str) -> Vec<String> {
        ranked(line).into_iter().map(|c| c.text).collect()
    }

    fn first(line: &str) -> String {
        texts(line).into_iter().next().unwrap_or_default()
    }

    #[test]
    fn contexts() {
        // After FROM: tables only.
        assert_eq!(first("select * from us"), "users");
        assert!(ranked("update o").iter().all(|c| c.kind == Kind::Table));
        assert_eq!(first("select * from users u, o"), "orders");
        assert_eq!(first("\\d u"), "users");
        // After SELECT/WHERE: columns (and keywords), never tables.
        assert_eq!(first("select e"), "email");
        assert_eq!(first("select id, e"), "email");
        assert_eq!(first("select * from users where u"), "user_id");
        assert!(ranked("select e").iter().all(|c| c.kind != Kind::Table));
        // Per word: after `users.`, only the column name is completed.
        assert_eq!(texts("select users.e"), vec!["email"]);
        assert_eq!(complete(&sql(), "select users.e", 14).0, 13);
        // The start of a statement: keywords and commands, in the typed case.
        assert_eq!(first("se"), "select");
        assert_eq!(first("SE"), "SELECT");
        assert!(ranked("e").iter().all(|c| matches!(c.kind, Kind::Keyword | Kind::Command)));
        assert_eq!(texts("\\d"), vec!["\\dt"]);
        assert_eq!(first("select * from users w"), "where");
        // Kinds come along.
        assert_eq!(ranked("select * from us")[0].kind, Kind::Table);
    }

    #[test]
    fn fuzzy_ranking() {
        // fzf-style: letters in order, not necessarily adjacent.
        assert_eq!(first("select * from pcat"), "productcategory");
        assert!(texts("select * from pc").contains(&"productcategory".to_string()));
        // A prefix beats a fuzzy match.
        let mut p = texts("select * from p");
        p.sort();
        assert_eq!(p, vec!["productcategory", "purchaseorder"]);
        assert_eq!(first("select * from or"), "orders");
        // The matched characters, for highlighting.
        let hit = &ranked("select * from pcat")[0];
        assert_eq!((hit.indices.len(), hit.indices[0]), (4, 0));
        // An empty word lists the context.
        assert_eq!(texts("select * from "), vec!["orders", "productcategory", "purchaseorder", "users"]);
    }

    #[test]
    fn dotted_levels() {
        let words = ["db.product", "db.product.find(", "db.productcategory", "db.productcategory.find(", "db.listCollections(", ".sort("];
        let c = Completions { other: words.iter().map(|s| (s.to_string(), Kind::Method)).collect(), dotted: true, ..Completions::default() };
        let texts = |line: &str| complete(&c, line, line.len()).1.into_iter().map(|c| c.text).collect::<Vec<_>>();
        // One word at a time: the names at that level, not whole paths.
        assert_eq!(texts("db.prod"), vec!["product", "productcategory"]);
        assert_eq!(texts("db.product."), vec!["find("]);
        assert_eq!(texts("db.l"), vec!["listCollections("]);
        assert_eq!(texts("db.lc"), vec!["listCollections("]);
        assert_eq!(texts("db.product.find().so"), vec!["sort("]);
        // The replaced span is just the word.
        assert_eq!(complete(&c, "db.prod", 7).0, 3);
    }
}
