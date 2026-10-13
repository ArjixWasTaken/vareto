//! Completion candidates: which words fit where the cursor is, and how well they
//! match what was typed. Rules decide the lists: after these words, offer this
//! (databases after `use`, tables after FROM, columns of the statement's tables
//! after SELECT, `\x` values, Redis keys after GET, ...). fzf's matching (nucleo)
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
    Database,
    Schema,
    Value,
    Variable,
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
            Kind::Database => "database",
            Kind::Schema => "schema",
            Kind::Value => "value",
            Kind::Variable => "variable",
        }
    }
}

/// What a rule offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offer {
    Tables,
    /// Columns, of the statement's tables when it names any.
    Columns,
    Databases,
    Schemas,
    Values(&'static [&'static str]),
    Vars,
    Keys,
    /// Free text (a title, a NULL string): nothing to offer.
    Nothing,
}

/// After `after` (the words just before the cursor, upper-case except backslash
/// commands; `*` is any word),
/// offer `offer`. The longest matching rule wins.
#[derive(Clone, Copy, Debug)]
pub struct Rule {
    pub after: &'static [&'static str],
    pub offer: Offer,
}

pub const fn rule(after: &'static [&'static str], offer: Offer) -> Rule {
    Rule { after, offer }
}

/// The SQL shells' shared rules.
pub const SQL_RULES: &[Rule] = &[
    rule(&["FROM"], Offer::Tables),
    rule(&["JOIN"], Offer::Tables),
    rule(&["INTO"], Offer::Tables),
    rule(&["UPDATE"], Offer::Tables),
    rule(&["TABLE"], Offer::Tables),
    rule(&["TRUNCATE"], Offer::Tables),
    rule(&["DESCRIBE"], Offer::Tables),
    rule(&["DESC"], Offer::Tables),
    rule(&["SELECT"], Offer::Columns),
    rule(&["DISTINCT"], Offer::Columns),
    rule(&["WHERE"], Offer::Columns),
    rule(&["AND"], Offer::Columns),
    rule(&["OR"], Offer::Columns),
    rule(&["NOT"], Offer::Columns),
    rule(&["ON"], Offer::Columns),
    rule(&["BY"], Offer::Columns),
    rule(&["SET"], Offer::Columns),
    rule(&["HAVING"], Offer::Columns),
    rule(&["RETURNING"], Offer::Columns),
    rule(&["CASE"], Offer::Columns),
    rule(&["WHEN"], Offer::Columns),
    rule(&["THEN"], Offer::Columns),
    rule(&["ELSE"], Offer::Columns),
    rule(&["="], Offer::Columns),
    rule(&["<>"], Offer::Columns),
    rule(&["!="], Offer::Columns),
    rule(&["<"], Offer::Columns),
    rule(&[">"], Offer::Columns),
    rule(&["<="], Offer::Columns),
    rule(&[">="], Offer::Columns),
];

/// What a flavor offers for completion.
#[derive(Clone, Debug, Default)]
pub struct Completions {
    /// An SQL shell: [`SQL_RULES`] apply, and where no rule does, tables, columns
    /// and keywords are offered. Otherwise only `other` is.
    pub sql: bool,
    /// The flavor's own rules (on top of [`SQL_RULES`] for SQL shells).
    pub rules: Vec<Rule>,
    /// Keywords; completed in the case being typed.
    pub keywords: Vec<String>,
    /// Client commands (`\dt`, `.tables`, `pager`, Redis commands), offered at the
    /// start of a line or after their prefix character.
    pub commands: Vec<String>,
    pub tables: Vec<String>,
    /// (table, column)
    pub columns: Vec<(String, String)>,
    pub databases: Vec<String>,
    pub schemas: Vec<String>,
    /// psql variables (`\set`).
    pub vars: Vec<String>,
    /// Redis keys.
    pub keys: Vec<String>,
    /// Redis commands whose first argument is a key; with a trailing `*`, every
    /// argument is (DEL, MGET, ...).
    pub keyed_commands: Vec<String>,
    /// Everything else (mongosh paths and helpers).
    pub other: Vec<(String, Kind)>,
    /// `other` holds dotted paths (`db.coll.find(`): offer one level at a time, so
    /// `db.pro` lists collections and `db.product.` that collection's methods.
    pub dotted: bool,
}

/// A candidate before ranking.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Word {
    pub text: String,
    pub kind: Kind,
    /// The table a column belongs to.
    pub owner: Option<String>,
}

impl Word {
    fn new(text: impl Into<String>, kind: Kind) -> Self {
        Word { text: text.into(), kind, owner: None }
    }
}

/// A ranked candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub text: String,
    pub kind: Kind,
    /// The table a column belongs to.
    pub owner: Option<String>,
    /// Positions (in chars) of `text` that matched what was typed.
    pub indices: Vec<usize>,
}

impl Candidate {
    /// What the popup shows next to it: `table`, `column (users)`.
    pub fn note(&self) -> String {
        match &self.owner {
            Some(o) => format!("{} ({o})", self.kind.label()),
            None => self.kind.label().to_string(),
        }
    }
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

/// The words of `text`, split at whitespace and parentheses, with commas and
/// quotes as words of their own. Upper-cased, except backslash commands, whose
/// case matters (`\c` and `\C` differ).
fn tokens(text: &str) -> Vec<String> {
    let norm = |w: String| if w.starts_with('\\') { w } else { w.to_uppercase() };
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_whitespace() || ch == '(' || ch == ')' || ch == ',' || ch == '\'' || ch == '"' {
            if !cur.is_empty() {
                out.push(norm(std::mem::take(&mut cur)));
            }
            if matches!(ch, ',' | '\'' | '"') {
                out.push(ch.to_string());
            }
        } else {
            cur.push(ch);
        }
    }
    if !cur.is_empty() {
        out.push(norm(cur));
    }
    out
}

fn ends_with(tokens: &[String], after: &[&str]) -> bool {
    tokens.len() >= after.len() && tokens[tokens.len() - after.len()..].iter().zip(after).all(|(t, a)| *a == "*" || t == a)
}

/// The offer of the longest rule that matches the words before the cursor.
fn offer(c: &Completions, before: &[String]) -> Option<Offer> {
    let rules = c.rules.iter().chain(if c.sql { SQL_RULES } else { &[] });
    let best = |toks: &[String]| rules.clone().filter(|r| ends_with(toks, r.after)).max_by_key(|r| r.after.len()).map(|r| r.offer);
    if let Some(o) = best(before) {
        return Some(o);
    }
    // `a, b, |` continues the list of the nearest clause keyword.
    if before.last().map(String::as_str) == Some(",") {
        return (0..before.len())
            .rev()
            .find_map(|i| best(&before[..=i]).filter(|o| matches!(o, Offer::Tables | Offer::Columns)));
    }
    // Redis: the key arguments of a command.
    if let Some(cmd) = before.first() {
        for k in &c.keyed_commands {
            let (name, every) = k.strip_suffix('*').map_or((k.as_str(), false), |n| (n, true));
            if cmd.eq_ignore_ascii_case(name) && (before.len() == 1 || every) {
                return Some(Offer::Keys);
            }
        }
        if !c.keyed_commands.is_empty() {
            return Some(Offer::Nothing);
        }
    }
    None
}

/// Words that end a table reference in FROM/JOIN/UPDATE/INTO (not an alias).
const CLAUSE_WORDS: &[&str] = &[
    "WHERE", "JOIN", "ON", "USING", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL", "GROUP", "ORDER", "LIMIT",
    "OFFSET", "HAVING", "UNION", "EXCEPT", "INTERSECT", "SET", "VALUES", "SELECT", "WINDOW", "FOR", "RETURNING", "LATERAL", ";",
];

/// The tables a statement names (after FROM, JOIN, UPDATE, INTO), with their
/// aliases: `select … from users u join orders as o` gives (users, u), (orders, o).
pub fn statement_tables(line: &str) -> Vec<(String, Option<String>)> {
    let raw: Vec<&str> = line.split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ')').filter(|t| !t.is_empty()).collect();
    let mut toks: Vec<String> = Vec::new();
    for t in raw {
        // Keep commas as words of their own.
        let mut rest = t;
        while let Some(i) = rest.find(',') {
            if i > 0 {
                toks.push(rest[..i].to_string());
            }
            toks.push(",".into());
            rest = &rest[i + 1..];
        }
        if !rest.is_empty() {
            toks.push(rest.trim_end_matches(';').to_string());
        }
    }
    let up = |t: &str| t.to_uppercase();
    let mut out = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        if matches!(up(&toks[i]).as_str(), "FROM" | "JOIN" | "UPDATE" | "INTO") {
            i += 1;
            while i < toks.len() {
                let name = toks[i].rsplit('.').next().unwrap_or("").trim_matches(|c| c == '`' || c == '"').to_string();
                if name.is_empty() || CLAUSE_WORDS.contains(&up(&name).as_str()) {
                    break;
                }
                i += 1;
                let mut alias = None;
                if i < toks.len() && up(&toks[i]) == "AS" {
                    i += 1;
                }
                if i < toks.len() && toks[i] != "," && !CLAUSE_WORDS.contains(&up(&toks[i]).as_str()) {
                    alias = Some(toks[i].trim_matches(|c| c == '`' || c == '"').to_string());
                    i += 1;
                }
                out.push((name, alias));
                if i < toks.len() && toks[i] == "," {
                    i += 1;
                    continue;
                }
                break;
            }
        } else {
            i += 1;
        }
    }
    out
}

/// Columns for the statement on `line`: those of the tables it names, or every
/// column when it names none.
fn columns_for(c: &Completions, line: &str) -> Vec<Word> {
    let named = statement_tables(line);
    let column = |(t, col): &(String, String)| Word { text: col.clone(), kind: Kind::Column, owner: Some(t.clone()) };
    if named.is_empty() {
        return c.columns.iter().map(column).collect();
    }
    c.columns.iter().filter(|(t, _)| named.iter().any(|(n, _)| n.eq_ignore_ascii_case(t))).map(column).collect()
}

/// The table a `prefix.` refers to: a table name, or an alias in the statement.
fn table_of(c: &Completions, line: &str, prefix: &str) -> Option<String> {
    if let Some(t) = c.tables.iter().find(|t| t.eq_ignore_ascii_case(prefix)) {
        return Some(t.clone());
    }
    statement_tables(line)
        .into_iter()
        .find(|(_, a)| a.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(prefix)))
        .map(|(t, _)| t)
}

/// `other` words for `word`: for dotted paths, one level (the same parent path as
/// the word, and no deeper).
fn others(c: &Completions, word: &str) -> Vec<Word> {
    let parent = word.rfind('.').map(|i| &word[..=i]).unwrap_or("");
    let d = depth(word);
    c.other
        .iter()
        .filter(|(w, _)| !c.dotted || (depth(w) == d && w.len() >= parent.len() && w[..parent.len()].eq_ignore_ascii_case(parent)))
        .map(|(w, k)| Word::new(w.clone(), *k))
        .collect()
}

/// Every candidate that fits the context of the word at `pos`, unranked: (start of
/// the word, candidates).
pub fn candidates(c: &Completions, line: &str, pos: usize) -> (usize, Vec<Word>) {
    let (start, word) = word_at(line, pos);
    let before_text = &line[..start];
    let before = tokens(before_text);
    let typed_upper = word.chars().next().is_some_and(char::is_uppercase);
    let at_start = before_text.trim().is_empty();
    let mut out: Vec<Word> = Vec::new();
    let keywords = |out: &mut Vec<Word>| {
        out.extend(c.keywords.iter().map(|w| Word::new(if typed_upper { w.clone() } else { w.to_lowercase() }, Kind::Keyword)));
    };
    let tables = |out: &mut Vec<Word>| out.extend(c.tables.iter().map(|t| Word::new(t.clone(), if c.sql { Kind::Table } else { Kind::Collection })));
    if word.starts_with('\\') || (word.starts_with('.') && at_start && !c.commands.is_empty()) {
        out.extend(c.commands.iter().map(|w| Word::new(w.clone(), Kind::Command)));
    } else if let Some(table) = c.sql.then(|| word.split_once('.')).flatten().and_then(|(p, _)| table_of(c, line, p)) {
        // `users.em`, `u.em`: that table's columns.
        let prefix = &word[..word.find('.').unwrap_or(0)];
        out.extend(
            c.columns
                .iter()
                .filter(|(t, _)| t.eq_ignore_ascii_case(&table))
                .map(|(t, col)| Word { text: format!("{prefix}.{col}"), kind: Kind::Column, owner: Some(t.clone()) }),
        );
    } else {
        match offer(c, &before) {
            Some(Offer::Tables) => tables(&mut out),
            Some(Offer::Columns) => {
                out.extend(columns_for(c, line));
                keywords(&mut out);
            }
            Some(Offer::Databases) => out.extend(c.databases.iter().map(|d| Word::new(d.clone(), Kind::Database))),
            Some(Offer::Schemas) => out.extend(c.schemas.iter().map(|d| Word::new(d.clone(), Kind::Schema))),
            Some(Offer::Values(vs)) => out.extend(vs.iter().map(|v| Word::new(*v, Kind::Value))),
            Some(Offer::Vars) => out.extend(c.vars.iter().map(|v| Word::new(v.clone(), Kind::Variable))),
            Some(Offer::Keys) => out.extend(c.keys.iter().map(|k| Word::new(k.clone(), Kind::Key))),
            Some(Offer::Nothing) => {}
            None if at_start => {
                keywords(&mut out);
                out.extend(c.commands.iter().map(|w| Word::new(w.clone(), Kind::Command)));
                out.extend(others(c, word));
            }
            None if c.sql => {
                tables(&mut out);
                out.extend(columns_for(c, line));
                keywords(&mut out);
            }
            None => {}
        }
        if !at_start && (c.dotted || offer(c, &before).is_none()) {
            out.extend(others(c, word));
        }
    }
    // The first of each text wins (a statement table's column over another's).
    let mut seen = std::collections::HashSet::new();
    out.retain(|w| seen.insert(w.text.clone()));
    (start, out)
}

/// The words at the level of the word at `pos` (after the last dot of a path),
/// unranked: (where the word starts, the word, the words).
fn level_words<'a>(c: &Completions, line: &'a str, pos: usize) -> (usize, &'a str, Vec<Word>) {
    let (start, all) = candidates(c, line, pos);
    let word = &line[start..pos];
    let cut = word.rfind('.').map(|i| i + 1).unwrap_or(0);
    let parent = &word[..cut];
    let mut seen = std::collections::HashSet::new();
    let words = all
        .into_iter()
        .filter(|w| w.text.len() > cut && w.text.is_char_boundary(cut) && w.text[..cut].eq_ignore_ascii_case(parent))
        .map(|w| Word { text: w.text[cut..].to_string(), ..w })
        .filter(|w| seen.insert(w.text.clone()))
        .collect();
    (start + cut, &word[cut..], words)
}

/// Every word that fits at `pos`, unranked, for the fzf finder to rank (Ctrl-T):
/// (where the word starts, the word typed so far, the words).
pub fn complete_all<'a>(c: &Completions, line: &'a str, pos: usize) -> (usize, &'a str, Vec<Candidate>) {
    let (start, typed, words) = level_words(c, line, pos);
    (start, typed, words.into_iter().map(|w| Candidate { text: w.text, kind: w.kind, owner: w.owner, indices: Vec::new() }).collect())
}

/// Candidates for the word at `pos`, best first: fzf-style fuzzy matches, with
/// those that start with the word ahead. Completion is per word: in a dotted path
/// (`db.product.fi`, `users.em`) only the part after the last dot is completed and
/// replaced, and candidates are the names at that level (`find(`, `email`). An
/// empty word gets every candidate of its context, in order. Returns where the
/// replaced word starts.
pub fn complete(c: &Completions, line: &str, pos: usize) -> (usize, Vec<Candidate>) {
    let (start, needle, words) = level_words(c, line, pos);
    if needle.is_empty() {
        return (start, words.into_iter().map(|w| Candidate { text: w.text, kind: w.kind, owner: w.owner, indices: Vec::new() }).collect());
    }
    let mut config = Config::DEFAULT;
    config.prefer_prefix = true;
    let mut matcher = Matcher::new(config);
    let atom = Atom::new(needle, CaseMatching::Ignore, Normalization::Smart, AtomKind::Fuzzy, false);
    let lower = needle.to_lowercase();
    let mut buf = Vec::new();
    let mut ranked: Vec<(bool, u16, Candidate)> = words
        .into_iter()
        .filter(|w| !w.text.eq_ignore_ascii_case(needle))
        .filter_map(|w| {
            let mut idx = Vec::new();
            let score = atom.indices(Utf32Str::new(&w.text, &mut buf), &mut matcher, &mut idx)?;
            idx.sort_unstable();
            idx.dedup();
            let prefix = w.text.to_lowercase().starts_with(&lower);
            let indices = idx.into_iter().map(|i| i as usize).collect();
            Some((prefix, score, Candidate { text: w.text, kind: w.kind, owner: w.owner, indices }))
        })
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.text.len().cmp(&b.2.text.len())).then(a.2.text.cmp(&b.2.text)));
    (start, ranked.into_iter().map(|(_, _, c)| c).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn sql() -> Completions {
        Completions {
            sql: true,
            rules: vec![
                rule(&["USE"], Offer::Databases),
                rule(&["SHOW", "TABLES", "FROM"], Offer::Databases),
                rule(&["\\c"], Offer::Databases),
                rule(&["\\x"], Offer::Values(&["on", "off", "auto"])),
                rule(&["\\set"], Offer::Vars),
                rule(&["\\pset"], Offer::Values(&["format", "border"])),
                rule(&["\\pset", "FORMAT"], Offer::Values(&["aligned", "csv"])),
                rule(&["\\pset", "TITLE"], Offer::Nothing),
                rule(&["SET", "SEARCH_PATH", "TO"], Offer::Schemas),
                rule(&["\\d"], Offer::Tables),
                rule(&[".MODE"], Offer::Values(&["box", "csv"])),
            ],
            keywords: strings(&["SELECT", "FROM", "WHERE", "ORDER BY"]),
            commands: strings(&["\\dt", "\\d", "\\x"]),
            tables: strings(&["users", "orders", "productcategory", "purchaseorder"]),
            columns: vec![
                ("users".into(), "id".into()),
                ("users".into(), "email".into()),
                ("orders".into(), "user_id".into()),
                ("orders".into(), "total".into()),
                ("productcategory".into(), "name".into()),
            ],
            databases: strings(&["shop", "analytics"]),
            schemas: strings(&["public", "audit"]),
            vars: strings(&["ON_ERROR_STOP", "PROMPT1"]),
            ..Completions::default()
        }
    }

    fn ranked(line: &str) -> Vec<Candidate> {
        complete(&sql(), line, line.len()).1
    }

    fn texts(line: &str) -> Vec<String> {
        ranked(line).into_iter().map(|c| c.text).collect()
    }

    fn sorted(line: &str) -> Vec<String> {
        let mut t = texts(line);
        t.sort();
        t
    }

    fn first(line: &str) -> String {
        texts(line).into_iter().next().unwrap_or_default()
    }

    #[test]
    fn sql_contexts() {
        // After FROM: tables only.
        assert_eq!(first("select * from us"), "users");
        assert!(ranked("update o").iter().all(|c| c.kind == Kind::Table));
        assert_eq!(first("select * from users u, o"), "orders");
        assert_eq!(first("\\d u"), "users");
        // After SELECT/WHERE: columns (and keywords), never tables.
        assert_eq!(first("select e"), "email");
        assert_eq!(first("select id, e"), "email");
        assert!(ranked("select e").iter().all(|c| c.kind != Kind::Table));
        // The start of a statement: keywords and commands, in the typed case.
        assert_eq!(first("se"), "select");
        assert_eq!(first("SE"), "SELECT");
        assert!(ranked("e").iter().all(|c| matches!(c.kind, Kind::Keyword | Kind::Command)));
        assert_eq!(texts("\\d"), vec!["\\dt"]);
        assert_eq!(first("select * from users w"), "where");
    }

    #[test]
    fn rules() {
        // `use`: databases, nothing else (the bug: tables were offered).
        assert_eq!(sorted("use "), vec!["analytics", "shop"]);
        assert_eq!(texts("use sh"), vec!["shop"]);
        assert!(ranked("use a").iter().all(|c| c.kind == Kind::Database));
        // The longest rule wins: SHOW TABLES FROM is databases, not FROM's tables.
        assert_eq!(sorted("show tables from "), vec!["analytics", "shop"]);
        assert_eq!(sorted("\\c "), vec!["analytics", "shop"]);
        assert_eq!(texts("\\x "), vec!["on", "off", "auto"]);
        assert_eq!(sorted("\\set "), vec!["ON_ERROR_STOP", "PROMPT1"]);
        assert_eq!(sorted("\\pset "), vec!["border", "format"]);
        assert_eq!(sorted("\\pset format "), vec!["aligned", "csv"]);
        assert!(ranked("\\pset title ").is_empty());
        assert_eq!(sorted("set search_path to "), vec!["audit", "public"]);
        assert_eq!(sorted(".mode "), vec!["box", "csv"]);
        // A backslash command's case matters: \C (title) isn't \c.
        assert!(ranked("\\C ").iter().all(|c| c.kind != Kind::Database));
    }

    #[test]
    fn scoped_columns() {
        // Columns of the statement's tables, with their table.
        let c = complete(&sql(), "select  from users", 7).1;
        let mut cols: Vec<&str> = c.iter().filter(|c| c.kind == Kind::Column).map(|c| c.text.as_str()).collect();
        cols.sort();
        assert_eq!(cols, vec!["email", "id"]);
        assert!(c.iter().any(|c| c.text == "email" && c.note() == "column (users)"));
        // users isn't in the statement, so its email isn't offered.
        assert!(complete(&sql(), "select e from orders", 8).1.iter().all(|c| c.text != "email"));
        // Both tables of a join, nothing else.
        let both: Vec<String> = {
            let mut t: Vec<String> = complete(&sql(), "select  from orders o join users u on", 7)
                .1
                .into_iter()
                .filter(|c| c.kind == Kind::Column)
                .map(|c| c.text)
                .collect();
            t.sort();
            t
        };
        assert_eq!(both, vec!["email", "id", "total", "user_id"]);
        // `alias.` is that table's columns.
        assert_eq!(complete(&sql(), "select u. from users u", 9).1.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(), vec!["id", "email"]);
        assert_eq!(texts("select users.e"), vec!["email"]);
        // No table named: every column.
        assert!(texts("select na").contains(&"name".to_string()));
    }

    #[test]
    fn statement_table_parsing() {
        assert_eq!(statement_tables("select * from users"), vec![("users".into(), None)]);
        assert_eq!(
            statement_tables("select * from shop.users as u join orders o on o.user_id = u.id, items"),
            vec![("users".into(), Some("u".into())), ("orders".into(), Some("o".into()))]
        );
        assert_eq!(statement_tables("select * from a x, b where 1"), vec![("a".into(), Some("x".into())), ("b".into(), None)]);
        assert_eq!(statement_tables("update users set a = 1"), vec![("users".into(), None)]);
    }

    #[test]
    fn redis_keys() {
        let c = Completions {
            commands: strings(&["GET", "INFO", "DEL"]),
            keys: strings(&["user:1", "user:2"]),
            keyed_commands: strings(&["GET", "DEL*"]),
            ..Completions::default()
        };
        let texts = |line: &str| {
            let mut t: Vec<String> = complete(&c, line, line.len()).1.into_iter().map(|c| c.text).collect();
            t.sort();
            t
        };
        assert_eq!(texts("G"), vec!["GET"]);
        assert_eq!(texts("GET "), vec!["user:1", "user:2"]);
        assert_eq!(texts("GET user:1 "), Vec::<String>::new());
        assert_eq!(texts("DEL user:1 "), vec!["user:1", "user:2"]);
        assert_eq!(texts("INFO "), Vec::<String>::new());
    }

    #[test]
    fn fuzzy_ranking() {
        assert_eq!(first("select * from pcat"), "productcategory");
        let mut p = texts("select * from p");
        p.sort();
        assert_eq!(p, vec!["productcategory", "purchaseorder"]);
        let hit = &ranked("select * from pcat")[0];
        assert_eq!((hit.indices.len(), hit.indices[0]), (4, 0));
    }

    #[test]
    fn dotted_levels() {
        let words = ["db.product", "db.product.find(", "db.productcategory", "db.productcategory.find(", "db.listCollections(", ".sort("];
        let c = Completions {
            rules: vec![rule(&["USE"], Offer::Databases), rule(&["SHOW"], Offer::Values(&["dbs", "collections"]))],
            databases: strings(&["admin", "shop"]),
            other: words.iter().map(|s| (s.to_string(), Kind::Method)).collect(),
            dotted: true,
            ..Completions::default()
        };
        let texts = |line: &str| complete(&c, line, line.len()).1.into_iter().map(|c| c.text).collect::<Vec<_>>();
        assert_eq!(texts("db.prod"), vec!["product", "productcategory"]);
        assert_eq!(texts("db.product."), vec!["find("]);
        assert_eq!(texts("db.l"), vec!["listCollections("]);
        assert_eq!(texts("db.product.find().so"), vec!["sort("]);
        assert_eq!(complete(&c, "db.prod", 7).0, 3);
        assert_eq!(texts("use "), vec!["admin", "shop"]);
        assert_eq!(texts("show "), vec!["dbs", "collections"]);
    }
}
