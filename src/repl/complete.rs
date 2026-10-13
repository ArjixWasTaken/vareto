//! Tab completion that knows where it is in an SQL statement: table names after
//! FROM/JOIN/INTO/UPDATE/…, column names after SELECT/WHERE/ON/BY/SET/…,
//! `table.column` after a table name, client commands after `\` (or `.`).

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
    /// Everything else (Redis commands, mongosh helpers): offered everywhere.
    pub other: Vec<String>,
    /// `other` holds dotted paths (`db.coll.find(`): offer one level at a time, so
    /// `db.pro` lists collections and `db.product.` that collection's methods.
    pub dotted: bool,
}

impl Completions {
    /// A flat word list, for flavors without SQL context.
    pub fn flat(words: Vec<String>) -> Self {
        Self { other: words, ..Self::default() }
    }
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

/// Candidates for the word at `pos` in `line`: (start of the word, candidates).
pub fn complete(c: &Completions, line: &str, pos: usize) -> (usize, Vec<String>) {
    let start = line[..pos]
        .rfind(|ch: char| !(ch.is_alphanumeric() || matches!(ch, '_' | '.' | '\\' | '$' | '+')))
        .map(|i| i + 1)
        .unwrap_or(0);
    let word = &line[start..pos];
    let before = &line[..start];
    if word.is_empty() {
        return (pos, Vec::new());
    }
    let lower = word.to_lowercase();
    let typed_upper = word.chars().next().is_some_and(char::is_uppercase);
    let matching = |w: &String| w.to_lowercase().starts_with(&lower) && w.len() > word.len();
    let mut out: Vec<String> = Vec::new();

    if word.starts_with('\\') || (word.starts_with('.') && before.trim().is_empty()) {
        out.extend(c.commands.iter().filter(|w| matching(w)).cloned());
    } else if let Some((table, _)) = word.split_once('.').filter(|(t, _)| c.tables.iter().any(|x| x.eq_ignore_ascii_case(t))) {
        out.extend(
            c.columns
                .iter()
                .filter(|(t, _)| t.eq_ignore_ascii_case(table))
                .map(|(_, col)| format!("{table}.{col}"))
                .filter(|w| matching(w)),
        );
    } else {
        let ctx = if before.trim().is_empty() { Context::Start } else { context(before) };
        if matches!(ctx, Context::Tables | Context::Anything) {
            out.extend(c.tables.iter().filter(|w| matching(w)).cloned());
        }
        if matches!(ctx, Context::Columns | Context::Anything) {
            out.extend(c.columns.iter().map(|(_, col)| col.clone()).filter(|w| matching(w)));
        }
        if ctx != Context::Tables {
            out.extend(
                c.keywords
                    .iter()
                    .filter(|w| matching(w))
                    .map(|w| if typed_upper { w.clone() } else { w.to_lowercase() }),
            );
        }
        if ctx == Context::Start {
            out.extend(c.commands.iter().filter(|w| matching(w)).cloned());
        }
        let level = |w: &String| -> String {
            match w[word.len()..].find('.') {
                // Keep the dot when it's the next character, so the next Tab goes on.
                Some(0) => w[..word.len() + 1].to_string(),
                Some(i) => w[..word.len() + i].to_string(),
                None => w.clone(),
            }
        };
        out.extend(c.other.iter().filter(|w| matching(w)).map(|w| if c.dotted { level(w) } else { w.clone() }));
    }
    out.sort();
    out.dedup();
    (start, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql() -> Completions {
        Completions {
            keywords: ["SELECT", "FROM", "WHERE", "ORDER BY"].iter().map(|s| s.to_string()).collect(),
            commands: ["\\dt", "\\d", "\\x"].iter().map(|s| s.to_string()).collect(),
            tables: vec!["users".into(), "orders".into()],
            columns: vec![("users".into(), "id".into()), ("users".into(), "email".into()), ("orders".into(), "user_id".into())],
            other: Vec::new(),
            dotted: false,
        }
    }

    fn cands(line: &str) -> Vec<String> {
        complete(&sql(), line, line.len()).1
    }

    #[test]
    fn contexts() {
        assert_eq!(cands("select * from u"), vec!["users"]);
        assert_eq!(cands("select e"), vec!["email"]);
        assert_eq!(cands("select id, e"), vec!["email"]);
        assert_eq!(cands("select * from users where u"), vec!["user_id"]);
        assert_eq!(cands("select users.e"), vec!["users.email"]);
        assert_eq!(cands("se"), vec!["select"]);
        assert_eq!(cands("e"), Vec::<String>::new());
        assert_eq!(cands("SE"), vec!["SELECT"]);
        assert_eq!(cands("\\d"), vec!["\\dt"]);
        assert_eq!(cands("select * from users w"), vec!["where"]);
        assert_eq!(cands("update o"), vec!["orders"]);
        assert_eq!(cands("select * from users u, o"), vec!["orders"]);
        assert_eq!(cands("\\d u"), vec!["users"]);
    }

    #[test]
    fn dotted_levels() {
        let words = ["db.product", "db.product.find(", "db.productcategory", "db.productcategory.find(", "db.listCollections("];
        let c = Completions { other: words.iter().map(|s| s.to_string()).collect(), dotted: true, ..Completions::default() };
        let cands = |line: &str| complete(&c, line, line.len()).1;
        assert_eq!(cands("db.prod"), vec!["db.product", "db.productcategory"]);
        assert_eq!(cands("db.product"), vec!["db.product.", "db.productcategory"]);
        assert_eq!(cands("db.product."), vec!["db.product.find("]);
        assert_eq!(cands("db.l"), vec!["db.listCollections("]);
    }
}
