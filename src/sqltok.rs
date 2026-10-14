//! SQL tokens with byte offsets: sqlparser's tokenizer (the dialect's quotes,
//! escapes, comments and quoted identifiers) for code that also needs the text as
//! written, such as a statement's target in `\copy` or a `SET` expression in LOAD DATA.

use sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::readonly::SqlDialect;

/// A token and where it is in the text (byte offsets, end exclusive).
#[derive(Debug, Clone, PartialEq)]
pub struct Tok {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

/// Byte offsets of each (line, column) position, as sqlparser counts them: lines
/// from 1 at `\n`, columns from 1 in characters.
struct Offsets(Vec<Vec<usize>>);

impl Offsets {
    fn new(text: &str) -> Self {
        let mut lines: Vec<Vec<usize>> = vec![Vec::new()];
        for (i, ch) in text.char_indices() {
            lines.last_mut().expect("a line").push(i);
            if ch == '\n' {
                lines.push(Vec::new());
            }
        }
        // One past each line's last character: where the next line starts, or the end.
        let ends: Vec<usize> = (0..lines.len()).map(|k| lines.get(k + 1).and_then(|l| l.first().copied()).unwrap_or(text.len())).collect();
        for (line, end) in lines.iter_mut().zip(ends) {
            line.push(end);
        }
        Offsets(lines)
    }

    fn at(&self, loc: Location, text_len: usize) -> usize {
        let (line, col) = (loc.line as usize, loc.column as usize);
        self.0.get(line.wrapping_sub(1)).and_then(|l| l.get(col.wrapping_sub(1))).copied().unwrap_or(text_len)
    }
}

/// The tokens of `text` in `dialect`, without whitespace and comments.
pub fn tokenize(dialect: SqlDialect, text: &str) -> Result<Vec<Tok>, sqlparser::tokenizer::TokenizerError> {
    let d = crate::readonly::sqlparser_dialect(dialect);
    let toks = Tokenizer::new(d.as_ref(), text).tokenize_with_location()?;
    let offsets = Offsets::new(text);
    Ok(toks
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
        .map(|t| Tok { start: offsets.at(t.span.start, text.len()), end: offsets.at(t.span.end, text.len()), token: t.token })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_offsets() {
        let text = "select 'é' as \"ünï\",\n  `x` from t";
        let toks = tokenize(SqlDialect::Mysql, text).unwrap();
        let spans: Vec<&str> = toks.iter().map(|t| &text[t.start..t.end]).collect();
        assert_eq!(spans, vec!["select", "'é'", "as", "\"ünï\"", ",", "`x`", "from", "t"]);
    }
}
