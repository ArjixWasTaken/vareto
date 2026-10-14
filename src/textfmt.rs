//! Result layouts that can be written one record at a time: psql `unaligned` and
//! `csv`, mysql `--batch`, sqlite3 `list`/`csv`/`line`/`json`. The shells render
//! through these, and the daemon streams through them when a shell sends its
//! output to a file or pipe (no row limit), so both write the same bytes.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A streamable layout and its settings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "layout", rename_all = "snake_case")]
pub enum StreamFormat {
    /// psql `\pset format unaligned`. `expanded`: one `name|value` line per field,
    /// records separated by an empty record.
    PsqlUnaligned { fieldsep: String, recordsep: String, null: String, header: bool, footer: bool, title: Option<String>, expanded: bool },
    /// psql `\pset format csv`.
    PsqlCsv { delimiter: char, null: String, header: bool },
    /// mysql `--batch`: tab-separated, escaped unless `raw`; nothing at all for an
    /// empty result. Binary columns print as `0x…` (MariaDB: only when not UTF-8).
    MysqlBatch { raw: bool, header: bool, mariadb: bool },
    /// sqlite3 `.mode list`.
    SqliteList { separator: String, null: String, header: bool },
    /// sqlite3 `.mode csv`.
    SqliteCsv { header: bool },
    /// sqlite3 `.mode line`.
    SqliteLine,
    /// sqlite3 `.mode json`.
    SqliteJson,
}

/// Terminal columns `s` takes: CJK and most emoji take two, combining marks none.
/// What clients line columns up by (psql's `pg_wcswidth`); `chars().count()` and
/// `format!` padding count characters instead.
pub fn width(s: &str) -> usize {
    unicode_width::UnicodeWidthStr::width(s)
}

/// `s` padded with spaces on the right to `w` columns.
pub fn pad_right(s: &str, w: usize) -> String {
    format!("{s}{}", " ".repeat(w.saturating_sub(width(s))))
}

/// `s` padded with spaces on the left to `w` columns.
pub fn pad_left(s: &str, w: usize) -> String {
    format!("{}{s}", " ".repeat(w.saturating_sub(width(s))))
}

/// `s` centred in `w` columns (an odd space goes to the right).
pub fn center(s: &str, w: usize) -> String {
    let free = w.saturating_sub(width(s));
    format!("{}{s}{}", " ".repeat(free / 2), " ".repeat(free - free / 2))
}

/// SQL types whose values are bytes (MySQL).
pub fn is_binary_type(t: &str) -> bool {
    let t = t.to_ascii_uppercase();
    matches!(t.as_str(), "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BIT" | "GEOMETRY")
}

/// The bytes of a binary value as it arrives: `base64:…` when it isn't UTF-8, else
/// the text itself.
pub fn value_bytes(v: &Value) -> Option<Vec<u8>> {
    use base64::Engine;
    let s = v.as_str()?;
    match s.strip_prefix("base64:") {
        Some(b) => base64::engine::general_purpose::STANDARD.decode(b).ok(),
        None => Some(s.as_bytes().to_vec()),
    }
}

/// `0x…` hex, as the mysql client shows binary values.
pub fn hex_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode_upper(bytes))
}

/// A binary column's value as the mysql client shows it: `0x…` hex. MariaDB's
/// client prints the raw bytes, so text that is valid UTF-8 stays text there.
pub fn mysql_binary(v: &Value, mariadb: bool) -> Option<Value> {
    if mariadb && v.as_str().is_some_and(|s| !s.starts_with("base64:")) {
        return None;
    }
    value_bytes(v).map(|b| Value::String(hex_bytes(&b)))
}

/// Cell text. `null` is how NULL prints (`NULL` in mysql, empty in psql).
pub fn cell(v: &Value, null: &str) -> String {
    match v {
        Value::Null => null.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// psql prints booleans as `t`/`f`.
pub fn psql_cell(v: &Value, null: &str) -> String {
    match v {
        Value::Bool(b) => if *b { "t" } else { "f" }.into(),
        v => cell(v, null),
    }
}

/// mysql `--batch` cell: tabs, newlines, backslashes and NULs escaped unless `raw`.
pub fn batch_cell(v: &Value, raw: bool) -> String {
    let text = cell(v, "NULL");
    if raw {
        return text;
    }
    text.replace('\\', "\\\\").replace('\n', "\\n").replace('\t', "\\t").replace('\0', "\\0")
}

fn csv_record(fields: impl IntoIterator<Item = String>, delimiter: u8) -> Vec<u8> {
    let mut w = csv::WriterBuilder::new().delimiter(delimiter).terminator(csv::Terminator::Any(b'\n')).from_writer(Vec::new());
    w.write_record(fields.into_iter().collect::<Vec<_>>()).ok();
    w.into_inner().unwrap_or_default()
}

/// Writes one result set in a [`StreamFormat`], a record at a time: [`Self::start`],
/// then [`Self::row`] per row, then [`Self::end`]. Each returns the bytes to write.
pub struct RecordFormatter {
    format: StreamFormat,
    columns: Vec<String>,
    binary: Vec<bool>,
    rows: u64,
    /// A header or record was written (separators go between them).
    wrote: bool,
    /// Anything was written (a title counts), so the output needs its final newline.
    any: bool,
}

impl RecordFormatter {
    pub fn new(format: StreamFormat, columns: Vec<String>, types: &[Option<String>]) -> Self {
        let binary = (0..columns.len()).map(|i| types.get(i).and_then(|t| t.as_deref()).is_some_and(is_binary_type)).collect();
        Self { format, columns, binary, rows: 0, wrote: false, any: false }
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Before the first row: what prints even for an empty result.
    pub fn start(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        match &self.format {
            StreamFormat::PsqlUnaligned { fieldsep, recordsep, header, title, expanded, .. } => {
                if let (Some(t), true) = (title, *header) {
                    out.extend_from_slice(t.as_bytes());
                    out.extend_from_slice(recordsep.as_bytes());
                    self.any = true;
                }
                if *header && !*expanded {
                    out.extend_from_slice(self.columns.join(fieldsep).as_bytes());
                    self.wrote = true;
                    self.any = true;
                }
            }
            StreamFormat::PsqlCsv { delimiter, header, .. } => {
                if *header {
                    out = csv_record(self.columns.iter().cloned(), *delimiter as u8);
                }
            }
            // These print nothing for an empty result; the header comes with the first row.
            StreamFormat::MysqlBatch { .. } | StreamFormat::SqliteList { .. } | StreamFormat::SqliteCsv { .. } | StreamFormat::SqliteLine | StreamFormat::SqliteJson => {}
        }
        out
    }

    pub fn row(&mut self, row: &[Value]) -> Vec<u8> {
        let first = self.rows == 0;
        self.rows += 1;
        let mut out = Vec::new();
        match &self.format {
            StreamFormat::PsqlUnaligned { fieldsep, recordsep, null, expanded, .. } => {
                if *expanded {
                    if self.wrote {
                        // Records are separated by an empty record.
                        out.extend_from_slice(recordsep.as_bytes());
                        if !first {
                            out.extend_from_slice(recordsep.as_bytes());
                        }
                    }
                    let fields: Vec<String> = self.columns.iter().zip(row).map(|(c, v)| format!("{c}{fieldsep}{}", psql_cell(v, null))).collect();
                    out.extend_from_slice(fields.join(recordsep).as_bytes());
                } else {
                    if self.wrote {
                        out.extend_from_slice(recordsep.as_bytes());
                    }
                    out.extend_from_slice(row.iter().map(|v| psql_cell(v, null)).collect::<Vec<_>>().join(fieldsep).as_bytes());
                }
            }
            StreamFormat::PsqlCsv { delimiter, null, .. } => out = csv_record(row.iter().map(|v| psql_cell(v, null)), *delimiter as u8),
            StreamFormat::MysqlBatch { raw, header, mariadb } => {
                if first && *header {
                    out.extend_from_slice(self.columns.join("\t").as_bytes());
                    out.push(b'\n');
                }
                let cells: Vec<String> = row
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let v = if self.binary[i] { mysql_binary(v, *mariadb).unwrap_or_else(|| v.clone()) } else { v.clone() };
                        batch_cell(&v, *raw)
                    })
                    .collect();
                out.extend_from_slice(cells.join("\t").as_bytes());
                out.push(b'\n');
            }
            StreamFormat::SqliteList { separator, null, header } => {
                if first && *header {
                    out.extend_from_slice(self.columns.join(separator).as_bytes());
                    out.push(b'\n');
                }
                out.extend_from_slice(row.iter().map(|v| cell(v, null)).collect::<Vec<_>>().join(separator).as_bytes());
                out.push(b'\n');
            }
            StreamFormat::SqliteCsv { header } => {
                if first && *header {
                    out = csv_record(self.columns.iter().cloned(), b',');
                }
                out.extend(csv_record(row.iter().map(|v| cell(v, "")), b','));
            }
            StreamFormat::SqliteLine => {
                if !first {
                    out.push(b'\n');
                }
                let w = self.columns.iter().map(|c| width(c)).max().unwrap_or(0);
                for (c, v) in self.columns.iter().zip(row) {
                    out.extend_from_slice(format!("{} = {}\n", pad_left(c, w), cell(v, "")).as_bytes());
                }
            }
            StreamFormat::SqliteJson => {
                out.extend_from_slice(if first { b"[" as &[u8] } else { b",\n" });
                let m: serde_json::Map<String, Value> = self.columns.iter().cloned().zip(row.iter().cloned()).collect();
                out.extend_from_slice(serde_json::to_string(&m).unwrap_or_default().as_bytes());
            }
        }
        self.wrote = true;
        self.any = true;
        out
    }

    /// After the last row: footers, closing brackets, the final newline.
    pub fn end(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        match &self.format {
            StreamFormat::PsqlUnaligned { recordsep, header, footer, .. } => {
                if *header && *footer {
                    out.extend_from_slice(recordsep.as_bytes());
                    let n = self.rows;
                    out.extend_from_slice(format!("({n} row{})", if n == 1 { "" } else { "s" }).as_bytes());
                    self.any = true;
                }
                if self.any {
                    out.push(b'\n');
                }
            }
            StreamFormat::SqliteJson if self.rows > 0 => out.extend_from_slice(b"]\n"),
            _ => {}
        }
        out
    }
}

/// A whole result set in `format`, as the shells print it.
pub fn render(format: &StreamFormat, columns: &[String], types: &[Option<String>], rows: &[Vec<Value>]) -> String {
    let mut f = RecordFormatter::new(format.clone(), columns.to_vec(), types);
    let mut out = f.start();
    for r in rows {
        out.extend(f.row(r));
    }
    out.extend(f.end());
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data() -> (Vec<String>, Vec<Vec<Value>>) {
        (vec!["id".into(), "note".into()], vec![vec![json!(1), json!("a\tb")], vec![json!(2), Value::Null]])
    }

    #[test]
    fn psql_unaligned() {
        let (c, r) = data();
        let f = StreamFormat::PsqlUnaligned {
            fieldsep: "|".into(),
            recordsep: "\n".into(),
            null: "∅".into(),
            header: true,
            footer: true,
            title: None,
            expanded: false,
        };
        assert_eq!(render(&f, &c, &[], &r), "id|note\n1|a\tb\n2|∅\n(2 rows)\n");
        assert_eq!(render(&f, &c, &[], &[]), "id|note\n(0 rows)\n");
        let StreamFormat::PsqlUnaligned { fieldsep, recordsep, null, title, .. } = f else { unreachable!() };
        let t = StreamFormat::PsqlUnaligned { fieldsep, recordsep, null, header: false, footer: true, title, expanded: true };
        assert_eq!(render(&t, &c, &[], &r), "id|1\nnote|a\tb\n\nid|2\nnote|∅\n");
        assert_eq!(render(&t, &c, &[], &[]), "");
    }

    #[test]
    fn display_width() {
        assert_eq!((width("abc"), width("名前"), width("🦀")), (3, 4, 2));
        assert_eq!(pad_right("名前", 6), "名前  ");
        assert_eq!(pad_left("名前", 6), "  名前");
        assert_eq!(center("ab", 5), " ab  ");
        // sqlite line mode lines the `=` up by columns.
        let cols = vec!["名前".to_string(), "id".to_string()];
        assert_eq!(render(&StreamFormat::SqliteLine, &cols, &[], &[vec![json!("a"), json!(1)]]), "名前 = a\n  id = 1\n");
    }

    #[test]
    fn mysql_batch() {
        let (c, r) = data();
        let f = StreamFormat::MysqlBatch { raw: false, header: true, mariadb: false };
        assert_eq!(render(&f, &c, &[], &r), "id\tnote\n1\ta\\tb\n2\tNULL\n");
        assert_eq!(render(&f, &c, &[], &[]), "");
        let bin = vec![vec![json!("base64:3q2+7w==")]];
        assert_eq!(render(&f, &["d".into()], &[Some("BLOB".into())], &bin), "d\n0xDEADBEEF\n");
    }

    #[test]
    fn sqlite_modes() {
        let (c, r) = data();
        assert_eq!(render(&StreamFormat::SqliteLine, &c, &[], &r), "  id = 1\nnote = a\tb\n\n  id = 2\nnote = \n");
        assert_eq!(render(&StreamFormat::SqliteJson, &c, &[], &r), "[{\"id\":1,\"note\":\"a\\tb\"},\n{\"id\":2,\"note\":null}]\n");
        assert_eq!(render(&StreamFormat::SqliteCsv { header: true }, &c, &[], &r), "id,note\n1,a\tb\n2,\n");
        assert_eq!(render(&StreamFormat::SqliteJson, &c, &[], &[]), "");
    }
}
