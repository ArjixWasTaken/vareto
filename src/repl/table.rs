//! Rendering query results the way the native clients print them.

use comfy_table::{Cell, CellAlignment, ContentLineStyle, LineStyle, Table, TableStyle};
use serde_json::Value;

/// A query response from the daemon (the shape of `Output::to_json`).
pub enum QueryResult {
    Table { columns: Vec<String>, rows: Vec<Vec<Value>>, truncated: bool },
    Docs { docs: Vec<Value>, truncated: bool },
    Affected { rows: u64, detail: Option<Value> },
    Value(Value),
}

impl QueryResult {
    pub fn from_json(v: &Value) -> Self {
        let truncated = v["truncated"].as_bool() == Some(true);
        if let Some(cols) = v["columns"].as_array() {
            let columns = cols.iter().map(|c| c["name"].as_str().unwrap_or("").to_string()).collect();
            let rows = v["rows"]
                .as_array()
                .map(|r| r.iter().map(|row| row.as_array().cloned().unwrap_or_default()).collect())
                .unwrap_or_default();
            QueryResult::Table { columns, rows, truncated }
        } else if let Some(docs) = v["documents"].as_array() {
            QueryResult::Docs { docs: docs.clone(), truncated }
        } else if let Some(n) = v["affected_rows"].as_u64() {
            QueryResult::Affected { rows: n, detail: v.get("detail").cloned() }
        } else {
            QueryResult::Value(v["result"].clone())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    /// `+----+` boxes.
    Mysql,
    /// ` a | b` with a `---+---` rule under the header.
    Psql,
    /// sqlite3 `.mode box`.
    Box,
    /// sqlite3 `.mode column`: no borders, dashes under the header.
    Column,
}

const MYSQL: TableStyle = TableStyle::new()
    .top_border(LineStyle::new('+', '-', '+', '+'))
    .header_lines(ContentLineStyle::new('|', '|', '|'))
    .header_separator(LineStyle::new('+', '-', '+', '+'))
    .content_lines(ContentLineStyle::new('|', '|', '|'))
    .bottom_border(LineStyle::new('+', '-', '+', '+'));

const PSQL: TableStyle = TableStyle::new()
    .header_lines(ContentLineStyle::none().junction('|'))
    .header_separator(LineStyle::none().fill('-').junction('+'))
    .content_lines(ContentLineStyle::none().junction('|'));

const BOX: TableStyle = TableStyle::new()
    .top_border(LineStyle::new('┌', '─', '┬', '┐'))
    .header_lines(ContentLineStyle::new('│', '│', '│'))
    .header_separator(LineStyle::new('├', '─', '┼', '┤'))
    .content_lines(ContentLineStyle::new('│', '│', '│'))
    .bottom_border(LineStyle::new('└', '─', '┴', '┘'));

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
pub fn psql_cell(v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "t" } else { "f" }.into(),
        v => cell(v, ""),
    }
}

fn numeric(rows: &[Vec<Value>], i: usize) -> bool {
    let mut any = false;
    for r in rows {
        match r.get(i) {
            Some(Value::Number(_)) => any = true,
            Some(Value::Null) | None => {}
            _ => return false,
        }
    }
    any
}

pub fn table(columns: &[String], rows: &[Vec<Value>], style: Style, headers: bool, fmt: &dyn Fn(&Value) -> String) -> String {
    let mut t = Table::new();
    t.load_style(match style {
        Style::Mysql => MYSQL,
        Style::Psql => PSQL,
        Style::Box => BOX,
        Style::Column => return column(columns, rows, headers, fmt),
    });
    if headers {
        t.set_header(columns.iter().map(|c| {
            let cell = Cell::new(c);
            if style == Style::Psql { cell.set_alignment(CellAlignment::Center) } else { cell }
        }));
    }
    for r in rows {
        t.add_row(r.iter().map(|v| Cell::new(fmt(v))));
    }
    for i in 0..columns.len() {
        if numeric(rows, i) {
            if let Some(c) = t.column_mut(i) {
                c.set_cell_alignment(CellAlignment::Right);
            }
        }
    }
    t.lines().map(|l| l.trim_end().to_string()).collect::<Vec<_>>().join("\n")
}

/// sqlite3 `.mode column`: two spaces between columns, dashes under the header.
fn column(columns: &[String], rows: &[Vec<Value>], headers: bool, fmt: &dyn Fn(&Value) -> String) -> String {
    let cells: Vec<Vec<String>> = rows.iter().map(|r| r.iter().map(fmt).collect()).collect();
    let widths: Vec<usize> = (0..columns.len())
        .map(|i| cells.iter().map(|r| r.get(i).map_or(0, |c| c.chars().count())).chain([columns[i].chars().count()]).max().unwrap_or(0))
        .collect();
    let right: Vec<bool> = (0..columns.len()).map(|i| numeric(rows, i)).collect();
    let line = |r: &[String]| {
        r.iter()
            .zip(&widths)
            .zip(&right)
            .map(|((c, &w), &right)| if right { format!("{c:>w$}") } else { format!("{c:<w$}") })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut out = Vec::new();
    if headers {
        out.push(line(columns));
        out.push(widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
    }
    out.extend(cells.iter().map(|r| line(r)));
    out.join("\n")
}

/// MySQL `\G`.
pub fn vertical_mysql(columns: &[String], rows: &[Vec<Value>]) -> String {
    let w = columns.iter().map(|c| c.chars().count()).max().unwrap_or(0);
    let mut out = String::new();
    for (n, r) in rows.iter().enumerate() {
        out.push_str(&format!("{} {}. row {}\n", "*".repeat(27), n + 1, "*".repeat(27)));
        for (c, v) in columns.iter().zip(r) {
            out.push_str(&format!("{c:>w$}: {}\n", cell(v, "NULL")));
        }
    }
    out.trim_end().to_string()
}

/// psql `\x`.
pub fn expanded_psql(columns: &[String], rows: &[Vec<Value>]) -> String {
    let w = columns.iter().map(|c| c.chars().count()).max().unwrap_or(0);
    let vw = rows.iter().flatten().map(|v| psql_cell(v).chars().count()).max().unwrap_or(0);
    let mut out = String::new();
    for (n, r) in rows.iter().enumerate() {
        let head = format!("-[ RECORD {} ]", n + 1);
        let total = w + 3 + vw;
        out.push_str(&format!("{head}{}\n", "-".repeat(total.saturating_sub(head.len()).max(1))));
        for (c, v) in columns.iter().zip(r) {
            out.push_str(&format!("{c:<w$} | {}\n", psql_cell(v)));
        }
    }
    out.trim_end().to_string()
}

/// sqlite3 `.mode line`.
pub fn line_sqlite(columns: &[String], rows: &[Vec<Value>]) -> String {
    let w = columns.iter().map(|c| c.chars().count()).max().unwrap_or(0);
    rows.iter()
        .map(|r| columns.iter().zip(r).map(|(c, v)| format!("{c:>w$} = {}", cell(v, ""))).collect::<Vec<_>>().join("\n"))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// sqlite3 `.mode csv`.
pub fn csv(columns: &[String], rows: &[Vec<Value>], headers: bool) -> String {
    let mut w = csv::WriterBuilder::new().terminator(csv::Terminator::Any(b'\n')).from_writer(Vec::new());
    if headers {
        w.write_record(columns).ok();
    }
    for r in rows {
        w.write_record(r.iter().map(|v| cell(v, ""))).ok();
    }
    String::from_utf8_lossy(&w.into_inner().unwrap_or_default()).trim_end().to_string()
}

/// sqlite3 `.mode json`.
pub fn json(columns: &[String], rows: &[Vec<Value>]) -> String {
    let objs: Vec<String> = rows
        .iter()
        .map(|r| {
            let m: serde_json::Map<String, Value> = columns.iter().cloned().zip(r.iter().cloned()).collect();
            serde_json::to_string(&m).unwrap_or_default()
        })
        .collect();
    format!("[{}]", objs.join(",\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data() -> (Vec<String>, Vec<Vec<Value>>) {
        (vec!["id".into(), "name".into()], vec![vec![json!(1), json!("ann")], vec![json!(10), Value::Null]])
    }

    #[test]
    fn mysql() {
        let (c, r) = data();
        let s = table(&c, &r, Style::Mysql, true, &|v| cell(v, "NULL"));
        assert_eq!(
            s,
            "+----+------+\n| id | name |\n+----+------+\n|  1 | ann  |\n| 10 | NULL |\n+----+------+"
        );
        assert_eq!(
            vertical_mysql(&c, &r[..1]),
            "*************************** 1. row ***************************\n  id: 1\nname: ann"
        );
    }

    #[test]
    fn psql() {
        let (c, r) = data();
        let s = table(&c, &r, Style::Psql, true, &psql_cell);
        assert_eq!(s, " id | name\n----+------\n  1 | ann\n 10 |");
        assert_eq!(expanded_psql(&c, &r[..1]), "-[ RECORD 1 ]-\nid   | 1\nname | ann");
    }

    #[test]
    fn sqlite_modes() {
        let (c, r) = data();
        assert_eq!(table(&c, &r, Style::Column, true, &|v| cell(v, "")), "id  name\n--  ----\n 1  ann\n10");
        assert_eq!(line_sqlite(&c, &r[..1]), "  id = 1\nname = ann");
        assert_eq!(csv(&c, &r, true), "id,name\n1,ann\n10,");
        assert_eq!(json(&c, &r[..1]), r#"[{"id":1,"name":"ann"}]"#);
    }
}
