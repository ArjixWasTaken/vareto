//! Rendering query results the way the native clients print them.

use comfy_table::{Cell, CellAlignment, ContentLineStyle, LineStyle, Table, TableStyle};
use serde_json::Value;

/// A query response from the daemon (the shape of `Output::to_json`).
pub enum QueryResult {
    /// `types`: the database's type name per column, when known.
    Table { columns: Vec<String>, types: Vec<Option<String>>, rows: Vec<Vec<Value>>, truncated: bool },
    Docs { docs: Vec<Value> },
    Affected { rows: u64 },
    Value(Value),
    /// One result per statement, for SQL that returned several.
    Multi(Vec<QueryResult>),
}

impl QueryResult {
    pub fn from_json(v: &Value) -> Self {
        let truncated = v["truncated"].as_bool() == Some(true);
        if let Some(parts) = v["results"].as_array() {
            QueryResult::Multi(parts.iter().map(QueryResult::from_json).collect())
        } else if let Some(cols) = v["columns"].as_array() {
            let columns = cols.iter().map(|c| c["name"].as_str().unwrap_or("").to_string()).collect();
            let types = cols.iter().map(|c| c["type"].as_str().map(str::to_string)).collect();
            let rows = v["rows"]
                .as_array()
                .map(|r| r.iter().map(|row| row.as_array().cloned().unwrap_or_default()).collect())
                .unwrap_or_default();
            QueryResult::Table { columns, types, rows, truncated }
        } else if let Some(docs) = v["documents"].as_array() {
            QueryResult::Docs { docs: docs.clone() }
        } else if let Some(n) = v["affected_rows"].as_u64() {
            QueryResult::Affected { rows: n }
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

impl QueryResult {
    /// The results in order: the parts of a `Multi`, or just this one.
    pub fn into_parts(self) -> Vec<QueryResult> {
        match self {
            QueryResult::Multi(parts) => parts.into_iter().flat_map(QueryResult::into_parts).collect(),
            other => vec![other],
        }
    }
}

pub use crate::textfmt::{cell, is_binary_type};

/// psql prints booleans as `t`/`f`.
pub fn psql_cell(v: &Value) -> String {
    crate::textfmt::psql_cell(v, "")
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
    }

    #[test]
    fn sqlite_modes() {
        let (c, r) = data();
        assert_eq!(table(&c, &r, Style::Column, true, &|v| cell(v, "")), "id  name\n--  ----\n 1  ann\n10");
    }
}
