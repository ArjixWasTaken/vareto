//! psql's `\pset`: output formats (aligned, wrapped, unaligned, csv, html,
//! asciidoc), borders and line styles, expanded mode, title, footer, NULL display,
//! tuples-only.

use anyhow::{bail, Result};
use comfy_table::{Cell, CellAlignment, ContentArrangement, ContentLineStyle, LineStyle, Table, TableStyle};
use serde_json::Value;

use super::psql_vars::bool_value;
use super::table;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Aligned,
    Wrapped,
    Unaligned,
    Csv,
    Html,
    Asciidoc,
}

const FORMATS: &[(&str, Format)] = &[
    ("aligned", Format::Aligned),
    ("wrapped", Format::Wrapped),
    ("unaligned", Format::Unaligned),
    ("csv", Format::Csv),
    ("html", Format::Html),
    ("asciidoc", Format::Asciidoc),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expanded {
    Off,
    On,
    Auto,
}

#[derive(Clone, Debug)]
pub struct PrintOpts {
    pub format: Format,
    pub border: u8,
    pub expanded: Expanded,
    pub null: String,
    pub footer: bool,
    pub tuples_only: bool,
    pub title: Option<String>,
    pub unicode: bool,
    pub fieldsep: String,
    pub recordsep: String,
    pub csv_fieldsep: char,
    pub tableattr: Option<String>,
}

impl Default for PrintOpts {
    fn default() -> Self {
        Self {
            format: Format::Aligned,
            border: 1,
            expanded: Expanded::Off,
            null: String::new(),
            footer: true,
            tuples_only: false,
            title: None,
            unicode: false,
            fieldsep: "|".into(),
            recordsep: "\n".into(),
            csv_fieldsep: ',',
            tableattr: None,
        }
    }
}

fn on_off(b: bool) -> &'static str {
    if b { "on" } else { "off" }
}

/// A `\pset` boolean argument; no argument toggles.
fn toggle(cur: bool, arg: Option<&str>, name: &str) -> Result<bool> {
    match arg {
        None => Ok(!cur),
        Some(a) => bool_value(a).ok_or_else(|| anyhow::anyhow!("unrecognized value \"{a}\" for \"{name}\": Boolean expected")),
    }
}

impl PrintOpts {
    /// `\pset name [value]`; returns psql's confirmation message.
    pub fn set(&mut self, name: &str, value: Option<&str>) -> Result<String> {
        Ok(match name {
            "format" => {
                if let Some(v) = value {
                    let v = v.to_ascii_lowercase();
                    let (_, f) = FORMATS
                        .iter()
                        .find(|(n, _)| n.starts_with(&v))
                        .ok_or_else(|| anyhow::anyhow!("\\pset: allowed formats are aligned, asciidoc, csv, html, unaligned, wrapped"))?;
                    self.format = *f;
                }
                format!("Output format is {}.", self.format_name())
            }
            "border" => {
                if let Some(v) = value {
                    self.border = v.parse::<u8>().map_err(|_| anyhow::anyhow!("\\pset: border must be a number"))?.min(2);
                }
                format!("Border style is {}.", self.border)
            }
            "expanded" | "x" => {
                self.expanded = match value.map(str::to_ascii_lowercase).as_deref() {
                    Some("auto") => Expanded::Auto,
                    None => if self.expanded == Expanded::Off { Expanded::On } else { Expanded::Off },
                    Some(v) => if toggle(false, Some(v), "expanded")? { Expanded::On } else { Expanded::Off },
                };
                format!("Expanded display is {}.", self.expanded_name())
            }
            "null" => {
                if let Some(v) = value {
                    self.null = v.to_string();
                }
                format!("Null display is \"{}\".", self.null)
            }
            "footer" => {
                self.footer = toggle(self.footer, value, name)?;
                format!("Default footer is {}.", on_off(self.footer))
            }
            "tuples_only" | "t" => {
                self.tuples_only = toggle(self.tuples_only, value, name)?;
                if self.tuples_only { "Tuples only is on.".into() } else { "Tuples only is off.".into() }
            }
            "title" | "C" => {
                self.title = value.filter(|v| !v.is_empty()).map(str::to_string);
                match &self.title {
                    Some(t) => format!("Title is \"{t}\"."),
                    None => "Title is unset.".into(),
                }
            }
            "linestyle" => {
                if let Some(v) = value {
                    self.unicode = match v.to_ascii_lowercase().as_str() {
                        "unicode" | "u" => true,
                        "ascii" | "a" | "old-ascii" => false,
                        _ => bail!("\\pset: allowed line styles are ascii, old-ascii, unicode"),
                    };
                }
                format!("Line style is {}.", if self.unicode { "unicode" } else { "ascii" })
            }
            "fieldsep" => {
                if let Some(v) = value {
                    self.fieldsep = v.to_string();
                }
                format!("Field separator is \"{}\".", self.fieldsep)
            }
            "fieldsep_zero" => {
                self.fieldsep = "\0".into();
                "Field separator is zero byte.".into()
            }
            "recordsep" => {
                if let Some(v) = value {
                    self.recordsep = v.to_string();
                }
                if self.recordsep == "\n" { "Record separator is <newline>.".into() } else { format!("Record separator is \"{}\".", self.recordsep) }
            }
            "recordsep_zero" => {
                self.recordsep = "\0".into();
                "Record separator is zero byte.".into()
            }
            "csv_fieldsep" => {
                if let Some(v) = value {
                    let mut chars = v.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) if c != '"' && c != '\n' && c != '\r' => self.csv_fieldsep = c,
                        _ => bail!("\\pset: csv_fieldsep must be a single one-byte character"),
                    }
                }
                format!("Field separator for CSV is \"{}\".", self.csv_fieldsep)
            }
            "tableattr" | "T" => {
                self.tableattr = value.filter(|v| !v.is_empty()).map(str::to_string);
                match &self.tableattr {
                    Some(t) => format!("Table attributes are \"{t}\"."),
                    None => "Table attributes unset.".into(),
                }
            }
            _ => bail!("\\pset: unknown option: {name}"),
        })
    }

    fn format_name(&self) -> &'static str {
        FORMATS.iter().find(|(_, f)| *f == self.format).map(|(n, _)| *n).unwrap_or("aligned")
    }

    fn expanded_name(&self) -> &'static str {
        match self.expanded {
            Expanded::On => "on",
            Expanded::Off => "off",
            Expanded::Auto => "auto",
        }
    }

    /// `\pset` with no arguments.
    pub fn show_all(&self) -> String {
        let q = |s: &str| format!("'{}'", s.replace('\n', "\\n"));
        [
            ("border", self.border.to_string()),
            ("csv_fieldsep", q(&self.csv_fieldsep.to_string())),
            ("expanded", self.expanded_name().into()),
            ("fieldsep", q(&self.fieldsep)),
            ("footer", on_off(self.footer).into()),
            ("format", self.format_name().into()),
            ("linestyle", if self.unicode { "unicode" } else { "ascii" }.into()),
            ("null", q(&self.null)),
            ("pager", match super::out::pager() {
                super::out::Pager::Off => "0",
                super::out::Pager::Auto => "1",
                super::out::Pager::Always => "2",
            }
            .into()),
            ("recordsep", q(&self.recordsep)),
            ("tableattr", self.tableattr.clone().unwrap_or_default()),
            ("title", self.title.clone().unwrap_or_default()),
            ("tuples_only", on_off(self.tuples_only).into()),
        ]
        .iter()
        .map(|(k, v)| format!("{k:<24} {v}"))
        .collect::<Vec<_>>()
        .join("\n")
    }

    fn cell(&self, v: &Value) -> String {
        match v {
            Value::Null => self.null.clone(),
            v => table::psql_cell(v),
        }
    }

    fn footer_line(&self, n: usize) -> Option<String> {
        (self.footer && !self.tuples_only).then(|| format!("({n} row{})", if n == 1 { "" } else { "s" }))
    }

    /// A result set as psql prints it. `vertical` forces expanded (`\gx`); `width`
    /// is the terminal width (for `\x auto` and wrapped).
    pub fn render(&self, columns: &[String], rows: &[Vec<Value>], vertical: bool, width: usize) -> String {
        let expanded = vertical
            || self.expanded == Expanded::On
            || (self.expanded == Expanded::Auto
                && matches!(self.format, Format::Aligned | Format::Wrapped)
                && self.aligned(columns, rows, None).lines().map(|l| l.chars().count()).max().unwrap_or(0) > width);
        match self.format {
            Format::Aligned | Format::Wrapped if expanded => self.expanded_aligned(columns, rows),
            Format::Aligned => self.with_title_footer(self.aligned(columns, rows, None), rows.len(), true),
            Format::Wrapped => self.with_title_footer(self.aligned(columns, rows, Some(width)), rows.len(), true),
            Format::Unaligned => self.unaligned(columns, rows, expanded),
            Format::Csv => self.csv(columns, rows),
            Format::Html => self.html(columns, rows),
            Format::Asciidoc => self.asciidoc(columns, rows),
        }
    }

    fn with_title_footer(&self, body: String, n: usize, centered: bool) -> String {
        let mut out = String::new();
        if let (Some(t), false) = (&self.title, self.tuples_only) {
            if centered {
                let w = body.lines().map(|l| l.chars().count()).max().unwrap_or(0);
                out.push_str(format!("{t:^w$}").trim_end());
            } else {
                out.push_str(t);
            }
            out.push('\n');
        }
        out.push_str(&body);
        if let Some(f) = self.footer_line(n) {
            out.push('\n');
            out.push_str(&f);
        }
        out
    }

    fn style(&self) -> TableStyle {
        match (self.border, self.unicode) {
            (0, false) => TableStyle::new()
                .header_lines(ContentLineStyle::none().junction(' '))
                .header_separator(LineStyle::none().fill('-').junction(' '))
                .content_lines(ContentLineStyle::none().junction(' ')),
            (0, true) => TableStyle::new()
                .header_lines(ContentLineStyle::none().junction(' '))
                .header_separator(LineStyle::none().fill('─').junction(' '))
                .content_lines(ContentLineStyle::none().junction(' ')),
            (1, false) => TableStyle::new()
                .header_lines(ContentLineStyle::none().junction('|'))
                .header_separator(LineStyle::none().fill('-').junction('+'))
                .content_lines(ContentLineStyle::none().junction('|')),
            (1, true) => TableStyle::new()
                .header_lines(ContentLineStyle::none().junction('│'))
                .header_separator(LineStyle::none().fill('─').junction('┼'))
                .content_lines(ContentLineStyle::none().junction('│')),
            (_, false) => TableStyle::new()
                .top_border(LineStyle::new('+', '-', '+', '+'))
                .header_lines(ContentLineStyle::new('|', '|', '|'))
                .header_separator(LineStyle::new('+', '-', '+', '+'))
                .content_lines(ContentLineStyle::new('|', '|', '|'))
                .bottom_border(LineStyle::new('+', '-', '+', '+')),
            (_, true) => TableStyle::new()
                .top_border(LineStyle::new('┌', '─', '┬', '┐'))
                .header_lines(ContentLineStyle::new('│', '│', '│'))
                .header_separator(LineStyle::new('├', '─', '┼', '┤'))
                .content_lines(ContentLineStyle::new('│', '│', '│'))
                .bottom_border(LineStyle::new('└', '─', '┴', '┘')),
        }
    }

    fn aligned(&self, columns: &[String], rows: &[Vec<Value>], wrap: Option<usize>) -> String {
        let mut t = Table::new();
        t.load_style(self.style());
        if !self.tuples_only {
            t.set_header(columns.iter().map(|c| Cell::new(c).set_alignment(CellAlignment::Center)));
        }
        for r in rows {
            t.add_row(r.iter().map(|v| Cell::new(self.cell(v))));
        }
        for i in 0..columns.len() {
            let numeric = rows.iter().all(|r| matches!(r.get(i), Some(Value::Number(_) | Value::Null) | None))
                && rows.iter().any(|r| matches!(r.get(i), Some(Value::Number(_))));
            if let Some(c) = t.column_mut(i) {
                if numeric {
                    c.set_cell_alignment(CellAlignment::Right);
                }
                if self.border == 0 {
                    c.set_padding((0, 0));
                }
            }
        }
        if let Some(w) = wrap {
            t.set_content_arrangement(ContentArrangement::Dynamic).set_width(w as u16);
        }
        t.lines().map(|l| l.trim_end().to_string()).collect::<Vec<_>>().join("\n")
    }

    fn expanded_aligned(&self, columns: &[String], rows: &[Vec<Value>]) -> String {
        if rows.is_empty() {
            return if self.tuples_only { String::new() } else { "(0 rows)".into() };
        }
        let sep = if self.unicode { '│' } else { '|' };
        let dash = if self.unicode { '─' } else { '-' };
        let w = columns.iter().map(|c| c.chars().count()).max().unwrap_or(0);
        let vw = rows.iter().flatten().map(|v| self.cell(v).chars().count()).max().unwrap_or(0);
        let mut out = Vec::new();
        if let (Some(t), false) = (&self.title, self.tuples_only) {
            out.push(t.clone());
        }
        for (n, r) in rows.iter().enumerate() {
            if !self.tuples_only {
                let head = format!("{dash}[ RECORD {} ]", n + 1);
                let total = w + 3 + vw;
                out.push(format!("{head}{}", dash.to_string().repeat(total.saturating_sub(head.chars().count()).max(1))));
            } else if n > 0 {
                out.push(String::new());
            }
            for (c, v) in columns.iter().zip(r) {
                out.push(format!("{c:<w$} {sep} {}", self.cell(v)).trim_end().to_string());
            }
        }
        out.join("\n")
    }

    fn unaligned(&self, columns: &[String], rows: &[Vec<Value>], expanded: bool) -> String {
        let mut out = String::new();
        if let (Some(t), false) = (&self.title, self.tuples_only) {
            out.push_str(t);
            out.push_str(&self.recordsep);
        }
        if expanded {
            let recs: Vec<String> = rows
                .iter()
                .map(|r| columns.iter().zip(r).map(|(c, v)| format!("{c}{}{}", self.fieldsep, self.cell(v))).collect::<Vec<_>>().join(&self.recordsep))
                .collect();
            out.push_str(&recs.join(&format!("{0}{0}", self.recordsep)));
        } else {
            let mut lines = Vec::new();
            if !self.tuples_only {
                lines.push(columns.join(&self.fieldsep));
            }
            lines.extend(rows.iter().map(|r| r.iter().map(|v| self.cell(v)).collect::<Vec<_>>().join(&self.fieldsep)));
            out.push_str(&lines.join(&self.recordsep));
        }
        if let Some(f) = self.footer_line(rows.len()) {
            out.push_str(&self.recordsep);
            out.push_str(&f);
        }
        out
    }

    fn csv(&self, columns: &[String], rows: &[Vec<Value>]) -> String {
        let mut w = csv::WriterBuilder::new().delimiter(self.csv_fieldsep as u8).terminator(csv::Terminator::Any(b'\n')).from_writer(Vec::new());
        if !self.tuples_only {
            w.write_record(columns).ok();
        }
        for r in rows {
            w.write_record(r.iter().map(|v| self.cell(v))).ok();
        }
        String::from_utf8_lossy(&w.into_inner().unwrap_or_default()).trim_end_matches('\n').to_string()
    }

    fn html(&self, columns: &[String], rows: &[Vec<Value>]) -> String {
        let esc = |s: &str| s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\n', "<br />\n");
        let mut out = format!("<table border=\"{}\"{}>\n", self.border, self.tableattr.as_ref().map(|a| format!(" {a}")).unwrap_or_default());
        if let (Some(t), false) = (&self.title, self.tuples_only) {
            out.push_str(&format!("  <caption>{}</caption>\n", esc(t)));
        }
        if !self.tuples_only {
            out.push_str("  <tr>\n");
            for c in columns {
                out.push_str(&format!("    <th align=\"center\">{}</th>\n", esc(c)));
            }
            out.push_str("  </tr>\n");
        }
        for r in rows {
            out.push_str("  <tr valign=\"top\">\n");
            for v in r {
                let align = if v.is_number() { "right" } else { "left" };
                let text = self.cell(v);
                out.push_str(&format!("    <td align=\"{align}\">{}</td>\n", if text.is_empty() { "&nbsp; ".into() } else { esc(&text) }));
            }
            out.push_str("  </tr>\n");
        }
        out.push_str("</table>");
        if let Some(f) = self.footer_line(rows.len()) {
            out.push_str(&format!("\n<p>{f}<br />\n</p>"));
        }
        out
    }

    fn asciidoc(&self, columns: &[String], rows: &[Vec<Value>]) -> String {
        let esc = |s: &str| s.replace('|', "\\|");
        let cols: Vec<&str> = (0..columns.len())
            .map(|i| if rows.iter().any(|r| r.get(i).is_some_and(Value::is_number)) { ">l" } else { "<l" })
            .collect();
        let mut out = String::new();
        if let (Some(t), false) = (&self.title, self.tuples_only) {
            out.push_str(&format!(".{t}\n"));
        }
        out.push_str(&format!(
            "[{}cols=\"{}\",frame=\"{}\",grid=\"{}\"]\n|====\n",
            if self.tuples_only { "" } else { "options=\"header\"," },
            cols.join(","),
            if self.border >= 2 { "all" } else { "none" },
            if self.border >= 1 { "all" } else { "none" },
        ));
        if !self.tuples_only {
            out.push_str(&columns.iter().map(|c| format!("^l|{}", esc(c))).collect::<Vec<_>>().join(" "));
            out.push('\n');
        }
        for r in rows {
            out.push_str(&r.iter().map(|v| format!("|{}", esc(&self.cell(v)))).collect::<Vec<_>>().join(" "));
            out.push('\n');
        }
        out.push_str("|====");
        if let Some(f) = self.footer_line(rows.len()) {
            out.push_str(&format!("\n\n....\n{f}\n...."));
        }
        out
    }
}

/// `\crosstabview [colV [colH [colD [sortcolH]]]]`: pivot a result so colV's values
/// become rows, colH's values become columns, and colD fills the cells. Columns are
/// given by name or 1-based number; with three columns the defaults are 1, 2, 3.
pub fn crosstab(columns: &[String], rows: &[Vec<Value>], args: &[String]) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let pick = |arg: Option<&String>, default: Option<usize>| -> Result<usize> {
        match arg {
            Some(a) => match a.parse::<usize>() {
                Ok(n) if n >= 1 && n <= columns.len() => Ok(n - 1),
                _ => columns.iter().position(|c| c == a).ok_or_else(|| anyhow::anyhow!("\\crosstabview: column name not found: \"{a}\"")),
            },
            None => default.ok_or_else(|| anyhow::anyhow!("\\crosstabview: data column must be specified when query returns more than three columns")),
        }
    };
    if columns.len() < 3 {
        bail!("\\crosstabview: query must return at least three columns");
    }
    let v = pick(args.first(), Some(0))?;
    let h = pick(args.get(1), Some(1))?;
    let others: Vec<usize> = (0..columns.len()).filter(|&i| i != v && i != h).collect();
    let d = pick(args.get(2), (others.len() == 1).then(|| others[0]))?;
    if v == h {
        bail!("\\crosstabview: vertical and horizontal headers must be different columns");
    }
    let sort = args.get(3).map(|a| pick(Some(a), None)).transpose()?;

    let key = |val: &Value| table::cell(val, "");
    let mut vkeys: Vec<Value> = Vec::new();
    let mut hkeys: Vec<(Value, Option<Value>)> = Vec::new();
    for r in rows {
        if !vkeys.iter().any(|k| k == &r[v]) {
            vkeys.push(r[v].clone());
        }
        if !hkeys.iter().any(|(k, _)| k == &r[h]) {
            hkeys.push((r[h].clone(), sort.map(|s| r[s].clone())));
        }
    }
    if sort.is_some() {
        hkeys.sort_by(|a, b| {
            let (a, b) = (a.1.as_ref().unwrap(), b.1.as_ref().unwrap());
            match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                _ => key(a).cmp(&key(b)),
            }
        });
    }
    let mut grid = vec![vec![Value::Null; hkeys.len() + 1]; vkeys.len()];
    let mut filled = vec![vec![false; hkeys.len()]; vkeys.len()];
    for (i, k) in vkeys.iter().enumerate() {
        grid[i][0] = k.clone();
    }
    for r in rows {
        let i = vkeys.iter().position(|k| k == &r[v]).unwrap();
        let j = hkeys.iter().position(|(k, _)| k == &r[h]).unwrap();
        if filled[i][j] {
            bail!("\\crosstabview: query result contains multiple data values for row \"{}\", column \"{}\"", key(&r[v]), key(&r[h]));
        }
        filled[i][j] = true;
        grid[i][j + 1] = r[d].clone();
    }
    let mut header = vec![columns[v].clone()];
    header.extend(hkeys.iter().map(|(k, _)| key(k)));
    Ok((header, grid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data() -> (Vec<String>, Vec<Vec<Value>>) {
        (vec!["id".into(), "name".into()], vec![vec![json!(1), json!("ann")], vec![json!(10), Value::Null]])
    }

    #[test]
    fn formats() {
        let (c, r) = data();
        let mut o = PrintOpts::default();
        assert_eq!(o.render(&c, &r, false, 80), " id | name\n----+------\n  1 | ann\n 10 |\n(2 rows)");
        o.set("border", Some("2")).unwrap();
        assert_eq!(o.render(&c, &r, false, 80), "+----+------+\n| id | name |\n+----+------+\n|  1 | ann  |\n| 10 |      |\n+----+------+\n(2 rows)");
        o.set("border", Some("0")).unwrap();
        assert_eq!(o.render(&c, &r, false, 80), "id name\n-- ----\n 1 ann\n10\n(2 rows)");
        let mut o = PrintOpts::default();
        o.set("format", Some("unaligned")).unwrap();
        o.set("null", Some("(null)")).unwrap();
        assert_eq!(o.render(&c, &r, false, 80), "id|name\n1|ann\n10|(null)\n(2 rows)");
        o.set("tuples_only", None).unwrap();
        assert_eq!(o.render(&c, &r, false, 80), "1|ann\n10|(null)");
        let mut o = PrintOpts::default();
        o.set("format", Some("csv")).unwrap();
        assert_eq!(o.render(&c, &r, false, 80), "id,name\n1,ann\n10,");
        o.set("format", Some("html")).unwrap();
        assert!(o.render(&c, &r, false, 80).starts_with("<table border=\"1\">\n  <tr>\n    <th align=\"center\">id</th>"));
        let mut o = PrintOpts::default();
        o.set("title", Some("People")).unwrap();
        o.set("linestyle", Some("unicode")).unwrap();
        assert_eq!(o.render(&c, &r[..1], false, 80), "  People\n id │ name\n────┼──────\n  1 │ ann\n(1 row)");
        assert_eq!(PrintOpts::default().render(&c, &r[..1], true, 80), "-[ RECORD 1 ]-\nid   | 1\nname | ann");
    }

    #[test]
    fn pset_messages() {
        let mut o = PrintOpts::default();
        assert_eq!(o.set("format", Some("c")).unwrap(), "Output format is csv.");
        assert_eq!(o.set("footer", Some("off")).unwrap(), "Default footer is off.");
        assert!(o.set("format", Some("latexish")).is_err());
        assert!(o.set("nope", None).is_err());
    }

    #[test]
    fn crosstabs() {
        let cols: Vec<String> = ["region", "year", "sales"].iter().map(|s| s.to_string()).collect();
        let rows = vec![
            vec![json!("north"), json!(2023), json!(5)],
            vec![json!("south"), json!(2024), json!(7)],
            vec![json!("north"), json!(2024), json!(6)],
        ];
        let (h, g) = crosstab(&cols, &rows, &[]).unwrap();
        assert_eq!(h, vec!["region", "2023", "2024"]);
        assert_eq!(g, vec![vec![json!("north"), json!(5), json!(6)], vec![json!("south"), Value::Null, json!(7)]]);
        let dup = vec![rows[0].clone(), rows[0].clone()];
        assert!(crosstab(&cols, &dup, &[]).is_err());
    }
}
