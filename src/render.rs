//! Shaping results for the model (limits, cell truncation, masking) and exporting them.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::ipc::ExportFormat;
use crate::mask::Masker;

#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    /// Source table, when the driver reports it (used for `table.column` mask rules).
    #[serde(skip)]
    pub table: Option<String>,
}

/// Everything a backend can return.
#[derive(Debug, Clone)]
pub enum Output {
    Table { columns: Vec<Column>, rows: Vec<Vec<Value>>, truncated: bool },
    /// Documents (MongoDB) with the collection they came from (for mask rules).
    Documents { collection: Option<String>, docs: Vec<Value>, truncated: bool },
    /// Statement executed without a result set.
    Affected { rows: u64, detail: Option<Value> },
    Value(Value),
}

impl Output {
    pub fn mask(&mut self, m: &Masker) {
        if m.is_noop() {
            return;
        }
        match self {
            Output::Table { columns, rows, .. } => {
                for row in rows {
                    for (col, cell) in columns.iter().zip(row.iter_mut()) {
                        m.mask_cell(col.table.as_deref(), &col.name, cell);
                    }
                }
            }
            Output::Documents { collection, docs, .. } => {
                for d in docs {
                    m.mask_document(collection.as_deref(), d);
                }
            }
            Output::Value(v) => m.mask_document(None, v),
            Output::Affected { .. } => {}
        }
    }

    /// Compact JSON for the model.
    pub fn to_json(&self, max_cell_chars: usize) -> Value {
        match self {
            Output::Table { columns, rows, truncated } => {
                let rows: Vec<Value> = rows
                    .iter()
                    .map(|r| Value::Array(r.iter().map(|c| truncate_value(c, max_cell_chars)).collect()))
                    .collect();
                let mut out = json!({ "columns": columns, "row_count": rows.len(), "rows": rows });
                if *truncated {
                    out["truncated"] = json!(true);
                    out["hint"] = json!("more rows exist; refine the query, raise `limit`, or use export_query");
                }
                out
            }
            Output::Documents { docs, truncated, .. } => {
                let docs: Vec<Value> = docs.iter().map(|d| truncate_value(d, max_cell_chars)).collect();
                let mut out = json!({ "count": docs.len(), "documents": docs });
                if *truncated {
                    out["truncated"] = json!(true);
                    out["hint"] = json!("more documents exist; refine the filter, raise `limit`, or use export_query");
                }
                out
            }
            Output::Affected { rows, detail } => {
                let mut out = json!({ "affected_rows": rows });
                if let Some(d) = detail {
                    out["detail"] = d.clone();
                }
                out
            }
            Output::Value(v) => json!({ "result": truncate_value(v, max_cell_chars) }),
        }
    }

    /// Write the full result to `path`. Returns the number of records written.
    pub fn export(&self, path: &Path, format: ExportFormat) -> Result<usize> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let file = std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
        let mut w = std::io::BufWriter::new(file);
        let n = match (self, format) {
            (Output::Table { columns, rows, .. }, ExportFormat::Csv) => {
                let mut csv = csv::Writer::from_writer(&mut w);
                csv.write_record(columns.iter().map(|c| c.name.as_str()))?;
                for r in rows {
                    csv.write_record(r.iter().map(cell_text))?;
                }
                csv.flush()?;
                rows.len()
            }
            (Output::Table { columns, rows, .. }, ExportFormat::Jsonl) => {
                for r in rows {
                    let obj: serde_json::Map<String, Value> =
                        columns.iter().zip(r).map(|(c, v)| (c.name.clone(), v.clone())).collect();
                    serde_json::to_writer(&mut w, &obj)?;
                    w.write_all(b"\n")?;
                }
                rows.len()
            }
            (Output::Documents { docs, .. }, ExportFormat::Jsonl) => {
                for d in docs {
                    serde_json::to_writer(&mut w, d)?;
                    w.write_all(b"\n")?;
                }
                docs.len()
            }
            (Output::Documents { docs, .. }, ExportFormat::Csv) => {
                // Columns = union of top-level keys in first-seen order; nested values as JSON.
                let mut keys: Vec<String> = Vec::new();
                for d in docs {
                    if let Value::Object(m) = d {
                        for k in m.keys() {
                            if !keys.contains(k) {
                                keys.push(k.clone());
                            }
                        }
                    }
                }
                let mut csv = csv::Writer::from_writer(&mut w);
                csv.write_record(&keys)?;
                for d in docs {
                    csv.write_record(keys.iter().map(|k| d.get(k).map(cell_text).unwrap_or_default()))?;
                }
                csv.flush()?;
                docs.len()
            }
            (Output::Value(v), _) => {
                serde_json::to_writer_pretty(&mut w, v)?;
                1
            }
            (Output::Affected { .. }, _) => anyhow::bail!("statement returned no rows to export"),
        };
        w.flush()?;
        Ok(n)
    }
}

fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn truncate_value(v: &Value, max: usize) -> Value {
    match v {
        Value::String(s) if s.chars().count() > max => {
            let kept: String = s.chars().take(max).collect();
            Value::String(format!("{kept}…[+{} chars]", s.chars().count() - max))
        }
        Value::Array(a) => Value::Array(a.iter().map(|x| truncate_value(x, max)).collect()),
        Value::Object(m) => Value::Object(m.iter().map(|(k, x)| (k.clone(), truncate_value(x, max))).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation() {
        let v = json!({"a": "x".repeat(10), "b": ["y".repeat(3)]});
        let t = truncate_value(&v, 4);
        assert_eq!(t["a"], json!("xxxx…[+6 chars]"));
        assert_eq!(t["b"][0], json!("yyy"));
    }

    #[test]
    fn export_csv_and_jsonl() {
        let dir = std::env::temp_dir().join(format!("vareto-test-{}", std::process::id()));
        let out = Output::Table {
            columns: vec![
                Column { name: "id".into(), type_name: None, table: None },
                Column { name: "name".into(), type_name: None, table: None },
            ],
            rows: vec![vec![json!(1), json!("a,b")], vec![json!(2), Value::Null]],
            truncated: false,
        };
        let csv_path = dir.join("o.csv");
        assert_eq!(out.export(&csv_path, ExportFormat::Csv).unwrap(), 2);
        assert_eq!(std::fs::read_to_string(&csv_path).unwrap(), "id,name\n1,\"a,b\"\n2,\n");
        let jl = dir.join("o.jsonl");
        out.export(&jl, ExportFormat::Jsonl).unwrap();
        assert_eq!(std::fs::read_to_string(&jl).unwrap(), "{\"id\":1,\"name\":\"a,b\"}\n{\"id\":2,\"name\":null}\n");
        std::fs::remove_dir_all(dir).ok();
    }
}
