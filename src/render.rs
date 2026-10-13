//! Shaping results for the model (limits, cell truncation, masking) and exporting them.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::ipc::ExportFormat;
use crate::mask::Masker;

/// Where a backend sends results as it reads them. `row`/`doc` return `false` to
/// ask the backend to stop reading (the consumer has enough).
pub trait Sink: Send {
    fn columns(&mut self, columns: Vec<Column>) -> Result<()>;
    fn row(&mut self, row: Vec<Value>) -> Result<bool>;
    fn doc(&mut self, doc: Value) -> Result<bool>;
    fn affected(&mut self, rows: u64, detail: Option<Value>) -> Result<()>;
    fn value(&mut self, v: Value) -> Result<()>;
}

/// Collects up to `limit` rows/documents in memory for a query response.
pub struct Collector {
    limit: usize,
    collection: Option<String>,
    columns: Option<Vec<Column>>,
    rows: Vec<Vec<Value>>,
    docs: Option<Vec<Value>>,
    affected: Option<(u64, Option<Value>)>,
    value: Option<Value>,
    truncated: bool,
}

impl Collector {
    pub fn new(limit: usize, collection: Option<String>) -> Self {
        Self { limit, collection, columns: None, rows: Vec::new(), docs: None, affected: None, value: None, truncated: false }
    }

    pub fn finish(self) -> Output {
        if let Some(columns) = self.columns {
            Output::Table { columns, rows: self.rows, truncated: self.truncated }
        } else if let Some(docs) = self.docs {
            Output::Documents { collection: self.collection, docs, truncated: self.truncated }
        } else if let Some(v) = self.value {
            Output::Value(v)
        } else {
            let (rows, detail) = self.affected.unwrap_or((0, None));
            Output::Affected { rows, detail }
        }
    }
}

impl Sink for Collector {
    fn columns(&mut self, columns: Vec<Column>) -> Result<()> {
        self.columns = Some(columns);
        Ok(())
    }
    fn row(&mut self, row: Vec<Value>) -> Result<bool> {
        if self.rows.len() >= self.limit {
            self.truncated = true;
            return Ok(false);
        }
        self.rows.push(row);
        Ok(true)
    }
    fn doc(&mut self, doc: Value) -> Result<bool> {
        let docs = self.docs.get_or_insert_with(Vec::new);
        if docs.len() >= self.limit {
            self.truncated = true;
            return Ok(false);
        }
        docs.push(doc);
        Ok(true)
    }
    fn affected(&mut self, rows: u64, detail: Option<Value>) -> Result<()> {
        let prev = self.affected.take().map(|(n, _)| n).unwrap_or(0);
        self.affected = Some((prev + rows, detail));
        Ok(())
    }
    fn value(&mut self, v: Value) -> Result<()> {
        self.value = Some(v);
        Ok(())
    }
}

/// Run blocking file I/O from inside the async row stream. On a multi-threaded
/// runtime the worker hands its other tasks off first, so a slow disk doesn't stall
/// them; outside a runtime (tests) it simply runs.
fn tokio_blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// The export file. Writes reach it in 256 KiB chunks from the `BufWriter` in front,
/// each one through [`tokio_blocking`].
struct BlockingFile(std::fs::File);

impl std::io::Write for BlockingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        tokio_blocking(|| self.0.write(buf))
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        tokio_blocking(|| self.0.write_all(buf))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        tokio_blocking(|| self.0.flush())
    }
}

/// How many documents decide the CSV columns of a document export.
const CSV_DOC_SAMPLE: usize = 1000;

/// Streams an export to a file, masking each record on the way.
pub struct ExportWriter<'m> {
    format: ExportFormat,
    masker: &'m Masker,
    table: Option<String>,
    collection: Option<String>,
    out: Option<std::io::BufWriter<BlockingFile>>,
    csv: Option<csv::Writer<std::io::BufWriter<BlockingFile>>>,
    columns: Vec<Column>,
    /// Document CSV: first documents, held back until the header is known.
    pending: Vec<Value>,
    doc_keys: Option<Vec<String>>,
    dropped_keys: std::collections::BTreeSet<String>,
    records: usize,
    no_rows: bool,
}

impl<'m> ExportWriter<'m> {
    /// `table`: source table for SQL `table.column` mask rules; `collection`: for Mongo.
    pub fn create(path: &Path, format: ExportFormat, masker: &'m Masker, table: Option<String>, collection: Option<String>) -> Result<Self> {
        let file = tokio_blocking(|| {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::File::create(path)
        })
        .with_context(|| format!("creating {}", path.display()))?;
        let file = BlockingFile(file);
        Ok(Self {
            format,
            masker,
            table,
            collection,
            out: Some(std::io::BufWriter::with_capacity(256 * 1024, file)),
            csv: None,
            columns: Vec::new(),
            pending: Vec::new(),
            doc_keys: None,
            dropped_keys: Default::default(),
            records: 0,
            no_rows: false,
        })
    }

    fn csv(&mut self) -> &mut csv::Writer<std::io::BufWriter<BlockingFile>> {
        if self.csv.is_none() {
            self.csv = Some(csv::Writer::from_writer(self.out.take().expect("writer")));
        }
        self.csv.as_mut().unwrap()
    }

    fn write_doc_csv(&mut self, d: &Value) -> Result<()> {
        let keys = self.doc_keys.clone().unwrap_or_default();
        if let Value::Object(m) = d {
            for k in m.keys() {
                if !keys.contains(k) {
                    self.dropped_keys.insert(k.clone());
                }
            }
        }
        self.csv().write_record(keys.iter().map(|k| d.get(k).map(cell_text).unwrap_or_default()))?;
        Ok(())
    }

    fn flush_pending_docs(&mut self) -> Result<()> {
        if self.doc_keys.is_some() {
            return Ok(());
        }
        // Columns = union of top-level keys of the first documents, in first-seen order.
        let mut keys: Vec<String> = Vec::new();
        for d in &self.pending {
            if let Value::Object(m) = d {
                for k in m.keys() {
                    if !keys.contains(k) {
                        keys.push(k.clone());
                    }
                }
            }
        }
        self.csv().write_record(&keys)?;
        self.doc_keys = Some(keys);
        for d in std::mem::take(&mut self.pending) {
            self.write_doc_csv(&d)?;
        }
        Ok(())
    }

    /// Flush and close the file. Returns a JSON summary.
    pub fn finish(mut self) -> Result<Value> {
        if matches!(self.format, ExportFormat::Csv) && (!self.pending.is_empty() || self.records > 0) && self.columns.is_empty() {
            self.flush_pending_docs()?;
        }
        if self.no_rows && self.records == 0 {
            anyhow::bail!("statement returned no rows to export");
        }
        if let Some(mut c) = self.csv.take() {
            c.flush()?;
        }
        if let Some(mut w) = self.out.take() {
            w.flush()?;
        }
        let mut v = json!({ "records": self.records });
        if !self.dropped_keys.is_empty() {
            v["dropped_fields"] = json!(self.dropped_keys);
            v["note"] = json!(format!(
                "CSV columns come from the first {CSV_DOC_SAMPLE} documents; these fields only appeared later and were left out (use jsonl to keep everything)"
            ));
        }
        Ok(v)
    }
}

impl Sink for ExportWriter<'_> {
    fn columns(&mut self, mut columns: Vec<Column>) -> Result<()> {
        for c in &mut columns {
            c.table = self.table.clone();
        }
        if matches!(self.format, ExportFormat::Csv) {
            let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            self.csv().write_record(&names)?;
        }
        self.columns = columns;
        Ok(())
    }

    fn row(&mut self, mut row: Vec<Value>) -> Result<bool> {
        for (col, cell) in self.columns.iter().zip(row.iter_mut()) {
            self.masker.mask_cell(col.table.as_deref(), &col.name, cell);
        }
        match self.format {
            ExportFormat::Csv => {
                self.csv().write_record(row.iter().map(cell_text))?;
            }
            ExportFormat::Jsonl => {
                let obj: serde_json::Map<String, Value> =
                    self.columns.iter().zip(row).map(|(c, v)| (c.name.clone(), v)).collect();
                let w = self.out.as_mut().expect("writer");
                serde_json::to_writer(&mut *w, &obj)?;
                w.write_all(b"\n")?;
            }
        }
        self.records += 1;
        Ok(true)
    }

    fn doc(&mut self, mut doc: Value) -> Result<bool> {
        self.masker.mask_document(self.collection.as_deref(), &mut doc);
        match self.format {
            ExportFormat::Jsonl => {
                let w = self.out.as_mut().expect("writer");
                serde_json::to_writer(&mut *w, &doc)?;
                w.write_all(b"\n")?;
            }
            ExportFormat::Csv => {
                if self.doc_keys.is_none() {
                    self.pending.push(doc);
                    if self.pending.len() >= CSV_DOC_SAMPLE {
                        self.flush_pending_docs()?;
                    }
                } else {
                    self.write_doc_csv(&doc)?;
                }
            }
        }
        self.records += 1;
        Ok(true)
    }

    fn affected(&mut self, _rows: u64, _detail: Option<Value>) -> Result<()> {
        self.no_rows = true;
        Ok(())
    }

    fn value(&mut self, mut v: Value) -> Result<()> {
        self.masker.mask_document(None, &mut v);
        let w = self.out.as_mut().context("cannot export a single value as CSV")?;
        serde_json::to_writer(&mut *w, &v)?;
        w.write_all(b"\n")?;
        self.records += 1;
        Ok(())
    }
}

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
    fn streaming_export() {
        let dir = std::env::temp_dir().join(format!("vareto-test-{}", std::process::id()));
        let masker = Masker::new(&[]).unwrap();
        let cols = || {
            vec![
                Column { name: "id".into(), type_name: None, table: None },
                Column { name: "name".into(), type_name: None, table: None },
            ]
        };
        let csv_path = dir.join("o.csv");
        let mut w = ExportWriter::create(&csv_path, ExportFormat::Csv, &masker, None, None).unwrap();
        w.columns(cols()).unwrap();
        w.row(vec![json!(1), json!("a,b")]).unwrap();
        w.row(vec![json!(2), Value::Null]).unwrap();
        assert_eq!(w.finish().unwrap()["records"], json!(2));
        assert_eq!(std::fs::read_to_string(&csv_path).unwrap(), "id,name\n1,\"a,b\"\n2,\n");

        let jl = dir.join("o.jsonl");
        let mut w = ExportWriter::create(&jl, ExportFormat::Jsonl, &masker, None, None).unwrap();
        w.columns(cols()).unwrap();
        w.row(vec![json!(1), json!("x")]).unwrap();
        w.finish().unwrap();
        assert_eq!(std::fs::read_to_string(&jl).unwrap(), "{\"id\":1,\"name\":\"x\"}\n");

        // Documents to CSV: header from the first documents, later new keys reported.
        let dc = dir.join("d.csv");
        let mut w = ExportWriter::create(&dc, ExportFormat::Csv, &masker, None, None).unwrap();
        for i in 0..CSV_DOC_SAMPLE {
            w.doc(json!({"a": i, "b": {"x": 1}})).unwrap();
        }
        w.doc(json!({"a": 1, "late": true})).unwrap();
        let summary = w.finish().unwrap();
        assert_eq!(summary["records"], json!(CSV_DOC_SAMPLE + 1));
        assert_eq!(summary["dropped_fields"], json!(["late"]));
        let text = std::fs::read_to_string(&dc).unwrap();
        assert!(text.starts_with("a,b\n0,\"{\"\"x\"\":1}\"\n"), "{}", &text[..40]);

        let empty = dir.join("e.csv");
        let mut w = ExportWriter::create(&empty, ExportFormat::Csv, &masker, None, None).unwrap();
        w.affected(3, None).unwrap();
        assert!(w.finish().is_err());
        std::fs::remove_dir_all(dir).ok();
    }
}
