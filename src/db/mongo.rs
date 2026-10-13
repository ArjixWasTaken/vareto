//! MongoDB via the official driver. Queries are structured (no shell JavaScript).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::ClientOptions;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde_json::{json, Value};

use crate::config::Connection;
use crate::ipc::{MongoOp, MongoQuery};
use crate::render::{Collector, Output, Sink};

pub struct Mongo {
    client: mongodb::Client,
    default_db: Option<String>,
}

fn enc(s: &str) -> String {
    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
}

pub fn build_uri(conn: &Connection, host: &str, port: u16, password: Option<&str>, tunneled: bool) -> String {
    let cfg = &conn.cfg;
    let auth = match (&cfg.user, password) {
        (Some(u), Some(p)) => format!("{}:{}@", enc(u), enc(p)),
        (Some(u), None) => format!("{}@", enc(u)),
        _ => String::new(),
    };
    let mut params: BTreeMap<String, String> = cfg.params.clone();
    // Through a tunnel we can only reach the one forwarded member; replica-set
    // discovery would hand back hostnames that don't resolve locally.
    if tunneled {
        params.entry("directConnection".into()).or_insert_with(|| "true".into());
    }
    params.entry("appName".into()).or_insert_with(|| "vareto".into());
    let query: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", enc(k), enc(v))).collect();
    let db = cfg.database.as_deref().map(enc).unwrap_or_default();
    format!("mongodb://{auth}{}/{db}?{}", crate::config::join_host_port(host, port), query.join("&"))
}

fn to_doc(v: &Value, what: &str) -> Result<Document> {
    match Bson::try_from(v.clone()).with_context(|| format!("invalid {what}"))? {
        Bson::Document(d) => Ok(d),
        _ => bail!("{what} must be a JSON object"),
    }
}

fn opt_doc(v: &Option<Value>, what: &str) -> Result<Document> {
    v.as_ref().map(|v| to_doc(v, what)).transpose().map(Option::unwrap_or_default)
}

fn to_json(d: Document) -> Value {
    Bson::Document(d).into_relaxed_extjson()
}

impl Mongo {
    pub async fn connect(uri: &str, default_db: Option<String>) -> Result<Self> {
        let mut opts = ClientOptions::parse(uri).await.context("parsing MongoDB options")?;
        opts.server_selection_timeout = Some(Duration::from_secs(10));
        opts.connect_timeout = Some(Duration::from_secs(10));
        let client = mongodb::Client::with_options(opts)?;
        // Fail fast on bad credentials / unreachable server.
        client
            .database(default_db.as_deref().unwrap_or("admin"))
            .run_command(doc! { "ping": 1 })
            .await
            .context("connecting to MongoDB")?;
        Ok(Self { client, default_db })
    }

    fn db(&self, name: Option<&str>) -> Result<mongodb::Database> {
        let name = name
            .or(self.default_db.as_deref())
            .context("no database given and the connection has no default `database`")?;
        Ok(self.client.database(name))
    }

    /// Run an operation, collecting up to `limit` documents.
    pub async fn run(&self, q: &MongoQuery, limit: usize, timeout: Duration) -> Result<Output> {
        let mut c = Collector::new(limit, Some(q.collection.clone()));
        self.stream(q, Some(limit), &mut c, timeout).await?;
        Ok(c.finish())
    }

    /// Run an operation, sending documents to `sink` as the cursor yields them.
    /// `limit` (if any) is also pushed to the server for `find`. `timeout` is sent as
    /// `maxTimeMS` and enforced locally.
    pub async fn stream(&self, q: &MongoQuery, limit: Option<usize>, sink: &mut dyn Sink, timeout: Duration) -> Result<()> {
        let fut = self.stream_inner(q, limit, sink, timeout);
        tokio::time::timeout(timeout + Duration::from_secs(5), fut)
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {}s", timeout.as_secs()))?
    }

    async fn stream_inner(&self, q: &MongoQuery, limit: Option<usize>, sink: &mut dyn Sink, timeout: Duration) -> Result<()> {
        let coll = self.db(q.database.as_deref())?.collection::<Document>(&q.collection);
        let filter = opt_doc(&q.filter, "filter")?;
        match q.op {
            MongoOp::Find => {
                let mut find = coll.find(filter).max_time(timeout);
                if let Some(l) = limit {
                    // One extra so the collector can tell the result was truncated.
                    find = find.limit((l + 1) as i64);
                }
                if let Some(p) = &q.projection {
                    find = find.projection(to_doc(p, "projection")?);
                }
                if let Some(s) = &q.sort {
                    find = find.sort(to_doc(s, "sort")?);
                }
                if let Some(n) = q.skip {
                    find = find.skip(n);
                }
                let mut cursor = find.await?;
                while let Some(d) = cursor.try_next().await? {
                    if !sink.doc(to_json(d))? {
                        break;
                    }
                }
            }
            MongoOp::Aggregate => {
                let pipeline = q
                    .pipeline
                    .as_ref()
                    .context("aggregate needs `pipeline`")?
                    .iter()
                    .map(|s| to_doc(s, "pipeline stage"))
                    .collect::<Result<Vec<_>>>()?;
                let mut cursor = coll.aggregate(pipeline).max_time(timeout).await?;
                while let Some(d) = cursor.try_next().await? {
                    if !sink.doc(to_json(d))? {
                        break;
                    }
                }
            }
            MongoOp::Count => sink.value(json!(coll.count_documents(filter).max_time(timeout).await?))?,
            MongoOp::Distinct => {
                let field = q.field.as_deref().context("distinct needs `field`")?;
                for v in coll.distinct(field, filter).max_time(timeout).await? {
                    if !sink.doc(v.into_relaxed_extjson())? {
                        break;
                    }
                }
            }
            MongoOp::InsertOne => {
                let d = q.documents.as_ref().and_then(|d| d.first()).context("insert_one needs `documents`")?;
                let r = coll.insert_one(to_doc(d, "document")?).await?;
                sink.affected(1, Some(json!({ "inserted_id": r.inserted_id.into_relaxed_extjson() })))?
            }
            MongoOp::InsertMany => {
                let docs = q
                    .documents
                    .as_ref()
                    .context("insert_many needs `documents`")?
                    .iter()
                    .map(|d| to_doc(d, "document"))
                    .collect::<Result<Vec<_>>>()?;
                let r = coll.insert_many(docs).await?;
                sink.affected(r.inserted_ids.len() as u64, None)?
            }
            MongoOp::UpdateOne | MongoOp::UpdateMany => {
                let update = q.update.as_ref().context("update needs `update`")?;
                let update: mongodb::options::UpdateModifications = match update {
                    Value::Array(stages) => mongodb::options::UpdateModifications::Pipeline(
                        stages.iter().map(|s| to_doc(s, "update stage")).collect::<Result<_>>()?,
                    ),
                    v => mongodb::options::UpdateModifications::Document(to_doc(v, "update")?),
                };
                let r = if q.op == MongoOp::UpdateOne {
                    coll.update_one(filter, update).await?
                } else {
                    coll.update_many(filter, update).await?
                };
                sink.affected(r.modified_count, Some(json!({ "matched": r.matched_count, "modified": r.modified_count })))?
            }
            MongoOp::DeleteOne => sink.affected(coll.delete_one(filter).await?.deleted_count, None)?,
            MongoOp::DeleteMany => sink.affected(coll.delete_many(filter).await?.deleted_count, None)?,
        }
        Ok(())
    }

    pub async fn server_version(&self) -> Result<String> {
        let info = self.client.database("admin").run_command(doc! { "buildInfo": 1 }).await?;
        Ok(info.get_str("version")?.to_string())
    }

    pub async fn list_databases(&self) -> Result<Output> {
        let names = self.client.list_database_names().await?;
        Ok(Output::Value(json!(names)))
    }

    pub async fn list_collections(&self, db: Option<&str>, pattern: Option<&str>) -> Result<Output> {
        let mut names = self.db(db)?.list_collection_names().await?;
        if let Some(p) = pattern {
            let g = globset::Glob::new(p)?.compile_matcher();
            names.retain(|n| g.is_match(n));
        }
        names.sort();
        Ok(Output::Value(json!(names)))
    }

    /// Indexes, estimated count, and a field/type summary inferred from sampled documents.
    pub async fn describe(&self, db: Option<&str>, collection: &str) -> Result<Value> {
        let coll = self.db(db)?.collection::<Document>(collection);
        let indexes: Vec<Value> = coll
            .list_indexes()
            .await?
            .map_ok(|i| json!({ "keys": to_json(i.keys), "options": i.options.map(|o| serde_json::to_value(o).unwrap_or(Value::Null)) }))
            .try_collect()
            .await?;
        let count = coll.estimated_document_count().await?;
        let sample: Vec<Document> = coll.aggregate([doc! { "$sample": { "size": 50 } }]).await?.with_type::<Document>().try_collect().await?;
        let mut fields: BTreeMap<String, BTreeMap<&'static str, usize>> = BTreeMap::new();
        for d in &sample {
            infer(&mut fields, String::new(), d);
        }
        let fields: serde_json::Map<String, Value> = fields
            .into_iter()
            .map(|(k, types)| (k, json!(types)))
            .collect();
        Ok(json!({
            "collection": collection,
            "estimated_count": count,
            "indexes": indexes,
            "sampled_documents": sample.len(),
            "fields": fields,
        }))
    }
}

fn type_name(b: &Bson) -> &'static str {
    match b {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Decimal128(_) => "decimal",
        Bson::Binary(_) => "binary",
        Bson::Timestamp(_) => "timestamp",
        _ => "other",
    }
}

fn infer(acc: &mut BTreeMap<String, BTreeMap<&'static str, usize>>, prefix: String, d: &Document) {
    for (k, v) in d {
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        *acc.entry(path.clone()).or_default().entry(type_name(v)).or_default() += 1;
        if let Bson::Document(sub) = v {
            if prefix.matches('.').count() < 3 {
                infer(acc, path, sub);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{resolve, Config};

    #[test]
    fn uri() {
        let cfg: Config = toml::from_str(
            r#"
            [connections.m]
            kind = "mongodb"
            user = "app user"
            database = "shop"
            params = { authSource = "admin" }
            "#,
        )
        .unwrap();
        let r = resolve(cfg, Config::default(), false).unwrap();
        let uri = build_uri(&r.connections["m"], "127.0.0.1", 40000, Some("p@ss/w"), true);
        assert_eq!(
            uri,
            "mongodb://app%20user:p%40ss%2Fw@127.0.0.1:40000/shop?appName=vareto&authSource=admin&directConnection=true"
        );
    }
}
