pub mod mongo;
pub mod redis;
pub mod load_data;
pub mod sql;

use serde_json::{json, Map, Value};

/// A database error in the server's words, with its structured fields (Postgres
/// SQLSTATE, detail, hint, position, ...; MySQL error number and SQLSTATE). The
/// daemon sends `info` along with the message so shells can print errors the way
/// the native clients do.
#[derive(Debug)]
pub struct DbError {
    pub info: Value,
}

impl DbError {
    pub fn message(&self) -> &str {
        self.info["message"].as_str().unwrap_or("")
    }

    pub fn from_sqlx(d: &(dyn sqlx::error::DatabaseError + 'static)) -> Self {
        let mut info = Map::new();
        if let Some(pg) = d.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
            info.insert("message".into(), json!(pg.message()));
            info.insert("severity".into(), json!(format!("{:?}", pg.severity()).to_uppercase()));
            info.insert("sqlstate".into(), json!(pg.code()));
            let fields = [
                ("detail", pg.detail()),
                ("hint", pg.hint()),
                ("where", pg.r#where()),
                ("schema", pg.schema()),
                ("table", pg.table()),
                ("column", pg.column()),
                ("data_type", pg.data_type()),
                ("constraint", pg.constraint()),
                ("routine", pg.routine()),
                ("file", pg.file()),
            ];
            for (k, v) in fields {
                if let Some(v) = v {
                    info.insert(k.into(), json!(v));
                }
            }
            if let Some(line) = pg.line() {
                info.insert("line".into(), json!(line));
            }
            match pg.position() {
                Some(sqlx::postgres::PgErrorPosition::Original(p)) => {
                    info.insert("position".into(), json!(p));
                }
                Some(sqlx::postgres::PgErrorPosition::Internal { position, query }) => {
                    info.insert("internal_position".into(), json!(position));
                    info.insert("internal_query".into(), json!(query));
                }
                None => {}
            }
        } else if let Some(my) = d.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
            // `1146 (42S02): Table 'x' doesn't exist`, as the mysql client words it.
            info.insert("message".into(), json!(d.to_string()));
            info.insert("code".into(), json!(my.number()));
            if let Some(state) = my.code() {
                info.insert("sqlstate".into(), json!(state));
            }
        } else {
            info.insert("message".into(), json!(d.message()));
            if let Some(code) = d.code() {
                info.insert("code".into(), json!(code));
            }
        }
        DbError { info: Value::Object(info) }
    }
}

/// The message, then psql-style `DETAIL:` / `HINT:` lines when there are any.
impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())?;
        for (key, label) in [("detail", "DETAIL"), ("hint", "HINT")] {
            if let Some(v) = self.info[key].as_str() {
                write!(f, "\n{label}:  {v}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for DbError {}
