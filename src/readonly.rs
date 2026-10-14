//! Static read-only guards. These run before anything reaches the database; each
//! backend additionally enforces read-only at the database level where it can.

use anyhow::{bail, Result};
use serde_json::Value;

use crate::ipc::{MongoOp, MongoQuery};

// ---------------------------------------------------------------- MongoDB

/// Informational commands a read-only connection may run.
const MONGO_READ_COMMANDS: &[&str] = &[
    "listIndexes", "listCollections", "listDatabases", "collStats", "dbStats", "dataSize", "buildInfo",
    "buildinfo", "hello", "isMaster", "ismaster", "ping", "serverStatus", "usersInfo", "rolesInfo",
    "connectionStatus", "hostInfo", "count", "distinct", "listCommands", "currentOp", "getParameter",
    "getLog", "top", "connPoolStats", "replSetGetStatus", "replSetGetConfig", "listShards", "balancerStatus",
    "balancerCollectionStatus", "getDefaultRWConcern", "getClusterParameter",
];

pub fn check_mongo(q: &MongoQuery) -> Result<()> {
    if q.op == MongoOp::RunCommand {
        return check_mongo_command(q.command.as_ref().unwrap_or(&Value::Null));
    }
    if !q.op.is_read() {
        bail!("read-only connection: {:?} is a write operation", q.op);
    }
    if q.op == MongoOp::Aggregate {
        if let Some(stage) = q.pipeline.iter().flatten().find_map(find_write_stage) {
            bail!("read-only connection: aggregation stage {stage} writes data");
        }
    }
    Ok(())
}

fn check_mongo_command(cmd: &Value) -> Result<()> {
    let Some((name, arg)) = cmd.as_object().and_then(|m| m.iter().next()) else { bail!("command must be a non-empty document") };
    match name.as_str() {
        n if MONGO_READ_COMMANDS.contains(&n) => Ok(()),
        // `{profile: -1}` reads the profiling level; other values set it.
        "profile" if arg.as_f64() == Some(-1.0) => Ok(()),
        "aggregate" => match cmd.get("pipeline").and_then(Value::as_array).into_iter().flatten().find_map(find_write_stage) {
            Some(stage) => bail!("read-only connection: aggregation stage {stage} writes data"),
            None => Ok(()),
        },
        // explain runs (the plan of) its inner command, which must itself be a read.
        "explain" => match arg.as_object().and_then(|m| m.keys().next()).map(String::as_str) {
            Some("find" | "count" | "distinct") => Ok(()),
            Some("aggregate") => match arg.get("pipeline").and_then(Value::as_array).into_iter().flatten().find_map(find_write_stage) {
                Some(stage) => bail!("read-only connection: aggregation stage {stage} writes data"),
                None => Ok(()),
            },
            _ => bail!("read-only connection: only find/aggregate/count/distinct can be explained"),
        },
        n => bail!("read-only connection: command {n} is not on the read-only allowlist"),
    }
}

/// Find `$out`/`$merge` anywhere (including `$facet`, `$lookup.pipeline`, `$unionWith`).
fn find_write_stage(v: &Value) -> Option<&'static str> {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                match k.as_str() {
                    "$out" => return Some("$out"),
                    "$merge" => return Some("$merge"),
                    _ => {}
                }
                if let Some(s) = find_write_stage(child) {
                    return Some(s);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(find_write_stage),
        _ => None,
    }
}

// ---------------------------------------------------------------- Redis

/// Commands that never make sense through this server (they block, hijack the
/// connection or change connection state), regardless of read-only mode.
const REDIS_ALWAYS_DENIED: &[&str] = &[
    "MONITOR", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE",
    "SYNC", "PSYNC", "SELECT", "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "QUIT", "RESET", "AUTH",
    "HELLO", "SHUTDOWN", "BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BLMPOP", "BZPOPMIN", "BZPOPMAX",
    "BZMPOP", "XREAD", "XREADGROUP", "WAIT", "WAITAOF", "CLIENT",
];

/// Single-word read-only commands.
const REDIS_READ: &[&str] = &[
    "GET", "MGET", "GETRANGE", "SUBSTR", "STRLEN", "LCS", "EXISTS", "TYPE", "TTL", "PTTL", "EXPIRETIME",
    "PEXPIRETIME", "KEYS", "SCAN", "RANDOMKEY", "DBSIZE", "DUMP", "TOUCH",
    "HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS", "HSTRLEN", "HSCAN", "HRANDFIELD",
    "HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME",
    "LRANGE", "LLEN", "LINDEX", "LPOS",
    "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SSCAN", "SRANDMEMBER", "SINTER", "SINTERCARD",
    "SUNION", "SDIFF",
    "ZRANGE", "ZRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREVRANGEBYLEX",
    "ZSCORE", "ZMSCORE", "ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANK", "ZREVRANK", "ZSCAN", "ZRANDMEMBER",
    "ZINTER", "ZINTERCARD", "ZUNION", "ZDIFF",
    "XRANGE", "XREVRANGE", "XLEN", "XPENDING",
    "GETBIT", "BITCOUNT", "BITPOS", "BITFIELD_RO",
    "PFCOUNT", "GEOPOS", "GEODIST", "GEOHASH", "GEOSEARCH", "GEORADIUS_RO", "GEORADIUSBYMEMBER_RO",
    "SORT_RO", "PING", "ECHO", "INFO", "TIME", "LASTSAVE", "ROLE", "LOLWUT",
    "JSON.GET", "JSON.MGET", "JSON.TYPE", "JSON.STRLEN", "JSON.ARRLEN", "JSON.OBJKEYS", "JSON.OBJLEN",
    "FT.SEARCH", "FT.AGGREGATE", "FT.INFO", "FT._LIST", "TS.GET", "TS.MGET", "TS.RANGE", "TS.MRANGE",
    "TS.REVRANGE", "TS.MREVRANGE", "TS.INFO",
];

/// Read-only subcommands of container commands.
const REDIS_READ_SUB: &[(&str, &[&str])] = &[
    ("OBJECT", &["ENCODING", "FREQ", "IDLETIME", "REFCOUNT", "HELP"]),
    ("MEMORY", &["USAGE", "STATS", "DOCTOR", "HELP"]),
    ("XINFO", &["STREAM", "GROUPS", "CONSUMERS", "HELP"]),
    ("COMMAND", &["COUNT", "INFO", "DOCS", "LIST", "GETKEYS", "HELP"]),
    ("SLOWLOG", &["GET", "LEN", "HELP"]),
    ("LATENCY", &["LATEST", "HISTORY", "DOCTOR", "HELP"]),
    ("CLUSTER", &["INFO", "NODES", "SLOTS", "SHARDS", "KEYSLOT", "COUNTKEYSINSLOT", "MYID"]),
];

pub fn check_redis(args: &[String], readonly: bool) -> Result<()> {
    let Some(name) = args.first() else { bail!("empty Redis command") };
    let name = name.to_ascii_uppercase();
    if REDIS_ALWAYS_DENIED.contains(&name.as_str()) {
        bail!("Redis command {name} is not supported (blocking or connection-stateful)");
    }
    if !readonly {
        return Ok(());
    }
    if REDIS_READ.contains(&name.as_str()) {
        return Ok(());
    }
    if let Some((_, subs)) = REDIS_READ_SUB.iter().find(|(c, _)| *c == name) {
        let sub = args.get(1).map(|s| s.to_ascii_uppercase()).unwrap_or_default();
        if subs.contains(&sub.as_str()) {
            return Ok(());
        }
        bail!("read-only connection: {name} {sub} is not allowed");
    }
    bail!("read-only connection: Redis command {name} is not on the read-only allowlist");
}

/// Commands an interactive session may use on its own dedicated connection.
const REDIS_SESSION_STATE: &[&str] = &["SELECT"];
/// Transactions: only on writable connections, and only in a session.
const REDIS_SESSION_TX: &[&str] = &["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"];

/// [`check_redis`] for an interactive session, which has its own connection: it may
/// switch databases, and use transactions when writable.
pub fn check_redis_session(args: &[String], readonly: bool) -> Result<()> {
    let Some(name) = args.first() else { bail!("empty Redis command") };
    let name = name.to_ascii_uppercase();
    if REDIS_SESSION_STATE.contains(&name.as_str()) {
        return Ok(());
    }
    if REDIS_SESSION_TX.contains(&name.as_str()) {
        if readonly {
            bail!("read-only connection: {name} is not allowed");
        }
        return Ok(());
    }
    check_redis(args, readonly)
}

// ---------------------------------------------------------------- SQL

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Mysql,
    Postgres,
    Sqlite,
}

/// Functions with side effects that a read-only transaction does not stop.
const SQL_DENIED_FUNCTIONS: &[&str] = &[
    // Postgres
    "pg_terminate_backend", "pg_cancel_backend", "pg_reload_conf", "pg_rotate_logfile", "set_config",
    "pg_advisory_lock", "pg_advisory_xact_lock", "pg_advisory_lock_shared", "pg_try_advisory_lock",
    "pg_promote", "pg_switch_wal", "pg_create_restore_point", "pg_logical_emit_message",
    "lo_import", "lo_export", "lo_unlink", "dblink", "dblink_exec", "dblink_connect", "pg_file_write",
    "pg_notify", "pg_create_logical_replication_slot", "pg_create_physical_replication_slot",
    "pg_drop_replication_slot",
    // MySQL
    "get_lock", "release_lock", "release_all_locks", "sleep", "benchmark",
];

/// Parse `sql` and reject anything that is not a single read statement.
pub fn check_sql(dialect: SqlDialect, sql: &str) -> Result<()> {
    if dialect == SqlDialect::Sqlite && read_pragma(sql) {
        return Ok(());
    }
    let stmts = parse(dialect, sql).map_err(|e| anyhow::anyhow!("read-only connection: could not parse SQL ({e}); only plain read statements are allowed"))?;
    let stmt = match stmts.as_slice() {
        [] => bail!("empty SQL"),
        [one] => one,
        _ => bail!("read-only connection: only a single statement is allowed"),
    };
    let mut v = SqlGuard;
    match sqlparser::ast::Visit::visit(stmt, &mut v) {
        std::ops::ControlFlow::Break(e) => Err(e),
        std::ops::ControlFlow::Continue(()) => Ok(()),
    }
}

/// Postgres `COPY … TO STDOUT` on a read-only connection: the copy must go to the
/// client (not a server file or program) and its query must be a read.
pub fn check_copy_out(sql: &str) -> Result<()> {
    use sqlparser::ast::{CopySource, CopyTarget, Statement};
    let stmts = parse(SqlDialect::Postgres, sql).map_err(|e| anyhow::anyhow!("read-only connection: could not parse COPY ({e})"))?;
    let [Statement::Copy { source, to: true, target: CopyTarget::Stdout, .. }] = stmts.as_slice() else {
        bail!("read-only connection: only COPY … TO STDOUT is allowed");
    };
    if let CopySource::Query(q) = source {
        if let std::ops::ControlFlow::Break(e) = sqlparser::ast::Visit::visit(q.as_ref(), &mut SqlGuard) {
            return Err(e);
        }
    }
    Ok(())
}

/// MySQL session variables a read-only shell may set: the ones `--safe-updates`
/// uses. (`SET NAMES` is allowed too.)
const MYSQL_SESSION_VARS: &[&str] = &["sql_safe_updates", "sql_select_limit", "max_join_size"];

/// A statement that only changes harmless session state (`USE db`, `SET NAMES`
/// and the `--safe-updates` variables on MySQL, `SET search_path` on Postgres). Interactive sessions on read-only connections
/// allow it, and run it outside the read-only transaction so it persists.
pub fn session_statement(dialect: SqlDialect, sql: &str) -> bool {
    use sqlparser::ast::{Expr, Set, Statement};
    let Ok(stmts) = parse(dialect, sql) else { return false };
    let [stmt] = stmts.as_slice() else { return false };
    match (dialect, stmt) {
        (SqlDialect::Mysql, Statement::Use(_)) => true,
        (SqlDialect::Mysql, Statement::Set(Set::SetNames { .. } | Set::SetNamesDefault {})) => true,
        (SqlDialect::Mysql, Statement::Set(Set::SingleAssignment { variable, values, hivevar: false, .. })) => {
            let name = variable.to_string().to_ascii_lowercase();
            MYSQL_SESSION_VARS.contains(&name.as_str()) && values.iter().all(|v| matches!(v, Expr::Value(_)))
        }
        (SqlDialect::Mysql, Statement::Set(Set::MultipleAssignments { assignments })) => assignments.iter().all(|a| {
            let name = a.name.to_string().to_ascii_lowercase();
            MYSQL_SESSION_VARS.contains(&name.as_str()) && matches!(a.value, Expr::Value(_))
        }),
        (SqlDialect::Postgres, Statement::Set(Set::SingleAssignment { variable, values, hivevar: false, .. })) => {
            variable.to_string().eq_ignore_ascii_case("search_path")
                && values.iter().all(|v| matches!(v, Expr::Identifier(_) | Expr::Value(_)))
        }
        _ => false,
    }
}

/// A single statement that returns rows (a query, SHOW, EXPLAIN, a read PRAGMA).
pub fn returns_rows(dialect: SqlDialect, sql: &str) -> bool {
    use sqlparser::ast::Statement as S;
    if dialect == SqlDialect::Sqlite && read_pragma(sql) {
        return true;
    }
    let Ok(stmts) = parse(dialect, sql) else { return false };
    matches!(
        stmts.as_slice(),
        [S::Query(_)
            | S::Explain { .. }
            | S::ExplainTable { .. }
            | S::ShowTables { .. }
            | S::ShowColumns { .. }
            | S::ShowVariable { .. }
            | S::ShowVariables { .. }
            | S::ShowStatus { .. }
            | S::ShowCreate { .. }
            | S::ShowDatabases { .. }
            | S::ShowSchemas { .. }
            | S::ShowViews { .. }
            | S::ShowFunctions { .. }
            | S::ShowCollation { .. }
            | S::ShowCharset(_)
            | S::ShowObjects(_)
            | S::Pragma { .. }]
    )
}

/// sqlparser's dialect for ours.
pub fn sqlparser_dialect(dialect: SqlDialect) -> Box<dyn sqlparser::dialect::Dialect> {
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
    match dialect {
        SqlDialect::Mysql => Box::new(MySqlDialect {}),
        SqlDialect::Postgres => Box::new(PostgreSqlDialect {}),
        SqlDialect::Sqlite => Box::new(SQLiteDialect {}),
    }
}

fn parse(dialect: SqlDialect, sql: &str) -> Result<Vec<sqlparser::ast::Statement>, sqlparser::parser::ParserError> {
    sqlparser::parser::Parser::parse_sql(sqlparser_dialect(dialect).as_ref(), sql)
}

/// The table a query reads from, when it reads from exactly one (used to apply
/// `table.column` mask rules). Best effort; `None` when unknown or ambiguous.
pub fn single_table(dialect: SqlDialect, sql: &str) -> Option<String> {
    let stmts = parse(dialect, sql).ok()?;
    let mut found: Option<String> = None;
    let mut many = false;
    let _ = sqlparser::ast::visit_relations(&stmts, |r| {
        let last = r.0.last().map(|p| p.to_string()).unwrap_or_default();
        let name = last.trim_matches(|c| c == '"' || c == '`').to_string();
        match &found {
            Some(f) if *f != name => many = true,
            _ => found = Some(name),
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    if many { None } else { found }
}

/// SQLite pragmas that only read (in their query form, `PRAGMA name` or
/// `PRAGMA name(arg)`; `PRAGMA name = value` sets and is never allowed).
const SQLITE_READ_PRAGMAS: &[&str] = &[
    "table_info", "table_xinfo", "table_list", "index_list", "index_info", "index_xinfo", "foreign_key_list",
    "database_list", "collation_list", "function_list", "pragma_list", "compile_options", "user_version",
    "schema_version", "page_count", "page_size", "encoding", "journal_mode", "foreign_keys", "integrity_check",
    "quick_check", "foreign_key_check",
];

/// `PRAGMA [schema.]name[(arg)]` for a read-only pragma. Checked by hand, since
/// sqlparser only accepts literals as the argument (not `table_info(users)`).
fn read_pragma(sql: &str) -> bool {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?i)^\s*PRAGMA\s+(?:\w+\.)?(\w+)\s*(?:\(\s*(?:\w+|'[^']*'|"[^"]*")\s*\))?\s*;?\s*$"#).unwrap()
    });
    RE.captures(sql).is_some_and(|c| SQLITE_READ_PRAGMAS.contains(&c[1].to_ascii_lowercase().as_str()))
}

struct SqlGuard;

impl sqlparser::ast::Visitor for SqlGuard {
    type Break = anyhow::Error;

    fn pre_visit_statement(&mut self, s: &sqlparser::ast::Statement) -> std::ops::ControlFlow<anyhow::Error> {
        use sqlparser::ast::Statement as S;
        let ok = match s {
            S::Query(_) | S::ExplainTable { .. } => true,
            S::Pragma { name, is_eq: false, .. } => {
                let n = name.0.last().map(|p| p.to_string().to_ascii_lowercase()).unwrap_or_default();
                SQLITE_READ_PRAGMAS.contains(&n.as_str())
            }
            // EXPLAIN ANALYZE executes its statement; the nested statement is visited too.
            S::Explain { .. } => true,
            S::ShowTables { .. }
            | S::ShowColumns { .. }
            | S::ShowVariable { .. }
            | S::ShowVariables { .. }
            | S::ShowStatus { .. }
            | S::ShowCreate { .. }
            | S::ShowDatabases { .. }
            | S::ShowSchemas { .. }
            | S::ShowViews { .. }
            | S::ShowFunctions { .. }
            | S::ShowCollation { .. }
            | S::ShowCharset(_)
            | S::ShowObjects(_) => true,
            _ => false,
        };
        if ok {
            std::ops::ControlFlow::Continue(())
        } else {
            let text = s.to_string();
            let head: String = text.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
            std::ops::ControlFlow::Break(anyhow::anyhow!("read-only connection: `{head} ...` is not a read statement"))
        }
    }

    fn pre_visit_query(&mut self, q: &sqlparser::ast::Query) -> std::ops::ControlFlow<anyhow::Error> {
        if !q.locks.is_empty() {
            return std::ops::ControlFlow::Break(anyhow::anyhow!("read-only connection: locking clauses (FOR UPDATE/SHARE) are not allowed"));
        }
        if set_expr_has_into(&q.body) {
            return std::ops::ControlFlow::Break(anyhow::anyhow!("read-only connection: SELECT ... INTO is not allowed"));
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &sqlparser::ast::Expr) -> std::ops::ControlFlow<anyhow::Error> {
        if let sqlparser::ast::Expr::Function(f) = e {
            let full = f.name.to_string().to_ascii_lowercase();
            let name = full.rsplit('.').next().unwrap_or(&full).trim_matches('"').trim_matches('`');
            if SQL_DENIED_FUNCTIONS.contains(&name) {
                return std::ops::ControlFlow::Break(anyhow::anyhow!("read-only connection: function {name}() has side effects"));
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn set_expr_has_into(s: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast::SetExpr;
    match s {
        SetExpr::Select(sel) => sel.into.is_some(),
        SetExpr::SetOperation { left, right, .. } => set_expr_has_into(left) || set_expr_has_into(right),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sql_allowed() {
        for (d, sql) in [
            (SqlDialect::Postgres, "SELECT 1"),
            (SqlDialect::Postgres, "select * from users where id = 1;"),
            (SqlDialect::Postgres, "WITH a AS (SELECT 1) SELECT * FROM a"),
            (SqlDialect::Postgres, "SELECT a FROM t UNION SELECT b FROM u"),
            (SqlDialect::Postgres, "EXPLAIN SELECT * FROM t"),
            (SqlDialect::Postgres, "SHOW search_path"),
            (SqlDialect::Postgres, "SELECT count(*) FROM (SELECT * FROM t) x"),
            (SqlDialect::Mysql, "SELECT * FROM `users` LIMIT 10"),
            (SqlDialect::Mysql, "SHOW TABLES"),
            (SqlDialect::Mysql, "SHOW CREATE TABLE users"),
            (SqlDialect::Mysql, "DESCRIBE users"),
            (SqlDialect::Mysql, "SHOW DATABASES"),
        ] {
            check_sql(d, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn copy_out() {
        for ok in ["COPY users TO STDOUT", "COPY users (id, email) TO STDOUT WITH (FORMAT csv, HEADER)", "COPY (SELECT * FROM users) TO STDOUT"] {
            check_copy_out(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in ["COPY users TO '/tmp/x'", "COPY users TO PROGRAM 'id'", "COPY users FROM STDIN", "SELECT 1",
                    "COPY (DELETE FROM users RETURNING *) TO STDOUT", "COPY users TO STDOUT; DROP TABLE users"] {
            assert!(check_copy_out(bad).is_err(), "should reject: {bad}");
        }
    }

    #[test]
    fn sqlite() {
        for ok in ["SELECT * FROM t", "PRAGMA table_info(users)", "PRAGMA main.index_list(users)", "PRAGMA user_version",
                   "SELECT * FROM pragma_table_info('users')"] {
            check_sql(SqlDialect::Sqlite, ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in ["INSERT INTO t VALUES (1)", "PRAGMA user_version = 3", "PRAGMA journal_mode = DELETE",
                    "PRAGMA writable_schema", "ATTACH DATABASE '/tmp/x.db' AS x", "VACUUM", "DELETE FROM t"] {
            assert!(check_sql(SqlDialect::Sqlite, bad).is_err(), "should reject: {bad}");
        }
    }

    #[test]
    fn sql_rejected() {
        for (d, sql) in [
            (SqlDialect::Postgres, "INSERT INTO t VALUES (1)"),
            (SqlDialect::Postgres, "UPDATE t SET a = 1"),
            (SqlDialect::Postgres, "DELETE FROM t"),
            (SqlDialect::Postgres, "DROP TABLE t"),
            (SqlDialect::Postgres, "TRUNCATE t"),
            (SqlDialect::Postgres, "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"),
            (SqlDialect::Postgres, "SELECT 1; DROP TABLE t"),
            (SqlDialect::Postgres, "SELECT * FROM t FOR UPDATE"),
            (SqlDialect::Postgres, "SELECT * INTO t2 FROM t"),
            (SqlDialect::Postgres, "SELECT pg_terminate_backend(123)"),
            (SqlDialect::Postgres, "SELECT set_config('x', 'y', false)"),
            (SqlDialect::Postgres, "SET default_transaction_read_only = off"),
            (SqlDialect::Postgres, "COMMIT"),
            (SqlDialect::Postgres, "EXPLAIN ANALYZE DELETE FROM t"),
            (SqlDialect::Postgres, "COPY t TO '/tmp/x'"),
            (SqlDialect::Mysql, "SELECT * FROM t INTO OUTFILE '/tmp/x'"),
            (SqlDialect::Mysql, "SELECT GET_LOCK('a', 10)"),
            (SqlDialect::Mysql, "SET SESSION TRANSACTION READ WRITE"),
            (SqlDialect::Mysql, "CALL do_things()"),
            (SqlDialect::Mysql, "LOCK TABLES t WRITE"),
            (SqlDialect::Mysql, "REPLACE INTO t VALUES (1)"),
        ] {
            assert!(check_sql(d, sql).is_err(), "should reject: {sql}");
        }
    }

    fn mq(op: MongoOp, pipeline: Option<Vec<Value>>) -> MongoQuery {
        MongoQuery {
            database: None,
            collection: "c".into(),
            op,
            filter: None,
            projection: None,
            sort: None,
            skip: None,
            pipeline,
            field: None,
            documents: None,
            update: None,
            cursor_limit: None,
            upsert: None,
            command: None,
            options: None,
        }
    }

    #[test]
    fn single_table_detection() {
        assert_eq!(single_table(SqlDialect::Postgres, "SELECT * FROM public.users WHERE id = 1").as_deref(), Some("users"));
        assert_eq!(single_table(SqlDialect::Mysql, "SELECT * FROM `users` u JOIN orders o ON o.uid = u.id"), None);
        assert_eq!(single_table(SqlDialect::Postgres, "SELECT 1"), None);
    }

    #[test]
    fn mongo() {
        assert!(check_mongo(&mq(MongoOp::Find, None)).is_ok());
        assert!(check_mongo(&mq(MongoOp::DeleteMany, None)).is_err());
        assert!(check_mongo(&mq(MongoOp::Aggregate, Some(vec![json!({"$match": {}})]))).is_ok());
        assert!(check_mongo(&mq(MongoOp::Aggregate, Some(vec![json!({"$out": "x"})]))).is_err());
        let nested = json!({"$facet": {"a": [{"$merge": {"into": "x"}}]}});
        assert!(check_mongo(&mq(MongoOp::Aggregate, Some(vec![nested]))).is_err());
        assert!(check_mongo(&mq(MongoOp::ReplaceOne, None)).is_err());
        let cmd = |c: Value| MongoQuery { command: Some(c), ..mq(MongoOp::RunCommand, None) };
        assert!(check_mongo(&cmd(json!({"listIndexes": "c"}))).is_ok());
        assert!(check_mongo(&cmd(json!({"explain": {"find": "c", "filter": {}}}))).is_ok());
        assert!(check_mongo(&cmd(json!({"explain": {"aggregate": "c", "pipeline": [{"$out": "x"}]}}))).is_err());
        assert!(check_mongo(&cmd(json!({"explain": {"delete": "c"}}))).is_err());
        assert!(check_mongo(&cmd(json!({"drop": "c"}))).is_err());
        assert!(check_mongo(&cmd(json!({"createIndexes": "c"}))).is_err());
        assert!(check_mongo(&cmd(json!({"profile": -1}))).is_ok());
        assert!(check_mongo(&cmd(json!({"profile": 2}))).is_err());
        assert!(check_mongo(&cmd(json!({"aggregate": 1, "pipeline": [{"$currentOp": {}}], "cursor": {}}))).is_ok());
        assert!(check_mongo(&cmd(json!({"aggregate": "c", "pipeline": [{"$merge": "x"}], "cursor": {}}))).is_err());
        assert!(check_mongo(&cmd(json!({"insert": "c", "documents": []}))).is_err());
    }

    #[test]
    fn redis() {
        let a = |s: &str| s.split(' ').map(String::from).collect::<Vec<_>>();
        assert!(check_redis(&a("get k"), true).is_ok());
        assert!(check_redis(&a("HGETALL k"), true).is_ok());
        assert!(check_redis(&a("object encoding k"), true).is_ok());
        assert!(check_redis(&a("SET k v"), true).is_err());
        assert!(check_redis(&a("CONFIG GET *"), true).is_err());
        assert!(check_redis(&a("EVAL x 0"), true).is_err());
        assert!(check_redis(&a("MEMORY PURGE"), true).is_err());
        assert!(check_redis(&a("SET k v"), false).is_ok());
        assert!(check_redis(&a("SUBSCRIBE ch"), false).is_err());
        assert!(check_redis(&a("SELECT 2"), false).is_err());
    }

    #[test]
    fn redis_session() {
        let a = |s: &str| s.split(' ').map(String::from).collect::<Vec<_>>();
        assert!(check_redis_session(&a("SELECT 2"), true).is_ok());
        assert!(check_redis_session(&a("MULTI"), false).is_ok());
        assert!(check_redis_session(&a("MULTI"), true).is_err());
        assert!(check_redis_session(&a("get k"), true).is_ok());
        assert!(check_redis_session(&a("SET k v"), true).is_err());
        assert!(check_redis_session(&a("SUBSCRIBE ch"), false).is_err());
    }

    #[test]
    fn session_statements() {
        assert!(session_statement(SqlDialect::Mysql, "USE shop"));
        assert!(session_statement(SqlDialect::Postgres, "SET search_path TO app, public"));
        assert!(session_statement(SqlDialect::Postgres, "set search_path = 'app'"));
        assert!(!session_statement(SqlDialect::Postgres, "SET default_transaction_read_only = off"));
        assert!(!session_statement(SqlDialect::Postgres, "SET search_path TO app; DROP TABLE t"));
        assert!(!session_statement(SqlDialect::Mysql, "SET SESSION TRANSACTION READ WRITE"));
        assert!(session_statement(SqlDialect::Mysql, "SET NAMES utf8mb4"));
        assert!(session_statement(SqlDialect::Mysql, "SET sql_safe_updates=1, sql_select_limit=1000, max_join_size=1000000"));
        assert!(!session_statement(SqlDialect::Mysql, "SET sql_safe_updates=1, autocommit=0"));
        assert!(!session_statement(SqlDialect::Mysql, "SELECT 1"));
    }
}
