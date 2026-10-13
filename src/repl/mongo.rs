//! mongosh: real JavaScript (boa), with the mongosh API from `mongo_prelude.js`.
//!
//! The engine runs on its own thread. Its `db.*` calls come back to the shell
//! thread as requests over a channel, which the shell answers with daemon calls
//! (structured `MongoQuery` operations), so the session never crosses threads.

use std::cell::RefCell;
use std::sync::mpsc::{channel, Receiver, Sender};

use anyhow::{bail, Context as _, Result};
use boa_engine::{js_string, Context, JsNativeError, JsResult, JsString, JsValue, NativeFunction, Source};
use serde_json::{json, Value};

use super::session::Session;
use super::{error_text, names, Flavor, Flow};
use crate::ipc::{MongoQuery, QuerySpec};

const PRELUDE: &str = include_str!("mongo_prelude.js");

enum ToJs {
    Eval(String),
    Reply(String),
}

enum FromJs {
    Ready(Result<(), String>),
    Call(String),
    /// Evaluation finished: an error to show, and whether `exit()` was called.
    Done(Option<String>, bool),
}

thread_local! {
    /// The JS thread's end of the channels, for the native functions.
    static BRIDGE: RefCell<Option<(Sender<FromJs>, Receiver<ToJs>)>> = const { RefCell::new(None) };
}

fn bridge_send(m: FromJs) {
    BRIDGE.with(|b| {
        if let Some((tx, _)) = &*b.borrow() {
            tx.send(m).ok();
        }
    });
}

fn bridge_recv() -> Option<ToJs> {
    BRIDGE.with(|b| b.borrow().as_ref().and_then(|(_, rx)| rx.recv().ok()))
}

fn arg_string(args: &[JsValue], ctx: &mut Context) -> JsResult<String> {
    Ok(args.first().cloned().unwrap_or_default().to_string(ctx)?.to_std_string_escaped())
}

/// `__vareto_call(json) -> json`: a request answered by the shell thread.
fn native_call(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
    bridge_send(FromJs::Call(arg_string(args, ctx)?));
    match bridge_recv() {
        Some(ToJs::Reply(r)) => Ok(JsValue::from(JsString::from(r.as_str()))),
        _ => Err(JsNativeError::error().with_message("the shell has closed").into()),
    }
}

fn native_print(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
    println!("{}", arg_string(args, ctx)?);
    Ok(JsValue::undefined())
}

fn eval_str(ctx: &mut Context, code: &str) -> JsResult<JsValue> {
    ctx.eval(Source::from_bytes(code))
}

/// An error as the prelude words it (`MongoServerError: ...`, `TypeError: ...`).
fn format_error(ctx: &mut Context, e: boa_engine::JsError) -> String {
    let fallback = e.to_string();
    let Ok(v) = e.into_opaque(ctx) else { return fallback };
    let formatted = ctx
        .global_object()
        .set(js_string!("__err"), v, false, ctx)
        .and_then(|_| eval_str(ctx, "__shell.formatError(__err)"))
        .and_then(|s| s.to_string(ctx));
    match formatted {
        Ok(s) => s.to_std_string_escaped(),
        Err(_) => fallback,
    }
}

fn js_main(tx: Sender<FromJs>, rx: Receiver<ToJs>) {
    BRIDGE.with(|b| *b.borrow_mut() = Some((tx, rx)));
    let mut ctx = Context::default();
    let setup = ctx
        .register_global_callable(js_string!("__vareto_call"), 1, NativeFunction::from_fn_ptr(native_call))
        .and_then(|()| ctx.register_global_callable(js_string!("__vareto_print"), 1, NativeFunction::from_fn_ptr(native_print)))
        .and_then(|()| eval_str(&mut ctx, PRELUDE).map(|_| ()));
    let ready = setup.map_err(|e| format!("loading the mongosh prelude: {e}"));
    let ok = ready.is_ok();
    bridge_send(FromJs::Ready(ready));
    if !ok {
        return;
    }
    while let Some(msg) = bridge_recv() {
        let ToJs::Eval(code) = msg else { continue };
        let res = eval_str(&mut ctx, &code).and_then(|v| {
            ctx.global_object().set(js_string!("__last"), v, false, &mut ctx)?;
            eval_str(&mut ctx, "__shell.display(__last)")
        });
        let err = res.err().map(|e| format_error(&mut ctx, e));
        let quit = eval_str(&mut ctx, "__shell.quitting").map(|v| v.to_boolean()).unwrap_or(false);
        bridge_send(FromJs::Done(err, quit));
    }
}

/// Whether `code` is a complete input: brackets closed, no open string, template
/// or comment, and not ending in an operator that needs more.
pub fn complete(code: &str) -> bool {
    #[derive(PartialEq)]
    enum St {
        Code,
        Str(char),
        LineComment,
        BlockComment,
    }
    let mut st = St::Code;
    let mut depth = 0i32;
    let mut chars = code.chars().peekable();
    while let Some(c) = chars.next() {
        match st {
            St::Code => match c {
                '\'' | '"' | '`' => st = St::Str(c),
                '/' if chars.peek() == Some(&'/') => st = St::LineComment,
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    st = St::BlockComment;
                }
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                _ => {}
            },
            St::Str(q) => {
                if c == '\\' {
                    chars.next();
                } else if c == q {
                    st = St::Code;
                } else if c == '\n' && q != '`' {
                    // An unterminated ordinary string is a syntax error; let it surface.
                    st = St::Code;
                }
            }
            St::LineComment => {
                if c == '\n' {
                    st = St::Code;
                }
            }
            St::BlockComment => {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    st = St::Code;
                }
            }
        }
    }
    if depth > 0 || matches!(st, St::Str('`') | St::BlockComment) {
        return false;
    }
    // An operator still waiting for its right-hand side (`=`, `==`, `=>` all end in `=`).
    let t = code.trim_end();
    ![".", ",", "=", "&&", "||", "?", ":"].iter().any(|d| t.ends_with(d))
}

pub struct Mongo {
    buf: Vec<String>,
    tx: Sender<ToJs>,
    rx: Receiver<FromJs>,
    collections: Vec<String>,
}

impl Mongo {
    pub fn new() -> Result<Self> {
        let (to_js, js_rx) = channel();
        let (js_tx, from_js) = channel();
        std::thread::Builder::new()
            .name("mongosh-js".into())
            .stack_size(16 * 1024 * 1024)
            .spawn(move || js_main(js_tx, js_rx))
            .context("starting the JavaScript engine")?;
        match from_js.recv() {
            Ok(FromJs::Ready(Ok(()))) => {}
            Ok(FromJs::Ready(Err(e))) => bail!(e),
            _ => bail!("the JavaScript engine failed to start"),
        }
        Ok(Self { buf: Vec::new(), tx: to_js, rx: from_js, collections: Vec::new() })
    }

    fn eval(&mut self, code: String, s: &mut Session) -> Result<Flow> {
        self.tx.send(ToJs::Eval(code)).context("the JavaScript engine has stopped")?;
        loop {
            match self.rx.recv().context("the JavaScript engine has stopped")? {
                FromJs::Call(req) => {
                    let resp = match handle(&req, s) {
                        Ok(v) => json!({ "ok": v }),
                        Err(e) => json!({ "error": error_text(&e) }),
                    };
                    self.tx.send(ToJs::Reply(resp.to_string())).ok();
                }
                FromJs::Done(err, quit) => {
                    if let Some(e) = err {
                        bail!(e);
                    }
                    return Ok(if quit { Flow::Quit } else { Flow::Continue });
                }
                FromJs::Ready(_) => {}
            }
        }
    }
}

/// Answer a request from the prelude.
fn handle(req: &str, s: &mut Session) -> Result<Value> {
    let v: Value = serde_json::from_str(req)?;
    match v["kind"].as_str().unwrap_or("") {
        "query" => {
            let q: MongoQuery = serde_json::from_value(v["query"].clone()).context("invalid query")?;
            let limit = v["limit"].as_u64().map(|n| n as usize).or(s.limit);
            let mut out = s.query_limit(QuerySpec::Mongo(q), limit)?;
            wrap_big_ints(&mut out);
            Ok(out)
        }
        "use" => {
            s.use_db(v["database"].as_str().context("use needs a database name")?)?;
            Ok(Value::Null)
        }
        "collections" => {
            let db = v["database"].as_str().map(str::to_string).or(s.database.clone());
            Ok(json!(names(&s.list_tables(db, None)?)))
        }
        "databases" => Ok(s.list_databases()?["result"].clone()),
        "info" => Ok(json!({
            "database": s.database.clone().unwrap_or_else(|| "test".into()),
            "readonly": s.readonly,
            "version": s.server_version,
            "connection": s.connection,
        })),
        other => bail!("unknown request {other:?}"),
    }
}

/// Integers JavaScript can't hold exactly become `{"$numberLong": "..."}`, which
/// the prelude turns into `Long`.
fn wrap_big_ints(v: &mut Value) {
    const MAX_SAFE: u64 = (1 << 53) - 1;
    match v {
        Value::Number(n) => {
            let big = n.as_i64().map(|i| i.unsigned_abs() > MAX_SAFE).or(n.as_u64().map(|u| u > MAX_SAFE)).unwrap_or(false);
            if big {
                *v = json!({ "$numberLong": n.to_string() });
            }
        }
        Value::Array(items) => items.iter_mut().for_each(wrap_big_ints),
        Value::Object(m) => m.values_mut().for_each(wrap_big_ints),
        _ => {}
    }
}

/// `use x`, `show dbs`, `it`, ... are shell syntax, not JavaScript.
fn shell_command(line: &str) -> Option<String> {
    let t = line.trim().trim_end_matches(';').trim();
    let (cmd, arg) = t.split_once(char::is_whitespace).map(|(c, a)| (c, a.trim())).unwrap_or((t, ""));
    let quoted = || serde_json::to_string(arg).unwrap_or_default();
    match cmd {
        "use" if !arg.is_empty() && !arg.contains(char::is_whitespace) => Some(format!("__shell.use({})", quoted())),
        "show" if !arg.is_empty() && !arg.contains(char::is_whitespace) => Some(format!("__shell.show({})", quoted())),
        "it" if arg.is_empty() => Some("__shell.it()".into()),
        "help" if arg.is_empty() => Some("__shell.help()".into()),
        _ => None,
    }
}

const METHODS: &[&str] = &[
    "find(", "findOne(", "aggregate(", "countDocuments(", "estimatedDocumentCount(", "distinct(", "insertOne(",
    "insertMany(", "updateOne(", "updateMany(", "replaceOne(", "deleteOne(", "deleteMany(", "getIndexes(",
    "createIndex(", "dropIndex(", "drop(", "stats(",
];

impl Flavor for Mongo {
    fn banner(&self, s: &Session) -> String {
        format!(
            "mongosh-compatible vareto shell\nUsing MongoDB: {}{}\nType \"help\" for help.\n",
            s.server_version,
            if s.readonly { " (read-only)" } else { "" }
        )
    }

    fn prompt(&self, s: &Session) -> String {
        if self.buf.is_empty() { format!("{}> ", s.database.as_deref().unwrap_or("test")) } else { "... ".into() }
    }

    fn line(&mut self, line: &str, s: &mut Session) -> Result<Flow> {
        if self.buf.is_empty() {
            if line.trim().is_empty() {
                return Ok(Flow::Continue);
            }
            if matches!(line.trim().trim_end_matches(';'), "exit" | "quit") {
                return Ok(Flow::Quit);
            }
            if let Some(code) = shell_command(line) {
                return self.eval(code, s);
            }
        }
        self.buf.push(line.to_string());
        let code = self.buf.join("\n");
        if !complete(&code) {
            return Ok(Flow::Continue);
        }
        self.buf.clear();
        self.eval(code, s)
    }

    fn finish(&mut self, s: &mut Session) -> Result<()> {
        if !self.buf.is_empty() {
            let code = std::mem::take(&mut self.buf).join("\n");
            self.eval(code, s)?;
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn reset(&mut self) {
        self.buf.clear();
    }

    fn error(&self, e: &anyhow::Error) -> String {
        error_text(e)
    }

    fn refresh(&mut self, s: &mut Session) {
        self.collections = s.list_tables(s.database.clone(), None).map(|v| names(&v)).unwrap_or_default();
    }

    fn words(&self) -> Vec<String> {
        let mut w: Vec<String> = ["show dbs", "show collections", "show users", "use", "it", "exit", "help", "db.", "ObjectId(", "ISODate("]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for c in &self.collections {
            w.push(format!("db.{c}"));
            w.extend(METHODS.iter().map(|m| format!("db.{c}.{m}")));
        }
        w
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        static SEEN: RefCell<Vec<Value>> = const { RefCell::new(Vec::new()) };
        static PRINTED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    /// Records requests; answers queries with one canned document.
    fn mock_call(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
        let req: Value = serde_json::from_str(&arg_string(args, ctx)?).unwrap();
        SEEN.with(|s| s.borrow_mut().push(req.clone()));
        let ok = match req["kind"].as_str() {
            Some("query") => json!({
                "documents": [{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "n": {"$numberLong": "9007199254740993"}}],
                "truncated": true,
            }),
            _ => Value::Null,
        };
        Ok(JsValue::from(JsString::from(json!({ "ok": ok }).to_string().as_str())))
    }

    fn mock_print(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
        let s = arg_string(args, ctx)?;
        PRINTED.with(|p| p.borrow_mut().push(s));
        Ok(JsValue::undefined())
    }

    fn engine() -> Context {
        let mut ctx = Context::default();
        ctx.register_global_callable(js_string!("__vareto_call"), 1, NativeFunction::from_fn_ptr(mock_call)).unwrap();
        ctx.register_global_callable(js_string!("__vareto_print"), 1, NativeFunction::from_fn_ptr(mock_print)).unwrap();
        eval_str(&mut ctx, PRELUDE).unwrap();
        ctx
    }

    fn eval_to_string(ctx: &mut Context, code: &str) -> String {
        let v = eval_str(ctx, code).unwrap_or_else(|e| panic!("{code}: {e}"));
        v.to_string(ctx).unwrap().to_std_string_escaped()
    }

    #[test]
    fn lowers_to_mongo_query() {
        let mut ctx = engine();
        eval_str(
            &mut ctx,
            "db.users.find({_id: ObjectId('507f1f77bcf86cd799439011'), at: {$gt: ISODate('2024-01-01T00:00:00Z')}, \
             re: /^a/i, n: NumberLong('5')}, {name: 1}).sort({a: -1}).skip(5).limit(2).toArray()",
        )
        .unwrap();
        let req = SEEN.with(|s| s.borrow_mut().pop()).unwrap();
        let q: MongoQuery = serde_json::from_value(req["query"].clone()).unwrap();
        assert_eq!(q.op, crate::ipc::MongoOp::Find);
        assert_eq!(q.collection, "users");
        assert_eq!(q.database, None, "follows the session's database");
        assert_eq!(
            q.filter.unwrap(),
            json!({
                "_id": {"$oid": "507f1f77bcf86cd799439011"},
                "at": {"$gt": {"$date": {"$numberLong": "1704067200000"}}},
                "re": {"$regularExpression": {"pattern": "^a", "options": "i"}},
                "n": {"$numberLong": "5"},
            })
        );
        assert_eq!(q.projection.unwrap(), json!({"name": 1}));
        assert_eq!(q.sort.unwrap(), json!({"a": -1}));
        assert_eq!((q.skip, q.cursor_limit), (Some(5), Some(2)));

        eval_str(&mut ctx, "db.getSiblingDB('other').logs.aggregate([{$match: {}}]).toArray()").unwrap();
        let req = SEEN.with(|s| s.borrow_mut().pop()).unwrap();
        assert_eq!(req["query"]["database"], "other");
        assert_eq!(req["query"]["op"], "aggregate");
        assert_eq!(req["query"]["collection"], "logs");

        eval_str(&mut ctx, "db.system.profile.updateOne({a: 1}, {$set: {b: 2}}, {upsert: true})").unwrap();
        let req = SEEN.with(|s| s.borrow_mut().pop()).unwrap();
        assert_eq!(req["query"]["collection"], "system.profile");
        assert_eq!(req["query"]["upsert"], true);
        assert_eq!(req["query"]["update"], json!({"$set": {"b": 2}}));
    }

    #[test]
    fn revives_and_prints() {
        let mut ctx = engine();
        assert_eq!(eval_to_string(&mut ctx, "db.users.findOne()._id instanceof ObjectId"), "true");
        assert_eq!(eval_to_string(&mut ctx, "util.inspect(db.users.findOne())"), "{ _id: ObjectId('507f1f77bcf86cd799439011'), n: Long('9007199254740993') }");
        assert_eq!(
            eval_to_string(&mut ctx, "util.inspect({a: \"it's\", 'b-c': [1, 2], d: new Date(0), e: null})"),
            "{ a: \"it's\", 'b-c': [ 1, 2 ], d: ISODate('1970-01-01T00:00:00.000Z'), e: null }"
        );
        let long = eval_to_string(&mut ctx, "util.inspect({text: 'x'.repeat(80), n: 1})");
        assert_eq!(long, format!("{{\n  text: '{}',\n  n: 1\n}}", "x".repeat(80)));
        assert_eq!(eval_to_string(&mut ctx, "util.inspect(UUID('0e1c4a76-1c8d-4a9b-9d6a-3b1c7e2f9a10'))"), "UUID('0e1c4a76-1c8d-4a9b-9d6a-3b1c7e2f9a10')");

        // A cursor prints its first batch, then "it" continues from there.
        PRINTED.with(|p| p.borrow_mut().clear());
        eval_str(&mut ctx, "__shell.display(db.users.find({}))").unwrap();
        eval_str(&mut ctx, "__shell.it()").unwrap();
        let printed = PRINTED.with(|p| p.borrow().clone());
        assert_eq!(printed[1], "Type \"it\" for more");
        let reqs = SEEN.with(|s| s.borrow().clone());
        let skips: Vec<Value> = reqs.iter().rev().take(2).map(|r| r["query"]["skip"].clone()).collect();
        assert_eq!(skips, vec![json!(1), json!(0)]);
    }

    #[test]
    fn completeness() {
        assert!(complete("db.users.find({a: 1})"));
        assert!(!complete("db.users.find({"));
        assert!(!complete("db.users.find({a: 1}).\n"));
        assert!(complete("db.users.find({a: '}'})"));
        assert!(!complete("const s = `a\nb"));
        assert!(complete("x == 1"));
        assert!(!complete("x ="));
        assert!(!complete("/* comment"));
        assert!(complete("// a { comment"));
    }

    #[test]
    fn big_ints() {
        let mut v = json!({"a": 9007199254740993i64, "b": [1, -9007199254740993i64], "c": 1.5});
        wrap_big_ints(&mut v);
        assert_eq!(v, json!({"a": {"$numberLong": "9007199254740993"}, "b": [1, {"$numberLong": "-9007199254740993"}], "c": 1.5}));
    }

    #[test]
    fn shell_syntax() {
        assert_eq!(shell_command("use shop").as_deref(), Some("__shell.use(\"shop\")"));
        assert_eq!(shell_command("show dbs;").as_deref(), Some("__shell.show(\"dbs\")"));
        assert_eq!(shell_command("it").as_deref(), Some("__shell.it()"));
        assert_eq!(shell_command("it = 3"), None);
        assert_eq!(shell_command("db.users.find()"), None);
    }
}
