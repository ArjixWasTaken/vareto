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

/// Loop iterations one function call may run before the engine stops it (about
/// 5 s in a release build). It stops runaway loops like `while (true) {}` with an
/// error JavaScript can't catch, which unwinds normally and keeps the session's
/// variables. Users change it with `config.set('loopIterationLimit', n)`.
const LOOP_ITERATION_LIMIT: u64 = 100_000_000;

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

/// `__vareto_loop_limit(n?)`: set the loop iteration limit when given; returns it.
fn native_loop_limit(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
    if let Some(v) = args.first().filter(|v| !v.is_undefined()) {
        let n = v.to_number(ctx)?;
        if !(n >= 1.0 && n.fract() == 0.0) {
            return Err(JsNativeError::range().with_message("loopIterationLimit must be a positive integer").into());
        }
        ctx.runtime_limits_mut().set_loop_iteration_limit(n as u64);
    }
    Ok(JsValue::from(ctx.runtime_limits().loop_iteration_limit() as f64))
}

/// A native function's signature.
type Native = fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>;

/// `__vareto_native(op, arg)`: mongosh's local helpers (`cat`, `ls`, `sleep`, ...).
/// Results are strings; lists are JSON.
fn native_helper(_: &JsValue, args: &[JsValue], ctx: &mut Context) -> JsResult<JsValue> {
    let op = arg_string(args, ctx)?;
    let arg = args.get(1).cloned().unwrap_or_default().to_string(ctx)?.to_std_string_escaped();
    let fail = |e: String| -> boa_engine::JsError { JsNativeError::error().with_message(e).into() };
    let out = match op.as_str() {
        "cat" => std::fs::read_to_string(&arg).map_err(|e| fail(format!("{arg}: {e}")))?,
        "ls" | "listFiles" => {
            let mut entries: Vec<Value> = std::fs::read_dir(&arg)
                .map_err(|e| fail(format!("{arg}: {e}")))?
                .filter_map(Result::ok)
                .map(|e| {
                    let meta = e.metadata().ok();
                    let dir = meta.as_ref().is_some_and(|m| m.is_dir());
                    let path = std::path::Path::new(&arg).join(e.file_name());
                    let name = e.file_name().to_string_lossy().to_string();
                    if op == "ls" {
                        json!(format!("{}{}", path.display(), if dir { "/" } else { "" }))
                    } else {
                        let mut v = json!({ "name": path.display().to_string(), "baseName": name, "isDirectory": dir });
                        if !dir {
                            v["size"] = json!(meta.map(|m| m.len()).unwrap_or(0));
                        }
                        v
                    }
                })
                .collect();
            entries.sort_by_key(|v| v.to_string());
            Value::Array(entries).to_string()
        }
        "pwd" => std::env::current_dir().map(|d| d.display().to_string()).unwrap_or_default(),
        "hostname" => nix::unistd::gethostname().map(|h| h.to_string_lossy().to_string()).unwrap_or_default(),
        "sleep" => {
            std::thread::sleep(std::time::Duration::from_millis(arg.parse().unwrap_or(0)));
            String::new()
        }
        "version" => crate::ipc::VERSION.to_string(),
        "passwordPrompt" => rpassword::prompt_password("Enter password: ").map_err(|e| fail(format!("passwordPrompt: {e}")))?,
        other => return Err(fail(format!("unknown native helper {other:?}"))),
    };
    Ok(JsValue::from(JsString::from(out.as_str())))
}

/// Register the natives the prelude needs and load it.
fn setup(ctx: &mut Context, call: Native, print: Native, interactive: bool) -> JsResult<()> {
    ctx.runtime_limits_mut().set_loop_iteration_limit(LOOP_ITERATION_LIMIT);
    ctx.register_global_callable(js_string!("__vareto_call"), 1, NativeFunction::from_fn_ptr(call))?;
    ctx.register_global_callable(js_string!("__vareto_print"), 1, NativeFunction::from_fn_ptr(print))?;
    ctx.register_global_callable(js_string!("__vareto_loop_limit"), 1, NativeFunction::from_fn_ptr(native_loop_limit))?;
    ctx.register_global_callable(js_string!("__vareto_native"), 2, NativeFunction::from_fn_ptr(native_helper))?;
    ctx.global_object().set(js_string!("__vareto_interactive"), interactive, false, ctx)?;
    eval_str(ctx, PRELUDE).map(|_| ())
}

fn eval_str(ctx: &mut Context, code: &str) -> JsResult<JsValue> {
    ctx.eval(Source::from_bytes(code))
}

/// An error as the prelude words it (`MongoServerError: ...`, `TypeError: ...`).
fn format_error(ctx: &mut Context, e: boa_engine::JsError) -> String {
    use boa_engine::error::{EngineError, RuntimeLimitError};
    match e.as_engine() {
        Some(EngineError::RuntimeLimit(RuntimeLimitError::LoopIteration)) => {
            let n = ctx.runtime_limits().loop_iteration_limit();
            return format!("RuntimeLimitError: a loop ran more than {n} iterations and was stopped (raise the limit with config.set('loopIterationLimit', n))");
        }
        // Engine errors carry a backtrace; the first line says it all.
        Some(other) => return other.to_string().lines().next().unwrap_or_default().to_string(),
        None => {}
    }
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

fn js_main(tx: Sender<FromJs>, rx: Receiver<ToJs>, interactive: bool) {
    BRIDGE.with(|b| *b.borrow_mut() = Some((tx, rx)));
    let mut ctx = Context::default();
    let ready = setup(&mut ctx, native_call, native_print, interactive).map_err(|e| format!("loading the mongosh prelude: {e}"));
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
    pub fn new(interactive: bool) -> Result<Self> {
        let (to_js, js_rx) = channel();
        let (js_tx, from_js) = channel();
        std::thread::Builder::new()
            .name("mongosh-js".into())
            .stack_size(16 * 1024 * 1024)
            .spawn(move || js_main(js_tx, js_rx, interactive))
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
        "show" if !arg.is_empty() => {
            // `show log <name>` takes an argument.
            let words: Vec<String> = arg.split_whitespace().map(|w| serde_json::to_string(w).unwrap_or_default()).collect();
            (words.len() <= 2).then(|| format!("__shell.show({})", words.join(", ")))
        }
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
            Some("query") if req["query"]["op"] == "run_command" => json!({
                "result": {"cursor": {"firstBatch": [{"name": "users", "type": "collection"}]}, "value": {"n": 1}, "ok": 1},
            }),
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
        setup(&mut ctx, mock_call, mock_print, false).unwrap();
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
    fn runaway_loops_stop_and_keep_the_session() {
        let mut ctx = engine();
        assert_eq!(eval_to_string(&mut ctx, "config.get('loopIterationLimit')"), "100000000");
        eval_str(&mut ctx, "config.set('loopIterationLimit', 1000); const kept = 41").unwrap();
        // JavaScript can't catch it, so `catch` doesn't swallow the stop.
        let e = eval_str(&mut ctx, "try { while (true) {} } catch (e) { 'caught' }").unwrap_err();
        assert_eq!(
            format_error(&mut ctx, e),
            "RuntimeLimitError: a loop ran more than 1000 iterations and was stopped (raise the limit with config.set('loopIterationLimit', n))"
        );
        assert_eq!(eval_to_string(&mut ctx, "kept + 1"), "42");
        assert_eq!(eval_to_string(&mut ctx, "let n = 0; for (let i = 0; i < 500; i++) n++; n"), "500");
        assert!(eval_str(&mut ctx, "config.set('loopIterationLimit', -1)").is_err());
        assert!(eval_str(&mut ctx, "config.set('nope', 1)").is_err());
    }

    #[test]
    fn db_methods_and_mistakes() {
        let mut ctx = engine();
        assert_eq!(eval_to_string(&mut ctx, "db.listCollections({name: 'users'})[0].name"), "users");
        let req = SEEN.with(|s| s.borrow_mut().pop()).unwrap();
        assert_eq!(req["query"]["command"], json!({"listCollections": 1, "filter": {"name": "users"}}));

        assert_eq!(eval_to_string(&mut ctx, "db.users.findOneAndUpdate({a: 1}, {$inc: {n: 1}}, {returnDocument: 'after'}).n"), "1");
        let req = SEEN.with(|s| s.borrow_mut().pop()).unwrap();
        assert_eq!(
            req["query"]["command"],
            json!({"findAndModify": "users", "query": {"a": 1}, "update": {"$inc": {"n": 1}}, "new": true})
        );

        // Known but unsupported: a clear error rather than a collection.
        assert_eq!(eval_to_string(&mut ctx, "typeof db.fsyncLock"), "function");
        let e = eval_str(&mut ctx, "db.fsyncLock()").unwrap_err();
        assert_eq!(
            format_error(&mut ctx, e),
            "TypeError: db.fsyncLock() is not available in the vareto shell (server and cluster administration is left to mongosh)"
        );
        let e = eval_str(&mut ctx, "rs.stepDown()").unwrap_err();
        assert!(format_error(&mut ctx, e).starts_with("TypeError: rs.stepDown() is not available"));
        let e = eval_str(&mut ctx, "db.users.watch()").unwrap_err();
        assert!(format_error(&mut ctx, e).contains("change streams"));
        // A typo is a collection; calling it explains that.
        let e = eval_str(&mut ctx, "db.users.fnd()").unwrap_err();
        assert!(format_error(&mut ctx, e).contains("db.foo is the collection \"foo\""));
        assert_eq!(eval_to_string(&mut ctx, "db.users.fnd.getName()"), "users.fnd");
    }

    fn last_request() -> Value {
        SEEN.with(|s| s.borrow_mut().pop()).unwrap()
    }

    #[test]
    fn cursor_options_and_explain() {
        let mut ctx = engine();
        eval_str(&mut ctx, "db.users.find({a: 1}).hint({a: 1}).collation({locale: 'en'}).comment('c').maxTimeMS(50).returnKey().toArray()").unwrap();
        let q = last_request()["query"].clone();
        assert_eq!(q["options"], json!({"hint": {"a": 1}, "collation": {"locale": "en"}, "comment": "c", "maxTimeMS": 50, "returnKey": true}));
        // find(filter, projection, options) splits sort/limit/skip out of options.
        eval_str(&mut ctx, "db.users.find({}, {a: 1}, {sort: {a: -1}, limit: 3, allowDiskUse: true}).toArray()").unwrap();
        let q = last_request()["query"].clone();
        assert_eq!((q["sort"].clone(), q["cursor_limit"].clone(), q["options"].clone()), (json!({"a": -1}), json!(3), json!({"allowDiskUse": true})));
        eval_str(&mut ctx, "db.users.aggregate([{$match: {}}], {allowDiskUse: true}).toArray()").unwrap();
        assert_eq!(last_request()["query"]["options"], json!({"allowDiskUse": true}));

        eval_str(&mut ctx, "db.users.explain('executionStats').aggregate([{$match: {a: 1}}])").unwrap();
        assert_eq!(
            last_request()["query"]["command"],
            json!({"explain": {"aggregate": "users", "pipeline": [{"$match": {"a": 1}}], "cursor": {}}, "verbosity": "executionStats"})
        );
        eval_str(&mut ctx, "db.users.find({a: 1}).sort({b: 1}).hint('a_1').explain()").unwrap();
        assert_eq!(last_request()["query"]["command"]["explain"], json!({"find": "users", "filter": {"a": 1}, "sort": {"b": 1}, "hint": "a_1"}));
    }

    #[test]
    fn bulk_writes_use_write_commands() {
        let mut ctx = engine();
        SEEN.with(|s| s.borrow_mut().clear());
        eval_str(
            &mut ctx,
            "db.users.bulkWrite([{insertOne: {document: {_id: 1}}}, {insertOne: {document: {_id: 2}}}, \
             {updateOne: {filter: {_id: 1}, update: {$set: {a: 1}}, upsert: true}}, {deleteMany: {filter: {a: 2}}}])",
        )
        .unwrap();
        let cmds: Vec<Value> = SEEN.with(|s| s.borrow().iter().map(|r| r["query"]["command"].clone()).collect());
        assert_eq!(
            cmds,
            vec![
                json!({"insert": "users", "documents": [{"_id": 1}, {"_id": 2}], "ordered": true}),
                json!({"update": "users", "updates": [{"q": {"_id": 1}, "u": {"$set": {"a": 1}}, "multi": false, "upsert": true}], "ordered": true}),
                json!({"delete": "users", "deletes": [{"q": {"a": 2}, "limit": 0}], "ordered": true}),
            ]
        );
        // The Bulk API builds the same operations.
        eval_str(&mut ctx, "const b = db.users.initializeUnorderedBulkOp(); b.find({x: 1}).upsert().updateOne({$set: {y: 1}}); b.find({z: 1}).removeOne(); b.execute()").unwrap();
        let last = last_request()["query"]["command"].clone();
        assert_eq!(last, json!({"delete": "users", "deletes": [{"q": {"z": 1}, "limit": 1}], "ordered": false}));
        // Options beyond upsert route a single update through the command too.
        eval_str(&mut ctx, "db.users.updateOne({_id: 1}, {$set: {'a.$[e]': 1}}, {arrayFilters: [{e: 0}]})").unwrap();
        assert_eq!(last_request()["query"]["command"]["updates"][0]["arrayFilters"], json!([{"e": 0}]));
    }

    #[test]
    fn database_helpers() {
        let mut ctx = engine();
        eval_str(&mut ctx, "db.aggregate([{$currentOp: {}}]).toArray()").unwrap();
        assert_eq!(
            last_request()["query"]["command"],
            json!({"aggregate": 1, "pipeline": [{"$currentOp": {}}], "cursor": {"batchSize": 5000}})
        );
        eval_str(&mut ctx, "db.setLogLevel(2, 'storage.journal')").unwrap();
        assert_eq!(
            last_request()["query"]["command"],
            json!({"setParameter": 1, "logComponentVerbosity": {"storage": {"journal": {"verbosity": 2}}}})
        );
        eval_str(&mut ctx, "db.createUser({user: 'app', pwd: 'x', roles: ['read']})").unwrap();
        assert_eq!(last_request()["query"]["command"], json!({"createUser": "app", "pwd": "x", "roles": ["read"]}));
        assert_eq!(eval_to_string(&mut ctx, "typeof isInteractive() + ' ' + _isWindows()"), "boolean false");
        assert_eq!(eval_to_string(&mut ctx, "util.inspect([Code('x'), DBRef('c', 1), HexData(0, 'ff'), BSONRegExp('a', 'xi')])"),
            "[\n  Code('x'),\n  DBRef('c', 1),\n  Binary.createFromBase64('/w==', 0),\n  BSONRegExp('a', 'ix')\n]");
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
        assert_eq!(shell_command("show log startupWarnings").as_deref(), Some("__shell.show(\"log\", \"startupWarnings\")"));
        assert_eq!(shell_command("it").as_deref(), Some("__shell.it()"));
        assert_eq!(shell_command("it = 3"), None);
        assert_eq!(shell_command("db.users.find()"), None);
    }
}
