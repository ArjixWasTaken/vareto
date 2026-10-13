//! Postgres NOTICE / WARNING / INFO messages for interactive sessions. sqlx reports
//! them as tracing events (target `sqlx::postgres::notice`) from inside the future
//! that runs the query, so a task-local collects them for the session query
//! running in that task. They never reach the daemon log, which keeps no query text.

use std::future::Future;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;

pub const TARGET: &str = "sqlx::postgres::notice";

tokio::task_local! {
    static NOTICES: Arc<Mutex<Vec<Value>>>;
}

/// Run `f`, returning its output and the notices the server sent meanwhile.
pub async fn collect<F: Future>(f: F) -> (F::Output, Vec<Value>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let out = NOTICES.scope(seen.clone(), f).await;
    let notices = std::mem::take(&mut *seen.lock().unwrap());
    (out, notices)
}

/// The tracing layer that hands notices to [`collect`].
pub struct Layer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Layer {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != TARGET {
            return;
        }
        let mut msg = Message(String::new());
        event.record(&mut msg);
        // sqlx maps the severity to a level: WARNING → WARN, NOTICE → INFO,
        // INFO and LOG → TRACE.
        let severity = match *event.metadata().level() {
            tracing::Level::ERROR => "ERROR",
            tracing::Level::WARN => "WARNING",
            tracing::Level::INFO => "NOTICE",
            tracing::Level::DEBUG => "DEBUG",
            tracing::Level::TRACE => "INFO",
        };
        NOTICES
            .try_with(|n| n.lock().unwrap().push(json!({ "severity": severity, "message": msg.0 })))
            .ok();
    }
}

struct Message(String);

impl Visit for Message {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && self.0.is_empty() {
            self.0 = format!("{value:?}");
        }
    }
}
