//! Daemon protocol: one length-delimited JSON request, one response, per frame.
//! A request that streams data (COPY, LOAD DATA, exports) carries an open file
//! descriptor with its frame (`SCM_RIGHTS`), so the data itself never crosses the
//! socket: the daemon reads or writes the CLI's file, pipe or terminal directly.

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context as TaskContext, Poll};

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, Interest, ReadBuf};
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const VERSION: &str = env!("VARETO_VERSION");
/// Bumped when requests change incompatibly; a daemon answering `Hello` with another
/// protocol (or none) is restarted, even when its version string matches.
pub const PROTOCOL: u32 = 2;
const MAX_FRAME: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// First frame on every connection. `force_readonly` applies to the rest of it.
    Hello { version: String, #[serde(default)] force_readonly: bool },
    Status,
    Shutdown,
    ListConnections,
    TunnelList,
    TunnelOpen { name: String },
    TunnelClose { name: String },
    SaveConnection(SaveConnection),
    DeleteSavedConnection { name: String },
    Query { connection: String, query: QuerySpec, limit: Option<usize> },
    Export { connection: String, query: QuerySpec, path: String, format: ExportFormat },
    ListDatabases { connection: String },
    ListTables { connection: String, database: Option<String>, pattern: Option<String> },
    DescribeTable { connection: String, database: Option<String>, table: String },
    /// Pin a dedicated connection for an interactive shell. Sessions belong to the
    /// socket that opened them and close with it.
    SessionOpen { connection: String, #[serde(default)] database: Option<String> },
    /// `warnings`: MySQL, also return the statement's warnings (not only their count).
    SessionQuery { session: u64, query: QuerySpec, #[serde(default)] limit: Option<usize>, #[serde(default)] warnings: bool },
    /// Replace the session's connection with a fresh one (mysql `connect`,
    /// `resetconnection`): session state is gone, the database stays.
    SessionReset { session: u64 },
    /// Switch the session's database (Postgres reconnects, MySQL `USE`, Mongo's
    /// current db, Redis `SELECT`).
    SessionUse { session: u64, database: String },
    /// Postgres `COPY … FROM STDIN`, with the data (text or CSV format).
    SessionCopyIn { session: u64, sql: String, data: String },
    /// Postgres `COPY … TO STDOUT`; the data comes back as text.
    SessionCopyOut { session: u64, sql: String },
    /// The result columns of a statement without running it (psql `\gdesc`).
    SessionDescribe { session: u64, sql: String },
    SessionClose { session: u64 },
    /// Cancel the query a session is running. Sent on a second socket, since the
    /// session's own socket is waiting for that query.
    SessionCancel { session: u64 },
}

impl Request {
    /// Requests sent with a file descriptor ([`send_with_fd`]).
    pub fn takes_fd(&self) -> bool {
        false
    }

    /// The request's `op` name, for messages.
    pub fn op(&self) -> String {
        serde_json::to_value(self).ok().and_then(|v| v["op"].as_str().map(str::to_string)).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok { result: Value },
    /// `info`: the database's structured error fields, when the error came from it.
    Err { error: String, #[serde(default, skip_serializing_if = "Option::is_none")] info: Option<Value> },
}

/// An error answered by the daemon. `info` carries the database's structured
/// error fields (see `db::DbError`).
#[derive(Debug)]
pub struct ServerError {
    pub message: String,
    pub info: Option<Value>,
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ServerError {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SaveConnection {
    /// New connection name (must not already exist).
    pub name: String,
    pub kind: crate::config::Kind,
    /// Name of an existing tunnel (SSH hop) to connect through; `host`/`port` are then
    /// the database address as seen from that SSH server.
    #[serde(default)]
    pub tunnel: Option<String>,
    /// SSH host to tunnel through (alternative to `tunnel`); host/port are then resolved on that server.
    #[serde(default)]
    pub ssh: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    /// Unix socket of a server on this machine (instead of host/port/tunnel).
    #[serde(default)]
    pub socket: Option<String>,
    /// SQLite: absolute path of the database file.
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    /// Stored in the OS keyring, never in the config file.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub params: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// What to run. One variant per backend family.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QuerySpec {
    Sql { sql: String },
    Mongo(MongoQuery),
    Redis { args: Vec<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MongoOp {
    Find,
    Aggregate,
    Count,
    Distinct,
    InsertOne,
    InsertMany,
    UpdateOne,
    UpdateMany,
    ReplaceOne,
    DeleteOne,
    DeleteMany,
    /// A database command (`command`), e.g. `{"listIndexes": "users"}`. Read-only
    /// connections allow only informational commands.
    RunCommand,
}

impl MongoOp {
    pub fn is_read(self) -> bool {
        matches!(self, MongoOp::Find | MongoOp::Aggregate | MongoOp::Count | MongoOp::Distinct)
    }
}

/// A structured MongoDB operation. JSON values are MongoDB Extended JSON
/// (e.g. `{"$oid": "..."}`, `{"$date": "..."}`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MongoQuery {
    /// Database; defaults to the connection's database.
    #[serde(default)]
    pub database: Option<String>,
    /// Collection (not needed for run_command).
    #[serde(default)]
    pub collection: String,
    pub op: MongoOp,
    /// Query filter (find, count, distinct, update_*, delete_*).
    #[serde(default)]
    pub filter: Option<Value>,
    /// Projection (find).
    #[serde(default)]
    pub projection: Option<Value>,
    /// Sort (find).
    #[serde(default)]
    pub sort: Option<Value>,
    /// Documents to skip (find).
    #[serde(default)]
    pub skip: Option<u64>,
    /// Server-side cursor limit for find (the shell's `.limit(n)`). Responses are
    /// capped by the request's own `limit` either way, so tools don't expose it.
    #[serde(default)]
    #[schemars(skip)]
    pub cursor_limit: Option<u64>,
    /// Aggregation pipeline (aggregate).
    #[serde(default)]
    pub pipeline: Option<Vec<Value>>,
    /// Field name (distinct).
    #[serde(default)]
    pub field: Option<String>,
    /// Documents (insert_one uses the first, insert_many all).
    #[serde(default)]
    pub documents: Option<Vec<Value>>,
    /// Update document or pipeline (update_*).
    #[serde(default)]
    pub update: Option<Value>,
    /// Insert when nothing matches (update_*, replace_one).
    #[serde(default)]
    pub upsert: Option<bool>,
    /// The command document (run_command).
    #[serde(default)]
    pub command: Option<Value>,
    /// Driver options for find / aggregate, in the server's camelCase (`hint`,
    /// `collation`, `min`, `max`, `comment`, `returnKey`, `allowDiskUse`, ...). The
    /// shell's cursor methods use it; `maxTimeMS` can only lower the query timeout.
    #[serde(default)]
    #[schemars(skip)]
    pub options: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Csv,
    Jsonl,
}

/// A unix stream that receives file descriptors along with its bytes. Every read goes
/// through `recvmsg`: a plain `read` would silently drop descriptors attached to
/// the bytes it returns. Received descriptors queue up until a request takes them.
pub struct FdStream {
    inner: UnixStream,
    fds: Arc<Mutex<VecDeque<OwnedFd>>>,
}

impl FdStream {
    /// The oldest descriptor received and not yet taken.
    pub fn take_fd(&self) -> Option<OwnedFd> {
        self.fds.lock().unwrap().pop_front()
    }

    /// Close descriptors nobody asked for.
    pub fn drop_fds(&self) {
        self.fds.lock().unwrap().clear();
    }
}

/// At most this many descriptors per message (requests send one).
const MAX_FDS: usize = 4;

fn recv_with_fds(fd: RawFd, buf: &mut [u8], queue: &Mutex<VecDeque<OwnedFd>>) -> io::Result<usize> {
    use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};
    let mut cmsg = nix::cmsg_space!([RawFd; MAX_FDS]);
    let mut iov = [io::IoSliceMut::new(buf)];
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    let flags = MsgFlags::MSG_CMSG_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
    let flags = MsgFlags::empty();
    let msg = recvmsg::<()>(fd, &mut iov, Some(&mut cmsg), flags).map_err(io::Error::from)?;
    let mut q = queue.lock().unwrap();
    for c in msg.cmsgs().map_err(io::Error::from)? {
        if let ControlMessageOwned::ScmRights(fds) = c {
            for f in fds {
                // SAFETY: the kernel just installed `f` in this process for us.
                let owned = unsafe { OwnedFd::from_raw_fd(f) };
                #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
                nix::fcntl::fcntl(&owned, nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC)).ok();
                q.push_back(owned);
            }
        }
    }
    Ok(msg.bytes)
}

impl AsyncRead for FdStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            ready!(this.inner.poll_read_ready(cx))?;
            let fd = this.inner.as_raw_fd();
            let queue = &this.fds;
            let unfilled = buf.initialize_unfilled();
            match this.inner.try_io(Interest::READABLE, || recv_with_fds(fd, unfilled, queue)) {
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncWrite for FdStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

pub type Conn = Framed<FdStream, LengthDelimitedCodec>;

pub fn framed(stream: UnixStream) -> Conn {
    let codec = LengthDelimitedCodec::builder().max_frame_length(MAX_FRAME).new_codec();
    Framed::new(FdStream { inner: stream, fds: Arc::default() }, codec)
}

pub async fn send<T: Serialize>(conn: &mut Conn, msg: &T) -> Result<()> {
    conn.send(bytes::Bytes::from(serde_json::to_vec(msg)?)).await?;
    Ok(())
}

/// [`send`], with `fd` attached to the frame's first bytes. The receiver gets its
/// own descriptor for the same open file; the caller may close `fd` afterwards.
pub async fn send_with_fd<T: Serialize>(conn: &mut Conn, msg: &T, fd: BorrowedFd<'_>) -> Result<()> {
    use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
    // Whatever Framed buffered must go first, or the frames would interleave.
    SinkExt::<bytes::Bytes>::flush(conn).await?;
    let body = serde_json::to_vec(msg)?;
    if body.len() > MAX_FRAME {
        bail!("message too large");
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    let stream = &mut conn.get_mut().inner;
    let fds = [fd.as_raw_fd()];
    let sent = loop {
        stream.writable().await?;
        let raw = stream.as_raw_fd();
        let res = stream.try_io(Interest::WRITABLE, || {
            sendmsg::<()>(raw, &[io::IoSlice::new(&frame)], &[ControlMessage::ScmRights(&fds)], MsgFlags::empty(), None)
                .map_err(io::Error::from)
        });
        match res {
            Ok(n) => break n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    };
    // The descriptor went with the first bytes; the rest is ordinary data.
    stream.write_all(&frame[sent..]).await?;
    Ok(())
}

pub async fn recv<T: DeserializeOwned>(conn: &mut Conn) -> Result<Option<T>> {
    match conn.next().await {
        None => Ok(None),
        Some(frame) => Ok(Some(serde_json::from_slice(&frame?).context("decoding message")?)),
    }
}

/// A client connection to the daemon.
pub struct Client {
    conn: Conn,
}

impl Client {
    pub async fn connect(path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { conn: framed(stream) })
    }

    pub async fn call(&mut self, req: &Request) -> Result<Value> {
        send(&mut self.conn, req).await?;
        self.response().await
    }

    /// [`Self::call`] for a request that takes a file descriptor ([`Request::takes_fd`]).
    pub async fn call_with_fd(&mut self, req: &Request, fd: BorrowedFd<'_>) -> Result<Value> {
        send_with_fd(&mut self.conn, req, fd).await?;
        self.response().await
    }

    async fn response(&mut self) -> Result<Value> {
        match recv::<Response>(&mut self.conn).await? {
            Some(Response::Ok { result }) => Ok(result),
            Some(Response::Err { error, info }) => Err(ServerError { message: error, info }.into()),
            None => bail!("daemon closed the connection"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::os::fd::AsFd as _;

    #[tokio::test]
    async fn descriptor_rides_with_its_frame() {
        let (a, b) = UnixStream::pair().unwrap();
        let (mut tx, mut rx) = (framed(a), framed(b));
        let (pipe_r, pipe_w) = nix::unistd::pipe().unwrap();

        send(&mut tx, &"before").await.unwrap();
        assert_eq!(recv::<String>(&mut rx).await.unwrap().as_deref(), Some("before"));
        assert!(rx.get_ref().take_fd().is_none());

        send_with_fd(&mut tx, &"with fd", pipe_w.as_fd()).await.unwrap();
        drop(pipe_w);
        assert_eq!(recv::<String>(&mut rx).await.unwrap().as_deref(), Some("with fd"));
        let fd = rx.get_ref().take_fd().expect("descriptor received");
        // The receiver writes into the sender's pipe.
        std::fs::File::from(fd).write_all(b"hello").unwrap();
        let mut got = String::new();
        std::fs::File::from(pipe_r).read_to_string(&mut got).unwrap();
        assert_eq!(got, "hello");

        send(&mut tx, &"after").await.unwrap();
        assert_eq!(recv::<String>(&mut rx).await.unwrap().as_deref(), Some("after"));
        assert!(rx.get_ref().take_fd().is_none());
    }
}
