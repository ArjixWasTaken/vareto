---
name: vareto
description: Query and explore databases through the vareto MCP server — MySQL/MariaDB, PostgreSQL, MongoDB and Redis, including ones only reachable through SSH tunnels. Use whenever the user asks about data in a database, wants to inspect a schema, run SQL / a Mongo query / a Redis command, debug data in staging or production, export query results to a file, or add a database connection through vareto. Also use it to install or set up vareto, or when the vareto tools (list_connections, sql_query, mongo_query, redis_command, ...) are expected but missing.
---

# Working with databases through vareto

vareto is an MCP server that connects to databases, opening SSH tunnels on its own when a
connection needs one. Tunnels and pools live in a background daemon: you never open
tunnels manually before querying. Tool names below are the bare names; in your tool list
they carry an MCP prefix (e.g. `mcp__vareto__sql_query` or `mcp__plugin_vareto_vareto__sql_query`).

## If the tools are missing: install vareto

The plugin starts the MCP server by running `vareto mcp`, so the `vareto` binary must
be on `PATH`. If no vareto tools are available, check with `command -v vareto`. If it
isn't installed, tell the user and offer to install it. Installing software changes
their system, so get a yes first. Then:

1. **Rust toolchain available** (`command -v cargo` succeeds): build from source.
   ```sh
   cargo install --locked --git https://github.com/ArjixWasTaken/vareto
   ```
   This puts the binary in `~/.cargo/bin`, which is normally already on `PATH`.
2. **No Rust:** download a prebuilt binary from the latest GitHub release. Each push to
   `master` publishes one, tagged `v<version>.<build>`.
   - Linux builds are static (musl), so they run on any distro.
   - Windows is not supported; vareto relies on unix sockets.

   | `uname -sm` | Asset |
   |---|---|
   | `Linux x86_64` | `vareto-x86_64-unknown-linux-musl.tar.gz` |
   | `Linux aarch64` / `arm64` | `vareto-aarch64-unknown-linux-musl.tar.gz` |
   | `Darwin arm64` | `vareto-aarch64-apple-darwin.tar.gz` |
   | `Darwin x86_64` | `vareto-x86_64-apple-darwin.tar.gz` |

   ```sh
   asset=vareto-x86_64-unknown-linux-musl            # from the table above
   url=https://github.com/ArjixWasTaken/vareto/releases/latest/download/$asset.tar.gz
   tmp=$(mktemp -d) && cd "$tmp"
   curl -fsSLO "$url" && curl -fsSLO "$url.sha256"
   shasum -a 256 -c "$asset.tar.gz.sha256"           # or: sha256sum -c
   tar -xzf "$asset.tar.gz"
   mkdir -p ~/.local/bin && install -m 755 "$asset/vareto" ~/.local/bin/vareto
   ```
   If `~/.local/bin` is not on `PATH`, tell the user to add it to their shell profile.
   Claude Code launches the MCP server with that `PATH`.
3. Check the install with `vareto --version`.
4. **Ask the user to reconnect.** The MCP server only starts once the binary exists, so
   the user must run `/mcp` and reconnect `vareto`, or restart Claude Code. You can't
   do this yourself.
5. **Configure.** If `~/.config/vareto/config.toml` doesn't exist, `vareto config init`
   writes an annotated example. Then:
   - Ask the user for their connections. Never invent hosts or credentials.
   - Help them fill in the file. Keep passwords out of it where possible: prefer
     `password = "${env:VAR}"`, `password_cmd`, or `keyring = true` with
     `vareto secret set <conn>`, which the user runs because it prompts for the
     password.
   - Validate with `vareto config check`.

To update an existing install, rerun the same `cargo install` command (add `--force`)
or download the binary again. The running daemon restarts itself on the next call when
the version changes.

## Workflow

1. **Find the connection.** Call `list_connections` first unless you already know the
   exact name. It shows each connection's `kind` (mysql / postgres / mongodb / redis),
   `readonly`, route (direct or tunnel + ssh host), default `database`, `description`,
   and `masked`. Pick by name/description; if several could match (e.g. `prod-pg` vs
   `staging-pg`), ask the user which one — never guess between production and
   non-production.
2. **Learn the schema before querying.** Don't guess table or column names:
   - `list_databases` → databases (Redis: keyspaces).
   - `list_tables` → tables / collections / Redis keys (with `pattern` to filter).
   - `describe_table` → columns, types, indexes, foreign keys (MongoDB: indexes,
     estimated count, field types inferred from a sample; Redis: type, TTL, size).
3. **Query with the tool matching the kind**: `sql_query`, `mongo_query`,
   `redis_command`. Start narrow (filters, `LIMIT`, projections) and widen only if needed.
4. **Large results → `export_query`** (CSV or JSONL, streamed, no row cap) instead of paging
   through them in context. Then read/analyse the file with normal tools.

The first call on a tunnelled connection can take a few seconds (SSH handshake +
connection setup); later calls reuse it.

## Results

- Responses are compact JSON: SQL → `{"columns", "row_count", "rows"}` (rows are arrays
  in column order); MongoDB → `{"count", "documents"}`; Redis/counts → `{"result"}`.
- Results are capped at the configured `row_limit` (default 200, max 5000 via `limit`).
  `"truncated": true` means more rows exist — refine the query, raise `limit`, or use
  `export_query`. Never present a truncated result as complete (e.g. don't count rows
  of a truncated result; use `COUNT(*)` / `op: "count"`).
- Long cell values are cut with `…[+N chars]`.
- Postgres `NUMERIC`/MySQL `DECIMAL` come back as strings to keep precision; binary
  values as `base64:...`.

## Read-only connections

Most connections are `readonly: true`. On those, vareto rejects anything but a single
read statement *before* it reaches the database, and also runs queries inside a READ
ONLY transaction:

- SQL: one `SELECT` / `WITH ... SELECT` / `EXPLAIN` / `SHOW` / `DESCRIBE` statement. No
  writes, DDL, `SET`, `FOR UPDATE`, `SELECT ... INTO`, multiple statements, or
  side-effect functions (`pg_terminate_backend`, `set_config`, `GET_LOCK`, ...).
- MongoDB: `find`, `aggregate` (without `$out`/`$merge`), `count`, `distinct`.
- Redis: read commands only (GET, HGETALL, SCAN, TTL, TYPE, ZRANGE, XRANGE, INFO, ...).

If a write is needed on a read-only connection, **stop and tell the user**: read-only can
only be lifted by a human editing `readonly = false` in `~/.config/vareto/config.toml`.
Do not try to work around the guard (rephrasing statements, other connections, writing
via functions) — a rejection is the intended outcome.

## Writable connections

When `readonly` is false, writes go straight to the database. Before any statement that
modifies or deletes data (`UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`, Mongo
`update_*`/`delete_*`, Redis `SET`/`DEL`/`EXPIRE`...):

- confirm with the user first, showing the exact statement and target connection,
  especially if the connection looks like production;
- check the blast radius with a `SELECT COUNT(*)` / `count` using the same filter;
- never run an `UPDATE`/`DELETE` without a `WHERE` (or Mongo `{}` filter) unless the user
  explicitly asked for every row.

On writable SQL connections multiple statements are allowed in one call and run with
autocommit (no implicit transaction) — wrap related changes in `BEGIN; ... COMMIT;`
yourself when they must be atomic.

## Masked data

`masked: true` on a connection means some values are replaced before they reach you:
`***` (redacted), `j***@corp.com` (partial), `h:3f9a0c12e4b7` (hash), `null`, or
realistic **fakes** (names, emails like `maria.jones.k3j9x2mq7v4hp8ra@example.org`,
phones, cards, dates shifted up to a year...). Free text can also have emails/cards/
phones/IBANs replaced inside it.

- Masked values are deterministic: equal originals give equal masked values, so joins,
  `GROUP BY`, `DISTINCT` and comparisons across rows still work.
- **Do not present masked values as real data**, and never try to recover originals
  (guessing, filtering on candidate values to confirm them, side channels). Filtering
  `WHERE email = 'real@x.com'` still works server-side, but the value you see back stays
  masked — that's fine and expected.
- If the user needs unmasked data, tell them masking is configured in vareto's config.

## Per-kind notes

Details and examples: [reference.md](reference.md). Short version:

- **PostgreSQL**: the connection is bound to its `database`; in `list_tables`/
  `describe_table`, `database` means *schema* (`describe_table` also accepts
  `schema.table`). Use `EXPLAIN` (not `EXPLAIN ANALYZE` on writable connections unless
  intended — it executes the statement).
- **MySQL/MariaDB**: `database` in introspection tools is the MySQL database; queries can
  use `db.table`.
- **MongoDB**: structured operations, not shell syntax. Filters/pipelines are MongoDB
  Extended JSON: `{"_id": {"$oid": "..."}}`, `{"createdAt": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}`.
- **Redis**: `args` is the command split into an array: `["HGETALL", "user:42"]`. Use
  `list_tables` with a `pattern` (SCAN, max 1000 keys) instead of `KEYS *`. SUBSCRIBE,
  MONITOR, SELECT, MULTI, blocking pops and XREAD are never allowed.

## Adding connections

`save_connection` stores a new connection in `saved.toml`. Saved connections are
**always read-only**; a password goes to the OS keyring, never to a file. Only save one
when the user asks, and only with details the user gave you. For a database behind SSH,
either use `tunnel` with the name of an existing tunnel (an SSH hop from the user's
config; preferred when one already reaches the network, since it shares the SSH
session) or `ssh` (an `~/.ssh/config` alias or `user@host[:port]`). In both cases
`host`/`port` are the database address as seen from that SSH server (default
`127.0.0.1` and the kind's default port). `delete_saved_connection` only removes connections created this
way; connections from `config.toml` are the user's to edit.

## Troubleshooting errors

| Error mentions | Meaning / what to tell the user |
|---|---|
| `HOST KEY MISMATCH` | The SSH server's key changed. Possible MITM — do not retry. User must verify and run `ssh-keygen -R <host>`. |
| `ssh authentication failed` | Key not accepted / encrypted key not in agent. User: `ssh-add <key>`, or fix User/IdentityFile in `~/.ssh/config`. |
| `timed out connecting` / `Connection refused` (ssh) | Bastion unreachable (VPN? host down?). |
| `tunnel "...": opening channel ... AdministrativelyProhibited` | The SSH server forbids port forwarding (`AllowTcpForwarding no`). |
| `tunnel "...": ... ConnectFailed` | SSH works, but the server can't reach the DB at the configured remote address. |
| `unknown connection` / `config error` | Check names with `list_connections`; the user can run `vareto config check`. |
| `read-only connection: ...` | See *Read-only connections* — report, don't bypass. |
| `query timed out` / `operation exceeded time limit` | The query hit `query_timeout` (default 60 s). Narrow it, check `EXPLAIN`, or, if the full result is the goal, use `export_query`, which has its own longer `export_timeout` (default 10 min). Only the user can change either limit, in config.toml. |

For deeper diagnosis the user can run `vareto status` or read the daemon log
(`~/.local/state/vareto/daemon.log`). If the tools are missing entirely, see
*If the tools are missing* above.
