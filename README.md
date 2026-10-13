# vareto

*βαρετό: Greek for "boring".* An MCP server that gives an assistant access to
MySQL/MariaDB, PostgreSQL, MongoDB and Redis. When a database sits behind SSH, vareto
opens the tunnel itself, the way GUI database clients do. It was inspired by
[boring](https://github.com/alebeck/boring).

## How it works

```
MCP client ──stdio──> vareto mcp ──unix socket──> vareto daemon ──ssh──> bastion ──> database
vareto list/open/close/sql ─────────────────────────┘
```

- **`vareto mcp`** is a thin stdio server. On first use it starts a detached daemon
  (`$XDG_RUNTIME_DIR/vareto/daemon.sock`) and forwards every tool call to it.
- **The daemon** owns the SSH sessions, port forwards and database pools, so it runs
  one set of tunnels for all MCP clients and for the CLI.
  - Tunnels open on demand, reconnect if they drop, and close after `idle_timeout`.
    Tunnels opened with `vareto open` stay open until you close them.
  - The daemon exits after `daemon_idle_timeout` with no tunnels open.
- **SSH** is pure Rust (russh).
  - Host settings come from `~/.ssh/config`: HostName, User, Port, IdentityFile and
    ProxyJump.
  - Authentication tries ssh-agent first, then unencrypted key files. Encrypted keys
    must be loaded into the agent.
  - Host keys are trust-on-first-use against `~/.ssh/known_hosts`. A changed key is
    always refused.

## Setup

Install the binary in one of these ways:

- **From source** (needs Rust): `cargo install --locked --git https://github.com/ArjixWasTaken/vareto`
- **Prebuilt:** download a binary from the
  [latest release](https://github.com/ArjixWasTaken/vareto/releases/latest).
  - Every push to `master` publishes one, tagged `v<version>.<run id>`.
  - Assets are named `vareto-<target>.tar.gz`, for x86_64/aarch64 Linux (static musl)
    and macOS.
  - `releases/latest/download/vareto-<target>.tar.gz` always points at the newest build.

Then:

```sh
vareto config init          # writes ~/.config/vareto/config.toml from config.example.toml
$EDITOR ~/.config/vareto/config.toml
vareto config check
```

Then connect it to Claude Code in one of two ways. Don't do both, or you'll get the tools
twice.

**Plugin (recommended).** It installs the MCP server together with a `vareto` skill that
teaches Claude how to use it. The skill covers schema discovery first, read-only and
masking etiquette, MongoDB Extended JSON and error handling.

```sh
# from a local checkout
claude plugin marketplace add /path/to/vareto
# or straight from GitHub
claude plugin marketplace add ArjixWasTaken/vareto

claude plugin install vareto@vareto
```

Inside Claude Code the equivalents are `/plugin marketplace add …` and
`/plugin install vareto@vareto`. The plugin runs `vareto mcp`, so the binary must be on
your `PATH`.

**MCP only:**

```sh
claude mcp add vareto -- vareto mcp          # or: vareto mcp --readonly
```

The repo is its own marketplace. `.claude-plugin/marketplace.json` lists one plugin in
`plugin/`, which contains `.mcp.json` and `skills/vareto/` (`SKILL.md` and
`reference.md`). After changing the skill, run `claude plugin marketplace update vareto`.

See [`config.example.toml`](config.example.toml) for every option. To reach a database
through SSH:

- **Shared SSH hop:** define `[tunnels.x]` with just `host`, the SSH server (a
  `~/.ssh/config` alias or `[user@]host[:port]`). On each connection, set
  `tunnel = "x"` and give the database address as seen from that server in `host` and
  `port`.
  - Every connection through the same SSH destination shares one SSH session; each
    database gets its own forward over it.
  - `tunnel_list` names these forwards `x/<host>:<port>`.
- **Shorthand:** set `ssh = "bastion"` on the connection, with `host` and `port` as
  above. `host` defaults to `127.0.0.1`. These share SSH sessions too.
- **Fixed forward:** a tunnel with `remote = "host:port"`, and optionally
  `local = "127.0.0.1:15432"`. It's useful for pointing a GUI client at a stable local
  port with `vareto open x`.

```toml
[tunnels.dc]                 # the SSH hop
host = "bastion"

[connections.orders]
kind = "mongodb"
tunnel = "dc"
host = "10.0.0.11"           # as seen from the bastion
[connections.billing]
kind = "postgres"
tunnel = "dc"
host = "10.0.0.12"           # same SSH session as `orders`
```

Passwords can be set in any of these ways:

| Setting | Behaviour |
|---|---|
| `password = "plaintext"` or `"${env:VAR}"` | Read from the config file or an environment variable. |
| `password_cmd = "pass show db/prod"` | Uses the command's output. |
| `keyring = true` | Reads the OS keyring. Store the password with `vareto secret set <conn>`. |

## Read-only mode

Connections are read-only unless `config.toml` says `readonly = false`. Each engine is
protected in layers:

| Engine | Protections |
|---|---|
| PostgreSQL | <ul><li>Statement check with sqlparser: one statement only, and it must be SELECT, EXPLAIN or SHOW. No writes inside CTEs, no `FOR UPDATE`, no `SELECT INTO`, and no functions with side effects such as `pg_terminate_backend` or `set_config`.</li><li>Runs inside `BEGIN READ ONLY … ROLLBACK`.</li><li>`default_transaction_read_only=on` on every pooled connection.</li></ul> |
| MySQL/MariaDB | <ul><li>The same statement check, plus no `INTO OUTFILE` and no `GET_LOCK`.</li><li>Runs inside `START TRANSACTION READ ONLY … ROLLBACK`.</li><li>`SET SESSION TRANSACTION READ ONLY` on every pooled connection.</li></ul> |
| MongoDB | <ul><li>Queries are structured operations, never shell JavaScript.</li><li>Only find, aggregate, count and distinct are allowed.</li><li>Pipelines are rejected if they contain `$out` or `$merge` anywhere, including nested stages.</li></ul> |
| Redis | <ul><li>Allowlist of read-only commands.</li><li>Commands that block or change connection state (SUBSCRIBE, MONITOR, SELECT, MULTI, BLPOP and similar) are always refused.</li></ul> |

Read-only can never be relaxed through the MCP:

- Connections the assistant creates with `save_connection` go to `saved.toml`, are
  always read-only, and keep their password in the OS keyring.
- `--readonly` (or `VARETO_READONLY=1`) forces every connection read-only for that
  client.

A database user with read-only grants is still the strongest guarantee, and it is the
only one available for Redis (ACLs).

## Masking

Masking is configured under `[defaults.mask]` or `[connections.<name>.mask]` and is
applied before results leave the daemon, including exports.

- **Column rules** (`columns`) are case-insensitive globs.
  - A pattern without a dot matches the column name, e.g. `*password*`.
  - A pattern with a dot matches `table.column`, or `collection.field.path` for MongoDB.
  - For SQL, the table is known only when a query reads from a single table, so
    `table.column` rules don't apply to joins. Use plain column patterns for those.
- **Value detectors** (`detect`) scan every string for `email`, `card` (Luhn-checked),
  `phone` and `iban`. `detect_strategy` sets how matches are replaced: `partial`
  (default), `redact`, `hash` or `fake`.

| Strategy | Result |
|---|---|
| `redact` (default) | `***` |
| `partial` | `j***@corp.com`, `************1111` |
| `hash` | `h:3f9a0c12e4b7`: stable, so values stay joinable |
| `null` | `null` |
| `fake` | A realistic value of the same kind, deterministic |

### `fake`

`fake` swaps each value for a realistic one. The RNG is seeded with
SHA-256(`seed`, kind, original value), so the same input always produces the same fake:

- across queries, tables, connections and daemon restarts;
- in exports as well as query results.

Joins, `GROUP BY`s and distinct counts on faked columns therefore still behave like the
real data.

The kind of fake is chosen in this order:

1. **`as = "<kind>"`** on the rule. Kinds: `email`, `name`, `first_name`, `last_name`,
   `username`, `phone`, `address`, `street`, `city`, `country`, `zip`, `company`, `ip`,
   `ipv6`, `mac`, `card`, `iban`, `uuid`, `date`, `text`, `number`, `shape`.
2. **The column name.** For example, `customer_email` gives `email`, `surname` gives
   `last_name`, and `phone_number` gives `phone`.
3. **The value itself:** an email, UUID, ISO date or IPv4 address is recognised.
   Anything else uses `shape`, which keeps the format: letters become letters, digits
   become digits, and separators stay. For example, `AB-1234-xy` becomes `QK-8051-rd`
   and `123-45-6789` becomes `904-17-2265`.

Format details:

- Cards keep their layout and length and have a valid Luhn digit.
- IBANs keep their country and length and have valid check digits.
- Dates are shifted by up to ±1 year.
- Numbers keep their sign and digit layout.
- MongoDB `{"$oid"}` and `{"$date"}` wrappers are preserved, and ObjectIds stay hex.

```toml
[connections.prod-pg.mask]
seed = "${env:VARETO_MASK_SEED}"   # secret; see below
columns = [
  { match = "*email*", strategy = "fake" },
  { match = "users.name", strategy = "fake" },
  { match = "orders.contact", strategy = "fake", as = "phone" },
]
detect = ["email", "card"]
detect_strategy = "fake"
```

**Set a secret `seed`.** Fakes and hashes are deterministic, so without a secret anyone
can confirm a guess by faking or hashing a candidate value and comparing. The seed also
feeds `hash`. A connection's seed overrides the default one, and two connections with
the same seed produce matching fakes.

**Emails are unique.** Apps usually enforce a unique email, so a faked email looks like
`maria.jones.k3j9x2mq7v4hp8ra@example.org`.

- The 16-character tag holds 80 bits of the seeded SHA-256 of the original.
- Two distinct emails collide only if those 80 bits match. For 100 million distinct
  emails, the chance of any collision is about 4×10⁻⁹.
- Output is lowercase, so it's unique under case-insensitive constraints too.
  `John@x.com` and `john@x.com` are different inputs and get different fakes.

Other kinds (names, cities, ...) can repeat, the way real names do.

## MCP tools

| Tools | Purpose |
|---|---|
| `list_connections`, `tunnel_list`, `tunnel_open`, `tunnel_close` | Connections and tunnels |
| `list_databases`, `list_tables`, `describe_table` | Schema introspection; for MongoDB, field types are inferred from a sample |
| `sql_query`, `mongo_query`, `redis_command` | Queries. Results are capped by `row_limit`, and long cells are truncated |
| `export_query` | Writes a full result (up to 1M rows) to CSV or JSONL |
| `save_connection`, `delete_saved_connection` | Manage connections saved by the assistant |

## CLI

```sh
vareto list                     # connections + tunnels
vareto open prod / close prod   # pin a tunnel open / close it
vareto sql prod-pg "select now()"
vareto mongo m '{"collection":"users","op":"find","filter":{"active":true}}'
vareto redis cache GET some:key
vareto status / stop
```

The daemon log is at `~/.local/state/vareto/daemon.log`. Set `VARETO_LOG=debug` for
more detail.

## Tests

```sh
cargo test                      # unit tests (guards, masking, config, ...)
python3 tests/e2e/run.py        # Docker: sshd + postgres/mariadb/mongo/redis on an internal-only network
```

The e2e suite reaches the databases only through the SSH container. It covers TOFU,
ProxyJump, reconnecting after an sshd restart, every read-only layer, masking, export
and the MCP protocol over stdio.

It uses its own config, SSH and runtime directories, but `save_connection` stores and
then deletes one entry in your real OS keyring (service `vareto`).
