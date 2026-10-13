# vareto tool reference

## Tools

| Tool | Arguments | Notes |
|---|---|---|
| `list_connections` | — | Start here. |
| `list_databases` | `connection` | Redis: keyspaces from `INFO keyspace`. |
| `list_tables` | `connection`, `database?`, `pattern?` | SQL: `LIKE` pattern (`*` → `%`). Mongo: glob. Redis: SCAN MATCH, max 1000 keys, with types. Postgres: `database` = schema. |
| `describe_table` | `connection`, `table`, `database?` | Postgres accepts `schema.table`. Redis: `table` = key. |
| `sql_query` | `connection`, `sql`, `limit?` | MySQL/MariaDB/Postgres. |
| `mongo_query` | `connection`, `collection`, `op`, `database?`, op-specific fields, `limit?` | See below. |
| `redis_command` | `connection`, `args` | `args[0]` is the command. |
| `export_query` | `connection`, exactly one of `sql` / `mongo` / `redis`, `path`, `format` (`csv`\|`jsonl`) | Streams the whole result to the file; no row cap, bounded by `export_timeout` (default 10 min). Relative paths resolve against the MCP server's working directory. Returns `{path, records, elapsed_secs}`, plus `dropped_fields` when a MongoDB CSV export saw fields after the first 1000 documents (use `jsonl` to keep them). A failed export deletes its partial file. Masking applies. |
| `tunnel_list` / `tunnel_open` / `tunnel_close` | `name` | Rarely needed: tunnels open on demand and close when idle. `tunnel_open` pins a tunnel open. |
| `save_connection` | `name`, `kind`, `ssh?`/`tunnel?`, `host?`, `port?`, `user?`, `password?`, `database?`, `params?`, `description?` | Always read-only. |
| `delete_saved_connection` | `name` | Only for saved connections. |

## SQL examples

```sql
-- Postgres: biggest tables
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) AS size
FROM pg_class WHERE relkind = 'r' ORDER BY pg_total_relation_size(oid) DESC LIMIT 20;

-- Postgres: slow plan check
EXPLAIN SELECT * FROM orders WHERE user_id = 42;

-- MySQL: row counts (approximate)
SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = DATABASE();
```

Prefer explicit columns over `SELECT *` on wide tables, and aggregate in SQL rather than
pulling rows to count/sum them yourself.

## MongoDB `mongo_query`

| `op` | Uses | Read-only allowed |
|---|---|---|
| `find` | `filter`, `projection`, `sort`, `skip`, `limit` | yes |
| `aggregate` | `pipeline` (array of stages) | yes, unless it contains `$out`/`$merge` |
| `count` | `filter` | yes |
| `distinct` | `field`, `filter` | yes |
| `insert_one` / `insert_many` | `documents` | no |
| `update_one` / `update_many` | `filter`, `update` (document or pipeline array) | no |
| `delete_one` / `delete_many` | `filter` | no |

`database` defaults to the connection's database. All JSON is MongoDB Extended JSON:

```json
{"connection": "prod-mongo", "collection": "orders", "op": "find",
 "filter": {"customerId": {"$oid": "64b7f0a1c2d3e4f5a6b7c8d9"},
            "createdAt": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}},
 "projection": {"items": 0}, "sort": {"createdAt": -1}, "limit": 20}
```

```json
{"connection": "prod-mongo", "collection": "orders", "op": "aggregate",
 "pipeline": [{"$match": {"status": "paid"}},
              {"$group": {"_id": "$customerId", "total": {"$sum": "$amount"}}},
              {"$sort": {"total": -1}}, {"$limit": 10}]}
```

Results come back as relaxed Extended JSON (`{"$oid": ...}`, `{"$date": ...}`).
`describe_table` on a collection samples 50 documents to report field paths and the
types seen for each — use it to learn document shape before writing filters.

## Redis `redis_command`

```json
{"connection": "cache", "args": ["HGETALL", "session:abc"]}
{"connection": "cache", "args": ["ZRANGE", "leaderboard", "0", "9", "REV", "WITHSCORES"]}
{"connection": "cache", "args": ["MEMORY", "USAGE", "big:key"]}
```

The database index is fixed per connection (its `database` setting); `SELECT` is not
allowed. Read-only allowlist covers the read commands of strings, hashes, lists, sets,
sorted sets, streams, bitmaps, HyperLogLog, geo, plus `INFO`, `TTL`/`PTTL`, `TYPE`,
`SCAN`/`KEYS`, `DBSIZE`, `OBJECT ENCODING|FREQ|IDLETIME`, `MEMORY USAGE`, `XINFO`,
`SLOWLOG GET`, and RedisJSON/Search/TimeSeries read commands. `CONFIG`, `EVAL` and admin commands
are refused on read-only connections; `CLIENT` is refused everywhere.
