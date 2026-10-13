#!/usr/bin/env python3
"""End-to-end test: databases reachable only through an SSH server in Docker.

    python3 tests/e2e/run.py            # builds vareto, starts compose, runs checks
    KEEP=1 python3 tests/e2e/run.py     # leave containers running afterwards
"""
import json, os, signal, subprocess, sys, tempfile, time, pathlib, shutil

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
BIN = ROOT / "target" / "debug" / "vareto"
work = pathlib.Path(tempfile.mkdtemp(prefix="vareto-e2e-"))
keys, ssh_dir, cfg_dir, sock_dir = work / "keys", work / "ssh", work / "config", work / "sock"
for d in (keys, ssh_dir, cfg_dir, sock_dir):
    d.mkdir()
os.chmod(ssh_dir, 0o700)
os.chmod(sock_dir, 0o777)  # postgres (another uid) creates its socket here

env = dict(os.environ)
env.pop("SSH_AUTH_SOCK", None)  # only use the test key
env.update(
    VARETO_CONFIG_DIR=str(cfg_dir),
    VARETO_RUNTIME_DIR=str(work / "run"),
    VARETO_STATE_DIR=str(work / "state"),
    VARETO_SSH_DIR=str(ssh_dir),
    KEYS_DIR=str(keys),
    SOCK_DIR=str(sock_dir),
)

def sh(*args, check=True, **kw):
    return subprocess.run(args, env=env, check=check, text=True, capture_output=True, **kw)

def compose(*args):
    return sh("docker", "compose", "-f", str(HERE / "docker-compose.yml"), *args)

failures = []
def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  -> {detail}"))
    if not cond:
        failures.append(name)

def v(*args, ok=True):
    r = sh(str(BIN), *args, check=False)
    if ok and r.returncode != 0:
        raise RuntimeError(f"vareto {' '.join(args)} failed: {r.stderr}")
    return r

def vj(*args):
    return json.loads(v(*args).stdout)

def shell(conn, script, *args):
    """Pipe a script into `vareto shell` (script mode stops at the first error)."""
    return subprocess.run([str(BIN), "shell", conn, *args], input=script, env=env, text=True, capture_output=True)

sh("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(keys / "id_ed25519"))
shutil.copy(keys / "id_ed25519", ssh_dir / "id_ed25519")
os.chmod(ssh_dir / "id_ed25519", 0o600)
(ssh_dir / "config").write_text(
    "Host e2e-bastion\n  HostName 127.0.0.1\n  Port 22220\n  User tester\n  IdentityFile %s\n\n"
    "Host e2e-inner\n  HostName sshd\n  User tester\n  ProxyJump e2e-bastion\n\n"
    # TEST-NET-1: never routed, so connecting hangs until the SSH connect timeout.
    "Host e2e-dead\n  HostName 192.0.2.1\n  User tester\n" % (ssh_dir / "id_ed25519")
)
(cfg_dir / "config.toml").write_text("""
[defaults]
row_limit = 50
query_timeout = "2s"
export_timeout = "30s"

# An SSH hop shared by several databases: one SSH session, one forward per destination.
[tunnels.dc]
host = "e2e-bastion"

# Legacy style: a tunnel with a fixed remote.
[tunnels.redis-fixed]
host = "e2e-bastion"
remote = "redis:6379"

[connections.pg]
kind = "postgres"
tunnel = "dc"
host = "postgres"
user = "postgres"
password = "${env:E2E_PG_PW}"
database = "shop"
[connections.pg.mask]
columns = [{ match = "users.email", strategy = "partial" }, { match = "*password*" }, { match = "users.name", strategy = "fake" }]
seed = "e2e-seed"
detect = ["card"]

[connections.pg-rw]
kind = "postgres"
tunnel = "dc"
host = "postgres"
user = "postgres"
password = "pgpass"
database = "shop"
readonly = false

[connections.maria]
kind = "mariadb"
ssh = "e2e-bastion"
host = "mariadb"
user = "app"
password_cmd = "printf 'my pass@1'"
database = "shop"

[connections.maria-rw]
kind = "mariadb"
ssh = "e2e-bastion"
host = "mariadb"
user = "app"
password_cmd = "printf 'my pass@1'"
database = "shop"
readonly = false

[connections.mongo]
kind = "mongodb"
tunnel = "dc"
host = "mongo"
user = "root"
password = "mongopass"
database = "shop"
params = { authSource = "admin" }

[connections.redis-jump]
kind = "redis"
ssh = "e2e-inner"
host = "redis"
password = "redispass"

[connections.redis]
kind = "redis"
tunnel = "dc"
host = "redis"
password = "redispass"

[connections.redis-fixed]
kind = "redis"
tunnel = "redis-fixed"
password = "redispass"

[connections.redis-rw]
kind = "redis"
tunnel = "dc"
host = "redis"
password = "redispass"
readonly = false

[connections.mongo-rw]
kind = "mongodb"
tunnel = "dc"
host = "mongo"
user = "root"
password = "mongopass"
database = "shop"
params = { authSource = "admin" }
readonly = false

# Postgres's socket directory is bind-mounted from the host (no tunnel).
[connections.pg-sock]
kind = "postgres"
socket = "%(sock)s"
user = "postgres"
database = "shop"

[connections.dead]
kind = "postgres"
ssh = "e2e-dead"
host = "db"

[connections.lite]
kind = "sqlite"
path = "%(lite)s"

[connections.lite-rw]
kind = "sqlite"
path = "%(lite)s"
readonly = false
""" % {"sock": sock_dir, "lite": work / "lite.db"})
import sqlite3
with sqlite3.connect(work / "lite.db") as db:
    db.executescript("""
        CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score REAL);
        CREATE INDEX notes_score ON notes (score);
        INSERT INTO notes (body, score) VALUES ('first', 1.5), ('second', NULL);
    """)
env["E2E_PG_PW"] = "pgpass"

print("building..."); subprocess.run(["cargo", "build", "-q"], cwd=ROOT, check=True)
print("starting containers..."); compose("up", "-d", "--build", "--wait")
try:
    # retry the first query while databases finish initialising
    for i in range(60):
        r = v("sql", "pg", "SELECT count(*) AS n FROM users", ok=False)
        if r.returncode == 0:
            break
        time.sleep(1)
    out = json.loads(r.stdout)
    check("postgres through tunnel", out["rows"] == [[300]], out)
    check("known_hosts learned (TOFU)", "[127.0.0.1]:22220" in (ssh_dir / "known_hosts").read_text())

    out = vj("sql", "pg", "SELECT id, email, password_hash, profile, active FROM users ORDER BY id")
    check("row limit + truncated flag", out["row_count"] == 50 and out.get("truncated") is True, out.get("row_count"))
    row = out["rows"][0]
    check("typed values (int/json/bool)", row[0] == 1 and row[3] == {"n": 1} and row[4] is True, row)
    check("mask partial email", row[1] == "u***@example.com", row[1])
    check("mask glob password", row[2] == "***", row[2])
    r = v("sql", "pg", "SELECT pg_sleep(3)", ok=False)
    check("query_timeout cancels slow queries", r.returncode != 0 and "timeout" in r.stderr.lower(), r.stderr)

    a = vj("sql", "pg", "SELECT id, name FROM users WHERE id IN (1, 2) ORDER BY id")["rows"]
    b = vj("sql", "pg", "SELECT name, id FROM users WHERE id IN (1, 2) ORDER BY id")["rows"]
    check("fake strategy replaces names", a[0][1] not in ("User 1", "") and a[0][1] != a[1][1], a)
    check("fake strategy is deterministic across queries", [r[1] for r in a] == [r[0] for r in b], (a, b))
    out = vj("sql", "pg", "SELECT notes FROM orders WHERE id = 1")
    check("card detector", "1111 1111 1111" not in out["rows"][0][0], out)

    for bad in ["DELETE FROM users", "WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d",
                "SELECT 1; DROP TABLE users", "SELECT pg_terminate_backend(1)"]:
        r = v("sql", "pg", bad, ok=False)
        check(f"readonly rejects: {bad[:40]}", r.returncode != 0 and "read-only" in r.stderr, r.stderr)
    r = v("sql", "pg", "SELECT nextval('users_id_seq')", ok=False)
    check("db-level READ ONLY txn blocks nextval", r.returncode != 0 and "read-only" in r.stderr.lower(), r.stderr)
    r = v("--readonly", "sql", "pg-rw", "DELETE FROM orders WHERE id = 2", ok=False)
    check("--readonly forces rw connection read-only", r.returncode != 0, r.stderr)
    out = vj("sql", "pg-rw", "UPDATE orders SET notes = 'x' WHERE id = 2")
    check("writable connection writes", out.get("affected_rows") == 1, out)

    for i in range(60):
        r = v("sql", "maria", "SELECT id, email, balance, data, created FROM users ORDER BY id", ok=False)
        if r.returncode == 0:
            break
        time.sleep(1)
    out = json.loads(r.stdout) if r.returncode == 0 else {}
    ok = r.returncode == 0 and out["rows"][0][0] == 1 and out["rows"][0][2] == "12.50" and out["rows"][1][3] is None
    check("mariadb via implicit ssh tunnel + password_cmd", ok, r.stderr or out)
    if ok:
        check("mariadb blob as base64", out["rows"][0][3] == "base64:3q2+7w==", out["rows"][0][3])
    r = v("sql", "maria", "INSERT INTO users (email) VALUES ('x')", ok=False)
    check("mariadb readonly rejects INSERT", r.returncode != 0, r.stderr)
    out = vj("sql", "maria", "SHOW TABLES")
    check("mariadb SHOW TABLES", out["rows"] == [["users"]], out)

    for i in range(30):
        r = v("mongo", "mongo", json.dumps({"collection": "customers", "op": "find", "filter": {"address.city": "Athens"}, "projection": {"_id": 0}}), ok=False)
        if r.returncode == 0:
            break
        time.sleep(1)
    out = json.loads(r.stdout) if r.returncode == 0 else {}
    check("mongo find", r.returncode == 0 and out["documents"][0]["name"] == "Ann", r.stderr or out)
    out = vj("mongo", "mongo", json.dumps({"collection": "customers", "op": "aggregate", "pipeline": [{"$group": {"_id": None, "n": {"$sum": 1}}}]}))
    check("mongo aggregate", out["documents"][0]["n"] == 2, out)
    r = v("mongo", "mongo", json.dumps({"collection": "customers", "op": "aggregate", "pipeline": [{"$out": "x"}]}), ok=False)
    check("mongo readonly rejects $out", r.returncode != 0, r.stderr)
    r = v("mongo", "mongo", json.dumps({"collection": "customers", "op": "delete_many", "filter": {}}), ok=False)
    check("mongo readonly rejects delete", r.returncode != 0, r.stderr)

    out = vj("redis", "redis", "PING")
    check("redis ping", out["result"] == "PONG", out)
    r = v("redis", "redis", "SET", "k", "v", ok=False)
    check("redis readonly rejects SET", r.returncode != 0, r.stderr)

    out = vj("redis", "redis-fixed", "PING")
    check("legacy tunnel with fixed remote", out["result"] == "PONG", out)
    st = vj("status")
    fwd = {t["name"] for t in st["tunnels"] if t.get("local")}
    check("one SSH session shared by all forwards to the bastion",
          st["ssh_sessions"] == 1 and {"dc/postgres:5432", "dc/mongo:27017", "dc/redis:6379", "@maria", "redis-fixed"} <= fwd,
          (st["ssh_sessions"], fwd))

    out = vj("redis", "redis-jump", "PING")
    check("ProxyJump (bastion -> inner sshd)", out["result"] == "PONG", out)

    # ---- vareto shell: one pinned session per run, native-style output
    r = shell("pg", "select count(*) from users;\n\\dt\n\\d orders\nset search_path to public;\nshow search_path;\n")
    check("psql: query, \\dt, \\d, SET persists", r.returncode == 0 and "   300\n(1 row)" in r.stdout
          and "public | orders | table" in r.stdout and '"orders_pkey" PRIMARY KEY, btree (id)' in r.stdout
          and " search_path\n-------------\n public" in r.stdout, r.stdout + r.stderr)
    r = shell("pg", "\\set ON_ERROR_STOP on\nselect 1;\ninsert into orders (user_id) values (1);\nselect 2;\n")
    check("psql: read-only refused; ON_ERROR_STOP stops with status 3", r.returncode == 3
          and "ERROR:  read-only connection" in r.stderr and "?column?" in r.stdout and " 2\n" not in r.stdout,
          (r.returncode, r.stdout + r.stderr))
    r = shell("pg", "select 1 as one;\nselect emial from users;\nselect 2 as two;\nselect id from users where false;\n")
    check("psql: continues past errors, LINE/HINT, empty results keep their header", r.returncode == 0
          and " two\n-----\n   2" in r.stdout and " id\n----\n(0 rows)" in r.stdout
          and 'ERROR:  column "emial" does not exist\nLINE 1: select emial from users\n               ^\nHINT:  Perhaps you meant' in r.stderr,
          r.stdout + r.stderr)
    r = shell("maria-rw", "call two_results();\nselect id, data from users order by id;\nselect * from users where id < 0;\n")
    check("mysql: each result set of a CALL, binary as hex, Empty set", r.returncode == 0
          and "| a |\n+---+\n| 1 |" in r.stdout and "| b | c |" in r.stdout and "Query OK, 0 rows affected" in r.stdout
          and "0xDEADBEEF" in r.stdout and "Empty set" in r.stdout, r.stdout + r.stderr)
    r = shell("maria", "select 1;\nselect * from nope;\nselect 2;\n", "--force")
    check("mysql: --force goes on after an error, exit 1", r.returncode == 1 and "ERROR 1146" in r.stderr and "| 2 |" in r.stdout,
          (r.returncode, r.stdout + r.stderr))
    r = shell("pg-rw", "begin;\ninsert into orders (user_id, total) values (1, 1);\nselect count(*) as n from orders;\nrollback;\nselect count(*) as n from orders;\n")
    counts = [l.strip() for l in r.stdout.splitlines() if l.strip().isdigit()]
    check("psql: transaction spans statements and rolls back", r.returncode == 0 and len(counts) == 2
          and int(counts[0]) == int(counts[1]) + 1 and "ROLLBACK" in r.stdout, r.stdout + r.stderr)
    r = shell("pg-rw", "\\c postgres\nselect current_database();\n")
    check("psql: \\c switches database", r.returncode == 0 and 'connected to database "postgres"' in r.stdout
          and " postgres\n(1 row)" in r.stdout, r.stdout + r.stderr)
    r = shell("pg-sock", "select count(*) as n from users;\n")
    check("postgres over a unix socket (no tunnel)", r.returncode == 0 and "300" in r.stdout, r.stdout + r.stderr)

    r = shell("maria", "use information_schema\nselect database();\nselect id, email from shop.users order by id\\G\n")
    check("mysql: USE persists, \\G vertical output", r.returncode == 0 and "Database changed" in r.stdout
          and "| information_schema |" in r.stdout and "*************************** 1. row" in r.stdout
          and "email: a@example.com" in r.stdout, r.stdout + r.stderr)
    r = shell("maria", "select * from nope;\n")
    check("mysql: server error wording", r.returncode != 0 and "ERROR 1146 (42S02)" in r.stderr, r.stderr)

    r = shell("redis-rw", 'select 2\nset shellkey "a b"\nselect 0\nget shellkey\nselect 2\nget shellkey\nmulti\nincr n\nexec\nlrange nope 0 -1\n')
    check("redis-cli: SELECT persists, MULTI/EXEC, reply formatting", r.returncode == 0
          and r.stdout.split("\n")[3] == "(nil)" and '"a b"' in r.stdout and "QUEUED" in r.stdout
          and "1) (integer) 1" in r.stdout and "(empty array)" in r.stdout, r.stdout + r.stderr)
    r = shell("redis", "select 3\nset k v\n")
    check("redis-cli: read-only session allows SELECT, refuses SET", r.returncode != 0 and r.stdout.startswith("OK")
          and "(error) read-only connection" in r.stderr, r.stdout + r.stderr)

    r = shell("mongo", "show collections\ndb.customers.find({}, {_id: 0, name: 1}).sort({name: -1})\n"
                       "const n = db.customers.countDocuments()\nprint('n =', n)\ndb.customers.deleteMany({})\n")
    check("mongosh: JS, cursor printing, read-only refused", r.returncode != 0 and "customers" in r.stdout
          and "[ { name: 'Ben' }, { name: 'Ann' } ]" in r.stdout and "n = 2" in r.stdout
          and "read-only connection" in r.stderr, r.stdout + r.stderr)
    r = shell("mongo-rw", "use scratch\ndb.items.drop()\ndb.items.insertMany(Array.from({length: 25}, (_, i) => ({i})))\n"
                          "db.items.find({}, {_id: 0}).sort({i: 1})\nit\n"
                          "db.items.updateOne({i: 99}, {$set: {i: 99}}, {upsert: true}).upsertedCount\n"
                          "db.items.findOne({i: 24}, {_id: 0})\n")
    check("mongosh: writes, `it` paging, upsert", r.returncode == 0 and "switched to db scratch" in r.stdout
          and 'Type "it" for more' in r.stdout and "{ i: 19 }\n]" in r.stdout and "[ { i: 20 }, { i: 21 }" in r.stdout
          and "\n1\n" in r.stdout and "{ i: 24 }" in r.stdout, r.stdout + r.stderr)

    r = shell("mongo-rw", "use scratch\n"
                          "db.bulk.bulkWrite([{insertOne: {document: {_id: 1}}}, {updateOne: {filter: {_id: 2}, update: {$set: {a: 1}}, upsert: true}}, {deleteOne: {filter: {_id: 1}}}]).upsertedCount\n"
                          "db.bulk.find({_id: 2}).hint({_id: 1}).returnKey().toArray()\n"
                          "db.bulk.explain().count({a: 1}).queryPlanner.namespace\n"
                          "db.customers.watch()\n")
    check("mongosh: bulkWrite, cursor options, explain, unsupported methods say why", r.returncode != 0
          and r.stdout.startswith("switched to db scratch\n1\n[ { _id: 2 } ]\nscratch.bulk")
          and "watch() is not available in the vareto shell (change streams" in r.stderr, r.stdout + r.stderr)
    # A hop that hangs while connecting must not hold up queries through other tunnels.
    dead = subprocess.Popen([str(BIN), "sql", "dead", "SELECT 1"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)
    t0 = time.time()
    r = v("sql", "pg", "SELECT 1 AS one", ok=False)
    took = time.time() - t0
    dead.kill()
    check("a hanging SSH hop doesn't stall other tunnels", r.returncode == 0 and took < 3, f"{took:.1f}s {r.stderr}")

    # Ctrl-C: during a query it cancels the query; in a script outside a query it ends the shell.
    p = subprocess.Popen([str(BIN), "shell", "pg-rw"], env=env, text=True, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Signal only once the session is up (the first result printed) and the sleep is running.
    p.stdin.write("select 1 as ready;\nselect pg_sleep(1.8);\n"); p.stdin.flush()
    while "(1 row)" not in p.stdout.readline():
        pass
    time.sleep(0.5); p.send_signal(signal.SIGINT)
    out, err = p.communicate(timeout=10)
    check("Ctrl-C cancels the running query", "canceling statement due to user request" in err, err)
    p = subprocess.Popen([str(BIN), "shell", "mongo"], env=env, text=True, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.stdin.write("db.customers.countDocuments()\nconfig.set('loopIterationLimit', 1e15)\nwhile (true) {}\n"); p.stdin.flush()
    time.sleep(2); p.send_signal(signal.SIGINT)
    try:
        p.communicate(timeout=5)
        check("Ctrl-C ends a script stuck outside a query", p.returncode == 130, p.returncode)
    except subprocess.TimeoutExpired:
        p.kill()
        check("Ctrl-C ends a script stuck outside a query", False, "still running 5s after SIGINT")

    r = shell("lite", ".tables\n.mode column\nselect id, body, score from notes order by id;\ninsert into notes (body) values ('x');\n")
    check("sqlite3: dot commands, column mode, read-only file", r.returncode != 0 and r.stdout.startswith("notes")
          and "id  body    score\n--  ------  -----\n 1  first     1.5" in r.stdout and "read-only" in r.stderr,
          r.stdout + r.stderr)
    r = shell("lite-rw", "begin;\ninsert into notes (body) values ('third');\nrollback;\n.mode list\n.headers off\nselect count(*) from notes;\n")
    check("sqlite3: transactions on a writable file", r.returncode == 0 and r.stdout.strip() == "2", r.stdout + r.stderr)
    out = vj("sql", "lite", "PRAGMA table_info(notes)")
    check("sqlite via sql_query (read-only pragma)", [row[1] for row in out["rows"]] == ["id", "body", "score"], out)

    st = vj("status")
    check("shell sessions close with their shell", st.get("shell_sessions") == 0, st)

    compose("restart", "sshd")
    time.sleep(2)
    r = v("sql", "pg", "SELECT 1 AS one", ok=False)
    if r.returncode != 0:  # the first query may hit a pooled connection that died with the session
        r = v("sql", "pg", "SELECT 1 AS one", ok=False)
    check("reconnects after sshd restart", r.returncode == 0 and json.loads(r.stdout)["rows"] == [[1]], r.stderr)

    lst = v("list").stdout
    check("list shows open tunnels", "@maria" in lst and "open on" in lst, lst)

    # ---------------- MCP over stdio
    p = subprocess.Popen([str(BIN), "mcp"], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    def rpc(i, method, params=None):
        msg = {"jsonrpc": "2.0", "id": i, "method": method}
        if params is not None:
            msg["params"] = params
        p.stdin.write(json.dumps(msg) + "\n"); p.stdin.flush()
        return json.loads(p.stdout.readline())
    init = rpc(1, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "e2e", "version": "0"}})
    check("mcp initialize", init["result"]["serverInfo"]["name"] == "vareto", init)
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"); p.stdin.flush()
    tools = [t["name"] for t in rpc(2, "tools/list")["result"]["tools"]]
    check("mcp tools listed", {"sql_query", "mongo_query", "redis_command", "export_query", "save_connection"} <= set(tools), tools)
    res = rpc(3, "tools/call", {"name": "describe_table", "arguments": {"connection": "pg", "table": "orders"}})
    body = json.loads(res["result"]["content"][0]["text"])
    check("mcp describe_table", any(r[0] == "user_id" for r in body["columns"]["rows"]) and body["foreign_keys"]["rows"], body)
    res = rpc(4, "tools/call", {"name": "sql_query", "arguments": {"connection": "pg", "sql": "DROP TABLE users"}})
    check("mcp readonly error surfaces as tool error", res["result"].get("isError") is True, res)
    export = work / "out.csv"
    res = rpc(5, "tools/call", {"name": "export_query", "arguments": {"connection": "pg", "sql": "SELECT id, email FROM users", "path": str(export), "format": "csv"}})
    body = json.loads(res["result"]["content"][0]["text"])
    lines = export.read_text().splitlines()
    check("mcp export_query (full result, masked)", body["records"] == 300 and len(lines) == 301 and "u***@example.com" in lines[1], body)
    slow = work / "slow.csv"
    res = rpc(51, "tools/call", {"name": "export_query", "arguments": {"connection": "pg", "sql": "SELECT pg_sleep(3), 42 AS answer", "path": str(slow), "format": "csv"}})
    body = json.loads(res["result"]["content"][0]["text"])
    check("export uses export_timeout, not query_timeout", not res["result"].get("isError") and slow.read_text().splitlines()[1].endswith(",42"), body)
    mj = work / "customers.jsonl"
    res = rpc(52, "tools/call", {"name": "export_query", "arguments": {"connection": "mongo", "mongo": {"collection": "customers", "op": "find", "filter": {}}, "path": str(mj), "format": "jsonl"}})
    lines = mj.read_text().splitlines() if mj.exists() else []
    check("mongo export streams documents (jsonl)", len(lines) == 2 and json.loads(lines[0])["name"] in ("Ann", "Ben"), res)
    mc = work / "customers.csv"
    res = rpc(53, "tools/call", {"name": "export_query", "arguments": {"connection": "mongo", "mongo": {"collection": "customers", "op": "find", "projection": {"_id": 0, "name": 1, "address": 1}}, "path": str(mc), "format": "csv"}})
    rows = mc.read_text().splitlines() if mc.exists() else []
    check("mongo export to csv", len(rows) == 3 and rows[0] == "name,address", rows)
    bad = work / "bad.csv"
    res = rpc(54, "tools/call", {"name": "export_query", "arguments": {"connection": "pg", "sql": "SELECT * FROM missing_table", "path": str(bad), "format": "csv"}})
    check("failed export leaves no partial file", res["result"].get("isError") is True and not bad.exists(), res)

    res = rpc(6, "tools/call", {"name": "save_connection", "arguments": {"name": "saved-redis", "kind": "redis", "ssh": "e2e-bastion", "host": "redis", "password": "redispass"}})
    saved_ok = not res["result"].get("isError")
    if saved_ok:
        saved = (cfg_dir / "saved.toml").read_text()
        check("save_connection writes no password, forces readonly", "redispass" not in saved and "readonly = true" in saved, saved)
        res = rpc(7, "tools/call", {"name": "redis_command", "arguments": {"connection": "saved-redis", "args": ["DBSIZE"]}})
        check("saved connection usable (keyring)", not res["result"].get("isError"), res)
        rpc(8, "tools/call", {"name": "delete_saved_connection", "arguments": {"name": "saved-redis"}})
    else:
        print("SKIP save_connection (OS keyring unavailable?):", res["result"]["content"][0]["text"])
    p.stdin.close(); p.wait(timeout=10)

    v("close", "dc/postgres:5432")
    st = vj("status")
    check("close tunnel drops its connections", "pg" not in st["connected"] and "pg-rw" not in st["connected"], st)
    r = v("open", "dc", ok=False)
    check("SSH hop without remote can't be opened directly", r.returncode != 0 and "SSH hop" in r.stderr, r.stderr)
    log = (work / "state" / "daemon.log").read_text()
    check("daemon log contains no SQL text", "pg_sleep" not in log and "FROM users" not in log, [l for l in log.splitlines() if "SELECT" in l][:2])
finally:
    v("stop", ok=False)
    if not os.environ.get("KEEP"):
        compose("down", "-v", "--remove-orphans")
    print("daemon log:", work / "state" / "daemon.log")

print(f"\n{len(failures)} failure(s)" if failures else "\nall passed")
sys.exit(1 if failures else 0)
