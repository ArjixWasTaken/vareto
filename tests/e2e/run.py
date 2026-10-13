#!/usr/bin/env python3
"""End-to-end test: databases reachable only through an SSH server in Docker.

    python3 tests/e2e/run.py            # builds vareto, starts compose, runs checks
    KEEP=1 python3 tests/e2e/run.py     # leave containers running afterwards
"""
import json, os, subprocess, sys, tempfile, time, pathlib, shutil

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
BIN = ROOT / "target" / "debug" / "vareto"
work = pathlib.Path(tempfile.mkdtemp(prefix="vareto-e2e-"))
keys, ssh_dir, cfg_dir = work / "keys", work / "ssh", work / "config"
for d in (keys, ssh_dir, cfg_dir):
    d.mkdir()
os.chmod(ssh_dir, 0o700)

env = dict(os.environ)
env.pop("SSH_AUTH_SOCK", None)  # only use the test key
env.update(
    VARETO_CONFIG_DIR=str(cfg_dir),
    VARETO_RUNTIME_DIR=str(work / "run"),
    VARETO_STATE_DIR=str(work / "state"),
    VARETO_SSH_DIR=str(ssh_dir),
    KEYS_DIR=str(keys),
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

sh("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(keys / "id_ed25519"))
shutil.copy(keys / "id_ed25519", ssh_dir / "id_ed25519")
os.chmod(ssh_dir / "id_ed25519", 0o600)
(ssh_dir / "config").write_text(
    "Host e2e-bastion\n  HostName 127.0.0.1\n  Port 22220\n  User tester\n  IdentityFile %s\n\n"
    "Host e2e-inner\n  HostName sshd\n  User tester\n  ProxyJump e2e-bastion\n" % (ssh_dir / "id_ed25519")
)
(cfg_dir / "config.toml").write_text("""
[defaults]
row_limit = 50

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
finally:
    v("stop", ok=False)
    if not os.environ.get("KEEP"):
        compose("down", "-v", "--remove-orphans")
    print("daemon log:", work / "state" / "daemon.log")

print(f"\n{len(failures)} failure(s)" if failures else "\nall passed")
sys.exit(1 if failures else 0)
