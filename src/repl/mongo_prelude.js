// The mongosh API, in plain JavaScript. Every database call goes through the
// native `__vareto_call(json) -> json`, which the shell turns into the daemon's
// structured operations; nothing here runs inside the daemon.
(function (global) {
  'use strict';

  const BATCH = 20;
  const MAX_DOCS = 5000;

  class MongoServerError extends Error {
    constructor(message) {
      super(message);
      this.name = 'MongoServerError';
    }
  }

  function host(req) {
    const res = JSON.parse(global.__vareto_call(JSON.stringify(req)));
    if (res.error !== undefined) throw new MongoServerError(res.error);
    return res.ok;
  }

  // ------------------------------------------------------------ BSON types

  let oidCounter = Math.floor(Math.random() * 0xffffff);
  const oidRandom = Array.from({ length: 5 }, () => Math.floor(Math.random() * 256));
  function hex(n, width) {
    return n.toString(16).padStart(width, '0');
  }

  function ObjectId(id) {
    if (!(this instanceof ObjectId)) return new ObjectId(id);
    if (id instanceof ObjectId) id = id.$oid;
    if (id === undefined || id === null) {
      oidCounter = (oidCounter + 1) % 0x1000000;
      id = hex(Math.floor(Date.now() / 1000), 8) + oidRandom.map((b) => hex(b, 2)).join('') + hex(oidCounter, 6);
    }
    id = String(id);
    if (!/^[0-9a-fA-F]{24}$/.test(id)) throw new TypeError('Argument passed in must be a string of 24 hex characters');
    this.$oid = id.toLowerCase();
  }
  ObjectId.prototype.toHexString = function () { return this.$oid; };
  ObjectId.prototype.toString = function () { return this.$oid; };
  ObjectId.prototype.toJSON = function () { return this.$oid; };
  ObjectId.prototype.equals = function (o) { return o instanceof ObjectId && o.$oid === this.$oid; };
  ObjectId.prototype.getTimestamp = function () { return new Date(parseInt(this.$oid.slice(0, 8), 16) * 1000); };
  ObjectId.prototype.__ejson = function () { return { $oid: this.$oid }; };
  ObjectId.prototype.__inspect = function () { return `ObjectId('${this.$oid}')`; };

  function Long(v) {
    if (!(this instanceof Long)) return new Long(v);
    this.value = String(v);
  }
  Long.prototype.toString = function () { return this.value; };
  Long.prototype.toNumber = function () { return Number(this.value); };
  Long.prototype.valueOf = function () { return Number(this.value); };
  Long.prototype.toJSON = function () { return this.value; };
  Long.prototype.__ejson = function () { return { $numberLong: this.value }; };
  Long.prototype.__inspect = function () { return `Long('${this.value}')`; };

  function Int32(v) {
    if (!(this instanceof Int32)) return new Int32(v);
    this.value = Math.trunc(Number(v));
  }
  Int32.prototype.valueOf = function () { return this.value; };
  Int32.prototype.toJSON = function () { return this.value; };
  Int32.prototype.__ejson = function () { return { $numberInt: String(this.value) }; };
  Int32.prototype.__inspect = function () { return `Int32(${this.value})`; };

  function Double(v) {
    if (!(this instanceof Double)) return new Double(v);
    this.value = Number(v);
  }
  Double.prototype.valueOf = function () { return this.value; };
  Double.prototype.toJSON = function () { return this.value; };
  Double.prototype.__ejson = function () { return { $numberDouble: String(this.value) }; };
  Double.prototype.__inspect = function () { return `Double(${this.value})`; };

  function Decimal128(v) {
    if (!(this instanceof Decimal128)) return new Decimal128(v);
    this.value = String(v);
  }
  Decimal128.prototype.toString = function () { return this.value; };
  Decimal128.prototype.toJSON = function () { return this.value; };
  Decimal128.prototype.__ejson = function () { return { $numberDecimal: this.value }; };
  Decimal128.prototype.__inspect = function () { return `Decimal128('${this.value}')`; };

  const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  function bytesToB64(bytes) {
    let out = '';
    for (let i = 0; i < bytes.length; i += 3) {
      const [a, b, c] = [bytes[i], bytes[i + 1], bytes[i + 2]];
      const n = (a << 16) | ((b || 0) << 8) | (c || 0);
      out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] + (b === undefined ? '=' : B64[(n >> 6) & 63]) + (c === undefined ? '=' : B64[n & 63]);
    }
    return out;
  }
  function b64ToBytes(s) {
    const bytes = [];
    const clean = s.replace(/[^A-Za-z0-9+/]/g, '');
    for (let i = 0; i < clean.length; i += 4) {
      const n = [0, 1, 2, 3].map((j) => (i + j < clean.length ? B64.indexOf(clean[i + j]) : 0));
      const v = (n[0] << 18) | (n[1] << 12) | (n[2] << 6) | n[3];
      bytes.push((v >> 16) & 255);
      if (i + 2 < clean.length) bytes.push((v >> 8) & 255);
      if (i + 3 < clean.length) bytes.push(v & 255);
    }
    return bytes;
  }

  function Binary(base64, subType) {
    if (!(this instanceof Binary)) return new Binary(base64, subType);
    this.base64 = base64;
    this.subType = subType === undefined ? 0 : subType;
  }
  Binary.createFromBase64 = (b, sub) => new Binary(b, sub);
  Binary.prototype.toString = function () { return this.base64; };
  Binary.prototype.__ejson = function () { return { $binary: { base64: this.base64, subType: hex(this.subType, 2) } }; };
  Binary.prototype.__inspect = function () {
    if (this.subType === 4) {
      const h = b64ToBytes(this.base64).map((b) => hex(b, 2)).join('');
      return `UUID('${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}')`;
    }
    return `Binary.createFromBase64('${this.base64}', ${this.subType})`;
  }
  function UUID(s) {
    if (s === undefined) {
      const r = Array.from({ length: 16 }, () => Math.floor(Math.random() * 256));
      r[6] = (r[6] & 0x0f) | 0x40;
      r[8] = (r[8] & 0x3f) | 0x80;
      return new Binary(bytesToB64(r), 4);
    }
    const h = String(s).replace(/-/g, '');
    if (!/^[0-9a-fA-F]{32}$/.test(h)) throw new TypeError('UUID must be a 32 hex digit string');
    const bytes = h.match(/../g).map((x) => parseInt(x, 16));
    return new Binary(bytesToB64(bytes), 4);
  }
  function BinData(subType, base64) { return new Binary(base64, subType); }

  function Timestamp(t, i) {
    if (!(this instanceof Timestamp)) return new Timestamp(t, i);
    if (typeof t === 'object' && t !== null) { i = t.i; t = t.t; }
    this.t = t || 0;
    this.i = i || 0;
  }
  Timestamp.prototype.__ejson = function () { return { $timestamp: { t: this.t, i: this.i } }; };
  Timestamp.prototype.__inspect = function () { return `Timestamp({ t: ${this.t}, i: ${this.i} })`; };

  function MinKey() { if (!(this instanceof MinKey)) return new MinKey(); }
  MinKey.prototype.__ejson = () => ({ $minKey: 1 });
  MinKey.prototype.__inspect = () => 'MinKey()';
  function MaxKey() { if (!(this instanceof MaxKey)) return new MaxKey(); }
  MaxKey.prototype.__ejson = () => ({ $maxKey: 1 });
  MaxKey.prototype.__inspect = () => 'MaxKey()';

  function ISODate(s) {
    const d = s === undefined ? new Date() : new Date(s);
    if (isNaN(d.getTime())) throw new Error(`invalid ISO date: ${s}`);
    return d;
  }

  // ------------------------------------------------------------ Extended JSON

  function toEJSON(v) {
    if (v === null || v === undefined) return null;
    switch (typeof v) {
      case 'number': return isFinite(v) ? v : { $numberDouble: String(v) };
      case 'bigint': return { $numberLong: v.toString() };
      case 'object': break;
      default: return v;
    }
    if (v instanceof Date) return { $date: { $numberLong: String(v.getTime()) } };
    if (v instanceof RegExp) {
      return { $regularExpression: { pattern: v.source, options: v.flags.replace(/[gyd]/g, '').split('').sort().join('') } };
    }
    if (typeof v.__ejson === 'function') return v.__ejson();
    if (Array.isArray(v)) return v.map(toEJSON);
    const out = {};
    for (const k of Object.keys(v)) {
      if (v[k] !== undefined && typeof v[k] !== 'function') out[k] = toEJSON(v[k]);
    }
    return out;
  }

  function fromEJSON(v) {
    if (v === null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(fromEJSON);
    const keys = Object.keys(v);
    if (keys.length === 1 && keys[0][0] === '$') {
      const x = v[keys[0]];
      switch (keys[0]) {
        case '$oid': return new ObjectId(x);
        case '$date': return new Date(typeof x === 'object' ? Number(x.$numberLong) : x);
        case '$numberLong': return new Long(x);
        case '$numberInt': return Number(x);
        case '$numberDouble': return Number(x);
        case '$numberDecimal': return new Decimal128(x);
        case '$binary': return new Binary(x.base64, parseInt(x.subType, 16));
        case '$timestamp': return new Timestamp(x.t, x.i);
        case '$minKey': return new MinKey();
        case '$maxKey': return new MaxKey();
        case '$regularExpression': {
          const flags = (x.options || '').replace(/[^imsu]/g, '');
          return new RegExp(x.pattern, flags);
        }
      }
    }
    const out = {};
    for (const k of keys) out[k] = fromEJSON(v[k]);
    return out;
  }

  // ------------------------------------------------------------ printing

  function quote(s) {
    const q = s.includes("'") && !s.includes('"') ? '"' : "'";
    const esc = s.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t');
    return q + (q === "'" ? esc.replace(/'/g, "\\'") : esc) + q;
  }

  function key(k) {
    return /^[A-Za-z_$][\w$]*$/.test(k) ? k : quote(k);
  }

  const WIDTH = 80;
  function inspect(v, indent, seen) {
    indent = indent || 0;
    seen = seen || [];
    if (v === null) return 'null';
    if (v === undefined) return 'undefined';
    switch (typeof v) {
      case 'string': return quote(v);
      case 'number': return Object.is(v, -0) ? '-0' : String(v);
      case 'boolean': return String(v);
      case 'bigint': return v + 'n';
      case 'function': return `[Function: ${v.name || '(anonymous)'}]`;
      case 'symbol': return v.toString();
    }
    if (typeof v.__inspect === 'function') return v.__inspect();
    if (v instanceof Date) return isNaN(v.getTime()) ? 'Invalid Date' : `ISODate('${v.toISOString()}')`;
    if (v instanceof RegExp) return String(v);
    if (v instanceof Error) return `${v.name}: ${v.message}`;
    if (seen.includes(v)) return '[Circular]';
    seen = seen.concat([v]);
    const isArr = Array.isArray(v);
    let entries;
    if (isArr) {
      entries = v.slice(0, 100).map((x) => inspect(x, indent + 2, seen));
      if (v.length > 100) entries.push(`... ${v.length - 100} more items`);
    } else {
      entries = Object.keys(v).map((k) => `${key(k)}: ${inspect(v[k], indent + 2, seen)}`);
    }
    const [open, close] = isArr ? ['[', ']'] : ['{', '}'];
    if (entries.length === 0) return open + close;
    const single = `${open} ${entries.join(', ')} ${close}`;
    if (indent + single.length <= WIDTH && !single.includes('\n')) return single;
    const pad = ' '.repeat(indent + 2);
    return `${open}\n${entries.map((e) => pad + e).join(',\n')}\n${' '.repeat(indent)}${close}`;
  }

  function text(v) {
    return typeof v === 'string' ? v : inspect(v);
  }

  function print(...args) {
    global.__vareto_print(args.map(text).join(' '));
  }

  // ------------------------------------------------------------ database API

  function query(dbName, q, limit) {
    const req = { kind: 'query', query: toEJSON(q), limit: limit };
    if (dbName !== null) req.query.database = dbName;
    return host(req);
  }

  function command(dbName, cmd) {
    return fromEJSON(query(dbName, { op: 'run_command', command: cmd }).result);
  }

  function writeResult(op, res) {
    const d = fromEJSON(res.detail || {});
    switch (op) {
      case 'insert_one': return { acknowledged: true, insertedId: d.inserted_id };
      case 'insert_many': {
        const ids = {};
        (d.inserted_ids || []).forEach((id, i) => { ids[i] = id; });
        return { acknowledged: true, insertedIds: ids };
      }
      case 'delete_one':
      case 'delete_many':
        return { acknowledged: true, deletedCount: res.affected_rows };
      default:
        return {
          acknowledged: true,
          insertedId: d.upserted_id === undefined ? null : d.upserted_id,
          matchedCount: d.matched,
          modifiedCount: d.modified,
          upsertedCount: d.upserted_id === undefined ? 0 : 1,
        };
    }
  }

  class Cursor {
    constructor(coll, op, spec) {
      this._coll = coll;
      this._op = op;
      this._spec = spec;
      this._docs = null;
      this._pos = 0;
    }
    sort(s) { this._spec.sort = s; return this; }
    limit(n) { this._spec.limit = n; return this; }
    skip(n) { this._spec.skip = n; return this; }
    projection(p) { this._spec.projection = p; return this; }
    project(p) { return this.projection(p); }
    pretty() { return this; }
    batchSize() { return this; }
    hint() { return this; }
    maxTimeMS() { return this; }
    collation() { return this; }
    allowDiskUse() { return this; }
    comment() { return this; }

    // Documents [from, from + n), and whether more follow.
    _batch(from, n) {
      let q;
      const limit = this._spec.limit ? Math.abs(this._spec.limit) : 0;
      if (limit && from >= limit) return { docs: [], more: false };
      if (limit) n = Math.min(n, limit - from);
      if (this._op === 'find') {
        q = { op: 'find', collection: this._coll._name, filter: this._spec.filter || {}, projection: this._spec.projection,
              sort: this._spec.sort, skip: (this._spec.skip || 0) + from, limit: limit ? limit - from : undefined };
      } else {
        const pipeline = (this._spec.pipeline || []).slice();
        if (from) pipeline.push({ $skip: from });
        q = { op: 'aggregate', collection: this._coll._name, pipeline: pipeline };
      }
      const res = query(this._coll._db._name, q, n);
      const docs = fromEJSON(res.documents || []);
      return { docs: docs, more: res.truncated === true };
    }
    toArray() {
      if (this._docs === null) this._docs = this._batch(0, MAX_DOCS).docs;
      return this._docs.slice(this._pos);
    }
    forEach(f) { this.toArray().forEach(f); }
    map(f) { return this.toArray().map(f); }
    hasNext() { return this.toArray().length > 0; }
    next() {
      const rest = this.toArray();
      if (rest.length === 0) throw new Error('cursor exhausted');
      this._pos++;
      return rest[0];
    }
    tryNext() { return this.hasNext() ? this.next() : null; }
    itcount() { return this.toArray().length; }
    size() { return this.toArray().length; }
    count() {
      if (this._op !== 'find') throw new Error('count() is only available on find cursors');
      return this._coll.countDocuments(this._spec.filter || {});
    }
    explain(verbosity) {
      const inner = this._op === 'find'
        ? { find: this._coll._name, filter: this._spec.filter || {}, projection: this._spec.projection, sort: this._spec.sort, skip: this._spec.skip, limit: this._spec.limit }
        : { aggregate: this._coll._name, pipeline: this._spec.pipeline || [], cursor: {} };
      return command(this._coll._db._name, { explain: inner, verbosity: verbosity || 'queryPlanner' });
    }
  }

  function indexName(keys) {
    return Object.keys(keys).map((k) => `${k}_${keys[k]}`).join('_');
  }

  class Collection {
    constructor(db, name) {
      this._db = db;
      this._name = name;
    }
    getName() { return this._name; }
    getFullName() { return `${this._db.getName()}.${this._name}`; }
    toString() { return this.getFullName(); }
    __inspect() { return this.getFullName(); }
    _q(q, limit) { q.collection = this._name; return query(this._db._name, q, limit); }

    find(filter, projection) { return new Cursor(this, 'find', { filter: filter, projection: projection }); }
    findOne(filter, projection) {
      const docs = this.find(filter, projection).limit(1).toArray();
      return docs.length ? docs[0] : null;
    }
    aggregate(pipeline) {
      if (!Array.isArray(pipeline)) pipeline = Array.from(arguments);
      return new Cursor(this, 'aggregate', { pipeline: pipeline });
    }
    countDocuments(filter) { return this._q({ op: 'count', filter: filter || {} }).result; }
    count(filter) { return this.countDocuments(filter); }
    estimatedDocumentCount() { return command(this._db._name, { count: this._name }).n; }
    distinct(field, filter) {
      return fromEJSON(this._q({ op: 'distinct', field: field, filter: filter || {} }, MAX_DOCS).documents || []);
    }
    insertOne(doc) { return writeResult('insert_one', this._q({ op: 'insert_one', documents: [doc] })); }
    insertMany(docs) { return writeResult('insert_many', this._q({ op: 'insert_many', documents: docs })); }
    insert(d) { return Array.isArray(d) ? this.insertMany(d) : this.insertOne(d); }
    _update(op, filter, update, opts) {
      return writeResult(op, this._q({ op: op, filter: filter || {}, update: update, upsert: (opts || {}).upsert }));
    }
    updateOne(f, u, o) { return this._update('update_one', f, u, o); }
    updateMany(f, u, o) { return this._update('update_many', f, u, o); }
    replaceOne(filter, doc, opts) {
      return writeResult('replace_one', this._q({ op: 'replace_one', filter: filter || {}, documents: [doc], upsert: (opts || {}).upsert }));
    }
    deleteOne(filter) { return writeResult('delete_one', this._q({ op: 'delete_one', filter: filter || {} })); }
    deleteMany(filter) { return writeResult('delete_many', this._q({ op: 'delete_many', filter: filter || {} })); }
    getIndexes() { return command(this._db._name, { listIndexes: this._name }).cursor.firstBatch; }
    getIndexKeys() { return this.getIndexes().map((i) => i.key); }
    createIndex(keys, opts) {
      const spec = Object.assign({ key: keys, name: indexName(keys) }, opts || {});
      command(this._db._name, { createIndexes: this._name, indexes: [spec] });
      return spec.name;
    }
    createIndexes(list, opts) { return list.map((k) => this.createIndex(k, opts)); }
    dropIndex(index) { return command(this._db._name, { dropIndexes: this._name, index: index }); }
    dropIndexes() { return command(this._db._name, { dropIndexes: this._name, index: '*' }); }
    drop() { command(this._db._name, { drop: this._name }); return true; }
    stats() { return command(this._db._name, { collStats: this._name }); }
    renameCollection(to) {
      const db = this._db.getName();
      return command('admin', { renameCollection: `${db}.${this._name}`, to: `${db}.${to}` });
    }
  }

  // Unknown properties are sub-collections: db.system.profile.
  function collectionProxy(db, name) {
    return new Proxy(new Collection(db, name), {
      get(target, prop, recv) {
        if (typeof prop === 'symbol' || prop in target) return Reflect.get(target, prop, recv);
        return collectionProxy(db, `${name}.${prop}`);
      },
    });
  }

  class DB {
    // `name === null`: the session's current database (follows `use`).
    constructor(name) { this._name = name; }
    getName() { return this._name !== null ? this._name : host({ kind: 'info' }).database; }
    toString() { return this.getName(); }
    __inspect() { return this.getName(); }
    getCollection(name) { return collectionProxy(this, name); }
    getSiblingDB(name) { return dbProxy(new DB(name)); }
    getMongo() { return { getDB: (n) => this.getSiblingDB(n), getDBNames: () => host({ kind: 'databases' }) }; }
    getCollectionNames() { return host({ kind: 'collections', database: this._name }); }
    getCollectionInfos() { return command(this._name, { listCollections: 1 }).cursor.firstBatch; }
    runCommand(cmd) { return command(this._name, typeof cmd === 'string' ? { [cmd]: 1 } : cmd); }
    adminCommand(cmd) { return command('admin', typeof cmd === 'string' ? { [cmd]: 1 } : cmd); }
    stats() { return command(this._name, { dbStats: 1 }); }
    serverStatus() { return command('admin', { serverStatus: 1 }); }
    hostInfo() { return command('admin', { hostInfo: 1 }); }
    version() { return host({ kind: 'info' }).version; }
    createCollection(name, opts) { return command(this._name, Object.assign({ create: name }, opts || {})); }
    dropDatabase() { return command(this._name, { dropDatabase: 1 }); }
    help() { shell.help(); }
  }

  function dbProxy(db) {
    return new Proxy(db, {
      get(target, prop, recv) {
        if (typeof prop === 'symbol' || prop in target || prop.startsWith('_')) return Reflect.get(target, prop, recv);
        return collectionProxy(target, prop);
      },
    });
  }

  // ------------------------------------------------------------ shell commands

  const HELP = [
    '  Shell Help:',
    '    use <db>                 Set current database',
    '    show dbs | databases     Print a list of all available databases',
    '    show collections|tables  Print a list of all collections for current database',
    '    show users               Print a list of all users for current database',
    '    it                       Show more results from the last cursor',
    '    exit | quit              Quit the shell',
    '',
    '  Collection methods: find, findOne, aggregate, countDocuments, estimatedDocumentCount,',
    '    distinct, insertOne, insertMany, updateOne, updateMany, replaceOne, deleteOne,',
    '    deleteMany, getIndexes, createIndex, dropIndex, drop, stats',
    '  Cursor methods: sort, limit, skip, projection, toArray, forEach, map, next, hasNext, count',
    '  Types: ObjectId, ISODate, Long (NumberLong), Int32 (NumberInt), Decimal128 (NumberDecimal),',
    '    UUID, Timestamp, Binary, MinKey, MaxKey',
  ].join('\n');

  function size(bytes) {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let i = 0;
    while (bytes >= 1024 && i < units.length - 1) { bytes /= 1024; i++; }
    return `${bytes.toFixed(2)} ${units[i]}`;
  }

  const shell = {
    cursor: null,
    shown: 0,
    quitting: false,
    help() { print(HELP); },
    use(name) {
      host({ kind: 'use', database: name });
      print(`switched to db ${name}`);
    },
    show(what) {
      switch (what) {
        case 'dbs':
        case 'databases': {
          let dbs;
          try {
            dbs = command('admin', { listDatabases: 1 }).databases.map((d) => [d.name, size(Number(d.sizeOnDisk))]);
          } catch (e) {
            dbs = host({ kind: 'databases' }).map((n) => [n, '']);
          }
          const w = Math.max(...dbs.map((d) => d[0].length), 0);
          print(dbs.map((d) => `${d[0].padEnd(w)}  ${d[1]}`.trimEnd()).join('\n'));
          break;
        }
        case 'collections':
        case 'tables':
          print(host({ kind: 'collections', database: null }).join('\n'));
          break;
        case 'users':
          print(inspect(command(null, { usersInfo: 1 }).users));
          break;
        default:
          throw new MongoServerError(`'${what}' is not a valid argument for "show".`);
      }
    },
    more() {
      const { docs, more } = this.cursor._batch(this.shown, BATCH);
      this.shown += docs.length;
      if (docs.length) print(inspect(docs));
      if (more) print('Type "it" for more');
      else this.cursor = null;
    },
    it() {
      if (this.cursor === null) print('no cursor');
      else this.more();
    },
    display(v) {
      if (v === undefined) return;
      if (v instanceof Cursor) {
        this.cursor = v;
        this.shown = 0;
        this.more();
        return;
      }
      print(typeof v === 'string' ? v : inspect(v));
    },
    formatError(e) {
      if (e instanceof Error) return `${e.name}: ${e.message}`;
      return `Uncaught ${inspect(e)}`;
    },
  };

  const db = dbProxy(new DB(null));
  Object.assign(global, {
    db: db,
    __shell: shell,
    print: print,
    printjson: (v) => print(inspect(v)),
    console: { log: print, info: print, warn: print, error: print },
    exit: () => { shell.quitting = true; },
    quit: () => { shell.quitting = true; },
    ObjectId: ObjectId,
    Long: Long,
    NumberLong: Long,
    Int32: Int32,
    NumberInt: Int32,
    Double: Double,
    Decimal128: Decimal128,
    NumberDecimal: Decimal128,
    Binary: Binary,
    BinData: BinData,
    UUID: UUID,
    Timestamp: Timestamp,
    MinKey: MinKey,
    MaxKey: MaxKey,
    ISODate: ISODate,
    MongoServerError: MongoServerError,
    EJSON: { stringify: (v) => JSON.stringify(toEJSON(v)), parse: (s) => fromEJSON(JSON.parse(s)), serialize: toEJSON, deserialize: fromEJSON },
    util: { inspect: inspect },
  });
})(globalThis);
