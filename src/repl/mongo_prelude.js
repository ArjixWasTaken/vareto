// The mongosh API, in plain JavaScript. Every database call goes through the
// native `__vareto_call(json) -> json`, which the shell turns into the daemon's
// structured operations; nothing here runs inside the daemon.
(function (global) {
  'use strict';

  let BATCH = 20;
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

  // Static constructors, as in mongosh's BSON library.
  ObjectId.createFromHexString = (h) => new ObjectId(h);
  ObjectId.createFromBase64 = (b) => new ObjectId(b64ToBytes(b).map((x) => hex(x, 2)).join(''));
  ObjectId.createFromTime = (secs) => new ObjectId(hex(Math.floor(secs), 8) + '0000000000000000');
  ObjectId.isValid = (v) => v instanceof ObjectId || /^[0-9a-fA-F]{24}$/.test(String(v));
  Long.fromNumber = (n) => new Long(Math.trunc(n));
  Long.fromInt = (n) => new Long(Math.trunc(n));
  Long.fromString = (s) => new Long(String(s));
  Long.fromBigInt = (b) => new Long(b.toString());
  Decimal128.fromString = (s) => new Decimal128(s);
  function hexToB64(h) {
    if (!/^([0-9a-fA-F]{2})*$/.test(h)) throw new TypeError('expected a hex string');
    return bytesToB64((h.match(/../g) || []).map((x) => parseInt(x, 16)));
  }
  Binary.createFromHexString = (h, sub) => new Binary(hexToB64(h), sub);
  Binary.prototype.hex = function () { return b64ToBytes(this.base64).map((b) => hex(b, 2)).join(''); };
  Binary.prototype.length = function () { return b64ToBytes(this.base64).length; };
  function HexData(subType, h) { return Binary.createFromHexString(h, subType); }
  function MD5(h) { return Binary.createFromHexString(h, 5); }

  // JavaScript code (map-reduce functions, $where): a function or its source.
  function Code(code, scope) {
    if (!(this instanceof Code)) return new Code(code, scope);
    this.code = String(code);
    this.scope = scope;
  }
  Code.prototype.__ejson = function () {
    return this.scope ? { $code: this.code, $scope: toEJSON(this.scope) } : { $code: this.code };
  };
  Code.prototype.__inspect = function () { return `Code(${quote(this.code)}${this.scope ? ', ' + inspect(this.scope) : ''})`; };

  function DBRef(collection, id, db) {
    if (!(this instanceof DBRef)) return new DBRef(collection, id, db);
    this.collection = collection;
    this.oid = id;
    this.db = db;
  }
  DBRef.prototype.__ejson = function () {
    const d = { $ref: this.collection, $id: toEJSON(this.oid) };
    if (this.db) d.$db = this.db;
    return d;
  };
  DBRef.prototype.__inspect = function () {
    return `DBRef(${quote(this.collection)}, ${inspect(this.oid)}${this.db ? ', ' + quote(this.db) : ''})`;
  };

  // A regular expression with MongoDB flags JavaScript lacks (x, l).
  function BSONRegExp(pattern, flags) {
    if (!(this instanceof BSONRegExp)) return new BSONRegExp(pattern, flags);
    this.pattern = pattern;
    this.options = (flags || '').split('').sort().join('');
  }
  BSONRegExp.prototype.__ejson = function () { return { $regularExpression: { pattern: this.pattern, options: this.options } }; };
  BSONRegExp.prototype.__inspect = function () { return `BSONRegExp(${quote(this.pattern)}, ${quote(this.options)})`; };

  function BSONSymbol(v) {
    if (!(this instanceof BSONSymbol)) return new BSONSymbol(v);
    this.value = String(v);
  }
  BSONSymbol.prototype.toString = function () { return this.value; };
  BSONSymbol.prototype.__ejson = function () { return { $symbol: this.value }; };
  BSONSymbol.prototype.__inspect = function () { return `BSONSymbol(${quote(this.value)})`; };

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
        case '$code': return new Code(x);
        case '$symbol': return new BSONSymbol(x);
        case '$maxKey': return new MaxKey();
        case '$regularExpression': {
          const flags = (x.options || '').replace(/[^imsu]/g, '');
          return new RegExp(x.pattern, flags);
        }
      }
    }
    if (keys.length === 2 && keys[0] === '$code' && keys[1] === '$scope') return new Code(v.$code, fromEJSON(v.$scope));
    if (keys[0] === '$ref' && keys[1] === '$id' && keys.length <= 3) return new DBRef(v.$ref, fromEJSON(v.$id), v.$db);
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

  // A command answering with a cursor: its first batch (the shell has no getMore).
  function cursorCommand(dbName, cmd) {
    const r = command(dbName, cmd);
    return r.cursor ? r.cursor.firstBatch : r;
  }

  const WHY = {
    session: 'sessions and transactions need a client-side session, which this shell does not keep',
    stream: 'change streams and tailable cursors need a long-lived cursor, which this shell does not keep',
    conn: 'vareto manages connections; open another one with `vareto shell <connection>`',
    admin: 'server and cluster administration is left to mongosh',
    crypt: 'in-use encryption is not supported',
    fs: 'the shell does not change the local filesystem',
    auth: 'vareto authenticates the connection itself',
  };
  function unsupported(what, why) {
    return () => { throw new TypeError(`${what}() is not available in the vareto shell (${why})`); };
  }

  // Result wrappers, as in mongosh.
  class WriteResult {
    constructor(r) { Object.assign(this, r); }
  }
  class BulkWriteResult {
    constructor(r) { Object.assign(this, r); }
  }
  class MongoBulkWriteError extends Error {
    constructor(message, result, writeErrors) {
      super(message);
      this.name = 'MongoBulkWriteError';
      this.result = result;
      this.writeErrors = writeErrors;
    }
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

  // Bulk writes through the server's insert/update/delete commands: consecutive
  // operations of one kind go in one command; `ordered` stops at the first error.
  function bulkWrite(coll, ops, ordered) {
    const result = { acknowledged: true, insertedCount: 0, insertedIds: {}, matchedCount: 0, modifiedCount: 0,
                     deletedCount: 0, upsertedCount: 0, upsertedIds: {} };
    const errors = [];
    const groups = [];
    ops.forEach((op, index) => {
      const [type, spec] = Object.entries(op)[0] || [];
      let kind, item;
      switch (type) {
        case 'insertOne': {
          const doc = Object.assign({}, spec.document);
          if (doc._id === undefined) doc._id = new ObjectId();
          result.insertedIds[index] = doc._id;
          kind = 'insert'; item = doc;
          break;
        }
        case 'updateOne': case 'updateMany': case 'replaceOne':
          kind = 'update';
          item = { q: spec.filter || {}, u: type === 'replaceOne' ? spec.replacement : spec.update,
                   multi: type === 'updateMany', upsert: spec.upsert === true };
          if (spec.arrayFilters) item.arrayFilters = spec.arrayFilters;
          if (spec.collation) item.collation = spec.collation;
          if (spec.hint) item.hint = spec.hint;
          break;
        case 'deleteOne': case 'deleteMany':
          kind = 'delete';
          item = { q: spec.filter || {}, limit: type === 'deleteOne' ? 1 : 0 };
          if (spec.collation) item.collation = spec.collation;
          if (spec.hint) item.hint = spec.hint;
          break;
        default:
          throw new TypeError(`unknown bulkWrite operation ${JSON.stringify(type)}`);
      }
      const last = groups[groups.length - 1];
      if (last && last.kind === kind) last.items.push([index, item]);
      else groups.push({ kind: kind, items: [[index, item]] });
    });
    for (const g of groups) {
      const field = { insert: 'documents', update: 'updates', delete: 'deletes' }[g.kind];
      const r = command(coll._db._name, { [g.kind]: coll._name, [field]: g.items.map((x) => x[1]), ordered: ordered });
      const n = r.n || 0;
      if (g.kind === 'insert') result.insertedCount += n;
      if (g.kind === 'delete') result.deletedCount += n;
      if (g.kind === 'update') {
        const ups = r.upserted || [];
        ups.forEach((u) => { result.upsertedIds[g.items[u.index][0]] = u._id; });
        result.upsertedCount += ups.length;
        result.matchedCount += n - ups.length;
        result.modifiedCount += r.nModified || 0;
      }
      for (const e of r.writeErrors || []) {
        const index = g.items[e.index][0];
        if (g.kind === 'insert') delete result.insertedIds[index];
        errors.push({ index: index, code: e.code, errmsg: e.errmsg, op: ops[index] });
      }
      if (errors.length && ordered) {
        // Inserts after the failure never happened.
        for (const [i] of g.items) if (i > errors[0].index) delete result.insertedIds[i];
        for (const later of groups.slice(groups.indexOf(g) + 1)) {
          if (later.kind === 'insert') for (const [i] of later.items) delete result.insertedIds[i];
        }
        break;
      }
    }
    const res = new BulkWriteResult(result);
    if (errors.length) throw new MongoBulkWriteError(errors.map((e) => e.errmsg).join('; '), res, errors);
    return res;
  }

  // Update/replace/delete results from a one-operation bulkWrite.
  function singleResult(type, r) {
    if (type.startsWith('delete')) return { acknowledged: true, deletedCount: r.deletedCount };
    const ids = Object.values(r.upsertedIds);
    return { acknowledged: true, insertedId: ids.length ? ids[0] : null, matchedCount: r.matchedCount,
             modifiedCount: r.modifiedCount, upsertedCount: r.upsertedCount };
  }

  const DBQuery = {
    Option: { tailable: 2, slaveOk: 4, oplogReplay: 8, noTimeout: 16, awaitData: 32, exhaust: 64, partial: 128 },
    shellBatchSize: 20,
  };

  // A cursor is lazy: nothing runs until it is displayed or iterated. Display pages
  // through it ("it"); iteration reads up to MAX_DOCS documents at once.
  class Cursor {
    constructor(coll, op, spec) {
      this._coll = coll;
      this._op = op;  // 'find' | 'aggregate' | 'array'
      this._spec = spec;
      this._spec.options = this._spec.options || {};
      this._docs = null;
      this._pos = 0;
      this._closed = false;
    }
    _opt(k, v) { this._spec.options[k] = v; return this; }
    sort(s) { this._spec.sort = s; return this; }
    limit(n) { this._spec.limit = n; return this; }
    skip(n) { this._spec.skip = n; return this; }
    projection(p) { this._spec.projection = p; return this; }
    project(p) { return this.projection(p); }
    hint(h) { return this._opt('hint', h); }
    min(d) { return this._opt('min', d); }
    max(d) { return this._opt('max', d); }
    collation(c) { return this._opt('collation', c); }
    comment(c) { return this._opt('comment', c); }
    maxTimeMS(n) { return this._opt('maxTimeMS', n); }
    batchSize(n) { return this._opt('batchSize', n); }
    allowDiskUse(b) { return this._opt('allowDiskUse', b !== false); }
    allowPartialResults() { return this._opt('allowPartialResults', true); }
    returnKey(b) { return this._opt('returnKey', b !== false); }
    showRecordId(b) { return this._opt('showRecordId', b !== false); }
    noCursorTimeout() { return this._opt('noCursorTimeout', true); }
    readConcern(level) { return this._opt('readConcern', { level: level }); }
    // Read preference is per connection in vareto; the setting is accepted and ignored.
    readPref() { return this; }
    pretty() { return this; }
    disableBlockWarnings() { return this; }
    addOption(flag) {
      if (flag & (DBQuery.Option.tailable | DBQuery.Option.awaitData | DBQuery.Option.exhaust)) {
        unsupported('cursor.addOption(tailable/awaitData/exhaust)', WHY.stream)();
      }
      if (flag & DBQuery.Option.noTimeout) this.noCursorTimeout();
      if (flag & DBQuery.Option.partial) this.allowPartialResults();
      return this;
    }
    tailable() { unsupported('cursor.tailable', WHY.stream)(); }
    maxAwaitTimeMS() { unsupported('cursor.maxAwaitTimeMS', WHY.stream)(); }

    // Documents [from, from + n), and whether more follow.
    _batch(from, n) {
      if (this._op === 'array') {
        const docs = this._spec.docs.slice(from, from + n);
        return { docs: docs, more: from + n < this._spec.docs.length };
      }
      let q;
      const limit = this._spec.limit ? Math.abs(this._spec.limit) : 0;
      if (limit && from >= limit) return { docs: [], more: false };
      if (limit) n = Math.min(n, limit - from);
      const options = Object.keys(this._spec.options).length ? this._spec.options : undefined;
      if (this._op === 'find') {
        q = { op: 'find', collection: this._coll._name, filter: this._spec.filter || {}, projection: this._spec.projection,
              sort: this._spec.sort, skip: (this._spec.skip || 0) + from, cursor_limit: limit ? limit - from : undefined,
              options: options };
      } else {
        const pipeline = (this._spec.pipeline || []).slice();
        if (from) pipeline.push({ $skip: from });
        q = { op: 'aggregate', collection: this._coll._name, pipeline: pipeline, options: options };
      }
      const res = query(this._coll._db._name, q, n);
      const docs = fromEJSON(res.documents || []);
      return { docs: docs, more: res.truncated === true };
    }
    toArray() {
      if (this._closed) return [];
      if (this._docs === null) this._docs = this._batch(0, MAX_DOCS).docs;
      const rest = this._docs.slice(this._pos);
      this._pos = this._docs.length;
      return rest;
    }
    _peek() {
      if (this._docs === null) this._docs = this._batch(0, MAX_DOCS).docs;
      return this._closed ? [] : this._docs.slice(this._pos);
    }
    forEach(f) { this.toArray().forEach(f); }
    map(f) { return this.toArray().map(f); }
    hasNext() { return this._peek().length > 0; }
    next() {
      const rest = this._peek();
      if (rest.length === 0) throw new Error('cursor exhausted');
      this._pos++;
      return rest[0];
    }
    tryNext() { return this.hasNext() ? this.next() : null; }
    [Symbol.iterator]() { return this.toArray()[Symbol.iterator](); }
    itcount() { return this.toArray().length; }
    objsLeftInBatch() { return this._docs === null ? 0 : this._peek().length; }
    close() { this._closed = true; }
    isClosed() { return this._closed; }
    isExhausted() { return this._closed || (this._docs !== null && this._pos >= this._docs.length); }
    size() {
      if (this._op !== 'find') return this._peek().length;
      return this._coll.countDocuments(this._spec.filter || {}, { skip: this._spec.skip, limit: this._spec.limit });
    }
    count(applySkipLimit) {
      if (this._op !== 'find') throw new Error('count() is only available on find cursors');
      return applySkipLimit ? this.size() : this._coll.countDocuments(this._spec.filter || {});
    }
    _explainCommand() {
      if (this._op === 'find') {
        const cmd = { find: this._coll._name, filter: this._spec.filter || {} };
        for (const k of ['projection', 'sort', 'skip', 'limit']) if (this._spec[k] !== undefined) cmd[k] = this._spec[k];
        return Object.assign(cmd, this._spec.options);
      }
      return Object.assign({ aggregate: this._coll._name, pipeline: this._spec.pipeline || [], cursor: {} }, this._spec.options);
    }
    explain(verbosity) {
      return command(this._coll._db._name, { explain: this._explainCommand(), verbosity: verbosity || 'queryPlanner' });
    }
  }

  // db.coll.explain(verbosity).find(...) etc.: the plan instead of the results.
  class Explainable {
    constructor(coll, verbosity) {
      this._coll = coll;
      this._verbosity = verbosity === true ? 'allPlansExecution' : verbosity || 'queryPlanner';
    }
    getVerbosity() { return this._verbosity; }
    setVerbosity(v) { this._verbosity = v; }
    _explain(cmd) { return command(this._coll._db._name, { explain: cmd, verbosity: this._verbosity }); }
    find(filter, projection, options) {
      const c = this._coll.find(filter, projection, options);
      c._explainVerbosity = this._verbosity;
      return c;
    }
    aggregate(pipeline, options) {
      if (!Array.isArray(pipeline)) pipeline = Array.from(arguments);
      return this._explain(Object.assign({ aggregate: this._coll._name, pipeline: pipeline, cursor: {} }, options || {}));
    }
    count(filter, options) { return this._explain(Object.assign({ count: this._coll._name, query: filter || {} }, options || {})); }
    countDocuments(filter) { return this.count(filter); }
    distinct(field, filter, options) {
      return this._explain(Object.assign({ distinct: this._coll._name, key: field, query: filter || {} }, options || {}));
    }
    _update(filter, update, multi, opts) {
      const u = Object.assign({ q: filter || {}, u: update, multi: multi }, opts || {});
      return this._explain({ update: this._coll._name, updates: [u] });
    }
    updateOne(f, u, o) { return this._update(f, u, false, o); }
    updateMany(f, u, o) { return this._update(f, u, true, o); }
    replaceOne(f, d, o) { return this._update(f, d, false, o); }
    update(f, u, o) { return this._update(f, u, !!(o && o.multi), o && { upsert: o.upsert }); }
    _delete(filter, one) { return this._explain({ delete: this._coll._name, deletes: [{ q: filter || {}, limit: one ? 1 : 0 }] }); }
    deleteOne(f) { return this._delete(f, true); }
    deleteMany(f) { return this._delete(f, false); }
    remove(f, justOne) { return this._delete(f, justOne === true || (justOne && justOne.justOne)); }
    findAndModify(spec) { return this._explain(Object.assign({ findAndModify: this._coll._name }, spec)); }
    help() { print('Explainable: find, aggregate, count, countDocuments, distinct, updateOne, updateMany, replaceOne, update, deleteOne, deleteMany, remove, findAndModify'); }
  }

  class PlanCache {
    constructor(coll) { this._coll = coll; }
    list(pipeline) { return cursorCommand(this._coll._db._name, { aggregate: this._coll._name, pipeline: [{ $planCacheStats: {} }].concat(pipeline || []), cursor: {} }); }
    clear() { return command(this._coll._db._name, { planCacheClear: this._coll._name }); }
    clearPlansByQuery(q, projection, sort) {
      const cmd = { planCacheClear: this._coll._name, query: q || {} };
      if (projection) cmd.projection = projection;
      if (sort) cmd.sort = sort;
      return command(this._coll._db._name, cmd);
    }
    help() { print('PlanCache: list([pipeline]), clear(), clearPlansByQuery(query, [projection], [sort])'); }
  }

  class Bulk {
    constructor(coll, ordered) {
      this._coll = coll;
      this._ordered = ordered;
      this._ops = [];
      this._executed = false;
    }
    insert(doc) { this._ops.push({ insertOne: { document: doc } }); return this; }
    find(filter) { return new BulkFind(this, filter); }
    execute() {
      if (this._executed) throw new Error('a bulk operation can only be executed once');
      this._executed = true;
      return bulkWrite(this._coll, this._ops, this._ordered);
    }
    getOperations() { return this._ops.slice(); }
    toJSON() {
      const kinds = this._ops.map((o) => Object.keys(o)[0]);
      let batches = 0;
      kinds.forEach((k, i) => { if (i === 0 || k.replace(/One|Many/, '') !== kinds[i - 1].replace(/One|Many/, '')) batches++; });
      return {
        nInsertOps: kinds.filter((k) => k === 'insertOne').length,
        nUpdateOps: kinds.filter((k) => /^(update|replace)/.test(k)).length,
        nRemoveOps: kinds.filter((k) => k.startsWith('delete')).length,
        nBatches: batches,
      };
    }
    toString() { return JSON.stringify(this.toJSON()); }
    __inspect() { return `Bulk(${inspect(this.toJSON())})`; }
  }

  class BulkFind {
    constructor(bulk, filter) {
      this._bulk = bulk;
      this._filter = filter || {};
      this._opts = {};
    }
    upsert() { this._opts.upsert = true; return this; }
    arrayFilters(a) { this._opts.arrayFilters = a; return this; }
    collation(c) { this._opts.collation = c; return this; }
    hint(h) { this._opts.hint = h; return this; }
    _add(type, extra) {
      this._bulk._ops.push({ [type]: Object.assign({ filter: this._filter }, this._opts, extra) });
      return this._bulk;
    }
    update(u) { return this._add('updateMany', { update: u }); }
    updateOne(u) { return this._add('updateOne', { update: u }); }
    replaceOne(d) { return this._add('replaceOne', { replacement: d }); }
    delete() { return this._add('deleteMany', {}); }
    deleteOne() { return this._add('deleteOne', {}); }
    remove() { return this.delete(); }
    removeOne() { return this.deleteOne(); }
  }

  function indexName(keys) {
    return Object.keys(keys).map((k) => `${k}_${keys[k]}`).join('_');
  }

  // find(filter, projection, options) also takes sort/limit/skip in `options`.
  function splitFindOptions(cursor, options) {
    if (!options) return cursor;
    for (const [k, v] of Object.entries(options)) {
      if (k === 'sort' || k === 'limit' || k === 'skip' || k === 'projection') cursor._spec[k] = v;
      else cursor._spec.options[k] = v;
    }
    return cursor;
  }

  class Collection {
    constructor(db, name) {
      this._db = db;
      this._name = name;
    }
    getName() { return this._name; }
    getFullName() { return `${this._db.getName()}.${this._name}`; }
    getDB() { return this._db; }
    getMongo() { return this._db.getMongo(); }
    toString() { return this.getFullName(); }
    __inspect() { return this.getFullName(); }
    _q(q, limit) { q.collection = this._name; return query(this._db._name, q, limit); }
    _cmd(cmd) { return command(this._db._name, cmd); }

    // ---- reads
    find(filter, projection, options) {
      return splitFindOptions(new Cursor(this, 'find', { filter: filter, projection: projection }), options);
    }
    findOne(filter, projection, options) {
      const docs = this.find(filter, projection, options).limit(1).toArray();
      return docs.length ? docs[0] : null;
    }
    aggregate(pipeline, options) {
      if (!Array.isArray(pipeline)) { pipeline = Array.from(arguments); options = undefined; }
      return new Cursor(this, 'aggregate', { pipeline: pipeline, options: Object.assign({}, options || {}) });
    }
    countDocuments(filter, options) {
      options = options || {};
      if (!options.skip && !options.limit && !options.hint && !options.collation) {
        return this._q({ op: 'count', filter: filter || {} }).result;
      }
      const pipeline = [{ $match: filter || {} }];
      if (options.skip) pipeline.push({ $skip: options.skip });
      if (options.limit) pipeline.push({ $limit: options.limit });
      pipeline.push({ $group: { _id: 1, n: { $sum: 1 } } });
      const opts = {};
      if (options.hint) opts.hint = options.hint;
      if (options.collation) opts.collation = options.collation;
      const docs = this.aggregate(pipeline, opts).toArray();
      return docs.length ? docs[0].n : 0;
    }
    count(filter, options) {
      return this._cmd(Object.assign({ count: this._name, query: filter || {} }, options || {})).n;
    }
    estimatedDocumentCount() { return this._cmd({ count: this._name }).n; }
    distinct(field, filter, options) {
      if (options) return this._cmd(Object.assign({ distinct: this._name, key: field, query: filter || {} }, options)).values;
      return fromEJSON(this._q({ op: 'distinct', field: field, filter: filter || {} }, MAX_DOCS).documents || []);
    }

    // ---- writes
    insertOne(doc) { return writeResult('insert_one', this._q({ op: 'insert_one', documents: [doc] })); }
    insertMany(docs, options) {
      if (options && options.ordered === false) {
        const r = this.bulkWrite(docs.map((d) => ({ insertOne: { document: d } })), { ordered: false });
        return { acknowledged: true, insertedIds: r.insertedIds };
      }
      return writeResult('insert_many', this._q({ op: 'insert_many', documents: docs }));
    }
    insert(d, options) { return Array.isArray(d) ? this.insertMany(d, options) : this.insertOne(d); }
    // Plain updates use the structured operations; anything with more options
    // (arrayFilters, collation, hint) goes through the update command.
    _update(op, type, filter, update, opts) {
      opts = opts || {};
      if (Object.keys(opts).some((k) => k !== 'upsert')) {
        const field = type === 'replaceOne' ? 'replacement' : 'update';
        return singleResult(type, this.bulkWrite([{ [type]: Object.assign({ filter: filter, [field]: update }, opts) }]));
      }
      const q = { op: op, filter: filter || {}, upsert: opts.upsert };
      if (op === 'replace_one') q.documents = [update];
      else q.update = update;
      return writeResult(op, this._q(q));
    }
    updateOne(f, u, o) { return this._update('update_one', 'updateOne', f, u, o); }
    updateMany(f, u, o) { return this._update('update_many', 'updateMany', f, u, o); }
    replaceOne(f, d, o) { return this._update('replace_one', 'replaceOne', f, d, o); }
    update(f, u, o) {
      o = o || {};
      const rest = Object.assign({}, o);
      delete rest.multi;
      return o.multi ? this.updateMany(f, u, rest) : this.updateOne(f, u, rest);
    }
    _delete(op, type, filter, opts) {
      if (opts && Object.keys(opts).length) return singleResult(type, this.bulkWrite([{ [type]: Object.assign({ filter: filter }, opts) }]));
      return writeResult(op, this._q({ op: op, filter: filter || {} }));
    }
    deleteOne(f, o) { return this._delete('delete_one', 'deleteOne', f, o); }
    deleteMany(f, o) { return this._delete('delete_many', 'deleteMany', f, o); }
    remove(filter, justOne) {
      const one = justOne === true || (justOne && justOne.justOne === true);
      return one ? this.deleteOne(filter) : this.deleteMany(filter);
    }
    bulkWrite(ops, options) { return bulkWrite(this, ops, !(options && options.ordered === false)); }
    initializeOrderedBulkOp() { return new Bulk(this, true); }
    initializeUnorderedBulkOp() { return new Bulk(this, false); }

    // findAndModify and the findOneAnd* forms return the document (before the
    // change, unless asked for the new one).
    findAndModify(spec) {
      const v = this._cmd(Object.assign({ findAndModify: this._name }, spec || {})).value;
      return v === undefined ? null : v;
    }
    _findOneAnd(filter, change, opts) {
      opts = opts || {};
      const spec = Object.assign({ query: filter || {} }, change);
      if (opts.sort) spec.sort = opts.sort;
      if (opts.projection) spec.fields = opts.projection;
      if (opts.upsert) spec.upsert = true;
      if (opts.arrayFilters) spec.arrayFilters = opts.arrayFilters;
      if (opts.collation) spec.collation = opts.collation;
      if (opts.hint) spec.hint = opts.hint;
      if (opts.returnDocument === 'after' || opts.returnNewDocument) spec.new = true;
      return this.findAndModify(spec);
    }
    findOneAndUpdate(filter, update, opts) { return this._findOneAnd(filter, { update: update }, opts); }
    findOneAndReplace(filter, doc, opts) { return this._findOneAnd(filter, { update: doc }, opts); }
    findOneAndDelete(filter, opts) { return this._findOneAnd(filter, { remove: true }, opts); }

    // ---- indexes
    getIndexes() { return this._cmd({ listIndexes: this._name }).cursor.firstBatch; }
    getIndexSpecs() { return this.getIndexes(); }
    getIndexKeys() { return this.getIndexes().map((i) => i.key); }
    createIndex(keys, opts, commitQuorum) { return this.createIndexes([keys], opts, commitQuorum)[0]; }
    createIndexes(list, opts, commitQuorum) {
      const specs = list.map((k) => Object.assign({ key: k, name: indexName(k) }, opts || {}));
      const cmd = { createIndexes: this._name, indexes: specs };
      if (commitQuorum !== undefined) cmd.commitQuorum = commitQuorum;
      this._cmd(cmd);
      return specs.map((s) => s.name);
    }
    ensureIndex(keys, opts) { return this.createIndex(keys, opts); }
    dropIndex(index) { return this._cmd({ dropIndexes: this._name, index: index }); }
    dropIndexes(indexes) { return this._cmd({ dropIndexes: this._name, index: indexes === undefined ? '*' : indexes }); }
    _hide(index, hidden) {
      const spec = typeof index === 'string' ? { name: index, hidden: hidden } : { keyPattern: index, hidden: hidden };
      return this._cmd({ collMod: this._name, index: spec });
    }
    hideIndex(index) { return this._hide(index, true); }
    unhideIndex(index) { return this._hide(index, false); }
    reIndex() { return this._cmd({ reIndex: this._name }); }
    getPlanCache() { return new PlanCache(this); }

    // ---- Atlas Search indexes
    getSearchIndexes(name) {
      return cursorCommand(this._db._name, { aggregate: this._name, pipeline: [{ $listSearchIndexes: name ? { name: name } : {} }], cursor: {} });
    }
    createSearchIndex(name, definition, type) {
      const spec = typeof name === 'object' ? name : { name: name, definition: definition };
      if (typeof type === 'object' && type.type) spec.type = type.type;
      return this.createSearchIndexes([spec])[0];
    }
    createSearchIndexes(specs) {
      return (this._cmd({ createSearchIndexes: this._name, indexes: specs }).indexesCreated || []).map((i) => i.name);
    }
    dropSearchIndex(name) { return this._cmd({ dropSearchIndex: this._name, name: name }); }
    updateSearchIndex(name, definition) { return this._cmd({ updateSearchIndex: this._name, name: name, definition: definition }); }

    // ---- collection info and maintenance
    stats(scale) {
      const s = typeof scale === 'object' ? scale : { scale: scale };
      return this._cmd(Object.assign({ collStats: this._name }, s.scale ? { scale: s.scale } : {}));
    }
    dataSize() { return this.stats().size; }
    storageSize() { return this.stats().storageSize; }
    totalIndexSize() { return this.stats().totalIndexSize; }
    totalSize() { const s = this.stats(); return s.storageSize + s.totalIndexSize; }
    isCapped() { return this.stats().capped === true; }
    latencyStats(options) {
      return cursorCommand(this._db._name, { aggregate: this._name, pipeline: [{ $collStats: { latencyStats: options || {} } }], cursor: {} });
    }
    validate(options) {
      const o = typeof options === 'object' ? options : { full: options === true };
      return this._cmd(Object.assign({ validate: this._name }, o));
    }
    drop(options) { this._cmd(Object.assign({ drop: this._name }, options || {})); return true; }
    renameCollection(to, dropTarget) {
      const db = this._db.getName();
      return command('admin', { renameCollection: `${db}.${this._name}`, to: `${db}.${to}`, dropTarget: dropTarget === true });
    }
    explain(verbosity) { return new Explainable(this, verbosity); }
    mapReduce(map, reduce, options) {
      options = typeof options === 'string' ? { out: options } : Object.assign({}, options || {});
      const cmd = Object.assign({ mapReduce: this._name, map: new Code(map), reduce: new Code(reduce), out: { inline: 1 } }, options);
      if (options.finalize) cmd.finalize = new Code(options.finalize);
      return this._cmd(cmd);
    }
    analyzeShardKey(key, options) {
      return command('admin', Object.assign({ analyzeShardKey: this.getFullName(), key: key }, options || {}));
    }
    configureQueryAnalyzer(options) {
      return command('admin', Object.assign({ configureQueryAnalyzer: this.getFullName() }, options || {}));
    }
    checkMetadataConsistency(options) {
      return cursorCommand(this._db._name, Object.assign({ checkMetadataConsistency: this._name }, options || {}));
    }
    help() {
      print('Collection methods: find, findOne, aggregate, count, countDocuments, estimatedDocumentCount, distinct,\n' +
        '  insertOne, insertMany, insert, updateOne, updateMany, replaceOne, update, deleteOne, deleteMany, remove,\n' +
        '  bulkWrite, initializeOrderedBulkOp, initializeUnorderedBulkOp, findAndModify, findOneAndUpdate,\n' +
        '  findOneAndReplace, findOneAndDelete, getIndexes, createIndex, createIndexes, dropIndex, dropIndexes,\n' +
        '  hideIndex, unhideIndex, reIndex, getPlanCache, getSearchIndexes, createSearchIndex(es), dropSearchIndex,\n' +
        '  updateSearchIndex, stats, dataSize, storageSize, totalIndexSize, totalSize, isCapped, latencyStats,\n' +
        '  validate, drop, renameCollection, explain, mapReduce, analyzeShardKey, configureQueryAnalyzer');
    }
  }

  // mongosh methods this shell can't offer: calling them says why, instead of the
  // name silently becoming a (sub)collection.
  const UNSUPPORTED_COLLECTION = {
    watch: WHY.stream, compactStructuredEncryptionData: WHY.crypt, getShardDistribution: WHY.admin,
    getShardVersion: WHY.admin, getShardLocation: WHY.admin,
  };
  const UNSUPPORTED_DB = {
    watch: WHY.stream, auth: WHY.auth, logout: WHY.auth, fsyncLock: WHY.admin, fsyncUnlock: WHY.admin,
    shutdownServer: WHY.admin, rotateCertificates: WHY.admin, createEncryptedCollection: WHY.crypt,
  };

  // Unknown properties are sub-collections: db.system.profile.
  function collectionProxy(db, name) {
    return new Proxy(new Collection(db, name), {
      get(target, prop, recv) {
        if (typeof prop === 'symbol' || prop in target) return Reflect.get(target, prop, recv);
        if (Object.prototype.hasOwnProperty.call(UNSUPPORTED_COLLECTION, prop)) {
          return unsupported(`db.${name}.${prop}`, UNSUPPORTED_COLLECTION[prop]);
        }
        return collectionProxy(db, `${name}.${prop}`);
      },
    });
  }

  // A cursor over documents already in hand (db.aggregate).
  function arrayCursor(db, docs) {
    return new Cursor(new Collection(db, ''), 'array', { docs: docs });
  }

  function size(bytes) {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let i = 0;
    while (bytes >= 1024 && i < units.length - 1) { bytes /= 1024; i++; }
    return `${bytes.toFixed(2)} ${units[i]}`;
  }

  function setNested(doc, path, value) {
    const parts = path.split('.');
    let d = doc;
    for (const p of parts.slice(0, -1)) d = d[p] = d[p] || {};
    d[parts[parts.length - 1]] = value;
    return doc;
  }

  class DB {
    // `name === null`: the session's current database (follows `use`).
    constructor(name) { this._name = name; }
    getName() { return this._name !== null ? this._name : host({ kind: 'info' }).database; }
    toString() { return this.getName(); }
    __inspect() { return this.getName(); }
    getCollection(name) { return collectionProxy(this, name); }
    getSiblingDB(name) { return dbProxy(new DB(name)); }
    getMongo() { return mongo; }
    getCollectionNames() { return host({ kind: 'collections', database: this._name }); }
    getCollectionInfos(filter, opts) {
      const cmd = Object.assign({ listCollections: 1, filter: filter || {} }, opts || {});
      return command(this._name, cmd).cursor.firstBatch;
    }
    listCollections(filter, opts) { return this.getCollectionInfos(filter, opts); }
    runCommand(cmd, opts) { return command(this._name, Object.assign(typeof cmd === 'string' ? { [cmd]: 1 } : cmd, opts || {})); }
    adminCommand(cmd) { return command('admin', typeof cmd === 'string' ? { [cmd]: 1 } : cmd); }
    aggregate(pipeline, options) {
      const cmd = Object.assign({ aggregate: 1, pipeline: pipeline, cursor: { batchSize: MAX_DOCS } }, options || {});
      return arrayCursor(this, cursorCommand(this._name, cmd));
    }
    commandHelp(name) { return command(this._name, { [name]: 1, help: true }).help; }
    listCommands() {
      const cmds = command(this._name, { listCommands: 1 }).commands;
      print(Object.keys(cmds).sort().map((n) => `${n}: ${(cmds[n].help || '').split('\n')[0]}`).join('\n'));
    }

    // ---- server info
    stats(scale) {
      const s = typeof scale === 'object' ? scale : { scale: scale };
      return command(this._name, Object.assign({ dbStats: 1 }, s.scale ? { scale: s.scale } : {}));
    }
    serverStatus(opts) { return command('admin', Object.assign({ serverStatus: 1 }, opts || {})); }
    serverBuildInfo() { return command('admin', { buildInfo: 1 }); }
    serverCmdLineOpts() { return command('admin', { getCmdLineOpts: 1 }); }
    hostInfo() { return command('admin', { hostInfo: 1 }); }
    version() { return host({ kind: 'info' }).version; }
    hello() { return command(this._name, { hello: 1 }); }
    isMaster() { return this.hello(); }
    currentOp(filter) {
      const f = filter === true ? { $all: true } : filter || {};
      return command('admin', Object.assign({ currentOp: 1 }, f));
    }
    killOp(op) { return command('admin', { killOp: 1, op: op }); }
    getLogComponents() { return command('admin', { getParameter: 1, logComponentVerbosity: 1 }).logComponentVerbosity; }
    setLogLevel(level, component) {
      const doc = component ? setNested({}, `${component}.verbosity`, level) : { verbosity: level };
      return command('admin', { setParameter: 1, logComponentVerbosity: doc });
    }
    getProfilingStatus() { return command(this._name, { profile: -1 }); }
    getProfilingLevel() { return this.getProfilingStatus().was; }
    setProfilingLevel(level, options) {
      const o = typeof options === 'number' ? { slowms: options } : options || {};
      return command(this._name, Object.assign({ profile: level }, o));
    }
    getReplicationInfo() {
      const local = this.getSiblingDB('local');
      const stats = local.oplog.rs.stats();
      const first = local.oplog.rs.find({}, { ts: 1 }).sort({ $natural: 1 }).limit(1).toArray()[0];
      const last = local.oplog.rs.find({}, { ts: 1 }).sort({ $natural: -1 }).limit(1).toArray()[0];
      const info = { configuredLogSizeMB: (stats.maxSize || 0) / (1024 * 1024), usedMB: Math.ceil(stats.size / (1024 * 1024) * 100) / 100 };
      if (first && last) {
        const t1 = first.ts.t, t2 = last.ts.t;
        Object.assign(info, { timeDiff: t2 - t1, timeDiffHours: Math.round((t2 - t1) / 36) / 100,
                              tFirst: new Date(t1 * 1000).toString(), tLast: new Date(t2 * 1000).toString(), now: new Date().toString() });
      }
      return info;
    }
    printReplicationInfo() {
      const i = this.getReplicationInfo();
      print(`actual oplog size\n  '${i.configuredLogSizeMB} MB'\n---\nconfigured oplog size\n  '${i.configuredLogSizeMB} MB'\n---\n` +
        `log length start to end\n  '${i.timeDiff} secs (${i.timeDiffHours} hrs)'\n---\noplog first event time\n  '${i.tFirst}'\n---\n` +
        `oplog last event time\n  '${i.tLast}'\n---\nnow\n  '${i.now}'`);
    }
    printSecondaryReplicationInfo() {
      const st = command('admin', { replSetGetStatus: 1 });
      const primary = st.members.find((m) => m.stateStr === 'PRIMARY');
      const out = [];
      for (const m of st.members.filter((x) => x.stateStr === 'SECONDARY')) {
        const lag = primary ? Math.round((primary.optimeDate - m.optimeDate) / 1000) : null;
        out.push(`source: ${m.name}\n{\n  syncedTo: '${m.optimeDate}',\n  replLag: '${lag} secs (${Math.round(lag / 36) / 100} hrs) behind the primary '\n}`);
      }
      print(out.join('\n---\n'));
    }
    printCollectionStats(scale) {
      for (const name of this.getCollectionNames()) {
        print(name);
        print(inspect(this.getCollection(name).stats(scale)));
        print('---');
      }
    }
    printShardingStatus() { sh.status(); }
    checkMetadataConsistency(options) {
      return cursorCommand(this._name, Object.assign({ checkMetadataConsistency: 1 }, options || {}));
    }

    // ---- collections and the database
    createCollection(name, opts) { return command(this._name, Object.assign({ create: name }, opts || {})); }
    createView(name, source, pipeline, opts) {
      return command(this._name, Object.assign({ create: name, viewOn: source, pipeline: pipeline || [] }, opts || {}));
    }
    dropDatabase() { return command(this._name, { dropDatabase: 1 }); }

    // ---- users and roles
    createUser(user, writeConcern) {
      const cmd = Object.assign({ createUser: user.user }, user);
      delete cmd.user;
      if (writeConcern) cmd.writeConcern = writeConcern;
      return command(this._name, cmd);
    }
    updateUser(name, update) { return command(this._name, Object.assign({ updateUser: name }, update)); }
    changeUserPassword(name, pwd) { return this.updateUser(name, { pwd: pwd }); }
    dropUser(name) { return command(this._name, { dropUser: name }); }
    removeUser(name) { return this.dropUser(name); }
    dropAllUsers() { return command(this._name, { dropAllUsersFromDatabase: 1 }); }
    getUser(name, opts) {
      const users = command(this._name, Object.assign({ usersInfo: { user: name, db: this.getName() } }, opts || {})).users;
      return users.length ? users[0] : null;
    }
    getUsers(opts) { return command(this._name, Object.assign({ usersInfo: 1 }, opts || {})).users; }
    grantRolesToUser(name, roles) { return command(this._name, { grantRolesToUser: name, roles: roles }); }
    revokeRolesFromUser(name, roles) { return command(this._name, { revokeRolesFromUser: name, roles: roles }); }
    createRole(role, writeConcern) {
      const cmd = Object.assign({ createRole: role.role }, role);
      delete cmd.role;
      if (writeConcern) cmd.writeConcern = writeConcern;
      return command(this._name, cmd);
    }
    updateRole(name, update) { return command(this._name, Object.assign({ updateRole: name }, update)); }
    dropRole(name) { return command(this._name, { dropRole: name }); }
    dropAllRoles() { return command(this._name, { dropAllRolesFromDatabase: 1 }); }
    getRole(name, opts) {
      const roles = command(this._name, Object.assign({ rolesInfo: name }, opts || {})).roles;
      return roles.length ? roles[0] : null;
    }
    getRoles(opts) { return command(this._name, Object.assign({ rolesInfo: 1 }, opts || {})).roles; }
    grantPrivilegesToRole(name, privileges) { return command(this._name, { grantPrivilegesToRole: name, privileges: privileges }); }
    revokePrivilegesFromRole(name, privileges) { return command(this._name, { revokePrivilegesFromRole: name, privileges: privileges }); }
    grantRolesToRole(name, roles) { return command(this._name, { grantRolesToRole: name, roles: roles }); }
    revokeRolesFromRole(name, roles) { return command(this._name, { revokeRolesFromRole: name, roles: roles }); }

    help() { shell.help(); }
  }

  function dbProxy(db) {
    return new Proxy(db, {
      get(target, prop, recv) {
        if (typeof prop === 'symbol' || prop in target || prop.startsWith('_')) return Reflect.get(target, prop, recv);
        if (Object.prototype.hasOwnProperty.call(UNSUPPORTED_DB, prop)) return unsupported(`db.${prop}`, UNSUPPORTED_DB[prop]);
        return collectionProxy(target, prop);
      },
    });
  }

  // db.getMongo(): this shell's one connection.
  const mongo = {
    getDB: (name) => dbProxy(new DB(name)),
    getDBNames: () => host({ kind: 'databases' }),
    getDBs: () => command('admin', { listDatabases: 1 }),
    getURI: () => `vareto connection "${host({ kind: 'info' }).connection}"`,
    getReadPrefMode: () => 'primary',
    getReadPrefTagSet: () => null,
    getWriteConcern: () => undefined,
    setReadPref: unsupported('Mongo.setReadPref', 'read preference is part of the connection settings'),
    setWriteConcern: unsupported('Mongo.setWriteConcern', 'pass writeConcern to the command instead'),
    setCausalConsistency: unsupported('Mongo.setCausalConsistency', WHY.session),
    startSession: unsupported('Mongo.startSession', WHY.session),
    watch: unsupported('Mongo.watch', WHY.stream),
    bulkWrite: unsupported('Mongo.bulkWrite', 'use db.collection.bulkWrite()'),
    toString: () => mongo.getURI(),
    __inspect: () => mongo.getURI(),
  };

  // ------------------------------------------------------------ rs / sh (read-only helpers)

  const ADMIN_RS = ['add', 'addArb', 'freeze', 'initiate', 'reconfig', 'reconfigForPSASet', 'remove', 'stepDown', 'syncFrom'];
  const rs = {
    status: () => command('admin', { replSetGetStatus: 1 }),
    conf: () => command('admin', { replSetGetConfig: 1 }).config,
    config: () => rs.conf(),
    hello: () => db.hello(),
    isMaster: () => db.hello(),
    printReplicationInfo: () => db.getSiblingDB('local').printReplicationInfo(),
    printSecondaryReplicationInfo: () => db.printSecondaryReplicationInfo(),
    help: () => print('rs: status(), conf(), hello(), printReplicationInfo(), printSecondaryReplicationInfo()\n' +
      `  (${ADMIN_RS.join(', ')}: ${WHY.admin})`),
  };
  for (const m of ADMIN_RS) rs[m] = unsupported(`rs.${m}`, WHY.admin);

  const ADMIN_SH = [
    'abortMoveCollection', 'abortReshardCollection', 'abortUnshardCollection', 'addShard', 'addShardTag', 'addShardToZone',
    'addTagRange', 'commitReshardCollection', 'disableAutoMerger', 'disableAutoSplit', 'disableBalancing', 'disableMigrations',
    'enableAutoMerger', 'enableAutoSplit', 'enableBalancing', 'enableMigrations', 'enableSharding', 'moveChunk',
    'moveCollection', 'moveRange', 'removeRangeFromZone', 'removeShardTag', 'removeShardFromZone', 'removeTagRange',
    'reshardCollection', 'setBalancerState', 'shardAndDistributeCollection', 'shardCollection', 'splitAt', 'splitFind',
    'startAutoMerger', 'startBalancer', 'stopAutoMerger', 'stopBalancer', 'unshardCollection', 'updateZoneKeyRange',
    'waitForBalancer', 'waitForBalancerOff', 'waitForPingChange',
  ];
  const sh = {
    listShards: () => command('admin', { listShards: 1 }).shards,
    getBalancerState: () => {
      const s = db.getSiblingDB('config').settings.findOne({ _id: 'balancer' });
      return !(s && (s.stopped === true || s.mode === 'off'));
    },
    isBalancerRunning: () => command('admin', { balancerStatus: 1 }),
    balancerCollectionStatus: (ns) => command('admin', { balancerCollectionStatus: ns }),
    getShardedDataDistribution: (options) =>
      cursorCommand('admin', { aggregate: 1, pipeline: [{ $shardedDataDistribution: options || {} }], cursor: {} }),
    checkMetadataConsistency: (options) => db.getSiblingDB('admin').checkMetadataConsistency(options),
    status: () => {
      const config = db.getSiblingDB('config');
      const out = { shardingVersion: config.version.findOne(), shards: sh.listShards(),
                    balancer: { 'Currently enabled': sh.getBalancerState(), 'Currently running': sh.isBalancerRunning().inBalancerRound },
                    databases: config.databases.find().toArray() };
      out.collections = config.collections.find({ dropped: { $ne: true } }, { _id: 1, key: 1, unique: 1 }).toArray();
      print(inspect(out));
    },
    help: () => print('sh: status(), listShards(), getBalancerState(), isBalancerRunning(), balancerCollectionStatus(ns),\n' +
      `  getShardedDataDistribution(), checkMetadataConsistency()\n  (changes to the cluster: ${WHY.admin})`),
  };
  for (const m of ADMIN_SH) sh[m] = unsupported(`sh.${m}`, WHY.admin);

  // Atlas Stream Processing is a different kind of instance.
  const sp = new Proxy({}, { get: (_, prop) => (typeof prop === 'symbol' ? undefined : unsupported(`sp.${prop}`, 'Atlas Stream Processing is not supported')) });

  function convertShardKeyToHashed(v) {
    const r = cursorCommand(null, { aggregate: 1, pipeline: [{ $documents: [{ v: v }] }, { $project: { _id: 0, h: { $toHashedIndexKey: '$v' } } }], cursor: {} });
    return r[0].h;
  }

  // ------------------------------------------------------------ native helpers

  function native(op, arg) {
    return global.__vareto_native(op, arg === undefined ? '' : String(arg));
  }

  const natives = {
    cat: (path) => native('cat', path),
    ls: (path) => JSON.parse(native('ls', path || '.')),
    listFiles: (path) => JSON.parse(native('listFiles', path || '.')),
    hostname: () => native('hostname'),
    getHostName: () => native('hostname'),
    pwd: () => native('pwd'),
    sleep: (ms) => { native('sleep', Math.max(0, Math.floor(Number(ms) || 0))); },
    version: () => native('version'),
    isInteractive: () => global.__vareto_interactive === true,
    passwordPrompt: () => native('passwordPrompt'),
    _isWindows: () => false,
    _rand: () => Math.random(),
    // Runs a script in the global scope, like mongosh's load().
    load: (path) => {
      (0, eval)(native('cat', path));
      return true;
    },
    mkdir: unsupported('mkdir', WHY.fs),
    removeFile: unsupported('removeFile', WHY.fs),
    md5sumFile: unsupported('md5sumFile', 'not available'),
    getMemInfo: unsupported('getMemInfo', 'not available'),
    connect: unsupported('connect', WHY.conn),
    Mongo: unsupported('Mongo', WHY.conn),
    Session: unsupported('Session', WHY.session),
    SessionOptions: unsupported('SessionOptions', WHY.session),
    getKeyVault: unsupported('getKeyVault', WHY.crypt),
    getClientEncryption: unsupported('getClientEncryption', WHY.crypt),
  };

  // ------------------------------------------------------------ shell commands

  const HELP = [
    '  Shell Help:',
    '    use <db>                 Set current database',
    '    show dbs | databases     Print a list of all available databases',
    '    show collections|tables  Print a list of all collections for current database',
    '    show users | roles       Print the users / roles of the current database',
    '    show profile             Print the five most recent operations over 1ms (system.profile)',
    '    show logs | log [name]   Print the server log names / the last entries of a log',
    '    it                       Show more results from the last cursor',
    '    cls                      Clear the screen (also console.clear(), Ctrl+L)',
    '    exit | quit              Quit the shell',
    '',
    '  db.help(), db.<collection>.help(), rs.help(), sh.help(), db.<collection>.getPlanCache().help()',
    '  Types: ObjectId, ISODate, Long (NumberLong), Int32 (NumberInt), Double, Decimal128 (NumberDecimal),',
    '    UUID, Binary (BinData, HexData, MD5), Timestamp, Code, DBRef, BSONRegExp, BSONSymbol, MinKey, MaxKey',
    '  Helpers: load(file), cat(file), ls([dir]), listFiles([dir]), pwd(), hostname(), sleep(ms), version(),',
    '    isInteractive(), passwordPrompt(), print, printjson, EJSON, util.inspect',
    "  Settings: config.get(key) / config.set(key, value) for displayBatchSize and loopIterationLimit",
  ].join('\n');

  const DB_HELP = [
    '  Database methods:',
    '    getName, getCollection, getCollectionNames, getCollectionInfos (listCollections), getSiblingDB, getMongo,',
    '    runCommand, adminCommand, aggregate, commandHelp, listCommands, stats, serverStatus, serverBuildInfo,',
    '    serverCmdLineOpts, hostInfo, version, hello, currentOp, killOp, getLogComponents, setLogLevel,',
    '    getProfilingStatus, setProfilingLevel, getReplicationInfo, printReplicationInfo,',
    '    printSecondaryReplicationInfo, printCollectionStats, printShardingStatus, checkMetadataConsistency,',
    '    createCollection, createView, dropDatabase,',
    '    createUser, updateUser, changeUserPassword, dropUser, dropAllUsers, getUser, getUsers, grantRolesToUser,',
    '    revokeRolesFromUser, createRole, updateRole, dropRole, dropAllRoles, getRole, getRoles,',
    '    grantPrivilegesToRole, revokePrivilegesFromRole, grantRolesToRole, revokeRolesFromRole',
  ].join('\n');

  // mongosh's `config` API, for the settings this shell has.
  const settings = {
    displayBatchSize: {
      get: () => BATCH,
      set: (v) => {
        if (!(Number.isInteger(v) && v > 0)) throw new RangeError('displayBatchSize must be a positive integer');
        BATCH = v;
      },
    },
    loopIterationLimit: { get: () => global.__vareto_loop_limit(), set: (v) => global.__vareto_loop_limit(v) },
  };
  function setting(key) {
    if (!Object.prototype.hasOwnProperty.call(settings, key)) {
      throw new Error(`unknown config option '${key}' (available: ${Object.keys(settings).join(', ')})`);
    }
    return settings[key];
  }
  const config = {
    get: (key) => setting(key).get(),
    set: (key, value) => {
      setting(key).set(value);
      return `Setting "${key}" has been changed`;
    },
  };

  const shell = {
    cursor: null,
    shown: 0,
    quitting: false,
    help() { print(HELP); print(DB_HELP); },
    use(name) {
      host({ kind: 'use', database: name });
      print(`switched to db ${name}`);
    },
    show(what, arg) {
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
        case 'roles':
          print(inspect(command(null, { rolesInfo: 1, showBuiltinRoles: true }).roles));
          break;
        case 'profile': {
          const ops = db.system.profile.find({ millis: { $gt: 0 } }).sort({ $natural: -1 }).limit(5).toArray();
          print(ops.length ? inspect(ops) : 'db.system.profile is empty\nUse db.setProfilingLevel(2) will enable profiling');
          break;
        }
        case 'logs':
          print(command('admin', { getLog: '*' }).names.join('\n'));
          break;
        case 'log':
          print(command('admin', { getLog: arg || 'global' }).log.join('\n'));
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
        if (v._explainVerbosity) {
          print(inspect(v.explain(v._explainVerbosity)));
          return;
        }
        this.cursor = v;
        this.shown = 0;
        this.more();
        return;
      }
      print(typeof v === 'string' ? v : inspect(v));
    },
    formatError(e) {
      // Calling something that isn't a method, which on db is usually a collection
      // (`db.listColections()`, `db.users.fnd()`): the engine doesn't say which.
      if (e instanceof TypeError && /not a callable function/.test(e.message)) {
        return `TypeError: ${e.message} (unknown names on db and on collections are collections, ` +
          'e.g. db.foo is the collection "foo"; type "help" for the methods)';
      }
      if (e instanceof Error) return `${e.name}: ${e.message}`;
      return `Uncaught ${inspect(e)}`;
    },
  };

  const db = dbProxy(new DB(null));
  Object.assign(global, natives, {
    db: db,
    rs: rs,
    sh: sh,
    sp: sp,
    __shell: shell,
    config: config,
    DBQuery: DBQuery,
    print: print,
    printjson: (v) => print(inspect(v)),
    printjsononeline: (v) => print(JSON.stringify(toEJSON(v))),
    tojson: (v) => inspect(v),
    console: { log: print, info: print, warn: print, error: print, clear: () => { native('clear'); } },
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
    HexData: HexData,
    MD5: MD5,
    UUID: UUID,
    Timestamp: Timestamp,
    Code: Code,
    DBRef: DBRef,
    BSONRegExp: BSONRegExp,
    BSONSymbol: BSONSymbol,
    MinKey: MinKey,
    MaxKey: MaxKey,
    ISODate: ISODate,
    WriteResult: WriteResult,
    BulkWriteResult: BulkWriteResult,
    MongoServerError: MongoServerError,
    MongoBulkWriteError: MongoBulkWriteError,
    convertShardKeyToHashed: convertShardKeyToHashed,
    EJSON: { stringify: (v) => JSON.stringify(toEJSON(v)), parse: (s) => fromEJSON(JSON.parse(s)), serialize: toEJSON, deserialize: fromEJSON },
    util: { inspect: inspect },
  });
})(globalThis);
