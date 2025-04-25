const ColumnFamily = require('./lib/column-family')
const Iterator = require('./lib/iterator')
const Snapshot = require('./lib/snapshot')
const State = require('./lib/state')
const { BloomFilterPolicy, RibbonFilterPolicy } = require('./lib/filter-policy')

class RocksDB {
  constructor(path, opts = {}) {
    const {
      columnFamily,
      state = new State(this, path, opts),
      snapshot = null,
      keyEncoding = null,
      valueEncoding = null
    } = opts

    this._state = state
    this._snapshot = snapshot
    this._columnFamily = state.getColumnFamily(columnFamily)
    this._keyEncoding = keyEncoding
    this._valueEncoding = valueEncoding
    this._index = -1

    this._state.addSession(this)
  }

  get opened() {
    return this._state.opened
  }

  get closed() {
    return this.isRoot() ? this._state.closed : this._index === -1
  }

  get path() {
    return this._state.path
  }

  get snapshotted() {
    return this._snapshot !== null
  }

  get defaultColumnFamily() {
    return this._columnFamily
  }

  session({
    columnFamily = this._columnFamily,
    snapshot = this._snapshot !== null,
    keyEncoding = this._keyEncoding,
    valueEncoding = this._valueEncoding
  } = {}) {
    maybeClosed(this)

    return new RocksDB(null, {
      state: this._state,
      columnFamily,
      snapshot: snapshot ? this._snapshot || new Snapshot(this._state) : null,
      keyEncoding,
      valueEncoding
    })
  }

  columnFamily(name, opts) {
    return this.session({ ...opts, columnFamily: name })
  }

  snapshot() {
    return this.session({ snapshot: true })
  }

  isRoot() {
    return this === this._state.db
  }

  ready() {
    return this._state.ready()
  }

  async close({ force } = {}) {
    if (this._index !== -1) this._state.removeSession(this)

    if (force) {
      while (this._state.sessions.length > 0) {
        await this._state.sessions[this._state.sessions.length - 1].close()
      }
    }

    return this.isRoot() ? this._state.close() : Promise.resolve()
  }

  suspend() {
    maybeClosed(this)

    return this._state.suspend()
  }

  resume() {
    maybeClosed(this)

    return this._state.resume()
  }

  isIdle() {
    return this._state.handles.isIdle()
  }

  idle() {
    return this._state.handles.idle()
  }

  iterator(range, opts) {
    maybeClosed(this)

    return new Iterator(this, false, { ...range, ...opts })
  }

  keyIterator(range, opts) {
    maybeClosed(this)

    return new Iterator(this, true, { ...range, ...opts })
  }

  async peek(range, opts) {
    for await (const value of this.iterator({ ...range, ...opts, limit: 1 })) {
      return value
    }

    return null
  }

  read(opts) {
    maybeClosed(this)

    return this._state.createReadBatch(this, opts)
  }

  write(opts) {
    maybeClosed(this)

    return this._state.createWriteBatch(this, opts)
  }

  flush(opts) {
    maybeClosed(this)

    return this._state.flush(this, opts)
  }

  async get(key, opts) {
    const batch = this.read({ ...opts, capacity: 1, autoDestroy: true })
    const value = batch.get(key)
    batch.tryFlush()
    return value
  }

  async put(key, value, opts) {
    const batch = this.write({ ...opts, capacity: 1, autoDestroy: true })
    batch.tryPut(key, value)
    await batch.flush()
  }

  async delete(key, opts) {
    const batch = this.write({ ...opts, capacity: 1, autoDestroy: true })
    batch.tryDelete(key)
    await batch.flush()
  }

  async deleteRange(start, end, opts) {
    const batch = this.write({ ...opts, capacity: 1, autoDestroy: true })
    batch.tryDeleteRange(start, end)
    await batch.flush()
  }

  _ref() {
    if (this._snapshot) this._snapshot.ref()
    this._state.handles.inc()
  }

  _unref() {
    if (this._snapshot) this._snapshot.unref()
    this._state.handles.dec()
  }
}

module.exports = exports = RocksDB

exports.ColumnFamily = ColumnFamily
exports.BloomFilterPolicy = BloomFilterPolicy
exports.RibbonFilterPolicy = RibbonFilterPolicy

function maybeClosed(db) {
  if (db._state.closing || db._index === -1)
    throw new Error('RocksDB session is closed')
}
