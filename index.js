const { ReadBatch, WriteBatch } = require('./lib/batch')
const ColumnFamily = require('./lib/column-family')
const Iterator = require('./lib/iterator')
const Snapshot = require('./lib/snapshot')
const DBState = require('./lib/state')

class RocksDB {
  constructor(path, opts = {}) {
    const {
      columnFamily,
      state = new DBState(this, path, opts),
      snapshot = null
    } = opts

    this._state = state
    this._snapshot = snapshot
    this._columnFamily = state.getColumnFamily(columnFamily)
    this._destroyed = false

    this._state.refSession()
  }

  get opened() {
    return this._state.opened
  }

  get closed() {
    return this._state.clsoed
  }

  get path() {
    return this._state.path
  }

  get defaultColumnFamily() {
    return this._columnFamily
  }

  session({ columnFamily = this._columnFamily } = {}) {
    return new RocksDB(null, {
      state: this._state,
      columnFamily,
      snapshot: null
    })
  }

  columnFamily(name) {
    return this.session({ columnFamily: name })
  }

  snapshot() {
    let snapshot = this._snapshot

    if (snapshot === null) snapshot = new Snapshot(this._state)
    else snapshot.ref()

    return new RocksDB(null, {
      state: this._state,
      columnFamily: this._columnFamily,
      snapshot
    })
  }

  isRoot() {
    return this === this._state.db
  }

  ready() {
    return this._state.ready()
  }

  close() {
    if (!this._destroyed) {
      this._destroyed = true
      this._state.unrefSession()
      if (this._snapshot) this._snapshot.unref()
    }

    if (this.isRoot()) return this._state.close()
    return Promise.resolve()
  }

  suspend() {
    return this._state.suspend()
  }

  resume() {
    return this._state.resume()
  }

  isIdle() {
    return this._state.refs.isIdle()
  }

  idle() {
    return this._state.refs.idle()
  }

  iterator(range, opts) {
    return new Iterator(this, { ...range, ...opts })
  }

  async peek(range, opts) {
    for await (const value of this.iterator({ ...range, ...opts, limit: 1 })) {
      return value
    }

    return null
  }

  read(opts) {
    return new ReadBatch(this, opts)
  }

  write(opts) {
    return new WriteBatch(this, opts)
  }

  async get(key, opts) {
    const batch = this.read({ ...opts, capacity: 1 })
    try {
      const value = batch.get(key)
      batch.tryFlush()
      return await value
    } finally {
      batch.destroy()
    }
  }

  async put(key, value, opts) {
    const batch = this.write({ ...opts, capacity: 1 })
    try {
      batch.tryPut(key, value)
      await batch.flush()
    } finally {
      batch.destroy()
    }
  }

  async delete(key, opts) {
    const batch = this.write({ ...opts, capacity: 1 })
    try {
      batch.tryDelete(key)
      await batch.flush()
    } finally {
      batch.destroy()
    }
  }

  async deleteRange(start, end, opts) {
    const batch = this.write({ ...opts, capacity: 1 })
    try {
      batch.tryDeleteRange(start, end)
      await batch.flush()
    } finally {
      batch.destroy()
    }
  }
}

module.exports = exports = RocksDB
exports.ColumnFamily = ColumnFamily
