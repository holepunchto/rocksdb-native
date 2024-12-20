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
    this._index = this._state.addSession(this)
  }

  get opened() {
    return this._state.opened
  }

  get closed() {
    return this.isRoot() ? this._state.closed : this._destroyed
  }

  get path() {
    return this._state.path
  }

  get defaultColumnFamily() {
    return this._columnFamily
  }

  session({
    columnFamily = this._columnFamily,
    snapshot = this._snapshot !== null
  } = {}) {
    let snap = null

    if (snapshot) {
      snap = this._snapshot
      if (snap === null) snap = new Snapshot(this._state)
      else snap.ref()
    }

    return new RocksDB(null, {
      state: this._state,
      columnFamily,
      snapshot: snap
    })
  }

  columnFamily(name) {
    return this.session({ columnFamily: name })
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
    if (this._index !== -1) {
      this._state.removeSession(this)
      this._index = -1
      if (this._snapshot) this._snapshot.unref()
    }

    if (force) {
      for (let i = this._state.sessions.length - 1; i >= 0; i--) {
        await this._state.sessions[i].close()
      }
    }

    return this.isRoot() ? this._state.close() : Promise.resolve()
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
