const ReadyResource = require('ready-resource')
const RefCounter = require('refcounter')
const rrp = require('resolve-reject-promise')
const { ReadBatch, WriteBatch } = require('./batch')
const ColumnFamily = require('./column-family')
const binding = require('../binding')

const MAX_BATCH_REUSE = 64

module.exports = class RocksDBState extends ReadyResource {
  constructor(db, path, opts) {
    super()

    const {
      columnFamily = new ColumnFamily('default', opts),
      columnFamilies = [],
      readOnly = false,
      createIfMissing = true,
      createMissingColumnFamilies = true,
      maxBackgroundJobs = 6,
      bytesPerSync = 1048576,
      maxOpenFiles = -1,
      useDirectReads = false
    } = opts

    this.path = path
    this.db = db
    this.handles = new RefCounter()
    this.io = new RefCounter()
    this.sessions = []
    this.columnFamilies = [columnFamily]
    this.deferSnapshotInit = true
    this.resumed = null

    this._suspended = false
    this._suspending = null
    this._resuming = null
    this._columnsFlushed = false
    this._readBatches = []
    this._writeBatches = []

    for (const columnFamily of columnFamilies) {
      this.columnFamilies.push(
        typeof columnFamily === 'string'
          ? new ColumnFamily(columnFamily, opts)
          : columnFamily
      )
    }

    this._handle = binding.init(
      readOnly,
      createIfMissing,
      createMissingColumnFamilies,
      maxBackgroundJobs,
      bytesPerSync,
      maxOpenFiles,
      useDirectReads
    )
  }

  createReadBatch(db, opts) {
    if (this._readBatches.length === 0) return new ReadBatch(db, opts)
    const batch = this._readBatches.pop()
    batch._reuse(db, opts)
    return batch
  }

  createWriteBatch(db, opts) {
    if (this._writeBatches.length === 0) return new WriteBatch(db, opts)
    const batch = this._writeBatches.pop()
    batch._reuse(db, opts)
    return batch
  }

  freeBatch(batch, writable) {
    const queue = writable ? this._writeBatches : this._readBatches
    if (queue.length >= MAX_BATCH_REUSE) return
    queue.push(batch)
  }

  addSession(db) {
    db._index = this.sessions.push(db) - 1
    if (db._snapshot) db._snapshot.ref()
  }

  removeSession(db) {
    const head = this.sessions.pop()
    if (head !== db) this.sessions[(head._index = db._index)] = head
    db._index = -1
    if (db._snapshot) db._snapshot.unref()
  }

  upsertColumnFamily(c) {
    if (typeof c === 'string') {
      let col = this.getColumnFamilyByName(c)
      if (col) return col
      col = this.columnFamilies[0].cloneSettings(c)
      this.columnFamilies.push(col)
      return col
    }

    if (this.columnFamilies.includes(c)) return c
    this.columnFamilies.push(c)
    return c
  }

  getColumnFamily(c) {
    if (!c) return this.columnFamilies[0]
    if (!this._columnsFlushed) return this.upsertColumnFamily(c)

    if (typeof c !== 'string') return c

    const col = this.getColumnFamilyByName(c)
    if (col === null) throw new Error('Unknown column family')
    return col
  }

  getColumnFamilyByName(name) {
    for (const col of this.columnFamilies) {
      if (col.name === name) return col
    }
    return null
  }

  async _open() {
    await Promise.resolve() // allow column families to populate if ondemand

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    this._columnsFlushed = true

    req.handle = binding.open(
      this._handle,
      this,
      this.path,
      this.columnFamilies.map((c) => c._handle),
      req,
      onopen
    )

    await promise

    this.deferSnapshotInit = false

    for (const session of this.sessions) {
      if (session._snapshot) session._snapshot._init()
    }

    function onopen(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async _close() {
    if (this.resumed) {
      const resumed = this.resumed
      this.resumed = null
      resumed.resolve(false)
    }

    while (!this.handles.isIdle()) await this.handles.idle()

    while (this.sessions.length > 0) {
      await this.sessions[this.sessions.length - 1].close()
    }

    for (const columnFamily of this.columnFamilies) columnFamily.destroy()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.close(this._handle, req, onclose)

    await promise

    function onclose(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async flush(db, opts) {
    if (this.opened === false) await this.ready()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.flush(
      this._handle,
      db._columnFamily._handle,
      req,
      onflush
    )

    await promise

    function onflush(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async suspend() {
    if (this._suspending === null) this._suspending = this._suspend()
    return this._suspending
  }

  async _suspend() {
    if (this.opened === false) await this.ready()
    if (this._resuming !== null) await this._resuming
    if (this._suspended === true) return

    while (!this.io.isIdle()) await this.io.idle()

    this.resumed = rrp()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.suspend(this._handle, req, onsuspend)

    try {
      await promise

      this._suspended = true
    } finally {
      this._suspending = null
    }

    function onsuspend(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  resume() {
    if (this._resuming === null) this._resuming = this._resume()
    return this._resuming
  }

  async _resume() {
    if (this.opened === false) await this.ready()
    if (this._suspending !== null) await this._suspending
    if (this._suspended === false) return

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.resume(this._handle, req, onresume)

    try {
      await promise

      this._suspended = false
    } finally {
      this._resuming = null
    }

    const resumed = this.resumed
    this.resumed = null
    resumed.resolve(true)

    function onresume(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }
}
