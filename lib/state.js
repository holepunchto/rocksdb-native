const ReadyResource = require('ready-resource')
const RefCounter = require('refcounter')
const ColumnFamily = require('./column-family')
const binding = require('../binding')

module.exports = class DBState extends ReadyResource {
  constructor(db, path, opts) {
    super()

    const {
      columnFamily = new ColumnFamily('default', opts),
      columnFamilies = [],
      readOnly = false,
      createIfMissing = true,
      createMissingColumnFamilies = true,
      maxBackgroundJobs = 6,
      bytesPerSync = 1048576
    } = opts

    this.path = path
    this.db = db
    this.refs = new RefCounter()
    this.sessionRefs = new RefCounter()
    this.sessions = []
    this.columnFamilies = [columnFamily]
    this.snapshots = new Set()

    this._suspending = null
    this._resuming = null
    this._columnsFlushed = false

    for (const col of columnFamilies) {
      this.columnFamilies.push(
        typeof col === 'string' ? new ColumnFamily(col, opts) : col
      )
    }

    this.handle = binding.init(
      Uint32Array.from([
        readOnly ? 1 : 0,
        createIfMissing ? 1 : 0,
        createMissingColumnFamilies ? 1 : 0,
        maxBackgroundJobs,
        bytesPerSync & 0xffffffff,
        Math.floor(bytesPerSync / 0x100000000)
      ])
    )
  }

  addSession(db) {
    this.sessionRefs.inc()
    db._index = this.sessions.push(db) - 1
  }

  removeSession(db) {
    this.sessionRefs.dec()
    const head = this.sessions.pop()
    if (head !== db) this.sessions[(head._index = db._index)] = head
  }

  ref() {
    if (this.closing) throw new Error('Database closed')
    this.refs.inc()
  }

  unref() {
    this.refs.dec()
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
      this.handle,
      this,
      this.path,
      this.columnFamilies.map((c) => c._handle),
      req,
      onopen
    )

    await promise

    for (const snapshot of this.snapshots) snapshot._init()

    function onopen(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async _close() {
    while (!this.refs.isIdle() || !this.sessionRefs.isIdle()) {
      await this.refs.idle()
      await this.sessionRefs.idle()
    }

    for (const columnFamily of this.columnFamilies) columnFamily.destroy()
    for (const snapshot of this.snapshots) snapshot.destroy()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.close(this.handle, req, onclose)

    await promise

    function onclose(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async suspend() {
    if (this._suspending === null) this._suspending = this._suspend()
    return this._suspending
  }

  async _suspend() {
    if (this._resuming) await this._resuming
    if (this.opened === false) await this.ready()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.suspend(this.handle, req, onsuspend)

    await promise

    this._suspending = null

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
    if (this._suspending) await this._suspending
    if (this.opened === false) await this.ready()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.resume(this.handle, req, onresume)

    await promise

    this._resuming = null

    function onresume(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }
}
