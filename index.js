const ReadyResource = require('ready-resource')
const binding = require('./binding')
const { ReadBatch, WriteBatch } = require('./lib/batch')
const Iterator = require('./lib/iterator')
const Snapshot = require('./lib/snapshot')
const ColumnFamily = require('./lib/column-family')

module.exports = class RocksDB extends ReadyResource {
  constructor(path, opts = {}) {
    const defaultColumnFamily = new ColumnFamily('default', opts)

    const {
      columnFamilies = [],
      readOnly = false,
      createIfMissing = true,
      createMissingColumnFamilies = true,
      maxBackgroundJobs = 6,
      bytesPerSync = 1048576
    } = opts

    super()

    this._path = path
    this._columnFamilies = [defaultColumnFamily, ...columnFamilies]
    this._snapshots = new Set()
    this._refs = 0
    this._resolvePreclose = null
    this._resolveOnIdle = null
    this._suspending = null
    this._resuming = null
    this._idlePromise = null

    this._handle = binding.init(
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

  get path() {
    return this._path
  }

  get defaultColumnFamily() {
    return this._columnFamilies[0]
  }

  async _open() {
    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.open(
      this._handle,
      this,
      this._path,
      this._columnFamilies.map((c) => c._handle),
      req,
      onopen
    )

    await promise

    for (const snapshot of this._snapshots) snapshot._init()

    function onopen(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async _close() {
    if (this._refs > 0) {
      await new Promise((resolve) => {
        this._resolvePreclose = resolve
      })
    }

    for (const columnFamily of this._columnFamilies) columnFamily.destroy()
    for (const snapshot of this._snapshots) snapshot.destroy()

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

  _incRef() {
    if (this.closing !== null) {
      throw new Error('Database closed')
    }

    this._refs++
  }

  _decRef() {
    if (--this._refs !== 0) return

    if (this._resolveOnIdle !== null) {
      const resolve = this._resolveOnIdle
      this._resolveOnIdle = null
      this._idlePromise = null
      resolve()
    }

    if (this._resolvePreclose !== null) {
      const resolve = this._resolvePreclose
      this._resolvePreclose = null
      resolve()
    }
  }

  async suspend() {
    if (this.opened === false) await this.ready()
    if (this._suspending === null) this._suspending = this._suspend()
    return this._suspending
  }

  async _suspend() {
    if (this._resuming) await this._resuming

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.suspend(this._handle, req, onsuspend)

    await promise

    this._suspending = null

    function onsuspend(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async resume() {
    if (this.opened === false) await this.ready()
    if (this._resuming === null) this._resuming = this._resume()
    return this._resuming
  }

  async _resume() {
    if (this._suspending) await this._suspending

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.resume(this._handle, req, onresume)

    await promise

    this._resuming = null

    function onresume(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  isIdle() {
    return this._refs === 0
  }

  idle() {
    if (this.isIdle()) return Promise.resolve()

    if (!this._idlePromise) {
      this._idlePromise = new Promise((resolve) => {
        this._resolveOnIdle = resolve
      })
    }

    return this._idlePromise
  }

  snapshot(opts) {
    return new Snapshot(this, opts)
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
    const batch = this.read(opts)
    const value = batch.get(key)
    await batch.flush()
    return value
  }

  async put(key, value, opts) {
    const batch = this.write(opts)
    batch.put(key, value)
    await batch.flush()
  }

  async delete(key, opts) {
    const batch = this.write(opts)
    batch.delete(key)
    await batch.flush()
  }

  async deleteRange(start, end, opts) {
    const batch = this.write(opts)
    batch.deleteRange(start, end)
    await batch.flush()
  }
}
