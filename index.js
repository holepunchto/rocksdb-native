const ReadyResource = require('ready-resource')
const binding = require('./binding')
const { ReadBatch, WriteBatch } = require('./lib/batch')
const Iterator = require('./lib/iterator')
const Snapshot = require('./lib/snapshot')
const ColumnFamily = require('./lib/column-family')

module.exports = exports = class RocksDB extends ReadyResource {
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
    this._onpreclose = null
    this._onidle = null
    this._suspending = null
    this._resuming = null
    this._idling = null

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
        this._onpreclose = resolve
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

  _ref() {
    if (this.closing !== null) {
      throw new Error('Database closed')
    }

    this._refs++
  }

  _unref() {
    if (--this._refs !== 0) return

    if (this._onidle !== null) {
      const resolve = this._onidle
      this._onidle = null
      this._idling = null
      resolve()
    }

    if (this._onpreclose !== null) {
      const resolve = this._onpreclose
      this._onpreclose = null
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

    if (!this._idling) {
      this._idling = new Promise((resolve) => {
        this._onidle = resolve
      })
    }

    return this._idling
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

exports.ColumnFamily = ColumnFamily
