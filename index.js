/* global Bare */
const ReadyResource = require('ready-resource')
const binding = require('./binding')
const { ReadBatch, WriteBatch } = require('./lib/batch')
const Iterator = require('./lib/iterator')
const Snapshot = require('./lib/snapshot')

const RocksDB = module.exports = class RocksDB extends ReadyResource {
  constructor (path, {
    // default options, https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
    readOnly = false,
    createIfMissing = true,
    maxBackgroundJobs = 6,
    bytesPerSync = 1048576,
    // blob options, https://github.com/facebook/rocksdb/wiki/BlobDB
    enableBlobFiles = false,
    minBlobSize = 0,
    blobFileSize = 0,
    enableBlobGarbageCollection = true,
    // (block) table options
    tableBlockSize = 16384,
    tableCacheIndexAndFilterBlocks = true,
    tableFormatVersion = 4
  } = {}) {
    super()

    this.path = path

    this.readOnly = readOnly
    this.createIfMissing = createIfMissing
    this.maxBackgroundJobs = maxBackgroundJobs
    this.bytesPerSync = bytesPerSync

    this.enableBlobFiles = enableBlobFiles
    this.minBlobSize = minBlobSize
    this.blobFileSize = blobFileSize
    this.enableBlobGarbageCollection = enableBlobGarbageCollection

    this.tableBlockSize = tableBlockSize
    this.tableCacheIndexAndFilterBlocks = tableCacheIndexAndFilterBlocks
    this.tableFormatVersion = tableFormatVersion

    this._snapshots = new Set()
    this._refs = 0
    this._resolvePreclose = null
    this._resolveOnIdle = null
    this._idlePromise = null

    this._handle = binding.init()
  }

  async _open () {
    const opts = new Uint32Array(16)

    opts[0] = this.readOnly ? 1 : 0
    opts[1] = this.createIfMissing ? 1 : 0
    opts[2] = this.maxBackgroundJobs
    opts[3] = this.bytesPerSync & 0xffffffff
    opts[4] = Math.floor(this.bytesPerSync / 0x100000000)
    opts[5] = 0
    opts[6] = this.enableBlobFiles ? 1 : 0
    opts[7] = this.minBlobSize & 0xffffffff
    opts[8] = Math.floor(this.minBlobSize / 0x100000000)
    opts[9] = this.blobFileSize & 0xffffffff
    opts[10] = Math.floor(this.blobFileSize / 0x100000000)
    opts[11] = this.enableBlobGarbageCollection ? 1 : 0
    opts[12] = this.tableBlockSize & 0xffffffff
    opts[13] = Math.floor(this.tableBlockSize / 0x100000000)
    opts[14] = this.tableCacheIndexAndFilterBlocks ? 1 : 0
    opts[15] = this.tableFormatVersion

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.open(this._handle, this.path, opts, req, onopen)

    RocksDB._instances.add(this)

    await promise

    for (const snapshot of this._snapshots) snapshot._init()

    function onopen (err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async _close () {
    if (this._refs > 0) {
      await new Promise((resolve) => {
        this._resolvePreclose = resolve
      })
    }

    for (const snapshot of this._snapshots) snapshot.destroy()

    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.close(this._handle, req, onclose)

    RocksDB._instances.delete(this)

    await promise

    function onclose (err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  _incRef () {
    if (this.closing !== null) {
      throw new Error('Database closed')
    }

    this._refs++
  }

  _decRef () {
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

  isIdle () {
    return this._refs === 0
  }

  idle () {
    if (this.isIdle()) return Promise.resolve()

    if (!this._idlePromise) {
      this._idlePromise = new Promise(resolve => { this._resolveOnIdle = resolve })
    }

    return this._idlePromise
  }

  snapshot (opts) {
    return new Snapshot(this, opts)
  }

  iterator (range, opts) {
    return new Iterator(this, { ...range, ...opts })
  }

  async peek (range, opts) {
    for await (const value of this.iterator({ ...range, ...opts, limit: 1 })) { // eslint-disable-line no-unreachable-loop
      return value
    }

    return null
  }

  read (opts) {
    return new ReadBatch(this, opts)
  }

  write (opts) {
    return new WriteBatch(this, opts)
  }

  async get (key, opts) {
    const batch = this.read(opts)
    const value = batch.get(key)
    await batch.flush()
    return value
  }

  async put (key, value, opts) {
    const batch = this.write(opts)
    batch.put(key, value)
    await batch.flush()
  }

  async delete (key, opts) {
    const batch = this.write(opts)
    batch.delete(key)
    await batch.flush()
  }

  async deleteRange (start, end, opts) {
    const batch = this.write(opts)
    batch.deleteRange(start, end)
    await batch.flush()
  }

  static _instances = new Set()
}

if (typeof Bare !== 'undefined') {
  Bare.on('exit', async () => {
    for (const db of RocksDB._instances) {
      await db.close()
    }
  })
}
