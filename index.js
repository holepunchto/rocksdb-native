/* global Bare */
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const binding = require('./binding')

const DEFAULT_BATCH_CAPACITY = 8
const EMPTY = b4a.alloc(0)

class Batch {
  constructor (db, capacity) {
    this._db = db
    this._capacity = capacity
    this._keys = []
    this._values = []
    this._promises = []
    this._enqueuePromise = this._enqueuePromise.bind(this)
    this._request = null
    this._resolveRequest = null
    this._destroying = null
    this._handle = db.opened === true ? binding.batchInit(capacity, this) : null
  }

  _onfinished () {
    const resolve = this._resolveRequest

    this._keys = []
    this._values = []
    this._promises = []
    this._request = null
    this._resolveRequest = null

    resolve()
  }

  _onread (errors, values) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]

      const err = errors[i]

      if (err) promise.reject(new Error(err))
      else promise.resolve(values[i].byteLength === 0 ? null : b4a.from(values[i]))
    }

    this._onfinished()
  }

  _onwrite (errors) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]

      const err = errors[i]

      if (err) promise.reject(new Error(err))
      else promise.resolve(this._values[i].byteLength === 0 ? null : this._values[i])
    }

    this._onfinished()
  }

  add (key, value = EMPTY) {
    if (this._request) throw new Error('Request already in progress')
    if (this._db.opened === false) return this._openAndAdd(key, value)

    const promise = new Promise(this._enqueuePromise)

    this._keys.push(this._encodeKey(key))
    this._values.push(this._encodeValue(value))
    this._resize()

    return promise
  }

  read () {
    if (this._request) throw new Error('Request already in progress')
    if (this._db.opened === false) return this._openAndRead()

    this._request = new Promise((resolve) => { this._resolveRequest = resolve })

    binding.read(this._db._handle, this._handle, this._keys, this._onread)

    return this._request
  }

  write () {
    if (this._request) throw new Error('Request already in progress')
    if (this._db.opened === false) return this._openAndWrite()

    this._request = new Promise((resolve) => { this._resolveRequest = resolve })

    binding.write(this._db._handle, this._handle, this._keys, this._values, this._onwrite)

    return this._request
  }

  async _destroy () {
    if (this._db.opened === false) await this._db.ready()
    await this._request

    binding.batchDestroy(this._handle)
  }

  destroy () {
    if (this._destroying) return this._destroying
    this._destroying = this._destroy()
    return this._destroying
  }

  async _setHandle () {
    await this._db.ready()
    if (this._handle === null) this._handle = binding.batchInit(this._capacity, this)
  }

  async _openAndAdd (key, value) {
    await this._setHandle()
    return this.add(key, value)
  }

  async _openAndRead () {
    await this._setHandle()
    return this.read()
  }

  async _openAndWrite () {
    await this._setHandle()
    return this.write()
  }

  _enqueuePromise (resolve, reject) {
    this._promises.push({ resolve, reject })
  }

  _resize () {
    if (this._keys.length <= this._capacity) return

    while (this._keys.length > this._capacity) {
      this._capacity *= 2
    }

    binding.batchResize(this._handle, this._capacity)
  }

  _encodeKey (k) {
    if (typeof k === 'string') return Buffer.from(k)
    return k
  }

  _encodeValue (v) {
    if (v === null) return EMPTY
    if (typeof v === 'string') return Buffer.from(v)
    return v
  }
}

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

    return promise

    function onopen (err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  async _close () {
    const req = { resolve: null, reject: null, handle: null }

    const promise = new Promise((resolve, reject) => {
      req.resolve = resolve
      req.reject = reject
    })

    req.handle = binding.close(this._handle, req, onclose)

    RocksDB._instances.delete(this)

    return promise

    function onclose (err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  batch ({ capacity = DEFAULT_BATCH_CAPACITY } = {}) {
    return new Batch(this, capacity)
  }

  static _instances = new Set()
}

Bare.on('exit', async () => {
  for (const db of RocksDB._instances) {
    await db.close()
  }
})
