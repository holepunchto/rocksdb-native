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
    this._handle = db.opened === true ? binding.batchInit(db._handle, capacity, this) : null
  }

  _onfinished () {
    const resolve = this._resolveRequest

    this._keys = []
    this._values = []
    this._promises = []
    this._request = null
    this._resolveRequest = null

    if (resolve !== null) resolve()
  }

  _onread (errors, values) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      const err = errors[i]

      if (err) promise.reject(new Error(err))
      else promise.resolve(values[i].byteLength === 0 ? null : b4a.from(values[i]))
    }

    this._onfinished()
  }

  _onwrite (errors) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      const err = errors[i]

      if (err) promise.reject(new Error(err))
      else promise.resolve(this._values[i].byteLength === 0 ? null : this._values[i])
    }

    this._onfinished()
  }

  _resize () {
    if (this._keys.length <= this._capacity) return

    while (this._keys.length > this._capacity) {
      this._capacity *= 2
    }

    if (this._handle !== null) binding.batchResize(this._handle, this._capacity)
  }

  async ready () {
    if (this._db.opened === false) await this._db.ready()

    if (this._handle === null) this._handle = binding.batchInit(this._db._handle, this._capacity, this)
  }

  add (key, value = EMPTY) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._keys.push(this._encodeKey(key))
    this._values.push(this._encodeValue(value))
    this._resize()

    return promise
  }

  tryAdd (key, value = EMPTY) {
    if (this._request) throw new Error('Request already in progress')

    this._keys.push(this._encodeKey(key))
    this._values.push(this._encodeValue(value))
    this._promises.push(null)
    this._resize()
  }

  read () {
    if (this._request) throw new Error('Request already in progress')

    this._request = new Promise((resolve) => { this._resolveRequest = resolve })
    this._read()

    return this._request
  }

  tryRead () {
    if (this._request) throw new Error('Request already in progress')

    this._request = true
    this._read()
  }

  async _read () {
    if (this._handle === null) await this.ready()

    binding.batchRead(this._handle, this._keys, this._onread)
  }

  write () {
    if (this._request) throw new Error('Request already in progress')

    this._request = new Promise((resolve) => { this._resolveRequest = resolve })
    this._write()

    return this._request
  }

  tryWrite () {
    if (this._request) throw new Error('Request already in progress')

    this._request = true
    this._write()
  }

  async _write () {
    if (this._handle === null) await this.ready()

    binding.batchWrite(this._handle, this._keys, this._values, this._onwrite)
  }

  destroy () {
    if (this._destroying) return this._destroying

    this._destroying = this._destroy()

    return this._destroying
  }

  async _destroy () {
    if (this._handle === null) await this.ready()

    await this._request

    binding.batchDestroy(this._handle)
  }

  _enqueuePromise (resolve, reject) {
    this._promises.push({ resolve, reject })
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
