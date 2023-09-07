const ReadyResource = require('ready-resource')
const binding = require('./binding')

const DEFAULT_BUFFER_SIZE = 8
const EMPTY = Buffer.alloc(0)

class ReadBatch {
  constructor () {
    this.keys = []
    this.promises = []
    this._queuePromiseBound = this._queuePromise.bind(this)
  }

  _queuePromise (resolve, reject) {
    this.promises.push({ resolve, reject })
  }

  queue (key) {
    const promise = new Promise(this._queuePromiseBound)

    this.keys.push(key)

    return promise
  }
}

class WriteBatch {
  constructor () {
    this.keys = []
    this.values = []
    this.promises = []
    this._queuePromiseBound = this._queuePromise.bind(this)
  }

  _queuePromise (resolve, reject) {
    this.promises.push({ resolve, reject })
  }

  queue (key, value) {
    const promise = new Promise(this._queuePromiseBound)

    this.keys.push(key)
    this.values.push(value)

    return promise
  }
}

module.exports = class RocksDB extends ReadyResource {
  constructor (path, {
    // default options based on https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
    readOnly = false,
    readonly = false, // alias
    createIfMissing = true,
    maxBackgroundJobs = 6,
    bytesPerSync = 1048576,
    compactionStyle = 3, // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/advanced_options.h#L41
    // (block) table options
    tableBlockSize = 16384,
    tableCacheIndexAndFilterBlocks = true,
    tablePinL0FilterAndIndexBlocksInCache = true,
    tableFormatVersion = 4,
    // blob options, https://github.com/facebook/rocksdb/wiki/BlobDB
    enableBlobFiles = false,
    minBlobSize = 0,
    blobFileSize = 0,
    enableBlobGarbageCollection = true
  } = {}) {
    super()

    this.path = path

    this.readOnly = readonly || readOnly
    this.createIfMissing = createIfMissing
    this.maxBackgroundJobs = maxBackgroundJobs
    this.bytesPerSync = bytesPerSync
    this.compactionStyle = compactionStyle

    this.tableBlockSize = tableBlockSize
    this.tableCacheIndexAndFilterBlocks = tableCacheIndexAndFilterBlocks
    this.tablePinL0FilterAndIndexBlocksInCache = tablePinL0FilterAndIndexBlocksInCache
    this.tableFormatVersion = tableFormatVersion

    this.enableBlobFiles = enableBlobFiles
    this.minBlobSize = minBlobSize
    this.blobFileSize = blobFileSize
    this.enableBlobGarbageCollection = enableBlobGarbageCollection

    this._handle = Buffer.allocUnsafe(binding.sizeof_rocksdb_native_t)
    this._status = new Int32Array(this._handle.buffer, this._handle.byteOffset + binding.offsetof_rocksdb_native_t_status, 1)
    this._readBufferSize = DEFAULT_BUFFER_SIZE
    this._writeBufferSize = DEFAULT_BUFFER_SIZE
    this._statusResolve = null
    this._flushes = []

    this._nextReads = new ReadBatch()
    this._nextWrites = new WriteBatch()

    this._reads = null
    this._writes = null
  }

  async _open () {
    binding.rocksdb_native_init(this._handle, DEFAULT_BUFFER_SIZE, this, this._onstatus, this._onbatch)

    const opening = new Promise(resolve => { this._statusResolve = resolve })

    const opts = new Uint32Array(14)

    opts[0] = this.readOnly ? 1 : 0
    opts[1] = this.createIfMissing ? 1 : 0
    opts[2] = this.maxBackgroundJobs
    opts[3] = this.bytesPerSync
    opts[4] = this.compactionStyle
    opts[5] = this.tableBlockSize
    opts[6] = this.tableCacheIndexAndFilterBlocks ? 1 : 0
    opts[7] = this.tablePinL0FilterAndIndexBlocksInCache ? 1 : 0
    opts[8] = this.tableFormatVersion
    opts[9] = this.enableBlobFiles ? 1 : 0
    opts[10] = this.minBlobSize
    opts[11] = this.blobFileSize & 0xffffffff
    opts[12] = Math.floor(this.blobFileSize / 0x100000000)
    opts[13] = this.enableBlobGarbageCollection ? 1 : 0

    binding.rocksdb_native_open(this._handle, this.path, opts)

    const err = await opening
    if (err) throw new Error(err)
  }

  async _close () {
    await this.flush()

    const closing = new Promise(resolve => { this._statusResolve = resolve })

    binding.rocksdb_native_close(this._handle)

    const err = await closing
    if (err) throw new Error(err)
  }

  _ensureReadBuffer (size) {
    if (size <= this._readBufferSize) return
    while (size > this._readBufferSize) this._readBufferSize *= 2
    binding.rocksdb_native_set_read_buffer_size(this._handle, this._readBufferSize)
  }

  _ensureWriteBuffer (size) {
    if (size <= this._writeBufferSize) return
    while (size > this._writeBufferSize) this._writeBufferSize *= 2
    binding.rocksdb_native_set_write_buffer_size(this._handle, this._writeBufferSize)
  }

  _onstatus (err) {
    const resolve = this._statusResolve
    this._statusResolve = null
    resolve(err)
  }

  _onbatch (getValues) {
    for (let i = 0; i < this._writes.promises.length; i++) {
      const { resolve } = this._writes.promises[i]
      resolve()
    }

    for (let i = 0; i < this._reads.promises.length; i++) {
      const { resolve } = this._reads.promises[i]
      const buffer = getValues[i]
      resolve(buffer === null ? null : Buffer.from(buffer, 0, buffer.byteLength))
    }

    this._reads = this._writes = null

    while (this._flushes.length > 0) this._flushes.shift()()
  }

  _encodeKey (k) {
    if (typeof k === 'string') return Buffer.from(k)
    return k
  }

  _encodeValue (v) {
    if (typeof v === 'string') return Buffer.from(v)
    return v
  }

  _flushMaybe () {
    if (this._reads !== null || this._writes !== null) return

    this._ensureReadBuffer(this._nextReads.keys.length)
    this._ensureWriteBuffer(this._nextWrites.keys.length)

    this._reads = this._nextReads
    this._writes = this._nextWrites

    binding.rocksdb_native_batch(this._handle, this._reads.keys, this._writes.keys, this._writes.values)

    if (this._reads.keys.length) {
      this._nextReads = new ReadBatch()
    }
    if (this._writes.keys.length) {
      this._nextWrites = new WriteBatch()
    }
  }

  flush () {
    for (let i = 0; i < this._nextReads.promises.length; i++) {
      this._nextReads.promises[i].reject(new Error('DB is closed'))
    }
    for (let i = 0; i < this._nextWrites.promises.length; i++) {
      this._nextWrites.promises[i].reject(new Error('DB is closed'))
    }

    if (this._reads === null) return
    return new Promise(resolve => { this._flushes.push(resolve) })
  }

  async batch (ops) {
    if (this.opened === false) await this.ready()
    if (this.closing) throw new Error('DB is closed')
    if (this.readOnly === true) throw new Error('DB is readOnly')

    const all = new Array(ops.length)

    for (let i = 0; i < ops.length; i++) {
      const { key, value } = ops[i]
      all[i] = this._nextWrites.queue(this._encodeKey(key), this._encodeValue(value || EMPTY))
    }

    this._flushMaybe()
    return Promise.all(all)
  }

  async put (key, value) {
    if (this.opened === false) await this.ready()
    if (this.closing) throw new Error('DB is closed')
    if (this.readOnly === true) throw new Error('DB is readOnly')

    const prom = this._nextWrites.queue(this._encodeKey(key), this._encodeValue(value))
    this._flushMaybe()
    return prom
  }

  async del (key) {
    if (this.opened === false) await this.ready()
    if (this.closing) throw new Error('DB is closed')
    if (this.readOnly === true) throw new Error('DB is readOnly')

    const prom = this._nextWrites.queue(this._encodeKey(key), EMPTY)
    this._flushMaybe()
    return prom
  }

  async getBatch (keys) {
    if (this.opened === false) await this.ready()
    if (this.closing) throw new Error('DB is closed')

    const all = new Array(keys.length)

    for (let i = 0; i < keys.length; i++) {
      all[i] = this._nextReads.queue(this._encodeKey(keys[i]))
    }

    this._flushMaybe()
    return Promise.all(all)
  }

  async get (key) {
    if (this.opened === false) await this.ready()
    if (this.closing) throw new Error('DB is closed')

    const prom = this._nextReads.queue(this._encodeKey(key))
    this._flushMaybe()
    return prom
  }
}
