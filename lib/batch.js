const c = require('compact-encoding')
const b4a = require('b4a')
const binding = require('../binding')

const empty = b4a.alloc(0)
const emptyPromise = Promise.resolve()

class RocksDBBatch {
  constructor (db, opts = {}) {
    const {
      capacity = 8,
      encoding = null,
      keyEncoding = encoding,
      valueEncoding = encoding
    } = opts

    this._db = db
    this._capacity = capacity
    this._operations = []
    this._promises = []

    this._keyEncoding = keyEncoding
    this._valueEncoding = valueEncoding

    this._enqueuePromise = this._enqueuePromise.bind(this)

    this._request = null
    this._resolveRequest = null

    this._handle = null
    this._buffer = null

    if (db.opened === true) this.ready()
  }

  _onfinished () {
    const resolve = this._resolveRequest

    this._operations = []
    this._promises = []
    this._request = null
    this._resolveRequest = null

    if (resolve !== null) resolve()
  }

  _resize () {
    if (this._operations.length <= this._capacity) return false

    while (this._operations.length > this._capacity) {
      this._capacity *= 2
    }

    return true
  }

  async ready () {
    if (this._handle !== null) return

    if (this._db.opened === false) await this._db.ready()

    this._init()
  }

  flush () {
    if (this._request) throw new Error('Request already in progress')

    this._request = new Promise((resolve) => { this._resolveRequest = resolve })
    this._flush()

    return this._request
  }

  tryFlush () {
    if (this._request) throw new Error('Request already in progress')

    this._request = emptyPromise
    this._flush()
  }

  async _flush () {
    if (this._handle === null) await this.ready()
  }

  _enqueuePromise (resolve, reject) {
    this._promises.push({ resolve, reject })
  }

  _encodeKey (k) {
    if (this._keyEncoding) return c.encode(this._keyEncoding, k)
    if (typeof k === 'string') return b4a.from(k)
    return k
  }

  _encodeValue (v) {
    if (this._valueEncoding) return c.encode(this._valueEncoding, v)
    if (v === null) return empty
    if (typeof v === 'string') return b4a.from(v)
    return v
  }

  _decodeValue (b) {
    if (this._valueEncoding) return c.decode(this._valueEncoding, b)
    return b
  }
}

exports.ReadBatch = class RocksDBReadBatch extends RocksDBBatch {
  constructor (db, opts = {}) {
    const {
      snapshot = null
    } = opts

    super(db, opts)

    this._snapshot = snapshot
  }

  _init () {
    this._handle = binding.readInit()
    this._buffer = binding.readBuffer(this._handle, this._capacity)
  }

  _resize () {
    if (super._resize() && this._handle !== null) {
      this._buffer = binding.readBuffer(this._handle, this._capacity)
    }
  }

  async _flush () {
    await super._flush()

    binding.read(this._db._handle, this._handle, this._operations, this._snapshot ? this._snapshot._handle : null, this, this._onread)
  }

  _onread (errs, values) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      const err = errs[i]

      if (err) promise.reject(new Error(err))
      else promise.resolve(values[i].byteLength === 0 ? null : this._decodeValue(b4a.from(values[i])))
    }

    this._onfinished()
  }

  get (key) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBGet(this._encodeKey(key)))
    this._resize()

    return promise
  }
}

exports.WriteBatch = class RocksDBWriteBatch extends RocksDBBatch {
  _init () {
    this._handle = binding.writeInit()
    this._buffer = binding.writeBuffer(this._handle, this._capacity)
  }

  _resize () {
    if (super._resize() && this._handle !== null) {
      this._buffer = binding.writeBuffer(this._handle, this._capacity)
    }
  }

  async _flush () {
    await super._flush()

    binding.write(this._db._handle, this._handle, this._operations, this, this._onwrite)
  }

  _onwrite (err) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      if (err) promise.reject(new Error(err))
      else promise.resolve()
    }

    this._onfinished()
  }

  put (key, value) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBPut(this._encodeKey(key), this._encodeValue(value)))
    this._resize()

    return promise
  }

  tryPut (key, value) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(new RocksDBPut(this._encodeKey(key), this._encodeValue(value)))
    this._promises.push(null)
    this._resize()
  }

  delete (key) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBDelete(this._encodeKey(key)))
    this._resize()

    return promise
  }

  tryDelete (key) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(new RocksDBDelete(this._encodeKey(key)))
    this._promises.push(null)
    this._resize()
  }

  deleteRange (start, end) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBDeleteRange(this._encodeKey(start), this._encodeKey(end)))
    this._resize()

    return promise
  }

  tryDeleteRange (start, end) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(new RocksDBDeleteRange(this._encodeKey(start), this._encodeKey(end)))
    this._promises.push(null)
    this._resize()
  }
}

class RocksDBGet {
  constructor (key) {
    this.key = key
  }

  get type () {
    return binding.GET
  }
}

class RocksDBPut {
  constructor (key, value) {
    this.key = key
    this.value = value
  }

  get type () {
    return binding.PUT
  }
}

class RocksDBDelete {
  constructor (key) {
    this.key = key
  }

  get type () {
    return binding.DELETE
  }
}

class RocksDBDeleteRange {
  constructor (start, end) {
    this.start = start
    this.end = end
  }

  get type () {
    return binding.DELETE_RANGE
  }
}
