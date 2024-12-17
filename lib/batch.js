const c = require('compact-encoding')
const binding = require('../binding')

const empty = Buffer.alloc(0)
const resolved = Promise.resolve()

class RocksDBBatch {
  constructor(db, opts = {}) {
    const {
      columnFamily = db.defaultColumnFamily,
      capacity = 8,
      encoding = null,
      keyEncoding = encoding,
      valueEncoding = encoding
    } = opts

    db._ref()

    this._db = db
    this._columnFamily = columnFamily
    this._destroyed = false
    this._capacity = capacity
    this._operations = []
    this._promises = []

    this._keyEncoding = keyEncoding
    this._valueEncoding = valueEncoding

    this._enqueuePromise = this._enqueuePromise.bind(this)

    this._request = null
    this._resolve = null

    this._handle = null
    this._buffer = null

    if (db.opened === true) this.ready()
  }

  _onfinished() {
    const resolve = this._resolve

    this._operations = []
    this._promises = []
    this._request = null
    this._resolve = null

    if (resolve !== null) resolve()
  }

  _resize() {
    if (this._operations.length <= this._capacity) return false

    while (this._operations.length > this._capacity) {
      this._capacity *= 2
    }

    return true
  }

  async ready() {
    if (this._handle !== null) return

    if (this._db.opened === false) await this._db.ready()

    this._init()
  }

  destroy() {
    if (this._request) throw new Error('Request in progress')
    if (this._destroyed) return

    this._destroyed = true

    this._db._unref()
  }

  flush() {
    if (this._request) throw new Error('Request in progress')
    if (this._destroyed) throw new Error('Batch is destroyed')

    this._request = new Promise((resolve) => {
      this._resolve = resolve
    })

    this._flush()

    return this._request
  }

  tryFlush() {
    if (this._request) throw new Error('Request in progress')

    this._request = resolved

    this._flush()
  }

  async _flush() {
    if (this._handle === null) await this.ready()
  }

  _enqueuePromise(resolve, reject) {
    this._promises.push({ resolve, reject })
  }

  _encodeKey(k) {
    if (this._keyEncoding) return c.encode(this._keyEncoding, k)
    if (typeof k === 'string') return Buffer.from(k)
    return k
  }

  _encodeValue(v) {
    if (this._valueEncoding) return c.encode(this._valueEncoding, v)
    if (v === null) return empty
    if (typeof v === 'string') return Buffer.from(v)
    return v
  }

  _decodeValue(b) {
    if (this._valueEncoding) return c.decode(this._valueEncoding, b)
    return b
  }
}

exports.ReadBatch = class RocksDBReadBatch extends RocksDBBatch {
  constructor(db, opts = {}) {
    const { snapshot = null } = opts

    super(db, opts)

    this._snapshot = snapshot
  }

  _init() {
    this._handle = binding.readInit()
    this._buffer = binding.readBuffer(this._handle, this._capacity)
  }

  _resize() {
    if (super._resize() && this._handle !== null) {
      this._buffer = binding.readBuffer(this._handle, this._capacity)
    }
  }

  async _flush() {
    await super._flush()

    binding.read(
      this._db._handle,
      this._handle,
      this._operations,
      this._snapshot ? this._snapshot._handle : null,
      this,
      this._onread
    )
  }

  _onread(errs, values) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      const err = errs[i]

      if (err) promise.reject(new Error(err))
      else
        promise.resolve(
          values[i].byteLength === 0
            ? null
            : this._decodeValue(Buffer.from(values[i]))
        )
    }

    this._onfinished()
  }

  get(key, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBGet(this._encodeKey(key), columnFamily))

    this._resize()

    return promise
  }
}

exports.WriteBatch = class RocksDBWriteBatch extends RocksDBBatch {
  _init() {
    this._handle = binding.writeInit()
    this._buffer = binding.writeBuffer(this._handle, this._capacity)
  }

  _resize() {
    if (super._resize() && this._handle !== null) {
      this._buffer = binding.writeBuffer(this._handle, this._capacity)
    }
  }

  async _flush() {
    await super._flush()

    binding.write(
      this._db._handle,
      this._handle,
      this._operations,
      this,
      this._onwrite
    )
  }

  _onwrite(err) {
    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      if (err) promise.reject(new Error(err))
      else promise.resolve()
    }

    this._onfinished()
  }

  put(key, value, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(
      new RocksDBPut(
        this._encodeKey(key),
        this._encodeValue(value),
        columnFamily
      )
    )

    this._resize()

    return promise
  }

  tryPut(key, value, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    this._operations.push(
      new RocksDBPut(
        this._encodeKey(key),
        this._encodeValue(value),
        columnFamily
      )
    )

    this._promises.push(null)

    this._resize()
  }

  delete(key, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBDelete(this._encodeKey(key), columnFamily))

    this._resize()

    return promise
  }

  tryDelete(key, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    this._operations.push(new RocksDBDelete(this._encodeKey(key), columnFamily))

    this._promises.push(null)

    this._resize()
  }

  deleteRange(start, end, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(
      new RocksDBDeleteRange(
        this._encodeKey(start),
        this._encodeKey(end),
        columnFamily
      )
    )

    this._resize()

    return promise
  }

  tryDeleteRange(start, end, opts = {}) {
    if (this._request) throw new Error('Request already in progress')

    const { columnFamily = this._columnFamily } = opts

    this._operations.push(
      new RocksDBDeleteRange(
        this._encodeKey(start),
        this._encodeKey(end),
        columnFamily
      )
    )

    this._promises.push(null)

    this._resize()
  }
}

class RocksDBGet {
  constructor(key, columnFamily) {
    this.key = key
    this.columnFamily = columnFamily._handle
  }

  get type() {
    return binding.GET
  }
}

class RocksDBPut {
  constructor(key, value, columnFamily) {
    this.key = key
    this.value = value
    this.columnFamily = columnFamily._handle
  }

  get type() {
    return binding.PUT
  }
}

class RocksDBDelete {
  constructor(key, columnFamily) {
    this.key = key
    this.columnFamily = columnFamily._handle
  }

  get type() {
    return binding.DELETE
  }
}

class RocksDBDeleteRange {
  constructor(start, end, columnFamily) {
    this.start = start
    this.end = end
    this.columnFamily = columnFamily._handle
  }

  get type() {
    return binding.DELETE_RANGE
  }
}
