const c = require('compact-encoding')
const binding = require('../binding')

const empty = Buffer.alloc(0)
const resolved = Promise.resolve()

class RocksDBBatch {
  constructor(db, opts = {}) {
    const { capacity = 8, autoDestroy = false } = opts

    db._ref()

    this._db = db
    this._destroyed = false
    this._capacity = capacity
    this._operations = []
    this._promises = []

    this._enqueuePromise = this._enqueuePromise.bind(this)

    this._request = null
    this._resolve = null
    this._reject = null

    this._handle = null
    this._buffer = null
    this._autoDestroy = autoDestroy

    if (db._state.opened === true) this.ready()
  }

  _reuse(db, opts = {}) {
    const { autoDestroy = false } = opts

    db._ref()

    this._db = db
    this._destroyed = false
    this._autoDestroy = autoDestroy
  }

  _onfinished(err) {
    const resolve = this._resolve
    const reject = this._reject

    if (this._request) this._db._state.io.dec()

    this._operations = []
    this._promises = []
    this._request = null
    this._resolve = null
    this._reject = null

    if (this._autoDestroy === true) this.destroy()

    if (reject !== null && err) reject(err)
    else if (resolve !== null) resolve()
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

    if (this._db._state.opened === false) await this._db._state.ready()

    this._init()
  }

  destroy() {
    if (this._request) throw new Error('Request in progress')
    if (this._destroyed) return

    this._destroyed = true

    if (this._promises.length) this._abort()

    this._db._unref()
    this._onfree()
  }

  _onfree() {
    this._db._state.freeBatch(this, false)
    this._db = null
  }

  _abort() {
    for (let i = 0; i < this._promises.length; i++) {
      const promise = this._promises[i]
      if (promise !== null) promise.reject(new Error('Batch is destroyed'))
    }

    this._onfinished(new Error('Batch is destroyed'))
  }

  async flush() {
    if (this._request) throw new Error('Request in progress')
    if (this._destroyed) throw new Error('Batch is destroyed')

    this._request = new Promise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
    })

    this._flush()

    return this._request
  }

  tryFlush() {
    if (this._request) throw new Error('Request in progress')
    if (this._destroyed) throw new Error('Batch is destroyed')

    this._request = resolved

    this._flush()
  }

  async _flush() {
    if (this._handle === null) await this.ready()

    this._db._state.io.inc()

    if (this._db._state.resumed !== null) {
      const resumed = await this._db._state.resumed.promise

      if (!resumed) {
        if (this._destroyed) {
          this._db._state.io.dec()
        } else {
          this._destroyed = true
          this._abort()
          this._db._unref()
        }
      }
    }
  }

  _enqueuePromise(resolve, reject) {
    this._promises.push({ resolve, reject })
  }

  _encodeKey(k) {
    if (this._db._keyEncoding) return c.encode(this._db._keyEncoding, k)
    if (typeof k === 'string') return Buffer.from(k)
    return k
  }

  _encodeValue(v) {
    if (this._db._valueEncoding) return c.encode(this._db._valueEncoding, v)
    if (v === null) return empty
    if (typeof v === 'string') return Buffer.from(v)
    return v
  }

  _decodeValue(b) {
    if (this._db._valueEncoding) return c.decode(this._db._valueEncoding, b)
    return b
  }
}

exports.ReadBatch = class RocksDBReadBatch extends RocksDBBatch {
  constructor(db, opts = {}) {
    super(db, opts)

    const { asyncIO = false, fillCache = true } = opts

    this._asyncIO = asyncIO
    this._fillCache = fillCache
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

    if (this._destroyed) return

    try {
      binding.read(
        this._db._state._handle,
        this._handle,
        this._operations,
        this._db._snapshot ? this._db._snapshot._handle : undefined,
        this._asyncIO,
        this._fillCache,
        this,
        this._onread
      )
    } catch (err) {
      this._db._state.io.dec()
      throw err
    }
  }

  _onread(errs, values) {
    let applied = true

    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      const err = errs[i]

      if (err) {
        applied = false

        promise.reject(new Error(err))
      } else {
        promise.resolve(values[i] ? this._decodeValue(Buffer.from(values[i])) : null)
      }
    }

    this._onfinished(applied ? null : new Error('Batch was not applied'))
  }

  get(key) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBGet(this._encodeKey(key), this._db._columnFamily))

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

  _onfree() {
    this._db._state.freeBatch(this, true)
  }

  async _flush() {
    await super._flush()

    if (this._destroyed) return

    try {
      binding.write(this._db._state._handle, this._handle, this._operations, this, this._onwrite)
    } catch (err) {
      this._db._state.io.dec()
      throw err
    }
  }

  _onwrite(err) {
    let applied = true

    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]
      if (promise === null) continue

      if (err) {
        applied = false

        promise.reject(new Error(err))
      } else {
        promise.resolve()
      }
    }

    this._onfinished(applied ? null : new Error('Batch was not applied'))
  }

  put(key, value) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(
      new RocksDBPut(this._encodeKey(key), this._encodeValue(value), this._db._columnFamily)
    )

    this._resize()

    return promise
  }

  tryPut(key, value) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(
      new RocksDBPut(this._encodeKey(key), this._encodeValue(value), this._db._columnFamily)
    )

    this._promises.push(null)

    this._resize()
  }

  delete(key) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(new RocksDBDelete(this._encodeKey(key), this._db._columnFamily))

    this._resize()

    return promise
  }

  tryDelete(key) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(new RocksDBDelete(this._encodeKey(key), this._db._columnFamily))

    this._promises.push(null)

    this._resize()
  }

  deleteRange(start, end) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._operations.push(
      new RocksDBDeleteRange(this._encodeKey(start), this._encodeKey(end), this._db._columnFamily)
    )

    this._resize()

    return promise
  }

  tryDeleteRange(start, end) {
    if (this._request) throw new Error('Request already in progress')

    this._operations.push(
      new RocksDBDeleteRange(this._encodeKey(start), this._encodeKey(end), this._db._columnFamily)
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
