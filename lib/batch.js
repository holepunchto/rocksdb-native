const c = require('compact-encoding')
const binding = require('../binding')

const empty = Buffer.alloc(0)
const resolved = Promise.resolve()

// Operations are stored as a structure of arrays instead of one object per op:
// _ops is a flat Uint32Array with STRIDE words per op. Word 0 packs the op type into
// the upper 8 bits and the column family index (into _columnFamilies) into the lower 24.
// Each key/value/range end is then 3 words: index into _buffers, byte offset, byte length.
// _buffers holds the backing ArrayBuffers, so keys carved from one slab and values from
// one pool share a single entry and the native side resolves them once per batch.
const SLICE_BITS = 24
const CF_MASK = (1 << SLICE_BITS) - 1

class RocksDBBatch {
  constructor(db, opts = {}) {
    const { capacity = 8, autoDestroy = false } = opts

    db._ref()

    this._db = db
    this._destroyed = false
    this._capacity = capacity
    this._ops = new Uint32Array(capacity * this._stride())
    this._length = 0
    this._buffers = []
    this._columnFamilies = []
    this._nativeCapacity = 0
    this._promises = []

    this._enqueuePromise = this._enqueuePromise.bind(this)

    this._request = null
    this._resolve = null
    this._reject = null

    this._handle = null
    this._buffer = null
    this._autoDestroy = autoDestroy
    this._stats = null

    this._resetStats()

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

    this._length = 0
    this._buffers.length = 0
    this._columnFamilies.length = 0
    this._promises.length = 0
    this._request = null
    this._resolve = null
    this._reject = null

    this._resetStats()

    if (this._autoDestroy === true) this.destroy()

    if (reject !== null && err) reject(err)
    else if (resolve !== null) resolve()
  }

  _push(type, columnFamily) {
    if (this._length === this._capacity) {
      this._capacity *= 2
      const ops = new Uint32Array(this._capacity * this._stride())
      ops.set(this._ops)
      this._ops = ops
    }

    const i = this._length++ * this._stride()
    this._ops[i] = (type << SLICE_BITS) | this._columnFamily(columnFamily._handle)
    return i + 1
  }

  _columnFamily(handle) {
    const cfs = this._columnFamilies
    const last = cfs.length - 1
    if (last >= 0 && cfs[last] === handle) return last
    for (let i = 0; i < last; i++) if (cfs[i] === handle) return i
    if (cfs.length > CF_MASK) throw new Error('Too many column families in one batch')
    return cfs.push(handle) - 1
  }

  _slice(i, view) {
    let ab = view.buffer
    let offset = view.byteOffset

    if (!(ab instanceof ArrayBuffer)) {
      // eg. SharedArrayBuffer backed views, copy them into a plain buffer
      view = Buffer.from(view)
      ab = view.buffer
      offset = view.byteOffset
    }

    const buffers = this._buffers
    const last = buffers.length - 1
    let index

    if (last >= 0 && buffers[last] === ab) index = last
    else if (last >= 1 && buffers[last - 1] === ab) index = last - 1
    else index = buffers.push(ab) - 1

    this._ops[i] = index
    this._ops[i + 1] = offset
    this._ops[i + 2] = view.byteLength
  }

  _nativeBuffer() {
    if (this._nativeCapacity >= this._length) return
    this._nativeCapacity = this._capacity
    this._buffer = this._createNativeBuffer()
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
    this._resetStats()
    this._db._state.freeBatch(this, false)
    this._db = null
  }

  _abort() {
    for (let i = 0; i < this._promises.length; i++) {
      this._promises[i].reject(new Error('Batch is destroyed'))
    }

    this._onfinished(new Error('Batch is destroyed'))
  }

  _resetStats() {}

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

    // Not _runWithIO: on a failed resume the batch releases io itself through _abort and _onfinished, so the wrapper would release it twice
    this._db._state.io.inc()

    if (this._db._state.resumed !== null) {
      const resumed = await this._db._state.waitForResume()

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

  // type|cf, key
  _stride() {
    return 4
  }

  _init() {
    this._handle = binding.readInit()
    this._nativeCapacity = this._capacity
    this._buffer = this._createNativeBuffer()
  }

  _createNativeBuffer() {
    return binding.readBuffer(this._handle, this._capacity)
  }

  async _flush() {
    await super._flush()

    if (this._destroyed) return

    try {
      this._nativeBuffer()

      binding.read(
        this._db._state._handle,
        this._handle,
        this._ops,
        this._length,
        this._buffers,
        this._columnFamilies,
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

    this._db._state.stats.gets += this._stats.gets
    this._db._state.stats.readBatches++
  }

  _onread(errs, values) {
    let applied = true

    for (let i = 0, n = this._promises.length; i < n; i++) {
      const err = errs[i]
      if (err) applied = false

      const promise = this._promises[i]

      if (err) promise.reject(err)
      else promise.resolve(values[i] ? this._decodeValue(Buffer.from(values[i])) : null)
    }

    this._onfinished(applied ? null : new AggregateError(errs, 'Batch was not applied'))
  }

  _resetStats() {
    this._stats = { gets: 0 }
  }

  get(key) {
    if (this._request) throw new Error('Request already in progress')
    this._stats.gets++

    const promise = new Promise(this._enqueuePromise)

    const i = this._push(binding.GET, this._db._columnFamily)
    this._slice(i, this._encodeKey(key))

    return promise
  }
}

exports.WriteBatch = class RocksDBWriteBatch extends RocksDBBatch {
  // type|cf, key or range start, value or range end
  _stride() {
    return 7
  }

  _init() {
    this._handle = binding.writeInit()
    this._nativeCapacity = this._capacity
    this._buffer = this._createNativeBuffer()
  }

  _createNativeBuffer() {
    return binding.writeBuffer(this._handle, this._capacity)
  }

  _onfree() {
    this._resetStats()
    this._db._state.freeBatch(this, true)
    this._db = null
  }

  async _flush() {
    await super._flush()

    if (this._destroyed) return

    try {
      this._nativeBuffer()

      binding.write(
        this._db._state._handle,
        this._handle,
        this._ops,
        this._length,
        this._buffers,
        this._columnFamilies,
        this,
        this._onwrite
      )
    } catch (err) {
      this._db._state.io.dec()
      throw err
    }

    this._db._state.stats.puts += this._stats.puts
    this._db._state.stats.deletes += this._stats.deletes
    this._db._state.stats.rangeDeletes += this._stats.rangeDeletes
    this._db._state.stats.writeBatches++
  }

  _onwrite(err) {
    const applied = !err

    for (let i = 0, n = this._promises.length; i < n; i++) {
      const promise = this._promises[i]

      if (err) promise.reject(err)
      else promise.resolve()
    }

    this._onfinished(applied ? null : new Error('Batch was not applied', { cause: err }))
  }

  _resetStats() {
    this._stats = { puts: 0, deletes: 0, rangeDeletes: 0 }
  }

  _put(key, value) {
    this._stats.puts++

    const i = this._push(binding.PUT, this._db._columnFamily)
    this._slice(i, this._encodeKey(key))
    this._slice(i + 3, this._encodeValue(value))
  }

  _delete(key) {
    this._stats.deletes++

    const i = this._push(binding.DELETE, this._db._columnFamily)
    this._slice(i, this._encodeKey(key))
  }

  _deleteRange(start, end) {
    this._stats.rangeDeletes++

    const i = this._push(binding.DELETE_RANGE, this._db._columnFamily)
    this._slice(i, this._encodeKey(start))
    this._slice(i + 3, this._encodeKey(end))
  }

  put(key, value) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)
    this._put(key, value)

    return promise
  }

  tryPut(key, value) {
    if (this._request) throw new Error('Request already in progress')

    this._put(key, value)
  }

  delete(key) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)
    this._delete(key)

    return promise
  }

  tryDelete(key) {
    if (this._request) throw new Error('Request already in progress')

    this._delete(key)
  }

  deleteRange(start, end) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)
    this._deleteRange(start, end)

    return promise
  }

  tryDeleteRange(start, end) {
    if (this._request) throw new Error('Request already in progress')

    this._deleteRange(start, end)
  }
}
