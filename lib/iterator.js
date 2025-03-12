const { Readable } = require('streamx')
const c = require('compact-encoding')
const binding = require('../binding')

const empty = Buffer.alloc(0)

module.exports = class RocksDBIterator extends Readable {
  constructor(db, opts = {}) {
    const {
      gt = null,
      gte = null,
      lt = null,
      lte = null,
      reverse = false,
      limit = Infinity,
      capacity = 8
    } = opts

    super()

    db._ref()

    this._db = db

    this._gt = gt ? this._encodeKey(gt) : empty
    this._gte = gte ? this._encodeKey(gte) : empty
    this._lt = lt ? this._encodeKey(lt) : empty
    this._lte = lte ? this._encodeKey(lte) : empty

    this._reverse = reverse
    this._limit = limit < 0 ? Infinity : limit
    this._capacity = capacity

    this._pendingOpen = null
    this._pendingRead = null
    this._pendingDestroy = null

    this._buffer = null
    this._handle = null

    if (this._db._state.opened === true) this.ready()
  }

  _onopen(err) {
    const cb = this._pendingOpen
    this._pendingOpen = null
    cb(err)
  }

  _onread(err, keys, values) {
    const cb = this._pendingRead
    this._pendingRead = null
    if (err) return cb(err)

    const n = keys.length

    this._limit -= n

    for (let i = 0; i < n; i++) {
      this.push({
        key: this._decodeKey(Buffer.from(keys[i])),
        value: this._decodeValue(Buffer.from(values[i]))
      })
    }

    if (n < this._capacity) this.push(null)

    cb(null)
  }

  _onclose(err) {
    const cb = this._pendingDestroy
    this._pendingDestroy = null
    this._db._unref()
    cb(err)
  }

  _resize() {
    if (this._handle !== null) {
      this._buffer = binding.iteratorBuffer(this._handle, this._capacity)
    }
  }

  async ready() {
    if (this._handle !== null) return

    if (this._db._state.opened === false) await this._db._state.ready()

    this._init()
  }

  _init() {
    this._handle = binding.iteratorInit()
    this._buffer = binding.iteratorBuffer(this._handle, this._capacity)
  }

  async _open(cb) {
    await this.ready()

    const state = this._db._state

    if (state._suspending || state._suspended) {
      return cb(new Error('Database is suspended'))
    }

    this._pendingOpen = cb

    binding.iteratorOpen(
      this._db._state._handle,
      this._handle,
      this._db._columnFamily._handle,
      this._gt,
      this._gte,
      this._lt,
      this._lte,
      this._reverse,
      this._db._snapshot ? this._db._snapshot._handle : null,
      this,
      this._onopen,
      this._onclose,
      this._onread
    )
  }

  _read(cb) {
    const state = this._db._state

    if (state._suspending || state._suspended) {
      return cb(new Error('Database is suspended'))
    }

    this._pendingRead = cb

    binding.iteratorRead(this._handle, Math.min(this._capacity, this._limit))
  }

  async _destroy(cb) {
    await this.ready()

    this._pendingDestroy = cb

    binding.iteratorClose(this._handle)
  }

  _encodeKey(k) {
    if (this._db._keyEncoding !== null)
      return c.encode(this._db._keyEncoding, k)
    if (typeof k === 'string') return Buffer.from(k)
    return k
  }

  _decodeKey(b) {
    if (this._db._keyEncoding !== null)
      return c.decode(this._db._keyEncoding, b)
    return b
  }

  _decodeValue(b) {
    if (this._db._valueEncoding !== null)
      return c.decode(this._db._valueEncoding, b)
    return b
  }
}
