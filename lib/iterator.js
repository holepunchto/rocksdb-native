const { Readable } = require('streamx')
const b4a = require('b4a')
const binding = require('../binding')

const empty = b4a.alloc(0)

module.exports = class RocksDBIterator extends Readable {
  constructor (db, start, end, opts = {}) {
    if (typeof start === 'string') start = b4a.from(start)
    if (typeof end === 'string') end = b4a.from(end)

    if (start && !b4a.isBuffer(start)) {
      opts = start
      start = empty
    } else if (end && !b4a.isBuffer(end)) {
      opts = end
      end = empty
    }

    const {
      gt = empty,
      gte = start,
      lt = end,
      lte = empty,
      reverse = false,
      limit = Infinity,
      capacity = 8
    } = opts

    super()

    this._db = db

    this._gt = toBuffer(gt) || empty
    this._gte = toBuffer(gte) || empty
    this._lt = toBuffer(lt) || empty
    this._lte = toBuffer(lte) || empty

    this._reverse = reverse
    this._limit = limit
    this._capacity = capacity

    this._pendingOpen = null
    this._pendingRead = null
    this._pendingDestroy = null

    this._buffer = null
    this._handle = null

    if (db.opened === true) this.ready()
  }

  _onopen (err) {
    const cb = this._pendingOpen
    this._pendingOpen = null
    cb(err)
  }

  _onread (err, keys, values) {
    const cb = this._pendingRead
    this._pendingRead = null
    if (err) return cb(err)

    const n = keys.length

    this._limit -= n

    for (let i = 0; i < n; i++) {
      this.push({ key: b4a.from(keys[i]), value: b4a.from(values[i]) })
    }

    if (n < this._capacity) this.push(null)

    cb(null)
  }

  _onclose (err) {
    const cb = this._pendingDestroy
    this._pendingDestroy = null
    cb(err)
  }

  _resize () {
    if (this._handle !== null) {
      this._buffer = binding.iteratorBuffer(this._handle, this._capacity)
    }
  }

  async ready () {
    if (this._handle !== null) return

    if (this._db.opened === false) await this._db.ready()

    this._handle = binding.iteratorInit(this._db._handle, this,
      this._onopen,
      this._onclose,
      this._onread
    )

    this._buffer = binding.iteratorBuffer(this._handle, this._capacity)
  }

  async _open (cb) {
    await this.ready()

    this._pendingOpen = cb

    binding.iteratorOpen(this._handle, this._gt, this._gte, this._lt, this._lte, this._reverse)
  }

  _read (cb) {
    this._pendingRead = cb

    binding.iteratorRead(this._handle, Math.min(this._capacity, this._limit))
  }

  async _destroy (cb) {
    await this.ready()

    this._pendingDestroy = cb

    binding.iteratorClose(this._handle)
  }
}

function toBuffer (data) {
  return typeof data === 'string' ? b4a.from(data) : data
}
