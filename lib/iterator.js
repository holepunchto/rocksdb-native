const { Readable } = require('streamx')
const b4a = require('b4a')
const binding = require('../binding')

const empty = b4a.alloc(0)

module.exports = class RocksDBIterator extends Readable {
  constructor (db, start, end, opts = {}) {
    if (typeof start === 'string') start = b4a.from(start)
    if (typeof end === 'string') end = b4a.from(end)

    if (!Buffer.isBuffer(end)) {
      opts = end
      end = empty
    }

    const {
      capacity = 8
    } = opts

    super()

    this._db = db
    this._start = start
    this._end = end
    this._capacity = capacity

    this._pendingOpen = null
    this._pendingRead = null
    this._pendingDestroy = null

    this._handle = null
    this._buffer = null

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

    binding.iteratorOpen(this._handle, this._start, this._end)
  }

  _read (cb) {
    this._pendingRead = cb

    binding.iteratorRead(this._handle)
  }

  async _destroy (cb) {
    await this.ready()

    this._pendingDestroy = cb

    binding.iteratorClose(this._handle)
  }
}
