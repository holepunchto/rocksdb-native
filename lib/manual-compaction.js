const c = require('compact-encoding')
const binding = require('../binding')

const empty = Buffer.alloc(0)

module.exports = class RocksDBManualCompaction {
  constructor(db, opts = {}) {
    const { exclusive = false } = opts

    this._db = db
    this._exclusive = exclusive
  }

  async compactRange(start = null, end = null) {
    this._db._ref()
    this._db._state.io.inc()

    const { promise, resolve, reject } = Promise.withResolvers()
    const req = { resolve, reject, handle: null }

    try {
      req.handle = binding.compactRange(
        this._db._state._handle,
        this._encodeKey(start),
        this._encodeKey(end),
        this._exclusive,
        req,
        cb
      )

      await promise
    } finally {
      this._db._state.io.dec()
      this._db._unref()
    }

    function cb(err) {
      if (err) req.reject(new Error(err))
      else req.resolve()
    }
  }

  _encodeKey(k) {
    if (this._db._keyEncoding) return c.encode(this._db._keyEncoding, k)
    if (typeof k === 'string') return Buffer.from(k)
    if (k === null) return empty
    return k
  }
}
