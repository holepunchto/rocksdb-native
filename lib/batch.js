const b4a = require('b4a')
const binding = require('../binding')

const empty = b4a.alloc(0)

module.exports = class RocksDBBatch {
  constructor (db, opts = {}) {
    const {
      capacity = 8
    } = opts

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

  add (key, value = empty) {
    if (this._request) throw new Error('Request already in progress')

    const promise = new Promise(this._enqueuePromise)

    this._keys.push(this._encodeKey(key))
    this._values.push(this._encodeValue(value))
    this._resize()

    return promise
  }

  tryAdd (key, value = empty) {
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
    if (v === null) return empty
    if (typeof v === 'string') return Buffer.from(v)
    return v
  }
}
