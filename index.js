const ReadyResource = require('ready-resource')
const binding = require('./binding')

class RocksDB extends ReadyResource {
  constructor (path) {
    super()

    if (typeof path !== 'string') throw new Error('path is required and must be a string')

    this._path = path
    this._handle = Buffer.allocUnsafe(binding.sizeof_rocksdb_native_t)
    this._autoFlush = new Int32Array(this._handle.buffer, this._handle.byteOffset + binding.offsetof_auto_flush, 1)
    this._status = null
    this._writes = null
    this._writesNext = null
    this._reads = null
    this._readsNext = null
    this._queueWriteBound = this._queueWrite.bind(this)
    this._queueReadBound = this._queueRead.bind(this)

    binding.rocksdb_native_init(this._handle, this, this._onstatuschange, this._onbatchdone)
    this.ready().catch(noop)
  }

  _encodeKey (k) {
    return typeof k === 'string' ? Buffer.from(k) : k
  }

  _encodeValue (v) {
    return typeof v === 'string' ? Buffer.from(v) : v
  }

  _open () {
    const promise = new Promise((resolve, reject) => {
      this._status = { resolve, reject }
    })
    binding.rocksdb_native_open(this._handle, this._path)
    return promise
  }

  _close () {
    console.log('TODO: close')
  }

  _queueWrite (resolve, reject) {
    if (this._writesNext === null) this._writesNext = []
    this._writesNext.push({
      key: null,
      value: null,
      resolve,
      reject
    })
  }

  _queueRead (resolve, reject) {
    if (this._readsNext === null) this._readsNext = []
    this._readsNext.push({
      key: null,
      resolve,
      reject
    })
  }

  _flush () {
    if (this._reads !== null || this._writes !== null) return

    this._reads = this._readsNext
    this._writes = this._writesNext
    this._writesNext = this._readsNext = null

    this._autoFlush[0] = 0

    if (this._writes !== null) {
      for (let i = 0; i < this._writes.length; i++) {
        const w = this._writes[i]
        if (i === this._writes.length - 1 && this._reads === null) this._autoFlush[0] = 1
        binding.rocksdb_native_queue_put(this._handle, w.key, w.value)
      }
    }

    if (this._reads !== null) {
      for (let i = 0; i < this._reads.length; i++) {
        const r = this._reads[i]
        if (i === this._reads.length - 1) this._autoFlush[0] = 1
        binding.rocksdb_native_queue_get(this._handle, r.key)
      }
    }
  }

  async batch (list) {
    for (let i = 0; i < list.length - 1; i++) {
      const op = list[i]
      this._queueWriteBound(null, null)
      const b = this._writesNext[this._writesNext.length - 1]
      b.key = this._encodeKey(op.key)
      b.value = this._encodeValue(op.value)
    }
    const op = list[list.length - 1]
    return this.put(op.key, op.value)
  }

  async put (key, value) {
    if (this.opened === false) await this.ready()

    const promise = new Promise(this._queueWriteBound)
    const b = this._writesNext[this._writesNext.length - 1]

    b.key = this._encodeKey(key)
    b.value = this._encodeValue(value)

    this._flush()

    return promise
  }

  async get (key) {
    if (this.opened === false) await this.ready()

    const promise = new Promise(this._queueReadBound)
    const b = this._readsNext[this._readsNext.length - 1]

    b.key = this._encodeKey(key)

    this._flush()

    return promise
  }

  _onbatchdone (buffers) {
    if (this._writes !== null) {
      const writes = this._writes
      this._writes = null
      for (let i = 0; i < writes.length; i++) {
        if (writes[i].resolve !== null) writes[i].resolve()
      }
    }

    if (this._reads !== null) {
      const reads = this._reads
      this._reads = null

      for (let i = 0; i < buffers.length; i++) {
        const buf = buffers[i]
        const val = buf === null ? null : Buffer.from(buffers[i])
        reads[i].resolve(val)
      }
    }

    if (this._writesNext !== null || this._readsNext !== null) {
      this._flush()
    }
  }

  _onstatuschange (status) {
    const err = status < 0 ? new Error(binding.rocksdb_native_clear_error(this._handle)) : null
    const { resolve, reject } = this._status
    this._status = null
    if (err) reject(err)
    else resolve(null)
  }
}

function noop () {}

main()

async function main () {
  const db = new RocksDB('sandbox/test-db')

  await db.batch([
    { key: 'a', value: 'a' },
    { key: 'b', value: 'b' },
    { key: 'c', value: 'c' }
  ])

  const val = await db.get('b')

  console.log('done!', val)
}
