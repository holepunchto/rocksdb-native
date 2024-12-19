const binding = require('../binding')

module.exports = class RocksDBSnapshot {
  constructor(state) {
    this._state = state
    this._state.snapshots.add(this)

    this._handle = null
    this._refs = 1

    if (state.opened === true) this._init()
  }

  _init() {
    this._handle = binding.snapshotCreate(this._state.handle)
  }

  ref() {
    this._refs++
  }

  unref() {
    if (--this._refs > 0) return

    this._state.snapshots.delete(this)

    if (this._handle === null) return

    binding.snapshotDestroy(this._handle)

    this._handle = null
  }
}
