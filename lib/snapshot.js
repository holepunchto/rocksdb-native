const binding = require('../binding')

module.exports = class RocksDBSnapshot {
  constructor(state) {
    this._state = state

    this._handle = null
    this._refs = 0 // starts unreffed, easier, must be reffed in first tick

    if (state.deferSnapshotInit === false) this._init()
  }

  _init() {
    this._handle = binding.snapshotCreate(this._state.handle)
  }

  ref() {
    this._refs++
  }

  unref() {
    if (--this._refs > 0) return

    if (this._handle === null) return

    binding.snapshotDestroy(this._handle)

    this._handle = null
  }
}
