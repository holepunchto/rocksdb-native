const binding = require('../binding')

module.exports = class RocksDBSnapshot {
  constructor(db) {
    this._db = db
    this._db._snapshots.add(this)

    this._handle = null

    if (db.opened === true) this._init()
  }

  _init() {
    this._handle = binding.snapshotCreate(this._db._handle)
  }

  destroy() {
    this._db._snapshots.delete(this)

    if (this._handle === null) return

    binding.snapshotDestroy(this._handle)

    this._handle = null
  }
}
