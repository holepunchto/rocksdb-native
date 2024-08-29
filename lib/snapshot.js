const binding = require('../binding')

module.exports = class RocksDBSnapshot {
  constructor (db) {
    if (db.opened === false) throw new Error('Database is not open')

    this._db = db

    this._handle = binding.snapshotCreate(db._handle)

    this._db._snapshots.add(this)
  }

  destroy () {
    if (this._handle === null) return

    binding.snapshotDestroy(this._handle)

    this._handle = null

    this._db._snapshots.delete(this)
  }
}
