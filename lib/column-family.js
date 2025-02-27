const binding = require('../binding')

class RocksDBColumnFamily {
  constructor(name, opts = {}) {
    const {
      // Blob options
      enableBlobFiles = false,
      minBlobSize = 0,
      blobFileSize = 0,
      enableBlobGarbageCollection = true,
      // Block table options
      tableBlockSize = 8192,
      tableCacheIndexAndFilterBlocks = true,
      tableFormatVersion = 6,
      // In case we are cloning
      settings = null
    } = opts

    this._name = name
    this._settings =
      settings ||
      Uint32Array.from([
        0,
        enableBlobFiles ? 1 : 0,
        minBlobSize & 0xffffffff,
        Math.floor(minBlobSize / 0x100000000),
        blobFileSize & 0xffffffff,
        Math.floor(blobFileSize / 0x100000000),
        enableBlobGarbageCollection ? 1 : 0,
        tableBlockSize & 0xffffffff,
        Math.floor(tableBlockSize / 0x100000000),
        tableCacheIndexAndFilterBlocks ? 1 : 0,
        tableFormatVersion
      ])

    this._handle = binding.columnFamilyInit(name, this._settings)
  }

  cloneSettings(name) {
    return new RocksDBColumnFamily(name, { settings: this._settings })
  }

  get name() {
    return this._name
  }

  destroy() {
    if (this._handle === null) return

    binding.columnFamilyDestroy(this._handle)

    this._handle = null
  }
}

module.exports = RocksDBColumnFamily
