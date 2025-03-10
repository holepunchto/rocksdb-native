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
      optimizeFiltersForMemory = false,
      blockCache = true
    } = opts

    this._name = name
    this._options = {
      enableBlobFiles,
      minBlobSize,
      blobFileSize,
      enableBlobGarbageCollection,
      tableBlockSize,
      tableCacheIndexAndFilterBlocks,
      tableFormatVersion,
      optimizeFiltersForMemory,
      blockCache
    }

    this._handle = binding.columnFamilyInit(
      name,
      enableBlobFiles,
      minBlobSize,
      blobFileSize,
      enableBlobGarbageCollection,
      tableBlockSize,
      tableCacheIndexAndFilterBlocks,
      tableFormatVersion,
      optimizeFiltersForMemory,
      blockCache === false
    )
  }

  cloneSettings(name) {
    return new RocksDBColumnFamily(name, this._options)
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
