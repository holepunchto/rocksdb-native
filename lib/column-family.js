const binding = require('../binding')
const { BloomFilterPolicy } = require('./filter-policy')

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
      blockCache = true,
      filterPolicy = new BloomFilterPolicy(10)
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
      blockCache,
      filterPolicy
    }

    const filterPolicyArguments = []

    if (filterPolicy === null) filterPolicyArguments.push(0)
    else {
      filterPolicyArguments.push(filterPolicy.type)

      switch (filterPolicy.type) {
        case 1: // Bloom filter policy
          filterPolicyArguments.push(filterPolicy.bitsPerKey)
          break
        case 2: // Ribbon filter policy
          filterPolicyArguments.push(
            filterPolicy.bloomEquivalentBitsPerKey,
            filterPolicy.bloomBeforeLevel
          )
          break
      }
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
      blockCache === false,
      ...filterPolicyArguments
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
