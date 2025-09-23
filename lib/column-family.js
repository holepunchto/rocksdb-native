const binding = require('../binding')
const constants = require('./constants')
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
      filterPolicy = new BloomFilterPolicy(10),
      topLevelIndexPinningTier = constants.pinningTier.ALL,
      partitionPinningTier = constants.pinningTier.ALL,
      unpartitionedPinningTier = constants.pinningTier.ALL,
      optimizeFiltersForHits = false,
      numLevels = 7,
      maxWriteBufferNumber = 2
    } = opts

    this._name = name
    this._flushing = null
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
      filterPolicy,
      topLevelIndexPinningTier,
      partitionPinningTier,
      unpartitionedPinningTier,
      optimizeFiltersForHits,
      numLevels,
      maxWriteBufferNumber
    }

    const filterPolicyArguments = [0, 0, 0]

    if (filterPolicy !== null) {
      filterPolicyArguments[0] = filterPolicy.type

      switch (filterPolicy.type) {
        case 1: // Bloom filter policy
          filterPolicyArguments[1] = filterPolicy.bitsPerKey
          break
        case 2: // Ribbon filter policy
          filterPolicyArguments[1] = filterPolicy.bloomEquivalentBitsPerKey
          filterPolicyArguments[2] = filterPolicy.bloomBeforeLevel

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
      ...filterPolicyArguments,
      topLevelIndexPinningTier,
      partitionPinningTier,
      unpartitionedPinningTier,
      optimizeFiltersForHits,
      numLevels,
      maxWriteBufferNumber
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
