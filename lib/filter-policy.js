exports.BloomFilterPolicy = class RocksDBBloomFilterPolicy {
  get type() {
    return 1
  }

  constructor(bitsPerKey) {
    this.bitsPerKey = bitsPerKey
  }
}

exports.RibbonFilterPolicy = class RocksDBRibbonFilterPolicy {
  get type() {
    return 2
  }

  constructor(bloomEquivalentBitsPerKey, bloomBeforeLevel = 0) {
    this.bloomEquivalentBitsPerKey = bloomEquivalentBitsPerKey
    this.bloomBeforeLevel = bloomBeforeLevel
  }
}
