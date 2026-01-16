module.exports = {
  pinningTier: {
    NONE: 0,
    FLUSHED_AND_SIMILAR: 1,
    ALL: 2
  },
  garbageCollectionPolicy: {
    DEFAULT: 0,
    FORCE: 1,
    DISABLE: 2
  },
  bottommostLevelCompaction: {
    NONE: 0,
    SKIP: 1,
    FORCE: 2
  }
}
