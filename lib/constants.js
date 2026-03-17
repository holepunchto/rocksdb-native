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
  },
  walRecoveryMode: {
    TOLERATE_CORRUPTED_TAIL_RECORDS: 0,
    ABSOLUTE_CONSISTENCY: 1,
    POINT_IN_TIME: 2,
    SKIP_ANY_CORRUPTED_RECORDS: 3
  }
}
