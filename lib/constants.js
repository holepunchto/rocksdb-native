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
  },
  statsLevel: {
    DISABLE_ALL: 0,
    EXCEPT_HISTOGRAM_OR_TIMERS: 1,
    EXCEPT_TIMERS: 2,
    EXCEPT_DETAILED_TIMERS: 3,
    EXCEPT_TIME_FOR_MUTEX: 4,
    ALL: 5
  },
  walFileType: {
    ARCHIVED: 0,
    ALIVE: 1
  }
}
