#!/usr/bin/env node
const { command, summary, flag } = require('paparam')
const { configure, test } = require('brittle')
const writeBenchmark = require('./write')
const readBenchmark = require('./read')
const RocksDB = require('..')
const { BloomFilterPolicy, RibbonFilterPolicy } = RocksDB

const cmd = command(
  'rocksdb-bench',
  summary('Interface for rocksdb-native benchmarks'),

  // Benchmark options
  flag('--duration|-d <value>', 'Maximum execution time per step in seconds, defaults to 15'),

  // RocksDB state options
  flag('--maxBackgroundJobs <value>', 'Defaults to 6'),
  flag('--bytesPerSync <value>', 'Defaults to 1048576'),
  flag('--maxOpenFiles <value>', 'Defaults to -1'),
  flag('--useDirectReads'),
  flag('--avoidUnnecessaryBlockingIO'),
  flag('--useDirectIOForFlushAndCompaction'),
  flag('--maxFileOpeningThreads <value>', 'Defaults to 16'),

  // Column family options
  flag('--enableBlobFiles'),
  flag('--minBlobSize <value>', 'Defaults to 0'),
  flag('--blobFileSize <value>', 'Defaults to 0'),
  flag('--noBlobGarbageCollection'),
  flag('--tableBlockSize <value>', 'Defaults to 8192'),
  flag('--noTableCacheIndexAndFilterBlocks'),
  flag('--tableFormatVersion <value>', 'Defaults to 6'),
  flag('--optimizeFiltersForMemory'),
  flag('--noBlockCache'),
  flag('--topLevelIndexPinningTier <value>', 'Defaults to 0 (ALL)'),
  flag('--partitionPinningTier <value>', 'Defaults to 0 (ALL)'),
  flag('--unpartitionedPinningTier <value>', 'Defaults to 0 (ALL)'),
  flag('--optimizeFiltersForHits'),
  flag('--numLevels <value>', 'Defaults to 7'),
  flag('--maxWriteBufferNumber <value>', 'Defaults to 2'),

  // Filter options
  flag('--ribbon|-r', 'Use Ribbon filter policy'),
  flag('--bitsPerKey <value>', 'Defaults to 10'),
  flag('--bloomBeforeLevel <value>', 'Defaults to 0'),

  // Read options
  flag('--asyncIO'),
  flag('--noFillCache'),

  async () => {
    // Benchmark options
    const benchOpts = {}

    if (cmd.flags.duration) benchOpts.duration = Number(cmd.flags.duration)

    // RocksDB options
    const dbOpts = {}

    // State options
    if (cmd.flags.maxBackgroundJobs) dbOpts.maxBackgroundJobs = Number(cmd.flags.maxBackgroundJobs)
    if (cmd.flags.bytesPerSync) dbOpts.bytesPerSync = Number(cmd.flags.bytesPerSync)
    if (cmd.flags.maxOpenFiles) dbOpts.maxOpenFiles = Number(cmd.flags.maxOpenFiles)
    if (cmd.flags.useDirectReads) dbOpts.useDirectReads = true
    if (cmd.flags.avoidUnnecessaryBlockingIO) dbOpts.avoidUnnecessaryBlockingIO = true
    if (cmd.flags.useDirectIOForFlushAndCompaction) dbOpts.useDirectIOForFlushAndCompaction = true
    if (cmd.flags.maxFileOpeningThreads)
      dbOpts.maxFileOpeningThreads = Number(cmd.flags.maxFileOpeningThreads)

    // Column family options
    if (cmd.flags.enableBlobFiles) dbOpts.enableBlobFiles = true
    if (cmd.flags.minBlobSize) dbOpts.minBlobSize = Number(cmd.flags.minBlobSize)
    if (cmd.flags.blobFileSize) dbOpts.blobFileSize = Number(cmd.flags.blobFileSize)
    if (cmd.flags.noBlobGarbageCollection) dbOpts.enableBlobGarbageCollection = false
    if (cmd.flags.tableBlockSize) dbOpts.tableBlockSize = Number(cmd.flags.tableBlockSize)
    if (cmd.flags.noTableCacheIndexAndFilterBlocks) dbOpts.tableCacheIndexAndFilterBlocks = false
    if (cmd.flags.tableFormatVersion)
      dbOpts.tableFormatVersion = Number(cmd.flags.tableFormatVersion)
    if (cmd.flags.optimizeFiltersForMemory) dbOpts.optimizeFiltersForMemory = true
    if (cmd.flags.noBlockCache) dbOpts.blockCache = false
    if (cmd.flags.topLevelIndexPinningTier)
      dbOpts.topLevelIndexPinningTier = Number(cmd.flags.topLevelIndexPinningTier)
    if (cmd.flags.partitionPinningTier)
      dbOpts.partitionPinningTier = Number(cmd.flags.partitionPinningTier)
    if (cmd.flags.unpartitionedPinningTier)
      dbOpts.unpartitionedPinningTier = Number(cmd.flags.unpartitionedPinningTier)
    if (cmd.flags.optimizeFiltersForHits) dbOpts.optimizeFiltersForHits = true
    if (cmd.flags.numLevels) dbOpts.numLevels = Number(cmd.flags.numLevels)
    if (cmd.flags.maxWriteBufferNumber)
      dbOpts.maxWriteBufferNumber = Number(cmd.flags.maxWriteBufferNumber)

    // Filter options
    if (cmd.flags.ribbon || cmd.flags.bitsPerKey) {
      const _bitsPerKey = cmd.flags.bitsPerKey ? Number(cmd.flags.bitsPerKey) : 10

      dbOpts.filterPolicy = cmd.flags.ribbon
        ? new RibbonFilterPolicy(_bitsPerKey, Number(cmd.flags.bloomBeforeLevel) || 0)
        : new BloomFilterPolicy(_bitsPerKey)
    }

    // Read options
    const readOpts = {}

    if (cmd.flags.asyncIO) readOpts.asyncIO = true
    if (cmd.flags.noFillCache) readOpts.fillCache = false

    startBenchmark(benchOpts, dbOpts, readOpts)
  }
)

async function startBenchmark(benchOpts, dbOpts, readOpts) {
  configure({ timeout: 600_000 })

  test('Benchmark', async (t) => {
    const db = new RocksDB(await t.tmp(), dbOpts)
    await db.ready()
    t.teardown(() => db.close())

    const keysAmount = await writeBenchmark(t, db, benchOpts)

    readBenchmark(t, db, keysAmount, benchOpts, readOpts)
  })
}

cmd.parse()
