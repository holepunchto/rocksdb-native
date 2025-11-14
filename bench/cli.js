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
  flag(
    '--duration|-d <value>',
    'Maximum execution time per step in seconds, defaults to 15 seconds'
  ),
  // RocksDB options
  flag('--maxBackgroundJobs <value>'),
  flag('--maxOpenFiles <value>'),
  flag('--useDirectReads'),
  flag('--avoidUnnecessaryBlockingIO'),
  flag('--useDirectIOForFlushAndCompaction'),
  flag('--maxFileOpeningThreads <value>'),
  // Filter options
  flag('--ribbon|-r', 'Use Ribbon filter policy'),
  flag('--bitsPerKey <value>'),
  flag('--bloomBeforeLevel <value>'),

  async () => {
    let {
      duration,

      maxBackgroundJobs,
      maxOpenFiles,
      useDirectReads,
      avoidUnnecessaryBlockingIO,
      useDirectIOForFlushAndCompaction,
      maxFileOpeningThreads,

      ribbon,
      bitsPerKey,
      bloomBeforeLevel
    } = cmd.flags

    const dbOpts = { useDirectReads, avoidUnnecessaryBlockingIO, useDirectIOForFlushAndCompaction }

    if (maxBackgroundJobs) dbOpts.maxBackgroundJobs = Number(maxBackgroundJobs)
    if (maxOpenFiles) dbOpts.maxOpenFiles = Number(maxOpenFiles)
    if (maxFileOpeningThreads) dbOpts.maxFileOpeningThreads = Number(maxFileOpeningThreads)

    if (ribbon || bitsPerKey || bloomBeforeLevel) {
      bitsPerKey = bitsPerKey ? Number(bitsPerKey) : 10
      bloomBeforeLevel = bloomBeforeLevel ? Number(bloomBeforeLevel) : 0

      dbOpts.filterPolicy = ribbon
        ? new RibbonFilterPolicy(bitsPerKey, bloomBeforeLevel)
        : new BloomFilterPolicy(bitsPerKey)
    }

    const benchOpts = { duration: duration ? Number(duration) * 1000 : 0 }

    startBenchmark(dbOpts, benchOpts)
  }
)

async function startBenchmark(dbOpts, benchOpts) {
  configure({ timeout: 600_000 })

  test('Benchmark', async (t) => {
    const db = new RocksDB(await t.tmp(), dbOpts)
    await db.ready()
    t.teardown(() => db.close())

    const keysAmount = await writeBenchmark(t, db, benchOpts)

    readBenchmark(t, db, keysAmount, benchOpts)
  })
}

cmd.parse()
