#!/usr/bin/env node
const { command, summary, flag } = require('paparam')
const { configure, test } = require('brittle')
const writeBenchmark = require('./write')
const readBenchmark = require('./read')
const RocksDB = require('..')

const cmd = command(
  'rocksdb-bench',
  summary('Interface for rocksdb-native benchmarks'),
  flag(
    '--duration|-d <value>',
    'Maximum execution time per step in seconds, defaults to 15 seconds'
  ),
  flag('--maxBackgroundJobs <value>'),
  flag('--maxOpenFiles <value>'),
  flag('--useDirectReads'),
  flag('--avoidUnnecessaryBlockingIO'),
  flag('--useDirectIOForFlushAndCompaction'),
  flag('--maxFileOpeningThreads <value>'),

  async () => {
    const {
      duration,
      maxBackgroundJobs,
      maxOpenFiles,
      useDirectReads,
      avoidUnnecessaryBlockingIO,
      useDirectIOForFlushAndCompaction,
      maxFileOpeningThreads
    } = cmd.flags

    const dbOpts = {
      useDirectReads,
      avoidUnnecessaryBlockingIO,
      useDirectIOForFlushAndCompaction
    }

    if (maxBackgroundJobs) dbOpts.maxBackgroundJobs = Number(maxBackgroundJobs)
    if (maxOpenFiles) dbOpts.maxOpenFiles = Number(maxOpenFiles)
    if (maxFileOpeningThreads) dbOpts.maxFileOpeningThreads = Number(maxFileOpeningThreads)

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
