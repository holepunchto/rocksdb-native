#!/usr/bin/env node
const { command, summary, flag } = require('paparam')
const { configure, test } = require('brittle')
const writeBenchmark = require('./write')
const readBenchmark = require('./read')
const RocksDB = require('..')

const cmd = command(
  'rocksdb-bench',
  summary('Interface for rocksdb-native benchmarks'),
  flag('--maxBackgroundJobs <value>'),
  flag('--maxOpenFiles <value>'),
  flag('--useDirectReads'),
  flag('--avoidUnnecessaryBlockingIO'),
  flag('--useDirectIOForFlushAndCompaction'),
  flag('--maxFileOpeningThreads <value>'),

  async () => {
    const {
      maxBackgroundJobs,
      maxOpenFiles,
      useDirectReads,
      avoidUnnecessaryBlockingIO,
      useDirectIOForFlushAndCompaction,
      maxFileOpeningThreads
    } = cmd.flags

    const opts = { useDirectReads, avoidUnnecessaryBlockingIO, useDirectIOForFlushAndCompaction }

    if (maxBackgroundJobs) opts.maxBackgroundJobs = Number(maxBackgroundJobs)
    if (maxOpenFiles) opts.maxOpenFiles = Number(maxOpenFiles)
    if (maxFileOpeningThreads) opts.maxFileOpeningThreads = Number(maxFileOpeningThreads)

    startBenchmark(opts)
  }
)

async function startBenchmark(opts) {
  configure({ timeout: 600_000 })

  test('Benchmark', async (t) => {
    const db = new RocksDB(await t.tmp(), opts)
    await db.ready()
    t.teardown(() => db.close())

    const keysAmount = await writeBenchmark(t, db)

    readBenchmark(t, db, keysAmount)
  })
}

cmd.parse()
