#!/usr/bin/env node
const { command, summary, flag } = require('paparam')
const start = require('./write-read')

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

    start(opts)
  }
)

cmd.parse()
