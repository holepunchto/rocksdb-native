const { RibbonFilterPolicy } = require('./filter-policy')

const KiB = 1024
const MiB = 1024 * KiB

// Presets tuned per environment. Each profile may set any DB-level or
// column-family-level option; explicitly passed options always win.
const profiles = {
  // The library defaults, named so callers can be explicit about it.
  desktop: {},

  // Memory and fd constrained, often suspended, read-mostly. Ribbon filters
  // trade a bit of CPU for roughly a third of the filter memory of Bloom.
  mobile: {
    maxBackgroundJobs: 2,
    bytesPerSync: 512 * KiB,
    maxOpenFiles: 256,
    writeBufferSize: 16 * MiB,
    optimizeFiltersForMemory: true,
    filterPolicy: new RibbonFilterPolicy(10)
  },

  // Long-running server replicating many cores, e.g. a blind mirror.
  // Write-heavy with a large dataset, so bigger memtables, more compaction
  // parallelism and Ribbon filters below the write-hot levels.
  mirror: {
    maxBackgroundJobs: 8,
    writeBufferSize: 128 * MiB,
    maxWriteBufferNumber: 4,
    optimizeFiltersForMemory: true,
    filterPolicy: new RibbonFilterPolicy(10, 2)
  },

  // Short-lived minimal-footprint open, e.g. a notification extension
  // peeking at the DB. Pair with readOnly: true (and walFilterPrefixes to
  // skip WAL replay) for the smallest possible footprint.
  ephemeral: {
    blockCache: false,
    tableCacheIndexAndFilterBlocks: false,
    optimizeFiltersForMemory: true,
    maxOpenFiles: 1,
    useDirectReads: true,
    filterPolicy: null
  }
}

function applyProfile(opts = {}) {
  const name = opts.profile
  if (!name) return opts

  const profile = profiles[name]
  if (!profile) throw new Error('Unknown RocksDB profile: ' + name)

  const merged = { ...profile }

  for (const key of Object.keys(opts)) {
    if (opts[key] !== undefined) merged[key] = opts[key]
  }

  return merged
}

module.exports = { profiles, applyProfile }
