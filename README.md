# rocksdb-native

<https://github.com/holepunchto/librocksdb> bindings for JavaScript.

```
npm i rocksdb-native
```

## Usage

```js
const RocksDB = require('rocksdb-native')

const db = new RocksDB('./example.db')

const w = db.write()
w.put('hello', 'world')
await w.flush()

const r = db.read()
const p = r.get('hello')
r.flush()

console.log(await p)
```

## Profiles

Pass `profile` to get defaults tuned for an environment. Any option you pass explicitly wins over the profile.

```js
const db = new RocksDB('./example.db', { profile: 'mobile' })
```

- `desktop` — the library defaults, named so callers can be explicit.
- `mobile` — memory and fd constrained devices: small write buffers (16 MiB),
  fewer background jobs, bounded open files, Ribbon filters and
  memory-optimized filter allocation.
- `mirror` — long-running servers replicating many cores (e.g. blind mirrors):
  large write buffers (128 MiB x 4), more compaction parallelism, Ribbon
  filters below the write-hot levels.
- `ephemeral` — short-lived minimal-footprint opens (e.g. a notification
  extension peeking at the DB): no block cache, no filters, direct reads,
  a single open file. Pair with `readOnly: true` (and `walFilterPrefixes`
  where applicable).

The presets are exported as `RocksDB.profiles`. A profile applies to both DB-level options and column-family options, including column families created by name and standalone `new RocksDB.ColumnFamily(name, { profile })`.

## Reclaiming space

`compact()` and `compactRange()` take `reclaim` for when the point of the compaction is to give disk space back, e.g. after deleting a range:

```js
await db.deleteRange(start, end)
await db.compactRange(start, end, { reclaim: true })
```

By default a compaction leaves the bottommost level alone and only collects blobs once the regular garbage collection thresholds are hit, so data that was deleted while it was pinned by a snapshot, and blob files that are mostly but not entirely dead, keep their space. `reclaim` forces both. It is more expensive, since the files get rewritten rather than skipped.

The underlying `bottommostLevelCompaction`, `blobGarbageCollectionPolicy` and `blobGarbageCollectionAgeCutoff` options remain available and take precedence over `reclaim` when passed explicitly.

## Block cache

`blockCacheSize` bounds the memory the database spends caching blocks, index and filter data:

```js
const db = new RocksDB('./example.db', { blockCacheSize: 8 * 1024 * 1024 })
```

One cache of that size is created when the database opens and shared by every column family, so the number tells you the total. Without it each column family gets its own cache of RocksDB's default size, which cannot be bounded and grows with the number of column families.

Column families that set `blockCache: false` keep their own setting and stay out of the shared cache. Note that disabling the cache also requires `tableCacheIndexAndFilterBlocks: false`, as index and filter blocks have nowhere else to live.

## License

Apache-2.0
