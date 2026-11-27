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
