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

## License

Apache-2.0
