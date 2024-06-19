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

## License

Apache-2.0
