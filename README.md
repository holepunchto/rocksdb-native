# rocksdb-native

<https://github.com/holepunchto/librocksdb> bindings for JavaScript.

```
npm i rocksdb-native
```

## Usage

```js
const RocksDB = require('rocksdb-native')

const db = new RocksDB('./example.db')

const b = db.batch()

b.add('hello', 'world')
await b.write()

const p = b.add('hello')
b.read()

console.log(await p)
```

## License

Apache-2.0
