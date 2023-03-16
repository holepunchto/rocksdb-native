const path = require('path')
const Database = require('..')

const db = new Database(path.join(__dirname, 'bench.db'))

const key = Buffer.from('key')
const value = Buffer.alloc(4 * 1024, 'value')

console.time('db.put(key, value)')

for (let i = 0; i < 1e5; i++) {
  db.put(key, value)
}

console.timeEnd('db.put(key, value)')
