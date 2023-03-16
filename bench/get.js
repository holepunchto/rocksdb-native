const path = require('path')
const Database = require('..')

const db = new Database(path.join(__dirname, 'bench.db'))

const key = Buffer.from('key')

db.put(key, Buffer.from('value'))

console.time('db.get(key)')

for (let i = 0; i < 1e6; i++) {
  db.get(key)
}

console.timeEnd('db.get(key)')
