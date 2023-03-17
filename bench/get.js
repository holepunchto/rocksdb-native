const path = require('path')
const Database = require('..')

const db = new Database(path.join(__dirname, 'bench.db'))

db.ready().then(async () => {
  const key = Buffer.from('key')

  await db.put(key, Buffer.alloc(4 * 1024, 'value'))

  console.time('db.get(key)')

  for (let i = 0; i < 1e5; i++) {
    await db.get(key)
  }

  console.timeEnd('db.get(key)')
})
