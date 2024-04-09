const test = require('brittle')
const b4a = require('b4a')
const RocksDB = require('.')

test('write + read', async (t) => {
  const db = new RocksDB('./test/fixtures/test.db')
  await db.ready()

  const batch = db.batch()

  {
    const p = batch.add('hello', 'world')
    await batch.write()
    await p
  }
  {
    const p = batch.add('hello')
    await batch.read()
    t.alike(await p, b4a.from('world'))
  }

  batch.destroy()

  await db.close()
})
