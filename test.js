const test = require('brittle')
const tmp = require('test-tmp')
const b4a = require('b4a')
const RocksDB = require('.')

test('write + read', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  {
    const p = batch.add('hello', 'world')
    await batch.write()
    t.alike(await p, b4a.from('world'))
  }
  {
    const p = batch.add('hello')
    await batch.read()
    t.alike(await p, b4a.from('world'))
  }

  batch.destroy()

  await db.close()
})
