const test = require('brittle')
const tmp = require('test-tmp')
const b4a = require('b4a')
const RocksDB = require('.')

test('open + close', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()
  await db.close()
})

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

test('write + read multiple', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  {
    const p = []

    for (let i = 0; i < 100; i++) {
      p.push(batch.add(`${i}`, `${i}`))
    }

    await batch.write()

    t.alike(await Promise.all(p), new Array(100).fill(0).map((_, i) => b4a.from(`${i}`)))
  }
  {
    const p = []

    for (let i = 0; i < 100; i++) {
      p.push(batch.add(`${i}`))
    }

    await batch.read()

    t.alike(await Promise.all(p), new Array(100).fill(0).map((_, i) => b4a.from(`${i}`)))
  }

  batch.destroy()

  await db.close()
})

test('read missing', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  const p = batch.add('hello')
  await batch.read()
  t.alike(await p, null)

  batch.destroy()

  await db.close()
})
