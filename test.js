const test = require('brittle')
const tmp = require('test-tmp')
const c = require('compact-encoding')
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

  await db.close()
})

test('read missing', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  const p = batch.add('hello')
  await batch.read()
  t.alike(await p, null)

  await db.close()
})

test('delete range', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.batch()

    batch.add('aa', 'aa')
    batch.add('ab', 'ab')
    batch.add('ba', 'ba')
    batch.add('bb', 'bb')
    batch.add('bc', 'bc')
    batch.add('ac', 'ac')

    await batch.write()
  }

  await db.deleteRange('a', 'b')

  {
    const batch = db.batch()

    const p = []

    p.push(batch.add('aa'))
    p.push(batch.add('ab'))
    p.push(batch.add('ac'))
    p.push(batch.add('ba'))
    p.push(batch.add('bb'))
    p.push(batch.add('bc'))

    await batch.read()

    t.alike(await Promise.all(p), [
      null,
      null,
      null,
      Buffer.from('ba'),
      Buffer.from('bb'),
      Buffer.from('bc')
    ])
  }

  await db.close()
})

test('prefix iterator', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  batch.add('aa', 'aa')
  batch.add('ab', 'ab')
  batch.add('ba', 'ba')
  batch.add('bb', 'bb')
  batch.add('ac', 'ac')
  await batch.write()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: b4a.from('aa'), value: b4a.from('aa') },
    { key: b4a.from('ab'), value: b4a.from('ab') },
    { key: b4a.from('ac'), value: b4a.from('ac') }
  ])

  await db.close()
})

test('prefix iterator, reverse', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  batch.add('aa', 'aa')
  batch.add('ab', 'ab')
  batch.add('ba', 'ba')
  batch.add('bb', 'bb')
  batch.add('ac', 'ac')
  await batch.write()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b', reverse: true })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: b4a.from('ac'), value: b4a.from('ac') },
    { key: b4a.from('ab'), value: b4a.from('ab') },
    { key: b4a.from('aa'), value: b4a.from('aa') }
  ])

  await db.close()
})

test('prefix iterator, reverse with limit', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch()

  batch.add('aa', 'aa')
  batch.add('ab', 'ab')
  batch.add('ba', 'ba')
  batch.add('bb', 'bb')
  batch.add('ac', 'ac')
  await batch.write()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b', reverse: true, limit: 1 })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: b4a.from('ac'), value: b4a.from('ac') }
  ])

  await db.close()
})

test('iterator with encoding', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.batch({ encoding: c.string })

  batch.add('a', 'hello')
  batch.add('b', 'world')
  batch.add('c', '!')
  await batch.write()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'c', encoding: c.string })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: 'a', value: 'hello' },
    { key: 'b', value: 'world' }
  ])

  await db.close()
})
