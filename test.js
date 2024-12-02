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

  {
    const batch = db.write()
    const p = batch.put('hello', 'world')
    await batch.flush()
    await t.execution(p)
  }
  {
    const batch = db.read()
    const p = batch.get('hello')
    await batch.flush()
    t.alike(await p, b4a.from('world'))
  }

  await db.close()
})

test('write + read multiple', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.write()
    const p = []

    for (let i = 0; i < 100; i++) {
      p.push(batch.put(`${i}`, `${i}`))
    }

    await batch.flush()

    await t.execution(await Promise.all(p))
  }
  {
    const batch = db.read()
    const p = []

    for (let i = 0; i < 100; i++) {
      p.push(batch.get(`${i}`))
    }

    await batch.flush()

    t.alike(
      await Promise.all(p),
      new Array(100).fill(0).map((_, i) => b4a.from(`${i}`))
    )
  }

  await db.close()
})

test('read missing', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.read()
  const p = batch.get('hello')
  await batch.flush()
  t.alike(await p, null)

  await db.close()
})

test('read with snapshot', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.write()
    const p = batch.put('hello', 'world')
    await batch.flush()
    await t.execution(p)
  }

  const snapshot = db.snapshot()

  {
    const batch = db.write()
    const p = batch.put('hello', 'earth')
    await batch.flush()
    await t.execution(p)
  }
  {
    const batch = db.read({ snapshot })
    const p = batch.get('hello')
    await batch.flush()
    t.alike(await p, b4a.from('world'))
  }

  await db.close()
})

test('delete range', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.write()
    batch.put('aa', 'aa')
    batch.put('ab', 'ab')
    batch.put('ba', 'ba')
    batch.put('bb', 'bb')
    batch.put('bc', 'bc')
    batch.put('ac', 'ac')
    await batch.flush()
  }
  {
    const batch = db.write()
    batch.deleteRange('a', 'b')
    await batch.flush()
  }
  {
    const batch = db.read()
    const p = []
    p.push(batch.get('aa'))
    p.push(batch.get('ab'))
    p.push(batch.get('ac'))
    p.push(batch.get('ba'))
    p.push(batch.get('bb'))
    p.push(batch.get('bc'))
    await batch.flush()

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

test('delete range, end does not exist', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.write()
    batch.put('aa', 'aa')
    batch.put('ab', 'ab')
    batch.put('ac', 'ac')
    await batch.flush()
  }
  {
    const batch = db.write()
    batch.deleteRange('a', 'b')
    await batch.flush()
  }
  {
    const batch = db.read()
    const p = []
    p.push(batch.get('aa'))
    p.push(batch.get('ab'))
    p.push(batch.get('ac'))
    await batch.flush()

    t.alike(await Promise.all(p), [null, null, null])
  }

  await db.close()
})

test('prefix iterator', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ba', 'ba')
  batch.put('bb', 'bb')
  batch.put('ac', 'ac')
  await batch.flush()

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

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ba', 'ba')
  batch.put('bb', 'bb')
  batch.put('ac', 'ac')
  await batch.flush()

  const entries = []

  for await (const entry of db.iterator(
    { gte: 'a', lt: 'b' },
    { reverse: true }
  )) {
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

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ba', 'ba')
  batch.put('bb', 'bb')
  batch.put('ac', 'ac')
  await batch.flush()

  const entries = []

  for await (const entry of db.iterator(
    { gte: 'a', lt: 'b' },
    { reverse: true, limit: 1 }
  )) {
    entries.push(entry)
  }

  t.alike(entries, [{ key: b4a.from('ac'), value: b4a.from('ac') }])

  await db.close()
})

test('iterator with encoding', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.write({ encoding: c.string })
  batch.put('a', 'hello')
  batch.put('b', 'world')
  batch.put('c', '!')
  await batch.flush()

  const entries = []

  for await (const entry of db.iterator(
    { gte: 'a', lt: 'c' },
    { encoding: c.string }
  )) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: 'a', value: 'hello' },
    { key: 'b', value: 'world' }
  ])

  await db.close()
})

test('iterator with snapshot', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ac', 'ac')
  await batch.flush()

  const snapshot = db.snapshot()

  batch.put('aa', 'ba')
  batch.put('ab', 'bb')
  batch.put('ac', 'bc')
  await batch.flush()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' }, { snapshot })) {
    entries.push(entry)
  }

  snapshot.destroy()

  t.alike(entries, [
    { key: b4a.from('aa'), value: b4a.from('aa') },
    { key: b4a.from('ab'), value: b4a.from('ab') },
    { key: b4a.from('ac'), value: b4a.from('ac') }
  ])

  await db.close()
})

test('iterator with snapshot before db open', async (t) => {
  const db = new RocksDB(await tmp(t))

  const snapshot = db.snapshot()

  await db.ready()

  const batch = db.write()
  batch.put('aa', 'ba')
  batch.put('ab', 'bb')
  batch.put('ac', 'bc')
  await batch.flush()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' }, { snapshot })) {
    entries.push(entry)
  }

  snapshot.destroy()

  t.alike(entries, [])

  await db.close()
})

test('destroy snapshot before db open', async (t) => {
  const db = new RocksDB(await tmp(t))

  const snapshot = db.snapshot()
  snapshot.destroy()

  await db.ready()
  await db.close()
})

test('peek', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ac', 'ac')
  await batch.flush()

  t.alike(await db.peek({ gte: 'a', lt: 'b' }), {
    key: b4a.from('aa'),
    value: b4a.from('aa')
  })

  await db.close()
})

test('peek, reverse', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ac', 'ac')
  await batch.flush()

  t.alike(await db.peek({ gte: 'a', lt: 'b' }, { reverse: true }), {
    key: b4a.from('ac'),
    value: b4a.from('ac')
  })

  await db.close()
})

test('delete', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    const batch = db.write()
    batch.put('hello', 'world')
    batch.put('next', 'value')
    batch.put('another', 'entry')
    await batch.flush()
  }
  {
    const batch = db.write()
    batch.delete('hello')
    batch.delete('next')
    batch.delete('another')
    await batch.flush()
  }
  {
    const batch = db.read()
    const p = []
    p.push(batch.get('hello'))
    p.push(batch.get('next'))
    p.push(batch.get('another'))
    await batch.flush()
    t.alike(await Promise.all(p), [null, null, null])
  }

  await db.close()
})

test('idle', async function (t) {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  t.ok(db.isIdle())

  {
    const b = db.write()
    b.put('hello', 'world')
    b.put('next', 'value')
    b.put('another', 'entry')

    t.absent(db.isIdle())
    const idle = db.idle()

    await b.flush()

    await t.execution(idle)
    t.ok(db.isIdle())
  }

  {
    let idle = false

    const b1 = db.read()
    const b2 = db.read()
    const b3 = db.read()

    const node1 = b1.get('hello')
    const node2 = b2.get('next')
    const node3 = b3.get('another')

    const promise = db.idle().then(() => {
      idle = true
    })

    b1.tryFlush()

    t.absent(idle)
    t.absent(db.isIdle())

    b2.tryFlush()

    t.absent(idle)
    t.absent(db.isIdle())

    b3.tryFlush()

    await t.execution(promise)

    t.ok(idle)
    t.ok(db.isIdle())

    t.alike(await node1, b4a.from('world'))
    t.alike(await node2, b4a.from('value'))
    t.alike(await node3, b4a.from('entry'))
  }

  await db.close()
})

test('write + read after close', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  await db.close()

  t.exception(() => db.read())
  t.exception(() => db.write())
})
