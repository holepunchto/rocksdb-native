const test = require('brittle')
const tmp = require('test-tmp')
const c = require('compact-encoding')
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
    batch.destroy()
    await t.execution(p)
  }
  {
    const batch = db.read()
    const p = batch.get('hello')
    await batch.flush()
    batch.destroy()
    t.alike(await p, Buffer.from('world'))
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
    batch.destroy()

    await t.execution(await Promise.all(p))
  }
  {
    const batch = db.read()
    const p = []

    for (let i = 0; i < 100; i++) {
      p.push(batch.get(`${i}`))
    }

    await batch.flush()
    batch.destroy()

    t.alike(
      await Promise.all(p),
      new Array(100).fill(0).map((_, i) => Buffer.from(`${i}`))
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
  batch.destroy()
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
    batch.destroy()
    await t.execution(p)
  }

  const snapshot = db.snapshot()

  {
    const batch = db.write()
    const p = batch.put('hello', 'earth')
    await batch.flush()
    batch.destroy()
    await t.execution(p)
  }
  {
    const batch = snapshot.read()
    const p = batch.get('hello')
    await batch.flush()
    batch.destroy()
    t.alike(await p, Buffer.from('world'))
  }

  await snapshot.close()
  await db.close()
})

test('delete range', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    let batch = db.write()
    batch.put('aa', 'aa')
    batch.put('ab', 'ab')
    batch.put('ba', 'ba')
    batch.put('bb', 'bb')
    batch.put('bc', 'bc')
    batch.put('ac', 'ac')
    await batch.flush()

    batch.deleteRange('a', 'b')
    await batch.flush()
    batch.destroy()
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
    batch.destroy()

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
    let batch = db.write()
    batch.put('aa', 'aa')
    batch.put('ab', 'ab')
    batch.put('ac', 'ac')
    await batch.flush()

    batch.deleteRange('a', 'b')
    await batch.flush()
    batch.destroy()
  }
  {
    const batch = db.read()
    const p = []
    p.push(batch.get('aa'))
    p.push(batch.get('ab'))
    p.push(batch.get('ac'))
    await batch.flush()
    batch.destroy()

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
  batch.destroy()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: Buffer.from('aa'), value: Buffer.from('aa') },
    { key: Buffer.from('ab'), value: Buffer.from('ab') },
    { key: Buffer.from('ac'), value: Buffer.from('ac') }
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
  batch.destroy()

  const entries = []

  for await (const entry of db.iterator(
    { gte: 'a', lt: 'b' },
    { reverse: true }
  )) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: Buffer.from('ac'), value: Buffer.from('ac') },
    { key: Buffer.from('ab'), value: Buffer.from('ab') },
    { key: Buffer.from('aa'), value: Buffer.from('aa') }
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
  batch.destroy()

  const entries = []

  for await (const entry of db.iterator(
    { gte: 'a', lt: 'b' },
    { reverse: true, limit: 1 }
  )) {
    entries.push(entry)
  }

  t.alike(entries, [{ key: Buffer.from('ac'), value: Buffer.from('ac') }])

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
  batch.destroy()

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

  let batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ac', 'ac')
  await batch.flush()

  const snapshot = db.snapshot()

  batch.put('aa', 'ba')
  batch.put('ab', 'bb')
  batch.put('ac', 'bc')
  await batch.flush()
  batch.destroy()

  const entries = []

  for await (const entry of snapshot.iterator({ gte: 'a', lt: 'b' })) {
    entries.push(entry)
  }

  await snapshot.close()

  t.alike(entries, [
    { key: Buffer.from('aa'), value: Buffer.from('aa') },
    { key: Buffer.from('ab'), value: Buffer.from('ab') },
    { key: Buffer.from('ac'), value: Buffer.from('ac') }
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
  batch.destroy()

  const entries = []

  for await (const entry of snapshot.iterator({ gte: 'a', lt: 'b' })) {
    entries.push(entry)
  }

  await snapshot.close()

  t.alike(entries, [])

  await db.close()
})

test('destroy snapshot before db open', async (t) => {
  const db = new RocksDB(await tmp(t))

  const snapshot = db.snapshot()
  await snapshot.close()

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
  batch.destroy()

  t.alike(await db.peek({ gte: 'a', lt: 'b' }), {
    key: Buffer.from('aa'),
    value: Buffer.from('aa')
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
  batch.destroy()

  t.alike(await db.peek({ gte: 'a', lt: 'b' }, { reverse: true }), {
    key: Buffer.from('ac'),
    value: Buffer.from('ac')
  })

  await db.close()
})

test('delete', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  {
    let batch = db.write()
    batch.put('hello', 'world')
    batch.put('next', 'value')
    batch.put('another', 'entry')
    await batch.flush()

    batch.delete('hello')
    batch.delete('next')
    batch.delete('another')
    await batch.flush()
    batch.destroy()
  }
  {
    const batch = db.read()
    const p = []
    p.push(batch.get('hello'))
    p.push(batch.get('next'))
    p.push(batch.get('another'))
    await batch.flush()
    batch.destroy()
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
    b.destroy()

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

    t.alike(await node1, Buffer.from('world'))
    t.alike(await node2, Buffer.from('value'))
    t.alike(await node3, Buffer.from('entry'))

    b1.destroy()
    b2.destroy()
    b3.destroy()

    await t.execution(promise)

    t.ok(idle)
    t.ok(db.isIdle())
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

test('put + get', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  await db.put('key', 'value')
  t.alike(await db.get('key'), Buffer.from('value'))

  await db.close()
})

test('put + delete + get', async (t) => {
  const db = new RocksDB(await tmp(t))
  await db.ready()

  await db.put('key', 'value')
  await db.delete('key')
  t.alike(await db.get('key'), null)

  await db.close()
})

test('column families, batch per family', async (t) => {
  const db = new RocksDB(await tmp(t), { columnFamilies: ['a', 'b'] })
  await db.ready()

  const a = db.session({ columnFamily: 'a' })
  const b = db.session({ columnFamily: 'b' })

  {
    const batch = a.write()
    batch.put('key', 'a')
    await batch.flush()
    batch.destroy()
  }
  {
    const batch = b.write()
    batch.put('key', 'b')
    await batch.flush()
    batch.destroy()
  }

  {
    const batch = a.read()
    const p = batch.get('key')
    batch.tryFlush()
    t.alike(await p, Buffer.from('a'))
    batch.destroy()
  }
  {
    const batch = b.read()
    const p = batch.get('key')
    batch.tryFlush()
    t.alike(await p, Buffer.from('b'))
    batch.destroy()
  }

  await a.close()
  await b.close()
  await db.close()
})
