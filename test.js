const test = require('brittle')
const c = require('compact-encoding')
const RocksDB = require('.')

test('open + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.close()
})

test('write + read', async (t) => {
  const db = new RocksDB(await t.tmp())
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

test('write + read multiple batches', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  {
    const batch = db.write()
    const p = batch.put('hello', 'world')
    await batch.flush()
    batch.destroy()
    await t.execution(p)
  }

  for (let i = 0; i < 50; i++) {
    const batch = db.read()
    const p = batch.get('hello')
    await batch.flush()
    batch.destroy()
    t.alike(await p, Buffer.from('world'))
  }

  await db.close()
})

test('write + read multiple', async (t) => {
  const db = new RocksDB(await t.tmp())
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

test('write + flush', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  const p = batch.put('hello', 'world')
  await batch.flush()
  batch.destroy()
  await t.execution(p)

  await db.flush()
  await db.close()
})

test('read missing', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.read()
  const p = batch.get('hello')
  await batch.flush()
  batch.destroy()
  t.alike(await p, null)

  await db.close()
})

test('read + autoDestroy', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.read({ autoDestroy: true })
  const p = batch.get('hello')
  await batch.flush()
  t.alike(await p, null)

  await db.close()
})

test('read with snapshot', async (t) => {
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())
  await db.ready()

  {
    const batch = db.write()
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
  const db = new RocksDB(await t.tmp())
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

test('values option', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('aa', '')
  batch.put('ab', '')
  batch.put('ba', '')
  batch.put('bb', '')
  batch.put('ac', '')
  await batch.flush()
  batch.destroy()

  const entries = []

  for await (const entry of db.iterator({ gte: 'a', lt: 'b', values: false })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: Buffer.from('aa'), value: null },
    { key: Buffer.from('ab'), value: null },
    { key: Buffer.from('ac'), value: null }
  ])

  await db.close()
})

test('key iterator', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('aa', '')
  batch.put('ab', '')
  batch.put('ba', '')
  batch.put('bb', '')
  batch.put('ac', '')
  await batch.flush()
  batch.destroy()

  const keys = []

  for await (const key of db.keys({ gte: 'a', lt: 'b' })) {
    keys.push(key)
  }

  t.alike(keys, [Buffer.from('aa'), Buffer.from('ab'), Buffer.from('ac')])

  await db.close()
})

test('manual compaction', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ba', 'ba')
  batch.put('bb', 'bb')
  batch.put('ac', 'ac')
  await batch.flush()
  batch.destroy()

  await db.compactRange('ab', 'bb')
  await db.compactRange('bb', { exclusive: true })
  await db.compactRange()

  t.pass()

  await db.close()
})

test('approximate size', async (t) => {
  t.plan(2)

  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('aa', 'aa')
  batch.put('ab', 'ab')
  batch.put('ba', 'ba')
  batch.put('bb', 'bb')
  batch.put('ac', 'ac')
  await batch.flush()
  batch.destroy()

  {
    const result = await db.approximateSize('aa', 'bb', {
      includeMemtables: true,
      includeFiles: true
    })

    t.ok(result > 0)
  }

  {
    await t.exception(async () => {
      await db.approximateSize('aa', 'bb', {
        includeMemtables: false,
        includeFiles: false
      })
    }, /Invalid options/)
  }

  await db.close()
})

test('prefix iterator, reverse', async (t) => {
  const db = new RocksDB(await t.tmp())
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

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' }, { reverse: true })) {
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
  const db = new RocksDB(await t.tmp())
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

  for await (const entry of db.iterator({ gte: 'a', lt: 'b' }, { reverse: true, limit: 1 })) {
    entries.push(entry)
  }

  t.alike(entries, [{ key: Buffer.from('ac'), value: Buffer.from('ac') }])

  await db.close()
})

test('iterator with encoding', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const session = db.session({ keyEncoding: c.string, valueEncoding: c.string })
  const batch = session.write()
  batch.put('a', 'hello')
  batch.put('b', 'world')
  batch.put('c', '!')
  await batch.flush()
  batch.destroy()

  const entries = []

  for await (const entry of session.iterator({ gte: 'a', lt: 'c' })) {
    entries.push(entry)
  }

  t.alike(entries, [
    { key: 'a', value: 'hello' },
    { key: 'b', value: 'world' }
  ])

  await session.close()
  await db.close()
})

test('iterator with snapshot', async (t) => {
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())

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

test('destroy iterator immediately', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const it = db.iterator({ gte: 'a', lt: 'b' })
  it.destroy()

  t.pass()

  await db.close()
})

test('destroy snapshot before db open', async (t) => {
  const db = new RocksDB(await t.tmp())

  const snapshot = db.snapshot()
  await snapshot.close()

  await db.ready()
  await db.close()
})

test('peek', async (t) => {
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())
  await db.ready()

  {
    const batch = db.write()
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
  const db = new RocksDB(await t.tmp())
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
  const db = new RocksDB(await t.tmp())
  await db.ready()

  await db.close()

  t.exception(() => db.read())
  t.exception(() => db.write())
})

test('session reuse after close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const session = db.session()
  const read = session.read({ autoDestroy: true })

  read.get('key')
  read.tryFlush()

  await session.close()

  t.exception(() => session.read())
  t.exception(() => session.write())

  await t.execution(db.close())
})

test('put + get', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  await db.put('key', 'value')
  t.alike(await db.get('key'), Buffer.from('value'))

  await db.close()
})

test('put + delete + get', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  await db.put('key', 'value')
  await db.delete('key')
  t.alike(await db.get('key'), null)

  await db.close()
})

test('column families, batch per family', async (t) => {
  const db = new RocksDB(await t.tmp(), { columnFamilies: ['a', 'b'] })
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

test('column families setup implicitly', async (t) => {
  const db = new RocksDB(await t.tmp())

  const a = db.columnFamily('a')
  const b = db.columnFamily('b')

  await b.put('hello', 'world')
  t.is(await a.get('hello'), null)
  t.alike(await b.get('hello'), Buffer.from('world'))

  await a.close()
  await b.close()
  await db.close()
})

test('read-only', async (t) => {
  const dir = await t.tmp()

  const w = new RocksDB(dir)
  await w.ready()

  {
    const batch = w.write()
    const p = batch.put('hello', 'world')
    await batch.flush()
    batch.destroy()
    await t.execution(p)
  }

  const r = new RocksDB(dir, { readOnly: true })
  await r.ready()

  {
    const batch = r.read()
    const p = batch.get('hello')
    await batch.flush()
    batch.destroy()
    t.alike(await p, Buffer.from('world'))
  }

  await w.close()
  await r.close()
})

test('read-only + write', async (t) => {
  const dir = await t.tmp()

  const w = new RocksDB(dir)
  await w.ready()

  const r = new RocksDB(dir, { readOnly: true })
  await r.ready()

  const batch = r.write()
  const p = batch.put('hello', 'world')
  await t.exception(batch.flush(), /Batch was not applied/)
  batch.destroy()
  await t.exception(p, /Not supported operation in read only mode/)

  await w.close()
  await r.close()
})

test('suspend + resume', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()
  await db.resume()
  await db.close()
})

test('suspend + resume + close before resolved', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  db.suspend()
  db.resume()
  await db.close()
})

test('suspend + resume + write', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()
  await db.resume()
  {
    const w = db.write({ autoDestroy: true })
    w.put('hello2', 'world2')
    await w.flush()
  }
  await db.close()
})

test('suspend + write + flush + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()
  {
    const w = db.write()
    const p = w.flush()
    p.catch(() => {})
  }
  await db.close()
})

test('suspend + close without resume', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()
  await db.close()
})

test('suspend + read', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()

  let flushed = false

  const batch = db.read()
  const p = batch.get('hello')
  batch.flush().then(
    () => (flushed = true),
    () => {}
  )

  await wait(250)
  t.is(flushed, false)

  p.catch(() => {}) // Will abort due to close during suspend
  await db.close()
})

test('resume + resume + suspend + resume', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  await wait(250)

  await db.resume()
  await db.resume()

  await db.suspend()
  await db.resume()

  const batch = db.write()
  batch.tryPut('hello', 'world')
  await batch.flush()
  batch.destroy()

  await db.close()
})

test('suspend + write', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()

  let flushed = false

  const batch = db.write()
  const p = batch.put('hello', 'world')
  batch.flush().then(
    () => (flushed = true),
    () => {}
  )

  await wait(250)
  t.is(flushed, false)

  p.catch(() => {}) // Will abort due to close during suspend
  await db.close()
})

test('suspend + write + resume + suspend before fully resumed', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()

  let flushed = false

  const batch = db.write()
  const p = batch.put('hello', 'world')
  batch.flush().then(() => {
    flushed = true
  })

  db.resume()
  await 1 // give it time to tick
  await db.suspend()

  await wait(250)
  t.is(flushed, true)
  batch.destroy()

  p.catch(() => {}) // Will abort due to close during suspend
  await db.close()
})

test('iterator + suspend', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('hello', 'world')
  await batch.flush()
  batch.destroy()

  const it = db.iterator({ gte: 'hello', lt: 'z' })

  await db.suspend()

  const gen = it[Symbol.asyncIterator]()
  const p = gen.next()

  await db.resume()

  t.alike(await p, {
    value: {
      key: Buffer.from('hello'),
      value: Buffer.from('world')
    },
    done: false
  })

  await db.close()
})

test('iterator + suspend + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()
  batch.put('hello', 'world')
  await batch.flush()
  batch.destroy()

  const it = db.iterator({ gte: 'hello', lt: 'z' })

  await db.suspend()

  const gen = it[Symbol.asyncIterator]()
  const p = gen.next()

  p.catch(() => {})
  await db.close()

  await t.exception(p)
})

test('make a batch, queue read, destroy + suspend + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.read()

  batch.get(Buffer.from('hello')).catch(noop)
  batch.destroy()

  await db.suspend()
  await db.close()
})

test('make a batch, queue write, destroy + suspend + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()

  batch.put(Buffer.from('hello'), Buffer.from('world')).catch(noop)
  batch.destroy()

  await db.suspend()
  await db.close()
})

test('make a batch, queue delete, destroy + suspend + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()

  const batch = db.write()

  batch.delete(Buffer.from('hello')).catch(noop)
  batch.destroy()

  await db.suspend()
  await db.close()
})

test('suspend + open new writer', async (t) => {
  const dir = await t.tmp()

  const w1 = new RocksDB(dir)
  await w1.ready()
  await w1.suspend()

  const w2 = new RocksDB(dir)
  await w2.ready()

  await t.exception(w1.resume())

  await w2.close()
  await t.execution(w1.resume())

  await w1.close()
})

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve), ms)
}

test('suspend + flush + close', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()

  let flushed = true

  db.flush().catch(() => {
    flushed = false
  })

  await db.close()

  t.is(flushed, false)
})

test('suspend + flush + resume', async (t) => {
  const db = new RocksDB(await t.tmp())
  await db.ready()
  await db.suspend()

  const p = db.flush()
  await db.resume()
  await p

  t.pass()

  await db.close()
})

function noop() {}
