const test = require('brittle')
const Database = require('.')

test('open + close', async (t) => {
  const db = new Database('test/fixtures/rocks.db')
  await db.ready()

  await db.close()

  t.pass()
})

test('put + get', async (t) => {
  const db = new Database('test/fixtures/rocks.db')
  await db.ready()
  t.teardown(() => db.close())

  await db.put(Buffer.from('key'), Buffer.from('value'))

  t.alike(await db.get(Buffer.from('key')), Buffer.from('value'))
})

test('put + delete + get', async (t) => {
  const db = new Database('test/fixtures/rocks.db')
  await db.ready()
  t.teardown(() => db.close())

  await db.put(Buffer.from('key'), Buffer.from('value'))

  await db.delete(Buffer.from('key'))

  t.absent(await db.get(Buffer.from('key')))
})
