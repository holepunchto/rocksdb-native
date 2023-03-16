const test = require('brittle')
const Database = require('.')

test('open + close', (t) => {
  const db = new Database('test/fixtures/rocks.db')

  db.close()

  t.pass()
})

test('put + get', (t) => {
  const db = new Database('test/fixtures/rocks.db')

  db.put(Buffer.from('key'), Buffer.from('value'))

  t.alike(db.get(Buffer.from('key')), Buffer.from('value'))

  db.close()
})

test('put + delete + get', (t) => {
  const db = new Database('test/fixtures/rocks.db')

  db.put(Buffer.from('key'), Buffer.from('value'))

  db.delete(Buffer.from('key'))

  t.absent(db.get(Buffer.from('key')))

  db.close()
})
