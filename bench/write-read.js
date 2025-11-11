const { configure, test } = require('brittle')
const bench = require('./harness')
const RocksDB = require('..')

configure({ timeout: 100000 })

test('write + read', async (t) => {
  const tmp = await t.tmp()

  const db = new RocksDB(tmp)
  await db.ready()

  const writeResult = await bench(async () => {
    const batch = db.write()
    const value = getRandomValue()
    const p = batch.put(String(value), Buffer.alloc(value))
    await batch.flush()
    batch.destroy()
    await p
  })

  t.comment('write -', writeResult, 'ops/s')

  const keys = []
  for await (const key of db.keys()) {
    keys.push(key)
  }

  const databaseLength = keys.length

  const readResult = await bench(async () => {
    const batch = db.read()
    const key = keys[getRandomValue(0, databaseLength - 1)]
    const p = batch.get(key)
    await batch.flush()
    batch.destroy()
    await p
  })

  t.comment('read -', readResult, 'ops/s')

  await db.close()
})

function getRandomValue(min = 262144, max = 524288) {
  return Math.floor(Math.random() * (max - min) + min)
}
