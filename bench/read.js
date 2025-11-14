const bench = require('./harness')

module.exports = function readBenchmark(t, db, keysLimit, opts) {
  t.test('Reading', async (t) => {
    t.plan(1)

    let keysRead = 0

    const result = await bench(async () => {
      const batch = db.read()
      const key = Buffer.from([getRandomValue(keysLimit)])
      const p = batch.get(key)
      await batch.flush()
      batch.destroy()
      await p

      keysRead++
    }, opts)

    t.comment('Reading performance:', result, 'ops/s')
    t.comment('Keys read:', keysRead)
    t.pass()
  })
}

const getRandomValue = (max) => Math.floor(Math.random() * max)
