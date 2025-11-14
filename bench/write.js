const crypto = require('crypto')
const bench = require('./harness')

module.exports = async function writeBenchmark(t, db, benchOpts) {
  const test = t.test('Writing')
  test.plan(1)

  let keysWrote = 0

  const result = await bench(async () => {
    const batch = db.write()
    const key = String(keysWrote)
    const p = batch.put(key, crypto.randomBytes(32))
    await batch.flush()
    batch.destroy()
    await p

    keysWrote++
  }, benchOpts)

  test.comment('Writing performance:', result, 'ops/s')
  test.comment('Keys wrote:', keysWrote)
  test.pass()

  return keysWrote
}
