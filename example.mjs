import Rocks from './index.js'

const db = new Rocks('./example.db')

const w = db.write()
w.put('hello', 'world')
await w.flush()

const r = db.read()
const p = r.get('hello')
r.flush()

console.log(await p)
