import Rocks from './index.js'

const db = new Rocks('./example.db')

const b = db.batch()

b.add('hello', 'world')
await b.write()

const p = b.add('hello')
b.read()

console.log(await p)
