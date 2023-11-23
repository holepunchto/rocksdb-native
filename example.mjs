import Rocks from './index.js'

const db = new Rocks('./example.db')

await db.put('hello', 'world')

console.log(await db.get('hello'))
