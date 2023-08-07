const binding = process.addon(__dirname)

const handle = Buffer.alloc(binding.sizeof_rocksdb_native_t)
const buffer = Buffer.alloc(65536)

binding.rocksdb_native_init(handle, './sandbox/db')

binding.rocksdb_native_put(handle, Buffer.from('hello'), Buffer.from('world'))

// for (let i = 0; i < 1e2; i++) {
//   console.time()
  const size = binding.rocksdb_native_get(handle, Buffer.from('hello'), buffer)
  console.log(buffer.subarray(0, size).toString())
  // console.timeEnd()
// }
