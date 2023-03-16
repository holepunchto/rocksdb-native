const binding = require('./binding')

module.exports = class Database {
  constructor (name) {
    this._handle = binding.open(name)
  }

  get (key) {
    const value = binding.get(this._handle, key)
    return value ? Buffer.from(value) : null
  }

  put (key, value) {
    binding.put(this._handle, key, value)
  }

  delete (key) {
    binding.delete(this._handle, key)
  }

  close () {
    binding.close(this._handle)
  }
}
