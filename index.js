const path = require('path')
const ReadyResource = require('ready-resource')
const binding = require('./binding')

module.exports = class Database extends ReadyResource {
  constructor (name) {
    super()

    this._name = path.resolve(name)
    this._handle = null
    this._reqs = []
  }

  async _open () {
    const req = binding.createOpenReq()

    const ctx = {
      self: this,
      req,
      resolve: null,
      reject: null
    }

    const promise = new Promise((resolve, reject) => {
      ctx.resolve = resolve
      ctx.reject = reject
    })

    binding.open(req, this._name, ctx, onopen)

    return promise
  }

  async _close () {
    const req = binding.createCloseReq()

    const ctx = {
      self: this,
      req,
      resolve: null,
      reject: null
    }

    const promise = new Promise((resolve, reject) => {
      ctx.resolve = resolve
      ctx.reject = reject
    })

    binding.close(req, this._handle, ctx, onclose)

    return promise
  }

  async get (key) {
    await this.ready()

    const req = this._getReq()

    const ctx = {
      self: this,
      req,
      resolve: null,
      reject: null,
      key
    }

    const promise = new Promise((resolve, reject) => {
      ctx.resolve = resolve
      ctx.reject = reject
    })

    binding.get(req, this._handle, key, ctx, onget)

    return promise
  }

  async put (key, value) {
    await this.ready()

    const req = this._getReq()

    const ctx = {
      self: this,
      req,
      resolve: null,
      reject: null,
      key,
      value
    }

    const promise = new Promise((resolve, reject) => {
      ctx.resolve = resolve
      ctx.reject = reject
    })

    binding.put(req, this._handle, key, value, ctx, onput)

    return promise
  }

  async delete (key) {
    await this.ready()

    const req = this._getReq()

    const ctx = {
      self: this,
      req,
      resolve: null,
      reject: null,
      key
    }

    const promise = new Promise((resolve, reject) => {
      ctx.resolve = resolve
      ctx.reject = reject
    })

    binding.delete(req, this._handle, key, ctx, ondelete)

    return promise
  }

  _getReq () {
    return this._reqs.length ? this._reqs.pop() : binding.createKeyValueReq()
  }

  _releaseReq (req) {
    this._reqs.push(req)
  }
}

function onopen (err, handle) {
  const {
    self,
    resolve,
    reject
  } = this

  if (err) reject(err)
  else {
    self._handle = handle
    resolve()
  }
}

function onclose (err) {
  const {
    self,
    resolve,
    reject
  } = this

  if (err) reject(err)
  else {
    self._handle = null
    resolve()
  }
}

function onget (err, buf) {
  const {
    self,
    req,
    resolve,
    reject
  } = this

  self._releaseReq(req)

  if (err) reject(err)
  else if (buf) resolve(Buffer.from(buf))
  else resolve(null)
}

function onput (err) {
  const {
    self,
    req,
    resolve,
    reject
  } = this

  self._releaseReq(req)

  if (err) reject(err)
  else resolve()
}

function ondelete (err) {
  const {
    self,
    req,
    resolve,
    reject
  } = this

  self._releaseReq(req)

  if (err) reject(err)
  else resolve()
}
