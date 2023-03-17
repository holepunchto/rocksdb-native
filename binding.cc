#include <assert.h>
#include <js.h>
#include <pear.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

using namespace rocksdb;

typedef struct {
  DB *db;
} binding_t;

typedef struct {
  uv_work_t req;
  int status;
  DB *db;
  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_after_work;
} binding_req_t;

typedef struct {
  binding_req_t base;
  char path[4097];
} binding_open_req_t;

typedef struct {
  binding_req_t base;
} binding_close_req_t;

typedef struct {
  binding_req_t base;
  Slice key;
  PinnableSlice value;
} binding_key_value_req_t;

static inline js_value_t *
binding_create_error (js_env_t *env, int status) {
  const char *str;

  switch (status) {
  case Status::kNotFound:
    str = "Not found";
    break;
  case Status::kCorruption:
    str = "Corruption";
    break;
  case Status::kNotSupported:
    str = "Not supported";
    break;
  case Status::kInvalidArgument:
    str = "Invalid argument";
    break;
  case Status::kIOError:
    str = "I/O error";
    break;
  case Status::kMergeInProgress:
    str = "Merge in progress";
    break;
  case Status::kIncomplete:
    str = "Incomplete";
    break;
  case Status::kShutdownInProgress:
    str = "Shutdown in progress";
    break;
  case Status::kTimedOut:
    str = "Timed out";
    break;
  case Status::kAborted:
    str = "Aborted";
    break;
  case Status::kBusy:
    str = "Busy";
    break;
  case Status::kExpired:
    str = "Expired";
    break;
  case Status::kTryAgain:
    str = "Try again";
    break;
  case Status::kCompactionTooLarge:
    str = "Compaction too large";
    break;
  case Status::kColumnFamilyDropped:
    str = "Column family dropped";
    break;
  }

  int err;

  js_value_t *msg;
  err = js_create_string_utf8(env, str, -1, &msg);
  assert(err == 0);

  js_value_t *result;
  err = js_create_error(env, nullptr, msg, &result);
  assert(err == 0);

  return result;
}

static js_value_t *
binding_create_open_req (js_env_t *env, js_callback_info_t *info) {
  int err;

  binding_open_req_t *req;

  js_value_t *result;
  err = js_create_arraybuffer(env, sizeof(binding_open_req_t), (void **) &req, &result);
  if (err < 0) return nullptr;

  req->base.env = env;
  req->base.req.data = (void *) req;

  return result;
}

static js_value_t *
binding_create_close_req (js_env_t *env, js_callback_info_t *info) {
  int err;

  binding_close_req_t *req;

  js_value_t *result;
  err = js_create_arraybuffer(env, sizeof(binding_open_req_t), (void **) &req, &result);
  if (err < 0) return nullptr;

  req->base.env = env;
  req->base.req.data = (void *) req;

  return result;
}

static js_value_t *
binding_create_key_value_req (js_env_t *env, js_callback_info_t *info) {
  int err;

  binding_key_value_req_t *req;

  js_value_t *result;
  err = js_create_arraybuffer(env, sizeof(binding_key_value_req_t), (void **) &req, &result);
  if (err < 0) return nullptr;

  req->base.env = env;
  req->base.req.data = (void *) req;

  req->value = PinnableSlice();

  return result;
}

static void
on_open (uv_work_t *uv_req) {
  binding_open_req_t *req = (binding_open_req_t *) uv_req->data;

  Options options;
  options.create_if_missing = true;

  Status status = DB::Open(options, req->path, &req->base.db);

  if (status.ok()) {
    req->base.status = 0;
  } else {
    req->base.status = status.code();
  }
}

static void
on_after_open (uv_work_t *uv_req, int status) {
  binding_open_req_t *req = (binding_open_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  js_value_t *argv[2];

  if (req->base.status == 0) {
    binding_t *handle;

    err = js_create_arraybuffer(env, sizeof(binding_t), (void **) &handle, &argv[1]);
    assert(err == 0);

    handle->db = req->base.db;

    err = js_get_null(env, &argv[0]);
    assert(err == 0);
  } else {
    argv[0] = binding_create_error(env, req->base.status);

    err = js_get_null(env, &argv[1]);
    assert(err == 0);
  }

  js_value_t *ctx;
  err = js_get_reference_value(env, req->base.ctx, &ctx);
  assert(err == 0);

  js_value_t *on_after_work;
  err = js_get_reference_value(env, req->base.on_after_work, &on_after_work);
  assert(err == 0);

  err = js_call_function(env, ctx, on_after_work, 2, argv, nullptr);
  assert(err == 0);

  err = js_delete_reference(env, req->base.on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.ctx);
  assert(err == 0);
}

static js_value_t *
binding_open (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 4;
  js_value_t *argv[4];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 4);

  binding_open_req_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, nullptr);
  if (err < 0) return nullptr;

  err = js_get_value_string_utf8(env, argv[1], req->path, 4097, nullptr);
  if (err < 0) return nullptr;

  err = js_create_reference(env, argv[2], 1, &req->base.ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[3], 1, &req->base.on_after_work);
  assert(err == 0);

  uv_loop_t *loop;
  js_get_env_loop(env, &loop);

  uv_queue_work(loop, &req->base.req, on_open, on_after_open);

  return nullptr;
}

static void
on_close (uv_work_t *uv_req) {
  binding_close_req_t *req = (binding_close_req_t *) uv_req->data;

  Status status = req->base.db->Close();

  if (status.ok()) {
    req->base.status = 0;
  } else {
    req->base.status = status.code();
  }

  delete req->base.db;
}

static void
on_after_close (uv_work_t *uv_req, int status) {
  binding_close_req_t *req = (binding_close_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  js_value_t *argv[1];

  if (req->base.status == 0) {
    err = js_get_null(env, &argv[0]);
    assert(err == 0);
  } else {
    argv[0] = binding_create_error(env, req->base.status);
  }

  js_value_t *ctx;
  err = js_get_reference_value(env, req->base.ctx, &ctx);
  assert(err == 0);

  js_value_t *on_after_work;
  err = js_get_reference_value(env, req->base.on_after_work, &on_after_work);
  assert(err == 0);

  err = js_call_function(env, ctx, on_after_work, 1, argv, nullptr);
  assert(err == 0);

  err = js_delete_reference(env, req->base.on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.ctx);
  assert(err == 0);
}

static js_value_t *
binding_close (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 4;
  js_value_t *argv[4];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 4);

  binding_open_req_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, nullptr);
  if (err < 0) return nullptr;

  binding_t *handle;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  err = js_create_reference(env, argv[2], 1, &req->base.ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[3], 1, &req->base.on_after_work);
  assert(err == 0);

  req->base.db = handle->db;

  uv_loop_t *loop;
  js_get_env_loop(env, &loop);

  uv_queue_work(loop, &req->base.req, on_close, on_after_close);

  return nullptr;
}

static void
on_get (uv_work_t *uv_req) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  Status status = req->base.db->Get(ReadOptions(), req->base.db->DefaultColumnFamily(), req->key, &req->value);

  if (status.ok()) {
    req->base.status = 0;
  } else {
    req->base.status = status.code();
  }
}

static void
on_after_get (uv_work_t *uv_req, int status) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  js_value_t *argv[2];

  if (req->base.status == 0) {
    std::string *value = req->value.GetSelf();

    void *data;
    err = js_create_arraybuffer(env, value->size(), &data, &argv[1]);
    assert(err == 0);

    memcpy(data, value->data(), value->size());

    err = js_get_null(env, &argv[0]);
    assert(err == 0);
  } else if (req->base.status == Status::kNotFound) {
    err = js_get_null(env, &argv[0]);
    assert(err == 0);

    err = js_get_null(env, &argv[1]);
    assert(err == 0);
  } else {
    argv[0] = binding_create_error(env, req->base.status);

    err = js_get_null(env, &argv[1]);
    assert(err == 0);
  }

  req->value.Reset();

  js_value_t *ctx;
  err = js_get_reference_value(env, req->base.ctx, &ctx);
  assert(err == 0);

  js_value_t *on_after_work;
  err = js_get_reference_value(env, req->base.on_after_work, &on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.ctx);
  assert(err == 0);

  err = js_call_function(env, ctx, on_after_work, 2, argv, nullptr);
  assert(err == 0);
}

static js_value_t *
binding_get (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 5;
  js_value_t *argv[5];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 5);

  binding_key_value_req_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, nullptr);
  if (err < 0) return nullptr;

  binding_t *handle;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[2], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  err = js_create_reference(env, argv[3], 1, &req->base.ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &req->base.on_after_work);
  assert(err == 0);

  req->base.db = handle->db;

  uv_loop_t *loop;
  js_get_env_loop(env, &loop);

  uv_queue_work(loop, &req->base.req, on_get, on_after_get);

  return nullptr;
}

static void
on_put (uv_work_t *uv_req) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  Status status = req->base.db->Put(WriteOptions(), req->base.db->DefaultColumnFamily(), req->key, req->value);

  if (status.ok()) {
    req->base.status = 0;
  } else {
    req->base.status = status.code();
  }
}

static void
on_after_put (uv_work_t *uv_req, int status) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  js_value_t *argv[2];

  if (req->base.status == 0) {
    err = js_get_null(env, &argv[0]);
    assert(err == 0);
  } else {
    argv[0] = binding_create_error(env, req->base.status);
  }

  req->value.Reset();

  js_value_t *ctx;
  err = js_get_reference_value(env, req->base.ctx, &ctx);
  assert(err == 0);

  js_value_t *on_after_work;
  err = js_get_reference_value(env, req->base.on_after_work, &on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.ctx);
  assert(err == 0);

  err = js_call_function(env, ctx, on_after_work, 1, argv, nullptr);
  assert(err == 0);
}

static js_value_t *
binding_put (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 6;
  js_value_t *argv[6];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 6);

  binding_key_value_req_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, nullptr);
  if (err < 0) return nullptr;

  binding_t *handle;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[2], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  size_t value_len;
  char *value;
  err = js_get_typedarray_info(env, argv[3], nullptr, (void **) &value, &value_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  err = js_create_reference(env, argv[4], 1, &req->base.ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[5], 1, &req->base.on_after_work);
  assert(err == 0);

  req->base.db = handle->db;

  req->value.PinSelf(Slice(value, value_len));

  uv_loop_t *loop;
  js_get_env_loop(env, &loop);

  uv_queue_work(loop, &req->base.req, on_put, on_after_put);

  return nullptr;
}

static void
on_delete (uv_work_t *uv_req) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  Status status = req->base.db->Delete(WriteOptions(), req->base.db->DefaultColumnFamily(), req->key);

  if (status.ok()) {
    req->base.status = 0;
  } else {
    req->base.status = status.code();
  }
}

static void
on_after_delete (uv_work_t *uv_req, int status) {
  binding_key_value_req_t *req = (binding_key_value_req_t *) uv_req->data;

  int err;

  js_env_t *env = req->base.env;

  js_value_t *argv[1];

  if (req->base.status == 0) {
    err = js_get_null(env, &argv[0]);
    assert(err == 0);
  } else {
    argv[0] = binding_create_error(env, req->base.status);
  }

  js_value_t *ctx;
  err = js_get_reference_value(env, req->base.ctx, &ctx);
  assert(err == 0);

  js_value_t *on_after_work;
  err = js_get_reference_value(env, req->base.on_after_work, &on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.on_after_work);
  assert(err == 0);

  err = js_delete_reference(env, req->base.ctx);
  assert(err == 0);

  err = js_call_function(env, ctx, on_after_work, 1, argv, nullptr);
  assert(err == 0);
}

static js_value_t *
binding_delete (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 5;
  js_value_t *argv[5];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 5);

  binding_key_value_req_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, nullptr);
  if (err < 0) return nullptr;

  binding_t *handle;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[2], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  err = js_create_reference(env, argv[3], 1, &req->base.ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &req->base.on_after_work);
  assert(err == 0);

  req->base.db = handle->db;

  uv_loop_t *loop;
  js_get_env_loop(env, &loop);

  uv_queue_work(loop, &req->base.req, on_delete, on_after_delete);

  return nullptr;
}

static js_value_t *
init (js_env_t *env, js_value_t *exports) {
  {
    js_value_t *fn;
    js_create_function(env, "createOpenReq", -1, binding_create_open_req, nullptr, &fn);
    js_set_named_property(env, exports, "createOpenReq", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "createCloseReq", -1, binding_create_close_req, nullptr, &fn);
    js_set_named_property(env, exports, "createCloseReq", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "createKeyValueReq", -1, binding_create_key_value_req, nullptr, &fn);
    js_set_named_property(env, exports, "createKeyValueReq", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "open", -1, binding_open, nullptr, &fn);
    js_set_named_property(env, exports, "open", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "close", -1, binding_close, nullptr, &fn);
    js_set_named_property(env, exports, "close", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "get", -1, binding_get, nullptr, &fn);
    js_set_named_property(env, exports, "get", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "put", -1, binding_put, nullptr, &fn);
    js_set_named_property(env, exports, "put", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "delete", -1, binding_delete, nullptr, &fn);
    js_set_named_property(env, exports, "delete", fn);
  }

  return exports;
}

PEAR_MODULE(init)
