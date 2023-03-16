#include <assert.h>
#include <js.h>
#include <pear.h>
#include <rocksdb/db.h>
#include <stdlib.h>
#include <string.h>

using namespace rocksdb;

typedef struct {
  DB *db;
} rocksdb_native_t;

static inline void
rocksdb_native_throw (js_env_t *env, Status status) {
  js_throw_errorf(env, nullptr, status.ToString().c_str());
}

static js_value_t *
rocksdb_native_open (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 1);

  size_t name_len;
  err = js_get_value_string_utf8(env, argv[0], nullptr, 0, &name_len);
  if (err < 0) return nullptr;

  char *name = new char[name_len + 1];
  err = js_get_value_string_utf8(env, argv[0], name, name_len + 1, nullptr);
  if (err < 0) {
    delete[] name;
    return nullptr;
  }

  rocksdb_native_t *handle;

  js_value_t *result;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_t), (void **) &handle, &result);
  assert(err == 0);

  Options options;
  options.create_if_missing = true;

  Status status = DB::Open(options, name, &handle->db);

  delete[] name;

  if (!status.ok()) {
    rocksdb_native_throw(env, status);
    return nullptr;
  }

  return result;
}

static js_value_t *
rocksdb_native_close (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 1);

  rocksdb_native_t *handle;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  Status status = handle->db->Close();

  delete handle->db;

  if (!status.ok()) {
    rocksdb_native_throw(env, status);
    return nullptr;
  }

  return nullptr;
}

static js_value_t *
rocksdb_native_get (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_t *handle;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[1], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  PinnableSlice slice;

  Status status = handle->db->Get(ReadOptions(), handle->db->DefaultColumnFamily(), Slice(key, key_len), &slice);

  if (status.IsNotFound()) {
    js_value_t *result;
    js_get_null(env, &result);
    return result;
  }

  if (!status.ok()) {
    rocksdb_native_throw(env, status);
    return nullptr;
  }

  std::string *value = slice.GetSelf();

  void *data;

  js_value_t *result;
  err = js_create_arraybuffer(env, value->length(), &data, &result);
  if (err < 0) return nullptr;

  memcpy(data, value->data(), value->length());

  return result;
}

static js_value_t *
rocksdb_native_put (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 3);

  rocksdb_native_t *handle;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[1], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  size_t value_len;
  char *value;
  err = js_get_typedarray_info(env, argv[2], nullptr, (void **) &value, &value_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  Status status = handle->db->Put(WriteOptions(), handle->db->DefaultColumnFamily(), Slice(key, key_len), Slice(value, value_len));

  if (!status.ok()) {
    rocksdb_native_throw(env, status);
    return nullptr;
  }

  return nullptr;
}

static js_value_t *
rocksdb_native_delete (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, nullptr, nullptr);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_t *handle;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &handle, nullptr);
  if (err < 0) return nullptr;

  size_t key_len;
  char *key;
  err = js_get_typedarray_info(env, argv[1], nullptr, (void **) &key, &key_len, nullptr, nullptr);
  if (err < 0) return nullptr;

  Status status = handle->db->Delete(WriteOptions(), handle->db->DefaultColumnFamily(), Slice(key, key_len));

  if (!status.ok()) {
    rocksdb_native_throw(env, status);
    return nullptr;
  }

  return nullptr;
}

static js_value_t *
init (js_env_t *env, js_value_t *exports) {
  {
    js_value_t *fn;
    js_create_function(env, "open", -1, rocksdb_native_open, nullptr, &fn);
    js_set_named_property(env, exports, "open", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "close", -1, rocksdb_native_close, nullptr, &fn);
    js_set_named_property(env, exports, "close", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "get", -1, rocksdb_native_get, nullptr, &fn);
    js_set_named_property(env, exports, "get", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "put", -1, rocksdb_native_put, nullptr, &fn);
    js_set_named_property(env, exports, "put", fn);
  }
  {
    js_value_t *fn;
    js_create_function(env, "delete", -1, rocksdb_native_delete, nullptr, &fn);
    js_set_named_property(env, exports, "delete", fn);
  }

  return exports;
}

PEAR_MODULE(init)
