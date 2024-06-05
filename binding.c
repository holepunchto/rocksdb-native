#include <assert.h>
#include <bare.h>
#include <js.h>
#include <rocksdb.h>
#include <stdlib.h>
#include <utf.h>

typedef struct {
  uint32_t low;
  uint32_t high;
} rocksdb_native_uint64_t;

typedef struct {
  uint32_t read_only;
  uint32_t create_if_missing;
  uint32_t max_background_jobs;
  rocksdb_native_uint64_t bytes_per_sync;
  uint32_t compation_style;
  uint32_t enable_blob_files;
  rocksdb_native_uint64_t min_blob_size;
  rocksdb_native_uint64_t blob_file_size;
  uint32_t enable_blob_garbage_collection;
  rocksdb_native_uint64_t table_block_size;
  uint32_t table_cache_index_and_filter_blocks;
  uint32_t table_format_version;
} rocksdb_native_open_options_t;

typedef struct {
  rocksdb_t handle;
} rocksdb_native_t;

typedef struct {
  rocksdb_open_t handle;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_open;
} rocksdb_native_open_t;

typedef struct {
  rocksdb_close_t handle;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_close;
} rocksdb_native_close_t;

typedef struct {
  rocksdb_delete_range_t handle;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_status;
} rocksdb_native_delete_range_t;

typedef struct {
  rocksdb_iterator_t handle;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_open;
  js_ref_t *on_close;
  js_ref_t *on_read;
} rocksdb_native_iterator_t;

typedef struct {
  rocksdb_batch_t handle;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;
  char **errors;

  size_t capacity;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_status;
} rocksdb_native_batch_t;

static void
rocksdb_native__on_free (js_env_t *env, void *data, void *finalize_hint) {
  free(data);
}

static js_value_t *
rocksdb_native_init (js_env_t *env, js_callback_info_t *info) {
  int err;

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  js_value_t *handle;

  rocksdb_native_t *db;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_t), (void **) &db, &handle);
  assert(err == 0);

  err = rocksdb_init(loop, &db->handle);
  assert(err == 0);

  return handle;
}

static inline uint64_t
rocksdb_native__to_uint6 (rocksdb_native_uint64_t n) {
  return n.high * 0x100000000 + n.low;
}

static void
rocksdb_native__on_open (rocksdb_open_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_open_t *req = (rocksdb_native_open_t *) handle->data;

  js_env_t *env = req->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, req->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, req->on_open, &cb);
  assert(err == 0);

  js_value_t *error;

  if (req->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) req->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);
  }

  js_call_function(env, ctx, cb, 1, (js_value_t *[]){error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);

  err = js_delete_reference(env, req->on_open);
  assert(err == 0);

  err = js_delete_reference(env, req->ctx);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_open (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 5;
  js_value_t *argv[5];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 5);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  utf8_t path[4096 + 1 /* NULL */];
  err = js_get_value_string_utf8(env, argv[1], path, sizeof(path), NULL);
  assert(err == 0);

  rocksdb_native_open_options_t *o;
  err = js_get_typedarray_info(env, argv[2], NULL, (void **) &o, NULL, NULL, NULL);
  assert(err == 0);

  rocksdb_options_t options = {
    0,
    o->read_only,
    o->create_if_missing,
    o->max_background_jobs,
    rocksdb_native__to_uint6(o->bytes_per_sync),
    o->compation_style,
    o->enable_blob_files,
    rocksdb_native__to_uint6(o->min_blob_size),
    rocksdb_native__to_uint6(o->blob_file_size),
    o->enable_blob_garbage_collection,
    rocksdb_native__to_uint6(o->table_block_size),
    o->table_cache_index_and_filter_blocks,
    o->table_format_version
  };

  js_value_t *handle;

  rocksdb_native_open_t *req;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_open_t), (void **) &req, &handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, argv[3], 1, &req->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &req->on_open);
  assert(err == 0);

  err = rocksdb_open(&db->handle, &req->handle, (const char *) path, &options, rocksdb_native__on_open);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native__on_close (rocksdb_close_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_close_t *req = (rocksdb_native_close_t *) handle->data;

  js_env_t *env = req->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, req->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, req->on_close, &cb);
  assert(err == 0);

  js_call_function(env, ctx, cb, 0, NULL, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);

  err = js_delete_reference(env, req->on_close);
  assert(err == 0);

  err = js_delete_reference(env, req->ctx);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_close (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 3);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  js_value_t *handle;

  rocksdb_native_close_t *req;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_close_t), (void **) &req, &handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, argv[1], 1, &req->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[2], 1, &req->on_close);
  assert(err == 0);

  err = rocksdb_close(&db->handle, &req->handle, rocksdb_native__on_close);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native__on_delete_range (rocksdb_delete_range_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_delete_range_t *req = (rocksdb_native_delete_range_t *) handle->data;

  js_env_t *env = req->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, req->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, req->on_status, &cb);
  assert(err == 0);

  js_value_t *error;

  if (req->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) req->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);
  }

  js_call_function(env, ctx, cb, 1, (js_value_t *[]){error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);

  err = js_delete_reference(env, req->on_status);
  assert(err == 0);

  err = js_delete_reference(env, req->ctx);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_delete_range (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 7;
  js_value_t *argv[7];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 7);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  js_value_t *handle;

  rocksdb_native_delete_range_t *req;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_delete_range_t), (void **) &req, &handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  rocksdb_range_t range;

  err = js_get_typedarray_info(env, argv[1], NULL, (void **) &range.gt.data, &range.gt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[2], NULL, (void **) &range.gte.data, &range.gte.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[3], NULL, (void **) &range.lt.data, &range.lt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[4], NULL, (void **) &range.lte.data, &range.lte.len, NULL, NULL);
  assert(err == 0);

  err = js_create_reference(env, argv[5], 1, &req->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[6], 1, &req->on_status);
  assert(err == 0);

  err = rocksdb_delete_range(&db->handle, &req->handle, range, rocksdb_native__on_delete_range);
  assert(err == 0);

  return handle;
}

static js_value_t *
rocksdb_native_iterator_init (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 5;
  js_value_t *argv[5];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 5);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  js_value_t *handle;

  rocksdb_native_iterator_t *iterator;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_iterator_t), (void **) &iterator, &handle);
  assert(err == 0);

  err = rocksdb_iterator_init(&db->handle, &iterator->handle);
  assert(err == 0);

  iterator->env = env;
  iterator->handle.data = (void *) iterator;

  err = js_create_reference(env, argv[1], 1, &iterator->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[2], 1, &iterator->on_open);
  assert(err == 0);

  err = js_create_reference(env, argv[3], 1, &iterator->on_close);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &iterator->on_read);
  assert(err == 0);

  return handle;
}

static js_value_t *
rocksdb_native_iterator_buffer (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_iterator_t *iterator;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &iterator, NULL);
  assert(err == 0);

  uint32_t capacity;
  err = js_get_value_uint32(env, argv[1], &capacity);
  assert(err == 0);

  js_value_t *handle;

  uint8_t *data;
  err = js_create_arraybuffer(env, 2 * capacity * sizeof(rocksdb_slice_t), (void **) &data, &handle);
  assert(err == 0);

  size_t offset = 0;

  iterator->keys = (rocksdb_slice_t *) &data[offset];

  offset += capacity * sizeof(rocksdb_slice_t);

  iterator->values = (rocksdb_slice_t *) &data[offset];

  return handle;
}

static void
rocksdb_native__on_iterator_open (rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *iterator = (rocksdb_native_iterator_t *) handle->data;

  js_env_t *env = iterator->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, iterator->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, iterator->on_open, &cb);
  assert(err == 0);

  js_value_t *error;

  if (iterator->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) iterator->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);
  }

  js_call_function(env, ctx, cb, 1, (js_value_t *[]){error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_iterator_open (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 6;
  js_value_t *argv[6];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 6);

  rocksdb_native_iterator_t *iterator;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &iterator, NULL);
  assert(err == 0);

  rocksdb_range_t range;

  err = js_get_typedarray_info(env, argv[1], NULL, (void **) &range.gt.data, &range.gt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[2], NULL, (void **) &range.gte.data, &range.gte.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[3], NULL, (void **) &range.lt.data, &range.lt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[4], NULL, (void **) &range.lte.data, &range.lte.len, NULL, NULL);
  assert(err == 0);

  bool reverse;
  err = js_get_value_bool(env, argv[5], &reverse);
  assert(err == 0);

  err = rocksdb_iterator_open(&iterator->handle, range, reverse, rocksdb_native__on_iterator_open);
  assert(err == 0);

  return NULL;
}

static void
rocksdb_native__on_iterator_close (rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *iterator = (rocksdb_native_iterator_t *) handle->data;

  js_env_t *env = iterator->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, iterator->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, iterator->on_close, &cb);
  assert(err == 0);

  js_value_t *error;

  if (iterator->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) iterator->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);
  }

  js_call_function(env, ctx, cb, 1, (js_value_t *[]){error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_iterator_close (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 1);

  rocksdb_native_iterator_t *iterator;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &iterator, NULL);
  assert(err == 0);

  err = rocksdb_iterator_close(&iterator->handle, rocksdb_native__on_iterator_close);
  assert(err == 0);

  return NULL;
}

static void
rocksdb_native__on_iterator_read (rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *iterator = (rocksdb_native_iterator_t *) handle->data;

  js_env_t *env = iterator->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, iterator->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, iterator->on_read, &cb);
  assert(err == 0);

  size_t len = iterator->handle.len;

  js_value_t *keys;
  err = js_create_array_with_length(env, len, &keys);
  assert(err == 0);

  js_value_t *values;
  err = js_create_array_with_length(env, len, &values);
  assert(err == 0);

  js_value_t *error;

  if (iterator->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) iterator->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);

    for (size_t i = 0; i < len; i++) {
      js_value_t *result;

      rocksdb_slice_t *key = &iterator->keys[i];

      err = js_create_external_arraybuffer(env, (void *) key->data, key->len, rocksdb_native__on_free, NULL, &result);
      assert(err == 0);

      err = js_set_element(env, keys, i, result);
      assert(err == 0);

      rocksdb_slice_t *value = &iterator->values[i];

      err = js_create_external_arraybuffer(env, (void *) value->data, value->len, rocksdb_native__on_free, NULL, &result);
      assert(err == 0);

      err = js_set_element(env, values, i, result);
      assert(err == 0);
    }
  }

  js_call_function(env, ctx, cb, 3, (js_value_t *[]){error, keys, values}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_iterator_read (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_iterator_t *iterator;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &iterator, NULL);
  assert(err == 0);

  uint32_t capacity;
  err = js_get_value_uint32(env, argv[1], &capacity);
  assert(err == 0);

  err = rocksdb_iterator_read(&iterator->handle, iterator->keys, iterator->values, capacity, rocksdb_native__on_iterator_read);
  assert(err == 0);

  return NULL;
}

static js_value_t *
rocksdb_native_batch_init (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  js_value_t *handle;

  rocksdb_native_batch_t *batch;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_batch_t), (void **) &batch, &handle);
  assert(err == 0);

  err = rocksdb_batch_init(&db->handle, &batch->handle);
  assert(err == 0);

  batch->env = env;
  batch->handle.data = (void *) batch;

  err = js_create_reference(env, argv[1], 1, &batch->ctx);
  assert(err == 0);

  return handle;
}

static js_value_t *
rocksdb_native_batch_buffer (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_batch_t *batch;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &batch, NULL);
  assert(err == 0);

  uint32_t capacity;
  err = js_get_value_uint32(env, argv[1], &capacity);
  assert(err == 0);

  js_value_t *handle;

  uint8_t *data;
  err = js_create_arraybuffer(env, 2 * capacity * sizeof(rocksdb_slice_t) + capacity * sizeof(char *), (void **) &data, &handle);
  assert(err == 0);

  batch->capacity = capacity;

  size_t offset = 0;

  batch->keys = (rocksdb_slice_t *) &data[offset];

  offset += capacity * sizeof(rocksdb_slice_t);

  batch->values = (rocksdb_slice_t *) &data[offset];

  offset += capacity * sizeof(rocksdb_slice_t);

  batch->errors = (char **) &data[offset];

  return handle;
}

static inline int
rocksdb_native__get_slices (js_env_t *env, js_value_t *arr, uint32_t len, rocksdb_slice_t *result) {
  int err;

  for (uint32_t i = 0; i < len; i++) {
    js_value_t *value;
    err = js_get_element(env, arr, i, &value);
    assert(err == 0);

    rocksdb_slice_t *slice = &result[i];

    err = js_get_typedarray_info(env, value, NULL, (void **) &slice->data, &slice->len, NULL, NULL);
    assert(err == 0);
  }

  return 0;
}

static void
rocksdb_native__on_batch_read (rocksdb_batch_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_batch_t *batch = (rocksdb_native_batch_t *) handle->data;

  js_env_t *env = batch->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  size_t len = batch->handle.len;

  js_value_t *errors;
  err = js_create_array_with_length(env, len, &errors);
  assert(err == 0);

  js_value_t *values;
  err = js_create_array_with_length(env, len, &values);
  assert(err == 0);

  for (size_t i = 0; i < len; i++) {
    js_value_t *result;

    char *error = batch->errors[i];

    if (error) {
      err = js_create_string_utf8(env, (utf8_t *) error, -1, &result);
      assert(err == 0);

      err = js_set_element(env, errors, i, result);
      assert(err == 0);
    } else {
      rocksdb_slice_t *slice = &batch->values[i];

      err = js_create_external_arraybuffer(env, (void *) slice->data, slice->len, rocksdb_native__on_free, NULL, &result);
      assert(err == 0);

      err = js_set_element(env, values, i, result);
      assert(err == 0);
    }
  }

  js_value_t *ctx;
  err = js_get_reference_value(env, batch->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, batch->on_status, &cb);
  assert(err == 0);

  js_call_function(env, ctx, cb, 2, (js_value_t *[]){errors, values}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_batch_read (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 3);

  rocksdb_native_batch_t *batch;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &batch, NULL);
  assert(err == 0);

  uint32_t len;
  err = js_get_array_length(env, argv[1], &len);
  assert(err == 0);

  err = js_create_reference(env, argv[2], 1, &batch->on_status);
  assert(err == 0);

  err = rocksdb_native__get_slices(env, argv[1], len, batch->keys);
  assert(err == 0);

  err = rocksdb_batch_read(&batch->handle, batch->keys, batch->values, batch->errors, len, rocksdb_native__on_batch_read);
  assert(err == 0);

  return NULL;
}

static void
rocksdb_native__on_batch_write (rocksdb_batch_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_batch_t *batch = (rocksdb_native_batch_t *) handle->data;

  js_env_t *env = batch->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *error;

  if (batch->handle.error) {
    err = js_create_string_utf8(env, (utf8_t *) batch->handle.error, -1, &error);
    assert(err == 0);
  } else {
    err = js_get_null(env, &error);
    assert(err == 0);
  }

  js_value_t *ctx;
  err = js_get_reference_value(env, batch->ctx, &ctx);
  assert(err == 0);

  js_value_t *cb;
  err = js_get_reference_value(env, batch->on_status, &cb);
  assert(err == 0);

  js_call_function(env, ctx, cb, 1, (js_value_t *[]){error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_batch_write (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 4;
  js_value_t *argv[4];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 4);

  rocksdb_native_batch_t *batch;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &batch, NULL);
  assert(err == 0);

  uint32_t len;
  err = js_get_array_length(env, argv[1], &len);
  assert(err == 0);

  err = js_create_reference(env, argv[3], 1, &batch->on_status);
  assert(err == 0);

  err = rocksdb_native__get_slices(env, argv[1], len, batch->keys);
  assert(err == 0);

  err = rocksdb_native__get_slices(env, argv[2], len, batch->values);
  assert(err == 0);

  err = rocksdb_batch_write(&batch->handle, batch->keys, batch->values, len, rocksdb_native__on_batch_write);
  assert(err == 0);

  return NULL;
}

static js_value_t *
rocksdb_native_exports (js_env_t *env, js_value_t *exports) {
  int err;

#define V(name, fn) \
  { \
    js_value_t *val; \
    err = js_create_function(env, name, -1, fn, NULL, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("init", rocksdb_native_init)
  V("open", rocksdb_native_open)
  V("close", rocksdb_native_close)
  V("deleteRange", rocksdb_native_delete_range)

  V("iteratorInit", rocksdb_native_iterator_init)
  V("iteratorBuffer", rocksdb_native_iterator_buffer)
  V("iteratorOpen", rocksdb_native_iterator_open)
  V("iteratorClose", rocksdb_native_iterator_close)
  V("iteratorRead", rocksdb_native_iterator_read)

  V("batchInit", rocksdb_native_batch_init)
  V("batchBuffer", rocksdb_native_batch_buffer)
  V("batchRead", rocksdb_native_batch_read)
  V("batchWrite", rocksdb_native_batch_write)
#undef V

  return exports;
}

BARE_MODULE(rocksdb_native, rocksdb_native_exports)
