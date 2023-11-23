#include <stdio.h>
#include <stdlib.h>
#include <rocksdb/c.h>
#include <js.h>
#include <js/ffi.h>
#include <utf.h>
#include <bare.h>
#include <assert.h>

typedef struct {
  int size;
  int max_size;

  char **keys;
  char **values;
  char **errors;

  size_t *key_lengths;
  size_t *value_lengths;
} rocksdb_native_batch_t;

typedef struct {
  uint32_t read_only;
  uint32_t create_if_missing;
  uint32_t max_background_jobs;
  uint32_t bytes_per_sync;
  uint32_t table_block_size;
  uint32_t table_cache_index_and_filter_blocks;
  uint32_t table_pin_l0_filter_and_index_blocks_in_cache;
  uint32_t table_format_version;
  uint32_t enable_blob_files;
  uint32_t min_blob_size;
  uint32_t blob_file_size_low;
  uint32_t blob_file_size_high;
  uint32_t enable_blob_garbage_collection;
} rocksdb_native_open_options_t;

typedef struct {
  utf8_t path[4096 + 1];
  rocksdb_native_open_options_t options;
} rocksdb_native_open_t;

typedef struct {
  uv_work_t work;
  int status;

  rocksdb_t *db;
  uv_loop_t *loop;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_status;
  js_ref_t *on_batch;

  rocksdb_native_batch_t write_batch;
  rocksdb_native_batch_t read_batch;
} rocksdb_native_t;

#define ROCKSDB_NATIVE_BATCH_ERROR    0b01
#define ROCKSDB_NATIVE_BATCH_NO_ERROR 0b10
#define ROCKSDB_NATIVE_AUTO_FLUSH     0b10

static void
resize_batch (rocksdb_native_batch_t *b, int max_size, int free_old) {
  if (free_old) free(b->keys);

  b->size = 0;
  b->max_size = max_size;

  char **tmp = malloc((3 * max_size * sizeof(char *)) + (2 * max_size * sizeof(size_t)));

  b->keys = tmp;
  b->values = b->keys + max_size;
  b->errors = b->values + max_size;

  tmp = b->errors + max_size;

  b->key_lengths = (size_t *) tmp;
  b->value_lengths = b->key_lengths + max_size;
}

static js_value_t *
rocksdb_native_init (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 5;
  js_value_t *argv[5];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  memset(self, 0, sizeof(uv_work_t));
  self->status = 0;
  self->db = NULL;

  js_get_env_loop(env, &(self->loop));
  self->env = env;

  int max_batch_size;
  js_get_value_int32(env, argv[1], &max_batch_size);

  js_create_reference(env, argv[2], 1, &(self->ctx));
  js_create_reference(env, argv[3], 1, &(self->on_status));
  js_create_reference(env, argv[4], 1, &(self->on_batch));

  resize_batch(&(self->write_batch), max_batch_size, 0);
  resize_batch(&(self->read_batch), max_batch_size, 0);

  return NULL;
}

static js_value_t *
rocksdb_native_set_read_buffer_size (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 2;
  js_value_t *argv[2];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  int max_size;
  js_get_value_int32(env, argv[1], &max_size);

  resize_batch(&(self->read_batch), max_size, 1);

  return NULL;
}

static js_value_t *
rocksdb_native_set_write_buffer_size (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 2;
  js_value_t *argv[2];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  int max_size;
  js_get_value_int32(env, argv[1], &max_size);

  resize_batch(&(self->write_batch), max_size, 1);

  return NULL;
}

static void
on_worker_status_cb (uv_work_t *req, int st) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;

  js_env_t *env = self->env;
  js_value_t *ctx;
  js_value_t *on_status;

  js_get_reference_value(env, self->ctx, &ctx);
  js_get_reference_value(env, self->on_status, &on_status);

  js_value_t *value;

  if (req->data != NULL) {
    js_create_string_utf8(env, (utf8_t *) req->data, -1, &value);
    free(req->data);
  } else {
    js_get_null(env, &value);
  }

  js_handle_scope_t *scope;
  js_open_handle_scope(env, &scope);
  js_call_function(env, ctx, on_status, 1, &value, NULL);
  js_close_handle_scope(env, scope);
}

static uint64_t
uint32s_to_64 (uint32_t low, uint32_t high) {
  uint64_t s = low;
  s *= 0x100000000;
  s += high;
  return s;
}

static void
on_worker_open (uv_work_t *req) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;
  uv_handle_t *handle = (uv_handle_t *) self;

  rocksdb_native_open_t *o = handle->data;

  rocksdb_options_t *opts = rocksdb_options_create();
  rocksdb_block_based_table_options_t *topts = rocksdb_block_based_options_create();

  rocksdb_options_set_create_if_missing(opts, o->options.create_if_missing);
  rocksdb_options_set_error_if_exists(opts, 0);

  rocksdb_options_set_max_background_jobs(opts, o->options.max_background_jobs);
  rocksdb_options_set_bytes_per_sync(opts, o->options.bytes_per_sync);

  rocksdb_block_based_options_set_block_size(topts, o->options.table_block_size);
  rocksdb_block_based_options_set_cache_index_and_filter_blocks(topts, o->options.table_cache_index_and_filter_blocks);
  rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(topts, o->options.table_pin_l0_filter_and_index_blocks_in_cache);
  rocksdb_block_based_options_set_format_version(topts, o->options.table_format_version);

  rocksdb_options_set_block_based_table_factory(opts, topts);

  // rocksdb_filterpolicy_t *filter = rocksdb_filterpolicy_create_bloom(10);
  // rocksdb_block_based_options_set_filter_policy(topts, filter);

  // rocksdb_options_set_hash_link_list_rep(opts, 200000);
  // rocksdb_options_set_allow_concurrent_memtable_write(opts, 0);

  if (o->options.enable_blob_files) {
    rocksdb_options_set_enable_blob_files(opts, 1);
    if (o->options.min_blob_size) rocksdb_options_set_min_blob_size(opts, o->options.min_blob_size);
    uint64_t blob_file_size = uint32s_to_64(o->options.blob_file_size_low, o->options.blob_file_size_high);
    if (blob_file_size) rocksdb_options_set_blob_file_size(opts, blob_file_size);
    rocksdb_options_set_enable_blob_gc(opts, o->options.enable_blob_garbage_collection);
  }

  char *error = NULL;

  rocksdb_t *db = o->options.read_only
    ? rocksdb_open_for_read_only(opts, (char *) o->path, 0, &error)
    : rocksdb_open(opts, (char *) o->path, &error);

  free(o);

  if (error != NULL) {
    req->data = error;
    return;
  }

  req->data = NULL;
  self->db = db;
}

static js_value_t *
rocksdb_native_open (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 3;
  js_value_t *argv[3];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  uv_handle_t *handle = (uv_handle_t *) self;
  rocksdb_native_open_t *o = handle->data = malloc(sizeof(rocksdb_native_open_t));

  js_get_value_string_utf8(env, argv[1], o->path, 4097, NULL);

  rocksdb_native_open_options_t *opts;
  size_t opts_len;
  js_get_typedarray_info(env, argv[2], NULL, (void **) &opts, &opts_len, NULL, NULL);

  o->options = *opts;

  uv_queue_work(self->loop, (uv_work_t *) self, on_worker_open, on_worker_status_cb);

  return NULL;
}

static void
on_worker_close (uv_work_t *req) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;
  uv_handle_t *handle = (uv_handle_t *) self;

  rocksdb_cancel_all_background_work(self->db, 1);
  rocksdb_close(self->db);

  handle->data = NULL;
}

static js_value_t *
rocksdb_native_close (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 1;
  js_value_t *argv[1];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  uv_queue_work(self->loop, (uv_work_t *) self, on_worker_close, on_worker_status_cb);
  return NULL;
}

static void
on_worker_batch (uv_work_t *req) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;

  rocksdb_native_batch_t *w = &(self->write_batch);

  if (w->size) {
    rocksdb_writebatch_t *b = rocksdb_writebatch_create();
    for (int i = 0; i < w->size; i++) {
      size_t length = w->value_lengths[i];
      if (length == 0) {
        rocksdb_writebatch_delete(b, w->keys[i], w->key_lengths[i]);
      } else {
        rocksdb_writebatch_put(b, w->keys[i], w->key_lengths[i], w->values[i], length);
      }
    }
    rocksdb_writeoptions_t *wo = rocksdb_writeoptions_create();
    rocksdb_write(self->db, wo, b, w->errors);
  }

  rocksdb_native_batch_t *r = &(self->read_batch);
  if (r->size) {
    rocksdb_readoptions_t *ro = rocksdb_readoptions_create();
    rocksdb_multi_get(self->db, ro, r->size, (const char * const*) r->keys, r->key_lengths, r->values, r->value_lengths, r->errors);
  }
}

static void
free_db_read (js_env_t *env, void *data, void *hint) {
  rocksdb_free(data);
}

static void
on_worker_batch_cb (uv_work_t *req, int st) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;
  rocksdb_native_batch_t *r = &(self->read_batch);

  js_env_t *env = self->env;

  js_handle_scope_t *scope;
  js_open_handle_scope(env, &scope);

  js_value_t *gets;
  js_create_array_with_length(env, r->size, &gets);

  js_value_t *value;
  for (int i = 0; i < r->size; i++) {
    if (r->values[i] == NULL) {
      js_get_null(env, &value);
      if (r->errors[i] != NULL) {
        free(r->errors[i]); // ignore for now... we signal null anyway
      }
    } else {
      // external array buffers with finalisation are broken atm, so just memcpy...
      char *data;
      js_create_arraybuffer(env, r->value_lengths[i], (void **) &data, &value);
      memcpy(data, r->values[i], r->value_lengths[i]);
      rocksdb_free(r->values[i]);
    }

    js_set_element(env, gets, i, value);
  }

  js_value_t *ctx;
  js_value_t *on_batch;

  js_get_reference_value(env, self->ctx, &ctx);
  js_get_reference_value(env, self->on_batch, &on_batch);

  js_value_t *values[1] = {
    gets
  };

  js_call_function(env, ctx, on_batch, 1, values, NULL);
  js_close_handle_scope(env, scope);
}

static js_value_t *
rocksdb_native_batch (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 4;
  js_value_t *argv[4];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  uint32_t reads;
  js_value_t *read_keys = argv[1];

  js_get_array_length(env, read_keys, &reads);

  rocksdb_native_batch_t *r = &(self->read_batch);

  r->size = reads;
  js_value_t *item;

  for (int i = 0; i < reads; i++) {
    js_get_element(env, read_keys, i, &item);

    char **key = r->keys + i;;
    size_t *key_length = r->key_lengths + i;

    js_get_typedarray_info(env, item, NULL, (void **) key, key_length, NULL, NULL);
  }

  uint32_t writes;
  js_value_t *write_keys = argv[2];
  js_value_t *write_values = argv[3];

  js_get_array_length(env, write_keys, &writes);
  js_get_array_length(env, write_values, &writes);

  rocksdb_native_batch_t *w = &(self->write_batch);

  w->size = writes;

  for (int i = 0; i < writes; i++) {
    js_get_element(env, write_keys, i, &item);

    char **key = w->keys + i;;
    size_t *key_length = w->key_lengths + i;

    js_get_typedarray_info(env, item, NULL, (void **) key, key_length, NULL, NULL);

    js_get_element(env, write_values, i, &item);

    char **value = w->values + i;;
    size_t *value_length = w->value_lengths + i;

    js_get_typedarray_info(env, item, NULL, (void **) value, value_length, NULL, NULL);
  }

  uv_queue_work(self->loop, (uv_work_t *) self, on_worker_batch, on_worker_batch_cb);

  return NULL;
}

static js_value_t *
init (js_env_t *env, js_value_t *exports) {
  {
    js_value_t *val;
    js_create_int32(env, sizeof(rocksdb_native_t), &val);
    js_set_named_property(env, exports, "sizeof_rocksdb_native_t", val);
  }

  {
    rocksdb_native_t tmp;
    char *a = (char *) &tmp;
    char *b = (char *) &(tmp.status);
    js_value_t *val;
    js_create_int32(env, (int) (b - a), &val);
    js_set_named_property(env, exports, "offsetof_rocksdb_native_t_status", val);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_init", -1, rocksdb_native_init, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_init", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_open", -1, rocksdb_native_open, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_open", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_close", -1, rocksdb_native_close, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_close", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_batch", -1, rocksdb_native_batch, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_batch", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_set_read_buffer_size", -1, rocksdb_native_set_read_buffer_size, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_set_read_buffer_size", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_set_write_buffer_size", -1, rocksdb_native_set_write_buffer_size, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_set_write_buffer_size", fn);
  }

  return exports;
}

BARE_MODULE(rocksdb_native, init)
