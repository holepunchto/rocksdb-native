#include <stdio.h>
#include <stdlib.h>
#include <rocksdb/c.h>
#include <js.h>
#include <utf.h>
#include <bare.h>
#include <assert.h>

typedef struct {
  char *key;
  size_t key_len;
  char *value;
  size_t value_len;
} rocksdb_native_queue_t;

typedef struct {
  uv_work_t work;
  int auto_flush;

  rocksdb_t *db;

  uv_loop_t *loop;
  js_env_t *env;

  js_ref_t *ctx;
  js_ref_t *on_status;
  js_ref_t *on_batch;

  utf8_t *path;
  char *error;

  int writes_length;
  int writes_max;
  rocksdb_native_queue_t *writes;

  int reads_length;
  int reads_max;
  rocksdb_native_queue_t *reads;
} rocksdb_native_t;

static js_value_t *
rocksdb_native_init (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 4;
  js_value_t *argv[4];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  memset(self, 0, sizeof(uv_work_t));
  self->db = NULL;

  js_get_env_loop(env, &(self->loop));
  self->env = env;

  js_create_reference(env, argv[1], 1, &(self->ctx));
  js_create_reference(env, argv[2], 1, &(self->on_status));
  js_create_reference(env, argv[3], 1, &(self->on_batch));

  self->path = NULL;
  self->error = NULL;

  self->auto_flush = 1;

  self->writes_max = 8;
  self->writes_length = 0;
  self->writes = malloc(sizeof(rocksdb_native_queue_t) * self->writes_max);

  self->reads_max = 8;
  self->reads_length = 0;
  self->reads = malloc(sizeof(rocksdb_native_queue_t) * self->reads_max);

  return NULL;
}

static js_value_t *
rocksdb_native_clear_error (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 1;
  js_value_t *argv[1];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  if (self->error == NULL) return NULL;

  js_value_t *result;
  js_create_string_utf8(env, (const utf8_t *) self->error, strlen(self->error), &result);
  free(self->error);
  self->error = NULL;

  return result;
}

static void
on_worker_open (uv_work_t *req) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;

  rocksdb_options_t *opts = rocksdb_options_create();

  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_error_if_exists(opts, 0);

  rocksdb_t *db = rocksdb_open(opts, (char *) self->path, &(self->error));
  free(self->path);
  self->path = NULL;

  if (self->error == NULL) {
    self->db = db;
  }
}

static void
on_worker_status_cb (uv_work_t *req, int st) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;

  js_env_t *env = self->env;
  js_value_t *ctx;
  js_value_t *on_status;

  js_get_reference_value(env, self->ctx, &ctx);
  js_get_reference_value(env, self->on_status, &on_status);

  js_value_t *status;
  js_create_int32(env, self->error ? -1 : 0, &status);

  js_call_function(env, ctx, on_status, 1, &status, NULL);
}

static js_value_t *
rocksdb_native_open (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 2;
  js_value_t *argv[2];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  self->path = malloc(4096 * sizeof(utf8_t) + 1);

  js_get_value_string_utf8(env, argv[1], self->path, 4097, NULL);

  uv_queue_work(self->loop, (uv_work_t *) self, on_worker_open, on_worker_status_cb);

  return NULL;
}

static js_value_t *
rocksdb_native_resize_buffers (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 3;
  js_value_t *argv[3];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  int writes;
  js_get_value_int32(env, argv[1], &writes);

  int reads;
  js_get_value_int32(env, argv[1], &reads);

  if (writes != self->writes_max) {
    self->writes_max = writes;
    self->writes = realloc(self->writes, sizeof(rocksdb_native_queue_t) * self->writes_max);
  }

  if (reads != self->reads_max) {
    self->reads_max = reads;
    self->reads = realloc(self->reads, sizeof(rocksdb_native_queue_t) * self->reads_max);
  }

  return NULL;
}

static void
on_worker_batch (uv_work_t *req) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;
  rocksdb_t *db = self->db;

  rocksdb_native_queue_t *w = self->writes;
  int len = self->writes_length;

  while (len-- && self->error == NULL) {
    rocksdb_writeoptions_t *wo = rocksdb_writeoptions_create();
    rocksdb_put(db, wo, w->key, w->key_len, w->value, w->value_len, &(self->error));
    w++;
  }

  rocksdb_native_queue_t *r = self->reads;
  len = self->reads_length;

  while (len-- && self->error == NULL) {
    rocksdb_readoptions_t *ro = rocksdb_readoptions_create();
    r->value = rocksdb_get(db, ro, r->key, r->key_len, &(r->value_len), &(self->error));
    r++;
  }
}

static void
free_db_read (js_env_t *env, void *data, void *hint) {
  free(data);
}

static void
on_worker_batch_cb (uv_work_t *req, int st) {
  rocksdb_native_t *self = (rocksdb_native_t *) req;

  js_env_t *env = self->env;
  js_value_t *ctx;
  js_value_t *on_batch;

  rocksdb_native_queue_t *r = self->reads;

  js_value_t *result;
  js_create_array_with_length(env, self->reads_length, &result);

  for (int i = 0; i < self->reads_length; i++) {
    js_value_t *val;
    if (r->value == NULL) js_get_null(env, &val);
    else js_create_external_arraybuffer(env, r->value, r->value_len, free_db_read, NULL, &val);
    js_set_element(env, result, i, val);
    r++;
  }

  self->reads_length = self->writes_length = 0;

  js_get_reference_value(env, self->ctx, &ctx);
  js_get_reference_value(env, self->on_batch, &on_batch);

  js_call_function(env, ctx, on_batch, 1, &result, NULL);
}

static js_value_t *
rocksdb_native_queue_put (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 3;
  js_value_t *argv[3];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  rocksdb_native_queue_t *w = self->writes + (self->writes_length++);

  char *key;
  size_t key_len;
  js_get_typedarray_info(env, argv[1], NULL, (void **) &(w->key), &(w->key_len), NULL, NULL);

  char *value;
  size_t value_len;
  js_get_typedarray_info(env, argv[2], NULL, (void **) &(w->value), &(w->value_len), NULL, NULL);

  if (self->auto_flush) {
    uv_queue_work(self->loop, (uv_work_t *) self, on_worker_batch, on_worker_batch_cb);
  }

  return NULL;
}

static js_value_t *
rocksdb_native_queue_get (js_env_t *env, js_callback_info_t *info) {
  size_t argc = 3;
  js_value_t *argv[3];

  js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  rocksdb_native_t *self;
  size_t self_len;
  js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  rocksdb_native_queue_t *r = self->reads + (self->reads_length++);

  char *key;
  size_t key_len;
  js_get_typedarray_info(env, argv[1], NULL, (void **) &(r->key), &(r->key_len), NULL, NULL);

  if (self->auto_flush) {
    uv_queue_work(self->loop, (uv_work_t *) self, on_worker_batch, on_worker_batch_cb);
  }

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
    char *b = (char *) &(tmp.auto_flush);
    js_value_t *val;
    js_create_int32(env, (int) (b - a), &val);
    js_set_named_property(env, exports, "offsetof_auto_flush", val);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_init", -1, rocksdb_native_init, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_init", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_clear_error", -1, rocksdb_native_clear_error, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_clear_error", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_resize_buffers", -1, rocksdb_native_resize_buffers, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_resize_buffers", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_open", -1, rocksdb_native_open, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_open", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_queue_put", -1, rocksdb_native_queue_put, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_queue_put", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_queue_get", -1, rocksdb_native_queue_get, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_queue_get", fn);
  }

  return exports;
}

BARE_MODULE(rocksdb_native, init)
