#include <stdio.h>
#include <stdlib.h>
#include <rocksdb/c.h>
#include <js.h>
#include <bare.h>
#include <assert.h>

typedef struct {
  rocksdb_t *db;
} rocksdb_native_t;

static js_value_t *
rocksdb_native_get (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);
  assert(argc == 3);

  rocksdb_native_t *self;
  size_t self_len;
  err = js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);
  assert(err == 0);

  char *key;
  size_t key_len;
  err = js_get_typedarray_info(env, argv[1], NULL, (void **) &key, &key_len, NULL, NULL);
  assert(err == 0);

  char *value;
  size_t value_len;
  err = js_get_typedarray_info(env, argv[2], NULL, (void **) &value, &value_len, NULL, NULL);
  assert(err == 0);

  char *db_err = NULL;

  rocksdb_t *db = self->db;

  rocksdb_readoptions_t *ro = rocksdb_readoptions_create();

  size_t db_value_len;
  char *db_value = rocksdb_get(db, ro, key, key_len, &db_value_len, &db_err);

  if (db_err != NULL) {
    fprintf(stderr, "get key %s\n", db_err);
    rocksdb_close(db);
    return NULL;
  }

  if (db_value_len > value_len) db_value_len = value_len;

  memcpy(value, db_value, db_value_len);

  free(db_err);
  db_err = NULL;

  js_value_t *val;
  js_create_int32(env, db_value_len, &val);
  return val;
}

static js_value_t *
rocksdb_native_put (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);
  assert(argc == 3);

  rocksdb_native_t *self;
  size_t self_len;
  err = js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);
  assert(err == 0);

  char *key;
  size_t key_len;
  err = js_get_typedarray_info(env, argv[1], NULL, (void **) &key, &key_len, NULL, NULL);
  assert(err == 0);

  char *value;
  size_t value_len;
  err = js_get_typedarray_info(env, argv[2], NULL, (void **) &value, &value_len, NULL, NULL);
  assert(err == 0);

  char *db_err = NULL;

  rocksdb_t *db = self->db;
  rocksdb_writeoptions_t *wo = rocksdb_writeoptions_create();

  rocksdb_put(db, wo, key, key_len, value, value_len, &db_err);
  if (db_err != NULL) {
    fprintf(stderr, "put key %s\n", db_err);
    rocksdb_close(db);
    return NULL;
  }
  free(db_err);
  db_err = NULL;

  return NULL;
}

static js_value_t *
rocksdb_native_init (js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);

  assert(err == 0);
  assert(argc == 2);

  rocksdb_native_t *self;
  size_t self_len;
  err = js_get_typedarray_info(env, argv[0], NULL, (void **) &self, &self_len, NULL, NULL);

  utf8_t db_name[4096];
  err = js_get_value_string_utf8(env, argv[1], db_name, 4096, NULL);

  rocksdb_options_t *opts = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_error_if_exists(opts, 0);

  char *db_err = NULL;

  rocksdb_t *db = rocksdb_open(opts, (char *) db_name, &db_err);
  if (db_err != NULL) {
    fprintf(stderr, "database open %s\n", db_err);
    return NULL;
  }

  free(db_err);
  db_err = NULL;

  self->db = db;

  return NULL;
}

static js_value_t *
init (js_env_t *env, js_value_t *exports) {
  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_init", -1, rocksdb_native_init, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_init", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_put", -1, rocksdb_native_put, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_put", fn);
  }

  {
    js_value_t *fn;
    js_create_function(env, "rocksdb_native_get", -1, rocksdb_native_get, NULL, &fn);
    js_set_named_property(env, exports, "rocksdb_native_get", fn);
  }

  {
    js_value_t *val;
    js_create_int32(env, sizeof(rocksdb_native_t), &val);
    js_set_named_property(env, exports, "sizeof_rocksdb_native_t", val);
  }

  return exports;
}

BARE_MODULE(rocksdb_native, init)
