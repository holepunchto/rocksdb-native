#include <assert.h>
#include <bare.h>
#include <js.h>
#include <jstl.h>
#include <rocksdb.h>
#include <stdlib.h>
#include <string.h>
#include <utf.h>

namespace {
using cb_on_open_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_close_t = js_function_t<void, js_receiver_t>;
using cb_on_suspend_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_resume_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_flush_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_write_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
}; // namespace

struct rocksdb_native_column_family_t {
  rocksdb_column_family_t *handle;
  rocksdb_column_family_descriptor_t descriptor;

  rocksdb_t *db;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
};

struct rocksdb_native_t {
  rocksdb_t handle;
  rocksdb_options_t options;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;

  bool closing;
  bool exiting;

  js_deferred_teardown_t *teardown;
};

struct rocksdb_native_open_t {
  rocksdb_open_t handle;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_open_t> on_open;

  js_persistent_t<js_array_t> column_families;
};

struct rocksdb_native_close_t {
  rocksdb_close_t handle;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_close_t> on_close;
};

struct rocksdb_native_suspend_t {
  rocksdb_suspend_t handle;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_suspend_t> on_suspend;
};

struct rocksdb_native_resume_t {
  rocksdb_resume_t handle;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_resume_t> on_resume;
};

struct rocksdb_native_iterator_t {
  rocksdb_iterator_t handle;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_open;
  js_ref_t *on_close;
  js_ref_t *on_read;

  bool closing;
  bool exiting;

  js_deferred_teardown_t *teardown;
};

struct rocksdb_native_read_batch_t {
  rocksdb_read_batch_t handle;

  rocksdb_read_t *reads;

  size_t capacity;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_status;
};

struct rocksdb_native_write_batch_t {
  rocksdb_write_batch_t handle;

  rocksdb_write_t *writes;

  size_t capacity;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_write_t> on_write;
};

struct rocksdb_native_flush_t {
  rocksdb_flush_t handle;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_flush_t> on_flush;

  js_persistent_t<rocksdb_native_column_family_t> column_family;
};

struct rocksdb_native_snapshot_t {
  rocksdb_snapshot_t handle;
};

static void
rocksdb_native__on_free(js_env_t *env, void *data, void *finalize_hint) {
  free(data);
}

static void
rocksdb_native__on_column_family_teardown(void *data);

static void
rocksdb_native__on_open(rocksdb_open_t *handle, int status) {
  int err;

  assert(status == 0);

  auto req = reinterpret_cast<rocksdb_native_open_t *>(handle->data);

  auto db = reinterpret_cast<rocksdb_native_t *>(req->handle.req.db);

  js_env_t *env = req->env;

  const rocksdb_column_family_descriptor_t *descriptors = handle->column_families;

  rocksdb_column_family_t **handles = handle->handles;

  if (db->exiting) {
    req->on_open.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_open_t cb;
    err = js_get_reference_value(env, req->on_open, cb);
    assert(err == 0);

    js_array_t column_families;
    err = js_get_reference_value(env, req->column_families, column_families);
    assert(err == 0);

    req->on_open.reset();
    req->column_families.reset();
    req->ctx.reset();

    std::optional<js_string_t> error;

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    }

    rocksdb_column_family_t **handles = handle->handles;

    if (req->handle.error == NULL) {
      std::vector<js_arraybuffer_t> elements;

      err = js_get_array_elements(env, column_families, elements);
      assert(err == 0);

      const auto len = elements.size();

      for (uint32_t i = 0; i < len; i++) {
        js_arraybuffer_t handle = elements[i];

        rocksdb_native_column_family_t *column_family;
        err = js_get_arraybuffer_info(env, handle, column_family);
        assert(err == 0);

        column_family->handle = handles[i];

        err = js_create_reference(env, ctx, column_family->ctx);
        assert(err == 0);

        err = js_add_teardown_callback(env, rocksdb_native__on_column_family_teardown, (void *) column_family);
        assert(err == 0);
      }
    }

    js_call_function_with_checkpoint(env, cb, ctx, error);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);

    delete[] descriptors;
    delete[] handles;
  }
}

static void
rocksdb_native__on_close(rocksdb_close_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_close_t *req = (rocksdb_native_close_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  js_deferred_teardown_t *teardown = db->teardown;

  if (db->exiting) {
    db->ctx.reset();

    if (db->closing) {
      req->on_close.reset();
      req->ctx.reset();
    } else {
      free(req);
    }
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_close_t cb;
    err = js_get_reference_value(env, req->on_close, cb);
    assert(err == 0);

    db->ctx.reset();
    req->on_close.reset();
    req->ctx.reset();

    js_call_function_with_checkpoint(env, cb, ctx);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }

  err = js_finish_deferred_teardown_callback(teardown);
  assert(err == 0);
}

static void
rocksdb_native__on_teardown(js_deferred_teardown_t *handle, void *data) {
  int err;

  rocksdb_native_t *db = (rocksdb_native_t *) data;

  js_env_t *env = db->env;

  db->exiting = true;

  if (db->closing) return;

  auto req = reinterpret_cast<rocksdb_native_close_t *>(malloc(sizeof(rocksdb_native_close_t)));

  req->env = env;
  req->handle.data = (void *) req;

  err = rocksdb_close(&db->handle, &req->handle, rocksdb_native__on_close);
  assert(err == 0);
}

static js_arraybuffer_t
rocksdb_native_init(
  js_env_t *env,
  bool read_only,
  bool create_if_missing,
  bool create_missing_column_families,
  int32_t max_background_jobs,
  uint64_t bytes_per_sync,
  int32_t max_open_files,
  bool use_direct_reads
) {
  int err;

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  js_arraybuffer_t handle;

  rocksdb_native_t *db;
  err = js_create_arraybuffer(env, db, handle);
  assert(err == 0);

  db->env = env;
  db->closing = false;
  db->exiting = false;

  db->options = (rocksdb_options_t) {
    1,
    read_only,
    create_if_missing,
    create_missing_column_families,
    max_background_jobs,
    bytes_per_sync,
    max_open_files,
    use_direct_reads
  };

  err = rocksdb_init(loop, &db->handle);
  assert(err == 0);

  return handle;
}

static js_arraybuffer_t
rocksdb_native_open(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_receiver_t self,
  char *path,
  js_array_t column_families_array,
  js_receiver_t ctx,
  cb_on_open_t on_open
) {
  int err;

  std::vector<js_arraybuffer_t> elements;

  err = js_get_array_elements(env, column_families_array, elements);
  assert(err == 0);

  const auto len = elements.size();

  auto column_families = new rocksdb_column_family_descriptor_t[len];

  for (uint32_t i = 0; i < len; i++) {
    js_arraybuffer_t handle = elements[i];

    rocksdb_native_column_family_t *column_family;
    err = js_get_arraybuffer_info(env, handle, column_family);
    assert(err == 0);

    memcpy(&column_families[i], &column_family->descriptor, sizeof(rocksdb_column_family_descriptor_t));

    column_family->db = &db->handle;
  }

  auto handles = new rocksdb_column_family_t *[len];

  js_arraybuffer_t handle;

  rocksdb_native_open_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = req;

  err = js_create_reference(env, self, db->ctx);
  assert(err == 0);

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_open, req->on_open);
  assert(err == 0);

  err = js_create_reference(env, column_families_array, req->column_families);
  assert(err == 0);

  err = rocksdb_open(&db->handle, &req->handle, path, &db->options, column_families, handles, len, rocksdb_native__on_open);
  assert(err == 0);

  err = js_add_deferred_teardown_callback(env, rocksdb_native__on_teardown, (void *) db, &db->teardown);
  assert(err == 0);

  return handle;
}

static js_arraybuffer_t
rocksdb_native_close(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_receiver_t ctx,
  cb_on_close_t on_close
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_close_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_close, req->on_close);
  assert(err == 0);

  db->closing = true;

  err = rocksdb_close(&db->handle, &req->handle, rocksdb_native__on_close);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native__on_suspend(rocksdb_suspend_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_suspend_t *req = (rocksdb_native_suspend_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  js_deferred_teardown_t *teardown = db->teardown;

  if (db->exiting) {
    req->on_suspend.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_suspend_t cb;
    err = js_get_reference_value(env, req->on_suspend, cb);
    assert(err == 0);

    std::optional<js_string_t> error;

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    }

    js_call_function_with_checkpoint(env, cb, ctx, error);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static js_arraybuffer_t
rocksdb_native_suspend(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_receiver_t ctx,
  cb_on_suspend_t on_suspend
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_suspend_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_suspend, req->on_suspend);
  assert(err == 0);

  err = rocksdb_suspend(&db->handle, &req->handle, rocksdb_native__on_suspend);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native__on_resume(rocksdb_resume_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_resume_t *req = (rocksdb_native_resume_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  js_deferred_teardown_t *teardown = db->teardown;

  if (db->exiting) {
    req->on_resume.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_resume_t cb;
    err = js_get_reference_value(env, req->on_resume, cb);
    assert(err == 0);

    req->on_resume.reset();
    req->ctx.reset();

    std::optional<js_string_t> error;

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    }

    js_call_function_with_checkpoint(env, cb, ctx, error);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static js_arraybuffer_t
rocksdb_native_resume(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_receiver_t ctx,
  cb_on_resume_t on_resume
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_resume_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_resume, req->on_resume);
  assert(err == 0);

  err = rocksdb_resume(&db->handle, &req->handle, rocksdb_native__on_resume);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native__on_column_family_teardown(void *data) {
  int err;

  rocksdb_native_column_family_t *column_family = (rocksdb_native_column_family_t *) data;

  js_env_t *env = column_family->env;

  err = rocksdb_column_family_destroy(column_family->db, column_family->handle);
  assert(err == 0);

  column_family->ctx.reset();
}

static js_arraybuffer_t
rocksdb_native_column_family_init(
  js_env_t *env,
  char *name,
  bool enable_blob_files,
  uint64_t min_blob_size,
  uint64_t blob_file_size,
  bool enable_blob_garbage_collection,
  uint64_t table_block_size,
  bool table_cache_index_and_filter_blocks,
  uint32_t table_format_version,
  bool optimize_filters_for_memory,
  bool no_block_cache,
  uint32_t filter_policy_type,
  double bits_per_key,
  int32_t bloom_before_level = 0
) {
  int err;

  rocksdb_filter_policy_t filter_policy = {rocksdb_filter_policy_type_t(filter_policy_type)};

  switch (filter_policy_type) {
  case rocksdb_bloom_filter_policy: {
    filter_policy.bloom = (rocksdb_bloom_filter_options_t) {
      0,
      bits_per_key
    };

    break;
  }
  case rocksdb_ribbon_filter_policy: {
    filter_policy.ribbon = (rocksdb_ribbon_filter_options_t) {
      0,
      bits_per_key,
      bloom_before_level,
    };

    break;
  }
  }

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  js_arraybuffer_t handle;

  rocksdb_native_column_family_t *column_family;
  err = js_create_arraybuffer(env, column_family, handle);
  assert(err == 0);

  column_family->env = env;
  column_family->db = NULL;
  column_family->handle = NULL;

  column_family->descriptor = (rocksdb_column_family_descriptor_t) {
    name,
    {
      2,
      rocksdb_level_compaction,
      enable_blob_files,
      min_blob_size,
      blob_file_size,
      enable_blob_garbage_collection,
      table_block_size,
      table_cache_index_and_filter_blocks,
      table_format_version,
      optimize_filters_for_memory,
      no_block_cache,
      filter_policy,
    }
  };

  return handle;
}

static void
rocksdb_native_column_family_destroy(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_column_family_t, 1> column_family
) {
  int err;

  if (column_family->handle == NULL) return;

  err = rocksdb_column_family_destroy(column_family->db, column_family->handle);
  assert(err == 0);

  err = js_remove_teardown_callback(env, rocksdb_native__on_column_family_teardown, column_family);
  assert(err == 0);

  column_family->ctx.reset();

  column_family->handle = NULL;
}

static js_value_t *
rocksdb_native_iterator_init(js_env_t *env, js_callback_info_t *info) {
  int err;

  js_value_t *handle;

  rocksdb_native_iterator_t *req;
  err = js_create_arraybuffer(env, sizeof(rocksdb_native_iterator_t), (void **) &req, &handle);
  assert(err == 0);

  req->env = env;
  req->closing = false;
  req->exiting = false;
  req->handle.data = (void *) req;

  return handle;
}

static js_value_t *
rocksdb_native_iterator_buffer(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_iterator_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, NULL);
  assert(err == 0);

  uint32_t capacity;
  err = js_get_value_uint32(env, argv[1], &capacity);
  assert(err == 0);

  js_value_t *handle;

  uint8_t *data;
  err = js_create_arraybuffer(env, 2 * capacity * sizeof(rocksdb_slice_t), (void **) &data, &handle);
  assert(err == 0);

  size_t offset = 0;

  req->keys = (rocksdb_slice_t *) &data[offset];

  offset += capacity * sizeof(rocksdb_slice_t);

  req->values = (rocksdb_slice_t *) &data[offset];

  return handle;
}

static void
rocksdb_native__on_iterator_close(rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *req = (rocksdb_native_iterator_t *) handle->data;

  js_env_t *env = req->env;

  js_deferred_teardown_t *teardown = req->teardown;

  if (req->exiting) {
    err = js_delete_reference(env, req->on_open);
    assert(err == 0);

    err = js_delete_reference(env, req->on_close);
    assert(err == 0);

    err = js_delete_reference(env, req->on_read);
    assert(err == 0);

    err = js_delete_reference(env, req->ctx);
    assert(err == 0);
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *ctx;
    err = js_get_reference_value(env, req->ctx, &ctx);
    assert(err == 0);

    js_value_t *cb;
    err = js_get_reference_value(env, req->on_close, &cb);
    assert(err == 0);

    err = js_delete_reference(env, req->on_open);
    assert(err == 0);

    err = js_delete_reference(env, req->on_close);
    assert(err == 0);

    err = js_delete_reference(env, req->on_read);
    assert(err == 0);

    err = js_delete_reference(env, req->ctx);
    assert(err == 0);

    js_value_t *error;

    if (req->handle.error) {
      err = js_create_string_utf8(env, (utf8_t *) req->handle.error, -1, &error);
      assert(err == 0);
    } else {
      err = js_get_null(env, &error);
      assert(err == 0);
    }

    js_call_function_with_checkpoint(env, ctx, cb, 1, (js_value_t *[]) {error}, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }

  err = js_finish_deferred_teardown_callback(teardown);
  assert(err == 0);
}

static void
rocksdb_native__on_iterator_open(rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *req = (rocksdb_native_iterator_t *) handle->data;

  if (req->exiting) return;

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

  js_call_function_with_checkpoint(env, ctx, cb, 1, (js_value_t *[]) {error}, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);
}

static void
rocksdb_native__on_iterator_teardown(js_deferred_teardown_t *handle, void *data) {
  int err;

  rocksdb_native_iterator_t *req = (rocksdb_native_iterator_t *) data;

  req->exiting = true;

  if (req->closing) return;

  err = rocksdb_iterator_close(&req->handle, rocksdb_native__on_iterator_close);
  assert(err == 0);
}

static js_value_t *
rocksdb_native_iterator_open(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 14;
  js_value_t *argv[14];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 14);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  rocksdb_native_iterator_t *req;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &req, NULL);
  assert(err == 0);

  rocksdb_native_column_family_t *column_family;
  err = js_get_arraybuffer_info(env, argv[2], (void **) &column_family, NULL);
  assert(err == 0);

  rocksdb_range_t range;

  err = js_get_typedarray_info(env, argv[3], NULL, (void **) &range.gt.data, &range.gt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[4], NULL, (void **) &range.gte.data, &range.gte.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[5], NULL, (void **) &range.lt.data, &range.lt.len, NULL, NULL);
  assert(err == 0);

  err = js_get_typedarray_info(env, argv[6], NULL, (void **) &range.lte.data, &range.lte.len, NULL, NULL);
  assert(err == 0);

  rocksdb_iterator_options_t options = {
    .version = 0,
  };

  err = js_get_value_bool(env, argv[7], &options.reverse);
  assert(err == 0);

  err = js_get_value_bool(env, argv[8], &options.keys_only);
  assert(err == 0);

  bool has_snapshot;
  err = js_is_arraybuffer(env, argv[9], &has_snapshot);
  assert(err == 0);

  if (has_snapshot) {
    err = js_get_arraybuffer_info(env, argv[9], (void **) &options.snapshot, NULL);
    assert(err == 0);
  }

  err = js_create_reference(env, argv[10], 1, &req->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[11], 1, &req->on_open);
  assert(err == 0);

  err = js_create_reference(env, argv[12], 1, &req->on_close);
  assert(err == 0);

  err = js_create_reference(env, argv[13], 1, &req->on_read);
  assert(err == 0);

  err = rocksdb_iterator_open(&db->handle, &req->handle, column_family->handle, range, &options, rocksdb_native__on_iterator_open);
  assert(err == 0);

  err = js_add_deferred_teardown_callback(env, rocksdb_native__on_iterator_teardown, (void *) req, &req->teardown);
  assert(err == 0);

  return NULL;
}

static js_value_t *
rocksdb_native_iterator_close(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 1);

  rocksdb_native_iterator_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, NULL);
  assert(err == 0);

  req->closing = true;

  err = rocksdb_iterator_close(&req->handle, rocksdb_native__on_iterator_close);
  assert(err == 0);

  return NULL;
}

static int
rocksdb_native_try_create_external_arraybuffer(js_env_t *env, void *data, size_t len, js_value_t **result) {
  // the external arraybuffer api is optional per (https://nodejs.org/api/n-api.html#napi_create_external_arraybuffer)
  // so provide a fallback that does a memcpy
  int err = js_create_external_arraybuffer(env, data, len, rocksdb_native__on_free, NULL, result);
  if (err == 0) return 0;

  void *cpy;
  err = js_create_arraybuffer(env, len, &cpy, result);
  if (err != 0) return err;

  memcpy(cpy, data, len);
  free(data);

  return 0;
}

static void
rocksdb_native__on_iterator_read(rocksdb_iterator_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_iterator_t *req = (rocksdb_native_iterator_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  size_t len = req->handle.len;

  if (db->exiting) {
    if (status == 0 && req->handle.error == NULL) {
      for (size_t i = 0; i < len; i++) {
        js_value_t *result;

        rocksdb_slice_destroy(&req->keys[i]);

        rocksdb_slice_destroy(&req->values[i]);
      }
    }
  } else {
    js_env_t *env = req->env;

    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *ctx;
    err = js_get_reference_value(env, req->ctx, &ctx);
    assert(err == 0);

    js_value_t *cb;
    err = js_get_reference_value(env, req->on_read, &cb);
    assert(err == 0);

    js_value_t *keys;
    err = js_create_array_with_length(env, len, &keys);
    assert(err == 0);

    js_value_t *values;
    err = js_create_array_with_length(env, len, &values);
    assert(err == 0);

    js_value_t *error;

    if (req->handle.error) {
      err = js_create_string_utf8(env, (utf8_t *) req->handle.error, -1, &error);
      assert(err == 0);
    } else {
      err = js_get_null(env, &error);
      assert(err == 0);

      for (size_t i = 0; i < len; i++) {
        js_value_t *result;

        rocksdb_slice_t *key = &req->keys[i];

        err = rocksdb_native_try_create_external_arraybuffer(env, (void *) key->data, key->len, &result);
        assert(err == 0);

        err = js_set_element(env, keys, i, result);
        assert(err == 0);

        rocksdb_slice_t *value = &req->values[i];

        err = rocksdb_native_try_create_external_arraybuffer(env, (void *) value->data, value->len, &result);
        assert(err == 0);

        err = js_set_element(env, values, i, result);
        assert(err == 0);
      }
    }

    js_call_function_with_checkpoint(env, ctx, cb, 3, (js_value_t *[]) {error, keys, values}, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static js_value_t *
rocksdb_native_iterator_read(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  rocksdb_native_iterator_t *req;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &req, NULL);
  assert(err == 0);

  uint32_t capacity;
  err = js_get_value_uint32(env, argv[1], &capacity);
  assert(err == 0);

  err = rocksdb_iterator_read(&req->handle, req->keys, req->values, capacity, rocksdb_native__on_iterator_read);
  assert(err == 0);

  return NULL;
}

static js_arraybuffer_t
rocksdb_native_read_init(js_env_t *env) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_read_batch_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = req;

  return handle;
}

static js_arraybuffer_t
rocksdb_native_read_buffer(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_read_batch_t, 1> req,
  uint32_t capacity
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_read_t *reads;
  err = js_create_arraybuffer(env, capacity, reads, handle);
  assert(err == 0);

  req->capacity = capacity;
  req->reads = reads;

  return handle;
}

static void
rocksdb_native__on_read(rocksdb_read_batch_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_read_batch_t *req = (rocksdb_native_read_batch_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  size_t len = req->handle.len;

  if (db->exiting) {
    if (status == 0) {
      for (size_t i = 0; i < len; i++) {
        js_value_t *result;

        char *error = req->handle.errors[i];

        if (error) continue;

        rocksdb_slice_destroy(&req->reads[i].value);
      }
    }

    err = js_delete_reference(env, req->on_status);
    assert(err == 0);

    err = js_delete_reference(env, req->ctx);
    assert(err == 0);
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *errors;
    err = js_create_array_with_length(env, len, &errors);
    assert(err == 0);

    js_value_t *values;
    err = js_create_array_with_length(env, len, &values);
    assert(err == 0);

    for (size_t i = 0; i < len; i++) {
      js_value_t *result;

      char *error = req->handle.errors[i];

      if (error) {
        err = js_create_string_utf8(env, (utf8_t *) error, -1, &result);
        assert(err == 0);

        err = js_set_element(env, errors, i, result);
        assert(err == 0);
      } else {
        rocksdb_slice_t *slice = &req->reads[i].value;

        if (slice->data == NULL && slice->len == (size_t) -1) {
          err = js_get_null(env, &result);
          assert(err == 0);
        } else {
          err = rocksdb_native_try_create_external_arraybuffer(env, (void *) slice->data, slice->len, &result);
          assert(err == 0);
        }

        err = js_set_element(env, values, i, result);
        assert(err == 0);
      }
    }

    js_value_t *ctx;
    err = js_get_reference_value(env, req->ctx, &ctx);
    assert(err == 0);

    js_value_t *cb;
    err = js_get_reference_value(env, req->on_status, &cb);
    assert(err == 0);

    err = js_delete_reference(env, req->on_status);
    assert(err == 0);

    err = js_delete_reference(env, req->ctx);
    assert(err == 0);

    js_call_function_with_checkpoint(env, ctx, cb, 2, (js_value_t *[]) {errors, values}, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static js_value_t *
rocksdb_native_read(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 6;
  js_value_t *argv[6];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 6);

  rocksdb_native_t *db;
  err = js_get_arraybuffer_info(env, argv[0], (void **) &db, NULL);
  assert(err == 0);

  rocksdb_native_read_batch_t *req;
  err = js_get_arraybuffer_info(env, argv[1], (void **) &req, NULL);
  assert(err == 0);

  uint32_t len;
  err = js_get_array_length(env, argv[2], &len);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &req->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[5], 1, &req->on_status);
  assert(err == 0);

  auto elements = reinterpret_cast<js_value_t **>(malloc(len * sizeof(js_value_t *)));

  err = js_get_array_elements(env, argv[2], elements, len, 0, NULL);
  assert(err == 0);

  for (uint32_t i = 0; i < len; i++) {
    js_value_t *read = elements[i];

    js_value_t *property;

    err = js_get_named_property(env, read, "type", &property);
    assert(err == 0);

    rocksdb_read_type_t type;
    err = js_get_value_uint32(env, property, reinterpret_cast<uint32_t *>(&type));
    assert(err == 0);

    req->reads[i].type = type;

    err = js_get_named_property(env, read, "columnFamily", &property);
    assert(err == 0);

    rocksdb_native_column_family_t *column_family;
    err = js_get_arraybuffer_info(env, property, (void **) &column_family, NULL);
    assert(err == 0);

    req->reads[i].column_family = column_family->handle;

    switch (type) {
    case rocksdb_get: {
      rocksdb_slice_t *key = &req->reads[i].key;

      err = js_get_named_property(env, read, "key", &property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, NULL, (void **) &key->data, &key->len, NULL, NULL);
      assert(err == 0);
      break;
    }
    }
  }

  free(elements);

  rocksdb_read_options_t options = {
    .version = 0,
  };

  bool has_snapshot;
  err = js_is_arraybuffer(env, argv[3], &has_snapshot);
  assert(err == 0);

  if (has_snapshot) {
    err = js_get_arraybuffer_info(env, argv[3], (void **) &options.snapshot, NULL);
    assert(err == 0);
  }

  err = rocksdb_read(&db->handle, &req->handle, req->reads, len, &options, rocksdb_native__on_read);
  assert(err == 0);

  return NULL;
}

static js_arraybuffer_t
rocksdb_native_write_init(js_env_t *env) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_write_batch_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = req;

  return handle;
}

static js_arraybuffer_t
rocksdb_native_write_buffer(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_write_batch_t, 1> req,
  uint32_t capacity
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_write_t *writes;
  err = js_create_arraybuffer(env, capacity, writes, handle);
  assert(err == 0);

  req->capacity = capacity;
  req->writes = writes;

  return handle;
}

static void
rocksdb_native__on_write(rocksdb_write_batch_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_write_batch_t *req = (rocksdb_native_write_batch_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  if (db->exiting) {
    req->on_write.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    std::optional<js_string_t> error;

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    }

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_write_t cb;
    err = js_get_reference_value(env, req->on_write, cb);
    assert(err == 0);

    req->on_write.reset();
    req->ctx.reset();

    js_call_function_with_checkpoint(env, cb, ctx, error);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static void
rocksdb_native_write(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_arraybuffer_span_of_t<rocksdb_native_write_batch_t, 1> req,
  js_array_t operations,
  js_receiver_t ctx,
  cb_on_write_t on_write
) {
  int err;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_write, req->on_write);
  assert(err == 0);

  std::vector<js_object_t> elements;

  err = js_get_array_elements(env, operations, elements);
  assert(err == 0);

  const auto len = elements.size();

  for (uint32_t i = 0; i < len; i++) {
    js_object_t write = elements[i];

    rocksdb_write_type_t type;
    err = js_get_property(env, write, "type", reinterpret_cast<uint32_t &>(type));
    assert(err == 0);

    req->writes[i].type = type;

    js_arraybuffer_t column_family_property;
    err = js_get_property(env, write, "columnFamily", column_family_property);
    assert(err == 0);

    rocksdb_native_column_family_t *column_family;
    err = js_get_arraybuffer_info(env, column_family_property, column_family);
    assert(err == 0);

    req->writes[i].column_family = column_family->handle;

    js_typedarray_t property;

    switch (type) {
    case rocksdb_put: {
      rocksdb_slice_t *key = &req->writes[i].key;

      err = js_get_property(env, write, "key", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, key->data, key->len);
      assert(err == 0);

      rocksdb_slice_t *value = &req->writes[i].value;

      err = js_get_property(env, write, "value", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, value->data, value->len);
      assert(err == 0);
      break;
    }

    case rocksdb_delete: {
      rocksdb_slice_t *key = &req->writes[i].key;

      err = js_get_property(env, write, "key", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, key->data, key->len);
      assert(err == 0);
      break;
    }

    case rocksdb_delete_range: {
      rocksdb_slice_t *start = &req->writes[i].start;

      err = js_get_property(env, write, "start", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, start->data, start->len);
      assert(err == 0);

      rocksdb_slice_t *end = &req->writes[i].end;

      err = js_get_property(env, write, "end", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, end->data, end->len);
      assert(err == 0);
      break;
    }
    }
  }

  err = rocksdb_write(&db->handle, &req->handle, req->writes, len, NULL, rocksdb_native__on_write);
  assert(err == 0);
}

static void
rocksdb_native__on_flush(rocksdb_flush_t *handle, int status) {
  int err;

  assert(status == 0);

  rocksdb_native_flush_t *req = (rocksdb_native_flush_t *) handle->data;

  rocksdb_native_t *db = (rocksdb_native_t *) req->handle.req.db;

  js_env_t *env = req->env;

  if (db->exiting) {
    req->on_flush.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    std::optional<js_string_t> error;

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    }

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_flush_t cb;
    err = js_get_reference_value(env, req->on_flush, cb);
    assert(err == 0);

    req->on_flush.reset();
    req->ctx.reset();

    js_call_function_with_checkpoint(env, cb, ctx, error);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static js_arraybuffer_t
rocksdb_native_flush(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_arraybuffer_span_of_t<rocksdb_native_column_family_t, 1> column_family,
  js_receiver_t ctx,
  cb_on_flush_t on_flush
) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_flush_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->handle.data = (void *) req;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_flush, req->on_flush);
  assert(err == 0);

  err = rocksdb_flush(&db->handle, &req->handle, column_family->handle, NULL, rocksdb_native__on_flush);
  assert(err == 0);

  return handle;
}

static js_arraybuffer_t
rocksdb_native_snapshot_create(js_env_t *env, js_arraybuffer_span_of_t<rocksdb_native_t, 1> db) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_snapshot_t *snapshot;
  err = js_create_arraybuffer(env, snapshot, handle);
  assert(err == 0);

  err = rocksdb_snapshot_create(&db->handle, &snapshot->handle);
  assert(err == 0);

  return handle;
}

static void
rocksdb_native_snapshot_destroy(js_env_t *env, js_arraybuffer_span_of_t<rocksdb_native_snapshot_t, 1> snapshot) {
  rocksdb_snapshot_destroy(&snapshot->handle);
}

static js_value_t *
rocksdb_native_exports(js_env_t *env, js_value_t *exports) {
  int err;

#define V(name, fn) \
  err = js_set_property<fn>(env, exports, name); \
  assert(err == 0);

  V("init", rocksdb_native_init)
  V("open", rocksdb_native_open)
  V("close", rocksdb_native_close)
  V("suspend", rocksdb_native_suspend)
  V("resume", rocksdb_native_resume)

  V("columnFamilyInit", rocksdb_native_column_family_init)
  V("columnFamilyDestroy", rocksdb_native_column_family_destroy)

  V("readInit", rocksdb_native_read_init)
  V("readBuffer", rocksdb_native_read_buffer)

  V("writeInit", rocksdb_native_write_init)
  V("writeBuffer", rocksdb_native_write_buffer)
  V("write", rocksdb_native_write)

  V("flush", rocksdb_native_flush)

  V("snapshotCreate", rocksdb_native_snapshot_create)
  V("snapshotDestroy", rocksdb_native_snapshot_destroy)
#undef V

#define V(name, fn) \
  { \
    js_value_t *val; \
    err = js_create_function(env, name, -1, fn, NULL, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("iteratorInit", rocksdb_native_iterator_init)
  V("iteratorBuffer", rocksdb_native_iterator_buffer)
  V("iteratorOpen", rocksdb_native_iterator_open)
  V("iteratorClose", rocksdb_native_iterator_close)
  V("iteratorRead", rocksdb_native_iterator_read)

  V("read", rocksdb_native_read)
#undef V

#define V(name, n) \
  { \
    js_value_t *val; \
    err = js_create_uint32(env, n, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("GET", rocksdb_get)
  V("PUT", rocksdb_put)
  V("DELETE", rocksdb_delete)
  V("DELETE_RANGE", rocksdb_delete_range)
#undef V

  return exports;
}

BARE_MODULE(rocksdb_native, rocksdb_native_exports)
