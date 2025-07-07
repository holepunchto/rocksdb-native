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
using cb_on_read_t = js_function_t<void, js_receiver_t, js_array_t, js_array_t>;
using cb_on_iterator_open_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_iterator_close_t = js_function_t<void, js_receiver_t, std::optional<js_string_t>>;
using cb_on_iterator_read_t = js_function_t<
  void,
  js_receiver_t,
  std::optional<js_string_t>,
  std::vector<js_arraybuffer_t>,
  std::vector<js_arraybuffer_t>>;
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
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_iterator_open_t> on_open;
  js_persistent_t<cb_on_iterator_close_t> on_close;
  js_persistent_t<cb_on_iterator_read_t> on_read;

  bool closing;
  bool exiting;

  js_deferred_teardown_t *teardown;
};

struct rocksdb_native_read_batch_t {
  rocksdb_read_batch_t handle;

  rocksdb_read_t *reads;

  size_t capacity;

  js_env_t *env;
  js_persistent_t<js_receiver_t> ctx;
  js_persistent_t<cb_on_read_t> on_read;
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

static js_arraybuffer_t
rocksdb_native_iterator_init(js_env_t *env) {
  int err;

  js_arraybuffer_t handle;

  rocksdb_native_iterator_t *req;
  err = js_create_arraybuffer(env, req, handle);
  assert(err == 0);

  req->env = env;
  req->closing = false;
  req->exiting = false;
  req->handle.data = req;

  return handle;
}

static js_arraybuffer_t
rocksdb_native_iterator_buffer(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_iterator_t, 1> req,
  uint32_t capacity
) {
  int err;

  js_arraybuffer_t handle;

  uint8_t *data;
  err = js_create_arraybuffer(env, 2 * capacity * sizeof(rocksdb_slice_t), data, handle);
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
    req->on_open.reset();
    req->on_close.reset();
    req->on_read.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_iterator_close_t cb;
    err = js_get_reference_value(env, req->on_close, cb);
    assert(err == 0);

    req->on_open.reset();
    req->on_close.reset();
    req->on_read.reset();
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

  js_receiver_t ctx;
  err = js_get_reference_value(env, req->ctx, ctx);
  assert(err == 0);

  cb_on_iterator_open_t cb;
  err = js_get_reference_value(env, req->on_open, cb);
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

static void
rocksdb_native__on_iterator_teardown(js_deferred_teardown_t *handle, void *data) {
  int err;

  rocksdb_native_iterator_t *req = (rocksdb_native_iterator_t *) data;

  req->exiting = true;

  if (req->closing) return;

  err = rocksdb_iterator_close(&req->handle, rocksdb_native__on_iterator_close);
  assert(err == 0);
}

static void
rocksdb_native_iterator_open(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_arraybuffer_span_of_t<rocksdb_native_iterator_t, 1> req,
  js_arraybuffer_span_of_t<rocksdb_native_column_family_t, 1> column_family,
  js_typedarray_t<> gt,
  js_typedarray_t<> gte,
  js_typedarray_t<> lt,
  js_typedarray_t<> lte,
  bool reverse,
  bool keys_only,
  std::optional<js_arraybuffer_t> snapshot,
  js_receiver_t ctx,
  cb_on_iterator_open_t on_open,
  cb_on_iterator_close_t on_close,
  cb_on_iterator_read_t on_read
) {
  int err;

  rocksdb_range_t range;

  err = js_get_typedarray_info(env, gt, range.gt.data, range.gt.len);
  assert(err == 0);

  err = js_get_typedarray_info(env, gte, range.gte.data, range.gte.len);
  assert(err == 0);

  err = js_get_typedarray_info(env, lt, range.lt.data, range.lt.len);
  assert(err == 0);

  err = js_get_typedarray_info(env, lte, range.lte.data, range.lte.len);
  assert(err == 0);

  rocksdb_iterator_options_t options = {
    .version = 0,
    .reverse = reverse,
    .keys_only = keys_only
  };

  if (snapshot) {
    err = js_get_arraybuffer_info(env, snapshot.value(), options.snapshot);
    assert(err == 0);
  }

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_open, req->on_open);
  assert(err == 0);

  err = js_create_reference(env, on_close, req->on_close);
  assert(err == 0);

  err = js_create_reference(env, on_read, req->on_read);
  assert(err == 0);

  err = rocksdb_iterator_open(&db->handle, &req->handle, column_family->handle, range, &options, rocksdb_native__on_iterator_open);
  assert(err == 0);

  err = js_add_deferred_teardown_callback(env, rocksdb_native__on_iterator_teardown, (void *) req, &req->teardown);
  assert(err == 0);
}

static void
rocksdb_native_iterator_close(js_env_t *env, js_arraybuffer_span_of_t<rocksdb_native_iterator_t, 1> req) {
  int err;

  req->closing = true;

  err = rocksdb_iterator_close(&req->handle, rocksdb_native__on_iterator_close);
  assert(err == 0);
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

static int
rocksdb_native_try_create_external_arraybuffer(js_env_t *env, char *data, size_t len, js_arraybuffer_t &result) {
  // the external arraybuffer api is optional per (https://nodejs.org/api/n-api.html#napi_create_external_arraybuffer)
  // so provide a fallback that does a std::copy
  int err = js_create_external_arraybuffer(env, data, len, result);
  if (err == 0) return 0;

  return js_create_arraybuffer(env, data, len, result);
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

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_iterator_read_t cb;
    err = js_get_reference_value(env, req->on_read, cb);
    assert(err == 0);

    std::optional<js_string_t> error;

    std::vector<js_arraybuffer_t> keys;
    keys.reserve(len);

    std::vector<js_arraybuffer_t> values;
    values.reserve(len);

    if (req->handle.error) {
      err = js_create_string(env, req->handle.error, error.emplace());
      assert(err == 0);
    } else {
      for (size_t i = 0; i < len; i++) {
        js_arraybuffer_t result;

        rocksdb_slice_t *key = &req->keys[i];

        err = rocksdb_native_try_create_external_arraybuffer(env, const_cast<char *>(key->data), key->len, result);
        assert(err == 0);

        keys.push_back(result);

        rocksdb_slice_t *value = &req->values[i];

        err = rocksdb_native_try_create_external_arraybuffer(env, const_cast<char *>(value->data), value->len, result);
        assert(err == 0);

        values.push_back(result);
      }
    }

    js_call_function_with_checkpoint(env, cb, ctx, error, keys, values);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static void
rocksdb_native_iterator_read(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_iterator_t, 1> req,
  uint32_t capacity
) {
  int err;

  err = rocksdb_iterator_read(&req->handle, req->keys, req->values, capacity, rocksdb_native__on_iterator_read);
  assert(err == 0);
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
        char *error = req->handle.errors[i];

        if (error) continue;

        rocksdb_slice_destroy(&req->reads[i].value);
      }
    }

    req->on_read.reset();
    req->ctx.reset();
  } else {
    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_array_t errors;
    err = js_create_array(env, len, errors);
    assert(err == 0);

    js_array_t values;
    err = js_create_array(env, len, values);
    assert(err == 0);

    for (size_t i = 0; i < len; i++) {
      char *error = req->handle.errors[i];

      if (error) {
        js_string_t result;

        err = js_create_string(env, error, result);
        assert(err == 0);

        err = js_set_element(env, errors, i, result);
        assert(err == 0);
      } else {
        js_arraybuffer_t result;

        rocksdb_slice_t *slice = &req->reads[i].value;

        if (slice->data == NULL && slice->len == (size_t) -1) {
          err = js_get_null(env, (js_value_t **) result);
          assert(err == 0);
        } else {
          err = rocksdb_native_try_create_external_arraybuffer(env, const_cast<char *>(slice->data), slice->len, result);
          assert(err == 0);
        }

        err = js_set_element(env, values, i, result);
        assert(err == 0);
      }
    }

    js_receiver_t ctx;
    err = js_get_reference_value(env, req->ctx, ctx);
    assert(err == 0);

    cb_on_read_t cb;
    err = js_get_reference_value(env, req->on_read, cb);
    assert(err == 0);

    req->on_read.reset();
    req->ctx.reset();

    js_call_function_with_checkpoint(env, cb, ctx, errors, values);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static void
rocksdb_native_read(
  js_env_t *env,
  js_arraybuffer_span_of_t<rocksdb_native_t, 1> db,
  js_arraybuffer_span_of_t<rocksdb_native_read_batch_t, 1> req,
  js_array_t operations,
  std::optional<js_arraybuffer_t> snapshot,
  js_receiver_t ctx,
  cb_on_read_t on_read
) {
  int err;

  err = js_create_reference(env, ctx, req->ctx);
  assert(err == 0);

  err = js_create_reference(env, on_read, req->on_read);
  assert(err == 0);

  std::vector<js_object_t> elements;

  err = js_get_array_elements(env, operations, elements);
  assert(err == 0);

  const auto len = elements.size();

  for (uint32_t i = 0; i < len; i++) {
    js_object_t read = elements[i];

    rocksdb_read_type_t type;
    err = js_get_property(env, read, "type", reinterpret_cast<uint32_t &>(type));
    assert(err == 0);

    req->reads[i].type = type;

    js_arraybuffer_t column_family_property;
    err = js_get_property(env, read, "columnFamily", column_family_property);
    assert(err == 0);

    rocksdb_native_column_family_t *column_family;
    err = js_get_arraybuffer_info(env, column_family_property, column_family);
    assert(err == 0);

    req->reads[i].column_family = column_family->handle;

    switch (type) {
    case rocksdb_get: {
      js_typedarray_t property;

      rocksdb_slice_t *key = &req->reads[i].key;

      err = js_get_property(env, read, "key", property);
      assert(err == 0);

      err = js_get_typedarray_info(env, property, key->data, key->len);
      assert(err == 0);
      break;
    }
    }
  }

  rocksdb_read_options_t options = {
    .version = 0,
  };

  if (snapshot) {
    err = js_get_arraybuffer_info(env, snapshot.value(), options.snapshot);
    assert(err == 0);
  }

  err = rocksdb_read(&db->handle, &req->handle, req->reads, len, &options, rocksdb_native__on_read);
  assert(err == 0);
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
  V("read", rocksdb_native_read)

  V("writeInit", rocksdb_native_write_init)
  V("writeBuffer", rocksdb_native_write_buffer)
  V("write", rocksdb_native_write)

  V("iteratorInit", rocksdb_native_iterator_init)
  V("iteratorBuffer", rocksdb_native_iterator_buffer)
  V("iteratorOpen", rocksdb_native_iterator_open)
  V("iteratorClose", rocksdb_native_iterator_close)
  V("iteratorRead", rocksdb_native_iterator_read)

  V("flush", rocksdb_native_flush)

  V("snapshotCreate", rocksdb_native_snapshot_create)
  V("snapshotDestroy", rocksdb_native_snapshot_destroy)
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
