{
  'targets': [{
    'target_name': 'rocksdb',
    'include_dirs': [
      '<!(bare-dev paths compat/napi)',
    ],
    'dependencies': [
      './vendor/librocksdb/librocksdb.gyp:librocksdb',
    ],
    'sources': [
      './binding.c',
    ],
    'configurations': {
      'Debug': {
        'defines': ['DEBUG'],
      },
      'Release': {
        'defines': ['NDEBUG'],
      },
    },
  }],
}
