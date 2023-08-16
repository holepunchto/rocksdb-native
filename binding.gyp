{
  'targets': [{
    'target_name': 'rocksdb',
    'include_dirs': [
      '<!(bare-dev paths compat/napi)',
      './vendor/rocksdb/include',
    ],
    'dependencies': [
      './vendor/rocksdb.gyp:librocksdb',
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
