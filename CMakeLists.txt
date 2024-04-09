cmake_minimum_required(VERSION 3.25)

project(rocksdb_native C CXX)

include(bare)

add_subdirectory(vendor/librocksdb EXCLUDE_FROM_ALL)

add_bare_module(rocksdb_native)

target_sources(
  ${rocksdb_native}
  PRIVATE
    binding.c
)

target_link_libraries(
  ${rocksdb_native}
  PUBLIC
    rocksdb_static
)
