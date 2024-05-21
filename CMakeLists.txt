cmake_minimum_required(VERSION 3.25)

project(rocksdb_native C CXX)

include(bare)
include(napi)

add_subdirectory(vendor/librocksdb EXCLUDE_FROM_ALL)

add_bare_module(rocksdb_native_bare)

target_sources(
  ${rocksdb_native_bare}
  PRIVATE
    binding.c
)

target_link_libraries(
  ${rocksdb_native_bare}
  PUBLIC
    rocksdb_static
)

add_napi_module(rocksdb_native_node)

bare_include_directories(compat NAPI)

target_include_directories(
  ${rocksdb_native_node}
  PRIVATE
    ${compat}
)

target_sources(
  ${rocksdb_native_node}
  PRIVATE
    binding.c
)

target_link_libraries(
  ${rocksdb_native_node}
  PUBLIC
    rocksdb_static
)
