cmake_minimum_required(VERSION 4.0)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)
find_package(cmake-napi REQUIRED PATHS node_modules/cmake-napi)
find_package(cmake-npm REQUIRED PATHS node_modules/cmake-npm)

project(rocksdb_native C CXX)

bare_target(target)

fetch_package("github:holepunchto/librocksdb#efb0607")
fetch_package("github:holepunchto/libjstl#387f6d0")

add_bare_module(rocksdb_native_bare)

target_sources(
  ${rocksdb_native_bare}
  PRIVATE
    binding.cc
)

target_link_libraries(
  ${rocksdb_native_bare}
  PRIVATE
    $<TARGET_OBJECTS:rocksdb>
    jstl
  PUBLIC
    rocksdb
    rocksdb_facebook
)

add_napi_module(rocksdb_native_napi)

target_sources(
  ${rocksdb_native_napi}
  PRIVATE
    binding.cc
)

target_compile_definitions(
  ${rocksdb_native_napi}
  PRIVATE
    NAPI_VERSION=9
)

target_link_libraries(
  ${rocksdb_native_napi}
  PRIVATE
    $<TARGET_OBJECTS:rocksdb>
    jstl
  PUBLIC
    rocksdb
    rocksdb_facebook
)

resolve_node_module(bare-compat-napi compat)

target_include_directories(
  ${rocksdb_native_napi}
  PRIVATE
    "${compat}/include"
)
