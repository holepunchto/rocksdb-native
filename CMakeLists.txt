cmake_minimum_required(VERSION 3.25)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)
find_package(cmake-napi REQUIRED PATHS node_modules/cmake-napi)
find_package(cmake-npm REQUIRED PATHS node_modules/cmake-npm)

project(rocksdb_native C CXX)

fetch_package("github:holepunchto/librocksdb#aa76842")

add_bare_module(rocksdb_native_bare)

target_sources(
  ${rocksdb_native_bare}
  PRIVATE
    binding.c
)

target_link_libraries(
  ${rocksdb_native_bare}
  PRIVATE
    $<TARGET_OBJECTS:rocksdb>
    $<TARGET_OBJECTS:path>
  PUBLIC
    rocksdb
    rocksdb_facebook
    path
)

add_napi_module(rocksdb_native_napi)

target_sources(
  ${rocksdb_native_napi}
  PRIVATE
    binding.c
)

target_link_libraries(
  ${rocksdb_native_napi}
  PRIVATE
    $<TARGET_OBJECTS:rocksdb>
    $<TARGET_OBJECTS:path>
  PUBLIC
    rocksdb
    rocksdb_facebook
    path
)

resolve_node_module(bare-compat-napi compat)

target_include_directories(
  ${rocksdb_native_napi}
  PRIVATE
    "${compat}/include"
)
