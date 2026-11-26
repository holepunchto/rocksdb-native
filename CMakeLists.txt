cmake_minimum_required(VERSION 4.0)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)
find_package(cmake-napi REQUIRED PATHS node_modules/cmake-napi)

project(rocksdb_native C CXX)

bare_target(target)

fetch_package("github:holepunchto/libnapish#8cbfe78")
fetch_package("github:holepunchto/librocksdb#918a4eb")
fetch_package("github:holepunchto/libjstl#7777016")

add_bare_module(rocksdb_native_bare INSTALL PRIVACY_MANIFEST PrivacyInfo.xcprivacy)

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

target_link_libraries(
  ${rocksdb_native_napi}
  PRIVATE
    $<TARGET_OBJECTS:rocksdb>
    jstl
    napish::static
  PUBLIC
    rocksdb
    rocksdb_facebook
)
