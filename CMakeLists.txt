cmake_minimum_required(VERSION 3.25)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)
find_package(cmake-napi REQUIRED PATHS node_modules/cmake-napi)
find_package(cmake-npm REQUIRED PATHS node_modules/cmake-npm)

project(rocksdb_native C CXX)

bare_target(target)

fetch_package("github:holepunchto/librocksdb#2abcccb")

if(TARGET c++)
  target_link_libraries(rocksdb PUBLIC c++)
  target_link_libraries(rocksdb_facebook PUBLIC c++)
endif()

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

if(target MATCHES "linux" AND NOT TARGET c++)
  target_link_options(
    ${rocksdb_native_bare}
    PUBLIC
      -static-libstdc++
  )
endif()

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

if(target MATCHES "linux")
  target_link_options(
    ${rocksdb_native_napi}
    PUBLIC
      -static-libstdc++
  )
endif()

resolve_node_module(bare-compat-napi compat)

target_include_directories(
  ${rocksdb_native_napi}
  PRIVATE
    "${compat}/include"
)
