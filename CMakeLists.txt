cmake_minimum_required(VERSION 3.25)

project(rocksdb_native C)

include(bare)

if(NOT TARGET rocksdb)
 set(WITH_GFLAGS OFF CACHE BOOL "Toggle GFlags" FORCE)

 set(PORTABLE 1 CACHE BOOL "Make a portable RocksDB build" FORCE)

  add_subdirectory(vendor/rocksdb EXCLUDE_FROM_ALL)
endif()

add_bare_module(rocksdb_native)

target_sources(
  rocksdb_native
  PRIVATE
    binding.c
)

target_include_directories(
  rocksdb_native
  PRIVATE
    vendor/rocksdb/include
)

target_link_libraries(
  rocksdb_native
  PUBLIC
    rocksdb
)
