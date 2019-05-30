cmake_minimum_required(VERSION 3.14)
project(trocksdb)

set(THIRD_PARTY $ENV{HOME}/third-party/)

#include_directories(/usr/local/Cellar/rocksdb/5.18.3/include
#        /usr/local/include
#        )

include_directories(
    ${THIRD_PARTY}/rocksdb/include
    ${THIRD_PARTY}/prometheus/include)
set(THIRD_LIBS
    ${THIRD_PARTY}/rocksdb/lib64/librocksdb.a
    ${THIRD_PARTY}/prometheus/lib64/libprometheus-cpp-pull.a
    ${THIRD_PARTY}/prometheus/lib64/libprometheus-cpp-core.a
    lz4
    snappy
    z
    zstd
    pthread
    benchmark
    )

set(SOURCES
    main.cpp
        metrics.hh
        rocksdb_metrics.cc
        rocksdb_metrics.hh
        system_metrics.cc
        system_metrics.hh)


add_executable(trocksdb ${SOURCES})
target_link_libraries(trocksdb ${THIRD_LIBS})

