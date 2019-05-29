cmake_minimum_required(VERSION 3.14)
project(trocksdb)

set(THIRD_PARTY $ENV{HOME}/third-party/)
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
    metric.cc
    metric.hh
    )


add_executable(trocksdb ${SOURCES})
target_link_libraries(trocksdb ${THIRD_LIBS})

