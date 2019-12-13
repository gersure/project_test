cmake_minimum_required(VERSION 3.0)
project(trocksdb)

set(THIRD_PARTY ${CMAKE_CURRENT_SOURCE_DIR}/third-party/)
set(CMAKE_CXX_STANDARD 11)

MESSAGE(STATUS "operation system is ${CMAKE_SYSTEM}")
IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    include_directories(/usr/local/include
            /usr/local/opt/bzip2/include)
    link_directories(/usr/local/lib
            /usr/local/opt/bzip2/lib)
ENDIF()

include_directories(
    ${THIRD_PARTY}/rocksdb/include
    ${THIRD_PARTY}/prometheus/include)
set(THIRD_LIBS
    ${THIRD_PARTY}/rocksdb/lib/librocksdb.a
    ${THIRD_PARTY}/prometheus/lib64/libprometheus-cpp-pull.a
    ${THIRD_PARTY}/prometheus/lib64/libprometheus-cpp-core.a
    lz4
    snappy
    z
    dl
    bz2
    zstd
    pthread
    benchmark
    gflags
    )

set(SOURCES
    main.cpp
        metrics.hh
        benchmark.hh
        generator.hh
        system_metrics.hh
        rocksdb_metrics.hh
        rocksdb_metrics.cc
        system_metrics.cc
        generator.cc
        benchmark.cc
        )


add_executable(trocksdb ${SOURCES})
target_link_libraries(trocksdb ${THIRD_LIBS})

