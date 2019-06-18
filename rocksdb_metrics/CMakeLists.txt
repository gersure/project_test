cmake_minimum_required(VERSION 3.0)
project(trocksdb)

set(THIRD_PARTY $ENV{HOME}/third-party/)
set(CMAKE_CXX_STANDARD 11)

include_directories(/usr/local/Cellar/rocksdb/5.18.3/include
        /usr/local/include
        )

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

