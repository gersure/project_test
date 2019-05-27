cmake_minimum_required(VERSION 3.14)
project(trocksddb)

set(CMAKE_CXX_STANDARD 14)


include_directories(/usr/local/Cellar/rocksdb/5.18.3/include
        /usr/local/include
        )

add_executable(trocksddb main.cpp metric.cc metric.hh)
target_link_libraries(trocksddb prometheus-cpp-core prometheus-cpp-pull rocksdb)