cmake_minimum_required(VERSION 3.0.0)
project(hybridfs C CXX)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/third-party/spdlog/include)

set(LIBHYBRIDFS_SRC
  src/hybridfs.cc
)

set(DEPENDENCIES
  stdc++fs
  fuse3
  gflags
)

add_executable(hybridfs hybridfs_main.cc ${LIBHYBRIDFS_SRC})
target_link_libraries(hybridfs ${DEPENDENCIES})

add_executable(testfs test/test.cc)