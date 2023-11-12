cmake_minimum_required(VERSION 3.10)

project(FAST-RPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

find_package(Boost REQUIRED)
find_package(Protobuf REQUIRED)

message(STATUS "Boost ${Boost_VERSION}, Protobuf ${Protobuf_VERSION}")

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR})

protobuf_generate_cpp(IMPL_PROTO_SRCS IMPL_PROTO_HDRS proto/fast_impl.proto)

file(GLOB FAST_SRC "src-common/*.cc" "src-resource/*.cc" "src-endpoint/*.cc")
add_library(fastrpc STATIC ${FAST_SRC} ${IMPL_PROTO_SRCS})
target_link_libraries(fastrpc "-libverbs -lrdmacm -lpthread" ${Boost_LIBRARIES} ${Protobuf_LIBRARIES})

protobuf_generate_cpp(TEST_PROTO_SRCS TEST_PROTO_HDRS test/test.proto)

add_executable(client test/client.cc ${TEST_PROTO_SRCS})
add_executable(server test/server.cc ${TEST_PROTO_SRCS})
target_link_libraries(client fastrpc)
target_link_libraries(server fastrpc)