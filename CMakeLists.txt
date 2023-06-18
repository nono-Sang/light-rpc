cmake_minimum_required(VERSION 3.10)

project(CSTest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# find_library(VERBS_PATH ibverbs)
# find_library(RDMACM_PATH rdmacm)
# message(STATUS "${VERBS_PATH}, ${RDMACM_PATH}")

# set(THREADS_PREFER_PTHREAD_FLAG ON)
# find_package(Threads REQUIRED)
find_package(Boost REQUIRED)
find_package(Mimalloc REQUIRED)
find_package(Protobuf REQUIRED)
message(STATUS "Boost ${Boost_VERSION}, Mimalloc ${Mimalloc_VERSION}, Protobuf ${Protobuf_VERSION}")

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${MIMALLOC_INCLUDE_DIR})
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/inc)

# message(STATUS ${Boost_INCLUDE_DIRS})
# message(STATUS ${MIMALLOC_INCLUDE_DIR})
# message(STATUS ${Protobuf_INCLUDE_DIRS})

protobuf_generate_cpp(IMPL_PROTO_SRCS IMPL_PROTO_HDRS proto/light_impl.proto)
# message(STATUS "PROTO_HDRS: ${IMPL_PROTO_HDRS}, ${IMPL_PROTO_SRCS}")

file(GLOB base_SRC "inc/*.h" "src/*.cc")
add_library(lightrpc STATIC ${base_SRC} ${IMPL_PROTO_SRCS} ${IMPL_PROTO_HDRS})
target_link_libraries(lightrpc 
                      "-libverbs -lrdmacm -lpthread"
                      ${Boost_LIBRARIES} 
                      ${Protobuf_LIBRARIES} 
                      ${MIMALLOC_LIBRARY_DIR}/libmimalloc.so)

protobuf_generate_cpp(TEST_PROTO_SRCS TEST_PROTO_HDRS test/proto/test.proto)
# message(STATUS "PROTO_SRCS: ${TEST_PROTO_SRCS}, ${TEST_PROTO_SRCS}")

add_executable(client test/client.cc ${TEST_PROTO_SRCS})
add_executable(server test/server.cc ${TEST_PROTO_SRCS})
target_link_libraries(client lightrpc)
target_link_libraries(server lightrpc)