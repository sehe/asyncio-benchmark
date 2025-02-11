cmake_minimum_required(VERSION 3.29)
project(asyncio_benchmark)

set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -Ofast -march=native")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address,undefined")

include(FetchContent)
FetchContent_Declare(
        asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
        GIT_TAG        asio-1-32-0
)

FetchContent_MakeAvailable(asio)

add_executable(asyncio_benchmark benchmark.cpp)
target_compile_definitions(asyncio_benchmark PRIVATE ASIO_DISABLE_BOOST)

target_include_directories(asyncio_benchmark PRIVATE ${asio_SOURCE_DIR}/asio/include)
