cmake_minimum_required(VERSION 3.20)
project(mini_db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DWIN32_LEAN_AND_MEAN -DNOMINMAX)

add_executable(mini_db
    src/main.cpp
    src/kv/kvstore.cpp
    src/kv/log_segment.cpp
    src/kv/win_file.cpp
    src/kv/crc32.cpp
)

target_include_directories(mini_db PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

if (MSVC)
  target_compile_options(mini_db PRIVATE /W4 /permissive- /EHsc)
else()
  target_compile_options(mini_db PRIVATE -Wall -Wextra -Wpedantic)
endif()
