cmake_minimum_required(VERSION 3.20)
project(toydb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
set(TDB_USE_LINKER "ld" CACHE STRING "Linker to use")

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(nlohmann_json REQUIRED)

# Optionally find libunwind for stack traces
find_package(libunwind QUIET)

include_directories(${CMAKE_SOURCE_DIR}/include)
file(GLOB_RECURSE DB_SOURCES src/*.cpp)
file(GLOB_RECURSE DB_HEADERS include/*.hpp)

add_library(toydb SHARED ${DB_SOURCES} ${DB_HEADERS})
target_link_options(toydb PRIVATE -fuse-ld=${TDB_USE_LINKER})

if (ENABLE_ASAN)
    target_compile_options(toydb PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_libraries(toydb PRIVATE -fsanitize=address)
endif()

target_include_directories(toydb PUBLIC ./include)
target_link_libraries(toydb PRIVATE spdlog::spdlog fmt::fmt arrow parquet nlohmann_json::nlohmann_json)

target_compile_options(toydb PRIVATE
    -Wall -Wextra -Wpedantic -Werror -Wno-gnu-zero-variadic-macro-arguments
)

add_executable(repl src/repl.cpp)
target_link_libraries(repl PRIVATE toydb)
target_link_options(repl PRIVATE -fuse-ld=${TDB_USE_LINKER})
target_include_directories(repl PRIVATE ${CMAKE_SOURCE_DIR}/include)

enable_testing()
add_subdirectory(test)
