cmake_minimum_required(VERSION 3.20)
project(ToyDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

if (ENABLE_ASAN)
    target_compile_options(toydb PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_libraries(toydb PRIVATE -fsanitize=address)
endif()

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

find_package(spdlog REQUIRED)

file(GLOB_RECURSE DB_SOURCES src/*.cpp)
file(GLOB_RECURSE DB_HEADERS include/*.hpp)

add_library(toydb SHARED ${DB_SOURCES} ${DB_HEADERS})
target_include_directories(toydb PUBLIC ./include)
target_link_libraries(toydb PRIVATE spdlog)

target_compile_options(toydb PRIVATE
    -Wall -Wextra -Wpedantic -Werror
)

enable_testing()
add_subdirectory(test)
