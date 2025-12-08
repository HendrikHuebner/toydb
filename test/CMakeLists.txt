file(GLOB TEST_SOURCES *.cpp)

list(FILTER TEST_SOURCES EXCLUDE REGEX ".*tests\\.cpp$")

# Every test is an executable, therefore make this a shared library
add_library(toydb_test_helpers STATIC test_helpers.cpp)
target_link_libraries(toydb_test_helpers PRIVATE toydb fmt::fmt)
target_include_directories(toydb_test_helpers PUBLIC ${CMAKE_SOURCE_DIR}/include)

list(FILTER TEST_SOURCES EXCLUDE REGEX ".*test_helpers\\.cpp$")

set(TEST_BINARY_DIR ${CMAKE_BINARY_DIR}/tests)
file(MAKE_DIRECTORY ${TEST_BINARY_DIR})

foreach(TEST_SOURCE ${TEST_SOURCES})
    # Get the base name without extension (e.g., lexer_test.cpp -> lexer_test)
    get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)

    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME} PRIVATE toydb toydb_test_helpers GTest::gtest_main fmt::fmt)
    target_link_options(${TEST_NAME} PRIVATE -fuse-ld=${TDB_USE_LINKER})
    set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TEST_BINARY_DIR})
    target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)

    # Register with CTest
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endforeach()
