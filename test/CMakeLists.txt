file(GLOB TEST_SOURCES *.cpp)

set(TEST_BINARY_DIR ${CMAKE_BINARY_DIR}/tests)
file(MAKE_DIRECTORY ${TEST_BINARY_DIR})

add_executable(toydb_tests ${TEST_SOURCES})
target_link_libraries(toydb_tests PRIVATE toydb GTest::gtest_main)
set_target_properties(toydb_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TEST_BINARY_DIR})
target_include_directories(toydb_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_test(NAME toydb_tests COMMAND toydb_tests)
