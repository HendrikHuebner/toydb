#include "gtest/gtest.h"
#include "common/result.hpp"

using namespace toydb;

TEST(ResultTest, BasicTest) {
    int i = 99;
    [[maybe_unused]] toydb::Result<std::string> result1 = result::success("foo");
    [[maybe_unused]] toydb::Result<int> result2 = result::success(i);
    [[maybe_unused]] toydb::Result<int&> result3 = result::success(i);
    [[maybe_unused]] toydb::Result<const int&> result5 = result::success(i);
    [[maybe_unused]] toydb::Result<bool> result4 = result::success(true);
    [[maybe_unused]] toydb::Result<int, std::string> result6 = result::success(123);
    [[maybe_unused]] toydb::Result<int, std::string> result7 = result::error( "boo" );
    [[maybe_unused]] toydb::Result<int> result8 = result::success(1);

    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_TRUE(result3.ok());
    ASSERT_TRUE(result4.ok());
    ASSERT_TRUE(result5.ok());
    ASSERT_TRUE(result6.ok());
    ASSERT_TRUE(result7.isError());
    ASSERT_TRUE(result8.isError());
}
