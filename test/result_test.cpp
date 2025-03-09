#include "gtest/gtest.h"
#include "common/result.hpp"
#include <memory>
#include <string>

using namespace toydb;

TEST(ResultTest, BasicTest) {
    int i = 99;
    std::string s{"wee"};
    
    [[maybe_unused]] toydb::Result<std::string> result1 = Success("foo");
    [[maybe_unused]] toydb::Result<int> result2 = Success(i);
    [[maybe_unused]] toydb::Result<std::unique_ptr<int>> result3 = Success(std::make_unique<int>(i));
    [[maybe_unused]] toydb::Result<bool> result4 = Success(true);
    [[maybe_unused]] toydb::Result<void, int> result5 = Success();
    [[maybe_unused]] toydb::Result<int, std::string> result6 = Success(123);

    auto temp = Error( "boo" );
    [[maybe_unused]] toydb::Result<int, std::string> result7 = temp;

    [[maybe_unused]] toydb::Result<void, int> result8 = Error(1);
    [[maybe_unused]] toydb::Result<std::string, std::string> result9 = Error(s);

    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_TRUE(result3.ok());
    ASSERT_TRUE(result4.ok());
    ASSERT_TRUE(result6.ok());
    ASSERT_TRUE(result7.isError());
    ASSERT_TRUE(result8.isError());
    ASSERT_TRUE(result9.isError());
}
