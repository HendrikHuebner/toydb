#include "gtest/gtest.h"
#include "../include/toydb.hpp"

TEST(ToyDBTest, InsertAndRetrieve) {
    ToyDB db;
    db.insert("key1", "value1");
    auto value = db.get("key1");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), "value1");
}

TEST(ToyDBTest, RemoveKey) {
    ToyDB db;
    db.insert("key1", "value1");
    EXPECT_TRUE(db.remove("key1"));
    EXPECT_FALSE(db.get("key1").has_value());
}

TEST(ToyDBTest, GetNonExistentKey) {
    ToyDB db;
    EXPECT_FALSE(db.get("nonexistent").has_value());
}
