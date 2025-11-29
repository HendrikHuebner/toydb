#include "parser/parser.hpp"
#include <gtest/gtest.h>
#include <string>

using namespace toydb;
using namespace toydb::parser;
using namespace toydb::ast;

class ParserTest : public ::testing::Test {
   protected:
    void SetUp() override {}

    void TearDown() override {}

    void testSuccessfulParse(const std::string& query) {
        Parser parser(query);
        auto result = parser.parseQuery();
        ASSERT_TRUE(result.has_value())
            << "Failed to parse query: " << query << ", error: " << result.error();
    }

    void testFailedParse(const std::string& query, const std::string& expectedErrorSubstr = "") {
        Parser parser(query);
        auto result = parser.parseQuery();
        ASSERT_TRUE(!result.has_value()) << "Query should have failed to parse: " << query;
        if (!expectedErrorSubstr.empty()) {
            ASSERT_NE(result.error().find(expectedErrorSubstr), std::string::npos)
                << "Error message should contain '" << expectedErrorSubstr
                << "' but was: " << result.error();
        }
    }
};

// INSERT tests
TEST_F(ParserTest, Insert) {
    testSuccessfulParse("INSERT INTO booleans (id) VALUES (False);");
}

TEST_F(ParserTest, InsertWithColumns) {
    testSuccessfulParse("INSERT INTO users (id, name, age, is_male) VALUES (1, 'John', 0, True)");
}

TEST_F(ParserTest, InsertWithSemicolon) {
    testSuccessfulParse("INSERT INTO users (id, name) VALUES (99, 'David');");
}

TEST_F(ParserTest, InsertWithoutColumns) {
    testSuccessfulParse("INSERT INTO users VALUES (1, 'John', 30)");
}

TEST_F(ParserTest, InsertMultipleRows) {
    testSuccessfulParse("INSERT INTO users (id, name) VALUES (1, 'John Doe'), (2, 'Jane')");
}

TEST_F(ParserTest, InsertMissingTable) {
    testFailedParse("INSERT INTO VALUES (1, 'John')", "Expected table name");
}

TEST_F(ParserTest, InsertMissingValues) {
    testFailedParse("INSERT INTO users (id, name)", "Expected VALUES");
}

TEST_F(ParserTest, InsertMissingValues2) {
    testFailedParse("INSERT INTO users (id, name) VALUES", "Expected value list");
}

TEST_F(ParserTest, InsertMissingValues3) {
    testFailedParse("INSERT INTO users (id, name) VALUES (123)",
                    "Number of entries in tuple does not match column list");
}

TEST_F(ParserTest, InsertTooManyValues) {
    testFailedParse("INSERT INTO users (id, name) VALUES (1, 'Bob', True)",
                    "Number of entries in tuple does not match column list");
}

// UPDATE tests
TEST_F(ParserTest, UpdateWithWhere) {
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1");
}

TEST_F(ParserTest, UpdateWithWhere2) {
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 AND age = 12");
}

TEST_F(ParserTest, UpdateWithWhere3) {
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 OR name = 'Bob'");
}

TEST_F(ParserTest, UpdateWithWhere4) {
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE id <= 1 OR (id = 2 OR id = 3) OR (id > 4)");
}

TEST_F(ParserTest, UpdateWithWhere5) {
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE (id != 1 OR id <= 5) AND (age > 23)");
}

TEST_F(ParserTest, UpdateWithoutWhere) {
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30");
}

TEST_F(ParserTest, UpdateMissingTable) {
    testFailedParse("UPDATE SET name = 'John'", "Expected table name");
}

TEST_F(ParserTest, UpdateMissingSet) {
    testFailedParse("UPDATE users name = 'John'", "Expected SET statement");
}

// DELETE tests
TEST_F(ParserTest, DeleteWithWhere) {
    testSuccessfulParse("DELETE FROM users WHERE id = 1");
}

TEST_F(ParserTest, DeleteWithoutWhere) {
    testSuccessfulParse("DELETE FROM users");
}

TEST_F(ParserTest, DeleteMissingFrom) {
    testFailedParse("DELETE users", "Expected FROM");
}

TEST_F(ParserTest, DeleteMissingTable) {
    testFailedParse("DELETE FROM", "Expected table name");
}

// CREATE TABLE tests
TEST_F(ParserTest, CreateTable) {
    testSuccessfulParse("CREATE TABLE users (id INT, name CHAR, age INT, active BOOL)");
}

TEST_F(ParserTest, CreateTableSingleColumn) {
    testSuccessfulParse("CREATE TABLE users (id INT)");
}

TEST_F(ParserTest, CreateTableMissingName) {
    testFailedParse("CREATE TABLE (id INT)", "Expected table name");
}

TEST_F(ParserTest, CreateTableMissingColumns) {
    testFailedParse("CREATE TABLE users", "Expected column definition list");
}

TEST_F(ParserTest, CreateTableInvalidDataType) {
    testFailedParse("CREATE TABLE users (id INVALID)", "Unknown data type");
}

// Mixed error tests
TEST_F(ParserTest, UnsupportedQueryType) {
    testFailedParse("SELECT * FROM users", "Unsupported query type");
}

TEST_F(ParserTest, EmptyQuery) {
    testFailedParse("", "Unsupported query type");
}
