#include "parser/parser.hpp"
#include "test_helpers.hpp"
#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <source_location>

using namespace toydb;
using namespace toydb::parser;
using namespace toydb::ast;
using namespace toydb::test;

class ParserTest : public ::testing::Test {
   protected:
    void SetUp() override {}

    void TearDown() override {}

    void testSuccessfulParse(const std::string& query, const QueryAST& expected,
                             std::source_location loc = std::source_location::current()) {
        Parser parser(query);
        auto result = parser.parseQuery();
        ASSERT_TRUE(result.has_value())
            << "Failed to parse query: " << query << ", error: " << result.error() << " at "
            << loc.file_name() << ":" << loc.line();
        bool equal = compareQueryAST(expected, *result.value());
        if (!equal) {
            std::cout << "Parsed AST: " << *result.value()->query_ << std::endl;
            std::cout << "Expected AST: " << *expected.query_ << std::endl;
        }
        ASSERT_TRUE(equal) << "Parsed AST does not match expected AST for query: " << query
                           << " at " << loc.file_name() << ":" << loc.line();
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

    // Helper functions to create AST nodes
    std::unique_ptr<Literal> makeLiteral(const std::string& value) {
        return std::make_unique<Literal>(value);
    }

    std::vector<std::unique_ptr<Expression>> makeRow(std::initializer_list<std::string> values) {
        std::vector<std::unique_ptr<Expression>> row;
        for (const auto& value : values) {
            row.push_back(makeLiteral(value));
        }
        return row;
    }

    std::unique_ptr<Insert> makeInsertInto(const std::string& tableName,
                                          std::initializer_list<std::string> columnNames,
                                          std::vector<std::vector<std::unique_ptr<Expression>>> rows) {
        auto insert = std::make_unique<Insert>(tableName);
        insert->columnNames.assign(columnNames.begin(), columnNames.end());
        insert->values = std::move(rows);
        return insert;
    }

    std::unique_ptr<Insert> makeInsertInto(const std::string& tableName,
                                          std::vector<std::vector<std::unique_ptr<Expression>>> rows) {
        auto insert = std::make_unique<Insert>(tableName);
        insert->values = std::move(rows);
        return insert;
    }

    std::unique_ptr<Condition> makeCondition(CompareOp op,
                                             std::unique_ptr<Expression> left,
                                             std::unique_ptr<Expression> right = nullptr) {
        auto cond = std::make_unique<Condition>();
        cond->op = op;
        cond->left = std::move(left);
        cond->right = std::move(right);
        return cond;
    }

    std::unique_ptr<Update> makeUpdate(const std::string& tableName,
                                      std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments,
                                      std::unique_ptr<Expression> where = nullptr) {
        auto update = std::make_unique<Update>(tableName);
        update->assignments = std::move(assignments);
        update->where = std::move(where);
        return update;
    }

    std::unique_ptr<Delete> makeDelete(const std::string& tableName, std::unique_ptr<Expression> where = nullptr) {
        auto deleteStmt = std::make_unique<Delete>(tableName);
        deleteStmt->where = std::move(where);
        return deleteStmt;
    }

    std::unique_ptr<CreateTable> makeCreateTable(const std::string& tableName,
                                                std::vector<std::pair<std::string, DataType>> columns) {
        auto createTable = std::make_unique<CreateTable>(tableName);
        for (auto& [name, type] : columns) {
            createTable->columns.emplace_back(name, type);
        }
        return createTable;
    }
};

// INSERT tests
TEST_F(ParserTest, Insert) {
    std::vector<std::vector<std::unique_ptr<Expression>>> rows;
    rows.push_back(makeRow({"False"}));
    auto insert = makeInsertInto("booleans", {"id"}, std::move(rows));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO booleans (id) VALUES (False);", expected);
}

TEST_F(ParserTest, InsertWithColumns) {
    std::vector<std::vector<std::unique_ptr<Expression>>> rows;
    rows.push_back(makeRow({"1", "John", "0", "True"}));
    auto insert = makeInsertInto("users", {"id", "name", "age", "is_male"}, std::move(rows));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users (id, name, age, is_male) VALUES (1, 'John', 0, True)", expected);
}

TEST_F(ParserTest, InsertWithSemicolon) {
    std::vector<std::vector<std::unique_ptr<Expression>>> rows;
    rows.push_back(makeRow({"99", "David"}));
    auto insert = makeInsertInto("users", {"id", "name"}, std::move(rows));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users (id, name) VALUES (99, 'David');", expected);
}

TEST_F(ParserTest, InsertWithoutColumns) {
    std::vector<std::vector<std::unique_ptr<Expression>>> rows;
    rows.push_back(makeRow({"1", "John", "30"}));
    auto insert = makeInsertInto("users", std::move(rows));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users VALUES (1, 'John', 30)", expected);
}

TEST_F(ParserTest, InsertMultipleRows) {
    std::vector<std::vector<std::unique_ptr<Expression>>> rows;
    rows.push_back(makeRow({"1", "John Doe"}));
    rows.push_back(makeRow({"2", "Jane"}));
    auto insert = makeInsertInto("users", {"id", "name"}, std::move(rows));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users (id, name) VALUES (1, 'John Doe'), (2, 'Jane')", expected);
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
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    auto where = makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("1"));
    auto update = makeUpdate("users", std::move(assignments), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1", expected);
}

TEST_F(ParserTest, UpdateWithWhere2) {
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    auto where = makeCondition(CompareOp::AND,
                               makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("1")),
                               makeCondition(CompareOp::EQUAL, makeLiteral("age"), makeLiteral("12")));
    auto update = makeUpdate("users", std::move(assignments), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 AND age = 12", expected);
}

TEST_F(ParserTest, UpdateWithWhere3) {
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    auto where = makeCondition(CompareOp::OR,
                               makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("1")),
                               makeCondition(CompareOp::EQUAL, makeLiteral("name"), makeLiteral("Bob")));
    auto update = makeUpdate("users", std::move(assignments), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 OR name = 'Bob'", expected);
}

TEST_F(ParserTest, UpdateWithWhere4) {
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    // WHERE id <= 1 OR (id = 2 OR id = 3) OR (id > 4)
    auto where = makeCondition(CompareOp::OR,
                              makeCondition(CompareOp::OR,
                                           makeCondition(CompareOp::LESS_EQUAL, makeLiteral("id"), makeLiteral("1")),
                                           makeCondition(CompareOp::OR,
                                                        makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("2")),
                                                        makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("3")))),
                                        makeCondition(CompareOp::GREATER, makeLiteral("id"), makeLiteral("4")));
    auto update = makeUpdate("users", std::move(assignments), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE id <= 1 OR (id = 2 OR id = 3) OR (id > 4)", expected);
}

TEST_F(ParserTest, UpdateWithWhere5) {
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    // WHERE (id != 1 OR id <= 5) AND (age > 23)
    auto where = makeCondition(CompareOp::AND,
                               makeCondition(CompareOp::OR,
                                           makeCondition(CompareOp::NOT_EQUAL, makeLiteral("id"), makeLiteral("1")),
                                           makeCondition(CompareOp::LESS_EQUAL, makeLiteral("id"), makeLiteral("5"))),
                               makeCondition(CompareOp::GREATER, makeLiteral("age"), makeLiteral("23")));
    auto update = makeUpdate("users", std::move(assignments), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE (id != 1 OR id <= 5) AND (age > 23)", expected);
}

TEST_F(ParserTest, UpdateWithoutWhere) {
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    assignments.emplace_back("name", makeLiteral("John"));
    assignments.emplace_back("age", makeLiteral("30"));
    auto update = makeUpdate("users", std::move(assignments));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30", expected);
}

TEST_F(ParserTest, UpdateMissingTable) {
    testFailedParse("UPDATE SET name = 'John'", "Expected table name");
}

TEST_F(ParserTest, UpdateMissingSet) {
    testFailedParse("UPDATE users name = 'John'", "Expected SET statement");
}

// DELETE tests
TEST_F(ParserTest, DeleteWithWhere) {
    auto deleteStmt = makeDelete("users", makeCondition(CompareOp::EQUAL, makeLiteral("id"), makeLiteral("1")));
    QueryAST expected(deleteStmt.release());
    testSuccessfulParse("DELETE FROM users WHERE id = 1", expected);
}

TEST_F(ParserTest, DeleteWithoutWhere) {
    auto deleteStmt = makeDelete("users");
    QueryAST expected(deleteStmt.release());
    testSuccessfulParse("DELETE FROM users", expected);
}

TEST_F(ParserTest, DeleteMissingFrom) {
    testFailedParse("DELETE users", "Expected FROM");
}

TEST_F(ParserTest, DeleteMissingTable) {
    testFailedParse("DELETE FROM", "Expected table name");
}

// CREATE TABLE tests
TEST_F(ParserTest, CreateTable) {
    std::vector<std::pair<std::string, DataType>> columns = {
        {"id", DataType::getInt32()},
        {"name", DataType::getString()},
        {"age", DataType::getInt32()},
        {"active", DataType::getBool()}
    };
    auto createTable = makeCreateTable("users", std::move(columns));
    QueryAST expected(createTable.release());
    testSuccessfulParse("CREATE TABLE users (id INT, name CHAR, age INT, active BOOL)", expected);
}

TEST_F(ParserTest, CreateTableSingleColumn) {
    std::vector<std::pair<std::string, DataType>> columns = {{"id", DataType::getInt32()}};
    auto createTable = makeCreateTable("users", std::move(columns));
    QueryAST expected(createTable.release());
    testSuccessfulParse("CREATE TABLE users (id INT)", expected);
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
