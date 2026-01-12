#include "parser/parser.hpp"
#include "test_helpers.hpp"
#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <source_location>
#include <cctype>

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
    std::unique_ptr<ConstantString> makeLiteral(const std::string& value) {
        return std::make_unique<ConstantString>(value);
    }

    std::unique_ptr<ColumnRef> ident(const std::string& name) {
        return std::make_unique<ColumnRef>(name);
    }

    std::unique_ptr<ConstantInt> makeIntLiteral(int64_t value, bool isInt64 = false) {
        return std::make_unique<ConstantInt>(value, isInt64);
    }

    std::unique_ptr<ConstantBool> makeBoolLiteral(bool value) {
        return std::make_unique<ConstantBool>(value);
    }

    std::unique_ptr<Expression> makeExpression(const std::string& value) {
        // Try to parse as integer, otherwise treat as string
        try {
            size_t pos = 0;
            int64_t intVal = std::stoll(value, &pos);
            if (pos == value.size()) {
                // Entire string is an integer
                return makeIntLiteral(intVal, false);
            }
        } catch (...) {
            // Not an integer, treat as string
        }
        // Check for boolean keywords (exact match - parser only recognizes "true"/"TRUE" and "false"/"FALSE")
        if (value == "true" || value == "TRUE") {
            return makeBoolLiteral(true);
        } else if (value == "false" || value == "FALSE") {
            return makeBoolLiteral(false);
        }
        // For mixed case like "False" or "True", parser treats them as identifiers (ColumnRef)
        // So we return them as string literals for INSERT values, or use ident() for WHERE clauses
        return makeLiteral(value);
    }

    std::vector<std::unique_ptr<Expression>> makeRow(std::initializer_list<std::string> values) {
        std::vector<std::unique_ptr<Expression>> row;
        for (const auto& value : values) {
            row.push_back(makeExpression(value));
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

    // Comparison condition helpers - create binary comparison conditions
    // Left side is always an identifier (parsed as ColumnRef by parser)
    // Right side can be string or int
    std::unique_ptr<Condition> eq(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::EQUAL, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> eq(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::EQUAL, ident(left), makeIntLiteral(right, false));
    }

    std::unique_ptr<Condition> ne(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::NOT_EQUAL, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> ne(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::NOT_EQUAL, ident(left), makeIntLiteral(right, false));
    }

    std::unique_ptr<Condition> gt(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::GREATER, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> gt(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::GREATER, ident(left), makeIntLiteral(right, false));
    }

    std::unique_ptr<Condition> lt(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::LESS, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> lt(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::LESS, ident(left), makeIntLiteral(right, false));
    }

    std::unique_ptr<Condition> gte(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::GREATER_EQUAL, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> gte(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::GREATER_EQUAL, ident(left), makeIntLiteral(right, false));
    }

    std::unique_ptr<Condition> lte(const std::string& left, const std::string& right) {
        return makeCondition(CompareOp::LESS_EQUAL, ident(left), makeExpression(right));
    }

    std::unique_ptr<Condition> lte(const std::string& left, int64_t right) {
        return makeCondition(CompareOp::LESS_EQUAL, ident(left), makeIntLiteral(right, false));
    }

    // Logical condition helpers - chain AND/OR conditions
    std::unique_ptr<Condition> andCond(std::unique_ptr<Condition> left, std::unique_ptr<Condition> right) {
        return makeCondition(CompareOp::AND, std::move(left), std::move(right));
    }

    std::unique_ptr<Condition> orCond(std::unique_ptr<Condition> left, std::unique_ptr<Condition> right) {
        return makeCondition(CompareOp::OR, std::move(left), std::move(right));
    }

    // Helper to create UPDATE assignments from initializer list
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> makeAssignments(
        std::initializer_list<std::pair<std::string, std::string>> pairs) {
        std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
        for (const auto& [col, val] : pairs) {
            assignments.emplace_back(col, makeExpression(val));
        }
        return assignments;
    }

    // Helper to create multiple rows for INSERT
    std::vector<std::vector<std::unique_ptr<Expression>>> makeRows(
        std::initializer_list<std::initializer_list<std::string>> rowLists) {
        std::vector<std::vector<std::unique_ptr<Expression>>> rows;
        for (const auto& rowList : rowLists) {
            rows.push_back(makeRow(rowList));
        }
        return rows;
    }
};

// INSERT tests
TEST_F(ParserTest, Insert) {
    // False (capital F) is parsed as an identifier (ColumnRef), not a boolean literal
    // Only "false" and "FALSE" are recognized as boolean keywords
    auto insert = makeInsertInto("booleans", {"id"}, {});
    std::vector<std::unique_ptr<Expression>> row;
    row.push_back(ident("False"));
    insert->values.push_back(std::move(row));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO booleans (id) VALUES (False);", expected);
}

TEST_F(ParserTest, InsertWithColumns) {
    // True (capital T) is parsed as an identifier (ColumnRef), not a boolean literal
    // Only "true" and "TRUE" are recognized as boolean keywords
    auto insert = makeInsertInto("users", {"id", "name", "age", "is_male"}, {});
    std::vector<std::unique_ptr<Expression>> row;
    row.push_back(makeIntLiteral(1, false));
    row.push_back(makeLiteral("John"));
    row.push_back(makeIntLiteral(0, false));
    row.push_back(ident("True"));
    insert->values.push_back(std::move(row));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users (id, name, age, is_male) VALUES (1, 'John', 0, True)", expected);
}

TEST_F(ParserTest, InsertWithSemicolon) {
    auto insert = makeInsertInto("users", {"id", "name"}, makeRows({{"99", "David"}}));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users (id, name) VALUES (99, 'David');", expected);
}

TEST_F(ParserTest, InsertWithoutColumns) {
    auto insert = makeInsertInto("users", makeRows({{"1", "John", "30"}}));
    QueryAST expected(insert.release());
    testSuccessfulParse("INSERT INTO users VALUES (1, 'John', 30)", expected);
}

TEST_F(ParserTest, InsertMultipleRows) {
    auto insert = makeInsertInto("users", {"id", "name"}, makeRows({{"1", "John Doe"}, {"2", "Jane"}}));
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
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}), eq("id", 1));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1", expected);
}

TEST_F(ParserTest, UpdateWithWhere2) {
    auto where = andCond(eq("id", 1), eq("age", 12));
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 AND age = 12", expected);
}

TEST_F(ParserTest, UpdateWithWhere3) {
    auto where = orCond(eq("id", 1), eq("name", "Bob"));
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse("UPDATE users SET name = 'John', age = 30 WHERE id = 1 OR name = 'Bob'", expected);
}

TEST_F(ParserTest, UpdateWithWhere4) {
    // WHERE id <= 1 OR (id = 2 OR id = 3) OR (id > 4)
    auto where = orCond(
        orCond(lte("id", 1), orCond(eq("id", 2), eq("id", 3))),
        gt("id", 4)
    );
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE id <= 1 OR (id = 2 OR id = 3) OR (id > 4)", expected);
}

TEST_F(ParserTest, UpdateWithWhere5) {
    // WHERE (id != 1 OR id <= 5) AND (age > 23)
    auto where = andCond(
        orCond(ne("id", 1), lte("id", 5)),
        gt("age", 23)
    );
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}), std::move(where));
    QueryAST expected(update.release());
    testSuccessfulParse(
        "UPDATE users SET name = 'John', age = 30 WHERE (id != 1 OR id <= 5) AND (age > 23)", expected);
}

TEST_F(ParserTest, UpdateWithoutWhere) {
    auto update = makeUpdate("users", makeAssignments({{"name", "John"}, {"age", "30"}}));
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
    auto deleteStmt = makeDelete("users", eq("id", 1));
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

TEST_F(ParserTest, EmptyQuery) {
    testFailedParse("", "Unsupported query type");
}
