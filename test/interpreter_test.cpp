#include "planner/interpreter.hpp"
#include "parser/parser.hpp"
#include "engine/predicate_expr.hpp"
#include "test_helpers.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_map>

using namespace toydb;
using namespace toydb::parser;
using namespace toydb::ast;
using namespace toydb::test::plan_validation;

class MockQueryCatalog : public PlaceholderCatalog {
private:
    std::unordered_map<std::string, TableMeta> tables_;
    std::unordered_map<std::string, std::unordered_map<std::string, ColumnId>> columnMap_;
    uint64_t nextColumnId_;

public:
    MockQueryCatalog() : nextColumnId_(1) {}

    void addTable(const std::string& tableName, const std::vector<std::pair<std::string, DataType>>& columns) {
        TableMeta meta;
        meta.name = tableName;
        // Generate TableId from table name (same as Catalog::makeId)
        std::hash<std::string> hasher;
        uint64_t tableIdValue = static_cast<uint64_t>(hasher(tableName));
        meta.id = TableId{tableIdValue, tableName};
        meta.format = StorageFormat::PARQUET;

        std::unordered_map<std::string, ColumnId> tableColumns;
        for (const auto& [colName, colType] : columns) {
            ColumnId colId(nextColumnId_++, colName, meta.id);
            tableColumns[colName] = colId;

            ColumnMeta colMeta;
            colMeta.name = colName;
            colMeta.type = colType.toString();
            colMeta.nullable = true;
            meta.schema.push_back(colMeta);
        }

        tables_[tableName] = meta;
        columnMap_[tableName] = tableColumns;
    }

    std::optional<TableMeta> getTable(const std::string& name) override {
        auto it = tables_.find(name);
        if (it != tables_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    std::optional<ColumnId> resolveColumn(const std::string& tableName, const std::string& columnName) override {
        tdb_assert(!tableName.empty(), "Table name cannot be empty");
        tdb_assert(!columnName.empty(), "Column name cannot be empty");

        auto it = columnMap_.find(tableName);
        if (it != columnMap_.end()) {
            auto colIt = it->second.find(columnName);
            if (colIt != it->second.end()) {
                return colIt->second;
            }
        }
        return std::nullopt;
    }

    // FIXME: This should be handled by the catalog
    std::optional<DataType> getColumnType(const ColumnId& columnId) override {
        // Find the table for this column
        std::string tableName = columnId.getTableId().getName();
        auto tableIt = tables_.find(tableName);
        if (tableIt == tables_.end()) {
            return std::nullopt;
        }

        // Find the column in the schema
        for (const auto& colMeta : tableIt->second.schema) {
            if (colMeta.name == columnId.getName()) {
                if (colMeta.type == "INT64") {
                    return DataType::getInt64();
                } else if (colMeta.type == "INT32") {
                    return DataType::getInt32();
                } else if (colMeta.type == "DOUBLE") {
                    return DataType::getDouble();
                } else if (colMeta.type == "BOOL") {
                    return DataType::getBool();
                } else if (colMeta.type == "STRING") {
                    return DataType::getString();
                }
            }
        }
        return std::nullopt;
    }
};

class InterpreterTest : public ::testing::Test {
protected:
    std::unique_ptr<MockQueryCatalog> catalog_;
    std::unique_ptr<SQLInterpreter> interpreter_;

    void SetUp() override {
        catalog_ = std::make_unique<MockQueryCatalog>();
        interpreter_ = std::make_unique<SQLInterpreter>(catalog_.get());

        // Add a test table
        catalog_->addTable("users", {
            {"id", DataType::getInt32()},
            {"name", DataType::getString()},
            {"age", DataType::getInt32()}
        });
    }

    void TearDown() override {}
};

TEST_F(InterpreterTest, SimpleSelect) {
    Parser parser("SELECT id, name FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    // Verify the plan structure
    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    // Should have a ProjectionOp at the root
    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);

    // Check projection columns
    const auto& columns = projection->getColumns();
    ASSERT_EQ(columns.size(), 2);
    ASSERT_EQ(columns[0].getName(), "id");
    ASSERT_EQ(columns[1].getName(), "name");
}

TEST_F(InterpreterTest, SelectWithWhere) {
    Parser parser("SELECT id FROM users WHERE id = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    // Verify the plan structure: Projection -> Filter
    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->getChildCount(), 1);

    auto* filter = dynamic_cast<FilterOp*>(projection->getChild(0).get());
    ASSERT_NE(filter, nullptr);
    ASSERT_NE(filter->getPredicate(), nullptr);

    // Verify the predicate is a CompareExpr with EQUAL operator
    auto* compareExpr = dynamic_cast<const CompareExpr*>(filter->getPredicate());
    ASSERT_NE(compareExpr, nullptr);
    ASSERT_EQ(compareExpr->getOp(), CompareOp::EQUAL);

    // Verify left operand is a ColumnRef for "id"
    auto* leftColRef = dynamic_cast<const ColumnRefExpr*>(compareExpr->getLeft());
    ASSERT_NE(leftColRef, nullptr);
    ASSERT_EQ(leftColRef->getColumnId().getName(), "id");

    // Verify right operand is a Constant with value 1
    auto* rightConst = dynamic_cast<const ConstantExpr*>(compareExpr->getRight());
    ASSERT_NE(rightConst, nullptr);
    ASSERT_FALSE(rightConst->isNull());
    ASSERT_EQ(rightConst->getType(), DataType::getInt32());
    ASSERT_EQ(rightConst->getIntValue(), 1);
}

TEST_F(InterpreterTest, SelectWithWhereAnd) {
    Parser parser("SELECT id FROM users WHERE id = 1 AND age > 20");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    // Verify the plan structure
    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->getChildCount(), 1);

    auto* filter = dynamic_cast<FilterOp*>(projection->getChild(0).get());
    ASSERT_NE(filter, nullptr);

    // The predicate should be a LogicalExpr with AND
    auto* logicalExpr = dynamic_cast<const LogicalExpr*>(filter->getPredicate());
    ASSERT_NE(logicalExpr, nullptr);
    ASSERT_EQ(logicalExpr->getOp(), CompareOp::AND);

    // Verify left operand: id = 1
    auto* leftCompare = dynamic_cast<const CompareExpr*>(logicalExpr->getLeft());
    ASSERT_NE(leftCompare, nullptr);
    ASSERT_EQ(leftCompare->getOp(), CompareOp::EQUAL);
    auto* leftCol = dynamic_cast<const ColumnRefExpr*>(leftCompare->getLeft());
    ASSERT_NE(leftCol, nullptr);
    ASSERT_EQ(leftCol->getColumnId().getName(), "id");
    auto* leftConst = dynamic_cast<const ConstantExpr*>(leftCompare->getRight());
    ASSERT_NE(leftConst, nullptr);
    ASSERT_EQ(leftConst->getIntValue(), 1);

    // Verify right operand: age > 20
    auto* rightCompare = dynamic_cast<const CompareExpr*>(logicalExpr->getRight());
    ASSERT_NE(rightCompare, nullptr);
    ASSERT_EQ(rightCompare->getOp(), CompareOp::GREATER);
    auto* rightCol = dynamic_cast<const ColumnRefExpr*>(rightCompare->getLeft());
    ASSERT_NE(rightCol, nullptr);
    ASSERT_EQ(rightCol->getColumnId().getName(), "age");
    auto* rightConst = dynamic_cast<const ConstantExpr*>(rightCompare->getRight());
    ASSERT_NE(rightConst, nullptr);
    ASSERT_EQ(rightConst->getIntValue(), 20);
}

TEST_F(InterpreterTest, SelectWithWhereOr) {
    Parser parser("SELECT id FROM users WHERE id = 1 OR age > 20");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    // Verify the plan structure
    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->getChildCount(), 1);

    auto* filter = dynamic_cast<FilterOp*>(projection->getChild(0).get());
    ASSERT_NE(filter, nullptr);

    // The predicate should be a LogicalExpr with OR
    auto* logicalExpr = dynamic_cast<const LogicalExpr*>(filter->getPredicate());
    ASSERT_NE(logicalExpr, nullptr);
    ASSERT_EQ(logicalExpr->getOp(), CompareOp::OR);

    // Verify left operand: id = 1
    auto* leftCompare = dynamic_cast<const CompareExpr*>(logicalExpr->getLeft());
    ASSERT_NE(leftCompare, nullptr);
    ASSERT_EQ(leftCompare->getOp(), CompareOp::EQUAL);
    auto* leftCol = dynamic_cast<const ColumnRefExpr*>(leftCompare->getLeft());
    ASSERT_NE(leftCol, nullptr);
    ASSERT_EQ(leftCol->getColumnId().getName(), "id");
    auto* leftConst = dynamic_cast<const ConstantExpr*>(leftCompare->getRight());
    ASSERT_NE(leftConst, nullptr);
    ASSERT_EQ(leftConst->getIntValue(), 1);

    // Verify right operand: age > 20
    auto* rightCompare = dynamic_cast<const CompareExpr*>(logicalExpr->getRight());
    ASSERT_NE(rightCompare, nullptr);
    ASSERT_EQ(rightCompare->getOp(), CompareOp::GREATER);
    auto* rightCol = dynamic_cast<const ColumnRefExpr*>(rightCompare->getLeft());
    ASSERT_NE(rightCol, nullptr);
    ASSERT_EQ(rightCol->getColumnId().getName(), "age");
    auto* rightConst = dynamic_cast<const ConstantExpr*>(rightCompare->getRight());
    ASSERT_NE(rightConst, nullptr);
    ASSERT_EQ(rightConst->getIntValue(), 20);
}

TEST_F(InterpreterTest, SelectWithComparisonOperators) {
    Parser parser("SELECT id FROM users WHERE id > 10");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->getChildCount(), 1);

    auto* filter = dynamic_cast<FilterOp*>(projection->getChild(0).get());
    ASSERT_NE(filter, nullptr);

    // The predicate should be a CompareExpr
    auto* compareExpr = dynamic_cast<const CompareExpr*>(filter->getPredicate());
    ASSERT_NE(compareExpr, nullptr);
    ASSERT_EQ(compareExpr->getOp(), CompareOp::GREATER);

    // Verify operands
    auto* leftCol = dynamic_cast<const ColumnRefExpr*>(compareExpr->getLeft());
    ASSERT_NE(leftCol, nullptr);
    ASSERT_EQ(leftCol->getColumnId().getName(), "id");

    auto* rightConst = dynamic_cast<const ConstantExpr*>(compareExpr->getRight());
    ASSERT_NE(rightConst, nullptr);
    ASSERT_EQ(rightConst->getIntValue(), 10);
}

TEST_F(InterpreterTest, SelectWithCast) {
    // Create a table with INT64 column to test cast from INT32 constant
    MockQueryCatalog castCatalog;
    castCatalog.addTable("test_table", {
        {"id", DataType::getInt64()}
    });
    SQLInterpreter castInterpreter(&castCatalog);

    Parser parser("SELECT id FROM test_table WHERE id = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = castInterpreter.interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->getChildCount(), 1);

    auto* filter = dynamic_cast<FilterOp*>(projection->getChild(0).get());
    ASSERT_NE(filter, nullptr);

    // The predicate should be a CompareExpr
    auto* compareExpr = dynamic_cast<const CompareExpr*>(filter->getPredicate());
    ASSERT_NE(compareExpr, nullptr);
    ASSERT_EQ(compareExpr->getOp(), CompareOp::EQUAL);

    // Left operand should be a ColumnRefExpr
    auto* leftCol = dynamic_cast<const ColumnRefExpr*>(compareExpr->getLeft());
    ASSERT_NE(leftCol, nullptr);
    ASSERT_EQ(leftCol->getColumnId().getName(), "id");
    ASSERT_EQ(leftCol->getType(), DataType::getInt64());

    // Right operand should be wrapped in CastExpr (INT32 -> INT64)
    auto* rightCast = dynamic_cast<const CastExpr*>(compareExpr->getRight());
    ASSERT_NE(rightCast, nullptr);
    ASSERT_EQ(rightCast->getType(), DataType::getInt64());

    // Unwrap to get the underlying constant
    auto* rightConst = dynamic_cast<const ConstantExpr*>(rightCast->getExpr());
    ASSERT_NE(rightConst, nullptr);
    ASSERT_EQ(rightConst->getType(), DataType::getInt32());
    ASSERT_EQ(rightConst->getIntValue(), 1);
}

TEST_F(InterpreterTest, SelectAllColumns) {
    Parser parser("SELECT id, name, age FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);

    const auto& columns = projection->getColumns();
    ASSERT_EQ(columns.size(), 3);
    ASSERT_EQ(columns[0].getName(), "id");
    ASSERT_EQ(columns[1].getName(), "name");
    ASSERT_EQ(columns[2].getName(), "age");
}

TEST_F(InterpreterTest, SelectWithoutWhere) {
    Parser parser("SELECT name FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);

    // Without WHERE, projection should have TableScanOp as child
    ASSERT_EQ(projection->getChildCount(), 1);
    auto* tableScan = dynamic_cast<TableScanOp*>(projection->getChild(0).get());
    ASSERT_NE(tableScan, nullptr);
    const auto& scanColumns = tableScan->getColumns();
    ASSERT_FALSE(scanColumns.empty());
    ASSERT_EQ(scanColumns[0].getTableId().getName(), "users");
}

TEST_F(InterpreterTest, TableNotFound) {
    Parser parser("SELECT id FROM nonexistent");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, ColumnNotFound) {
    Parser parser("SELECT invalid_column FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, QualifiedColumnReferences) {
    Parser parser("SELECT users.id, users.name FROM users WHERE users.age > 20");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    // Verify projection columns are qualified correctly
    auto* projection = expectProjectionRoot(*plan);
    expectProjectionColumns(*projection, {
        {"users", "id"},
        {"users", "name"}
    });

    // Verify WHERE clause uses qualified column reference
    auto* filter = expectFilterChild(*projection, 0);
    auto* compareExpr = expectComparePredicate(*filter, CompareOp::GREATER);
    expectColumnRefOperand(*compareExpr, CompareSide::LEFT, "users", "age");
    expectConstantOperand(*compareExpr, CompareSide::RIGHT, 20);
}

// TODO: Add tests for table aliases once parser supports them
// Example queries to test:
// - SELECT u.id FROM users u WHERE u.name = 'test'
// - SELECT u.id, u.name FROM users u WHERE u.age > 20

TEST_F(InterpreterTest, AmbiguousColumnError) {
    // Add a second table with overlapping column names
    catalog_->addTable("orders", {
        {"id", DataType::getInt32()},
        {"user_id", DataType::getInt32()},
        {"amount", DataType::getDouble()}
    });

    // TODO: Parser doesn't support multiple tables yet
    // Once parser supports multiple tables, replace this with:
    // Parser parser("SELECT id FROM users, orders");

    // Manually construct AST with multiple tables
    auto selectFrom = std::make_unique<ast::SelectFrom>();
    selectFrom->columns.emplace_back("id");

    selectFrom->tables.emplace_back(ast::Table("users"));
    selectFrom->tables.emplace_back(ast::Table("orders"));

    ast::QueryAST ast(selectFrom.release());

    // Should throw an exception due to ambiguous column
    EXPECT_THROW({
        auto plan = interpreter_->interpret(ast);
    }, std::runtime_error);
}

TEST_F(InterpreterTest, AmbiguousColumnResolvedWithQualified) {
    // Add a second table with overlapping column names
    catalog_->addTable("orders", {
        {"id", DataType::getInt32()},
        {"user_id", DataType::getInt32()},
        {"amount", DataType::getDouble()}
    });

    // TODO: Interpreter doesn't support multiple tables yet
    // Once multiple tables are supported, this should succeed
    // Parser parser("SELECT users.id, orders.id FROM users, orders");

    // Manually construct AST with qualified references (parser supports qualified, but not multiple tables)
    auto selectFrom = std::make_unique<ast::SelectFrom>();
    selectFrom->columns.emplace_back("users", "id", "");
    selectFrom->columns.emplace_back("orders", "id", "");

    selectFrom->tables.emplace_back(ast::Table("users"));
    selectFrom->tables.emplace_back(ast::Table("orders"));

    ast::QueryAST ast(selectFrom.release());

    // TODO: Implement this, currently it fails
}

TEST_F(InterpreterTest, SelectStarNoProjection) {
    Parser parser("SELECT * FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    // Should just be a table scan of everything
    auto* tableScan = dynamic_cast<TableScanOp*>(root);
    ASSERT_NE(tableScan, nullptr);
    const auto& scanColumns = tableScan->getColumns();
    ASSERT_EQ(scanColumns.size(), 3); // id, name, age
    ASSERT_EQ(scanColumns[0].getTableId().getName(), "users");
}

TEST_F(InterpreterTest, SelectStarWithWhere) {
    Parser parser("SELECT * FROM users WHERE id = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";

    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);

    // Should have FilterOp -> TableScanOp
    auto* filter = dynamic_cast<FilterOp*>(root);
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(filter->getChildCount(), 1);

    auto* tableScan = dynamic_cast<TableScanOp*>(filter->getChild(0).get());
    ASSERT_NE(tableScan, nullptr);
    const auto& scanColumns = tableScan->getColumns();
    ASSERT_EQ(scanColumns.size(), 3);
}

TEST_F(InterpreterTest, QualifiedColumnNotFound) {
    Parser parser("SELECT users.nonexistent FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, QualifiedColumnInvalidTable) {
    Parser parser("SELECT nonexistent.id FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception - table qualifier not found
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, ColumnNotFoundInWhere) {
    Parser parser("SELECT id FROM users WHERE nonexistent = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, QualifiedColumnNotFoundInWhere) {
    Parser parser("SELECT id FROM users WHERE users.nonexistent = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, InvalidTableQualifierInWhere) {
    Parser parser("SELECT id FROM users WHERE nonexistent.id = 1");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query. Error: " << result.error();

    // Should throw an exception - invalid table qualifier
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}
