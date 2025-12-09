#include "planner/interpreter.hpp"
#include "parser/parser.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_map>

using namespace toydb;
using namespace toydb::parser;
using namespace toydb::ast;

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
        meta.id = tableName;
        meta.format = StorageFormat::PARQUET;
        
        std::unordered_map<std::string, ColumnId> tableColumns;
        for (const auto& [colName, colType] : columns) {
            ColumnId colId(nextColumnId_++, colName);
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
        if (tableName.empty()) {
            // Try to find in all tables (for now, just check first table)
            // In a real implementation, we'd need to handle ambiguity
            if (!tables_.empty()) {
                auto& firstTable = tables_.begin()->second;
                auto it = columnMap_.find(firstTable.name);
                if (it != columnMap_.end()) {
                    auto colIt = it->second.find(columnName);
                    if (colIt != it->second.end()) {
                        return colIt->second;
                    }
                }
            }
        } else {
            auto it = columnMap_.find(tableName);
            if (it != columnMap_.end()) {
                auto colIt = it->second.find(columnName);
                if (colIt != it->second.end()) {
                    return colIt->second;
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
            {"id", DataType::getInt64()},
            {"name", DataType::getString()},
            {"age", DataType::getInt64()}
        });
    }

    void TearDown() override {}
};

TEST_F(InterpreterTest, SimpleSelect) {
    Parser parser("SELECT id, name FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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
    ASSERT_EQ(rightConst->getType(), DataType::getInt64());
    ASSERT_EQ(rightConst->getIntValue(), 1);
}

TEST_F(InterpreterTest, SelectWithWhereAnd) {
    Parser parser("SELECT id FROM users WHERE id = 1 AND age > 20");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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

TEST_F(InterpreterTest, SelectAllColumns) {
    Parser parser("SELECT id, name, age FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
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
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
    auto plan = interpreter_->interpret(*result.value());
    ASSERT_TRUE(plan.has_value()) << "Failed to interpret query";
    
    auto* root = plan->getRoot();
    ASSERT_NE(root, nullptr);
    
    auto* projection = dynamic_cast<ProjectionOp*>(root);
    ASSERT_NE(projection, nullptr);
    
    // Without WHERE, there should be no filter child
    ASSERT_EQ(projection->getChildCount(), 0);
}

TEST_F(InterpreterTest, TableNotFound) {
    Parser parser("SELECT id FROM nonexistent");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}

TEST_F(InterpreterTest, ColumnNotFound) {
    Parser parser("SELECT invalid_column FROM users");
    auto result = parser.parseQuery();
    ASSERT_TRUE(result.has_value()) << "Failed to parse query";
    
    // Should throw an exception
    EXPECT_THROW({
        auto plan = interpreter_->interpret(*result.value());
    }, std::runtime_error);
}
