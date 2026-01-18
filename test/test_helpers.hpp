#pragma once

#include "engine/physical_operator.hpp"
#include "planner/logical_operator.hpp"
#include "engine/predicate_expr.hpp"
#include "common/errors.hpp"
#include "common/types.hpp"
#include "parser/query_ast.hpp"
#include <vector>
#include <random>
#include <string>
#include <memory>
#include <cassert>
#include <gtest/gtest.h>

namespace toydb::test {

/**
 * @brief Recursively compare two AST nodes for equality
 */
bool compareASTNodes(const toydb::ast::ASTNode* expected,
                     const toydb::ast::ASTNode* actual,
                     const std::string& path = "root");

/**
 * @brief Compare two QueryAST objects for equality
 */
bool compareQueryAST(const toydb::ast::QueryAST& expected,
                     const toydb::ast::QueryAST& actual);

/**
 * @brief RAII helper for managing column buffer storage in tests
 */
class ColumnBufferStorage {
private:
    std::vector<std::vector<int64_t>> int64Storage_;
    std::vector<std::vector<double>> doubleStorage_;
    std::vector<std::vector<std::string>> stringStorage_;

public:
    ColumnBufferStorage() = default;
    ~ColumnBufferStorage() = default;

    // Non-copyable
    ColumnBufferStorage(const ColumnBufferStorage&) = delete;
    ColumnBufferStorage& operator=(const ColumnBufferStorage&) = delete;

    // Movable
    ColumnBufferStorage(ColumnBufferStorage&&) = default;
    ColumnBufferStorage& operator=(ColumnBufferStorage&&) = default;

    ColumnBuffer createIntColumn(const std::vector<int64_t>& values, uint64_t colId, const std::string& colName) {
        int64Storage_.push_back(values);

        ColumnId columnId(colId, colName);
        void* data = int64Storage_.back().data();
        int64_t capacity = static_cast<int64_t>(values.size());
        ColumnBuffer col(columnId, DataType::getInt64(), data, capacity, nullptr);
        col.count = static_cast<int64_t>(values.size());

        return col;
    }

    ColumnBuffer createDoubleColumn(const std::vector<double>& values, uint64_t colId, const std::string& colName) {
        doubleStorage_.push_back(values);

        ColumnId columnId(colId, colName);
        void* data = doubleStorage_.back().data();
        int64_t capacity = static_cast<int64_t>(values.size());
        ColumnBuffer col(columnId, DataType::getDouble(), data, capacity, nullptr);
        col.count = static_cast<int64_t>(values.size());

        return col;
    }

    ColumnBuffer createStringColumn(const std::vector<std::string>& values, uint64_t colId, const std::string& colName) {
        stringStorage_.push_back(values);

        // TODO: Implement proper string column buffer creation
        // This requires fixed-size storage per string element
        throw NotYetImplementedError("String column creation");
    }
};

/**
 * @brief Helper functions for creating data vectors
 */
namespace data_helpers {

// Generate integer sequence vector: [start, start+1, start+2, ..., start+count-1]
inline std::vector<int64_t> intSequence(int64_t start, int64_t count) {
    std::vector<int64_t> result;
    result.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        result.push_back(start + i);
    }
    return result;
}

// Generate random integers vector in range [min, max)
inline std::vector<int64_t> randomInts(int64_t min, int64_t max, int64_t count, int seed = 42) {
    std::vector<int64_t> result;
    result.reserve(count);
    std::mt19937 gen(static_cast<unsigned>(seed));
    std::uniform_int_distribution<int64_t> dist(min, max - 1);
    for (int64_t i = 0; i < count; ++i) {
        result.push_back(dist(gen));
    }
    return result;
}

// Generate double sequence vector: [start, start+step, start+2*step, ..., start+(count-1)*step]
inline std::vector<double> doubleSequence(double start, int64_t count, double step = 1.0) {
    std::vector<double> result;
    result.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        result.push_back(start + (step * static_cast<double>(i)));
    }
    return result;
}

// Generate random doubles vector in range [min, max)
inline std::vector<double> randomDoubles(double min, double max, int64_t count, int seed = 42) {
    std::vector<double> result;
    result.reserve(count);
    std::mt19937 gen(static_cast<unsigned>(seed));
    std::uniform_real_distribution<double> dist(min, max);
    for (int64_t i = 0; i < count; ++i) {
        result.push_back(dist(gen));
    }
    return result;
}

// Generate string sequence vector: ["prefix0", "prefix1", "prefix2", ..., "prefix(count-1)"]
inline std::vector<std::string> stringSequence(const std::string& prefix, int64_t count) {
    std::vector<std::string> result;
    result.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        result.push_back(prefix + std::to_string(i));
    }
    return result;
}

} // namespace data_helpers

/**
 * @brief Plan validation helpers for interpreter tests
 * These helpers provide clear, descriptive assertions for validating query plans
 */
namespace plan_validation {

/**
 * @brief Enum for specifying which operand of a comparison to validate
 */
enum class CompareSide {
    LEFT,
    RIGHT,
};

/**
 * @brief Expect the root of a plan to be a ProjectionOp
 * @param plan The query plan to validate
 * @return Pointer to the ProjectionOp (never null, assertion fails if wrong type)
 */
const toydb::ProjectionOp* expectProjectionRoot(const toydb::LogicalQueryPlan& plan);

/**
 * @brief Validate projection columns match expected table.column pairs
 * @param projection The ProjectionOp to validate
 * @param expectedColumns Vector of (table_name, column_name) pairs
 */
void expectProjectionColumns(const toydb::ProjectionOp& projection, 
                            const std::vector<std::pair<std::string, std::string>>& expectedColumns);

/**
 * @brief Expect a child operator at the given index to be a FilterOp
 * @param parent The parent operator
 * @param childIndex Index of the child to validate
 * @return Pointer to the FilterOp (never null, assertion fails if wrong type or missing)
 */
const toydb::FilterOp* expectFilterChild(const toydb::LogicalOperator& parent, size_t childIndex);

/**
 * @brief Expect a predicate to be a CompareExpr with the given operator
 * @param filter The FilterOp containing the predicate
 * @param expectedOp The expected comparison operator
 * @return Pointer to the CompareExpr (never null, assertion fails if wrong type or operator)
 */
const toydb::CompareExpr* expectComparePredicate(const toydb::FilterOp& filter, toydb::CompareOp expectedOp);

/**
 * @brief Expect an operand of a CompareExpr to be a ColumnRefExpr
 * @param compareExpr The CompareExpr to validate
 * @param side Which operand (LEFT or RIGHT)
 * @param expectedTable Expected table name (empty string if unqualified)
 * @param expectedColumn Expected column name
 */
void expectColumnRefOperand(const toydb::CompareExpr& compareExpr, CompareSide side,
                           const std::string& expectedTable, const std::string& expectedColumn);

/**
 * @brief Expect an operand of a CompareExpr to be a ConstantExpr with an integer value
 * @param compareExpr The CompareExpr to validate
 * @param side Which operand (LEFT or RIGHT)
 * @param expectedValue Expected integer value
 */
void expectConstantOperand(const toydb::CompareExpr& compareExpr, CompareSide side, int64_t expectedValue);

} // namespace plan_validation

/**
 * @brief Builder for creating MockOperator instances
 */
class MockOperatorBuilder {
private:
    ColumnBufferStorage* storage_;
    std::vector<std::vector<int64_t>> int64Columns_;
    std::vector<std::vector<double>> doubleColumns_;
    std::vector<uint64_t> columnIds_;
    std::vector<std::string> columnNames_;
    std::vector<int64_t> batchSizes_;

    void validateColumnCounts() const {
        if (!int64Columns_.empty() && !doubleColumns_.empty()) {
            // Check all int64 columns have same size
            if (int64Columns_.size() > 1) {
                size_t expectedSize = int64Columns_[0].size();
                for (size_t i = 1; i < int64Columns_.size(); ++i) {
                    assert(int64Columns_[i].size() == expectedSize &&
                           "All INT64 columns must have the same row count");
                }
            }
            // Check all double columns have same size
            if (doubleColumns_.size() > 1) {
                size_t expectedSize = doubleColumns_[0].size();
                for (size_t i = 1; i < doubleColumns_.size(); ++i) {
                    assert(doubleColumns_[i].size() == expectedSize &&
                           "All DOUBLE columns must have the same row count");
                }
            }
            // Check int64 and double columns have same size
            if (!int64Columns_.empty() && !doubleColumns_.empty()) {
                assert(int64Columns_[0].size() == doubleColumns_[0].size() &&
                       "All columns must have the same row count");
            }
        }
    }

public:
    explicit MockOperatorBuilder(ColumnBufferStorage* storage)
        : storage_(storage) {}

    // Add an INT64 column with pre-defined values
    MockOperatorBuilder& addInt64Column(uint64_t colId, const std::string& colName,
                                        const std::vector<int64_t>& values) {
        int64Columns_.push_back(values);
        columnIds_.push_back(colId);
        columnNames_.push_back(colName);
        validateColumnCounts();
        return *this;
    }

    // Add a DOUBLE column with pre-defined values
    MockOperatorBuilder& addDoubleColumn(uint64_t colId, const std::string& colName,
                                         const std::vector<double>& values) {
        doubleColumns_.push_back(values);
        columnIds_.push_back(colId);
        columnNames_.push_back(colName);
        validateColumnCounts();
        return *this;
    }

    // Set batch sizes: {5, 10, 3} means split sequence into batches of 5, 10, 3 rows
    MockOperatorBuilder& withBatchSizes(const std::vector<int64_t>& sizes) {
        batchSizes_ = sizes;
        return *this;
    }

    // Build the MockOperator
    std::unique_ptr<PhysicalOperator> build();
};

/**
 * @brief Mock operator for testing that returns pre-defined data in batches
 */
class MockOperator : public PhysicalOperator {
private:
    ColumnBufferStorage* storage_;
    std::vector<RowVector> batches_;
    size_t currentBatchIndex_;

public:
    MockOperator(ColumnBufferStorage* storage, std::vector<RowVector> batches)
        : storage_(storage)
        , batches_(std::move(batches))
        , currentBatchIndex_(0) {}

    void initialize() override {
        currentBatchIndex_ = 0;
    }

    int64_t next(RowVector& out) override {
        if (currentBatchIndex_ >= batches_.size()) {
            return 0;
        }

        const RowVector& batch = batches_[currentBatchIndex_];
        for (int64_t i = 0; i < batch.getColumnCount(); ++i) {
            out.addColumn(batch.getColumn(i));
        }
        out.setRowCount(batch.getRowCount());

        currentBatchIndex_++;
        return batch.getRowCount();
    }
};

inline std::unique_ptr<PhysicalOperator> MockOperatorBuilder::build() {
    // Build batches from sequence data
    std::vector<RowVector> batches;

    if (!int64Columns_.empty() || !doubleColumns_.empty()) {
        size_t totalRowCount = 0;

        if (!int64Columns_.empty()) {
            totalRowCount = int64Columns_[0].size();
        } else if (!doubleColumns_.empty()) {
            totalRowCount = doubleColumns_[0].size();
        }

        if (batchSizes_.empty()) {
            // Single batch - return all data at once
            RowVector batch;

            // Add all INT64 columns
            for (size_t i = 0; i < int64Columns_.size(); ++i) {
                ColumnBuffer col = storage_->createIntColumn(int64Columns_[i],
                                                            columnIds_[i],
                                                            columnNames_[i]);
                batch.addColumn(col);
            }

            // Add all DOUBLE columns
            for (size_t i = 0; i < doubleColumns_.size(); ++i) {
                ColumnBuffer col = storage_->createDoubleColumn(doubleColumns_[i],
                                                               columnIds_[int64Columns_.size() + i],
                                                               columnNames_[int64Columns_.size() + i]);
                batch.addColumn(col);
            }

            batch.setRowCount(static_cast<int64_t>(totalRowCount));
            batches.push_back(std::move(batch));
        } else {
            // Split into batches
            size_t rowOffset = 0;
            for (int64_t batchSize : batchSizes_) {
                if (rowOffset >= totalRowCount) {
                    break; // No more data to split
                }

                size_t actualBatchSize = std::min(static_cast<size_t>(batchSize), totalRowCount - rowOffset);

                RowVector batch;

                // Add INT64 columns (slice the vectors)
                for (size_t colIdx = 0; colIdx < int64Columns_.size(); ++colIdx) {
                    std::vector<int64_t> batchData(int64Columns_[colIdx].begin() + rowOffset,
                                                  int64Columns_[colIdx].begin() + rowOffset + actualBatchSize);
                    ColumnBuffer col = storage_->createIntColumn(batchData,
                                                                columnIds_[colIdx],
                                                                columnNames_[colIdx]);
                    batch.addColumn(col);
                }

                // Add DOUBLE columns (slice the vectors)
                for (size_t colIdx = 0; colIdx < doubleColumns_.size(); ++colIdx) {
                    std::vector<double> batchData(doubleColumns_[colIdx].begin() + rowOffset,
                                                 doubleColumns_[colIdx].begin() + rowOffset + actualBatchSize);
                    ColumnBuffer col = storage_->createDoubleColumn(batchData,
                                                                   columnIds_[int64Columns_.size() + colIdx],
                                                                   columnNames_[int64Columns_.size() + colIdx]);
                    batch.addColumn(col);
                }

                batch.setRowCount(static_cast<int64_t>(actualBatchSize));
                batches.push_back(std::move(batch));

                rowOffset += actualBatchSize;
            }
        }
    }

    return std::make_unique<MockOperator>(storage_, std::move(batches));
}
} // namespace toydb::test

