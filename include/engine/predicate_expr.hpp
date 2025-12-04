#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "common/types.hpp"
#include "engine/physical_operator.hpp"
#include "engine/predicate_result.hpp"

namespace toydb {

// Forward declaration
class RowVectorBuffer;

class PredicateExpr {
protected:
    // Maps the columnId to the index used in the ColumnRefExpressions.
    // When the predicate is evaluated, the input rows are expected to be in the locations specified by this map.
    std::unordered_map<ColumnId, int32_t, ColumnIdHash> columnIndexMap_;

public:
    std::unordered_map<ColumnId, int32_t, ColumnIdHash> getColumnIndexMap() const {
        return columnIndexMap_;
    }

    /**
     * @brief Initialize column indices by numbering ColumnRefExpr nodes in post-order traversal
     * @param nextIndex Pointer to index counter (if nullptr, creates local counter)
     */
    virtual void initializeIndexMap(int32_t* nextIndex = nullptr) = 0;

    /**
     * @brief Debug assertions to check that the index map matches the location of the columns in the buffer
     */
    void assertIndexMapValid([[maybe_unused]] const RowVectorBuffer& buffer) const noexcept {
        tdb_assert(static_cast<int64_t>(columnIndexMap_.size()) == buffer.getColumnCount(),
                   "Buffer column count mismatch: expected " + std::to_string(columnIndexMap_.size()) +
                   " columns, got " + std::to_string(buffer.getColumnCount()));

        for (int64_t i = 0; i < buffer.getColumnCount(); ++i) {
            const ColumnBuffer& col = buffer.getColumn(i);
            [[maybe_unused]] auto it = columnIndexMap_.find(col.columnId_);
            tdb_assert(it != columnIndexMap_.end(),
                       "Column {} in buffer is not referenced by predicate", col.columnId.getName());
            tdb_assert(static_cast<int64_t>(it->second) == i,
                       "Column {} at buffer index {} but expected at index {}", col.columnId.getName(), i, it->second);
        }
    }

    virtual ~PredicateExpr() = default;

    /**
     * @brief Evaluate the predicate over a row buffer
     *
     * @param buffer Row buffer to evaluate against
     * @return PredicateResult with evaluation results for each row
     */
    virtual PredicateResultVector evaluate(const RowVectorBuffer& buffer) const = 0;

    /**
     * @brief Evaluate predicate for a single row (tuple-by-tuple)
     *
     * @param buffer Row buffer
     * @param rowIndex Index of the row to evaluate
     * @return PredicateValue for this row
     */
    virtual PredicateValue evaluateRow(
        const RowVectorBuffer& buffer,
        int64_t rowIndex) const = 0;
};

/**
 * @brief Reference to a column by ID
 */
class ColumnRefExpr : public PredicateExpr {
private:
    ColumnId columnId_;

protected:
    int32_t columnIndex_ = -1;

public:
    explicit ColumnRefExpr(const ColumnId& columnId) : columnId_(columnId), columnIndex_(-1) {}

    const ColumnId& getColumnId() const noexcept {
        return columnId_;
    }

    int32_t getColumnIndex() const noexcept {
        return columnIndex_;
    }



    void initializeIndexMap(int32_t* nextIndex = nullptr) override {
        (void)nextIndex;
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        tdb_assert(columnIndex_ >= 0, "Column index not initialized. Call initialize() first.");
        const ColumnBuffer& col = buffer.getColumn(columnIndex_);
        PredicateResultVector result(col.count);

        // Evaluate each row
        for (int64_t i = 0; i < col.count; ++i) {
            result.set(i, evaluateRow(buffer, i));
        }

        return result;
    }

    PredicateValue evaluateRow(
            const RowVectorBuffer& buffer,
            int64_t rowIndex) const override {
        tdb_assert(columnIndex_ >= 0, "Column index not initialized. Call initialize() first.");
        const ColumnBuffer& col = buffer.getColumn(columnIndex_);

        // Check for null
        if (col.nullBitmap && (col.nullBitmap[rowIndex / 8] & (1 << (rowIndex % 8))) == 0) {
            return PredicateValue::NULL_VALUE;
        }
        return PredicateValue::TRUE;
    }
};

/**
 * @brief Constant/literal value
 */
class ConstantExpr : public PredicateExpr {
    DataType type_;
    union {
        int64_t intValue_;
        double doubleValue_;
        bool boolValue_;
    };
    std::string stringValue_;

    void initializeIndexMap(int32_t* nextIndex = nullptr) override {
        // Constants don't reference any columns, so map stays empty
        (void)nextIndex;
        columnIndexMap_.clear();
    }

public:
    explicit ConstantExpr(DataType type, int64_t value) : type_(type), intValue_(value) {}
    explicit ConstantExpr(DataType type, double value) : type_(type), doubleValue_(value) {}
    explicit ConstantExpr(DataType type, const std::string& value) : type_(type), stringValue_(value) {}
    explicit ConstantExpr(DataType type, bool value) : type_(type), boolValue_(value) {}
    ConstantExpr() : type_(DataType::NULL_CONST) {}
    explicit ConstantExpr(DataType type) : type_(type) {}

    DataType getType() const noexcept {
        return type_;
    }

    int64_t getIntValue() const {
        return intValue_;
    }

    double getDoubleValue() const {
        return doubleValue_;
    }

    const std::string& getStringValue() const {
        return stringValue_;
    }

    bool getBoolValue() const {
        return boolValue_;
    }

    bool isNull() const noexcept {
        return type_ == DataType::getNullConst();
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        int64_t rowCount = buffer.getRowCount();
        PredicateResultVector result(rowCount);

        if (isNull()) [[unlikely]] {
            for (int64_t i = 0; i < rowCount; ++i) {
                result.setAll(PredicateValue::NULL_VALUE);
            }
        } else {
            for (int64_t i = 0; i < rowCount; ++i) {
                result.setAll(PredicateValue::TRUE);
            }
        }

        return result;
    }

    PredicateValue evaluateRow(
        [[maybe_unused]] const RowVectorBuffer& buffer,
        [[maybe_unused]] int64_t rowIndex) const override {
        return isNull() ? PredicateValue::NULL_VALUE : PredicateValue::TRUE;
    }
};

/**
 * @brief Comparison expression (>, <, =, !=, >=, <=)
 */
class CompareExpr : public PredicateExpr {
private:
    CompareOp op_;
    DataType type_;
    std::unique_ptr<PredicateExpr> left_;
    std::unique_ptr<PredicateExpr> right_;

    template<typename T>
    PredicateValue compareValues(T left, T right, bool leftNull, bool rightNull) const {
        if (leftNull || rightNull) {
            return PredicateValue::NULL_VALUE;
        }

        bool result = false;
        switch (op_) {
            case CompareOp::EQUAL:
                result = (left == right);
                break;
            case CompareOp::NOT_EQUAL:
                result = (left != right);
                break;
            case CompareOp::GREATER:
                result = (left > right);
                break;
            case CompareOp::LESS:
                result = (left < right);
                break;
            case CompareOp::GREATER_EQUAL:
                result = (left >= right);
                break;
            case CompareOp::LESS_EQUAL:
                result = (left <= right);
                break;
            default:
                return PredicateValue::NULL_VALUE;
        }

        return result ? PredicateValue::TRUE : PredicateValue::FALSE;
    }

    template<typename T>
    T getValue(const ColumnBuffer& col, int64_t rowIndex, bool& isNull) const {
        isNull = false;
        if (col.nullBitmap && (col.nullBitmap[rowIndex / 8] & (1 << (rowIndex % 8))) == 0) {
            isNull = true;
            return T{};
        }

        T* data = static_cast<T*>(col.data);
        return data[rowIndex];
    }

    template<typename T>
    bool extractValue(PredicateExpr* expr, const RowVectorBuffer& buffer,
                      int64_t rowIndex, T& value, bool& isNull) const {
        if (auto* colRef = dynamic_cast<ColumnRefExpr*>(expr)) {
            // ColumnRefExpr stores its index directly
            int32_t colIdx = colRef->getColumnIndex();
            tdb_assert(colIdx >= 0, "Column index not initialized. Call initialize() first.");
            const ColumnBuffer& col = buffer.getColumn(colIdx);

            // Check null bitmap
            if (col.nullBitmap && (col.nullBitmap[rowIndex / 8] & (1 << (rowIndex % 8))) == 0) {
                isNull = true;
                return false;
            }

            // Extract value based on type
            if constexpr (std::is_same_v<T, int64_t>) {
                if (col.type_ == DataType::getInt64()) {
                    int64_t* data = static_cast<int64_t*>(col.data);
                    value = data[rowIndex];
                    isNull = false;
                    return true;
                }
            } else if constexpr (std::is_same_v<T, double>) {
                if (col.type_ == DataType::getDouble()) {
                    double* data = static_cast<double*>(col.data);
                    value = data[rowIndex];
                    isNull = false;
                    return true;
                }
            }
            isNull = true;
            return false;
        } else if (auto* constant = dynamic_cast<ConstantExpr*>(expr)) {
            if (constant->isNull()) [[unlikely]] {
                isNull = true;
                return false;
            }

            if constexpr (std::is_same_v<T, int64_t>) {
                if (constant->getType() == DataType::getInt64()) {
                    value = constant->getIntValue();
                    isNull = false;
                    return true;
                }
            } else if constexpr (std::is_same_v<T, double>) {
                if (constant->getType() == DataType::getDouble()) {
                    value = constant->getDoubleValue();
                    isNull = false;
                    return true;
                }
            }
            isNull = true;
            return false;
        }

        tdb_unreachable("Unsupported expression type");
    }

    PredicateValue evaluateComparison(
            const RowVectorBuffer& buffer,
            int64_t rowIndex) const {
        // Use the stored type to directly extract and compare values
        if (type_ == DataType::getInt64()) {
            int64_t leftInt = 0, rightInt = 0;
            bool leftNull = false, rightNull = false;
            bool leftOk = extractValue(left_.get(), buffer, rowIndex, leftInt, leftNull);
            bool rightOk = extractValue(right_.get(), buffer, rowIndex, rightInt, rightNull);

            if (leftOk && rightOk) {
                return compareValues(leftInt, rightInt, leftNull, rightNull);
            }
        } else if (type_ == DataType::getDouble()) {
            double leftDouble = 0.0, rightDouble = 0.0;
            bool leftNull = false, rightNull = false;
            bool leftOk = extractValue(left_.get(), buffer, rowIndex, leftDouble, leftNull);
            bool rightOk = extractValue(right_.get(), buffer, rowIndex, rightDouble, rightNull);

            if (leftOk && rightOk) {
                return compareValues(leftDouble, rightDouble, leftNull, rightNull);
            }
        }

        // If either is null or type mismatch, return NULL
        return PredicateValue::NULL_VALUE;
    }

public:
    CompareExpr(CompareOp op, DataType type, std::unique_ptr<PredicateExpr> left, std::unique_ptr<PredicateExpr> right)
        : op_(op), type_(type), left_(std::move(left)), right_(std::move(right)) {}

    CompareOp getOp() const noexcept {
        return op_;
    }

    DataType getType() const noexcept {
        return type_;
    }

    const PredicateExpr* getLeft() const {
        return left_.get();
    }

    const PredicateExpr* getRight() const {
        return right_.get();
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        assertIndexMapValid(buffer);
        
        int64_t rowCount = buffer.getRowCount();
        PredicateResultVector result(rowCount);

        // Evaluate comparison for each row
        for (int64_t i = 0; i < rowCount; ++i) {
            result.set(i, evaluateComparison(buffer, i));
        }

        return result;
    }

    PredicateValue evaluateRow(
            const RowVectorBuffer& buffer,
            int64_t rowIndex) const override {
        return evaluateComparison(buffer, rowIndex);
    }

    void initializeIndexMap(int32_t* nextIndex = nullptr) override {
        int32_t localIndex = 0;
        if (nextIndex == nullptr) {
            nextIndex = &localIndex;
        }

        left_->initializeIndexMap(nextIndex);
        right_->initializeIndexMap(nextIndex);

        columnIndexMap_.clear();
        auto leftMap = left_->getColumnIndexMap();
        auto rightMap = right_->getColumnIndexMap();
        columnIndexMap_.insert(leftMap.begin(), leftMap.end());
        for (const auto& [colId, idx] : rightMap) {
            if (columnIndexMap_.find(colId) == columnIndexMap_.end()) {
                columnIndexMap_[colId] = idx;
            }
        }
    }
};

/**
 * @brief Logical expression (AND, OR)
 */
class LogicalExpr : public PredicateExpr {
private:
    CompareOp op_;  // AND or OR
    std::unique_ptr<PredicateExpr> left_;
    std::unique_ptr<PredicateExpr> right_;

public:
    void initializeIndexMap(int32_t* nextIndex = nullptr) override {
        int32_t localIndex = 0;
        if (nextIndex == nullptr) {
            nextIndex = &localIndex;
        }

        left_->initializeIndexMap(nextIndex);
        right_->initializeIndexMap(nextIndex);

        columnIndexMap_.clear();
        auto leftMap = left_->getColumnIndexMap();
        auto rightMap = right_->getColumnIndexMap();
        columnIndexMap_.insert(leftMap.begin(), leftMap.end());
        for (const auto& [colId, idx] : rightMap) {
            if (columnIndexMap_.find(colId) == columnIndexMap_.end()) {
                columnIndexMap_[colId] = idx;
            }
        }
    }

public:
    LogicalExpr(CompareOp op, std::unique_ptr<PredicateExpr> left, std::unique_ptr<PredicateExpr> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    CompareOp getOp() const noexcept {
        return op_;
    }

    const PredicateExpr* getLeft() const {
        return left_.get();
    }

    const PredicateExpr* getRight() const {
        return right_.get();
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        assertIndexMapValid(buffer);
        
        PredicateResultVector leftResult = left_->evaluate(buffer);
        PredicateResultVector rightResult = right_->evaluate(buffer);

        if (op_ == CompareOp::AND) {
            leftResult.combineAnd(rightResult);
            return leftResult;
        } else if (op_ == CompareOp::OR) {
            leftResult.combineOr(rightResult);
            return leftResult;
        }

        return leftResult;
    }

    PredicateValue evaluateRow(
        const RowVectorBuffer& buffer,
        int64_t rowIndex) const override {
        PredicateValue leftVal = left_->evaluateRow(buffer, rowIndex);
        PredicateValue rightVal = right_->evaluateRow(buffer, rowIndex);

        if (op_ == CompareOp::AND) {
            // Three-valued AND logic
            if (leftVal == PredicateValue::FALSE || rightVal == PredicateValue::FALSE) {
                return PredicateValue::FALSE;
            }
            if (leftVal == PredicateValue::NULL_VALUE || rightVal == PredicateValue::NULL_VALUE) {
                return PredicateValue::NULL_VALUE;
            }
            return PredicateValue::TRUE;
        } else if (op_ == CompareOp::OR) {
            // Three-valued OR logic
            if (leftVal == PredicateValue::TRUE || rightVal == PredicateValue::TRUE) {
                return PredicateValue::TRUE;
            }
            if (leftVal == PredicateValue::NULL_VALUE || rightVal == PredicateValue::NULL_VALUE) {
                return PredicateValue::NULL_VALUE;
            }
            return PredicateValue::FALSE;
        }

        return PredicateValue::FALSE;
    }
};

} // namespace toydb
