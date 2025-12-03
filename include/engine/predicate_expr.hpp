#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include "common/types.hpp"
#include "engine/physical_operator.hpp"
#include "engine/predicate_result.hpp"

namespace toydb {

// Forward declaration
class RowVectorBuffer;

class PredicateExpr {
public:
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

public:
    explicit ColumnRefExpr(const ColumnId& columnId) : columnId_(columnId) {}

    const ColumnId& getColumnId() const noexcept {
        return columnId_;
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        // For column reference, we evaluate it as a boolean column
        // This is mainly used in comparisons, not as a standalone predicate
        int64_t colIdx = buffer.getColumnIndex(columnId_);
        const ColumnBuffer& col = buffer.getColumn(colIdx);
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
        int64_t colIdx = buffer.getColumnIndex(columnId_);
        const ColumnBuffer& col = buffer.getColumn(colIdx);

        // Check for null
        if (col.nullBitmap && (col.nullBitmap[rowIndex / 8] & (1 << (rowIndex % 8))) == 0) {
            return PredicateValue::NULL_VALUE;
        }

        // For now, treat non-null column values as true
        // This will be used in comparisons, not standalone
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
        // Constants don't depend on columns, but we need to know the row count
        int64_t rowCount = buffer.getRowCount();
        PredicateResultVector result(rowCount);

        // Constants are always the same value
        // This will be used in comparisons, not standalone
        for (int64_t i = 0; i < rowCount; ++i) {
            result.set(i, isNull() ? PredicateValue::NULL_VALUE : PredicateValue::TRUE);
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
    CmpOp op_;
    std::unique_ptr<PredicateExpr> left_;
    std::unique_ptr<PredicateExpr> right_;

    template<typename T>
    PredicateValue compareValues(T left, T right, bool leftNull, bool rightNull) const {
        if (leftNull || rightNull) {
            return PredicateValue::NULL_VALUE;
        }

        bool result = false;
        switch (op_) {
            case CmpOp::EQUAL:
                result = (left == right);
                break;
            case CmpOp::NOT_EQUAL:
                result = (left != right);
                break;
            case CmpOp::GREATER:
                result = (left > right);
                break;
            case CmpOp::LESS:
                result = (left < right);
                break;
            case CmpOp::GREATER_EQUAL:
                result = (left >= right);
                break;
            case CmpOp::LESS_EQUAL:
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
        ColumnRefExpr* colRef = dynamic_cast<ColumnRefExpr*>(expr);
        ConstantExpr* constant = dynamic_cast<ConstantExpr*>(expr);

        if (colRef) {
            int64_t colIdx = buffer.getColumnIndex(colRef->getColumnId());
            const ColumnBuffer& col = buffer.getColumn(colIdx);

            // Check null bitmap
            if (col.nullBitmap && (col.nullBitmap[rowIndex / 8] & (1 << (rowIndex % 8))) == 0) {
                isNull = true;
                return false;
            }

            // Extract value based on type
            if constexpr (std::is_same_v<T, int64_t>) {
                if (col.type == DataType::getInt64()) {
                    int64_t* data = static_cast<int64_t*>(col.data);
                    value = data[rowIndex];
                    isNull = false;
                    return true;
                }
            } else if constexpr (std::is_same_v<T, double>) {
                if (col.type == DataType::getDouble()) {
                    double* data = static_cast<double*>(col.data);
                    value = data[rowIndex];
                    isNull = false;
                    return true;
                }
            }
            isNull = true;
            return false;
        } else if (constant) {
            if (constant->isNull()) {
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

        isNull = true;
        return false;
    }

    PredicateValue evaluateComparison(
            const RowVectorBuffer& buffer,
            int64_t rowIndex) const {

        // Try to extract values as int64_t first
        int64_t leftInt = 0, rightInt = 0;
        bool leftNull = false, rightNull = false;
        bool leftOk = extractValue(left_.get(), buffer, rowIndex, leftInt, leftNull);
        bool rightOk = extractValue(right_.get(), buffer, rowIndex, rightInt, rightNull);

        if (leftOk && rightOk && !leftNull && !rightNull) {
            return compareValues(leftInt, rightInt, false, false);
        }

        // Try double
        double leftDouble = 0.0, rightDouble = 0.0;
        leftOk = extractValue(left_.get(), buffer, rowIndex, leftDouble, leftNull);
        rightOk = extractValue(right_.get(), buffer, rowIndex, rightDouble, rightNull);

        if (leftOk && rightOk && !leftNull && !rightNull) {
            return compareValues(leftDouble, rightDouble, false, false);
        }

        // If either is null, return NULL
        if (leftNull || rightNull) {
            return PredicateValue::NULL_VALUE;
        }

        // Type mismatch or unsupported type
        return PredicateValue::NULL_VALUE;
    }

public:
    CompareExpr(CmpOp op, std::unique_ptr<PredicateExpr> left, std::unique_ptr<PredicateExpr> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    CmpOp getOp() const noexcept {
        return op_;
    }

    const PredicateExpr* getLeft() const {
        return left_.get();
    }

    const PredicateExpr* getRight() const {
        return right_.get();
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
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
};

/**
 * @brief Logical expression (AND, OR)
 */
class LogicalExpr : public PredicateExpr {
private:
    CmpOp op_;  // AND or OR
    std::unique_ptr<PredicateExpr> left_;
    std::unique_ptr<PredicateExpr> right_;

public:
    LogicalExpr(CmpOp op, std::unique_ptr<PredicateExpr> left, std::unique_ptr<PredicateExpr> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {
        // Ensure op is AND or OR
    }

    CmpOp getOp() const noexcept {
        return op_;
    }

    const PredicateExpr* getLeft() const {
        return left_.get();
    }

    const PredicateExpr* getRight() const {
        return right_.get();
    }

    PredicateResultVector evaluate(const RowVectorBuffer& buffer) const override {
        PredicateResultVector leftResult = left_->evaluate(buffer);
        PredicateResultVector rightResult = right_->evaluate(buffer);

        if (op_ == CmpOp::AND) {
            leftResult.combineAnd(rightResult);
            return leftResult;
        } else if (op_ == CmpOp::OR) {
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

        if (op_ == CmpOp::AND) {
            // Three-valued AND logic
            if (leftVal == PredicateValue::FALSE || rightVal == PredicateValue::FALSE) {
                return PredicateValue::FALSE;
            }
            if (leftVal == PredicateValue::NULL_VALUE || rightVal == PredicateValue::NULL_VALUE) {
                return PredicateValue::NULL_VALUE;
            }
            return PredicateValue::TRUE;
        } else if (op_ == CmpOp::OR) {
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
