#include "gtest/gtest.h"
#include "engine/predicate_result.hpp"
#include "engine/predicate_expr.hpp"
#include <memory>

using namespace toydb;

class PredicateTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

// Test BitmaskResult basic operations
TEST_F(PredicateTest, BitmaskResultBasic) {
    BitmaskResult result(10);

    EXPECT_EQ(result.size(), 10);
    EXPECT_EQ(result.count(), 0);

    result.setTrue(0);
    result.setTrue(5);
    result.setFalse(1);
    result.setNull(2);

    EXPECT_TRUE(result.isTrue(0));
    EXPECT_TRUE(result.isFalse(1));
    EXPECT_TRUE(result.isNull(2));
    EXPECT_FALSE(result.isTrue(1));
    EXPECT_FALSE(result.isNull(0));

    EXPECT_EQ(result.count(), 2);
}

// Test PredicateResult wrapper
TEST_F(PredicateTest, PredicateResultWrapper) {
    PredicateResultVector result(5);

    result.setTrue(0);
    result.setFalse(1);
    result.setNull(2);

    EXPECT_EQ(result.get(0), PredicateValue::TRUE);
    EXPECT_EQ(result.get(1), PredicateValue::FALSE);
    EXPECT_EQ(result.get(2), PredicateValue::NULL_VALUE);
    EXPECT_EQ(result.count(), 1);
}

// Test ConstantExpr
TEST_F(PredicateTest, ConstantExpr) {
    ConstantExpr intConst(DataType::getInt64(), 42L);
    ConstantExpr doubleConst(DataType::getDouble(), 3.14);
    ConstantExpr boolConst(DataType::getBool(), true);
    ConstantExpr nullConst;

    EXPECT_EQ(intConst.getType(), DataType::getInt64());
    EXPECT_EQ(intConst.getIntValue(), 42);

    EXPECT_EQ(doubleConst.getType(), DataType::getDouble());
    EXPECT_DOUBLE_EQ(doubleConst.getDoubleValue(), 3.14);

    EXPECT_EQ(boolConst.getType(), DataType::getBool());
    EXPECT_TRUE(boolConst.getBoolValue());

    EXPECT_TRUE(nullConst.isNull());

    // Test constructors with explicit type
    ConstantExpr intConstWithType(DataType::getInt64(), 42L);
    ConstantExpr doubleConstWithType(DataType::getDouble(), 3.14);
    ConstantExpr boolConstWithType(DataType::getBool(), true);
    ConstantExpr nullConstWithType(DataType::getNullConst());

    EXPECT_EQ(intConstWithType.getType(), DataType::getInt64());
    EXPECT_EQ(intConstWithType.getIntValue(), 42);

    EXPECT_EQ(doubleConstWithType.getType(), DataType::getDouble());
    EXPECT_DOUBLE_EQ(doubleConstWithType.getDoubleValue(), 3.14);

    EXPECT_EQ(boolConstWithType.getType(), DataType::getBool());
    EXPECT_TRUE(boolConstWithType.getBoolValue());

    EXPECT_TRUE(nullConstWithType.isNull());
}

// Test CompareExpr with constants
TEST_F(PredicateTest, CompareExprConstants) {
    // Create comparison: 5 < 3
    auto left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    auto right = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    CompareExpr compare(CmpOp::LESS, std::move(left), std::move(right));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    // For constants, we can test the row evaluation
    PredicateValue result = compare.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);

    // Test 3 < 5 (should be true)
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare2(CmpOp::LESS, std::move(left), std::move(right));

    result = compare2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Create comparison: 5 > 3
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    CompareExpr compare3(CmpOp::GREATER, std::move(left), std::move(right));

    emptyBuffer.setRowCount(1);

    // For constants, we can test the row evaluation
    result = compare3.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test 3 > 5 (should be false)
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare4(CmpOp::GREATER, std::move(left), std::move(right));

    result = compare4.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);

    // Test equality
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare5(CmpOp::EQUAL, std::move(left), std::move(right));

    result = compare5.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);
}

// Test CompareExpr with NULL
TEST_F(PredicateTest, CompareExprNull) {
    // Test NULL > 5
    auto left = std::make_unique<ConstantExpr>();
    auto right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare(CmpOp::GREATER, std::move(left), std::move(right));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = compare.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);

    // Test NULL = NULL
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>();
    CompareExpr compare2(CmpOp::EQUAL, std::move(left), std::move(right));

    result = compare2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);

    // Test NULL < 5
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare3(CmpOp::LESS, std::move(left), std::move(right));

    result = compare3.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);

    // Test NULL >= 5
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare4(CmpOp::GREATER_EQUAL, std::move(left), std::move(right));

    result = compare4.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);
}

// Test LogicalExpr AND
TEST_F(PredicateTest, LogicalExprAND) {
    auto left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    auto right = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    auto compare1 = std::make_unique<CompareExpr>(CmpOp::GREATER,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 3L));
    auto compare2 = std::make_unique<CompareExpr>(CmpOp::LESS,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));

    LogicalExpr andExpr(CmpOp::AND, std::move(compare1), std::move(compare2));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = andExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test AND with false
    auto compare3 = std::make_unique<CompareExpr>(CmpOp::GREATER,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));
    auto compare4 = std::make_unique<CompareExpr>(CmpOp::LESS,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // false

    LogicalExpr andExpr2(CmpOp::AND, std::move(compare3), std::move(compare4));
    result = andExpr2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);
}

// Test LogicalExpr OR
TEST_F(PredicateTest, LogicalExprOR) {
    auto compare1 = std::make_unique<CompareExpr>(CmpOp::GREATER,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // false
    auto compare2 = std::make_unique<CompareExpr>(CmpOp::LESS,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // true

    LogicalExpr orExpr(CmpOp::OR, std::move(compare1), std::move(compare2));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = orExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test OR with both false
    auto compare3 = std::make_unique<CompareExpr>(CmpOp::GREATER,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));
    auto compare4 = std::make_unique<CompareExpr>(CmpOp::GREATER,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));

    LogicalExpr orExpr2(CmpOp::OR, std::move(compare3), std::move(compare4));
    result = orExpr2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);
}

// Test three-valued logic with NULL
TEST_F(PredicateTest, ThreeValuedLogic) {
    // NULL AND true = NULL
    auto nullConst = std::make_unique<ConstantExpr>();
    auto trueConst = std::make_unique<ConstantExpr>(DataType::getBool(), true);
    auto compare1 = std::make_unique<CompareExpr>(CmpOp::EQUAL,
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L));  // true

    LogicalExpr andExpr(CmpOp::AND, std::move(nullConst), std::move(compare1));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = andExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);
}

// Test predicate evaluation with actual column data
TEST_F(PredicateTest, ColumnComparison) {
    // Create a column buffer with some data
    static std::vector<int64_t> intData = {10, 20, 30, 40, 50};
    ColumnBuffer col;
    col.columnId = ColumnId(0, "col0");
    col.type = DataType::getInt64();
    col.data = intData.data();
    col.nullBitmap = nullptr;
    col.count = 5;

    RowVectorBuffer buffer;
    buffer.addColumn(col);
    buffer.setRowCount(5);

    // Create predicate: column > 25
    ColumnId colId(0, "col0");
    auto colRef = std::make_unique<ColumnRefExpr>(colId);
    auto constant = std::make_unique<ConstantExpr>(DataType::getInt64(), 25L);
    CompareExpr compare(CmpOp::GREATER, std::move(colRef), std::move(constant));

    // Evaluate for each row
    EXPECT_EQ(compare.evaluateRow(buffer, 0), PredicateValue::FALSE);  // 10 > 25 = false
    EXPECT_EQ(compare.evaluateRow(buffer, 1), PredicateValue::FALSE);  // 20 > 25 = false
    EXPECT_EQ(compare.evaluateRow(buffer, 2), PredicateValue::TRUE);   // 30 > 25 = true
    EXPECT_EQ(compare.evaluateRow(buffer, 3), PredicateValue::TRUE);   // 40 > 25 = true
    EXPECT_EQ(compare.evaluateRow(buffer, 4), PredicateValue::TRUE);  // 50 > 25 = true
}

