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
    CompareExpr compare(CompareOp::LESS, DataType::getInt64(), std::move(left), std::move(right));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    // For constants, we can test the row evaluation
    PredicateValue result = compare.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);

    // Test 3 < 5 (should be true)
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare2(CompareOp::LESS, DataType::getInt64(), std::move(left), std::move(right));

    result = compare2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Create comparison: 5 > 3
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    CompareExpr compare3(CompareOp::GREATER, DataType::getInt64(), std::move(left), std::move(right));

    emptyBuffer.setRowCount(1);

    // For constants, we can test the row evaluation
    result = compare3.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test 3 > 5 (should be false)
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare4(CompareOp::GREATER, DataType::getInt64(), std::move(left), std::move(right));

    result = compare4.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);

    // Test equality
    left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare5(CompareOp::EQUAL, DataType::getInt64(), std::move(left), std::move(right));

    result = compare5.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);
}

// Test CompareExpr with NULL
TEST_F(PredicateTest, CompareExprNull) {
    // Test NULL > 5
    auto left = std::make_unique<ConstantExpr>();
    auto right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare(CompareOp::GREATER, DataType::getInt64(), std::move(left), std::move(right));
    compare.initializeIndexMap();
    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = compare.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);

    // Test NULL = NULL
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>();
    CompareExpr compare2(CompareOp::EQUAL, DataType::getInt64(), std::move(left), std::move(right));
    compare2.initializeIndexMap();
    result = compare2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);

    // Test NULL < 5
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare3(CompareOp::LESS, DataType::getInt64(), std::move(left), std::move(right));
    compare3.initializeIndexMap();
    result = compare3.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);

    // Test NULL >= 5
    left = std::make_unique<ConstantExpr>();
    right = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    CompareExpr compare4(CompareOp::GREATER_EQUAL, DataType::getInt64(), std::move(left), std::move(right));
    compare4.initializeIndexMap();
    result = compare4.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);
}

// Test LogicalExpr AND
TEST_F(PredicateTest, LogicalExprAND) {
    auto left = std::make_unique<ConstantExpr>(DataType::getInt64(), 5L);
    auto right = std::make_unique<ConstantExpr>(DataType::getInt64(), 3L);
    auto compare1 = std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 3L));
    auto compare2 = std::make_unique<CompareExpr>(CompareOp::LESS, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));

    LogicalExpr andExpr(CompareOp::AND, std::move(compare1), std::move(compare2));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = andExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test AND with false
    auto compare3 = std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));
    auto compare4 = std::make_unique<CompareExpr>(CompareOp::LESS, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // false

    LogicalExpr andExpr2(CompareOp::AND, std::move(compare3), std::move(compare4));
    result = andExpr2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);
}

// Test LogicalExpr OR
TEST_F(PredicateTest, LogicalExprOR) {
    auto compare1 = std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // false
    auto compare2 = std::make_unique<CompareExpr>(CompareOp::LESS, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 2L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));  // true

    LogicalExpr orExpr(CompareOp::OR, std::move(compare1), std::move(compare2));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = orExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::TRUE);

    // Test OR with both false
    auto compare3 = std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));
    auto compare4 = std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 1L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 10L));

    LogicalExpr orExpr2(CompareOp::OR, std::move(compare3), std::move(compare4));
    result = orExpr2.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::FALSE);
}

// Test three-valued logic with NULL
TEST_F(PredicateTest, ThreeValuedLogic) {
    // NULL AND true = NULL
    auto nullConst = std::make_unique<ConstantExpr>();
    auto trueConst = std::make_unique<ConstantExpr>(DataType::getBool(), true);
    auto compare1 = std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L),
        std::make_unique<ConstantExpr>(DataType::getInt64(), 5L));  // true

    LogicalExpr andExpr(CompareOp::AND, std::move(nullConst), std::move(compare1));

    RowVectorBuffer emptyBuffer;
    emptyBuffer.setRowCount(1);

    PredicateValue result = andExpr.evaluateRow(emptyBuffer, 0);
    EXPECT_EQ(result, PredicateValue::NULL_VALUE);
}

TEST_F(PredicateTest, ColumnComparison) {
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
    auto colRef = std::make_unique<ColumnRefExpr>(colId, DataType::getInt64());
    auto constant = std::make_unique<ConstantExpr>(DataType::getInt64(), 25L);
    CompareExpr compare(CompareOp::GREATER, DataType::getInt64(), std::move(colRef), std::move(constant));
    compare.initializeIndexMap();

    EXPECT_EQ(compare.evaluateRow(buffer, 0), PredicateValue::FALSE);  // 10 > 25 = false
    EXPECT_EQ(compare.evaluateRow(buffer, 1), PredicateValue::FALSE);  // 20 > 25 = false
    EXPECT_EQ(compare.evaluateRow(buffer, 2), PredicateValue::TRUE);   // 30 > 25 = true
    EXPECT_EQ(compare.evaluateRow(buffer, 3), PredicateValue::TRUE);   // 40 > 25 = true
    EXPECT_EQ(compare.evaluateRow(buffer, 4), PredicateValue::TRUE);  // 50 > 25 = true
}

TEST_F(PredicateTest, ComplexNestedPredicateWithColumnIndexMap) {
    static std::vector<int64_t> col0Data = {10, 20, 30, 40, 50};
    static std::vector<int64_t> col1Data = {5, 15, 25, 35, 45};
    static std::vector<int64_t> col2Data = {100, 200, 300, 400, 500};

    ColumnBuffer col0;
    col0.columnId = ColumnId(0, "col0");
    col0.type = DataType::getInt64();
    col0.data = col0Data.data();
    col0.nullBitmap = nullptr;
    col0.count = 5;

    ColumnBuffer col1;
    col1.columnId = ColumnId(1, "col1");
    col1.type = DataType::getInt64();
    col1.data = col1Data.data();
    col1.nullBitmap = nullptr;
    col1.count = 5;

    ColumnBuffer col2;
    col2.columnId = ColumnId(2, "col2");
    col2.type = DataType::getInt64();
    col2.data = col2Data.data();
    col2.nullBitmap = nullptr;
    col2.count = 5;

    // Create buffer with only the columns referenced by the predicate
    // Predicate: (col0 > 25) AND ((col1 < 20) OR (col2 > 250))
    RowVectorBuffer buffer;
    buffer.addColumn(col0);  // Index 0
    buffer.addColumn(col1);  // Index 1
    buffer.addColumn(col2);  // Index 2
    buffer.setRowCount(5);

    // Build complex nested predicate: (col0 > 25) AND ((col1 < 20) OR (col2 > 250))
    ColumnId col0Id(0, "col0");
    ColumnId col1Id(1, "col1");
    ColumnId col2Id(2, "col2");

    // col0 > 25
    auto col0Ref = std::make_unique<ColumnRefExpr>(col0Id, DataType::getInt64());
    auto const25 = std::make_unique<ConstantExpr>(DataType::getInt64(), 25L);
    auto col0Gt25 = std::make_unique<CompareExpr>(
        CompareOp::GREATER, DataType::getInt64(), std::move(col0Ref), std::move(const25));

    // col1 < 20
    auto col1Ref = std::make_unique<ColumnRefExpr>(col1Id, DataType::getInt64());
    auto const20 = std::make_unique<ConstantExpr>(DataType::getInt64(), 20L);
    auto col1Lt20 = std::make_unique<CompareExpr>(
        CompareOp::LESS, DataType::getInt64(), std::move(col1Ref), std::move(const20));

    // col2 > 250
    auto col2Ref = std::make_unique<ColumnRefExpr>(col2Id, DataType::getInt64());
    auto const250 = std::make_unique<ConstantExpr>(DataType::getInt64(), 250L);
    auto col2Gt250 = std::make_unique<CompareExpr>(
        CompareOp::GREATER, DataType::getInt64(), std::move(col2Ref), std::move(const250));

    // (col1 < 20) OR (col2 > 250)
    auto orExpr = std::make_unique<LogicalExpr>(
        CompareOp::OR, std::move(col1Lt20), std::move(col2Gt250));

    // (col0 > 25) AND ((col1 < 20) OR (col2 > 250))
    LogicalExpr complexPred(CompareOp::AND, std::move(col0Gt25), std::move(orExpr));

    // Initialize column indices (pre-order traversal)
    complexPred.initializeIndexMap();

    // Test getColumnIndexMap
    auto indexMap = complexPred.getColumnIndexMap();
    EXPECT_EQ(indexMap.size(), 3);
    EXPECT_EQ(indexMap[col0Id], 0);
    EXPECT_EQ(indexMap[col1Id], 1);
    EXPECT_EQ(indexMap[col2Id], 2);

    // Evaluate for each row:
    // Row 0: col0=10, col1=5, col2=100
    //   col0 > 25 = false, so AND result = false
    EXPECT_EQ(complexPred.evaluateRow(buffer, 0), PredicateValue::FALSE);

    // Row 1: col0=20, col1=15, col2=200
    //   col0 > 25 = false, so AND result = false
    EXPECT_EQ(complexPred.evaluateRow(buffer, 1), PredicateValue::FALSE);

    // Row 2: col0=30, col1=25, col2=300
    //   col0 > 25 = true
    //   col1 < 20 = false, col2 > 250 = true, so OR = true
    //   AND result = true
    EXPECT_EQ(complexPred.evaluateRow(buffer, 2), PredicateValue::TRUE);

    // Row 3: col0=40, col1=35, col2=400
    //   col0 > 25 = true
    //   col1 < 20 = false, col2 > 250 = true, so OR = true
    //   AND result = true
    EXPECT_EQ(complexPred.evaluateRow(buffer, 3), PredicateValue::TRUE);

    // Row 4: col0=50, col1=45, col2=500
    //   col0 > 25 = true
    //   col1 < 20 = false, col2 > 250 = true, so OR = true
    //   AND result = true
    EXPECT_EQ(complexPred.evaluateRow(buffer, 4), PredicateValue::TRUE);

    // Test full evaluation
    PredicateResultVector result = complexPred.evaluate(buffer);
    EXPECT_EQ(result.get(0), PredicateValue::FALSE);
    EXPECT_EQ(result.get(1), PredicateValue::FALSE);
    EXPECT_EQ(result.get(2), PredicateValue::TRUE);
    EXPECT_EQ(result.get(3), PredicateValue::TRUE);
    EXPECT_EQ(result.get(4), PredicateValue::TRUE);
}

