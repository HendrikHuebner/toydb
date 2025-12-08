#include <memory>
#include "engine/nested_loop_join.hpp"
#include "engine/predicate_expr.hpp"
#include "gtest/gtest.h"
#include "test_helpers.hpp"

using namespace toydb;
using namespace toydb::test;
using namespace toydb::test::data_helpers;

class NestedLoopJoinTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Setup will be done in individual tests
    }

    // Helper to create a sequence of integers
    std::vector<int64_t> createSequence(int64_t start, int64_t count) {
        std::vector<int64_t> result;
        result.reserve(count);
        for (int64_t i = 0; i < count; ++i) {
            result.push_back(start + i);
        }
        return result;
    }
};

// Test basic nested loop join with equality predicate
TEST_F(NestedLoopJoinTest, BasicEqualityJoin) {
    ColumnBufferStorage storage;

    // Left table: [1, 2, 3] with column ID 0
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", {1, 2, 3}).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right table: [2, 3, 4] with column ID 1 (different from left)
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col1", {2, 3, 4}).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    // Create predicate: left.col0 = right.col0
    // Left column has ID 0, right column has ID 1
    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");

    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));

    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [1, 2, 3], Right: [2, 3, 4]
    // Matches: left[1]=2 matches right[0]=2, left[2]=3 matches right[1]=3
    // Expected: 2 matches
    EXPECT_EQ(resultCount, 2);
}

// Test nested loop join with greater than predicate
TEST_F(NestedLoopJoinTest, GreaterThanJoin) {
    ColumnBufferStorage storage;

    // Left table: [5, 10, 15]
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", {5, 10, 15}).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right table: [3, 8, 12]
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col0", {3, 8, 12}).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    // Create predicate: left.col0 > right.col0
    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col0");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));

    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [5, 10, 15], Right: [3, 8, 12]
    // Matches: (5>3), (5>8=false), (5>12=false), (10>3), (10>8), (10>12=false), (15>3), (15>8), (15>12)
    // Expected: 6 matches (5>3, 10>3, 10>8, 15>3, 15>8, 15>12)
    EXPECT_EQ(resultCount, 6);
}

// Test nested loop join with complex predicate (AND)
TEST_F(NestedLoopJoinTest, ComplexPredicateJoin) {
    ColumnBufferStorage storage;

    // Left table with two columns: col0=[1,2,3], col1=[10,20,30]
    auto leftOpPtr = MockOperatorBuilder(&storage)
                         .addInt64Column(0, "col0", {1, 2, 3})
                         .addInt64Column(1, "col1", {10, 20, 30})
                         .build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right table: [2, 3, 4]
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(2, "col0", {2, 3, 4}).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    // Create predicate: (left.col0 = right.col0) AND (left.col1 > 15)
    ColumnId leftCol0Id(0, "col0");
    ColumnId rightCol0Id(2, "col0");
    ColumnId leftCol1Id(1, "col1");

    auto eqCompare =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::make_unique<ColumnRefExpr>(leftCol0Id),
                                      std::make_unique<ColumnRefExpr>(rightCol0Id));

    auto const15 = std::make_unique<ConstantExpr>(DataType::getInt64(), 15L);
    auto gtCompare = std::make_unique<CompareExpr>(
        CompareOp::GREATER, DataType::getInt64(), std::make_unique<ColumnRefExpr>(leftCol1Id), std::move(const15));

    auto andPred =
        std::make_unique<LogicalExpr>(CompareOp::AND, std::move(eqCompare), std::move(gtCompare));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(andPred));

    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [(1,10), (2,20), (3,30)], Right: [2, 3, 4]
    // Predicate: (left.col0 = right.col0) AND (left.col1 > 15)
    // For right=2: (2,20) matches (2=2 AND 20>15) = 1
    // For right=3: (3,30) matches (3=3 AND 30>15) = 1
    // For right=4: no match
    // Expected: 2 matches
    EXPECT_EQ(resultCount, 2);
}

// Test nested loop join with no matches
TEST_F(NestedLoopJoinTest, NoMatches) {
    ColumnBufferStorage storage;

    // Left table: [1, 2]
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", {1, 2}).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right table: [100, 200] (no matches with left)
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col0", {100, 200}).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    // Create predicate: left.col0 = right.col0 (will never match)
    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col0");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));

    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Should have 0 matches
    EXPECT_EQ(resultCount, 0);
}

// Test nested loop join with empty right table
TEST_F(NestedLoopJoinTest, EmptyRightTable) {
    ColumnBufferStorage storage;

    // Left table: [1, 2, 3]
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", {1, 2, 3}).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Empty right table
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col0", {}).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col0");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));

    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Should return 0 when right is empty
    EXPECT_EQ(resultCount, 0);
}

// Test nested loop join with large data set (single batch)
TEST_F(NestedLoopJoinTest, LargeDataEqualityJoin) {
    ColumnBufferStorage storage;

    // Left: [0, 1, 2, ..., 999] (1000 rows)
    auto leftOpPtr =
        MockOperatorBuilder(&storage).addInt64Column(0, "col0", intSequence(0, 1000)).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [500, 501, ..., 1499] (1000 rows, half overlap)
    auto rightOpPtr =
        MockOperatorBuilder(&storage).addInt64Column(1, "col1", intSequence(500, 1000)).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Overlap: left[500..999] matches right[0..499] = 500 matches
    EXPECT_EQ(resultCount, 500);
}

// Test nested loop join with large data set in multiple batches (left side)
TEST_F(NestedLoopJoinTest, LargeDataMultiBatchLeft) {
    ColumnBufferStorage storage;

    // Left: [0, 1, 2, ..., 999] split into 5 batches of 200
    std::vector<int64_t> leftData = createSequence(0, 1000);
    auto leftOpPtr = MockOperatorBuilder(&storage)
                         .addInt64Column(0, "col0", leftData)
                         .withBatchSizes({200, 200, 200, 200, 200})
                         .build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [500, 501, ..., 999] (500 rows, matches second half of left)
    std::vector<int64_t> rightData = createSequence(500, 500);
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col1", rightData).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left batches: [0-199], [200-399], [400-599], [600-799], [800-999]
    // Right: [500-999]
    // Matches: left[500-599] with right[0-99] = 100, left[600-799] with right[100-299] = 200, left[800-999] with right[300-499] = 200
    // Total: 500 matches
    EXPECT_EQ(resultCount, 500);
}

// Test nested loop join with large data set in multiple batches (right side)
TEST_F(NestedLoopJoinTest, LargeDataMultiBatchRight) {
    ColumnBufferStorage storage;

    // Left: [0, 1, 2, ..., 999] (1000 rows)
    std::vector<int64_t> leftData = createSequence(0, 1000);
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", leftData).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [500, 501, ..., 1499] split into 5 batches of 200
    std::vector<int64_t> rightData = createSequence(500, 1000);
    auto rightOpPtr = MockOperatorBuilder(&storage)
                          .addInt64Column(1, "col1", rightData)
                          .withBatchSizes({200, 200, 200, 200, 200})
                          .build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [0-999], Right batches: [500-699], [700-899], [900-1099], [1100-1299], [1300-1499]
    // Matches: left[500-699] with right batch 1[0-199] = 200, left[700-899] with right batch 2[0-199] = 200, left[900-999] with right batch 3[0-99] = 100
    // Total: 500 matches
    EXPECT_EQ(resultCount, 500);
}

// Test nested loop join with large data set and greater than predicate
TEST_F(NestedLoopJoinTest, LargeDataGreaterThanJoin) {
    ColumnBufferStorage storage;

    // Left: [100, 101, 102, ..., 199] (100 rows)
    std::vector<int64_t> leftData = createSequence(100, 100);
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", leftData).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [0, 1, 2, ..., 149] (150 rows)
    std::vector<int64_t> rightData = createSequence(0, 150);
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col1", rightData).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::GREATER, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [100-199], Right: [0-149]
    // For each left value: matches all right values < left value
    // left[100] matches right[0-99] = 100
    // left[101] matches right[0-100] = 101
    // ...
    // left[149] matches right[0-148] = 149
    // left[150-199] matches right[0-149] = 150 each (50 values)
    // Total: sum(100..149) + (50*150) = 6225 + 7500 = 13725
    int64_t expected = 0;
    for (int64_t leftVal = 100; leftVal < 150; ++leftVal) {
        expected += leftVal;  // matches right[0..leftVal-1]
    }
    for (int64_t leftVal = 150; leftVal < 200; ++leftVal) {
        expected += 150;  // matches all 150 right values
    }
    EXPECT_EQ(resultCount, expected);
    EXPECT_EQ(expected, 13725);  // Verify expected value
}

// Test nested loop join with very large data set in multiple batches on both sides
TEST_F(NestedLoopJoinTest, VeryLargeDataMultiBatchBothSides) {
    ColumnBufferStorage storage;

    // Left: [0, 1, 2, ..., 4999] split into 10 batches of 500
    std::vector<int64_t> leftData = createSequence(0, 1000);
    auto leftOpPtr = MockOperatorBuilder(&storage)
                         .addInt64Column(0, "col0", leftData)
                         .withBatchSizes({550, 550, 550, 1000, 550, 550, 550, 200, 500})
                         .build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [2000, 2001, ..., 4999] split into 5 batches of 600
    std::vector<int64_t> rightData = createSequence(2000, 3000);
    auto rightOpPtr = MockOperatorBuilder(&storage)
                          .addInt64Column(1, "col1", rightData)
                          .withBatchSizes({600, 600, 600, 450, 600, 150})
                          .build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // Left: [0-4999], Right: [2000-4999]
    // Overlap: left[2000-4999] matches right[0-2999] = 3000 matches
    EXPECT_EQ(resultCount, 3000);
}

// Test nested loop join with large data set and no matches
TEST_F(NestedLoopJoinTest, LargeDataNoMatches) {
    ColumnBufferStorage storage;

    // Left: [0, 1, 2, ..., 999] (1000 rows)
    std::vector<int64_t> leftData = createSequence(0, 1000);
    auto leftOpPtr = MockOperatorBuilder(&storage).addInt64Column(0, "col0", leftData).build();
    PhysicalOperator* leftOp = leftOpPtr.get();

    // Right: [10000, 10001, ..., 10999] (1000 rows, no overlap)
    std::vector<int64_t> rightData = createSequence(10000, 1000);
    auto rightOpPtr = MockOperatorBuilder(&storage).addInt64Column(1, "col1", rightData).build();
    PhysicalOperator* rightOp = rightOpPtr.get();

    ColumnId leftColId(0, "col0");
    ColumnId rightColId(1, "col1");
    auto leftCol = std::make_unique<ColumnRefExpr>(leftColId);
    auto rightCol = std::make_unique<ColumnRefExpr>(rightColId);
    auto predicate =
        std::make_unique<CompareExpr>(CompareOp::EQUAL, DataType::getInt64(), std::move(leftCol), std::move(rightCol));

    NestedLoopJoinExec join(leftOp, rightOp, std::move(predicate));
    join.initialize();

    RowVectorBuffer output;
    int64_t resultCount = join.next(output);

    // No overlap, should have 0 matches
    EXPECT_EQ(resultCount, 0);
}
