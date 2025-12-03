#pragma once

#include "common/logging.hpp"
#include "engine/physical_operator.hpp"
#include "engine/predicate_expr.hpp"
#include "engine/memory.hpp"
#include "engine/predicate_result.hpp"
#include <cstdint>
#include <memory>
#include <cstring>
#include <vector>

namespace toydb {

class PredicateExpr;

class NestedLoopJoinExec : public PhysicalOperator {
private:
    PhysicalOperator* left_;
    PhysicalOperator* right_;
    std::unique_ptr<PredicateExpr> predicate_;
    memory::BufferManager bufferManager_;

    // Materialized left side (build input)
    std::vector<RowVectorBuffer> materializedLeft_;

public:
    NestedLoopJoinExec(PhysicalOperator* left, PhysicalOperator* right,
                      std::unique_ptr<PredicateExpr> predicate)
        : left_(left), right_(right), predicate_(std::move(predicate)) {}

    void initialize() override {
        // Initialize both operators
        left_->initialize();
        right_->initialize();
    }

    int64_t next(RowVectorBuffer& out) override {
        Logger::debug("NestedLoopJoinExec::next");

        // Materialize the entire left side (build input)
        materializeLeftSide();


        int64_t totalOutputRows = 0;
        auto tempBuffer = bufferManager_.allocate();

        // Scan right side (probe input) in chunks
        RowVectorBuffer rightVector;
        int64_t rightCount = right_->next(rightVector);

        if (rightCount == 0) {
            return 0;
        }

        while (true) {
            Logger::debug("NestedLoopJoinExec::next: rightOut = \n{}", rightVector.toPrettyString());
            Logger::debug("NestedLoopJoinExec::next: leftVector = \n{}", materializedLeft_[0].toPrettyString());

            // Join this right chunk against all materialized left rows
            for (int64_t rightRowIdx = 0; rightRowIdx < rightCount; ++rightRowIdx) {
                Logger::debug("NestedLoopJoinExec::next: processing right row {}", rightRowIdx);

                // Build a buffer with the right row repeated for each left row
                // TODO: only repeat the columns that are actually used in the predicate
                RowVectorBuffer vectorizedBuffer = buildRepeatedRowBuffer(
                    rightVector, rightRowIdx, tempBuffer.get());

                for (size_t leftBatchIdx = 0; leftBatchIdx < materializedLeft_.size(); ++leftBatchIdx) {
                    const RowVectorBuffer& leftVector = materializedLeft_[leftBatchIdx];

                    // Add the left rows to the vectorized buffer
                    for (const auto& column : leftVector.getColumns()){
                        vectorizedBuffer.addOrReplaceColumn(column);
                    }
                    // TODO: What if the left row count is higher?
                    vectorizedBuffer.setRowCount(leftVector.getRowCount());

                    Logger::debug("NestedLoopJoinExec::next: processing left batch {} with {} rows",
                                leftBatchIdx, leftVector.getRowCount());


                    PredicateResultVector predicateResults = predicate_->evaluate(vectorizedBuffer);
                    int64_t matchesInBatch = copyMatchedRows(
                        leftVector, rightVector, rightRowIdx, predicateResults, out, totalOutputRows);

                    totalOutputRows += matchesInBatch;
                }
            }

            rightCount = right_->next(rightVector);
            if (rightCount == 0) {
                break;
            }
        }

        out.setRowCount(totalOutputRows);
        return totalOutputRows;
    }

private:

    /**
     * @brief Materialize the entire left side (build input) into memory
     */
    void materializeLeftSide() {
        Logger::debug("NestedLoopJoinExec::materializeLeftSide: starting materialization");

        RowVectorBuffer leftBatch;
        int64_t batchCount = 0;

        // Read all batches from the left operator until exhausted
        while (true) {
            int64_t rowCount = left_->next(leftBatch);
            if (rowCount == 0) {
                break;
            }

            // Store the batch in the materialized vector
            materializedLeft_.push_back(leftBatch);
            batchCount++;

            Logger::debug("NestedLoopJoinExec::materializeLeftSide: materialized batch {} with {} rows",
                         batchCount, rowCount);

            // Clear the batch for the next iteration
            leftBatch = RowVectorBuffer();
        }

        Logger::debug("NestedLoopJoinExec::materializeLeftSide: completed materialization of {} batches",
                     materializedLeft_.size());
    }

    /**
     * @brief Creates RowVectorBuffer for evaluating a prediacate over two RowVectorBuffers
     *
     * The right row at the supplied index is repeated for each left row in the batch.
     *
     * TODO:
     * 1. Only copy the column entries that are actually used in the predicate
     * 2. Avoid copying the column entries and just do pointer arithmetic
     * 3. Figure out a way to not have to repeat row data N times in the first place
     */
    RowVectorBuffer buildRepeatedRowBuffer(
        const RowVectorBuffer& rightVector,
        int64_t rightRowIdx,
        void* tempBuffer) const {

        RowVectorBuffer repeatedRowBuffer;

        // Calculate total buffer space needed and partition it among right columns
        char* bufferPtr = static_cast<char*>(tempBuffer);
        std::size_t bufferSize = memory::BufferManager::BUFFER_SIZE;

        int64_t rowSize = 0;
        for (const auto& column : rightVector.getColumns()){
            rowSize += column.type.getSize();
        }

        int64_t copiedRowCount = bufferSize / rowSize;
        std::size_t offset = 0;

        // Copy right columns - repeat the current right row N times (where N = left row count)
        for (const auto& column : rightVector.getColumns()){
            int32_t entrySize = column.type.getSize();

            ColumnBuffer repeatedCol = column;
            repeatedCol.data = bufferPtr + offset;
            repeatedCol.count = copiedRowCount;

            for (int64_t i = 0; i < copiedRowCount; ++i) {
                std::memcpy(bufferPtr + offset, static_cast<const char*>(column.data) + (entrySize * rightRowIdx), entrySize);
                offset += entrySize;
            }

            // TODO: Handle null bitmap properly when repeating rows
            // For now, we'll leave it as-is (pointing to the original null bitmap)

            repeatedRowBuffer.addColumn(repeatedCol);
        }

        repeatedRowBuffer.setRowCount(copiedRowCount);
        Logger::debug("buildRepeatedRowBuffer: repeatedRowBuffer.rowCount = {}", repeatedRowBuffer.getRowCount());
        return repeatedRowBuffer;
    }

    /**
     * @brief Copy matched rows to the output buffer based on predicate results
     *
     * @return Number of matched rows copied
     */
    int64_t copyMatchedRows(
        [[maybe_unused]] const RowVectorBuffer& leftVector,
        [[maybe_unused]] const RowVectorBuffer& rightVector,
        [[maybe_unused]] int64_t rightRowIdx,
        const PredicateResultVector& predicateResults,
        [[maybe_unused]] RowVectorBuffer& out,
        [[maybe_unused]] int64_t currentOutputOffset) const {

        int64_t matchedCount = 0;
        int64_t leftRowCount = leftVector.getRowCount();

        // Count matches first
        for (int64_t i = 0; i < leftRowCount; ++i) {
            if (predicateResults.isTrue(i)) {
                matchedCount++;
            }
        }

        if (matchedCount == 0) {
            return 0;
        }

        // TODO: Implement actual row copying to output buffer
        // For now, we'll just return the count
        // In a full implementation, we would:
        // 1. Ensure output buffer has enough space
        // 2. Copy left row data for each match
        // 3. Copy corresponding right row data for each match
        // 4. Update output buffer's row count

        return matchedCount;
    }
};

} // namespace toydb