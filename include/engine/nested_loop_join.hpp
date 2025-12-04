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

/**
 * @brief Implements the execution of a nested loop join with a comparison expression.
 *        The comparison must be a single compare operator with a left and right column or constants.
 */
class NestedLoopJoinExec : public PhysicalOperator {
private:
    PhysicalOperator* build_;
    PhysicalOperator* probe_;
    std::unique_ptr<PredicateExpr> joinExpr_;
    memory::BufferManager bufferManager_;

    // Materialized left side (build input)
    std::vector<RowVectorBuffer> materializedLeft_;

public:
    NestedLoopJoinExec(PhysicalOperator* build, PhysicalOperator* probe,
                       std::unique_ptr<PredicateExpr> joinExpr)
        : build_(build), probe_(probe), joinExpr_(std::move(joinExpr)) {}

    void initialize() override {
        // Initialize both operators
        build_->initialize();
        probe_->initialize();
    }

    int64_t next(RowVectorBuffer& out) override {
        Logger::debug("NestedLoopJoinExec::next");

        materializeBuildInput();

        int64_t totalOutputRows = 0;

        // Scan right side (probe input) in chunks
        RowVectorBuffer rightVector;
        int64_t rightCount = probe_->next(rightVector);

        // TODO: Implement the join logic.
        // 0. Get the index map for the predicate
        // 1. iterate over the rows in the probe vector
        // 2. for each row, iterate over the materialized left rows
        // 3. evaluate the predicate for the row: Create a row vector with the required columns specified by the index map
        // 4. if the predicate is true, add the row to the output vector (for now just returne the count and leave a todo)
        while (rightCount > 0) {

        }

        out.setRowCount(totalOutputRows);
        return totalOutputRows;
    }

private:

    /**
     * @brief Materialize the entire left side (build input) into memory
     */
    void materializeBuildInput() {
        Logger::debug("NestedLoopJoinExec::materializeLeftSide: starting materialization");

        RowVectorBuffer leftBatch;
        int64_t batchCount = 0;

        // Read all batches from the left operator until exhausted
        while (true) {
            int64_t rowCount = build_->next(leftBatch);
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
};
} // namespace toydb