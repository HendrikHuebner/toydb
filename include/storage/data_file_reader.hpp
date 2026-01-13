#pragma once

#include <cstdint>
#include <filesystem>
#include "engine/physical_operator.hpp"
#include "storage/catalog.hpp"

namespace toydb {

class DataFileReader {
public:
    virtual ~DataFileReader() = default;

    /**
     * @brief Read next batch of rows into the buffer
     * @param out Buffer to fill with column data. Must be pre-allocated,
     * set up with the right schema and have enough capacity for the requested rows.
     * @param requestedRows Maximum number of rows to read (hint)
     * @return Number of rows actually read (0 if EOF)
     */
    virtual int64_t readBatch(RowVector& out, int64_t requestedRows = 8192) = 0;

    virtual bool hasMore() const noexcept = 0;

    /**
     * @brief Reset to beginning of file
     */
    virtual void reset() = 0;

    virtual std::filesystem::path getPath() const noexcept = 0;

    virtual const Schema& getSchema() const noexcept = 0;
};

}  // namespace toydb
