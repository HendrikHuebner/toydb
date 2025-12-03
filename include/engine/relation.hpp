#pragma once

#include <cstddef>
#include <expected>
#include <filesystem>
#include <vector>
#include "common/types.hpp"
#include "engine/memory.hpp"

namespace toydb {

class Column {
    std::string identifier;
    DataType type;

    std::string_view getIdentifier() const noexcept {
        return identifier;
    }

    DataType getDataType() const noexcept {
        return type;
    }
};

class Batch {
};

class BatchReader {

};

class Relation {
    std::vector<Column> columns;

  public:
    int32_t getColumnCount() const noexcept { return columns.size(); };
    const std::vector<Column>& getColumns() const noexcept { return columns; };

    virtual int64_t getRowCount() const noexcept;

    /**
     * Create RowCursor initialized at the start of the Relation
     */
    virtual BatchReader* getBatchReader();

    virtual ~Relation();
};

} // namespace toydb
