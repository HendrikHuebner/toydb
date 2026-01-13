#pragma once

#include <filesystem>
#include <memory>
#include <vector>
#include "common/assert.hpp"
#include "engine/physical_operator.hpp"
#include "storage/catalog.hpp"
#include "storage/data_file_reader.hpp"
#include "common/types.hpp"

namespace toydb {

class TableIterator {
public:
    virtual ~TableIterator() = default;

    /**
     * @brief Read next batch of rows
     * @param out Buffer to fill with column data
     * @return Number of rows read (0 if no more data)
     */
    virtual int64_t next(RowVector& out) = 0;

    /**
     * @brief Check if more data is available
     */
    virtual bool hasMore() const noexcept = 0;

    /**
     * @brief Reset iterator to beginning
     */
    virtual void reset() = 0;
};

class TableHandle {
public:
    explicit TableHandle(TableId tableId, StorageFormat format, const std::vector<ColumnMetadata>& schema,
                         const std::vector<std::filesystem::path>& filePaths)
    : table_id_(tableId), format_(format), schema_(schema), file_paths_(filePaths) {
        tdb_assert(filePaths.size() == 1, "TableHandle currently only supports exactly one file");
    }

    TableHandle(const TableHandle&) = delete;
    TableHandle& operator=(const TableHandle&) = delete;

    ~TableHandle();

    /**
     * @brief Create iterator for reading table in batches
     */
    std::unique_ptr<TableIterator> createIterator(int64_t requestedBatchSize = 8192);

    const std::vector<ColumnMetadata>& getSchema() const noexcept { return schema_; }

    TableId getTableId() const noexcept { return table_id_; }

    StorageFormat getFormat() const noexcept { return format_; }

    std::vector<std::filesystem::path> getFilePaths() const noexcept { return file_paths_; }

    /**
     * @brief Factory method to create a file reader for the given file path and file format
     */
    std::unique_ptr<DataFileReader> createFileReader(const std::filesystem::path& filePath);

private:
    TableId table_id_;
    StorageFormat format_;
    std::vector<ColumnMetadata> schema_;
    std::vector<std::filesystem::path> file_paths_;
};

class TableIteratorImpl : public TableIterator {
public:
    TableIteratorImpl(TableHandle* handle, int64_t batchSize);

    int64_t next(RowVector& out) override;

    bool hasMore() const noexcept override;

    void reset() override;

private:
    TableHandle* handle_;
    int64_t batch_size_;
    size_t current_file_index_;
    std::vector<std::unique_ptr<DataFileReader>> readers_;
    bool initialized_;

    void initialize();
};

}  // namespace toydb
