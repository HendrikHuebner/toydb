#pragma once

#include <filesystem>
#include <fstream>
#include <vector>
#include "engine/physical_operator.hpp"
#include "storage/data_file_reader.hpp"
#include "storage/catalog.hpp"
#include "common/types.hpp"

namespace toydb {

/**
 * @brief Very dumb CSV file reader. The following is supported:
 * - Comma separated values
 * - Double quotes can be used to escape the comma (Double quotes cannot be escaped themselves right now)
 * - DataTypes INT32, INT64, STRING, BOOL
 * - NULL/null values (Cannot be escaped...)
 * - BOOL values are case insensitive
 * - Invalid CSV is UB
 */
class CsvDataFileReader : public DataFileReader {
public:
    CsvDataFileReader(const std::filesystem::path& filePath, const Schema& schema, TableId tableId);

    CsvDataFileReader(const CsvDataFileReader&) = delete;
    CsvDataFileReader& operator=(const CsvDataFileReader&) = delete;

    ~CsvDataFileReader() override = default;

    /**
     * @brief Read a batch of rows from the CSV file. RowVector must be pre-allocated
     * and initialized with the correct schema. Assertion failure / UB otherwise.
     */
    int64_t readBatch(RowVector& out, int64_t requestedRows = 8192) override;

    bool hasMore() const noexcept override;

    void reset() override;

    std::filesystem::path getPath() const noexcept override { return file_path_; }

    const Schema& getSchema() const noexcept override { return schema_; }

private:
    std::filesystem::path file_path_;
    Schema schema_;
    TableId table_id_;
    std::ifstream file_;
    bool header_read_;
    bool eof_;
    char separator_ = ',';

    std::vector<std::string> parseCSVLine(const std::string& line);
};

}  // namespace toydb
