#include "storage/csv_data_file_reader.hpp"
#include <cstring>
#include <algorithm>
#include "common/logging.hpp"
#include "common/types.hpp"
#include "common/assert.hpp"

namespace toydb {

CsvDataFileReader::CsvDataFileReader(const std::filesystem::path& filePath, const Schema& schema, TableId tableId)
    : file_path_(filePath), schema_(schema), table_id_(tableId), header_read_(false), eof_(false) {
    file_.open(filePath);
    if (!file_.is_open() || !file_.good()) {
        Logger::error("Failed to open CSV file: {}", filePath.string());
        eof_ = true;
        return;
    }
}

void CsvDataFileReader::reset() {
    if (file_.is_open()) {
        file_.clear();
        file_.seekg(0, std::ios::beg);
    }
    header_read_ = false;
    eof_ = false;
}

bool CsvDataFileReader::hasMore() const noexcept {
    return !eof_ && file_.is_open() && file_.good();
}

std::vector<std::string> CsvDataFileReader::parseCSVLine(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    bool inQuotes = false;

    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];

        if (c == '"') {
            inQuotes = !inQuotes;
        } else if (c == separator_ && !inQuotes) {
            fields.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    fields.push_back(field);

    return fields;
}


template<is_db_type T>
void parseAndWriteValue(const std::string& valueStr, ColumnBuffer& colBuf, int64_t index) {
    if (valueStr.empty() || valueStr == "NULL" || valueStr == "null") {
        colBuf.setNull(index);
        return;
    }

    colBuf.clearNull(index);

    if constexpr (std::same_as<T, db_int32>) {
        colBuf.writeEntry(index, static_cast<db_int32>(std::stoi(valueStr)));
    } else if constexpr (std::same_as<T, db_int64>) {
        colBuf.writeEntry(index, static_cast<db_int64>(std::stoll(valueStr)));
    } else if constexpr (std::same_as<T, db_double>) {
        colBuf.writeEntry(index, static_cast<db_double>(std::stod(valueStr)));
    } else if constexpr (std::same_as<T, db_bool>) {
        std::string lower = valueStr;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        colBuf.writeEntry(index, static_cast<db_bool>(lower == "true"));
    } else if constexpr (std::same_as<T, db_string>) {
        db_string str{};
        size_t len = std::min(valueStr.length(), size_t(255));
        std::strncpy(str, valueStr.c_str(), len);
        str[len] = '\0';
        colBuf.writeEntry(index, str);
    }
}

int64_t CsvDataFileReader::readBatch(RowVector& out, int64_t requestedRows) {
    if (eof_ || !file_.is_open() || !file_.good()) {
        return 0;
    }

    if (!header_read_) {
        std::string headerLine;
        if (!std::getline(file_, headerLine)) {
            eof_ = true;
            return 0;
        }
        header_read_ = true;
    }

    // Verify ColumnBuffers exist and have sufficient capacity
    tdb_assert(out.getColumnCount() == static_cast<int64_t>(schema_.columns.size()),
        "RowVector column count ({}) does not match schema column count ({})",
        out.getColumnCount(), schema_.columns.size());

    std::vector<ColumnBuffer*> columnBuffers;
    size_t colIdx = 0;
    for (const auto& [colId, colMeta] : schema_.columns) {
        tdb_assert(colIdx < static_cast<size_t>(out.getColumnCount()),
            "Column index {} out of range", colIdx);

        ColumnBuffer& colBuf = out.getColumn(static_cast<int64_t>(colIdx));

        tdb_assert(colBuf.columnId == colId,
            "Column {} mismatch: expected {}, got {}",
            colIdx, colId.getId(), colBuf.columnId.getId());
        tdb_assert(colBuf.type == colMeta.type,
            "Column {} type mismatch: expected {}, got {}",
            colIdx, colMeta.type.toString(), colBuf.type.toString());
        tdb_assert(colBuf.getCapacity() >= requestedRows,
            "Column {} capacity ({}) insufficient for requested rows ({})",
            colIdx, colBuf.getCapacity(), requestedRows);

        columnBuffers.push_back(&colBuf);
        ++colIdx;
    }

    int64_t rowsRead = 0;
    std::string line;
    while (rowsRead < requestedRows && std::getline(file_, line)) {
        if (line.empty()) {
            continue;
        }

        std::vector<std::string> fields = parseCSVLine(line);
        if (fields.size() != schema_.columns.size()) {
            Logger::warn("CSV line has {} fields, expected {}: {}", fields.size(), schema_.columns.size(), line);
            continue;
        }

        colIdx = 0;
        for (const auto& [colId, colMeta] : schema_.columns) {
            ColumnBuffer& colBuf = *columnBuffers[colIdx];

            switch (colMeta.type.getType()) {
                case DataType::Type::INT32:
                    parseAndWriteValue<db_int32>(fields[colIdx], colBuf, rowsRead);
                    break;
                case DataType::Type::INT64:
                    parseAndWriteValue<db_int64>(fields[colIdx], colBuf, rowsRead);
                    break;
                case DataType::Type::DOUBLE:
                    parseAndWriteValue<db_double>(fields[colIdx], colBuf, rowsRead);
                    break;
                case DataType::Type::BOOL:
                    parseAndWriteValue<db_bool>(fields[colIdx], colBuf, rowsRead);
                    break;
                case DataType::Type::STRING:
                    parseAndWriteValue<db_string>(fields[colIdx], colBuf, rowsRead);
                    break;
                default:
                    tdb_unreachable("Unsupported type");
            }

            ++colIdx;
        }

        ++rowsRead;
    }

    if (rowsRead == 0) {
        eof_ = true;
        return 0;
    }

    // Update count for all columns
    for (auto* colBuf : columnBuffers) {
        colBuf->count = rowsRead;
    }

    out.setRowCount(rowsRead);

    if (!file_.good()) {
        eof_ = true;
    }

    return rowsRead;
}

}  // namespace toydb
