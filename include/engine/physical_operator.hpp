#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/types.hpp"

namespace toydb {

class ColumnBuffer {
   public:
    ColumnId columnId;
    DataType type;
    void* data;
    uint8_t* nullBitmap;
    int64_t count; // number of rows in the column

    bool isNull(int64_t index) const noexcept {
        if (!nullBitmap) {
            return false;
        }
        return (nullBitmap[index / 8] & (1 << (index % 8))) == 0;
    }

    int32_t getInt32(int64_t index) const {
        tdb_assert(type == DataType::getInt32(), "Column type is not INT32");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if (isNull(index)) {
            return 0;
        }
        int32_t* dataPtr = static_cast<int32_t*>(data);
        return dataPtr[index];
    }

    int64_t getInt64(int64_t index) const {
        tdb_assert(type == DataType::getInt64(), "Column type is not INT64");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if (isNull(index)) {
            return 0;
        }
        int64_t* dataPtr = static_cast<int64_t*>(data);
        return dataPtr[index];
    }

    double getDouble(int64_t index) const {
        tdb_assert(type == DataType::getDouble(), "Column type is not DOUBLE");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if (isNull(index)) {
            return 0.0;
        }
        double* dataPtr = static_cast<double*>(data);
        return dataPtr[index];
    }

    bool getBool(int64_t index) const {
        tdb_assert(type == DataType::getBool(), "Column type is not BOOL");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if (isNull(index)) {
            return false;
        }
        bool* dataPtr = static_cast<bool*>(data);
        return dataPtr[index];
    }

    std::string getString(int64_t index) const {
        tdb_assert(type == DataType::getString(), "Column type is not STRING");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if (isNull(index)) {
            return "";
        }
        // STRING is stored as fixed-size char[256] per element
        const char* dataPtr = static_cast<const char*>(data);
        const char* strPtr = dataPtr + (index * 256);
        // Find null terminator or take up to 256 chars
        size_t len = 0;
        while (len < 256 && strPtr[len] != '\0') {
            ++len;
        }
        return std::string(strPtr, len);
    }

    std::string getValueAsString(int64_t index) const {
        if (isNull(index)) {
            return "NULL";
        }

        switch (type.getType()) {
            case DataType::Type::INT32:
                return std::to_string(getInt32(index));
            case DataType::Type::INT64:
                return std::to_string(getInt64(index));
            case DataType::Type::DOUBLE:
                return std::to_string(getDouble(index));
            case DataType::Type::BOOL:
                return getBool(index) ? "true" : "false";
            case DataType::Type::STRING:
                return "'" + getString(index) + "'";
            case DataType::Type::NULL_CONST:
                return "NULL";
            default:
                return "UNKNOWN";
        }
    }

    std::string toPrettyString(int64_t maxValues = 10) const {
        if (count == 0) {
            return columnId.getName() + " (" + type.toString() + "): [empty]";
        }

        std::string result = columnId.getName() + " (" + type.toString() + "): [";

        // Limit output to first maxValues values
        int64_t maxDisplay = count > maxValues ? maxValues : count;
        for (int64_t i = 0; i < maxDisplay; ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += getValueAsString(i);
        }

        if (count > maxDisplay) {
            result += ", ... (" + std::to_string(count - maxDisplay) + " more)";
        }

        result += "]";
        return result;
    }
};

class RowVectorBuffer {
    std::vector<ColumnBuffer> columns_;
    std::unordered_map<ColumnId, int64_t, ColumnIdHash> columnIdToIndex_;
    int64_t rowCount_;

    public:
    int64_t getRowCount() const noexcept {
        return rowCount_;
    }

    int64_t getColumnCount() const noexcept {
        return static_cast<int64_t>(columns_.size());
    }

    const std::vector<ColumnBuffer>& getColumns() const noexcept {
        return columns_;
    }

    const ColumnBuffer& getColumn(int64_t index) const {
        tdb_assert(index >= 0 && static_cast<size_t>(index) < columns.size(),
            "Tried accessing non existing column: " + std::to_string(index));
        return columns_[static_cast<size_t>(index)];
    }

    ColumnBuffer& getColumn(int64_t index) {
        tdb_assert(index >= 0 && static_cast<size_t>(index) < columns.size(),
            "Tried accessing non existing column: " + std::to_string(index));
        return columns_[static_cast<size_t>(index)];
    }

    const ColumnBuffer& getColumnById(const ColumnId& colId) const {
        auto it = columnIdToIndex_.find(colId);
        tdb_assert(it != columnIdToIndex.end(),
            "Tried accessing non existing column: " + std::to_string(colId.getId()));
        return columns_[static_cast<size_t>(it->second)];
    }

    int64_t getColumnIndex(const ColumnId& colId) const {
        auto it = columnIdToIndex_.find(colId);
        if (it != columnIdToIndex_.end()) {
            return it->second;
        }
        return -1;
    }

    void setRowCount(int64_t count) noexcept {
        rowCount_ = count;
    }

    void addColumn(const ColumnBuffer& col) {
        int64_t index = static_cast<int64_t>(columns_.size());
        columns_.push_back(col);
        columnIdToIndex_[col.columnId] = index;
        if (rowCount_ == 0) {
            rowCount_ = col.count;
        }
    }

    void addOrReplaceColumn(const ColumnBuffer& col) {
        int64_t index = getColumnIndex(col.columnId);
        if (index != -1) {
            columns_[index] = col;
        } else {
            addColumn(col);
        }
    }

    /**
     * @brief Convert the row vector buffer to a pretty string representation in table format
     * @param maxRows Maximum number of rows to display (default: 20). Set to -1 for all rows.
     */
    std::string toPrettyString(int64_t maxRows = 20) const {
        if (columns_.empty() || rowCount_ == 0) {
            return "[empty buffer]";
        }

        // Calculate column widths for alignment
        std::vector<size_t> colWidths;
        for (const auto& col : columns_) {
            size_t width = col.columnId.getName().length();
            colWidths.push_back(width);
        }

        // Determine how many rows to display
        bool truncated = false;
        int64_t displayRows = rowCount_;
        if (maxRows >= 0 && rowCount_ > maxRows) {
            displayRows = maxRows;
            truncated = true;
        }

        // First pass: calculate maximum width for each column
        for (int64_t row = 0; row < displayRows; ++row) {
            for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
                std::string valueStr = columns_[colIdx].getValueAsString(row);
                if (valueStr.length() > colWidths[colIdx]) {
                    colWidths[colIdx] = valueStr.length();
                }
            }
        }

        std::string result;

        // Print header
        result += "+";
        for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
            result += std::string(colWidths[colIdx] + 2, '-');
            result += "+";
        }
        result += "\n";

        result += "|";
        for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
            std::string name = columns_[colIdx].columnId.getName();
            result += " " + name;
            result += std::string(colWidths[colIdx] - name.length() + 1, ' ');
            result += "|";
        }
        result += "\n";

        result += "+";
        for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
            result += std::string(colWidths[colIdx] + 2, '-');
            result += "+";
        }
        result += "\n";

        // Print rows
        for (int64_t row = 0; row < displayRows; ++row) {
            result += "|";
            for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
                std::string valueStr = columns_[colIdx].getValueAsString(row);
                result += " " + valueStr;
                result += std::string(colWidths[colIdx] - valueStr.length() + 1, ' ');
                result += "|";
            }
            result += "\n";
        }

        // Print truncation indicator if needed
        if (truncated) {
            result += "+";
            for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
                result += std::string(colWidths[colIdx] + 2, '-');
                result += "+";
            }
            result += "\n";

            std::string truncMsg = "... (" + std::to_string(rowCount_ - maxRows) + " more rows)";
            // Ensure message fits in first column width
            if (truncMsg.length() > colWidths[0]) {
                truncMsg = "... (" + std::to_string(rowCount_ - maxRows) + " more)";
                if (truncMsg.length() > colWidths[0]) {
                    truncMsg = "...";
                }
            }

            result += "|";
            result += " " + truncMsg;
            result += std::string(colWidths[0] - truncMsg.length() + 1, ' ');
            result += "|";

            // Fill remaining columns with spaces
            for (size_t colIdx = 1; colIdx < columns_.size(); ++colIdx) {
                result += std::string(colWidths[colIdx] + 2, ' ');
                result += "|";
            }
            result += "\n";
        }

        // Print footer
        result += "+";
        for (size_t colIdx = 0; colIdx < columns_.size(); ++colIdx) {
            result += std::string(colWidths[colIdx] + 2, '-');
            result += "+";
        }

        return result;
    }
};

class PhysicalOperator {
    public:
    virtual void initialize() = 0;
    virtual int64_t next(RowVectorBuffer& out) = 0;
    virtual ~PhysicalOperator() {};
};

} // namespace toydb
