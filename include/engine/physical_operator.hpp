#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/assert.hpp"
#include "common/types.hpp"

namespace toydb {

class NullBitmap {
public:
    NullBitmap() : bitmap_(nullptr) {}
    explicit NullBitmap(uint8_t* bitmap) : bitmap_(bitmap) {}

    bool isNull(int64_t index) const noexcept {
        if (!bitmap_) return false;
        return (bitmap_[index / 8] & (1 << (index % 8))) == 0;
    }

    void setNull(int64_t index) noexcept {
        if (bitmap_) {
            bitmap_[index / 8] &= ~(1 << (index % 8));
        }
    }

    void clearNull(int64_t index) noexcept {
        if (bitmap_) {
            bitmap_[index / 8] |= (1 << (index % 8));
        }
    }

    void setAllNull(int64_t count) noexcept {
        if (bitmap_) {
            std::memset(bitmap_, 0, (count + 7) / 8);
        }
    }

    void clearAllNull(int64_t count) noexcept {
        if (bitmap_) {
            std::memset(bitmap_, 0xFF, (count + 7) / 8);
        }
    }

    uint8_t* data() noexcept { return bitmap_; }
    const uint8_t* data() const noexcept { return bitmap_; }

private:
    uint8_t* bitmap_;
};

class ColumnBuffer {
   public:
    ColumnBuffer() : columnId{}, type{}, count{0}, data_{nullptr}, nullBitmap_{}, capacity_{0} {}

    ColumnBuffer(ColumnId colId, DataType type, void* data, int64_t capacity, uint8_t* nullBitmap = nullptr)
        : columnId{std::move(colId)}, type{type}, count{0}, data_{data}, nullBitmap_{nullBitmap}, capacity_{capacity} {}

    static int64_t calculateCapacity(size_t dataSize, DataType type) noexcept {
        int32_t typeSize = type.getSize();
        if (typeSize == 0) return 0;
        return static_cast<int64_t>(dataSize / static_cast<size_t>(typeSize));
    }

    static size_t calculateDataSize(int64_t capacity, DataType type) noexcept {
        return static_cast<size_t>(capacity) * static_cast<size_t>(type.getSize());
    }

    ColumnId columnId;
    DataType type;
    int64_t count; // number of rows in the column

    int64_t getCapacity() const noexcept {
        return capacity_;
    }

    bool isNull(int64_t index) const noexcept {
        return nullBitmap_.isNull(index);
    }

    void setNull(int64_t index) noexcept {
        nullBitmap_.setNull(index);
    }

    void clearNull(int64_t index) noexcept {
        nullBitmap_.clearNull(index);
    }

    template<is_db_type T>
    std::span<T> getDataAs() const {
        tdb_assert(type == getDataTypeFor<T>(), "Column type mismatch");
        if constexpr (std::same_as<T, db_string>) {
            // For string arrays, we need to handle char[256] specially
            // Return span of first element, caller must handle stride manually
            tdb_unreachable("getDataAs<db_string> not supported, use getEntryAt instead");
        } else {
            T* dataPtr = static_cast<T*>(data_);
            return std::span<T>(dataPtr, static_cast<size_t>(capacity_));
        }
    }

    template<is_db_type T>
    const T& getEntry(int64_t index) const {
        tdb_assert(type == getDataTypeFor<T>(), "Column type mismatch");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if constexpr (std::same_as<T, db_string>) {
            const char* dataPtr = static_cast<const char*>(data_);
            return *reinterpret_cast<const db_string*>(dataPtr + (index * 256));
        } else {
            const T* dataPtr = static_cast<const T*>(data_);
            return dataPtr[index];
        }
    }

    template<is_db_type T>
    T& getEntry(int64_t index) {
        tdb_assert(type == getDataTypeFor<T>(), "Column type mismatch");
        tdb_assert(index >= 0 && index < count, "Index out of range");
        if constexpr (std::same_as<T, db_string>) {
            char* dataPtr = static_cast<char*>(data_);
            return *reinterpret_cast<db_string*>(dataPtr + (index * sizeof(db_string)));
        } else {
            T* dataPtr = static_cast<T*>(data_);
            return dataPtr[index];
        }
    }

    template<is_db_type T>
    void writeEntry(int64_t index, const T& value) {
        tdb_assert(type == getDataTypeFor<T>(), "Column type mismatch");
        tdb_assert(index >= 0 && index < capacity_, "Index out of range");
        if constexpr (std::same_as<T, db_string>) {
            char* dataPtr = static_cast<char*>(data_);
            char* strPtr = dataPtr + (index * sizeof(db_string));
            std::memcpy(strPtr, value, sizeof(db_string));
        } else {
            T* dataPtr = static_cast<T*>(data_);
            dataPtr[index] = value;
        }
        if (index >= count) {
            count = index + 1;
        }
    }

    std::string getValueAsString(int64_t index) const {
        if (isNull(index)) {
            return "NULL";
        }

        switch (type.getType()) {
            case DataType::Type::INT32:
                return std::to_string(getEntry<db_int32>(index));
            case DataType::Type::INT64:
                return std::to_string(getEntry<db_int64>(index));
            case DataType::Type::DOUBLE:
                return std::to_string(getEntry<db_double>(index));
            case DataType::Type::BOOL:
                return getEntry<db_bool>(index) ? "true" : "false";
            case DataType::Type::STRING:
                return "'" + std::string(getEntry<db_string>(index)) + "'";
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

private:
    void* data_;
    NullBitmap nullBitmap_;
    int64_t capacity_;
};

class RowVector {
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
        tdb_assert(index >= 0 && static_cast<size_t>(index) < columns_.size(),
            "Tried accessing non existing column: " + std::to_string(index));
        return columns_[static_cast<size_t>(index)];
    }

    ColumnBuffer& getColumn(int64_t index) {
        tdb_assert(index >= 0 && static_cast<size_t>(index) < columns_.size(),
            "Tried accessing non existing column: " + std::to_string(index));
        return columns_[static_cast<size_t>(index)];
    }

    const ColumnBuffer& getColumnById(const ColumnId& colId) const {
        auto it = columnIdToIndex_.find(colId);
        tdb_assert(it != columnIdToIndex_.end(),
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
    virtual int64_t next(RowVector& out) = 0;
    virtual ~PhysicalOperator() {};
};

} // namespace toydb
