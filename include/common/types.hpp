#pragma once

#include <fmt/base.h>
#include <fmt/format.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include "common/assert.hpp"

namespace toydb {

enum class CompareOp { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

/**
 * @brief Column identifier with a unique ID and human-readable name
 */
class ColumnId {
private:
    uint64_t id_;
    std::string name_;

public:
    ColumnId() : id_(0), name_() {}
    ColumnId(uint64_t id, std::string name) : id_(id), name_(std::move(name)) {}

    uint64_t getId() const noexcept { return id_; }
    const std::string& getName() const noexcept { return name_; }

    bool operator==(const ColumnId& other) const noexcept { return id_ == other.id_; }
    bool operator!=(const ColumnId& other) const noexcept { return id_ != other.id_; }
    bool operator<(const ColumnId& other) const noexcept { return id_ < other.id_; }
};

struct ColumnIdHash {
    std::size_t operator()(const ColumnId& colId) const noexcept {
        return std::hash<uint64_t>{}(colId.getId());
    }
};

class DataType {

   public:
    enum Type { NULL_CONST, INT32, INT64, DOUBLE, BOOL, STRING };

    DataType() : type_(Type::NULL_CONST) {}

    explicit DataType(Type t) : type_(t) {}

    static DataType getNullConst() noexcept { return DataType{Type::NULL_CONST}; }

    static DataType getInt32() noexcept { return DataType{Type::INT32}; }

    static DataType getInt64() noexcept { return DataType{Type::INT64}; }

    static DataType getDouble() noexcept { return DataType{Type::DOUBLE}; }

    static DataType getBool() noexcept { return DataType{Type::BOOL}; }

    static DataType getString() noexcept { return DataType{Type::STRING}; }

    Type getType() const noexcept { return type_; }

    int32_t getSize() const noexcept {
        switch (type_) {
            case Type::INT32:
                return 4;
            case Type::INT64:
                return 8;
            case Type::DOUBLE:
                return 8;
            case Type::BOOL:
                return 1;
            case Type::STRING:
                return 256;
            default:
                tdb_unreachable("Invalid data type: " + toString());
        }
    }

    std::string toString() const noexcept {
        switch (type_) {
            case Type::NULL_CONST:
                return "NULL";
            case Type::INT32:
                return "INT32";
            case Type::INT64:
                return "INT64";
            case Type::DOUBLE:
                return "DOUBLE";
            case Type::BOOL:
                return "BOOL";
            case Type::STRING:
                return "STRING";
            default:
                return "UNKNOWN";
        }
    }

    bool operator==(const DataType& other) const noexcept { return type_ == other.type_; }

    bool operator!=(const DataType& other) const noexcept { return type_ != other.type_; }

   private:
    Type type_;
};

}  // namespace toydb
