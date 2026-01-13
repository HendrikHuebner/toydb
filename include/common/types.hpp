#pragma once

#include <fmt/base.h>
#include <fmt/format.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <concepts>

namespace toydb {

enum class CompareOp { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

inline std::string toString(CompareOp op) noexcept {
    switch (op) {
        case CompareOp::EQUAL: return "=";
        case CompareOp::NOT_EQUAL: return "!=";
        case CompareOp::GREATER: return ">";
        case CompareOp::LESS: return "<";
        case CompareOp::GREATER_EQUAL: return ">=";
        case CompareOp::LESS_EQUAL: return "<=";
        case CompareOp::AND: return "AND";
        case CompareOp::OR: return "OR";
        case CompareOp::NOT: return "NOT";
        default: return "UNKNOWN";
    }
}

/**
 * @brief Table identifier with a unique ID and human-readable name
 */
class TableId {
private:
    uint64_t id_;
    std::string name_;

public:
    TableId() : id_(0), name_() {}
    TableId(uint64_t id, std::string name) : id_(id), name_(std::move(name)) {}

    uint64_t getId() const noexcept { return id_; }
    const std::string& getName() const noexcept { return name_; }

    bool operator==(const TableId& other) const noexcept { return id_ == other.id_; }
    bool operator!=(const TableId& other) const noexcept { return id_ != other.id_; }
    bool operator<(const TableId& other) const noexcept { return id_ < other.id_; }
};

struct TableIdHash {
    std::size_t operator()(const TableId& tableId) const noexcept {
        return std::hash<uint64_t>{}(tableId.getId());
    }
};

/**
 * @brief Column identifier with a unique ID, human-readable name, and table reference
 */
class ColumnId {
private:
    uint64_t id_;
    std::string name_;
    TableId tableId_;

public:
    ColumnId() : id_(0), name_(), tableId_() {}
    ColumnId(uint64_t id, std::string name, TableId tableId)
        : id_(id), name_(std::move(name)), tableId_(std::move(tableId)) {}
    ColumnId(uint64_t id, std::string name)
        : id_(id), name_(std::move(name)) {}

    uint64_t getId() const noexcept { return id_; }
    const std::string& getName() const noexcept { return name_; }
    const TableId& getTableId() const noexcept { return tableId_; }

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

    bool isIntegral() const noexcept {
        return type_ == Type::INT32 || type_ == Type::INT64 || type_ == Type::BOOL;
    }

    int32_t getSize() const noexcept;

    int32_t getAlign() const noexcept;

    std::string toString() const noexcept;

    static std::optional<DataType> fromString(const std::string& typeStr) noexcept;

    bool operator==(const DataType& other) const noexcept { return type_ == other.type_; }

    bool operator!=(const DataType& other) const noexcept { return type_ != other.type_; }

   private:
    Type type_;
};

using db_int32 = int32_t;
using db_int64 = int64_t;
using db_bool = bool;
using db_double = double;
using db_string = char[256];

template<typename T>
concept is_db_type =
    std::same_as<T, db_int32> ||
    std::same_as<T, db_int64> ||
    std::same_as<T, db_bool> ||
    std::same_as<T, db_double> ||
    std::same_as<T, db_string>;

/**
 * @brief Maps database type to DataType enum value
 */
template<is_db_type T>
constexpr DataType getDataTypeFor() noexcept;

template<>
constexpr DataType getDataTypeFor<db_int32>() noexcept {
    return DataType::getInt32();
}

template<>
constexpr DataType getDataTypeFor<db_int64>() noexcept {
    return DataType::getInt64();
}

template<>
constexpr DataType getDataTypeFor<db_bool>() noexcept {
    return DataType::getBool();
}

template<>
constexpr DataType getDataTypeFor<db_double>() noexcept {
    return DataType::getDouble();
}

template<>
constexpr DataType getDataTypeFor<db_string>() noexcept {
    return DataType::getString();
}
}  // namespace toydb
