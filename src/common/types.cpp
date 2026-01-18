#include "common/types.hpp"
#include "common/assert.hpp"

namespace toydb {

int32_t DataType::getSize() const noexcept {
    switch (type_) {
        case Type::INT32:
            return sizeof(db_int32);
        case Type::INT64:
            return sizeof(db_int64);
        case Type::DOUBLE:
            return sizeof(db_double);
        case Type::BOOL:
            return sizeof(db_bool);
        case Type::STRING:
            return sizeof(db_string);
        default:
            tdb_unreachable("Invalid data type: " + toString());
    }
}

int32_t DataType::getAlign() const noexcept {
    switch (type_) {
        case Type::INT32:
            return alignof(db_int32);
        case Type::INT64:
            return alignof(db_int64);
        case Type::DOUBLE:
            return alignof(db_double);
        case Type::BOOL:
            return alignof(db_bool);
        case Type::STRING:
            return alignof(db_string);
        default:
            tdb_unreachable("Invalid data type: " + toString());
    }
}

std::string DataType::toString() const noexcept {
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

std::optional<DataType> DataType::fromString(const std::string& typeStr) noexcept {
    if (typeStr == "INT32") {
        return DataType::getInt32();
    } else if (typeStr == "INT64") {
        return DataType::getInt64();
    } else if (typeStr == "DOUBLE") {
        return DataType::getDouble();
    } else if (typeStr == "BOOL") {
        return DataType::getBool();
    } else if (typeStr == "STRING") {
        return DataType::getString();
    } else {
        return std::nullopt;
    }
}

}  // namespace toydb
