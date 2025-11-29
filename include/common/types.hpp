#pragma once

#include <cstddef>

namespace toydb {

enum class CmpOp { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

enum class DataType : char {
    INT,     // unsigned 4 byte int
    STRING,  // 256 character fixed length string
    BOOL     // one byte boolean
};

inline std::size_t getSize(DataType type) noexcept {
    switch (type) {
        case DataType::INT:
            return 4;
        case DataType::BOOL:
            return 1;
        case DataType::STRING:
            return 256;
    }
}

}  // namespace toydb
