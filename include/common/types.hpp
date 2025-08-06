#pragma once

#include <cstddef>

namespace toydb {

enum class Operator { 
    EQUAL, 
    NOT_EQUAL, 
    GREATER, 
    LESS, 
    GREATER_EQUAL, 
    LESS_EQUAL, 
    AND, 
    OR, 
    NOT 
};

enum class Type : char { 
    INT,    // unsigned 4 byte int
    STRING, // 256 character fixed length string
    BOOL    // one byte boolean
};

inline std::size_t getSize(Type type) {
    switch (type) {
        case Type::INT: return 4;
        case Type::BOOL: return 1;
        case Type::STRING: return 256;
    }
}

} // namespace toydb
