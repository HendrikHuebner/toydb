#pragma once

#include <cstddef>
#include <expected>
#include <filesystem>
#include <vector>
#include "common/types.hpp"
#include "memory/memory.hpp"

namespace toydb {

namespace memory {

enum struct AccessError : char {
    OK = 0,
    DOES_NOT_EXIST,
    OUT_OF_BOUNDS
};

enum struct IndexType : char {
    NONE = 0,
    BTREE
};

struct Index {

};

struct Row {
    char* data;
};


struct Table {

    using TableID = unsigned int;
    using TableFD = MemoryManager::FD;

    /**
    * Memory layout:
    * <header>
    * <column types>
    * <index if present> 
    * <tuples>
    */
    struct Data {

        struct Header {
            char name[32];
            TableID id;
            std::size_t size;        // number of tuples
            std::size_t columnCount; // number of columns
            IndexType index;
            std::size_t indexSize;
        };

        Header header;
        std::vector<Type> columns;
        Index* index;
        char* data;

        /**
        * @brief Reinterpret memory as Table
        */
        Data(char* data_) : header(*reinterpret_cast<Header*>(data_)) {
            data = data_ + sizeof(header);

            auto columnsPtr = reinterpret_cast<Type*>(data_);
            columns = std::vector<Type>{ columnsPtr, columnsPtr + header.columnCount};
            
            if (header.index != IndexType::NONE) {
                data = data_ + sizeof(Type) * header.columnCount;
                index = reinterpret_cast<Index*>(data_);
            
            } else {
                index = nullptr;
            }

            data = data_ + header.indexSize;
            data = data_;
        }
    };
    
    std::filesystem::path path;
    TableFD fd;
    Data data;
    const PageDirectory& pageCache;
    
    Table(std::filesystem::path& path, TableFD& fd, const PageDirectory& pageCache, char* data)
        : path(path), fd(fd), data(data), pageCache(pageCache) {}


    std::expected<Row, AccessError> getRow(const char* key) const noexcept;

    std::expected<Row, AccessError> getRow(std::size_t index) const noexcept;

    AccessError addRow(Row row) noexcept;
};

} // namespace plan
} // namespace toydb

