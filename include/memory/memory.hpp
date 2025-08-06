#pragma once

#include <cstddef>
#include <expected>
#include <string>
#include <unordered_map>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace toydb {

namespace memory {

struct Page {
    using ID = unsigned int;

    void* data;
};

enum struct MemoryError {
    OK = 0,
    FILE_ERROR,
    MMAP_FAILED,
    UNKNOWN_PAGE
};

struct ManagedRegion {
    void* data;
    std::size_t size;
};

struct MemoryManager {

    using FD = int;
    using Offset = std::size_t;

    std::size_t pageSize{};

    MemoryError init() noexcept {
        pageSize = sysconf(_SC_PAGE_SIZE);
        return MemoryError::OK;
    }

    std::size_t alignToPageSize(std::size_t offset) const noexcept {
        return offset & ~(pageSize - 1);
    }

    std::expected<void*, MemoryError> mapNewFile(const std::string& name, std::size_t size) noexcept {
        
        FD fd = open(name.c_str(), O_CREAT | O_RDWR);
        if (fd == -1) {
            return std::unexpected(MemoryError::FILE_ERROR);
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1)
            return std::unexpected(MemoryError::FILE_ERROR);

        void* addr = mmap(NULL, size, PROT_READ,
                    MAP_PRIVATE, fd, 0);

        if (addr == MAP_FAILED)
            return std::unexpected(MemoryError::MMAP_FAILED);

        return addr;
    }

    std::expected<void*, MemoryError> load(FD fd, Offset offset) const noexcept;
};


struct PageCache {

    using FD = int;

    struct Entry {
        void* page;
        bool pinned;
        bool read;
    };

    std::size_t size; // number of pages
    std::unordered_map<Page::ID, Entry> pageMappings;
    MemoryManager memory;

    Page getPage(Page::ID id) noexcept;
};


struct PageDirectory {
    using Offset = MemoryManager::Offset;
    
    std::unordered_map<Page::ID, Offset> pageMappings;
    MemoryManager& memory;
    MemoryManager::FD fd;

    std::expected<Page, MemoryError> getPage(Page::ID id) noexcept {
        auto it = pageMappings.find(id);

        if (it == pageMappings.end()) {
            return std::unexpected(MemoryError::UNKNOWN_PAGE);
        }

        auto result = memory.load(fd, it->second);
        return result.transform([](void* value) -> Page { 
            return Page{ value }; 
        });
    }
};



} // namespace plan
} // namespace toydb

