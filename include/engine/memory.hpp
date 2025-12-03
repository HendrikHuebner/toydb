#pragma once

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstddef>
#include <expected>
#include <string>
#include <unordered_map>
#include <memory>
#include <vector>

namespace toydb {

namespace memory {

struct Page {
    using ID = unsigned int;

    void* data;
};

enum struct MemoryError { OK = 0, FILE_ERROR, MMAP_FAILED, UNKNOWN_PAGE };

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

    std::expected<void*, MemoryError> mapNewFile(const std::string& name,
                                                 std::size_t size) noexcept {

        FD fd = open(name.c_str(), O_CREAT | O_RDWR);
        if (fd == -1) {
            return std::unexpected(MemoryError::FILE_ERROR);
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1)
            return std::unexpected(MemoryError::FILE_ERROR);

        void* addr = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

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

    std::size_t size;  // number of pages
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
        return result.transform([](void* value) -> Page { return Page{value}; });
    }
};

/**
 * @brief Manages a pool of memory to create temporary RowVectorBuffers
 */
class BufferManager {
public:
    static constexpr std::size_t BUFFER_SIZE = 64 * 1024; // 64KB

    /**
     * @brief RAII handle to a temporary memory buffer
     */
    class BufferHandle {
    private:
        BufferManager* manager_;
        void* buffer_;

        friend class BufferManager;

        BufferHandle(BufferManager* manager, void* buffer)
            : manager_(manager), buffer_(buffer) {}

    public:
        BufferHandle(BufferHandle&& other) noexcept
            : manager_(other.manager_), buffer_(other.buffer_) {
            other.manager_ = nullptr;
            other.buffer_ = nullptr;
        }

        BufferHandle& operator=(BufferHandle&& other) noexcept {
            if (this != &other) {
                release();
                manager_ = other.manager_;
                buffer_ = other.buffer_;
                other.manager_ = nullptr;
                other.buffer_ = nullptr;
            }
            return *this;
        }

        ~BufferHandle() {
            release();
        }

        // Non-copyable
        BufferHandle(const BufferHandle&) = delete;
        BufferHandle& operator=(const BufferHandle&) = delete;

        void* get() const noexcept {
            return buffer_;
        }

        std::size_t size() const noexcept {
            return BUFFER_SIZE;
        }

        void release() {
            if (manager_ && buffer_) {
                manager_->releaseBuffer(buffer_);
                manager_ = nullptr;
                buffer_ = nullptr;
            }
        }
    };

private:
    std::vector<std::unique_ptr<char[]>> pool_;
    std::vector<void*> available_;

    void releaseBuffer(void* buffer) {
        available_.push_back(buffer);
    }

public:
    BufferManager() = default;
    ~BufferManager() = default;

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;

    BufferHandle allocate() {
        // Try to reuse an available buffer first
        if (!available_.empty()) {
            void* buffer = available_.back();
            available_.pop_back();
            return BufferHandle(this, buffer);
        }

        // TODO: Come up with a better strategy that grows
        // and shrinks the pool exponentially, while also preventing OOMs.
        // ...or use a fixed size pool.
        auto buffer = std::make_unique<char[]>(BUFFER_SIZE);
        void* raw_buffer = buffer.get();

        pool_.push_back(std::move(buffer));
        return BufferHandle(this, raw_buffer);
    }

    static constexpr std::size_t getBufferSize() noexcept {
        return BUFFER_SIZE;
    }
};

}  // namespace memory
}  // namespace toydb
