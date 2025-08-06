#pragma once

#include <cstddef>
#include <expected>
#include <filesystem>
#include <optional>
#include "table.hpp"
#include "common/logger.hpp"
#include "common/memory.hpp"

namespace toydb {

namespace memory {


class TableManager {

    using path_t = std::filesystem::path;

    path_t basePath{};
    std::vector<Table> tables;
    const MemoryManager& memoryManager;

    public:
    
    TableManager(path_t& basePath, const MemoryManager& memoryManager) noexcept : basePath(basePath), memoryManager(memoryManager) {
        init();
    }

    void init() noexcept {
        auto paths = discoverTables(basePath);
        tables.reserve(paths.size());

        for (auto& path : paths) {
            auto* dataPtr = memoryManager.mapNewFile(path, );
            Table& table = tables.emplace_back(path);
        }
    }

    static std::vector<path_t> discoverTables(const path_t& basePath) noexcept {
        std::vector<path_t> tablePaths;

        try {
            if (!std::filesystem::exists(basePath) || !std::filesystem::is_directory(basePath)) {
                Logger::error("Base path {} does not exist or is not a directory.", basePath.string());
                return {};
            }

            for (const auto& entry : std::filesystem::recursive_directory_iterator(basePath)) {
                if (entry.is_regular_file() && entry.path().extension() == ".tbl") {
                    tablePaths.push_back(entry.path());
                }
            }
        } catch (const std::filesystem::filesystem_error& e) {
            Logger::error("Error discovering tables: {}", e.what());
            return {};
        }

        return tablePaths;
    }
};

} // namespace plan
} // namespace toydb

