#pragma once

#include <filesystem>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace toydb {

namespace fs = std::filesystem;
using json = nlohmann::json;

struct ColumnMeta {
    std::string name;
    std::string type;
    bool nullable = true;

    json to_json() const { return json{{"name", name}, {"type", type}, {"nullable", nullable}}; }

    static ColumnMeta from_json(const json& obj) {
        return ColumnMeta{obj.at("name").get<std::string>(), obj.at("type").get<std::string>(),
                          obj.value("nullable", true)};
    }
};

struct FileEntry {
    std::string path;
    std::optional<int64_t> row_count;

    json to_json() const {
        json obj;
        obj["path"] = path;
        if (row_count)
            obj["row_count"] = *row_count;
        return obj;
    }

    static FileEntry from_json(const json& j) {
        FileEntry fileEntry;
        fileEntry.path = j.at("path").get<std::string>();
        if (j.contains("row_count"))
            fileEntry.row_count = j.at("row_count").get<int64_t>();
        return fileEntry;
    }
};

struct TableMeta {
    std::string name;
    std::string id;
    std::string format;
    std::vector<ColumnMeta> schema;
    std::vector<FileEntry> files;

    json to_json() const;

    static TableMeta from_json(const json& obj);
};

class Catalog {
   public:
    explicit Catalog(fs::path catalog_file_path)
        : catalog_path(std::move(catalog_file_path)),
          lock_path(catalog_path.string() + ".lock") {
        loadOrCreate();
    }

    // Non-copyable
    Catalog(const Catalog&) = delete;
    Catalog& operator=(const Catalog&) = delete;

    // List table names
    std::vector<std::string> listTables();

    // Get table metadata (returns nullopt if not found)
    std::optional<TableMeta> getTable(const std::string& name);

    // Create (register) a table. If exists, returns false.
    bool createTable(const TableMeta& meta);

    // Drop table; optionally remove files from disk when remove_files=true
    bool dropTable(const std::string& name, bool remove_files = false);

    // Add files to an existing table (idempotent if identical path exists)
    bool addFiles(const std::string& tableName, const std::vector<FileEntry>& newFiles);

    bool discoverDirectoryAsTable(const std::string& table_name, const fs::path& dir,
                                     const std::string& format = "parquet");

    // Update table schema (e.g., after an ALTER or after inspecting a parquet file)
    bool updateSchema(const std::string& table_name, const std::vector<ColumnMeta>& schema);

    // Reload from disk (in case other process updated). Returns true on success.
    bool reload();

   private:
    fs::path catalog_path;
    fs::path lock_path;
    std::mutex mutex;
    std::unordered_map<std::string, TableMeta> tables;

    static std::string make_id(const std::string& name);

    // Persist atomically: write tmp and rename
    void persist_atomic();

    bool loadOrCreate();
};

}  // namespace toydb
