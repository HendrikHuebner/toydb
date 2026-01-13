#pragma once

#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/types.hpp"

namespace toydb {

namespace fs = std::filesystem;
using json = nlohmann::json;

struct ColumnMetadata {
    std::string name;
    DataType type;
    bool nullable = true;

    static ColumnMetadata from_json(const json& obj);
};

struct FileEntry {
    fs::path path;
    std::optional<int64_t> row_count;

    static FileEntry from_json(const json& obj);
};

struct Schema {
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columns;

    /**
     * @brief Get column metadata by ColumnId
     * @throw std::runtime_error if column not found
     */
    ColumnMetadata getColumn(const ColumnId& colId) const;

    std::optional<ColumnMetadata> getColumnByName(const std::string& name) const noexcept;
};

enum struct StorageFormat { PARQUET, CSV };

std::string storageFormatToString(StorageFormat format) noexcept;

std::optional<StorageFormat> storageFormatFromString(const std::string& s) noexcept;


struct TableMetadata {
    std::string name;
    TableId id;
    StorageFormat format;
    Schema schema;
    std::vector<FileEntry> files;
    std::unordered_map<std::string, ColumnId> column_map;
};

class CatalogManifest {
public:
    virtual ~CatalogManifest() = default;

    /**
     * @brief Load the manifest from disk
     * @return true on success, false on error
     */
    virtual bool load() = 0;

    virtual std::vector<std::string> getTableNames() const = 0;

    virtual std::optional<TableMetadata> getTableMetadata(const std::string& name) const = 0;

    virtual std::optional<TableMetadata> getTableMetadata(const TableId& id) const = 0;

    virtual fs::path getManifestPath() const = 0;
};

class TableHandle;

class Catalog {
public:
    virtual ~Catalog() = default;

    virtual std::vector<TableId> listTables() = 0;

    virtual std::optional<TableId> getTableIdByName(const std::string& name) const noexcept = 0;

    /**
     * @brief Get table name by TableId
     * @throw std::runtime_error if table not found
     */
    virtual std::string getTableName(const TableId& id) const = 0;

    /**
     * @brief Resolve a column name to ColumnId
     * @throw std::runtime_error if table or column not found
     */
    virtual ColumnId resolveColumn(const TableId& tableId, const std::string& columnName) const = 0;

    /**
     * @brief Get the DataType for a column
     * @throw std::runtime_error if column not found
     */
    virtual DataType getColumnType(const ColumnId& columnId) const = 0;

    /**
     * @brief Get a TableHandle for reading/writing table data
     * @throw std::runtime_error if table not found
     */
    virtual std::unique_ptr<TableHandle> getTableHandle(const TableId& tableId) = 0;
};

class CatalogImpl : public Catalog {
public:
    explicit CatalogImpl(std::unique_ptr<CatalogManifest> manifest);

    ~CatalogImpl() override = default;

    std::vector<TableId> listTables() override;

    std::optional<TableId> getTableIdByName(const std::string& name) const noexcept override;

    std::string getTableName(const TableId& id) const override;

    ColumnId resolveColumn(const TableId& tableId, const std::string& columnName) const override;

    DataType getColumnType(const ColumnId& columnId) const override;

    std::unique_ptr<TableHandle> getTableHandle(const TableId& tableId) override;

protected:
    std::unique_ptr<CatalogManifest> manifest_;
    std::unordered_map<std::string, TableId> name_to_table_id_;
    std::unordered_map<TableId, TableMetadata, TableIdHash> tables_by_id_;

    void initialize();
};

class JsonCatalogManifest : public CatalogManifest {
public:
    explicit JsonCatalogManifest(fs::path manifest_path)
        : manifest_path_(std::move(manifest_path)) {}

    bool load() override;

    std::vector<std::string> getTableNames() const override;

    std::optional<TableMetadata> getTableMetadata(const std::string& name) const override;

    std::optional<TableMetadata> getTableMetadata(const TableId& id) const override;

    fs::path getManifestPath() const override { return manifest_path_; }

private:
    fs::path manifest_path_;
    std::unordered_map<std::string, TableMetadata> tables_by_name_;
    std::unordered_map<TableId, TableMetadata, TableIdHash> tables_by_id_;
    bool loaded_ = false;

    bool parseManifest();
};

class JsonCatalog : public CatalogImpl {
public:
    explicit JsonCatalog(fs::path manifestPath)
        : CatalogImpl(std::make_unique<JsonCatalogManifest>(std::move(manifestPath))) {}
};

}  // namespace toydb
