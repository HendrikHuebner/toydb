#pragma once

#include <expected>
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

enum class CatalogError {
    TABLE_NOT_FOUND,
    COLUMN_NOT_FOUND,
    INVALID_COLUMN_ID
};

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

class Schema {
    std::vector<ColumnId> columnIds;
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;

    public:

    Schema() noexcept = default;
    Schema(std::vector<ColumnId> columnIds, std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById)
        : columnIds(std::move(columnIds)), columnsById(std::move(columnsById)) {}

    std::vector<ColumnId> getColumnIds() const noexcept { return columnIds; }

    /**
     * @brief Get column metadata by ColumnId
     * @return ColumnMetadata on success, CatalogError::COLUMN_NOT_FOUND on failure
     */
    std::expected<ColumnMetadata, CatalogError> getColumn(const ColumnId& colId) const noexcept;

    std::optional<ColumnMetadata> getColumnByName(const std::string& name) const noexcept;

    void addColumn(const ColumnId& colId, const ColumnMetadata& colMeta) noexcept {
        columnIds.push_back(colId);
        columnsById[colId] = colMeta;
    }
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
     * @return Table name on success, CatalogError::TABLE_NOT_FOUND on failure
     */
    virtual std::expected<std::string, CatalogError> getTableName(const TableId& id) const noexcept = 0;

    /**
     * @brief Resolve a column name to ColumnId
     * @return ColumnId on success, CatalogError::TABLE_NOT_FOUND or CatalogError::COLUMN_NOT_FOUND on failure
     */
    virtual std::expected<ColumnId, CatalogError> resolveColumn(const TableId& tableId, const std::string& columnName) const noexcept = 0;

    /**
     * @brief Get the DataType for a column
     * @return DataType on success, CatalogError::COLUMN_NOT_FOUND or CatalogError::TABLE_NOT_FOUND on failure
     */
    virtual std::expected<DataType, CatalogError> getColumnType(const ColumnId& columnId) const noexcept = 0;

    /**
     * @brief Get a TableHandle for reading/writing table data
     * @return TableHandle on success, CatalogError::TABLE_NOT_FOUND on failure
     */
    virtual std::expected<std::unique_ptr<TableHandle>, CatalogError> getTableHandle(const TableId& tableId) noexcept = 0;
};

class CatalogImpl : public Catalog {
public:
    explicit CatalogImpl(std::unique_ptr<CatalogManifest> manifest);

    ~CatalogImpl() override = default;

    std::vector<TableId> listTables() override;

    std::optional<TableId> getTableIdByName(const std::string& name) const noexcept override;

    std::expected<std::string, CatalogError> getTableName(const TableId& id) const noexcept override;

    std::expected<ColumnId, CatalogError> resolveColumn(const TableId& tableId, const std::string& columnName) const noexcept override;

    std::expected<DataType, CatalogError> getColumnType(const ColumnId& columnId) const noexcept override;

    std::expected<std::unique_ptr<TableHandle>, CatalogError> getTableHandle(const TableId& tableId) noexcept override;

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
