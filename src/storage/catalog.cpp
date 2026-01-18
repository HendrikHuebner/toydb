#include "storage/catalog.hpp"
#include "common/errors.hpp"
#include "storage/table_handle.hpp"
#include <fstream>
#include "common/assert.hpp"
#include "common/logging.hpp"

namespace toydb {

std::string storageFormatToString(StorageFormat format) noexcept {
    switch (format) {
        case StorageFormat::PARQUET:
            return "parquet";
        case StorageFormat::CSV:
            return "csv";
        default:
            return "unknown";
    }
}

std::optional<StorageFormat> storageFormatFromString(const std::string& s) noexcept {
    if (s == "parquet") {
        return StorageFormat::PARQUET;
    } else if (s == "csv") {
        return StorageFormat::CSV;
    } else {
        return std::nullopt;
    }
}

ColumnMetadata ColumnMetadata::from_json(const json& obj) {
    ColumnMetadata meta;
    meta.name = obj.at("name").get<std::string>();
    std::string typeStr = obj.at("type").get<std::string>();
    auto typeOpt = DataType::fromString(typeStr);
    if (!typeOpt) {
        Logger::error("Failed to parse type: {}", typeStr);
        throw nlohmann::json::type_error::create(0, "Invalid type in ColumnMeta: " + typeStr, &obj);
    }
    meta.type = *typeOpt;
    meta.nullable = obj.value("nullable", true);
    return meta;
}

FileEntry FileEntry::from_json(const json& j) {
    FileEntry entry;
    entry.path = j.at("path").get<std::string>();
    if (j.contains("row_count")) {
        entry.row_count = j.at("row_count").get<int64_t>();
    }
    return entry;
}

std::expected<ColumnMetadata, CatalogError> Schema::getColumn(const ColumnId& colId) const noexcept {
    auto it = columnsById.find(colId);
    if (it != columnsById.end()) {
        return it->second;
    }
    return std::unexpected(CatalogError::COLUMN_NOT_FOUND);
}

std::optional<ColumnMetadata> Schema::getColumnByName(const std::string& name) const noexcept {
    for (const auto& [colId, colMeta] : columnsById) {
        if (colMeta.name == name) {
            return colMeta;
        }
    }
    return std::nullopt;
}

CatalogImpl::CatalogImpl(std::unique_ptr<CatalogManifest> manifest) : manifest_(std::move(manifest)) {
    if (!manifest_->load()) {
        Logger::error("Failed to load catalog manifest");
        return;
    }
    initialize();
}

void CatalogImpl::initialize() {
    name_to_table_id_.clear();
    tables_by_id_.clear();

    auto tableNames = manifest_->getTableNames();
    for (const auto& name : tableNames) {
        auto metaOpt = manifest_->getTableMetadata(name);
        if (!metaOpt) {
            continue;
        }

        TableMetadata& meta = *metaOpt;
        name_to_table_id_[name] = meta.id;
        tables_by_id_[meta.id] = std::move(meta);
    }
}

std::vector<TableId> CatalogImpl::listTables() {
    std::vector<TableId> tables;
    tables.reserve(name_to_table_id_.size());
    for (const auto& [_, tableId] : name_to_table_id_) {
        tables.push_back(tableId);
    }
    return tables;
}

std::optional<TableId> CatalogImpl::getTableIdByName(const std::string& name) const noexcept {
    auto it = name_to_table_id_.find(name);
    if (it != name_to_table_id_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::expected<std::string, CatalogError> CatalogImpl::getTableName(const TableId& id) const noexcept {
    auto it = tables_by_id_.find(id);
    if (it == tables_by_id_.end()) {
        return std::unexpected(CatalogError::TABLE_NOT_FOUND);
    }
    return it->second.name;
}

std::expected<ColumnId, CatalogError> CatalogImpl::resolveColumn(const TableId& tableId, const std::string& columnName) const noexcept {
    auto tableIt = tables_by_id_.find(tableId);
    if (tableIt == tables_by_id_.end()) {
        return std::unexpected(CatalogError::TABLE_NOT_FOUND);
    }

    const auto& meta = tableIt->second;
    auto colIt = meta.column_map.find(columnName);
    if (colIt != meta.column_map.end()) {
        return colIt->second;
    }

    return std::unexpected(CatalogError::COLUMN_NOT_FOUND);
}

std::expected<DataType, CatalogError> CatalogImpl::getColumnType(const ColumnId& columnId) const noexcept {
    auto tableIt = tables_by_id_.find(columnId.getTableId());
    if (tableIt == tables_by_id_.end()) {
        return std::unexpected(CatalogError::TABLE_NOT_FOUND);
    }

    const auto& meta = tableIt->second;
    auto colResult = meta.schema.getColumn(columnId);
    if (!colResult.has_value()) {
        return std::unexpected(colResult.error());
    }
    return colResult->type;
}

std::expected<std::unique_ptr<TableHandle>, CatalogError> CatalogImpl::getTableHandle(const TableId& tableId) noexcept {
    auto it = tables_by_id_.find(tableId);
    if (it == tables_by_id_.end()) {
        return std::unexpected(CatalogError::TABLE_NOT_FOUND);
    }

    const auto& meta = it->second;
    std::vector<fs::path> filePaths;
    filePaths.reserve(meta.files.size());

    fs::path manifestPath = manifest_->getManifestPath();
    fs::path baseDir = manifestPath.parent_path();
    if (baseDir.empty() || !fs::exists(baseDir)) {
        baseDir = fs::current_path();
    }

    for (const auto& fileEntry : meta.files) {
        fs::path filePath = baseDir / fileEntry.path;
        filePaths.push_back(filePath);
    }

    std::vector<ColumnMetadata> columns;
    const auto& columnIds = meta.schema.getColumnIds();
    columns.reserve(columnIds.size());
    for (const auto& colId : columnIds) {
        auto colResult = meta.schema.getColumn(colId);
        if (!colResult)
            return std::unexpected(colResult.error());
        columns.push_back(*colResult);
    }

    return std::make_unique<TableHandle>(meta.id, meta.format, columns, filePaths);
}

bool JsonCatalogManifest::load() {
    if (loaded_) {
        return true;
    }

    if (!fs::exists(manifest_path_)) {
        Logger::error("Manifest file does not exist: {}", manifest_path_.string());
        return false;
    }

    return parseManifest();
}

bool JsonCatalogManifest::parseManifest() {
    std::ifstream ifs(manifest_path_);
    if (!ifs) {
        Logger::error("Failed to open manifest file: {}", manifest_path_.string());
        return false;
    }

    json root;
    try {
        ifs >> root;
    } catch (const std::exception& e) {
        Logger::error("Error parsing manifest JSON: {}", e.what());
        return false;
    }

    if (!root.contains("tables")) {
        Logger::error("Manifest missing 'tables' field");
        return false;
    }

    tables_by_name_.clear();
    tables_by_id_.clear();

    try {
        for (const auto& tableJson : root.at("tables")) {
            TableMetadata meta;
            meta.name = tableJson.at("name").get<std::string>();

            uint64_t idValue = tableJson.at("id").get<uint64_t>();
            std::string idName = tableJson.at("id_name").get<std::string>();
            meta.id = TableId{idValue, idName};

            std::string formatStr = tableJson.at("format").get<std::string>();
            auto formatOpt = storageFormatFromString(formatStr);
            if (!formatOpt) {
                Logger::error("Invalid format in manifest: {}", formatStr);
                return false;
            }
            meta.format = *formatOpt;

            if (tableJson.contains("schema")) {
                uint64_t nextColumnId = 1;
                std::vector<ColumnId> columnIds;
                std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;

                for (const auto& colJson : tableJson.at("schema")) {
                    ColumnMetadata colMeta = ColumnMetadata::from_json(colJson);
                    ColumnId colId(nextColumnId++, colMeta.name, meta.id);
                    columnIds.push_back(colId);
                    columnsById[colId] = colMeta;
                    meta.column_map[colMeta.name] = colId;
                }

                meta.schema = Schema(std::move(columnIds), std::move(columnsById));
            }

            if (tableJson.contains("files")) {
                for (const auto& fileJson : tableJson.at("files")) {
                    meta.files.push_back(FileEntry::from_json(fileJson));
                }
            }

            tables_by_name_[meta.name] = meta;
            tables_by_id_[meta.id] = meta;
        }
    } catch (const std::exception& e) {
        Logger::error("Error parsing table metadata: {}", e.what());
        return false;
    }

    loaded_ = true;
    return true;
}

std::vector<std::string> JsonCatalogManifest::getTableNames() const {
    std::vector<std::string> names;
    names.reserve(tables_by_name_.size());
    for (const auto& [name, _] : tables_by_name_) {
        names.push_back(name);
    }
    return names;
}

std::optional<TableMetadata> JsonCatalogManifest::getTableMetadata(const std::string& name) const {
    auto it = tables_by_name_.find(name);
    if (it != tables_by_name_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<TableMetadata> JsonCatalogManifest::getTableMetadata(const TableId& id) const {
    auto it = tables_by_id_.find(id);
    if (it != tables_by_id_.end()) {
        return it->second;
    }
    return std::nullopt;
}

}  // namespace toydb
