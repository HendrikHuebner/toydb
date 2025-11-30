
#pragma once

#include "storage/catalog.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace toydb {

namespace fs = std::filesystem;
using json = nlohmann::json;

std::vector<std::string> Catalog::listTables() {
    std::lock_guard<std::mutex> l(mutex);
    std::vector<std::string> out;
    out.reserve(tables.size());
    for (auto& kv : tables)
        out.push_back(kv.first);
    return out;
}

std::optional<TableMeta> Catalog::getTable(const std::string& name) {
    std::lock_guard<std::mutex> l(mutex);
    auto it = tables.find(name);
    if (it == tables.end())
        return std::nullopt;
    return it->second;
}

bool Catalog::createTable(const TableMeta& meta) {
    std::lock_guard<std::mutex> l(mutex);
    if (tables.count(meta.name))
        return false;
    tables[meta.name] = meta;
    persist_atomic();
    return true;
}

bool Catalog::dropTable(const std::string& name, bool remove_files) {
    std::lock_guard<std::mutex> l(mutex);
    auto it = tables.find(name);
    if (it == tables.end())
        return false;
    if (remove_files) {
        for (auto& f : it->second.files) {
            std::error_code ec;
            fs::remove(f.path, ec);
            // ignore errors for now, but log
            if (ec)
                std::cerr << "Warning: could not remove " << f.path << ": " << ec.message() << "\n";
        }
    }
    tables.erase(it);
    persist_atomic();
    return true;
}

bool Catalog::addFiles(const std::string& tableName, const std::vector<FileEntry>& newFiles) {
    std::lock_guard<std::mutex> l(mutex);
    auto it = tables.find(tableName);
    if (it == tables.end())
        return false;
    auto& files = it->second.files;
    // naive de-dup by path
    std::unordered_map<std::string, bool> present;
    for (auto& f : files)
        present[f.path] = true;
    for (auto& nf : newFiles) {
        if (!present.count(nf.path))
            files.push_back(nf);
    }
    persist_atomic();
    return true;
}

bool Catalog::discoverDirectoryAsTable(const std::string& table_name, const fs::path& dir,
                                       const std::string& format) {
    if (!fs::exists(dir) || !fs::is_directory(dir))
        return false;

    TableMeta meta;
    meta.name = table_name;
    meta.id = make_id(table_name);
    meta.format = format;

    for (auto& p : fs::directory_iterator(dir)) {
        if (!fs::is_regular_file(p))
            continue;
        // simple extension-based filter
        if (format == "parquet" && p.path().extension() == ".parquet") {
            FileEntry f;
            f.path = p.path().string();
            meta.files.push_back(f);
        } else if (format == "csv" && p.path().extension() == ".csv") {
            FileEntry f;
            f.path = p.path().string();
            meta.files.push_back(f);
        }
    }

    std::lock_guard<std::mutex> g(mutex);
    if (tables.count(table_name))
        return false;
    tables[table_name] = meta;
    persist_atomic();
    return true;
}

bool Catalog::updateSchema(const std::string& table_name, const std::vector<ColumnMeta>& schema) {
    std::lock_guard<std::mutex> g(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return false;
    it->second.schema = schema;
    persist_atomic();
    return true;
}

bool Catalog::reload() {
    std::lock_guard<std::mutex> g(mutex);
    return loadOrCreate();
}

void Catalog::persist_atomic() {
    json root;
    root["generated_at"] = getCurrentTimeStamp();
    root["tables"] = json::array();
    for (auto& kv : tables)
        root["tables"].push_back(kv.second.to_json());

    // Lock (blocking)
    Lockfile lf{lock_path};
    lf.lock();

    auto tmp = catalog_path;
    tmp += ".tmp";
    {
        std::ofstream ofs(tmp.string(), std::ios::trunc);
        ofs << root.dump(2);
        ofs.flush();
        ofs.close();
    }
    // Rename (atomic on most OSes)
    std::error_code ec;
    fs::rename(tmp, catalog_path, ec);
    if (ec) {
        std::cerr << "Error writing catalog: " << ec.message() << "\n";
    }
}

bool Catalog::loadOrCreate() {
    if (!fs::exists(catalog_path)) {
        // create empty catalog
        tables.clear();
        persist_atomic();
        return true;
    }
    std::ifstream ifs(catalog_path.string());
    if (!ifs)
        return false;
    json root;
    try {
        ifs >> root;
    } catch (const std::exception& e) {
        std::cerr << "Failed parse of catalog.json: " << e.what() << "\n";
        return false;
    }
    ifs.close();
    tables.clear();
    if (root.contains("tables")) {
        for (auto& tj : root.at("tables")) {
            TableMeta t = TableMeta::from_json(tj);
            tables[t.name] = t;
        }
    }
    return true;
}

json TableMeta::to_json() const {
    json obj;
    obj["name"] = name;
    obj["id"] = id;
    obj["format"] = format;
    obj["schema"] = json::array();
    for (auto& c : schema)
        obj["schema"].push_back(c.to_json());
    obj["files"] = json::array();
    for (auto& f : files)
        obj["files"].push_back(f.to_json());
    return obj;
}

TableMeta TableMeta::from_json(const json& obj) {
    TableMeta table;
    table.name = obj.at("name").get<std::string>();
    table.id = obj.at("id").get<std::string>();
    table.format = obj.at("format").get<std::string>();
    if (obj.contains("schema")) {
        for (auto& cj : obj.at("schema"))
            table.schema.push_back(ColumnMeta::from_json(cj));
    }
    if (obj.contains("files")) {
        for (auto& fj : obj.at("files"))
            table.files.push_back(FileEntry::from_json(fj));
    }
    return table;
}
}  // namespace toydb
