#include "storage/table_handle.hpp"
#include "storage/csv_data_file_reader.hpp"
#include "common/logging.hpp"

namespace toydb {

TableHandle::~TableHandle() = default;

std::unique_ptr<DataFileReader> TableHandle::createFileReader(const std::filesystem::path& filePath) {
    Schema schema;
    for (const auto& colMeta : schema_) {
        std::hash<std::string> colHasher;
        uint64_t colIdValue = static_cast<uint64_t>(colHasher(colMeta.name));
        ColumnId colId(colIdValue, colMeta.name, table_id_);
        schema.columns[colId] = colMeta;
    }

    switch (format_) {
        case StorageFormat::CSV:
            return std::make_unique<CsvDataFileReader>(filePath, schema, table_id_);
        case StorageFormat::PARQUET:
            Logger::error("Parquet format not yet implemented");
            return nullptr;
        default:
            Logger::error("Unknown storage format");
            return nullptr;
    }
}

std::unique_ptr<TableIterator> TableHandle::createIterator(int64_t requestedBatchSize) {
    return std::make_unique<TableIteratorImpl>(this, requestedBatchSize);
}

TableIteratorImpl::TableIteratorImpl(TableHandle* handle, int64_t batchSize)
    : handle_(handle), batch_size_(batchSize), current_file_index_(0), initialized_(false) {}

void TableIteratorImpl::initialize() {
    if (initialized_) {
        return;
    }

    for (const auto& filePath : handle_->getFilePaths()) {
        auto reader = handle_->createFileReader(filePath);
        if (reader) {
            readers_.push_back(std::move(reader));
        }
    }

    initialized_ = true;
}

int64_t TableIteratorImpl::next(RowVector& out) {
    initialize();

    while (current_file_index_ < readers_.size()) {
        auto& reader = readers_[current_file_index_];
        if (!reader->hasMore()) {
            ++current_file_index_;
            continue;
        }

        int64_t rowsRead = reader->readBatch(out, batch_size_);
        if (rowsRead > 0) {
            return rowsRead;
        }

        ++current_file_index_;
    }

    return 0;
}

bool TableIteratorImpl::hasMore() const noexcept {
    if (!initialized_) {
        return !handle_->getFilePaths().empty();
    }

    for (size_t i = current_file_index_; i < readers_.size(); ++i) {
        if (readers_[i]->hasMore()) {
            return true;
        }
    }

    return false;
}

void TableIteratorImpl::reset() {
    for (auto& reader : readers_) {
        if (reader) {
            reader->reset();
        }
    }
    current_file_index_ = 0;
}

}  // namespace toydb
