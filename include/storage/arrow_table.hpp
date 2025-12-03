#pragma once

#include "engine/relation.hpp"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"

namespace toydb {

class ArrowTable : public Relation {

};

ArrowTable* readArrowTable(const std::string& path) {

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(
        reader_builder.OpenFile(path, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    ARROW_ASSIGN_OR_RAISE(rb_reader, arrow_reader->GetRecordBatchReader());
}

} // namespace toydb