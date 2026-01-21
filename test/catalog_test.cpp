#include "storage/catalog.hpp"
#include "storage/csv_data_file_reader.hpp"
#include "engine/physical_operator.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <vector>

using namespace toydb;
namespace fs = std::filesystem;

class CatalogTest : public ::testing::Test {
protected:
    fs::path testDataDir_;
    fs::path manifestPath_;
    fs::path tempDir_;

    void SetUp() override {
        testDataDir_ = fs::path(__FILE__).parent_path() / "data";
        manifestPath_ = testDataDir_ / "tdb_manifest.json";
        tempDir_ = fs::temp_directory_path() / "tdb_tests" / "catalog_test";
        fs::create_directories(tempDir_);
    }

    void TearDown() override {
        // Clean up temp files
        if (fs::exists(tempDir_)) {
            fs::remove_all(tempDir_);
        }
    }

    fs::path createTempManifest(const std::string& content) {
        fs::path path = tempDir_ / "test_manifest.json";
        std::ofstream file(path);
        file << content;
        file.close();
        return path;
    }

    fs::path createTempCSV(const std::string& content) {
        fs::path path = tempDir_ / "test.csv";
        std::ofstream file(path);
        file << content;
        file.close();
        return path;
    }

    std::vector<std::vector<uint8_t>> rowVectorDataStorage_;
    std::vector<std::vector<uint8_t>> rowVectorNullBitmapStorage_;

    RowVector createRowVectorForSchema(const Schema& schema, int64_t capacity) {
        RowVector rowVec;

        // Clear storage for new RowVector
        rowVectorDataStorage_.clear();
        rowVectorNullBitmapStorage_.clear();


       const auto& columnIds = schema.getColumnIds();
        for (const auto& colId : columnIds) {
            const auto& colMeta = schema.getColumn(colId);
            tdb_assert(colMeta, "Column {} not found in schema", colId.getId());
            size_t dataSize = ColumnBuffer::calculateDataSize(capacity, colMeta->type);
            rowVectorDataStorage_.emplace_back(dataSize);
            rowVectorNullBitmapStorage_.emplace_back((capacity + 7) / 8, 0xFF);

            ColumnBuffer colBuf(colId, colMeta->type, rowVectorDataStorage_.back().data(), capacity,
                NullBitmap(rowVectorNullBitmapStorage_.back().data(), capacity));
            rowVec.addColumn(colBuf);
        }

        return rowVec;
    }

    // Helper to verify a column exists in schema with expected properties
    void verifyColumn(const Schema& schema, const std::string& colName,
                     const DataType& expectedType, bool expectedNullable) {
        auto colMetaOpt = schema.getColumnByName(colName);
        ASSERT_TRUE(colMetaOpt.has_value()) << "Column '" << colName << "' not found in schema";
        EXPECT_EQ(colMetaOpt->type, expectedType)
            << "Column '" << colName << "' type mismatch";
        EXPECT_EQ(colMetaOpt->nullable, expectedNullable)
            << "Column '" << colName << "' nullable mismatch";
    }

    // Helper to verify entire table schema
    void verifyTableSchema(const TableMetadata& meta,
                          const std::vector<std::tuple<std::string, DataType, bool>>& expectedColumns) {
        EXPECT_EQ(meta.schema.getColumnIds().size(), expectedColumns.size())
            << "Schema column count mismatch for table '" << meta.name << "'";

        for (const auto& [colName, expectedType, expectedNullable] : expectedColumns) {
            verifyColumn(meta.schema, colName, expectedType, expectedNullable);
        }
    }

    // Helper to verify a single row's data across all columns
    void verifyRowData(const RowVector& rowVec, int64_t rowIndex,
                     const std::vector<std::pair<std::string, std::function<void(const ColumnBuffer&)>>>& columnVerifiers) {
        ASSERT_GE(rowIndex, 0) << "Row index must be non-negative";
        ASSERT_LT(rowIndex, rowVec.getRowCount()) << "Row index out of range";

        for (size_t colIdx = 0; colIdx < columnVerifiers.size(); ++colIdx) {
            const auto& [colName, verifier] = columnVerifiers[colIdx];
            ASSERT_LT(colIdx, static_cast<size_t>(rowVec.getColumnCount()))
                << "Column index " << colIdx << " out of range";

            const ColumnBuffer& col = rowVec.getColumn(static_cast<int64_t>(colIdx));
            EXPECT_EQ(col.columnId.getName(), colName)
                << "Column name mismatch at index " << colIdx;

            verifier(col);
        }
    }

    // Helper to create a verifier function for INT64 column
    std::function<void(const ColumnBuffer&)> makeInt64Verifier(int64_t rowIndex, int64_t expectedValue, bool expectNull = false) {
        return [rowIndex, expectedValue, expectNull](const ColumnBuffer& col) {
            if (expectNull) {
                EXPECT_TRUE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should be NULL";
            } else {
                EXPECT_FALSE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should not be NULL";
                EXPECT_EQ(col.getEntry<db_int64>(rowIndex), expectedValue)
                    << "Row " << rowIndex << " column " << col.columnId.getName() << " value mismatch";
            }
        };
    }

    // Helper to create a verifier function for INT32 column
    std::function<void(const ColumnBuffer&)> makeInt32Verifier(int64_t rowIndex, int32_t expectedValue, bool expectNull = false) {
        return [rowIndex, expectedValue, expectNull](const ColumnBuffer& col) {
            if (expectNull) {
                EXPECT_TRUE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should be NULL";
            } else {
                EXPECT_FALSE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should not be NULL";
                EXPECT_EQ(col.getEntry<db_int32>(rowIndex), expectedValue)
                    << "Row " << rowIndex << " column " << col.columnId.getName() << " value mismatch";
            }
        };
    }

    // Helper to create a verifier function for DOUBLE column
    std::function<void(const ColumnBuffer&)> makeDoubleVerifier(int64_t rowIndex, double expectedValue, bool expectNull = false) {
        return [rowIndex, expectedValue, expectNull](const ColumnBuffer& col) {
            if (expectNull) {
                EXPECT_TRUE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should be NULL";
            } else {
                EXPECT_FALSE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should not be NULL";
                EXPECT_DOUBLE_EQ(col.getEntry<db_double>(rowIndex), expectedValue)
                    << "Row " << rowIndex << " column " << col.columnId.getName() << " value mismatch";
            }
        };
    }

    // Helper to create a verifier function for STRING column
    std::function<void(const ColumnBuffer&)> makeStringVerifier(int64_t rowIndex, const std::string& expectedValue, bool expectNull = false) {
        return [rowIndex, expectedValue, expectNull](const ColumnBuffer& col) {
            if (expectNull) {
                EXPECT_TRUE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should be NULL";
            } else {
                EXPECT_FALSE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should not be NULL";
                EXPECT_STREQ(col.getEntry<db_string>(rowIndex), expectedValue.c_str())
                    << "Row " << rowIndex << " column " << col.columnId.getName() << " value mismatch";
            }
        };
    }

    // Helper to create a verifier function for BOOL column
    std::function<void(const ColumnBuffer&)> makeBoolVerifier(int64_t rowIndex, bool expectedValue, bool expectNull = false) {
        return [rowIndex, expectedValue, expectNull](const ColumnBuffer& col) {
            if (expectNull) {
                EXPECT_TRUE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should be NULL";
            } else {
                EXPECT_FALSE(col.isNull(rowIndex)) << "Row " << rowIndex << " column " << col.columnId.getName() << " should not be NULL";
                EXPECT_EQ(col.getEntry<db_bool>(rowIndex), expectedValue)
                    << "Row " << rowIndex << " column " << col.columnId.getName() << " value mismatch";
            }
        };
    }

    // Helper to build users table schema
    Schema buildUsersSchema() {
        TableId tableId(11699830787864871553ULL, "users");
        uint64_t colId = 1;
        std::vector<ColumnId> columnIds;
        std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;

        ColumnId idCol(colId++, "id", tableId);
        ColumnId nameCol(colId++, "name", tableId);
        ColumnId emailCol(colId++, "email", tableId);
        ColumnId ageCol(colId++, "age", tableId);
        ColumnId cityCol(colId++, "city", tableId);
        ColumnId createdAtCol(colId++, "created_at", tableId);

        columnIds = {idCol, nameCol, emailCol, ageCol, cityCol, createdAtCol};
        columnsById[idCol] = ColumnMetadata{"id", DataType::getInt64(), false};
        columnsById[nameCol] = ColumnMetadata{"name", DataType::getString(), false};
        columnsById[emailCol] = ColumnMetadata{"email", DataType::getString(), false};
        columnsById[ageCol] = ColumnMetadata{"age", DataType::getInt32(), false};
        columnsById[cityCol] = ColumnMetadata{"city", DataType::getString(), true};
        columnsById[createdAtCol] = ColumnMetadata{"created_at", DataType::getString(), false};

        return Schema(std::move(columnIds), std::move(columnsById));
    }
};

TEST_F(CatalogTest, LoadValidManifest) {
    JsonCatalogManifest manifest(manifestPath_);

    ASSERT_TRUE(manifest.load());

    auto tableNames = manifest.getTableNames();
    ASSERT_EQ(tableNames.size(), 4);

    // Verify users table
    auto usersMeta = manifest.getTableMetadata("users");
    ASSERT_TRUE(usersMeta.has_value());
    EXPECT_EQ(usersMeta->name, "users");
    EXPECT_EQ(usersMeta->format, StorageFormat::CSV);
    EXPECT_EQ(usersMeta->files.size(), 1);
    EXPECT_EQ(usersMeta->files[0].path, "users.csv");
    verifyTableSchema(*usersMeta, {
        {"id", DataType::getInt64(), false},
        {"name", DataType::getString(), false},
        {"email", DataType::getString(), false},
        {"age", DataType::getInt32(), false},
        {"city", DataType::getString(), true},  // nullable
        {"created_at", DataType::getString(), false}
    });

    // Verify products table
    auto productsMeta = manifest.getTableMetadata("products");
    ASSERT_TRUE(productsMeta.has_value());
    EXPECT_EQ(productsMeta->name, "products");
    EXPECT_EQ(productsMeta->format, StorageFormat::CSV);
    verifyTableSchema(*productsMeta, {
        {"id", DataType::getInt64(), false},
        {"name", DataType::getString(), false},
        {"price", DataType::getDouble(), false},
        {"category", DataType::getString(), false},
        {"stock_quantity", DataType::getInt64(), false},
        {"description", DataType::getString(), false},
        {"featured", DataType::getBool(), false}  // boolean column
    });

    // Verify orders table
    auto ordersMeta = manifest.getTableMetadata("orders");
    ASSERT_TRUE(ordersMeta.has_value());
    EXPECT_EQ(ordersMeta->name, "orders");
    EXPECT_EQ(ordersMeta->format, StorageFormat::CSV);
    verifyTableSchema(*ordersMeta, {
        {"id", DataType::getInt64(), false},
        {"user_id", DataType::getInt64(), false},
        {"order_date", DataType::getString(), false},
        {"total_amount", DataType::getDouble(), false},
        {"status", DataType::getString(), false},
        {"shipping_address", DataType::getString(), false},
        {"discount_percent", DataType::getInt32(), true}  // nullable int column
    });

    // Verify order_items table
    auto orderItemsMeta = manifest.getTableMetadata("order_items");
    ASSERT_TRUE(orderItemsMeta.has_value());
    EXPECT_EQ(orderItemsMeta->name, "order_items");
    EXPECT_EQ(orderItemsMeta->format, StorageFormat::CSV);
    verifyTableSchema(*orderItemsMeta, {
        {"id", DataType::getInt64(), false},
        {"order_id", DataType::getInt64(), false},
        {"product_id", DataType::getInt64(), false},
        {"quantity", DataType::getInt64(), false},
        {"unit_price", DataType::getDouble(), false},
        {"subtotal", DataType::getDouble(), false}
    });
}

// Test invalid manifest error handling
TEST_F(CatalogTest, InvalidManifestErrors) {
    // Test non-existent file
    JsonCatalogManifest manifest1(tempDir_ / "nonexistent.json");
    EXPECT_FALSE(manifest1.load());

    // Test invalid JSON
    fs::path invalidJson = createTempManifest("{ invalid json }");
    JsonCatalogManifest manifest2(invalidJson);
    EXPECT_FALSE(manifest2.load());

    // Test missing "tables" field
    fs::path noTables = createTempManifest("{\"other\": \"data\"}");
    JsonCatalogManifest manifest3(noTables);
    EXPECT_FALSE(manifest3.load());
}

TEST_F(CatalogTest, CsvReaderBasicReading) {
    fs::path csvPath = testDataDir_ / "users.csv";
    Schema schema = buildUsersSchema();
    TableId tableId(11699830787864871553ULL, "users");

    CsvDataFileReader reader(csvPath, schema, tableId);

    // Use batch size 5 to ensure we get 2 batches (10 rows total)
    const int64_t batchSize = 5;
    RowVector rowVec = createRowVectorForSchema(schema, batchSize);

    // Read first batch (rows 0-4)
    int64_t rowsRead1 = reader.readBatch(rowVec, batchSize);
    EXPECT_EQ(rowsRead1, 5);
    EXPECT_EQ(rowVec.getRowCount(), 5);
    EXPECT_TRUE(reader.hasMore());

    // Verify first row of first batch (row 0: Alice Johnson)
    verifyRowData(rowVec, 0, {
        {"id", makeInt64Verifier(0, 1)},
        {"name", makeStringVerifier(0, "Alice Johnson")},
        {"email", makeStringVerifier(0, "alice.johnson@email.com")},
        {"age", makeInt32Verifier(0, 28)},
        {"city", makeStringVerifier(0, "New York")},
        {"created_at", makeStringVerifier(0, "2023-01-15")}
    });

    // Verify last row of first batch (row 4: Edward Norton)
    verifyRowData(rowVec, 4, {
        {"id", makeInt64Verifier(4, 5)},
        {"name", makeStringVerifier(4, "Edward Norton")},
        {"email", makeStringVerifier(4, "edward.norton@email.com")},
        {"age", makeInt32Verifier(4, 31)},
        {"city", makeStringVerifier(4, "Boston")},
        {"created_at", makeStringVerifier(4, "2023-05-12")}
    });

    // Read second batch (rows 5-9)
    RowVector rowVec2 = createRowVectorForSchema(schema, batchSize);
    int64_t rowsRead2 = reader.readBatch(rowVec2, batchSize);
    EXPECT_EQ(rowsRead2, 5);
    EXPECT_EQ(rowVec2.getRowCount(), 5);
    EXPECT_FALSE(reader.hasMore());

    // Verify first row of second batch (row 5: Fiona Apple)
    verifyRowData(rowVec2, 0, {
        {"id", makeInt64Verifier(0, 6)},
        {"name", makeStringVerifier(0, "Fiona Apple")},
        {"email", makeStringVerifier(0, "fiona.apple@email.com")},
        {"age", makeInt32Verifier(0, 26)},
        {"city", makeStringVerifier(0, "San Francisco")},
        {"created_at", makeStringVerifier(0, "2023-06-18")}
    });

    // Verify last row of second batch (row 9: Jane Doe)
    verifyRowData(rowVec2, 4, {
        {"id", makeInt64Verifier(4, 10)},
        {"name", makeStringVerifier(4, "Jane Doe")},
        {"email", makeStringVerifier(4, "jane.doe@email.com")},
        {"age", makeInt32Verifier(4, 33)},
        {"city", makeStringVerifier(4, "Portland")},
        {"created_at", makeStringVerifier(4, "2023-10-08")}
    });

    // Verify nullable city column - row 2 in second batch (index 2, which is row 7 overall: Hannah Montana) should be NULL
    const ColumnBuffer& cityCol = rowVec2.getColumn(4);
    EXPECT_TRUE(cityCol.isNull(2));   // Hannah Montana has NULL city (row 7 overall, index 2 in batch)
    EXPECT_FALSE(cityCol.isNull(0));  // Fiona has city
    EXPECT_FALSE(cityCol.isNull(4));  // Jane has city
}

// Test CSV reader with products table (includes boolean column)
TEST_F(CatalogTest, CsvReaderWithBooleanColumn) {
    fs::path csvPath = testDataDir_ / "products.csv";

    TableId tableId(14573828066597895305ULL, "products");
    uint64_t colId = 1;

    std::vector<ColumnId> columnIds;
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;

    ColumnId idCol(colId++, "id", tableId);
    ColumnId nameCol(colId++, "name", tableId);
    ColumnId priceCol(colId++, "price", tableId);
    ColumnId categoryCol(colId++, "category", tableId);
    ColumnId stockQtyCol(colId++, "stock_quantity", tableId);
    ColumnId descCol(colId++, "description", tableId);
    ColumnId featuredCol(colId++, "featured", tableId);

    columnIds = {idCol, nameCol, priceCol, categoryCol, stockQtyCol, descCol, featuredCol};
    columnsById[idCol] = ColumnMetadata{"id", DataType::getInt64(), false};
    columnsById[nameCol] = ColumnMetadata{"name", DataType::getString(), false};
    columnsById[priceCol] = ColumnMetadata{"price", DataType::getDouble(), false};
    columnsById[categoryCol] = ColumnMetadata{"category", DataType::getString(), false};
    columnsById[stockQtyCol] = ColumnMetadata{"stock_quantity", DataType::getInt64(), false};
    columnsById[descCol] = ColumnMetadata{"description", DataType::getString(), false};
    columnsById[featuredCol] = ColumnMetadata{"featured", DataType::getBool(), false};

    Schema schema(std::move(columnIds), std::move(columnsById));

    CsvDataFileReader reader(csvPath, schema, tableId);

    RowVector rowVec = createRowVectorForSchema(schema, 20);

    int64_t rowsRead = reader.readBatch(rowVec, 20);
    EXPECT_EQ(rowsRead, 10);

    // Verify boolean column parsing
    const ColumnBuffer& featuredColBuf = rowVec.getColumn(6);
    EXPECT_TRUE(featuredColBuf.getEntry<db_bool>(0));   // First product is featured
    EXPECT_FALSE(featuredColBuf.getEntry<db_bool>(1));  // Second product is not featured
    EXPECT_TRUE(featuredColBuf.getEntry<db_bool>(2));   // Third product is featured
}

// Test CSV reader with orders table (includes nullable int column)
TEST_F(CatalogTest, CsvReaderWithNullableIntColumn) {
    fs::path csvPath = testDataDir_ / "orders.csv";

    TableId tableId(14579454068846827673ULL, "orders");
    uint64_t colId = 1;

    std::vector<ColumnId> columnIds;
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;

    ColumnId idCol(colId++, "id", tableId);
    ColumnId userIdCol(colId++, "user_id", tableId);
    ColumnId orderDateCol(colId++, "order_date", tableId);
    ColumnId totalAmountCol(colId++, "total_amount", tableId);
    ColumnId statusCol(colId++, "status", tableId);
    ColumnId shippingAddrCol(colId++, "shipping_address", tableId);
    ColumnId discountColId(colId++, "discount_percent", tableId);

    columnIds = {idCol, userIdCol, orderDateCol, totalAmountCol, statusCol, shippingAddrCol, discountColId};
    columnsById[idCol] = ColumnMetadata{"id", DataType::getInt64(), false};
    columnsById[userIdCol] = ColumnMetadata{"user_id", DataType::getInt64(), false};
    columnsById[orderDateCol] = ColumnMetadata{"order_date", DataType::getString(), false};
    columnsById[totalAmountCol] = ColumnMetadata{"total_amount", DataType::getDouble(), false};
    columnsById[statusCol] = ColumnMetadata{"status", DataType::getString(), false};
    columnsById[shippingAddrCol] = ColumnMetadata{"shipping_address", DataType::getString(), false};
    columnsById[discountColId] = ColumnMetadata{"discount_percent", DataType::getInt32(), true};

    Schema schema(std::move(columnIds), std::move(columnsById));

    CsvDataFileReader reader(csvPath, schema, tableId);

    RowVector rowVec = createRowVectorForSchema(schema, 20);

    int64_t rowsRead = reader.readBatch(rowVec, 20);
    EXPECT_EQ(rowsRead, 10);

    // Verify nullable int column
    const ColumnBuffer& discountColBuf = rowVec.getColumn(6);
    EXPECT_FALSE(discountColBuf.isNull(0));  // First order has discount
    EXPECT_EQ(discountColBuf.getEntry<db_int32>(0), 10);

    EXPECT_TRUE(discountColBuf.isNull(1));   // Second order has NULL discount
    EXPECT_TRUE(discountColBuf.isNull(3));    // Fourth order has NULL discount

    EXPECT_FALSE(discountColBuf.isNull(6));  // Seventh order has discount (0)
    EXPECT_EQ(discountColBuf.getEntry<db_int32>(6), 0);
}

// Test CSV reader reset functionality
TEST_F(CatalogTest, CsvReaderReset) {
    fs::path csvPath = testDataDir_ / "users.csv";
    Schema schema = buildUsersSchema();
    TableId tableId(11699830787864871553ULL, "users");

    CsvDataFileReader reader(csvPath, schema, tableId);

    RowVector rowVec1 = createRowVectorForSchema(schema, 20);
    int64_t rowsRead1 = reader.readBatch(rowVec1, 20);
    EXPECT_EQ(rowsRead1, 10);
    EXPECT_FALSE(reader.hasMore());

    // Reset and read again
    reader.reset();
    EXPECT_TRUE(reader.hasMore());

    RowVector rowVec2 = createRowVectorForSchema(schema, 20);
    int64_t rowsRead2 = reader.readBatch(rowVec2, 20);
    EXPECT_EQ(rowsRead2, 10);

    // Verify same data
    EXPECT_EQ(rowVec1.getColumn(0).getEntry<db_int64>(0), rowVec2.getColumn(0).getEntry<db_int64>(0));
}

// Test CSV reader batch reading
TEST_F(CatalogTest, CsvReaderBatchReading) {
    fs::path csvPath = testDataDir_ / "users.csv";
    Schema schema = buildUsersSchema();
    TableId tableId(11699830787864871553ULL, "users");

    CsvDataFileReader reader(csvPath, schema, tableId);

    // Read in small batches
    RowVector rowVec = createRowVectorForSchema(schema, 5);

    int64_t totalRows = 0;
    int64_t batchCount = 0;

    while (reader.hasMore()) {
        int64_t rowsRead = reader.readBatch(rowVec, 3);
        if (rowsRead > 0) {
            totalRows += rowsRead;
            batchCount++;
        } else {
            break;
        }
    }

    EXPECT_EQ(totalRows, 10);
    EXPECT_GT(batchCount, 1);  // Should require multiple batches
}

TEST_F(CatalogTest, EmptyManifest) {
    fs::path emptyManifest = createTempManifest("{}");
    JsonCatalogManifest manifest(emptyManifest);
    EXPECT_FALSE(manifest.load());
}

TEST_F(CatalogTest, EmptyCSV) {
    fs::path emptyCsv = createTempCSV("id,name\n");

    TableId tableId(1, "test");
    ColumnId idCol(1, "id", tableId);
    ColumnId nameCol(2, "name", tableId);

    std::vector<ColumnId> columnIds = {idCol, nameCol};
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;
    columnsById[idCol] = ColumnMetadata{"id", DataType::getInt64(), false};
    columnsById[nameCol] = ColumnMetadata{"name", DataType::getString(), false};

    Schema schema(std::move(columnIds), std::move(columnsById));

    CsvDataFileReader reader(emptyCsv, schema, tableId);

    RowVector rowVec = createRowVectorForSchema(schema, 10);
    int64_t rowsRead = reader.readBatch(rowVec, 10);

    EXPECT_EQ(rowsRead, 0);
    EXPECT_FALSE(reader.hasMore());
}

TEST_F(CatalogTest, NonExistentCSV) {
    fs::path nonexistent = tempDir_ / "nonexistent.csv";

    TableId tableId(1, "test");
    ColumnId idCol(1, "id", tableId);

    std::vector<ColumnId> columnIds = {idCol};
    std::unordered_map<ColumnId, ColumnMetadata, ColumnIdHash> columnsById;
    columnsById[idCol] = ColumnMetadata{"id", DataType::getInt64(), false};

    Schema schema(std::move(columnIds), std::move(columnsById));

    CsvDataFileReader reader(nonexistent, schema, tableId);

    EXPECT_FALSE(reader.hasMore());

    RowVector rowVec = createRowVectorForSchema(schema, 10);
    int64_t rowsRead = reader.readBatch(rowVec, 10);
    EXPECT_EQ(rowsRead, 0);
}
