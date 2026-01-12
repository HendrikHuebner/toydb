#include "planner/interpreter.hpp"
#include "engine/predicate_expr.hpp"
#include "common/logging.hpp"
#include <stdexcept>
#include <cctype>

namespace toydb {

/**
 * @brief Resolve a column name to a column id by looking it up in the catalog.
 *
 * @throws std::runtime_error if the column is not found
 */
ColumnId SQLInterpreter::resolveColumnName(const std::string& columnName, const std::string& tableName) {
    auto colId = catalog_->resolveColumn(tableName, columnName);
    if (!colId.has_value()) {
        std::string error = "Column '" + columnName + "'";
        if (!tableName.empty()) {
            error += " in table '" + tableName + "'";
        }
        error += " not found";
        throw std::runtime_error(error);
    }
    return *colId;
}

/**
 * @brief Get the DataType for a column by looking it up in the catalog.
 *
 * @throws std::runtime_error if the table or column is not found
 */
static DataType getColumnType(const std::string& columnName, const std::string& tableName, PlaceholderCatalog* catalog) {
    auto tableMeta = catalog->getTable(tableName);
    if (!tableMeta.has_value()) {
        throw std::runtime_error("Table '" + tableName + "' not found");
    }
    for (const auto& colMeta : tableMeta->schema) {
        if (colMeta.name == columnName) {
            if (colMeta.type == "INT64") {
                return DataType::getInt64();
            } else if (colMeta.type == "INT32") {
                return DataType::getInt32();
            } else if (colMeta.type == "DOUBLE") {
                return DataType::getDouble();
            } else if (colMeta.type == "BOOL") {
                return DataType::getBool();
            } else if (colMeta.type == "STRING") {
                return DataType::getString();
            } else {
                throw std::runtime_error("Unknown column type: " + colMeta.type);
            }
        }
    }
    throw std::runtime_error("Column '" + columnName + "' not found in table '" + tableName + "'");
}

std::unique_ptr<PredicateExpr> SQLInterpreter::lowerConstant(const ast::Constant* constant) {
    if (auto* constInt = dynamic_cast<const ast::ConstantInt*>(constant)) {
        DataType type = constInt->isInt64 ? DataType::getInt64() : DataType::getInt32();
        return std::make_unique<ConstantExpr>(type, constInt->value);
    } else if (auto* constDouble = dynamic_cast<const ast::ConstantDouble*>(constant)) {
        return std::make_unique<ConstantExpr>(DataType::getDouble(), constDouble->value);
    } else if (auto* constString = dynamic_cast<const ast::ConstantString*>(constant)) {
        return std::make_unique<ConstantExpr>(DataType::getString(), constString->value);
    } else if (dynamic_cast<const ast::ConstantNull*>(constant)) {
        return std::make_unique<ConstantExpr>(DataType::getNullConst());
    } else if (auto* constBool = dynamic_cast<const ast::ConstantBool*>(constant)) {
        // Booleans are ints of size 1
        return std::make_unique<ConstantExpr>(DataType::getBool(), constBool->value);
    } else {
        throw std::runtime_error("Unknown constant type");
    }
}

// Helper to convert AST Expression to PredicateExpr
std::unique_ptr<PredicateExpr> SQLInterpreter::lowerPredicate(const ast::Expression* expr, const std::string& tableName) {
    if (auto* columnRef = dynamic_cast<const ast::ColumnRef*>(expr)) {
        ColumnId colId = resolveColumnName(columnRef->name, tableName);
        DataType colType = getColumnType(columnRef->name, tableName, catalog_);
        return std::make_unique<ColumnRefExpr>(colId, colType);
    } else if (auto* constant = dynamic_cast<const ast::Constant*>(expr)) {
        if (auto* constString = dynamic_cast<const ast::ConstantString*>(constant)) {
            throw std::runtime_error("Unexpected string literal in predicate: " + constString->value);
        }
        return lowerConstant(constant);
    } else if (auto* condition = dynamic_cast<const ast::Condition*>(expr)) {
        return lowerCondition(condition, tableName);
    } else {
        throw std::runtime_error("Unsupported expression type in WHERE clause");
    }
}

/**
 * Given two DataTypes, determine the type to implicitly convert to when applying a binop
 * to the two operands.
 *
 * Type <op> Type -> Type
 * Int32 <op> Int64 -> Int64
 * IntXY <op> Double -> Double
 * IntXY <op> Boolean -> IntXY
 */
static DataType getCommonType(const DataType& left, const DataType& right) {
    if (left == right) {
        return left;
    } else if (left == DataType::getInt32() && right == DataType::getInt64()) {
        return DataType::getInt64();
    } else if (left == DataType::getInt64() && right == DataType::getInt32()) {
        return DataType::getInt64();
    } else if (left.isIntegral() && right == DataType::getDouble()) {
        return DataType::getDouble();
    } else if (left == DataType::getDouble() && right.isIntegral()) {
        return DataType::getDouble();
    } else if (left == DataType::getBool() && right.isIntegral()) {
        return right;
    } else if (left.isIntegral() && right == DataType::getBool()) {
        return left;
    } else {
        throw std::runtime_error("Unsupported operand types for binary operation");
    }
}

std::unique_ptr<PredicateExpr> SQLInterpreter::lowerCondition(const ast::Condition* condition, const std::string& tableName) {
    if (condition->isUnop()) {
        throw std::runtime_error("unary operator is NYI");
    }

    // Binary operator
    auto left = lowerPredicate(condition->left.get(), tableName);
    auto right = lowerPredicate(condition->right.get(), tableName);

    if (condition->op == CompareOp::AND || condition->op == CompareOp::OR) {
        return std::make_unique<LogicalExpr>(condition->op, std::move(left), std::move(right));
    } else {
        // Comparison operator

        // TODO: Implement a more generic "Expression", with a type, rather than assuming everyting is a predicate.
        // For now, left and right must be a column reference or a constant. Cast operators are inserted if necessary.

        DataType leftType;
        DataType rightType;

        if (auto leftColumnRef = dynamic_cast<ColumnRefExpr*>(left.get())) {
            leftType = leftColumnRef->getType();
        } else if (auto leftConstant = dynamic_cast<ConstantExpr*>(left.get())) {
            leftType = leftConstant->getType();
        } else {
            throw std::runtime_error("Left operand must be a column reference or a constant");
        }

        if (auto rightColumnRef = dynamic_cast<ColumnRefExpr*>(right.get())) {
            rightType = rightColumnRef->getType();
        } else if (auto rightConstant = dynamic_cast<ConstantExpr*>(right.get())) {
            rightType = rightConstant->getType();
        } else {
            throw std::runtime_error("Right operand must be a column reference or a constant");
        }

        DataType compareType = getCommonType(leftType, rightType);

        // Insert cast operators if necessary
        if (leftType != compareType) {
            left = std::make_unique<CastExpr>(compareType, std::move(left));
        }

        if (rightType != compareType) {
            right = std::make_unique<CastExpr>(compareType, std::move(right));
        }

        return std::make_unique<CompareExpr>(condition->op, compareType, std::move(left), std::move(right));
    }
}

std::optional<LogicalQueryPlan> SQLInterpreter::interpret(const ast::QueryAST& ast) {
    if (!ast.query_) {
        Logger::error("Interpretation failed: AST query node is null");
        return std::nullopt;
    }

    // Dispatch based on AST node type
    try {
        if (auto* selectFrom = dynamic_cast<const ast::SelectFrom*>(ast.query_.get())) {
            return handleSelectFrom(*selectFrom);
        } else if (auto* createTable = dynamic_cast<const ast::CreateTable*>(ast.query_.get())) {
            return handleCreateTable(*createTable);
        } else if (auto* insert = dynamic_cast<const ast::Insert*>(ast.query_.get())) {
            return handleInsert(*insert);
        } else if (auto* update = dynamic_cast<const ast::Update*>(ast.query_.get())) {
            return handleUpdate(*update);
        } else if (auto* deleteStmt = dynamic_cast<const ast::Delete*>(ast.query_.get())) {
            return handleDelete(*deleteStmt);
        } else {
            Logger::error("Could not execute query: Unknown AST node type");
            return std::nullopt;
        }
    } catch (const std::exception& e) {
        Logger::error("Could not execute query: {}", e.what());
        throw;
    } catch (...) {
        Logger::error("Could not execute query: Unknown exception");
        throw;
    }
}

LogicalQueryPlan SQLInterpreter::handleSelectFrom(const ast::SelectFrom& selectFrom) {
    // Verify we have at least one table
    if (selectFrom.tables.empty()) {
        throw std::runtime_error("SELECT query must have at least one table");
    }

    if (selectFrom.tables.size() > 1) {
        // TODO: Implement joins
        throw std::runtime_error("Multiple tables (joins) not yet supported");
    }

    // Get the table name
    const auto& tableExpr = selectFrom.tables[0];
    std::string tableName = tableExpr.table.name;

    auto tableMeta = catalog_->getTable(tableName);
    if (!tableMeta.has_value()) {
        throw std::runtime_error("Table '" + tableName + "' not found");
    }

    // Create TableScanOp as the base of the query plan
    auto tableScanOp = std::make_shared<TableScanOp>(tableName);
    std::shared_ptr<LogicalOperator> current = tableScanOp;

    // Add filter if WHERE clause exists
    if (selectFrom.where) {
        auto predicate = lowerPredicate(selectFrom.where.get(), tableName);
        auto filterOp = std::make_shared<FilterOp>(std::move(predicate));
        filterOp->addChild(current);
        current = filterOp;
    }

    // Add projection for selected columns
    std::vector<ColumnId> projectionColumns;
    for (const auto& col : selectFrom.columns) {
        try {
            ColumnId colId = resolveColumnName(col.name, tableName);
            projectionColumns.push_back(colId);
        } catch (const std::exception& e) {
            Logger::error("Interpretation failed: {}", e.what());
            throw;
        }
    }

    auto projectionOp = std::make_shared<ProjectionOp>(std::move(projectionColumns));
    projectionOp->addChild(current);

    LogicalQueryPlan plan;
    plan.setRoot(projectionOp);

    return plan;
}


} // namespace toydb


