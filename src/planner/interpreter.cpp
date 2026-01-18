#include "planner/interpreter.hpp"
#include "engine/predicate_expr.hpp"
#include "common/errors.hpp"
#include "common/logging.hpp"
#include "storage/catalog.hpp"
#include <cctype>

namespace toydb {

static QueryContext buildSelectContext(const ast::SelectFrom& selectFrom, PlaceholderCatalog* catalog) {
    QueryContext context;

    for (const auto& tableExpr : selectFrom.tables) {
        const std::string& tableName = tableExpr.table.name;
        const std::string& tableAlias = tableExpr.table.alias;

        auto tableMeta = catalog->getTable(tableName);
        if (!tableMeta.has_value()) {
            throw UnresolvedColumnException("Table '" + tableName + "' not found");
        }

        context.tables[tableName] = *tableMeta;

        if (!tableAlias.empty()) {
            if (context.aliasToTable.find(tableAlias) != context.aliasToTable.end()) {
                throw InternalSQLError("Duplicate alias '" + tableAlias + "'");
            }
            context.aliasToTable[tableAlias] = tableName;
        }

        // TODO: Handle nested joins
    }

    return context;
}

ColumnId SQLInterpreter::resolveColumnRef(const ast::ColumnRef& columnRef, const QueryContext& context) {
    const std::string& columnName = columnRef.name;
    const std::string& tableQualifier = columnRef.table;

    // Qualified reference (table.column or alias.column)
    if (!tableQualifier.empty()) {
        auto actualTableName = context.getCanonicalTableName(tableQualifier);
        if (!actualTableName.has_value()) {
            throw UnresolvedColumnException("Table or alias '" + tableQualifier + "' not found");
        }

        auto columnId = catalog_->resolveColumn(*actualTableName, columnName);
        if (!columnId.has_value()) {
            throw UnresolvedColumnException("Column '" + columnName + "' not found in table '" + *actualTableName + "'");
        }

        return *columnId;
    }

    // Unqualified reference
    std::vector<std::string> matchingTables;

    for (const auto& [tableName, tableMeta] : context.tables) {
        if (tableMeta.schema.getColumnByName(columnName).has_value()) {
            matchingTables.push_back(tableName);
        }
    }

    if (matchingTables.empty()) {
        throw UnresolvedColumnException("Column '" + columnName + "' not found in any available table");
    }

    if (matchingTables.size() > 1) {
        std::string error = "Column '" + columnName + "' is ambiguous: found in tables ";
        for (size_t i = 0; i < matchingTables.size(); ++i) {
            if (i > 0) error += ", ";
            error += matchingTables[i];
        }
        throw UnresolvedColumnException(error);
    }

    // Exactly one match
    const std::string& actualTableName = matchingTables[0];
    auto columnId = catalog_->resolveColumn(actualTableName, columnName);
    if (!columnId.has_value()) {
        throw UnresolvedColumnException("Column '" + columnName + "' not found in table '" + actualTableName + "'");
    }

    return *columnId;
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
        throw InternalSQLError("Unknown constant type");
    }
}

// Helper to convert AST Expression to PredicateExpr
std::unique_ptr<PredicateExpr> SQLInterpreter::lowerPredicate(const ast::Expression* expr, const QueryContext& context) {
    if (auto* columnRef = dynamic_cast<const ast::ColumnRef*>(expr)) {
        ColumnId colId = resolveColumnRef(*columnRef, context);
        auto colType = catalog_->getColumnType(colId);
        return std::make_unique<ColumnRefExpr>(colId, colType);
    } else if (auto* constant = dynamic_cast<const ast::Constant*>(expr)) {
        if (auto* constString = dynamic_cast<const ast::ConstantString*>(constant)) {
            throw UnresolvedColumnException("Unexpected string literal in predicate: " + constString->value);
        }
        return lowerConstant(constant);
    } else if (auto* condition = dynamic_cast<const ast::Condition*>(expr)) {
        return lowerCondition(condition, context);
    } else {
        throw InternalSQLError("Unsupported expression type in WHERE clause");
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
        throw InternalSQLError("Unsupported operand types for binary operation");
    }
}

std::unique_ptr<PredicateExpr> SQLInterpreter::lowerCondition(const ast::Condition* condition, const QueryContext& context) {
    if (condition->isUnop()) {
        throw NotYetImplementedError("unary operator");
    }

    // Binary operator
    auto left = lowerPredicate(condition->left.get(), context);
    auto right = lowerPredicate(condition->right.get(), context);

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
            throw InternalSQLError("Left operand must be a column reference or a constant");
        }

        if (auto rightColumnRef = dynamic_cast<ColumnRefExpr*>(right.get())) {
            rightType = rightColumnRef->getType();
        } else if (auto rightConstant = dynamic_cast<ConstantExpr*>(right.get())) {
            rightType = rightConstant->getType();
        } else {
            throw InternalSQLError("Right operand must be a column reference or a constant");
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
        throw InternalSQLError("SELECT query must have at least one table");
    }

    if (selectFrom.tables.size() > 1) {
        // TODO: Implement joins
        throw NotYetImplementedError("Multiple tables (joins)");
    }

    QueryContext context = buildSelectContext(selectFrom, catalog_);

    // Collect all columns for TableScanOp
    std::vector<ColumnId> scanColumns;
    for (const auto& [_, tableMeta] : context.tables) {
        const auto& columnIds = tableMeta.schema.getColumnIds();
        for (const auto& colId : columnIds) {
            scanColumns.push_back(colId);
        }
    }

    // Create TableScanOp as the base of the query plan
    auto tableScanOp = std::make_shared<TableScanOp>(std::move(scanColumns));
    std::shared_ptr<LogicalOperator> current = tableScanOp;

    // Add filter if WHERE clause exists
    if (selectFrom.where) {
        auto predicate = lowerPredicate(selectFrom.where.get(), context);
        auto filterOp = std::make_shared<FilterOp>(std::move(predicate));
        filterOp->addChild(current);
        current = filterOp;
    }

    if (selectFrom.selectAll) {
        LogicalQueryPlan plan;
        plan.setRoot(current);
        return plan;
    }

    // Add projection for selected columns
    std::vector<ColumnId> projectionColumns;
    for (const auto& col : selectFrom.columns) {
        try {
            ColumnId colId = resolveColumnRef(col, context);
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


