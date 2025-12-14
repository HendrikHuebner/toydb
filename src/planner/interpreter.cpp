#include "planner/interpreter.hpp"
#include "engine/predicate_expr.hpp"
#include <stdexcept>
#include <cctype>

namespace toydb {


// Helper to resolve column name to ColumnId
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

// Helper to convert AST Constant to ConstantExpr
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
    if (auto* constant = dynamic_cast<const ast::Constant*>(expr)) {
        if (auto* constString = dynamic_cast<const ast::ConstantString*>(constant)) {
            throw std::runtime_error("String literal is not a predicate: " + constString->value);
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

// Helper to convert AST Condition to PredicateExpr
std::unique_ptr<PredicateExpr> SQLInterpreter::lowerCondition(const ast::Condition* condition, const std::string& tableName) {
    if (condition->isUnop()) {
        throw std::runtime_error("NYI unary operator");
    }
    
    // Binary operator
    auto left = lowerPredicate(condition->left.get(), tableName);
    auto right = lowerPredicate(condition->right.get(), tableName);
    
    DataType compareType;
    
    // Check if this is a logical operator (AND/OR)
    if (condition->op == CompareOp::AND || condition->op == CompareOp::OR) {
        return std::make_unique<LogicalExpr>(condition->op, std::move(left), std::move(right));
    } else {
        // Comparison operator
        return std::make_unique<CompareExpr>(condition->op, compareType, std::move(left), std::move(right));
    }
}

std::optional<LogicalQueryPlan> SQLInterpreter::interpret(const ast::QueryAST& ast) {
    if (!ast.query_) {
        return std::nullopt;
    }
    
    // Dispatch based on AST node type
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
        return std::nullopt;
    }
}

LogicalQueryPlan SQLInterpreter::handleSelectFrom(const ast::SelectFrom& selectFrom) {
    // Verify we have at least one table
    if (selectFrom.tables.empty()) {
        throw std::runtime_error("SELECT query must have at least one table");
    }
    
    // For now, we only handle single table queries (no joins)
    if (selectFrom.tables.size() > 1) {
        throw std::runtime_error("Multiple tables (joins) not yet supported");
    }
    
    // Get the table name
    const auto& tableExpr = selectFrom.tables[0];
    std::string tableName = tableExpr.table.name;
    
    // Verify table exists
    auto tableMeta = catalog_->getTable(tableName);
    if (!tableMeta.has_value()) {
        throw std::runtime_error("Table '" + tableName + "' not found");
    }
    
    // Start building the logical plan from the bottom up
    // For now, we'll create a simple structure:
    // Table (implicit) -> Filter (if WHERE) -> Projection
    
    std::shared_ptr<LogicalOperator> current;
    
    // Create a placeholder for the table scan
    // For now, we'll use a simple approach: the table is implicit
    // In a real implementation, we'd have a TableScanOp here
    
    // Add filter if WHERE clause exists
    if (selectFrom.where) {
        auto predicate = lowerPredicate(selectFrom.where.get(), tableName);
        auto filterOp = std::make_shared<FilterOp>(std::move(predicate));
        current = filterOp;
    }
    
    // Add projection for selected columns
    std::vector<ColumnId> projectionColumns;
    for (const auto& col : selectFrom.columns) {
        ColumnId colId = resolveColumnName(col.name, tableName);
        projectionColumns.push_back(colId);
    }
    
    auto projectionOp = std::make_shared<ProjectionOp>(std::move(projectionColumns));
    
    if (current) {
        projectionOp->addChild(current);
    }
    
    LogicalQueryPlan plan;
    plan.setRoot(projectionOp);
    
    return plan;
}


} // namespace toydb


