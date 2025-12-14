#pragma once

#include <memory>
#include <optional>
#include <string>
#include "parser/query_ast.hpp"
#include "planner/logical_operator.hpp"
#include "storage/catalog.hpp"

namespace toydb {

// TODO: Create proper catalog using libarrow
class PlaceholderCatalog {
   public:
    virtual ~PlaceholderCatalog() = default;

    virtual std::optional<TableMeta> getTable(const std::string& name) = 0;

    /**
     * @brief Resolve a column name to a ColumnId
     * @param tableName Table name (empty if not qualified)
     * @param columnName Column name
     * @return ColumnId if found, nullopt otherwise
     */
    virtual std::optional<ColumnId> resolveColumn(const std::string& tableName,
                                                  const std::string& columnName) = 0;
};

/**
 * @brief Executes the query given as the AST
 */
class SQLInterpreter {
   private:
    PlaceholderCatalog* catalog_;

    ColumnId resolveColumnName(const std::string& columnName, const std::string& tableName);

    std::unique_ptr<PredicateExpr> lowerConstant(const ast::Constant* constant);

    std::unique_ptr<PredicateExpr> lowerPredicate(const ast::Expression* expr, const std::string& tableName);

    std::unique_ptr<PredicateExpr> lowerCondition(const ast::Condition* condition, const std::string& tableName);

   public:
    explicit SQLInterpreter(PlaceholderCatalog* catalog) : catalog_(catalog) {}

    /**
     * @brief Interpret an AST and convert it to a logical query plan
     * @param ast The query AST to interpret
     * @return LogicalQueryPlan if successful, nullopt on error
     */
    std::optional<LogicalQueryPlan> interpret(const ast::QueryAST& ast);

    // Handlers for different types of queries

    // For now, only handleSelectFrom is implemented
    LogicalQueryPlan handleSelectFrom(const ast::SelectFrom& selectFrom);

    LogicalQueryPlan handleCreateTable([[maybe_unused]] const ast::CreateTable& createTable) {
        throw std::runtime_error("CREATE TABLE not yet implemented");
    }

    LogicalQueryPlan handleInsert([[maybe_unused]] const ast::Insert& insert) {
        throw std::runtime_error("INSERT not yet implemented");
    }

    LogicalQueryPlan handleUpdate([[maybe_unused]] const ast::Update& update) {
        throw std::runtime_error("UPDATE not yet implemented");
    }

    LogicalQueryPlan handleDelete([[maybe_unused]] const ast::Delete& deleteStmt) {
        throw std::runtime_error("DELETE not yet implemented");
    }
};

}  // namespace toydb
