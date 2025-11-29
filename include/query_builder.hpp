#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <vector>
#include "parser/query_ast.hpp"

namespace toydb {

struct invalid_query : public std::runtime_error {
    invalid_query(const std::string& msg) : std::runtime_error(msg) {}
};

class ConditionBuilder {

    ast::Condition* condition;

    public:
    ConditionBuilder& isGreater([[maybe_unused]] ast::Expression& left, [[maybe_unused]] ast::Expression& right) {
        return *this;
    }

    ConditionBuilder& isEqual() {
        return *this;
    }

    ConditionBuilder& addAnd() {
        return *this;
    }

    ConditionBuilder& addOr() {
        return *this;
    }

    std::unique_ptr<ast::Condition> build() {
        return std::unique_ptr<ast::Condition>(condition);
    }
};

class TableExpressionBuilder {
    ast::TableExpression* tableExpr;

    public:
    TableExpressionBuilder(ast::Table table) : tableExpr(new ast::TableExpression(table)) {}

    TableExpressionBuilder& join(ast::Table table) {
        tableExpr = new ast::TableExpression(table, std::unique_ptr<ast::TableExpression>(tableExpr));
        return *this;
    }

    TableExpressionBuilder& on(std::unique_ptr<ast::Condition> condition) {
        if (!tableExpr->join)
            throw invalid_query("Tried to add join condition before JOIN");

        tableExpr->condition = std::move(condition);
        return *this;
    }

    TableExpressionBuilder& on(ConditionBuilder& cb) {
        if (!tableExpr->join)
            throw invalid_query("Tried to add join condition before JOIN");

        tableExpr->condition = cb.build();
        return *this;
    }

    std::unique_ptr<ast::TableExpression> build() {
        return std::unique_ptr<ast::TableExpression>(tableExpr);
    }
};

class SelectBuilder {

    protected:
    ast::Select* select_;
    
    public:
    SelectBuilder& from(std::initializer_list<ast::Table> tables) {
        select_->tables.insert(select_->tables.begin(), tables.begin(), tables.end());
        return *this;
    }

    SelectBuilder& from(ast::Table table) {
        select_->tables.emplace_back(table);
        return *this;
    }

    SelectBuilder& where(ConditionBuilder& cb) {
        select_->where = cb.build();
        return *this;
    }

    SelectBuilder& where(std::unique_ptr<ast::Condition> condition) {
        select_->where = std::move(condition);
        return *this;
    }

    SelectBuilder orderBy(ast::Column& column) {
        select_->orderBy = column;
        return *this;
    }

    ast::QueryAST build() {
        return ast::QueryAST(select_);
    }
};

class QueryBuilder : SelectBuilder {

    ast::ASTNode* query_ = nullptr;

    public:
    SelectBuilder& select(std::initializer_list<ast::Column> columns) {
        if (query_)
            throw invalid_query("Invalid use of 'select'");

        select_ = new ast::Select();
        select_->columns = std::vector(columns.begin(), columns.end());
        query_ = select_;

        return static_cast<SelectBuilder&>(*this);
    }

    SelectBuilder& select(ast::Column& column) {
        select_->columns = { column };
        return static_cast<SelectBuilder&>(*this);
    }
};

} // namespace toydb
