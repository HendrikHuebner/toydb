#pragma once

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include "common/assert.hpp"

namespace toydb {

namespace ast {

enum class Operator { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual std::ostream& print(std::ostream&) const noexcept = 0;
    friend std::ostream& operator<<(std::ostream&, const ASTNode&);
};

struct QueryAST {
    explicit QueryAST(ASTNode* query) : query_(query) {}
    std::unique_ptr<ASTNode> query_;
    friend std::ostream& operator<<(std::ostream& os, const QueryAST&);
};


struct Column : public ASTNode {
    std::string name;
    std::string alias;

    Column(std::string name) noexcept : name(std::move(name)) {}
    Column(std::string name, std::string alias) noexcept : name(std::move(name)), alias(std::move(alias)) {}
    
    std::ostream& print(std::ostream&) const noexcept override;
};

struct Table : public ASTNode {
    std::string name;
    std::string alias;

    Table(std::string name) noexcept : name(std::move(name)) {}
    Table(std::string name, std::string alias) noexcept : name(std::move(name)), alias(std::move(alias)) {}

    std::ostream& print(std::ostream&) const noexcept override;
};


struct Expression : public ASTNode {};

struct Literal : public Expression {
    std::string value;

    Literal(std::string value) noexcept : value(std::move(value)) {}    

    std::ostream& print(std::ostream&) const noexcept override;
};

struct Condition : public Expression {
    Operator op;
    std::unique_ptr<Expression> left, right;
 
    bool isUnop() const {
        return right == nullptr;
    }

    std::ostream& print(std::ostream&) const noexcept override;

private:
    static std::string getOperatorString(Operator op) noexcept {
        switch (op) {
            case Operator::EQUAL: return "=";
            case Operator::NOT_EQUAL: return "!=";
            case Operator::GREATER: return ">";
            case Operator::LESS: return "<";
            case Operator::GREATER_EQUAL: return ">=";
            case Operator::LESS_EQUAL: return "<=";
            case Operator::AND: return "AND";
            case Operator::OR: return "OR";
            case Operator::NOT: return "NOT";
            default: return "";
        }
    }
};

struct TableExpression : public ASTNode {
    Table table;
    std::unique_ptr<TableExpression> join;
    std::unique_ptr<Expression> condition;

    TableExpression(Table table) noexcept : table(table) {}
    TableExpression(Table table, std::unique_ptr<TableExpression> join) noexcept : table(table), join(std::move(join)) {}

    std::ostream& print(std::ostream&) const noexcept override;
};


struct Select : public ASTNode {

    std::vector<Column> columns;
    std::vector<TableExpression> tables;
    std::unique_ptr<Expression> where;
    std::optional<Column> orderBy;
    bool distinct = false;

    std::ostream& print(std::ostream&) const noexcept override;
};

std::ostream& operator<<(std::ostream& os, const ASTNode& node);

std::ostream& operator<<(std::ostream& os, const QueryAST& ast);

} // namespace ast
} // namespace toydb
