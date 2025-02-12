#pragma once

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include "common/assert.hpp"
#include "common/concepts.hpp"

namespace toydb {

namespace ast {

enum class Operator { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual std::ostream& print(std::ostream&) const = 0;
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

    Column(std::string name) : name(std::move(name)) {}
    Column(std::string name, std::string alias) : name(std::move(name)), alias(std::move(alias)) {}
    
    Column() = default;
    Column(const Column& other) = default;
    Column(Column&& other) = default;
    Column& operator=(const Column& other) = default;
    Column& operator=(Column&& other) = default;
    std::ostream& print(std::ostream&) const override;
};

struct Table : public ASTNode {
    std::string name;
    std::string alias;

    Table(std::string name) : name(std::move(name)) {}
    Table(std::string name, std::string alias) : name(std::move(name)), alias(std::move(alias)) {}

    Table() = default;
    Table(const Table& other) = default;
    Table(Table&& other) = default;
    Table& operator=(const Table& other) = default;
    Table& operator=(Table&& other) = default;
    std::ostream& print(std::ostream&) const override;
};


struct Expression : public ASTNode {};

struct Literal : public Expression {
    std::string value;

    Literal(std::string value) : value(std::move(value)) {}    

    Literal(const Literal& other) = default;
    Literal(Literal&& other) = default;
    Literal& operator=(const Literal& other) = default;
    Literal& operator=(Literal&& other) = default;
    std::ostream& print(std::ostream&) const override;
};

struct Condition : public Expression {
    Operator op;
    std::unique_ptr<Expression> left, right;
 
    bool isUnop() const {
        return right == nullptr;
    }

    std::ostream& print(std::ostream&) const override;

private:
    static std::string getOperatorString(Operator op) {
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

    TableExpression(Table table) : table(table) {}
    TableExpression(Table table, std::unique_ptr<TableExpression> join) : table(table), join(std::move(join)) {}

    std::ostream& print(std::ostream&) const override;
};


struct Select : public ASTNode {

    std::vector<Column> columns;
    std::vector<TableExpression> tables;
    std::unique_ptr<Expression> where;
    std::optional<Column> orderBy;
    bool distinct = false;

    std::ostream& print(std::ostream&) const override;
};

std::ostream& operator<<(std::ostream& os, const ASTNode& node);

std::ostream& operator<<(std::ostream& os, const QueryAST& ast);

} // namespace ast
} // namespace toydb
