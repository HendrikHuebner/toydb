#pragma once

#include <string>
#include <vector>
#include <memory>
#include <iostream>

namespace toydb {

namespace ast {

enum class Operator { EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, AND, OR, NOT };

enum class DataType { INT, FLOAT, STRING, BOOL };

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

    Column(const std::string& name) noexcept : name(name) {}
    Column(const std::string& name, const std::string& alias) noexcept : name(name), alias(alias) {}
    
    std::ostream& print(std::ostream&) const noexcept override;
};

struct Table : public ASTNode {
    std::string name;
    std::string alias;

    Table(const std::string& name) noexcept : name(name) {}
    Table(const std::string& name, const std::string& alias) noexcept : name(name), alias(alias) {}

    std::ostream& print(std::ostream&) const noexcept override;
};


struct Expression : public ASTNode {};

struct Literal : public Expression {
    std::string value;

    Literal(const std::string& value) noexcept : value(value) {}    

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

    TableExpression(const Table& table) noexcept : table(table) {}
    TableExpression(const Table& table, std::unique_ptr<TableExpression> join) noexcept : table(table), join(std::move(join)) {}

    std::ostream& print(std::ostream&) const noexcept override;
};


struct ColumnDefinition : public ASTNode {
    std::string name;
    DataType type;

    ColumnDefinition(const std::string& name, DataType type) noexcept 
        : name(name), type(std::move(type)) {}

    std::ostream& print(std::ostream&) const noexcept override;
};

struct CreateTable : public ASTNode {
    std::string tableName;
    std::vector<ColumnDefinition> columns;

    CreateTable(const std::string& tableName) noexcept : tableName(tableName) {}

    std::ostream& print(std::ostream&) const noexcept override;
};

struct Insert : public ASTNode {
    std::string tableName;
    std::vector<std::string> columnNames;
    std::vector<std::vector<std::unique_ptr<Expression>>> values;

    Insert(const std::string& tableName) noexcept : tableName(tableName) {}

    std::ostream& print(std::ostream&) const noexcept override;
};

struct Update : public ASTNode {
    std::string tableName;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments;
    std::unique_ptr<Expression> where;

    Update(const std::string& tableName) noexcept : tableName(tableName) {}

    std::ostream& print(std::ostream&) const noexcept override;
};

struct Delete : public ASTNode {
    std::string tableName;
    std::unique_ptr<Expression> where;

    Delete(const std::string& tableName) noexcept : tableName(tableName) {}

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
