#include "parser/query_ast.hpp"
#include <ostream>
#include "common/assert.hpp"

namespace toydb {

namespace ast {

std::ostream& Table::print(std::ostream& os) const noexcept {
    return os << name;
}

std::ostream& TableExpr::print(std::ostream& os) const noexcept {
    if (join) {
        if (condition) {
            return os << table << " JOIN " << *join << " ON " << *condition;
        } else {
            return os << table << " JOIN " << *join;
        }
    } else {
        return os << table;
    }
}

std::ostream& ColumnRef::print(std::ostream& os) const noexcept {
    if (!table.empty()) {
        os << table << ".";
    }
    os << name;
    if (!alias.empty()) {
        os << " AS " << alias;
    }
    return os;
}

std::ostream& ConstantInt::print(std::ostream& os) const noexcept {
    return os << value;
}

std::ostream& ConstantDouble::print(std::ostream& os) const noexcept {
    return os << value;
}

std::ostream& ConstantString::print(std::ostream& os) const noexcept {
    return os << "'" << value << "'";
}

std::ostream& ConstantNull::print(std::ostream& os) const noexcept {
    return os << "NULL";
}

std::ostream& ConstantBool::print(std::ostream& os) const noexcept {
    return os << (value ? "TRUE" : "FALSE");
}

std::ostream& Condition::print(std::ostream& os) const noexcept {
    if (isUnop()) {
        return os << getOperatorString(op) << " (" << *left << ")";
    } else {
        return os << "(" << *left << " " << getOperatorString(op) << " " << *right << ")";
    }
}

std::ostream& SelectFrom::print(std::ostream& os) const noexcept {
    tdb_assert(selectAll || columns.size() > 0, "Select node must select at least one column.");
    tdb_assert(tables.size() > 0, "Select node must have at least one table");

    os << "SELECT ";

    if (distinct)
        os << "DISTINCT ";

    if (selectAll) {
        os << "*";
    } else {
        for (size_t i = 0; i < columns.size(); ++i) {
            os << columns[i];
            if (i < columns.size() - 1)
                os << ", ";
        }
    }

    os << " FROM ";

    for (size_t i = 0; i < tables.size(); ++i) {
        os << tables[i];
        if (i < tables.size() - 1)
            os << ", ";
    }

    if (where) {
        os << " WHERE " << *where;
    }

    if (orderBy) {
        os << " ORDER BY " << orderBy.value();
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, const ASTNode& node) {
    return node.print(os);
}

std::string getDataTypeString(DataType type) noexcept {
    switch (type.getType()) {
        case DataType::INT32:
            return "INT32";
        case DataType::STRING:
            return "STRING";
        case DataType::BOOL:
            return "BOOL";
        default:
            return "UNKNOWN";
    }
}

std::ostream& ColumnDefinition::print(std::ostream& os) const noexcept {
    return os << name << " " << getDataTypeString(type);
}

std::ostream& CreateTable::print(std::ostream& os) const noexcept {
    os << "CREATE TABLE " << tableName << " (";
    for (size_t i = 0; i < columns.size(); ++i) {
        os << columns[i];
        if (i < columns.size() - 1)
            os << ", ";
    }
    return os << ")";
}

std::ostream& Insert::print(std::ostream& os) const noexcept {
    os << "INSERT INTO " << tableName;
    if (!columnNames.empty()) {
        os << " (";
        for (size_t i = 0; i < columnNames.size(); ++i) {
            os << columnNames[i];
            if (i < columnNames.size() - 1)
                os << ", ";
        }
        os << ")";
    }
    os << " VALUES ";
    for (size_t i = 0; i < values.size(); ++i) {
        os << "(";
        for (size_t j = 0; j < values[i].size(); ++j) {
            os << *values[i][j];
            if (j < values[i].size() - 1)
                os << ", ";
        }
        os << ")";
        if (i < values.size() - 1)
            os << ", ";
    }
    return os;
}

std::ostream& Update::print(std::ostream& os) const noexcept {
    os << "UPDATE " << tableName << " SET ";
    for (size_t i = 0; i < assignments.size(); ++i) {
        os << assignments[i].first << " = " << *assignments[i].second;
        if (i < assignments.size() - 1)
            os << ", ";
    }
    if (where)
        os << " WHERE " << *where;
    return os;
}

std::ostream& Delete::print(std::ostream& os) const noexcept {
    os << "DELETE FROM " << tableName;
    if (where)
        os << " WHERE " << *where;
    return os;
}

std::ostream& operator<<(std::ostream& os, const QueryAST& ast) {
    return os << ast.query_;
}

}  // namespace ast
}  // namespace toydb
