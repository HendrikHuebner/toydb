#include "parser/query_ast.hpp"
#include <ostream>

namespace toydb {

namespace ast {


std::ostream& Table::print(std::ostream& os) const noexcept { return os << name; }

std::ostream& TableExpression::print(std::ostream& os) const noexcept { 
    if (join) {
        if (condition) {
            return os << table << " JOIN " << join << " ON " << *condition;
        } else {
            return os << table << " JOIN " << join;
        }
    } else {
        return os << table;
    } 
}

std::ostream& Column::print(std::ostream& os) const noexcept { return os << name; }

std::ostream& Literal::print(std::ostream& os) const  noexcept { return os << value; };

std::ostream& Condition::print(std::ostream& os) const noexcept {
    if (isUnop()) {
        return os << getOperatorString(op) << " (" << left << ")";
    } else {
        return os << "(" << left << " " << getOperatorString(op) << " " << right << ")";
    }
}

std::ostream& Select::print(std::ostream& os) const noexcept {
    debug_assert(columns.size() > 0, "Select node must have at least one column");
    debug_assert(tables.size() > 0, "Select node must have at least one table");

    os << "SELECT ";

    if (distinct)
        os << "DISTINCT ";

    for (size_t i = 0; i < columns.size(); ++i) {
        os << columns[i];
        if (i < columns.size() - 1) 
            os << ", ";
    }

    for (size_t i = 0; i < tables.size(); ++i) {
        os << tables[i];
        if (i < tables.size() - 1) 
            os << ", ";
    }

    if (where) {
        os << *where;
    }

    if (orderBy) {
        os << " ORDER BY " << orderBy.value();
    }

    return os;
}


std::ostream& operator<<(std::ostream& os, const ASTNode& node) {
    return node.print(os);
}

std::ostream& operator<<(std::ostream& os, const QueryAST& ast) {
    return os << ast.query_;
}

} // namespace ast
} // namespace toydb
