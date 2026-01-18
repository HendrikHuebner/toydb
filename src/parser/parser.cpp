#include "parser/parser.hpp"
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "common/assert.hpp"
#include "common/debug.hpp"
#include "common/errors.hpp"
#include "common/logging.hpp"
#include "common/errors.hpp"
#include "parser/lexer.hpp"
#include "parser/query_ast.hpp"

namespace toydb {
namespace parser {

/**
 * Parses an identifier token and returns it.
 * @throws ParserException if next token is not an identifier
 */
Token Parser::parseIdentifier(const std::string& context) {
    auto token = ts.next();
    if (token.type != TokenType::IdentifierType) {
        throw ParserException("Expected " + context + ", but got " + token.toString(),
                              ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
    }
    return token;
}

/**
 * Parses a qualified or unqualified column reference (table.column) and returns the table and column names.
 * @param context Context string for error messages
 * @return Pair of (table name, column name). If unqualified, table name is empty.
 */
std::pair<std::string, std::string> Parser::parseQualifiedColumnRef(const std::string& context) {
    auto token = parseIdentifier(context);
    std::string firstPart = token.getString();

    auto peeked = ts.peek();
    if (peeked.type == TokenType::Dot) {
        ts.next();

        auto afterDot = ts.peek();
        auto secondToken = parseIdentifier(context);

        return {firstPart, secondToken.getString()};
    }

    // Unqualified identifier
    return {"", firstPart};
}

/**
 * Verifies that the next token matches the expected type.
 * @param expected The expected token type
 * @param context Context string for error messages
 * @throws ParserException if token does not match expected type
 */
void Parser::expectToken(TokenType expected, const std::string& context) {
    auto token = ts.peek();
    if (token.type != expected) {
        throw ParserException("Expected " + context + ", but got " + token.toString(),
                              ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
    }
    ts.next();
}

bool isBinop(TokenType type) noexcept {
    switch (type) {
        case TokenType::OpEquals:
        case TokenType::OpNotEquals:
        case TokenType::OpGreaterEq:
        case TokenType::OpGreaterThan:
        case TokenType::OpLessThan:
        case TokenType::OpLessEq:
        case TokenType::OpAnd:
        case TokenType::OpOr:
            return true;
        default:
            return false;
    }
}

/**
 * Get the operator precence of the binop token type.
 * A higher return value means higher precendence.
 */
int getPrecedence(TokenType type) {
    switch (type) {
        case TokenType::OpEquals:
        case TokenType::OpNotEquals:
        case TokenType::OpGreaterEq:
        case TokenType::OpGreaterThan:
        case TokenType::OpLessThan:
        case TokenType::OpLessEq:
            return 3;
        case TokenType::OpAnd:
            return 2;
        case TokenType::OpOr:
            return 1;
        default:
            return 0;
    }
}

/**
 * Parses a SQL expression and returns its AST representation.
 * Handles comparison operators and nested expressions.
 */
std::unique_ptr<ast::Expression> Parser::parseExpression() {
    auto left = parseTerm();
    Token token = ts.peek();

    int prevPrecedence = std::numeric_limits<int>::max();
    int precedence = getPrecedence(token.type);

    while (precedence > 0) {
        ts.next();
        auto right = parseTerm();
        auto condition = std::make_unique<ast::Condition>();

        switch (token.type) {
            case TokenType::OpEquals:
                condition->op = CompareOp::EQUAL;
                break;
            case TokenType::OpNotEquals:
                condition->op = CompareOp::NOT_EQUAL;
                break;
            case TokenType::OpGreaterThan:
                condition->op = CompareOp::GREATER;
                break;
            case TokenType::OpLessThan:
                condition->op = CompareOp::LESS;
                break;
            case TokenType::OpGreaterEq:
                condition->op = CompareOp::GREATER_EQUAL;
                break;
            case TokenType::OpLessEq:
                condition->op = CompareOp::LESS_EQUAL;
                break;
            case TokenType::OpAnd:
                condition->op = CompareOp::AND;
                break;
            case TokenType::OpOr:
                condition->op = CompareOp::OR;
                break;
            default:
                break;
        }

        condition->right = std::move(right);

        if (precedence <= prevPrecedence) {
            condition->left = std::move(left);
            left = std::move(condition);

        } else {
            auto leftCondition = dynamic_cast<ast::Condition*>(left.get());
            tdb_assert(leftCondition, "Left should be binop");

            condition->left = std::move(leftCondition->right);
            leftCondition->right = std::move(condition);
        }

        token = ts.peek();
        prevPrecedence = precedence;
        precedence = getPrecedence(token.type);
    }

    return left;
}

/**
 * Parses a term (identifier or literal) and returns its AST representation.
 */
std::unique_ptr<ast::Expression> Parser::parseTerm() {
    auto token = ts.peek();

    if (token.type == TokenType::IdentifierType) {
        // Parse qualified identifier (table.column or just column)
        auto [table, column] = parseQualifiedColumnRef("column name");
        return std::make_unique<ast::ColumnRef>(table, column, "");
    }

    token = ts.next();

    if (token.type == TokenType::Int32Literal) {
        return std::make_unique<ast::ConstantInt>(token.getInt(), false);

    } else if (token.type == TokenType::Int64Literal) {
        return std::make_unique<ast::ConstantInt>(token.getInt(), true);

    } else if (token.type == TokenType::DoubleLiteral) {
        return std::make_unique<ast::ConstantDouble>(token.getDouble());

    } else if (token.type == TokenType::StringLiteral) {
        return std::make_unique<ast::ConstantString>(token.getString());

    } else if (token.type == TokenType::NullLiteral) {
        return std::make_unique<ast::ConstantNull>();

    } else if (token.type == TokenType::TrueLiteral) {
        return std::make_unique<ast::ConstantBool>(token.getBool());

    } else if (token.type == TokenType::FalseLiteral) {
        return std::make_unique<ast::ConstantBool>(token.getBool());

    } else if (token.type == TokenType::ParenthesisL) {
        auto&& result = parseExpression();
        expectToken(TokenType::ParenthesisR, "closing parenthesis");
        return result;
    }

    throw ParserException("Expected term but got " + token.toString(), ts.getCurrentLineNumber(),
                          ts.getLinePosition(), ts.getQuery());
}

/**
 * Parses a WHERE <Expression> clause and returns its AST representation.
 * @return AST node for WHERE clause or nullptr if no WHERE clause
 */
std::unique_ptr<ast::Expression> Parser::parseWhere() {
    auto token = ts.peek();

    if (token.type != TokenType::KeyWhere) {
        return nullptr;
    }

    ts.next();
    auto expr = parseExpression();
    if (!expr) {
        throw ParserException("Expected expression after WHERE", ts.getCurrentLineNumber(),
                              ts.getLinePosition(), ts.getQuery());
    }
    return expr;
}

/**
 * Parses a SELECT ... FROM ... [WHERE ...] statement and returns its AST representation.
 * @throws ParserException if syntax is invalid
 */
std::unique_ptr<ast::SelectFrom> Parser::parseSelect() {
    getLogger().trace("Parsing SELECT statement");

    expectToken(TokenType::KeySelect, "SELECT statement");

    // TODO: Support DISTINCT keyword
    auto selectFrom = std::make_unique<ast::SelectFrom>();

    // Parse column list or *
    auto token = ts.peek();
    if (token.type == TokenType::Asterisk) {
        ts.next();
        selectFrom->selectAll = true;
    } else {
        // Parse column list
        bool first = true;
        while (true) {
            if (!first) {
                if (ts.peek().type != TokenType::Comma) {
                    break;
                }
                ts.next();
            }
            first = false;

            auto [table, column] = parseQualifiedColumnRef("column name");

            std::string alias{};
            if (ts.peek().type == TokenType::KeyAs) {
                ts.next();
                token = parseIdentifier("column alias");
                alias = token.getString();
            }

            selectFrom->columns.emplace_back(table, column, alias);
        }

        if (selectFrom->columns.empty()) {
            throw ParserException("SELECT must have at least one column or *",
                                  ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
        }
    }

    // Parse FROM clause
    expectToken(TokenType::KeyFrom, "FROM statement");

    // Parse table list (comma-separated)
    // TODO: Support JOIN syntax (currently only comma-separated tables, cross join)
    bool firstTable = true;
    while (true) {
        if (!firstTable) {
            if (ts.peek().type != TokenType::Comma) {
                break;
            }
            ts.next();
        }
        firstTable = false;

        token = parseIdentifier("table name");

        ast::Table table(token.getString());

        if (ts.peek().type == TokenType::KeyAs) {
            ts.next();
            token = parseIdentifier("table alias");
            table.alias = token.getString();
        }

        selectFrom->tables.emplace_back(table);
    }

    if (selectFrom->tables.empty()) {
        throw ParserException("SELECT must have at least one table",
                              ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
    }

    selectFrom->where = parseWhere();

    // TODO: Support ORDER BY clause

    return selectFrom;
}

DataType Parser::parseDataType(Token token, size_t line, size_t pos) {
    if (token.type == TokenType::KeyIntegerType)
        return DataType::getInt32();
    if (token.type == TokenType::KeyBigintType)
        return DataType::getInt64();
    if (token.type == TokenType::KeyDoubleType)
        return DataType::getDouble();
    if (token.type == TokenType::KeyCharType)
        return DataType::getString();
    if (token.type == TokenType::KeyStringType)
        return DataType::getString();
    if (token.type == TokenType::KeyBoolType)
        return DataType::getBool();
    throw ParserException("Unknown data type: " + token.toString(), line, pos, ts.getQuery());
}

std::unique_ptr<ast::CreateTable> Parser::parseCreateTable() {
    getLogger().trace("Parsing CREATE TABLE statement");

    expectToken(TokenType::KeyCreate, "CREATE statement");
    expectToken(TokenType::KeyTable, "TABLE statement");

    auto token = parseIdentifier("table name");
    auto tableName = token.getString();
    auto createTable = std::make_unique<ast::CreateTable>(tableName);

    expectToken(TokenType::ParenthesisL, "column definition list");

    while (ts.peek().type != TokenType::ParenthesisR) {
            token = parseIdentifier("column name");
            std::string colName = token.getString();

        token = ts.next();
        auto colType = parseDataType(token, ts.getCurrentLineNumber(), ts.getLinePosition());
        createTable->columns.emplace_back(colName, colType);

        if (ts.peek().type == TokenType::Comma) {
            ts.next();
        } else if (ts.peek().type != TokenType::ParenthesisR) {
            throw ParserException(
                "Expected comma or closing parenthesis, but got " + ts.peek().toString(),
                ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
        }
    }

    expectToken(TokenType::ParenthesisR, "column definition list");

    return createTable;
}

/**
 * Parses an INSERT INTO <table> (column_list) VALUES (value_list) statement and returns its AST representation.
 * @throws ParserException if syntax is invalid
 */
std::unique_ptr<ast::Insert> Parser::parseInsertInto() {
    getLogger().trace("Parsing INSERT INTO statement");

    expectToken(TokenType::KeyInsert, "INSERT statement");
    expectToken(TokenType::KeyInto, "INTO statement");

    auto token = parseIdentifier("table name");
    auto tableName = token.getString();
    auto insert = std::make_unique<ast::Insert>(tableName);

    // column list
    if (ts.peek().type == TokenType::ParenthesisL) {
        ts.next();

        bool first = true;
        while (ts.peek().type != TokenType::ParenthesisR) {
            if (!first) {
                expectToken(TokenType::Comma, "column list");
            }
            first = false;

            token = parseIdentifier("column name");
            insert->columnNames.push_back(token.getString());
        }
        expectToken(TokenType::ParenthesisR, "column list");
    }

    expectToken(TokenType::KeyValues, "VALUES statement");

    // value tuples
    bool firstRow = true;
    while (true) {
        if (!firstRow) {
            if (ts.peek().type != TokenType::Comma) {
                break;
            }
            ts.next();
        }
        firstRow = false;

        expectToken(TokenType::ParenthesisL, "value list");
        std::vector<std::unique_ptr<ast::Expression>> row;

        bool firstVal = true;
        while (ts.peek().type != TokenType::ParenthesisR) {
            if (!firstVal) {
                expectToken(TokenType::Comma, "value list");
            }
            firstVal = false;

            auto expr = parseTerm();
            tdb_assert(expr != nullptr, "Expected expression in VALUES list");
            row.push_back(std::move(expr));
        }

        if (insert->columnNames.size() > 0 && insert->columnNames.size() != row.size()) {
            throw ParserException("Number of entries in tuple does not match column list",
                                  ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
        }

        expectToken(TokenType::ParenthesisR, "value list");
        insert->values.push_back(std::move(row));
    }

    return insert;
}

/**
 * Parses an UPDATE statement and returns its AST representation.
 * @throws ParserException if syntax is invalid
 */
std::unique_ptr<ast::Update> Parser::parseUpdate() {
    getLogger().trace("Parsing UPDATE statement");

    expectToken(TokenType::KeyUpdate, "UPDATE statement");

    Token token = ts.next();
    if (token.type != TokenType::IdentifierType) {
        throw ParserException("Expected table name, but got " + token.toString(),
                              ts.getCurrentLineNumber(), ts.getLinePosition(), ts.getQuery());
    }

    auto tableName = token.getString();
    auto update = std::make_unique<ast::Update>(tableName);

    expectToken(TokenType::KeySet, "SET statement");

    bool first = true;
    while (true) {
        if (!first) {
            if (ts.peek().type != TokenType::Comma) {
                break;
            }
            ts.next();
        }

        first = false;

        token = parseIdentifier("column name");
        auto colName = token.getString();

        expectToken(TokenType::OpEquals, "assignment in UPDATE statement");

        auto value = parseTerm();
        tdb_assert(value != nullptr, "Expected expression after = in UPDATE statement");
        update->assignments.emplace_back(colName, std::move(value));
    }

    update->where = parseWhere();
    return update;
}

/**
 * Parses a DELETE statement and returns its AST representation.
 * @throws ParserException if syntax is invalid
 */
std::unique_ptr<ast::Delete> Parser::parseDeleteFrom() {
    getLogger().trace("Parsing DELETE FROM statement {}");

    expectToken(TokenType::KeyDelete, "DELETE statement");
    expectToken(TokenType::KeyFrom, "FROM statement");

    auto token = parseIdentifier("table name");
    auto tableName = token.getString();
    auto deleteFrom = std::make_unique<ast::Delete>(tableName);

    deleteFrom->where = parseWhere();

    return deleteFrom;
}

/**
 * Parses a query string and returns a unique_ptr to the parsed query AST.
 * @param query The query string to parse.
 * @return A unique_ptr to the parsed query AST.
 */
std::expected<std::unique_ptr<ast::QueryAST>, std::string> Parser::parseQuery() {
    auto token = ts.peek();
    ast::ASTNode* query{};

    try {
        switch (token.type) {
            case TokenType::KeySelect:
                query = parseSelect().release();
                break;
            case TokenType::KeyInsert:
                query = parseInsertInto().release();
                break;
            case TokenType::KeyDelete:
                query = parseDeleteFrom().release();
                break;
            case TokenType::KeyUpdate:
                query = parseUpdate().release();
                break;
            case TokenType::KeyCreate:
                query = parseCreateTable().release();
                break;
            default:
                return std::unexpected("Unsupported query type: " + token.toString());
        }
    } catch (const ParserException& e) {
        getLogger().info("Query parsing failed: {}", e.what());
        return std::unexpected(e.what());
    }
    if (ts.peek().type == TokenType::EndOfStatement) {
        ts.next();
    }

    expectToken(TokenType::EndOfFile, "end of query");

    tdb_assert(query != nullptr, "Query AST should not be null");
    std::stringstream ss;
    ss << *query;
    getLogger().debug("Successfully parsed query: {}", ss.str());

    return std::make_unique<ast::QueryAST>(query);
}

}  // namespace parser
}  // namespace toydb
