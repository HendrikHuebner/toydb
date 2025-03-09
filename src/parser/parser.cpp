#include "parser/parser.hpp"
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "common/assert.hpp"
#include "common/logging.hpp"
#include "parser/lexer.hpp"
#include "parser/query_ast.hpp"

namespace toydb {
namespace parser {

class ParserException : public std::runtime_error {
public:
    ParserException(const std::string& message, size_t line, size_t position)
        : std::runtime_error(message + " at line " + std::to_string(line) + ", position " + std::to_string(position)) {}
};

/**
 * Parses an identifier token and returns it.
 * @throws ParserException if next token is not an identifier
 */
Token Parser::parseIdentifier() {
    auto token = ts.next();
    if (token.type != TokenType::IdentifierType) {
        throw ParserException("Expected identifier, but got " + token.toString(),
                            ts.getCurrentLineNumber(), ts.getLinePosition());
    }
    return token;
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
        throw ParserException("Expected " + std::string(token.toString()) + ", but got " + 
                            token.toString() + " in " + context,
                            ts.getCurrentLineNumber(), ts.getLinePosition());
    }
    ts.next();
}

/**
 * Parses a SQL expression and returns its AST representation.
 * Handles comparison operators and nested expressions.
 */
std::unique_ptr<ast::Expression> Parser::parseExpression() {
    auto left = parseTerm();
    auto token = ts.peek();

    while (token.type == TokenType::OpEquals || token.type == TokenType::OpNotEquals ||
           token.type == TokenType::OpGreaterThan || token.type == TokenType::OpLessThan ||
           token.type == TokenType::OpGreaterEq || token.type == TokenType::OpLessEq) {
        ts.next();
        auto right = parseTerm();
        auto condition = std::make_unique<ast::Condition>();
        condition->left = std::move(left);
        condition->right = std::move(right);

        switch (token.type) {
            case TokenType::OpEquals: condition->op = ast::Operator::EQUAL; break;
            case TokenType::OpNotEquals: condition->op = ast::Operator::NOT_EQUAL; break;
            case TokenType::OpGreaterThan: condition->op = ast::Operator::GREATER; break;
            case TokenType::OpLessThan: condition->op = ast::Operator::LESS; break;
            case TokenType::OpGreaterEq: condition->op = ast::Operator::GREATER_EQUAL; break;
            case TokenType::OpLessEq: condition->op = ast::Operator::LESS_EQUAL; break;
            default: break;
        }

        left = std::move(condition);
        token = ts.peek();
    }

    return left;
}

/**
 * Parses a term (identifier or literal) and returns its AST representation.
 */
std::unique_ptr<ast::Expression> Parser::parseTerm() {
    auto token = ts.peek();

    if (token.type == TokenType::IdentifierType) {
        return std::make_unique<ast::Literal>(std::string(token.lexeme));

    } else if (token.type == TokenType::IntLiteral || token.type == TokenType::FloatLiteral ||
               token.type == TokenType::StringLiteral) {
        ts.next();
        return std::make_unique<ast::Literal>(std::string(token.lexeme));
    }
    throw ParserException("Expected term but got " + token.toString(),
                        ts.getCurrentLineNumber(), ts.getLinePosition());
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
        throw ParserException("Expected expression after WHERE",
                            ts.getCurrentLineNumber(), ts.getLinePosition());
    }
    return expr;
}

ast::DataType parseDataType(Token token, size_t line, size_t pos) {
    if (token.type == TokenType::KeyIntType) return ast::DataType::INT;
    if (token.type == TokenType::KeyFloatType) return ast::DataType::FLOAT;
    if (token.type == TokenType::KeyCharType) return ast::DataType::STRING;
    if (token.type == TokenType::KeyBoolType) return ast::DataType::BOOL;
    throw ParserException("Unknown data type: " + token.toString(), line, pos);
}

std::unique_ptr<ast::CreateTable> Parser::parseCreateTable() {
    getLogger().trace("Parsing CREATE TABLE statement");

    expectToken(TokenType::KeyCreate, "CREATE TABLE statement");
    expectToken(TokenType::KeyTable, "CREATE TABLE statement");

    auto token = parseIdentifier();
    auto tableName = token.lexeme;
    auto createTable = std::make_unique<ast::CreateTable>(std::string(tableName));

    expectToken(TokenType::ParenthesisL, "column definition list");

    while (ts.peek().type != TokenType::ParenthesisR) {
        token = parseIdentifier();
        std::string_view colName = token.lexeme;
        
        token = ts.next();
        auto colType = parseDataType(token, ts.getCurrentLineNumber(), ts.getLinePosition());
        createTable->columns.emplace_back(std::string(colName), colType);

        if (ts.peek().type == TokenType::Comma) {
            ts.next();
        } else if (ts.peek().type != TokenType::ParenthesisR) {
            throw ParserException("Expected comma or closing parenthesis, but got " + ts.peek().toString(),
                                ts.getCurrentLineNumber(), ts.getLinePosition());
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

    auto token = parseIdentifier();
    auto tableName = token.lexeme;
    auto insert = std::make_unique<ast::Insert>(std::string(tableName));

    if (ts.peek().type == TokenType::ParenthesisL) {
        ts.next();

        bool first = true;
        while (ts.peek().type != TokenType::ParenthesisR) {
            if (!first) {
                expectToken(TokenType::Comma, "column list");
            }
            first = false;
            
            token = parseIdentifier();
            insert->columnNames.push_back(std::string(token.lexeme));
        }
        expectToken(TokenType::ParenthesisR, "column list");
    }

    expectToken(TokenType::KeyValues, "INSERT statement");

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
            debug_assert(expr != nullptr, "Expected expression in VALUES list");
            row.push_back(std::move(expr));
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
                            ts.getCurrentLineNumber(), ts.getLinePosition());
    }

    auto tableName = token.lexeme;
    auto update = std::make_unique<ast::Update>(std::string(tableName));

    expectToken(TokenType::KeySet, "UPDATE statement");

    bool first = true;
    while (true) {
        if (!first) {
            if (ts.peek().type != TokenType::Comma) {
                break;
            }
            ts.next();
        }

        first = false;
        
        token = parseIdentifier();
        auto colName = token.lexeme;
        
        expectToken(TokenType::OpEquals, "assignment in UPDATE statement");
        
        auto value = parseTerm();
        debug_assert(value != nullptr, "Expected expression after = in UPDATE statement");
        update->assignments.emplace_back(std::string(colName), std::move(value));
    }

    update->where = parseWhere();
    return update;
}

/**
 * Parses a DELETE statement and returns its AST representation.
 * @throws ParserException if syntax is invalid
 */
std::unique_ptr<ast::Delete> Parser::parseDeleteFrom() {
    getLogger().trace("Parsing DELETE FROM statement");

    expectToken(TokenType::KeyDelete, "DELETE statement");
    expectToken(TokenType::KeyFrom, "FROM statement");

    auto token = parseIdentifier();
    auto tableName = token.lexeme;
    auto deleteFrom = std::make_unique<ast::Delete>(std::string(tableName));

    deleteFrom->where = parseWhere();
    
    return deleteFrom;
}

/**
 * Parses a query string and returns a unique_ptr to the parsed query AST.
 * @param query The query string to parse.
 * @return A unique_ptr to the parsed query AST.
 */
Result<std::unique_ptr<ast::QueryAST>, std::string> Parser::parseQuery() noexcept {
    auto token = ts.peek();
    ast::ASTNode* query{};

    using result_t = Result<std::unique_ptr<ast::QueryAST>, std::string>;

    try {
        switch (token.type) {
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
                return result_t::make_error("Unsupported query type: " + token.toString());
        }
    } catch (const ParserException& e) {
        getLogger().info("Query parsing failed: {}", e.what());
        return result_t::make_error(e.what());
    }

    if (ts.peek().type == TokenType::EndOfStatement) {
        ts.next();
    }

    expectToken(TokenType::EndOfFile, "end of query");

    debug_assert(query != nullptr, "Query AST should not be null");
    getLogger().trace("Successfully parsed query");

    return result_t::make_success(std::make_unique<ast::QueryAST>(query));
}

} // namespace parser
} // namespace toydb