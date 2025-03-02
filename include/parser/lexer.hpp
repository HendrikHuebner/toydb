#pragma once

#include <cassert>
#include <cctype>
#include <optional>
#include <string>
#include "common/result.hpp"
#include "common/assert.hpp"

namespace toydb {

namespace parser {

enum class TokenType {
    IdentifierType,
    IntLiteral,
    FloatLiteral,
    BooleanLiteral,
    StringLiteral,
    NullLiteral,
    EndOfStatement,
    EndOfFile,

    OpGreaterThan,
    OpLessThan,
    OpGreaterEq,
    OpLessEq,
    OpEquals,
    OpNotEquals,

    KeyInsertInto,
    KeyValues,
    KeySelect,
    KeyFrom,
    KeyWhere,
    KeyJoin,
    KeyOn,
    KeyOrderBy,
    KeyUpdate,
    KeySet,
    KeyDeleteFrom,

    ParenthesisR,
    ParenthesisL,
    Comma,

    Unknown
};


struct Token {

    TokenType type;
    std::string_view lexeme;

    Token(TokenType type = TokenType::Unknown) : type(type), lexeme("") {}
    Token(TokenType type, std::string_view lexeme) : type(type), lexeme(lexeme) {}

    std::string toString() const;
};


/**
 * @brief Tokenizes a query. The next Token can be peeked/popped similar to a stack.
 * 
 */
class TokenStream {

    // input query
    std::string_view query;

    // current position in query
    size_t position = 0;
    size_t line = 1;
    size_t lineStart = 0;

    mutable std::optional<Token> top = std::nullopt;

   public:
    explicit TokenStream(std::string_view query) : query(query), position(0) {}

    /**
     * @brief Returns next token in query and moves to the next token
     * 
     * @return Token 
     */
    [[maybe_unused]] Token next() noexcept;

    /**
    * @brief Returns next token in query without moving to the next token
    * 
    * @return Token 
    */
    Token peek() noexcept;

    /**
     * @brief Returns true if there no more tokens to read
     */
    bool empty() noexcept;

    std::string_view getCurrentLine() const noexcept {
        return query.substr(lineStart, std::max(query.size() - 1, position + 1));
    }

    size_t getCurrentLineNumber() const noexcept { return line; }

    size_t getLinePosition() const noexcept { return position - lineStart - 1; }

   private:
    Result<char> moveToNextToken() noexcept;

    Token lexOperator() noexcept;
    Token lexWord() noexcept;
    Token lexNumber() noexcept;
    Token lexPunctuationChar() noexcept;
};

} // end namespace parser

} // end namespace toydb
