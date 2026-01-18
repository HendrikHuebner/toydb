#pragma once

#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <expected>

namespace toydb {

namespace parser {

enum class TokenType {
    IdentifierType,
    Int32Literal,
    Int64Literal,
    DoubleLiteral,
    TrueLiteral,
    FalseLiteral,
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
    OpAnd,
    OpOr,

    KeyInsert,
    KeyInto,
    KeyValues,
    KeySelect,
    KeyFrom,
    KeyWhere,
    KeyAs,
    KeyJoin,
    KeyOn,
    KeyOrder,
    KeyBy,
    KeyUpdate,
    KeySet,
    KeyDelete,
    KeyCreate,
    KeyTable,

    KeyBoolType,
    KeyIntegerType,
    KeyBigintType,
    KeyDoubleType,
    KeyCharType,
    KeyStringType,

    Asterisk,
    Quote,
    ParenthesisL,
    ParenthesisR,
    Comma,
    Dot,

    Unknown = 666,
};

struct Token {
    using TokenValue = std::variant<std::monostate, std::string, int64_t, double>;

    TokenType type;
    TokenValue value;

    Token(TokenType type = TokenType::Unknown) : type(type), value(std::monostate{}) {}
    Token(TokenType type, const std::string& str) : type(type), value(str) {}
    Token(TokenType type, std::string&& str) : type(type), value(std::move(str)) {}
    Token(TokenType type, int64_t val) : type(type), value(val) {}
    Token(TokenType type, double val) : type(type), value(val) {}

    std::string getString() const;
    int64_t getInt() const;
    double getDouble() const;
    bool getBool() const;

    std::string toString() const noexcept;
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
    explicit TokenStream(std::string_view query) noexcept : query(query), position(0) {}

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

    std::string_view getQuery() const noexcept { return query; }

    std::string_view getCurrentLine() const noexcept {
        return query.substr(lineStart, std::max(query.size() - 1, position + 1));
    }

    size_t getCurrentLineNumber() const noexcept { return line; }

    size_t getLinePosition() const noexcept { return position - lineStart - 1; }

   private:
    std::optional<char> moveToNextToken() noexcept;

    Token lexOperator() noexcept;
    Token lexWord() noexcept;
    Token lexString() noexcept;
    Token lexNumber() noexcept;
    Token lexPunctuationChar() noexcept;
};

} // end namespace parser

} // end namespace toydb
