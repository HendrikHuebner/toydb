#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <optional>
#include <unordered_map>

#include "parser/lexer.hpp"

namespace toydb {

namespace parser {

const std::unordered_map<std::string, TokenType> keywords = [](){
    std::unordered_map<std::string, TokenType> result;
    std::pair<std::string, TokenType> upperKeywords[] = {
        {"SELECT", TokenType::KeySelect},
        {"FROM", TokenType::KeyFrom},
        {"WHERE", TokenType::KeyWhere},
        {"JOIN", TokenType::KeyJoin},
        {"ON", TokenType::KeyOn},
        {"ORDER", TokenType::KeyOrder},
        {"BY", TokenType::KeyBy},
        {"INSERT", TokenType::KeyInsert},
        {"INTO", TokenType::KeyInto},
        {"UPDATE", TokenType::KeyUpdate},
        {"CREATE", TokenType::KeyCreate},
        {"TABLE", TokenType::KeyTable},
        {"SET", TokenType::KeySet},
        {"DELETE", TokenType::KeyDelete},
        {"VALUES", TokenType::KeyValues},
        {"AND", TokenType::OpAnd},
        {"OR", TokenType::OpOr},
        {"INT", TokenType::KeyIntType},
        {"CHAR", TokenType::KeyCharType},
        {"BOOL", TokenType::KeyBoolType},
        {"NULL", TokenType::NullLiteral},
        {"TRUE", TokenType::TrueLiteral},
        {"FALSE", TokenType::FalseLiteral}
    };

    // add lowercase versions
    for (auto& entry : upperKeywords) {
        result.insert(entry);
        std::string lower{entry.first};
        std::transform(entry.first.begin(), entry.first.end(), lower.begin(), ::tolower);
        result[lower] = entry.second;
    }

    return result;
}();


enum CharType {
    X,  // None
    A,  // Alphabetical char or underscore
    O,  // Operator
    P,  // Punctuation char
    S,  // Quote / String
    N   // Numeric char
};

const CharType lut[128] = {
    X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X,
    X, O, X, X, X, O, O, S, P, P, O, O, P, O, P, O, N, N, N, N, N, N, N, N, N, N, X, P, O, O, O, X,
    P, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, P, X, P, O, A,
    X, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, P, O, P, O, X,
};

/**
 * @brief Returns CharType of c
 */
CharType lookupChar(unsigned char c) noexcept {
    if (c >= 128)
        return X;

    return lut[c];
}

Token TokenStream::next() noexcept {
    Token token;

    if (top) {
        token = top.value();
        top = std::nullopt;
        return token;
    }

    auto c = moveToNextToken();

    if (position >= query.size() || !c.has_value()) {
        return Token(TokenType::EndOfFile);
    }

    CharType charType = lookupChar(c.value());

    switch (charType) {
        case CharType::A: token = lexWord(); break;
        case CharType::N: token = lexNumber(); break;
        case CharType::O: token = lexOperator(); break;
        case CharType::S: token = lexString(); break;
        case CharType::P: token = lexPunctuationChar(); break;
        default: {
            position++;
            return TokenType::Unknown;
        }
    }

    return token;
}

Token TokenStream::peek() noexcept {
    if (top) {
        return top.value();
    }

    Token next = this->next();
    top = std::make_optional<Token>(next);
    return next;
}

bool TokenStream::empty() noexcept {
    return peek().type == TokenType::EndOfFile;
}

std::optional<char> TokenStream::moveToNextToken() noexcept {
    while (position < query.size()) {
        char c = query[position];

        if (c == '/' && position + 1 < query.size()) {
            if (query[++position] == '/') {

                // skip comment
                while (position < query.size() && query[position] != '\n') {
                    ++position;
                }

                if (position >= query.size()) {
                    return std::nullopt;
                }

                ++position;
                lineStart = position;
                ++line;

                continue;
            }
        }

        // check whitespace
        if (std::isspace(c)) {
            if (c == '\n') {
                ++line;
                lineStart = position;
            }

            ++position;

        } else {
            return c;
        }
    }

    return std::nullopt;
}

Token TokenStream::lexOperator() noexcept {
    char c = query[position];
    char c2 = (position + 1 >= query.size()) ? 0 : query[position + 1];

    std::optional<TokenType> op = std::nullopt;

    switch (c) {
        case '=':
            op = TokenType::OpEquals;
            break;

        case '*':
            op = TokenType::Asterisk;
            break;
        
        case '<': {
            if (c2 == '=') {
                op = TokenType::OpLessEq;
                ++position;
            } else if (c2 == '>') {
                op = TokenType::OpNotEquals;
                ++position;
            } else {
                op = TokenType::OpLessThan;
            }
            break;
        }
        case '!': {
            if (c2 == '=') {
                op = TokenType::OpNotEquals;
                ++position;
            }
            break;
        }
        case '>': {
            if (c2 == '=') {
                op = TokenType::OpGreaterEq;
                ++position;
            } else {
                op = TokenType::OpGreaterThan;
            }
            break;
        }
    }

    ++position;

    if (!op.has_value()) {
        return { TokenType::Unknown };

    } else {
        return {op.value()};
    }
}

Token TokenStream::lexWord() noexcept {
    size_t start = position;
    char c = query[position];

    while (position < query.size() && (std::isalpha(c) || c == '_')) {
        position++;

        if (position < query.size())
            c = query[position];
    }

    std::string lexeme { query.substr(start, position - start) };

    if (keywords.find(lexeme) != keywords.end()) {
        return { keywords.at(lexeme) };
    }

    return {TokenType::IdentifierType, std::move(lexeme) };
}

Token TokenStream::lexString() noexcept {
    size_t start = ++position;
    char c;

    while (true) {
        if (position >= query.size())
            return TokenType::Unknown;

        c = query[position];

        if (c == '\'') {
            break;
        }

        ++position;
    }

    std::string lexeme { query.substr(start, position - start) };
    ++position;
    return { TokenType::StringLiteral, lexeme };
}

Token TokenStream::lexNumber() noexcept {
    size_t start = position;

    while (position < query.size()) {
        char c = query[position];
        if (std::isdigit(c)) {
            ++position;
        } else {
            break;
        }
    }

    return {TokenType::IntLiteral, std::string(query.substr(start, position - start))};
}

Token TokenStream::lexPunctuationChar() noexcept {
    switch (query[position++]) {
        case ';': return TokenType::EndOfStatement;
        case '(': return TokenType::ParenthesisL;
        case ')': return TokenType::ParenthesisR;
        case ',': return TokenType::Comma;
        default: return TokenType::Unknown;
    }
}

std::string Token::toString() const noexcept {
    switch (type) {
        case TokenType::OpGreaterThan: return ">";
        case TokenType::OpLessThan: return "<";
        case TokenType::OpGreaterEq: return ">=";
        case TokenType::OpLessEq: return "<=";
        case TokenType::OpEquals: return "=";
        case TokenType::OpNotEquals: return "!=";
        case TokenType::OpAnd: return "AND";
        case TokenType::OpOr: return "OR";

        case TokenType::KeyInsert: return "INSERT";
        case TokenType::KeyInto: return "INTO";
        case TokenType::KeyValues: return "VALUES";
        case TokenType::KeySelect: return "SELECT";
        case TokenType::KeyFrom: return "FROM";
        case TokenType::KeyWhere: return "WHERE";
        case TokenType::KeyCreate: return "CREATE";
        case TokenType::KeyTable: return "TABLE";
        case TokenType::KeyJoin: return "JOIN";
        case TokenType::KeyOn: return "ON";
        case TokenType::KeyOrder: return "ORDER";
        case TokenType::KeyBy: return "BY";
        case TokenType::KeyUpdate: return "UPDATE";
        case TokenType::KeySet: return "SET";
        case TokenType::KeyDelete: return "DELETE";
        case TokenType::NullLiteral: return "NULL";
        case TokenType::TrueLiteral: return "TRUE";
        case TokenType::FalseLiteral: return "FALSE";

        case TokenType::ParenthesisR: return ")";
        case TokenType::ParenthesisL: return "(";
        case TokenType::Comma: return ",";
        case TokenType::Asterisk: return "*";

        case TokenType::IdentifierType:
        case TokenType::IntLiteral:
        case TokenType::StringLiteral: 
            return lexeme;

        case TokenType::EndOfFile: return "<EOF>";
        case TokenType::Unknown: return "<UNKNOWN>";

        default: return "?";
    }
}

} // end namespace parser

} // end namespace tdb
