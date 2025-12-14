#include "gtest/gtest.h"
#include "parser/lexer.hpp"
#include <limits>

using namespace toydb::parser;

TEST(LexerTest, KeyWords) {
    std::string input = "SELECT select from FROM where WHERE";
    TokenStream ts{input};

    ASSERT_TRUE(ts.next().type == TokenType::KeySelect);
    ASSERT_TRUE(ts.next().type == TokenType::KeySelect);
    ASSERT_TRUE(ts.next().type == TokenType::KeyFrom);
    ASSERT_TRUE(ts.next().type == TokenType::KeyFrom);
    ASSERT_TRUE(ts.next().type == TokenType::KeyWhere);
    ASSERT_TRUE(ts.next().type == TokenType::KeyWhere);

    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}

TEST(LexerTest, Peek) {
    std::string input = "select from";
    TokenStream ts{input};

    ASSERT_TRUE(ts.peek().type == TokenType::KeySelect);
    ASSERT_TRUE(ts.peek().type == TokenType::KeySelect);
    ASSERT_TRUE(ts.next().type == TokenType::KeySelect);

    ASSERT_TRUE(ts.peek().type == TokenType::KeyFrom);
    ASSERT_TRUE(ts.next().type == TokenType::KeyFrom);

    ASSERT_TRUE(ts.empty());
    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
    ASSERT_TRUE(ts.empty());
}

TEST(LexerTest, Literals) {
    std::string input = "123 foobar '99 foo %$^`~ ' true FALSE '' ";
    TokenStream ts{input};

    ASSERT_TRUE(ts.next().type == TokenType::Int32Literal);
    ASSERT_TRUE(ts.next().type == TokenType::IdentifierType);
    ASSERT_TRUE(ts.next().type == TokenType::StringLiteral);
    ASSERT_TRUE(ts.next().type == TokenType::TrueLiteral);
    ASSERT_TRUE(ts.next().type == TokenType::FalseLiteral);
    ASSERT_TRUE(ts.next().type == TokenType::StringLiteral);

    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}


TEST(LexerTest, Chars) {
    std::string input = "(1,2) * foo;#";
    TokenStream ts{input};

    ASSERT_TRUE(ts.next().type == TokenType::ParenthesisL);
    ASSERT_TRUE(ts.next().type == TokenType::Int32Literal);
    ASSERT_TRUE(ts.next().type == TokenType::Comma);
    ASSERT_TRUE(ts.next().type == TokenType::Int32Literal);
    ASSERT_TRUE(ts.next().type == TokenType::ParenthesisR);

    ASSERT_TRUE(ts.next().type == TokenType::Asterisk);
    ASSERT_TRUE(ts.next().type == TokenType::IdentifierType);
    ASSERT_TRUE(ts.next().type == TokenType::EndOfStatement);
    ASSERT_TRUE(ts.next().type == TokenType::Unknown);
    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}

TEST(LexerTest, Operators) {
    std::string input = "> < = <> != >= <=";
    TokenStream ts{input};

    ASSERT_TRUE(ts.next().type == TokenType::OpGreaterThan);
    ASSERT_TRUE(ts.next().type == TokenType::OpLessThan);
    ASSERT_TRUE(ts.next().type == TokenType::OpEquals);
    ASSERT_TRUE(ts.next().type == TokenType::OpNotEquals);
    ASSERT_TRUE(ts.next().type == TokenType::OpNotEquals);

    ASSERT_TRUE(ts.next().type == TokenType::OpGreaterEq);
    ASSERT_TRUE(ts.next().type == TokenType::OpLessEq);
}

TEST(LexerTest, Int32LiteralValues) {
    std::string input = "0 42 -2147483648 2147483647";
    TokenStream ts{input};

    Token token1 = ts.next();
    ASSERT_EQ(token1.type, TokenType::Int32Literal);
    ASSERT_EQ(token1.getInt(), 0);

    Token token2 = ts.next();
    ASSERT_EQ(token2.type, TokenType::Int32Literal);
    ASSERT_EQ(token2.getInt(), 42);

    Token token3 = ts.next();
    ASSERT_EQ(token3.type, TokenType::Int32Literal);
    ASSERT_EQ(token3.getInt(), -2147483648);

    Token token4 = ts.next();
    ASSERT_EQ(token4.type, TokenType::Int32Literal);
    ASSERT_EQ(token4.getInt(), 2147483647);

    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}

TEST(LexerTest, Int64LiteralValues) {
    std::string input = "2147483648 -2147483649 9223372036854775807 -9223372036854775808";
    TokenStream ts{input};

    Token token1 = ts.next();
    ASSERT_EQ(token1.type, TokenType::Int64Literal);
    ASSERT_EQ(token1.getInt(), 2147483648LL);

    Token token2 = ts.next();
    ASSERT_EQ(token2.type, TokenType::Int64Literal);
    ASSERT_EQ(token2.getInt(), -2147483649LL);

    Token token3 = ts.next();
    ASSERT_EQ(token3.type, TokenType::Int64Literal);
    ASSERT_EQ(token3.getInt(), 9223372036854775807LL);

    Token token4 = ts.next();
    ASSERT_EQ(token4.type, TokenType::Int64Literal);
    int64_t minInt64 = std::numeric_limits<int64_t>::min();
    ASSERT_EQ(token4.getInt(), minInt64);

    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}

TEST(LexerTest, DoubleLiteralValues) {
    std::string input = "3.14 -2.5 0.0 123.456 -0.001";
    TokenStream ts{input};

    Token token1 = ts.next();
    ASSERT_EQ(token1.type, TokenType::DoubleLiteral);
    ASSERT_DOUBLE_EQ(token1.getDouble(), 3.14);

    Token token2 = ts.next();
    ASSERT_EQ(token2.type, TokenType::DoubleLiteral);
    ASSERT_DOUBLE_EQ(token2.getDouble(), -2.5);

    Token token3 = ts.next();
    ASSERT_EQ(token3.type, TokenType::DoubleLiteral);
    ASSERT_DOUBLE_EQ(token3.getDouble(), 0.0);

    Token token4 = ts.next();
    ASSERT_EQ(token4.type, TokenType::DoubleLiteral);
    ASSERT_DOUBLE_EQ(token4.getDouble(), 123.456);

    Token token5 = ts.next();
    ASSERT_EQ(token5.type, TokenType::DoubleLiteral);
    ASSERT_DOUBLE_EQ(token5.getDouble(), -0.001);

    ASSERT_TRUE(ts.next().type == TokenType::EndOfFile);
}
