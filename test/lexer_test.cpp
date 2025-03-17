#include "gtest/gtest.h"
#include "parser/lexer.hpp"

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

    ASSERT_TRUE(ts.next().type == TokenType::IntLiteral);
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
    ASSERT_TRUE(ts.next().type == TokenType::IntLiteral);
    ASSERT_TRUE(ts.next().type == TokenType::Comma);
    ASSERT_TRUE(ts.next().type == TokenType::IntLiteral);
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
