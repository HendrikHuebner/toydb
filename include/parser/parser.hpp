#pragma once

#include <cassert>
#include <cctype>
#include "common/result.hpp"
#include "parser/lexer.hpp"
#include "parser/query_ast.hpp"

namespace toydb {

namespace parser {

class Parser {

    TokenStream ts;

    void expectToken(TokenType expected, const std::string& context);

    void expectIdentifier();

    Token parseIdentifier();

    std::unique_ptr<ast::Expression> parseExpression();

    std::unique_ptr<ast::Expression> parseTerm();

    std::unique_ptr<ast::Expression> parseWhere();

    std::unique_ptr<ast::Expression> parseSelect();

    std::unique_ptr<ast::Insert> parseInsertInto();

    std::unique_ptr<ast::Delete> parseDeleteFrom();

    std::unique_ptr<ast::Update> parseUpdate();

    std::unique_ptr<ast::CreateTable> parseCreateTable();

public:
    Parser(std::string_view query) noexcept : ts(query) {}   

    Result<std::unique_ptr<ast::QueryAST>, std::string> parseQuery() noexcept;
};

} // end namespace parser

} // end namespace toydb
