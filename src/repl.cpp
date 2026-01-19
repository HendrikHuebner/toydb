#include "common/stacktrace.hpp"
#include "parser/parser.hpp"
#include <iostream>
#include <string>

using namespace toydb;
using namespace toydb::parser;

int main() {
    std::string input;
    toydb::initializeSignalHandlers();
    std::cout << "toydb> ";
    while (std::getline(std::cin, input)) {
        if (input.empty()) {
            std::cout << "toydb> ";
            continue;
        }

        Parser parser{input};
        auto result = parser.parseQuery();

        if (result.has_value()) {
            result.value()->query_->print(std::cout);
            std::cout << std::endl;
        } else {
            std::cout << "Error: " << result.error() << std::endl;
        }

        std::cout << "toydb> ";
    }

    return 0;
}
