#include "common/assert.hpp"
#include <iostream>
#include <source_location>

namespace toydb {

void logAssertionFailed(std::string_view condition,
                                       const std::source_location& source_location,
                                       std::string msg) noexcept {
std::cerr << std::format("Assertion '{}' in {}:{} in function {} failed!\n Message: {}", condition,
                         source_location.file_name(), source_location.line(), source_location.function_name(),
                         msg)
          << std::endl;
}
}