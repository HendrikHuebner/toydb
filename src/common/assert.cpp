#include "common/assert.hpp"
#include <iostream>
#include <source_location>

namespace toydb {

void logAssertionFailed(std::string_view condition,
                                       const std::source_location& source_location,
                                       std::string msg) noexcept {
std::cerr << std::format("Assertion '{}' in {} at {}:{} failed!\n Message: {}", condition,
                         source_location.file_name(), source_location.function_name(),
                         source_location.line(), msg)
          << std::endl;
}
}