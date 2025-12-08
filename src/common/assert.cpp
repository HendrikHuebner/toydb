#include "common/assert.hpp"
#include <source_location>
#include "common/logging.hpp"

namespace toydb {

void logAssertionFailed(std::string_view condition, const std::source_location& source_location,
                        std::string msg) noexcept {
    Logger::error("Assertion '{}' in {}:{} in function {} failed!\n Message: {}", condition,
                  source_location.file_name(), source_location.line(),
                  source_location.function_name(), msg);
}
}  // namespace toydb