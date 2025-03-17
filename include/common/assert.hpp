#pragma once

#ifdef NDEBUG
#define ASSERT(...)
#else

#include <iostream>
#include <source_location>

namespace toydb {

template <typename... Args>
static void constexpr printAssertFailed(std::string_view condition, std::string_view message,
                              const std::source_location& source_location, Args&&... args) {
    
    auto&& formatted_message = std::vformat(message, std::make_format_args(args...));

    std::cerr << std::format("Assertion '{}' in {} at {}:{} failed!\n Message: {}", 
                            condition,
                            source_location.file_name(), 
                            source_location.function_name(),
                            source_location.line(), 
                            formatted_message)
              << std::endl;
}

} // namespace toydb

#define debug_assert(cond, msg, ...)                                                            \
    if (!(cond)) {                                                                              \
        toydb::printAssertFailed(#cond, (msg), std::source_location::current(), ##__VA_ARGS__); \
    }

#endif
