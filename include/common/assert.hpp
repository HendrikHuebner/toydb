#pragma once

#ifdef NDEBUG
#define tdb_assert(...)
#define tdb_unreachable(...) __builtin__unreachable()
#else

#include <format>
#include <source_location>
#include <string_view>

namespace toydb {

void logAssertionFailed(std::string_view, const std::source_location&,
                                         std::string msg) noexcept;

template <typename... Args>
static void printAssertFailed(std::string_view condition, std::string_view message,
                                        const std::source_location& source_location,
                                        Args&&... args) noexcept {

    std::string formatted_message = std::vformat(message, std::make_format_args(args...));
    logAssertionFailed(condition, source_location, formatted_message);
    abort();
}

}  // namespace toydb

#define tdb_assert(cond, msg, ...)                                                              \
    if (!(cond)) {                                                                              \
        toydb::printAssertFailed(#cond, (msg), std::source_location::current(), ##__VA_ARGS__); \
    }

#define tdb_unreachable(msg)                                                           \
    do {                                                                               \
        toydb::printAssertFailed("unreachable", msg, std::source_location::current()); \
        __builtin_unreachable();                                                       \
    } while (0)

#endif
