#pragma once

#include <spdlog/spdlog.h>

namespace toydb {

using logger_t = spdlog::logger;

logger_t& getLogger();

namespace Logger {

template <typename... Args>
inline void trace(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().trace(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().debug(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().info(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().warn(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().error(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void critical(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    getLogger().critical(fmt, std::forward<Args>(args)...);
}
}  // namespace Logger
}  // namespace toydb
