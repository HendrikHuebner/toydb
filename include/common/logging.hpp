#pragma once

#include <spdlog/logger.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <filesystem>
#include <memory>

namespace toydb {

using logger_t = spdlog::logger;

const auto logDirectory =  std::filesystem::path{ ".." } / ".." / "logs";

inline logger_t& getLogger() {
    static auto logger = []() {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(spdlog::level::debug);

        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logDirectory / "latest.log", true);
        file_sink->set_level(spdlog::level::trace);

        auto logger = spdlog::logger("multi_sink", spdlog::sinks_init_list{console_sink, file_sink});
        logger.set_pattern("[%Y-%m-%d %H:%M:%S] [%^%l%$] [%s:%#] %v");
        logger.set_level(spdlog::level::trace);

        return logger;
    }();
    
    return logger;
}

namespace Logger {

    template <typename... Args>
    inline void trace(Args&&... args) {
        getLogger().trace(std::forward<Args>(args)...);
    }

    template <typename... Args>
    inline void debug(Args&&... args) {
        getLogger().debug(std::forward<Args>(args)...);
    }

    template <typename... Args>
    inline void info(Args&&... args) {
        getLogger().info(std::forward<Args>(args)...);
    }

        template <typename... Args>
    inline void warn(Args&&... args) {
        getLogger().warn(std::forward<Args>(args)...);
    }

        template <typename... Args>
    inline void error(Args&&... args) {
        getLogger().error(std::forward<Args>(args)...);
    }

        template <typename... Args>
    inline void critical(Args&&... args) {
        getLogger().critical(std::forward<Args>(args)...);
    }
}

} // namespace toydb
