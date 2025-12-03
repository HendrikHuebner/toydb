#include "common/logging.hpp"

#include <spdlog/logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <filesystem>

namespace toydb {

const auto logDirectory =  std::filesystem::path{ ".." } / ".." / "logs";

logger_t& getLogger() {
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

} // namespace toydb
