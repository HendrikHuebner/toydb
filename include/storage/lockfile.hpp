#pragma once

#include <fcntl.h>
#include <sys/file.h>
#include <filesystem>
#include "common/logging.hpp"

namespace toydb {

static std::string getCurrentTimeStamp() {
    using namespace std::chrono;
    auto t = system_clock::now();
    auto s = system_clock::to_time_t(t);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&s), "%Y-%m-%dT%H:%M:%S");
    return oss.str();
}

class Lockfile {

    std::filesystem::path path;
    int fd = 0;

    Lockfile(std::filesystem::path path) : path(path) {}

    bool tryLock() noexcept {
        fd = open(path.c_str(), O_CREAT | O_WRONLY);
        if (!fd)
            return false;

        int err = flock(fd, LOCK_EX | LOCK_NB);

        if (!err) {
            // Write the process PID and a timestamp
            std::string lockInfo =
                "pid=" + std::to_string(getpid()) + " ts=" + getCurrentTimeStamp();
            return true;
        }

        // Lock is being held
        if (errno == EWOULDBLOCK) {
            return false;
        }

        Logger::error("Error locking lock file at '{}': {}", path.string(), strerror(errno));
        return false;
    }

    void unlock() noexcept {
        if (!fd)
            Logger::warn("No lock being held for lock file at '{}'", path.string());

        int err = flock(fd, LOCK_UN);
        fd = 0;

        if (err) {
            Logger::error("Error unlocking lock file at '{}': {}", path.string(), strerror(errno));
        }
    }
};

}  // namespace toydb
