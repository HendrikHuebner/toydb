#pragma once

#include <fcntl.h>
#include <sys/file.h>
#include <filesystem>
#include "common/logging.hpp"

namespace toydb {

inline std::string getCurrentTimeStamp() noexcept {
    using namespace std::chrono;
    auto t = system_clock::now();
    auto s = system_clock::to_time_t(t);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&s), "%Y-%m-%dT%H:%M:%S");
    return oss.str();
}

class Lockfile {
public:
    explicit Lockfile(const std::filesystem::path& p) : path(p), fd(-1) {}

    ~Lockfile() {
        unlock();
    }

    bool lock(bool block = true) noexcept {
        fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd == -1)
            return false;

        int extraLockFlags = (block ? LOCK_EX | LOCK_NB : 0);
        if (flock(fd, LOCK_EX | extraLockFlags) != 0) {
            if (errno != EWOULDBLOCK) {
                Logger::error("Error locking '{}': {}", path.string(), strerror(errno));
            }
            close(fd);
            fd = -1;
            return false;
        }

        // Write lock metadata
        std::string info =
            "pid=" + std::to_string(getpid()) +
            " ts=" + getCurrentTimeStamp() + "\n";
        write(fd, info.data(), info.size());
        fsync(fd);

        return true;
    }

    void unlock() noexcept {
        if (fd == -1)
            return; // nothing to unlock

        if (flock(fd, LOCK_UN) != 0)
            Logger::error("Error unlocking '{}': {}", path.string(), strerror(errno));

        close(fd);
        fd = -1;
    }

private:
    std::filesystem::path path;
    int fd;
};

}  // namespace toydb
