#include <iostream>
#include <iomanip>
#include <mutex>

namespace toydb {
namespace debug {

static thread_local int depth = 0;
static std::mutex cout_mutex;

struct Trace {
    Trace() {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << std::setw(2 * depth) << "";
        ++depth;
    }

    ~Trace() {
        --depth;
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << std::setw(2 * depth) << "";
    }
};

} // namespace toydb
} // namespace debug

#define TRACE(...)                                                   \
    {                                                                \
        toydb::debug::Trace trace_obj;                               \
        std::lock_guard<std::mutex> lock(toydb::debug::cout_mutex);  \
        std::cout << std::setw(4 * toydb::debug::depth) << "";       \
        std::cout << __VA_ARGS__ << std::endl;                       \
    }
