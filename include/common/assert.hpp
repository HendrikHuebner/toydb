#ifdef NDEBUG
#define ASSERT(...)
#else

#include <iostream>

[[maybe_unused]]
static void printAssertFailed(std::string condition, std::string message) {
    std::cerr << std::format("Assertion '{}' in {} at line {} failed!\n Message: {}", condition,
                             __FILE__, __LINE__, message)
              << std::endl;
}

#define debug_assert(cond, msg)                \
    if (!(cond)) {                       \
        printAssertFailed(#cond, (msg)); \
    }
#endif
