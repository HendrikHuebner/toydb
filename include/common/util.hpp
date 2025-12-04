#pragma once

#define tdb_likely(x) __builtin_expect(!!(x), 1)
#define tdb_unlikely(x) __builtin_expect(!!(x), 0)
