#pragma once

#include <ostream>
#include <vector>
#include "common/assert.hpp"

namespace toydb {

namespace plan {

enum struct InstructionType {
    CREATE_TABLE,
};

struct Value {

};

struct Instruction {
    std::vector<Value> arguments;
};


struct QueryPlan {
    std::vector<Instruction> instructions;
};

} // namespace plan
} // namespace toydb
