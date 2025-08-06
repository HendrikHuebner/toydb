#include "toydb.hpp"
#include "parser/query_ast.hpp"
#include "engine/table.hpp"
#include "engine/interpreter.hpp"
#include "planner/query_plan.hpp"
#include "planner/planner.hpp"
#include "memory/memory.hpp"
#include "common/logging.hpp"
#include "common/types.hpp"
#include "memory/memory.hpp"
#include "engine/database.hpp"


void foo() {
    toydb::ast::Literal l("foo");
}