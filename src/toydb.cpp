#include "toydb.hpp"
#include "parser/query_ast.hpp"
#include "engine/relation.hpp"
#include "planner/query_plan.hpp"
#include "engine/memory.hpp"
#include "common/logging.hpp"
#include "common/types.hpp"
#include "engine/join.hpp"

void foo() {
    toydb::ast::Literal l("foo");
}