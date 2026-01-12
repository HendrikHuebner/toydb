#include "test_helpers.hpp"
#include <sstream>
#include "common/logging.hpp"

namespace toydb::test {

bool compareASTNodes(const toydb::ast::ASTNode* expected, const toydb::ast::ASTNode* actual,
                     const std::string& path) {
    using namespace toydb::ast;

    if (!expected && !actual) {
        return true;
    }

    if (!expected || !actual) {
        std::stringstream ss;
        ss << "AST mismatch at " << path << ": ";
        if (!expected) {
            ss << "expected is null but actual is not";
        } else {
            ss << "actual is null but expected is not";
        }
        toydb::Logger::error("{}", ss.str());
        return false;
    }

    // Compare Insert nodes
    if (auto* expInsert = dynamic_cast<const Insert*>(expected)) {
        auto* actInsert = dynamic_cast<const Insert*>(actual);
        if (!actInsert) {
            toydb::Logger::error("AST mismatch at {}: expected Insert but got different type",
                                 path);
            return false;
        }

        if (expInsert->tableName != actInsert->tableName) {
            toydb::Logger::error("AST mismatch at {}.tableName: expected '{}' but got '{}'", path,
                                 expInsert->tableName, actInsert->tableName);
            return false;
        }

        if (expInsert->columnNames.size() != actInsert->columnNames.size()) {
            toydb::Logger::error("AST mismatch at {}.columnNames: expected {} columns but got {}",
                                 path, expInsert->columnNames.size(),
                                 actInsert->columnNames.size());
            return false;
        }

        for (size_t i = 0; i < expInsert->columnNames.size(); ++i) {
            if (expInsert->columnNames[i] != actInsert->columnNames[i]) {
                toydb::Logger::error(
                    "AST mismatch at {}.columnNames[{}]: expected '{}' but got '{}'", path, i,
                    expInsert->columnNames[i], actInsert->columnNames[i]);
                return false;
            }
        }

        if (expInsert->values.size() != actInsert->values.size()) {
            toydb::Logger::error("AST mismatch at {}.values: expected {} rows but got {}", path,
                                 expInsert->values.size(), actInsert->values.size());
            return false;
        }

        for (size_t i = 0; i < expInsert->values.size(); ++i) {
            if (expInsert->values[i].size() != actInsert->values[i].size()) {
                toydb::Logger::error("AST mismatch at {}.values[{}]: expected {} values but got {}",
                                     path, i, expInsert->values[i].size(),
                                     actInsert->values[i].size());
                return false;
            }

            for (size_t j = 0; j < expInsert->values[i].size(); ++j) {
                std::stringstream valuePath;
                valuePath << path << ".values[" << i << "][" << j << "]";
                if (!compareASTNodes(expInsert->values[i][j].get(), actInsert->values[i][j].get(),
                                     valuePath.str())) {
                    return false;
                }
            }
        }

        return true;
    }

    // Compare Update nodes
    if (auto* expUpdate = dynamic_cast<const Update*>(expected)) {
        auto* actUpdate = dynamic_cast<const Update*>(actual);
        if (!actUpdate) {
            toydb::Logger::error("AST mismatch at {}: expected Update but got different type",
                                 path);
            return false;
        }

        if (expUpdate->tableName != actUpdate->tableName) {
            toydb::Logger::error("AST mismatch at {}.tableName: expected '{}' but got '{}'", path,
                                 expUpdate->tableName, actUpdate->tableName);
            return false;
        }

        if (expUpdate->assignments.size() != actUpdate->assignments.size()) {
            toydb::Logger::error(
                "AST mismatch at {}.assignments: expected {} assignments but got {}", path,
                expUpdate->assignments.size(), actUpdate->assignments.size());
            return false;
        }

        for (size_t i = 0; i < expUpdate->assignments.size(); ++i) {
            if (expUpdate->assignments[i].first != actUpdate->assignments[i].first) {
                toydb::Logger::error(
                    "AST mismatch at {}.assignments[{}].column: expected '{}' but got '{}'", path,
                    i, expUpdate->assignments[i].first, actUpdate->assignments[i].first);
                return false;
            }

            std::stringstream assignPath;
            assignPath << path << ".assignments[" << i << "].value";
            if (!compareASTNodes(expUpdate->assignments[i].second.get(),
                                 actUpdate->assignments[i].second.get(), assignPath.str())) {
                return false;
            }
        }

        if ((expUpdate->where == nullptr) != (actUpdate->where == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.where: one is null and the other is not",
                                 path);
            return false;
        }

        if (expUpdate->where) {
            if (!compareASTNodes(expUpdate->where.get(), actUpdate->where.get(), path + ".where")) {
                return false;
            }
        }

        return true;
    }

    // Compare Delete nodes
    if (auto* expDelete = dynamic_cast<const Delete*>(expected)) {
        auto* actDelete = dynamic_cast<const Delete*>(actual);
        if (!actDelete) {
            toydb::Logger::error("AST mismatch at {}: expected Delete but got different type",
                                 path);
            return false;
        }

        if (expDelete->tableName != actDelete->tableName) {
            toydb::Logger::error("AST mismatch at {}.tableName: expected '{}' but got '{}'", path,
                                 expDelete->tableName, actDelete->tableName);
            return false;
        }

        if ((expDelete->where == nullptr) != (actDelete->where == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.where: one is null and the other is not",
                                 path);
            return false;
        }

        if (expDelete->where) {
            if (!compareASTNodes(expDelete->where.get(), actDelete->where.get(), path + ".where")) {
                return false;
            }
        }

        return true;
    }

    // Compare CreateTable nodes
    if (auto* expCreate = dynamic_cast<const CreateTable*>(expected)) {
        auto* actCreate = dynamic_cast<const CreateTable*>(actual);
        if (!actCreate) {
            toydb::Logger::error("AST mismatch at {}: expected CreateTable but got different type",
                                 path);
            return false;
        }

        if (expCreate->tableName != actCreate->tableName) {
            toydb::Logger::error("AST mismatch at {}.tableName: expected '{}' but got '{}'", path,
                                 expCreate->tableName, actCreate->tableName);
            return false;
        }

        if (expCreate->columns.size() != actCreate->columns.size()) {
            toydb::Logger::error("AST mismatch at {}.columns: expected {} columns but got {}", path,
                                 expCreate->columns.size(), actCreate->columns.size());
            return false;
        }

        for (size_t i = 0; i < expCreate->columns.size(); ++i) {
            if (expCreate->columns[i].name != actCreate->columns[i].name) {
                toydb::Logger::error(
                    "AST mismatch at {}.columns[{}].name: expected '{}' but got '{}'", path, i,
                    expCreate->columns[i].name, actCreate->columns[i].name);
                return false;
            }

            if (expCreate->columns[i].type != actCreate->columns[i].type) {
                toydb::Logger::error(
                    "AST mismatch at {}.columns[{}].type: expected '{}' but got '{}'", path, i,
                    expCreate->columns[i].type.toString(), actCreate->columns[i].type.toString());
                return false;
            }
        }

        return true;
    }

    // Compare ConstantString nodes
    if (auto* expConstString = dynamic_cast<const ConstantString*>(expected)) {
        auto* actConstString = dynamic_cast<const ConstantString*>(actual);
        if (!actConstString) {
            toydb::Logger::error("AST mismatch at {}: expected ConstantString but got different type",
                                 path);
            return false;
        }

        if (expConstString->value != actConstString->value) {
            toydb::Logger::error("AST mismatch at {}.value: expected '{}' but got '{}'", path,
                                 expConstString->value, actConstString->value);
            return false;
        }

        return true;
    }

    // Compare ConstantInt nodes
    if (auto* expConstInt = dynamic_cast<const ConstantInt*>(expected)) {
        auto* actConstInt = dynamic_cast<const ConstantInt*>(actual);
        if (!actConstInt) {
            toydb::Logger::error("AST mismatch at {}: expected ConstantInt but got different type",
                                 path);
            return false;
        }

        if (expConstInt->value != actConstInt->value || expConstInt->isInt64 != actConstInt->isInt64) {
            toydb::Logger::error("AST mismatch at {}.value: expected {} (isInt64: {}) but got {} (isInt64: {})", path,
                                 expConstInt->value, expConstInt->isInt64, actConstInt->value, actConstInt->isInt64);
            return false;
        }

        return true;
    }

    // Compare ConstantDouble nodes
    if (auto* expConstDouble = dynamic_cast<const ConstantDouble*>(expected)) {
        auto* actConstDouble = dynamic_cast<const ConstantDouble*>(actual);
        if (!actConstDouble) {
            toydb::Logger::error("AST mismatch at {}: expected ConstantDouble but got different type",
                                 path);
            return false;
        }

        if (expConstDouble->value != actConstDouble->value) {
            toydb::Logger::error("AST mismatch at {}.value: expected {} but got {}", path,
                                 expConstDouble->value, actConstDouble->value);
            return false;
        }

        return true;
    }

    // Compare ConstantBool nodes
    if (auto* expConstBool = dynamic_cast<const ConstantBool*>(expected)) {
        auto* actConstBool = dynamic_cast<const ConstantBool*>(actual);
        if (!actConstBool) {
            toydb::Logger::error("AST mismatch at {}: expected ConstantBool but got different type",
                                 path);
            return false;
        }

        if (expConstBool->value != actConstBool->value) {
            toydb::Logger::error("AST mismatch at {}.value: expected {} but got {}", path,
                                 expConstBool->value, actConstBool->value);
            return false;
        }

        return true;
    }

    // Compare ConstantNull nodes
    if (dynamic_cast<const ConstantNull*>(expected)) {
        if (!dynamic_cast<const ConstantNull*>(actual)) {
            toydb::Logger::error("AST mismatch at {}: expected ConstantNull but got different type",
                                 path);
            return false;
        }
        return true;
    }

    // Compare Condition nodes
    if (auto* expCondition = dynamic_cast<const Condition*>(expected)) {
        auto* actCondition = dynamic_cast<const Condition*>(actual);
        if (!actCondition) {
            toydb::Logger::error("AST mismatch at {}: expected Condition but got different type",
                                 path);
            return false;
        }

        if (expCondition->op != actCondition->op) {
            toydb::Logger::error("AST mismatch at {}.op: operators differ", path);
            return false;
        }

        if ((expCondition->left == nullptr) != (actCondition->left == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.left: one is null and the other is not", path);
            return false;
        }

        if (expCondition->left) {
            if (!compareASTNodes(expCondition->left.get(), actCondition->left.get(),
                                 path + ".left")) {
                return false;
            }
        }

        if ((expCondition->right == nullptr) != (actCondition->right == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.right: one is null and the other is not",
                                 path);
            return false;
        }

        if (expCondition->right) {
            if (!compareASTNodes(expCondition->right.get(), actCondition->right.get(),
                                 path + ".right")) {
                return false;
            }
        }

        return true;
    }

    // Compare Column nodes
    if (auto* expColumn = dynamic_cast<const ColumnRef*>(expected)) {
        auto* actColumn = dynamic_cast<const ColumnRef*>(actual);
        if (!actColumn) {
            toydb::Logger::error("AST mismatch at {}: expected Column but got different type",
                                 path);
            return false;
        }

        if (expColumn->name != actColumn->name) {
            toydb::Logger::error("AST mismatch at {}.name: expected '{}' but got '{}'", path,
                                 expColumn->name, actColumn->name);
            return false;
        }

        if (expColumn->alias != actColumn->alias) {
            toydb::Logger::error("AST mismatch at {}.alias: expected '{}' but got '{}'", path,
                                 expColumn->alias, actColumn->alias);
            return false;
        }

        return true;
    }

    // Compare Table nodes
    if (auto* expTable = dynamic_cast<const Table*>(expected)) {
        auto* actTable = dynamic_cast<const Table*>(actual);
        if (!actTable) {
            toydb::Logger::error("AST mismatch at {}: expected Table but got different type", path);
            return false;
        }

        if (expTable->name != actTable->name) {
            toydb::Logger::error("AST mismatch at {}.name: expected '{}' but got '{}'", path,
                                 expTable->name, actTable->name);
            return false;
        }

        if (expTable->alias != actTable->alias) {
            toydb::Logger::error("AST mismatch at {}.alias: expected '{}' but got '{}'", path,
                                 expTable->alias, actTable->alias);
            return false;
        }

        return true;
    }

    // Compare TableExpr nodes
    if (auto* expTableExpr = dynamic_cast<const TableExpr*>(expected)) {
        auto* actTableExpr = dynamic_cast<const TableExpr*>(actual);
        if (!actTableExpr) {
            toydb::Logger::error("AST mismatch at {}: expected TableExpr but got different type",
                                 path);
            return false;
        }

        if (!compareASTNodes(&expTableExpr->table, &actTableExpr->table, path + ".table")) {
            return false;
        }

        if ((expTableExpr->join == nullptr) != (actTableExpr->join == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.join: one is null and the other is not", path);
            return false;
        }

        if (expTableExpr->join) {
            if (!compareASTNodes(expTableExpr->join.get(), actTableExpr->join.get(),
                                 path + ".join")) {
                return false;
            }
        }

        if ((expTableExpr->condition == nullptr) != (actTableExpr->condition == nullptr)) {
            toydb::Logger::error("AST mismatch at {}.condition: one is null and the other is not",
                                 path);
            return false;
        }

        if (expTableExpr->condition) {
            if (!compareASTNodes(expTableExpr->condition.get(), actTableExpr->condition.get(),
                                 path + ".condition")) {
                return false;
            }
        }

        return true;
    }

    // Compare ColumnDefinition nodes
    if (auto* expColDef = dynamic_cast<const ColumnDefinition*>(expected)) {
        auto* actColDef = dynamic_cast<const ColumnDefinition*>(actual);
        if (!actColDef) {
            toydb::Logger::error(
                "AST mismatch at {}: expected ColumnDefinition but got different type", path);
            return false;
        }

        if (expColDef->name != actColDef->name) {
            toydb::Logger::error("AST mismatch at {}.name: expected '{}' but got '{}'", path,
                                 expColDef->name, actColDef->name);
            return false;
        }

        if (expColDef->type != actColDef->type) {
            toydb::Logger::error("AST mismatch at {}.type: expected '{}' but got '{}'", path,
                                 expColDef->type.toString(), actColDef->type.toString());
            return false;
        }

        return true;
    }

    // Unknown node type
    toydb::Logger::error("AST mismatch at {}: unknown AST node type", path);
    return false;
}

bool compareQueryAST(const toydb::ast::QueryAST& expected, const toydb::ast::QueryAST& actual) {
    if ((expected.query_ == nullptr) != (actual.query_ == nullptr)) {
        toydb::Logger::error("QueryAST mismatch: one query is null and the other is not");
        return false;
    }

    if (expected.query_) {
        return compareASTNodes(expected.query_.get(), actual.query_.get(), "root");
    }

    return true;
}

}  // namespace toydb::test
