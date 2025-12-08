#pragma once

#include <memory>
#include <ostream>
#include <vector>
#include "common/assert.hpp"
#include "common/types.hpp"
#include "engine/predicate_expr.hpp"

namespace toydb {

enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL_OUTER,
    CROSS,
};


/**
 * @brief Logical operator base class. Represents a node in a DAG of relational operators.
 */
class LogicalOperator {
protected:
    std::vector<std::shared_ptr<LogicalOperator>> children_;
    std::vector<LogicalOperator*> parents_;

public:
    LogicalOperator() = default;
    virtual ~LogicalOperator() = default;

    void addChild(std::shared_ptr<LogicalOperator> child) {
        if (child) {
            child->addParent(this);
            children_.push_back(child);
        }
    }

    const std::vector<std::shared_ptr<LogicalOperator>>& getChildren() const noexcept {
        return children_;
    }

    const std::vector<LogicalOperator*>& getParents() const noexcept {
        return parents_;
    }

    void addParent(LogicalOperator* parent) {
        if (parent) {
            parents_.push_back(parent);
        }
    }

    size_t getChildCount() const noexcept {
        return children_.size();
    }

    size_t getParentCount() const noexcept {
        return parents_.size();
    }

    std::shared_ptr<LogicalOperator> getChild(size_t index) const {
        tdb_assert(index < children_.size(), "Child index out of range");
        return children_[index];
    }

    virtual std::ostream& print(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const LogicalOperator& op) {
        return op.print(os);
    }
};

/**
 * @brief Root node of a logical query plan.
 */
class LogicalQueryPlan {
private:
    std::shared_ptr<LogicalOperator> root_;

public:
    LogicalQueryPlan() = default;

    explicit LogicalQueryPlan(std::shared_ptr<LogicalOperator> root)
        : root_(root) {}

    LogicalQueryPlan(const LogicalQueryPlan&) = delete;
    LogicalQueryPlan& operator=(const LogicalQueryPlan&) = delete;

    LogicalQueryPlan(LogicalQueryPlan&&) = default;
    LogicalQueryPlan& operator=(LogicalQueryPlan&&) = default;

    void setRoot(std::shared_ptr<LogicalOperator> root) {
        root_ = root;
    }

    LogicalOperator* getRoot() const noexcept {
        return root_.get();
    }

    bool hasRoot() const noexcept {
        return root_ != nullptr;
    }

    std::ostream& print(std::ostream& os) const {
        if (root_) {
            root_->print(os);
        } else {
            os << "[Empty Query Plan]";
        }
        return os;
    }

    friend std::ostream& operator<<(std::ostream& os, const LogicalQueryPlan& plan) {
        return plan.print(os);
    }
};

class ProjectionOp : public LogicalOperator {
private:
    std::vector<ColumnId> columns_;

public:
    explicit ProjectionOp(std::vector<ColumnId> columns)
        : columns_(std::move(columns)) {}

    const std::vector<ColumnId>& getColumns() const noexcept {
        return columns_;
    }

    std::ostream& print(std::ostream& os) const override {
        os << "Projection[";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) os << ", ";
            os << columns_[i].getName();
        }
        os << "]";
        return os;
    }
};

class FilterOp : public LogicalOperator {
private:
    std::unique_ptr<PredicateExpr> predicate_;

public:
    explicit FilterOp(std::unique_ptr<PredicateExpr> predicate)
        : predicate_(std::move(predicate)) {}

    const PredicateExpr* getPredicate() const noexcept {
        return predicate_.get();
    }

    PredicateExpr* getPredicate() noexcept {
        return predicate_.get();
    }

    std::ostream& print(std::ostream& os) const override {
        os << "Filter[";
        os << "predicate]";
        return os;
    }
};

class JoinOp : public LogicalOperator {
private:
    JoinType joinType_;
    std::unique_ptr<PredicateExpr> condition_;

public:
    JoinOp(JoinType joinType, std::unique_ptr<PredicateExpr> condition)
        : joinType_(joinType), condition_(std::move(condition)) {}

    JoinType getJoinType() const noexcept {
        return joinType_;
    }

    const PredicateExpr* getCondition() const noexcept {
        return condition_.get();
    }

    PredicateExpr* getCondition() noexcept {
        return condition_.get();
    }

    std::ostream& print(std::ostream& os) const override {
        const char* joinTypeStr = "UNKNOWN";
        switch (joinType_) {
            case JoinType::INNER:
                joinTypeStr = "INNER";
                break;
            case JoinType::LEFT:
                joinTypeStr = "LEFT";
                break;
            case JoinType::RIGHT:
                joinTypeStr = "RIGHT";
                break;
            case JoinType::FULL_OUTER:
                joinTypeStr = "FULL_OUTER";
                break;
            case JoinType::CROSS:
                joinTypeStr = "CROSS";
                break;
        }
        os << "Join[" << joinTypeStr;
        if (condition_) {
            os << ", condition";
        }
        os << "]";
        return os;
    }
};

class CrossProductOp : public LogicalOperator {
public:
    CrossProductOp() {}

    std::ostream& print(std::ostream& os) const override {
        os << "CrossProduct";
        return os;
    }
};

class ColumnRefOp : public LogicalOperator {
private:
    ColumnId columnId_;

public:
    explicit ColumnRefOp(const ColumnId& columnId)
        : columnId_(columnId) {}

    const ColumnId& getColumnId() const noexcept {
        return columnId_;
    }

    std::ostream& print(std::ostream& os) const override {
        os << "ColumnRef[" << columnId_.getName() << "]";
        return os;
    }
};

class ConstantOp : public LogicalOperator {
private:
    DataType type_;
    union {
        int64_t intValue_;
        double doubleValue_;
        bool boolValue_;
    };
    std::string stringValue_;

public:
    explicit ConstantOp(DataType type, int64_t value)
        : type_(type), intValue_(value) {}

    explicit ConstantOp(DataType type, double value)
        : type_(type), doubleValue_(value) {}

    explicit ConstantOp(DataType type, const std::string& value)
        : type_(type), stringValue_(value) {}

    explicit ConstantOp(DataType type, bool value)
        : type_(type), boolValue_(value) {}

    explicit ConstantOp(DataType type)
        : type_(type) {}

    DataType getType() const noexcept {
        return type_;
    }

    int64_t getIntValue() const {
        return intValue_;
    }

    double getDoubleValue() const {
        return doubleValue_;
    }

    const std::string& getStringValue() const {
        return stringValue_;
    }

    bool getBoolValue() const {
        return boolValue_;
    }

    bool isNull() const noexcept {
        return type_ == DataType::getNullConst();
    }

    std::ostream& print(std::ostream& os) const override {
        os << "Constant[";
        if (isNull()) {
            os << "NULL";
        } else {
            switch (type_.getType()) {
                case DataType::Type::INT32:
                case DataType::Type::INT64:
                    os << intValue_;
                    break;
                case DataType::Type::DOUBLE:
                    os << doubleValue_;
                    break;
                case DataType::Type::BOOL:
                    os << (boolValue_ ? "true" : "false");
                    break;
                case DataType::Type::STRING:
                    os << "'" << stringValue_ << "'";
                    break;
                default:
                    os << "UNKNOWN";
                    break;
            }
        }
        os << "]";
        return os;
    }
};

} // namespace toydb
