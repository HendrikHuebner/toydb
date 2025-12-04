#pragma once

#include <cstdint>
#include <vector>
#include <cstring>

namespace toydb {

/**
 * @brief Three-valued logic result for predicate evaluation
 *
 * Represents true, false, or null (unknown) for each row.
 */
enum class PredicateValue : uint8_t {
    FALSE = 0,
    TRUE = 1,
    NULL_VALUE = 2
};

/**
 * @brief Bitmask-based predicate result implementation
 *
 * Stores predicate evaluation results as a bitmask where each row
 * can be true, false, or null. Uses two bits per row to represent
 * three states.
 */
class BitmaskResult {
private:
    std::vector<uint8_t> bits_;  // Packed bits: 2 bits per row
    int64_t size_;

    static constexpr uint8_t TRUE_MASK = 0x01;
    static constexpr uint8_t NULL_MASK = 0x02;

    void setBit(int64_t index, uint8_t mask, bool value) noexcept {
        int64_t byteIdx = index / 4;
        int64_t bitOffset = (index % 4) * 2;

        if (value) {
            bits_[byteIdx] |= (mask << bitOffset);
        } else {
            bits_[byteIdx] &= ~(mask << bitOffset);
        }
    }

    bool getBit(int64_t index, uint8_t mask) const noexcept {
        int64_t byteIdx = index / 4;
        int64_t bitOffset = (index % 4) * 2;
        return (bits_[byteIdx] & (mask << bitOffset)) != 0;
    }

public:
    explicit BitmaskResult(int64_t size) : size_(size) {
        // 2 bits per row, 4 rows per byte
        int64_t bytesNeeded = (size + 3) / 4;
        bits_.resize(bytesNeeded, 0);
    }

    int64_t size() const noexcept {
        return size_;
    }

    void setTrue(int64_t index) noexcept {
        setBit(index, TRUE_MASK, true);
        setBit(index, NULL_MASK, false);
    }

    void setFalse(int64_t index) noexcept {
        setBit(index, TRUE_MASK, false);
        setBit(index, NULL_MASK, false);
    }

    void setNull(int64_t index) noexcept {
        setBit(index, TRUE_MASK, false);
        setBit(index, NULL_MASK, true);
    }

    PredicateValue get(int64_t index) const noexcept {
        if (getBit(index, NULL_MASK)) {
            return PredicateValue::NULL_VALUE;
        }
        return getBit(index, TRUE_MASK) ? PredicateValue::TRUE : PredicateValue::FALSE;
    }

    bool isTrue(int64_t index) const noexcept {
        return get(index) == PredicateValue::TRUE;
    }

    bool isFalse(int64_t index) const noexcept {
        return get(index) == PredicateValue::FALSE;
    }

    bool isNull(int64_t index) const noexcept {
        return get(index) == PredicateValue::NULL_VALUE;
    }

    /**
     * @brief Count the number of true values
     */
    int64_t count() const noexcept {
        int64_t cnt = 0;
        for (int64_t i = 0; i < size_; ++i) {
            if (isTrue(i)) {
                ++cnt;
            }
        }
        return cnt;
    }

    /**
     * @brief Combine with another result using AND logic (three-valued)
     */
    void combineAnd(const BitmaskResult& other) noexcept {
        for (int64_t i = 0; i < size_ && i < other.size_; ++i) {
            PredicateValue left = get(i);
            PredicateValue right = other.get(i);

            // Three-valued AND logic
            if (left == PredicateValue::FALSE || right == PredicateValue::FALSE) {
                setFalse(i);
            } else if (left == PredicateValue::NULL_VALUE || right == PredicateValue::NULL_VALUE) {
                setNull(i);
            } else {
                setTrue(i);
            }
        }
    }

    /**
     * @brief Combine with another result using OR logic (three-valued)
     */
    void combineOr(const BitmaskResult& other) noexcept {
        for (int64_t i = 0; i < size_ && i < other.size_; ++i) {
            PredicateValue left = get(i);
            PredicateValue right = other.get(i);

            // Three-valued OR logic
            if (left == PredicateValue::TRUE || right == PredicateValue::TRUE) {
                setTrue(i);
            } else if (left == PredicateValue::NULL_VALUE || right == PredicateValue::NULL_VALUE) {
                setNull(i);
            } else {
                setFalse(i);
            }
        }
    }

    /**
     * @brief Create a new result by combining this and other with AND
     */
    BitmaskResult andResult(const BitmaskResult& other) const {
        BitmaskResult result(size_);
        for (int64_t i = 0; i < size_; ++i) {
            result.set(i, get(i));
        }
        result.combineAnd(other);
        return result;
    }

    /**
     * @brief Create a new result by combining this and other with OR
     */
    BitmaskResult orResult(const BitmaskResult& other) const {
        BitmaskResult result(size_);
        for (int64_t i = 0; i < size_; ++i) {
            result.set(i, get(i));
        }
        result.combineOr(other);
        return result;
    }

    void set(int64_t index, PredicateValue value) noexcept {
        switch (value) {
            case PredicateValue::TRUE:
                setTrue(index);
                break;
            case PredicateValue::FALSE:
                setFalse(index);
                break;
            case PredicateValue::NULL_VALUE:
                setNull(index);
                break;
        }
    }

    void setAll(PredicateValue value) noexcept {
        for (int64_t i = 0; i < size_; ++i) {
            set(i, value);
        }
    }
};

/**
 * @brief Type-erased predicate result abstraction
 *
 * Can represent either bitmask or selection vector (future).
 * Currently uses BitmaskResult as the concrete implementation.
 */
class PredicateResultVector {
private:
    BitmaskResult bitmask_;

public:
    explicit PredicateResultVector(int64_t size) : bitmask_(size) {}

    int64_t size() const noexcept {
        return bitmask_.size();
    }

    void setTrue(int64_t index) noexcept {
        bitmask_.setTrue(index);
    }

    void setFalse(int64_t index) noexcept {
        bitmask_.setFalse(index);
    }

    void setNull(int64_t index) noexcept {
        bitmask_.setNull(index);
    }

    void set(int64_t index, PredicateValue value) noexcept {
        bitmask_.set(index, value);
    }

    void setAll(PredicateValue value) noexcept {
        bitmask_.setAll(value);
    }

    PredicateValue get(int64_t index) const noexcept {
        return bitmask_.get(index);
    }

    bool isTrue(int64_t index) const noexcept {
        return bitmask_.isTrue(index);
    }

    bool isFalse(int64_t index) const noexcept {
        return bitmask_.isFalse(index);
    }

    bool isNull(int64_t index) const noexcept {
        return bitmask_.isNull(index);
    }

    int64_t count() const noexcept {
        return bitmask_.count();
    }

    void combineAnd(const PredicateResultVector& other) noexcept {
        bitmask_.combineAnd(other.bitmask_);
    }

    void combineOr(const PredicateResultVector& other) noexcept {
        bitmask_.combineOr(other.bitmask_);
    }

    PredicateResultVector andResult(const PredicateResultVector& other) const {
        PredicateResultVector result(size());
        result.bitmask_ = bitmask_.andResult(other.bitmask_);
        return result;
    }

    PredicateResultVector orResult(const PredicateResultVector& other) const {
        PredicateResultVector result(size());
        result.bitmask_ = bitmask_.orResult(other.bitmask_);
        return result;
    }
};

} // namespace toydb
