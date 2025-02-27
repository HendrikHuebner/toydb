
#include <bits/types/error_t.h>
#include <functional>
#include <ostream>
#include "concepts.hpp"

namespace toydb {

template <BoolConvertible error_t_>
struct ResultBase {
    
    using error_t = error_t_;

    ResultBase(error_t&& error) : error(error) {}

    /**
     * @brief Returns true if result is not an error
     * 
     * @return true 
     * @return false 
     */
    bool ok() const { return !isError(); }

    /**
     * @brief Returns true if result is an error
     * 
     * @return true 
     * @return false 
     */
    bool isError() const { return static_cast<bool>(error); }

    const error_t& getError() const {
        return error;
    }

    /**
     * @brief Invokes callback if result is erroneous
     * 
     * @param callback 
     */
    void onError(const std::function<void(const error_t&)>& callback) {
        if (isError()) {
            callback(error);
        }
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    void onSuccess(const std::function<void(void)>& callback) {
        if (ok()) {
            callback();
        }
    }

protected:
    error_t error{};
};

template <typename data_t_, BoolConvertible error_t_ = bool>
struct Result : public ResultBase<error_t_> {
    using data_t = data_t_;
    using error_t = error_t_;
    using base_t = ResultBase<error_t>;

    const data_t& get() const {
        return data;
    }

    /**
     * @brief Returns data if successful, otherwise returns alternative
     * 
     * @param alternative 
     * @return data_t& 
     */
    data_t& orElse(data_t& alternative) {
        return reinterpret_cast<base_t>(*this).isError() ? alternative : data;
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    void onSuccess(const std::function<void(data_t&)>& callback) {
        if (reinterpret_cast<base_t>(*this).ok()) {
            callback(data);
        }
    }

private:
    data_t data{};
};

struct Error {};

template <>
struct Result<void> : public ResultBase<bool> {
    using data_t = void;
    using error_t = bool;
    using base_t = ResultBase<error_t>;

    Result(Error) : base_t(true) {}
};

template <typename data_t_>
struct Result<data_t_> : public ResultBase<bool> {
    using data_t = data_t_;
    using error_t = bool;
    using base_t = ResultBase<error_t>;

    Result(Error) : base_t(true) {}

    const data_t& get() const {
        return data;
    }

    /**
     * @brief Returns data if successful, otherwise returns alternative
     * 
     * @param alternative 
     * @return data_t& 
     */
    data_t& orElse(data_t& alternative) {
        return reinterpret_cast<base_t>(*this).isError() ? alternative : data;
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    void onSuccess(const std::function<void(data_t&)>& callback) {
        if (reinterpret_cast<base_t>(*this).ok()) {
            callback(data);
        }
    }

private:
    data_t data{};
};

template<typename data_t_, typename error_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, error_t_&> result) {
    return (result.isError()) ? 
        os << "Result<Error: " << result.getError() << ">" : 
        os << "Result<Ok: " << result.get() << ">";
}

template<typename data_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, bool&> result) {
    return (result.isError()) ? 
        os << "Result<Error>" : 
        os << "Result<Ok: " << result.get() << ">";
}

inline std::ostream& operator<<(std::ostream& os, const Result<void, bool> result) {
    return (result.isError()) ? 
        os << "Result<Error: " << result.getError() << ">" : 
        os << "Result<Ok>";
}

} // namespace toydb
