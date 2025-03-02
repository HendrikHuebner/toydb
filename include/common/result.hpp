
#include <ostream>
#include <type_traits>

namespace toydb {

template <typename T>
concept ErrorType =  std::convertible_to<T, bool> && std::is_default_constructible_v<T>;

struct Error {
    const bool isError{false};

    operator bool() const {
        return isError;
    }
};

struct ErrorMessage {
    const std::string message;

    operator bool() const {
        return !message.empty();
    }
};

template <ErrorType error_t_>
struct ResultBase {
    
    using error_t = error_t_;

    ResultBase(error_t&& error) noexcept : error(error) {}

    /**
     * @brief Returns true if result is not an error
     * 
     * @return true 
     * @return false 
     */
    bool ok() const noexcept { return !isError(); }

    /**
     * @brief Returns true if result is an error
     * 
     * @return true 
     * @return false 
     */
    bool isError() const noexcept { return static_cast<bool>(error); }

    const error_t& getError() const noexcept {
        return error;
    }

    /**
     * @brief Invokes callback if result is erroneous
     * 
     * @param callback 
     */
    template<typename callback_t> requires std::is_nothrow_invocable_v<callback_t, error_t&>
    void onError(const callback_t& callback) noexcept {
        if (isError()) {
            callback(error);
        }
    }

protected:
    error_t error{};
};

template <typename data_t_, ErrorType error_t_ = Error>
struct Result : public ResultBase<error_t_> {
    using data_t = data_t_;
    using error_t = error_t_;
    using base_t = ResultBase<error_t>;

    Result(error_t error) noexcept : ResultBase<error_t>(error) {}
    
    Result(data_t data) noexcept : data(data) {}

    const data_t& get() const noexcept {
        return data;
    }

    /**
     * @brief Returns data if successful, otherwise returns alternative
     * 
     * @param alternative 
     * @return data_t& 
     */
    data_t& orElse(data_t& alternative) noexcept {
        return reinterpret_cast<base_t>(*this).isError() ? alternative : data;
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    template<typename callback_t> requires std::is_nothrow_invocable_v<callback_t, data_t&>
    void onSuccess(const callback_t& callback) noexcept {
        if (reinterpret_cast<base_t>(*this).ok()) {
            callback(data);
        }
    }

private:
    data_t data;
};

template <>
struct Result<void> : public ResultBase<Error> {
    using data_t = void;
    using error_t = Error;
    using base_t = ResultBase<error_t>;

    Result(Error) noexcept : base_t({true}) {}
};

template<typename data_t_, typename error_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, error_t_>& result) {
    return (result.isError()) ? 
        os << "Result<Error: " << result.getError() << ">" : 
        os << "Result<Ok: " << result.get() << ">";
}

template<typename data_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, Error>& result) {
    return (result.isError()) ? 
        os << "Result<Error>" : 
        os << "Result<Ok: " << result.get() << ">";
}

inline std::ostream& operator<<(std::ostream& os, const Result<void, Error>& result) {
    return (result.isError()) ? 
        os << "Result<Error>" : 
        os << "Result<Ok>";
}

} // namespace toydb
