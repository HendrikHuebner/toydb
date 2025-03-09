#pragma once

#include <bits/types/error_t.h>
#include <ostream>
#include <type_traits>
#include "assert.hpp"
namespace toydb {

template<typename lambda_t, typename... Args>
concept NoThrowLambda = std::is_nothrow_invocable_v<lambda_t, Args...>;

namespace detail {

    struct Success {};
    struct Error {};
    struct Empty {};

    template <typename data_t_, typename error_t_>
    struct Storage {
        static constexpr bool hasData = !std::is_void_v<data_t_>;
        static constexpr bool hasError = !std::is_void_v<error_t_>;

        using data_t = std::conditional<hasData, data_t_, Empty>::type;
        using error_t = std::conditional<hasError, error_t_, Empty>::type;

        union Content {
            data_t data;
            error_t error;
            ~Content() {}
        } content;

        bool isError;

        template<typename arg_t>
        Storage(std::true_type, arg_t&& arg) noexcept
            : isError(true) {
            new (&content.error) error_t(std::forward<arg_t>(arg));
        }

        template<typename arg_t>
        Storage(std::false_type, arg_t&& arg) noexcept
            : isError(false) {
            new (&content.data) data_t(std::forward<arg_t>(arg));
        }

        template<bool isError_v, typename other_data_t, typename other_error_t>
        Storage(std::bool_constant<isError_v>, const Storage<other_data_t, other_error_t>& other) noexcept
            : isError(isError_v) {
            if constexpr (isError_v) {
                new (&content.error) error_t(other.content.error);
            } else {
                new (&content.data) data_t(other.content.data);
            }
        }

        template<bool isError_v, typename other_data_t, typename other_error_t>
        Storage(std::bool_constant<isError_v>, Storage<other_data_t, other_error_t>&& other) noexcept
            : isError(isError_v) {
            if constexpr (isError_v) {
                new (&content.error) error_t(std::move(other.content.error));
            } else {
                new (&content.data) data_t(std::move(other.content.data));
            }
        }

        ~Storage() {
            if (isError)
                content.data.~data_t();
            else
                content.error.~error_t();
        }

        data_t& getData() noexcept {
            return content.data;
        }

        error_t& getError() noexcept {
            return content.error;
        }
    };


    template<>
    struct Storage<void, void> {
        static constexpr bool hasData = false;
        static constexpr bool hasError = false;
        
        union { Empty data; Empty error; } content;
        bool isError;
    };
}

/**
 * @brief Zero-overhead result type. Stores either a result, an error or neither.
 * 
 */
template <typename data_t_ = void, typename error_t_ = void>
struct Result {
    
    using data_t = data_t_;
    using error_t = error_t_;
    using storage_t = detail::Storage<data_t, error_t>;

    template <typename... Args, bool isError_v, typename arg_t = typename std::conditional<isError_v, error_t, data_t>::type>
    Result(std::bool_constant<isError_v>, Args&&... args) noexcept
        : storage(storage_t{std::bool_constant<isError_v>{}, arg_t(std::forward<Args>(args)...)}) {
    }

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
    bool isError() const noexcept { return storage.isError; }

    /**
     * @brief Returns the data from storage. If the result is an error the behavior is undefined.
     */
    template <typename data_t = data_t> requires storage_t::hasData
    data_t& get() noexcept {
        debug_assert(ok(), "Trying to access result data but result is an error");
        return storage.getData();
    }

    /**
     * @brief Returns the error from storage. If the result is not an error the behavior is undefined.
     */
    template <typename error_t = error_t> requires storage_t::hasError
    error_t& getError() noexcept {
        debug_assert(isError(), "Trying to access result error but result is ok");
        return storage.getError();
    }

    /**
     * @brief Returns data if successful, otherwise returns alternative
     * 
     * @param alternative 
     * @return data_t& 
     */
    template <typename data_t = data_t> requires storage_t::hasData
    data_t& orElse(data_t& alternative) noexcept {
        return isError() ? alternative : storage.getData();
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    template <NoThrowLambda<data_t&> callback_t, typename data_t = data_t> requires storage_t::hasData
    void onSuccess(const callback_t& callback) noexcept {
        if (ok()) {
            callback(storage.getData());
        }
    }

    /**
     * @brief Invokes callback if result is successful
     * 
     * @param callback 
     */
    template <NoThrowLambda<error_t&> callback_t, typename error_t = error_t> requires storage_t::hasError
    void onError(const callback_t& callback) noexcept {
        if (isError()) {
            callback(storage.getError());
        }
    }

    storage_t storage;

    template<typename... Args>
    static Result make_success(Args&&... args) {
        return Result(std::bool_constant<false>{}, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static Result make_error(Args&&... args) {
        return Result(std::bool_constant<true>{}, std::forward<Args>(args)...);
    }
};


namespace result_ {

template<typename... Args>
inline auto error(Args&&... args) {
    return Result(std::forward<Args>(args)..., detail::Error{});
}

template<typename... Args>
inline auto success(Args&&... args) {
    return Result(std::forward<Args>(args)..., detail::Success{});
}

} // namespace result

template<typename data_t_, typename error_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, error_t_>& result) {
    return (result.isError()) ? 
        os << "Result<Error: " << result.getError() << ">" : 
        os << "Result<Ok: " << result.get() << ">";
}

template<typename data_t_>
std::ostream& operator<<(std::ostream& os, const Result<data_t_, void>& result) {
    return (result.isError()) ? 
        os << "Result<Error>" : 
        os << "Result<Ok: " << result.get() << ">";
}

inline std::ostream& operator<<(std::ostream& os, const Result<void, void>& result) {
    return (result.isError()) ? 
        os << "Result<Error>" : 
        os << "Result<Ok>";
}

} // namespace toydb
