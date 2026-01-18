#pragma once

#include <optional>
#include <stdexcept>
#include <string>

namespace toydb {

class SQLException : public std::runtime_error {
   public:
    explicit SQLException(const std::string& message, std::optional<std::string> originQuerySQL = std::nullopt)
        : std::runtime_error(message), query_(std::move(originQuerySQL)) {}

    explicit SQLException(const char* message, std::optional<std::string> originQuerySQL = std::nullopt)
        : std::runtime_error(message), query_(std::move(originQuerySQL)) {}

    const std::optional<std::string>& getSql() const noexcept { return query_; }

   private:
    std::optional<std::string> query_;
};

class ParserException : public SQLException {
   public:
    ParserException(const std::string& message, size_t line, size_t position,
                    std::string_view query)
        : SQLException(message + " at line " + std::to_string(line) + ", position " +
                       std::to_string(position), std::string(query)),
          line_(line),
          position_(position) {}

    size_t getLine() const noexcept { return line_; }
    size_t getPosition() const noexcept { return position_; }

   private:
    size_t line_;
    size_t position_;
};

class SQLRuntimeException : public SQLException {
   public:
    explicit SQLRuntimeException(const std::string& message, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException(message, std::move(originQuerySQL)) {}

    explicit SQLRuntimeException(const char* message, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException(message, std::move(originQuerySQL)) {}
};

class NotYetImplementedError : public SQLException {
   public:
    explicit NotYetImplementedError(const std::string& feature, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException("Not yet implemented: " + feature, std::move(originQuerySQL)),
          feature_(feature) {}

    explicit NotYetImplementedError(const char* feature, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException(std::string("Not yet implemented: ") + feature, std::move(originQuerySQL)),
          feature_(feature) {}

    const std::string& getFeature() const noexcept { return feature_; }

   private:
    std::string feature_;
};

class UnresolvedColumnException : public SQLException {
   public:
    explicit UnresolvedColumnException(const std::string& message, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException(message, std::move(originQuerySQL)) {}

    explicit UnresolvedColumnException(const char* message, std::optional<std::string> originQuerySQL = std::nullopt)
        : SQLException(message, std::move(originQuerySQL)) {}
};

class InternalSQLError : public SQLException {
   public:
    explicit InternalSQLError(const std::string& message)
        : SQLException("Internal error: " + message) {}

    explicit InternalSQLError(const char* message)
        : SQLException(std::string("Internal error: ") + message) {}
};

}  // namespace toydb
