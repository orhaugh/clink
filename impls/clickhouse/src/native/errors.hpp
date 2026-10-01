#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace clink::clickhouse::native {

// Every refusal and permanent failure the native sink raises. what() is
// "[<code>] <message>"; code() is the stable identifier the docs, the
// refusals metric and the tests key on.
class NativeSinkError : public std::runtime_error {
public:
    NativeSinkError(std::string code, const std::string& message);
    [[nodiscard]] const std::string& code() const noexcept { return code_; }

private:
    std::string code_;
};

// A row the target cannot accept (null into non-Nullable, out of range, bad
// text for UUID/IPv4/IPv6/Enum, FixedString too long). Always permanent.
class ConversionError : public NativeSinkError {
public:
    ConversionError(std::string column, std::int64_t row, const std::string& reason);
    [[nodiscard]] const std::string& column() const noexcept { return column_; }
    // The row within the chunk being converted.
    [[nodiscard]] std::int64_t row() const noexcept { return row_; }

private:
    std::string column_;
    std::int64_t row_;
};

namespace code {
inline constexpr const char* kUnknownOption = "clickhouse.unknown_option";
inline constexpr const char* kOptionInvalid = "clickhouse.option_invalid";
inline constexpr const char* kOptionConflict = "clickhouse.option_conflict";
inline constexpr const char* kSecretUnset = "clickhouse.secret_unset";
inline constexpr const char* kDeliveryUnsupported = "clickhouse.delivery_unsupported";
inline constexpr const char* kNativeUnavailable = "clickhouse.native_unavailable";
inline constexpr const char* kTlsUnavailable = "clickhouse.tls_unavailable";
inline constexpr const char* kTlsVerifyFailed = "clickhouse.tls_verify_failed";
inline constexpr const char* kAccessDenied = "clickhouse.access_denied";
inline constexpr const char* kServerSettingsUnsupported = "clickhouse.server_settings_unsupported";
inline constexpr const char* kTargetMissing = "clickhouse.target_missing";
inline constexpr const char* kTargetEngineUnsupported = "clickhouse.target_engine_unsupported";
inline constexpr const char* kTargetAsyncInsert = "clickhouse.target_async_insert";
inline constexpr const char* kTargetUnreadable = "clickhouse.target_unreadable";
inline constexpr const char* kColumnPlan = "clickhouse.column_plan";
inline constexpr const char* kMemoryBudgetTooSmall = "clickhouse.memory_budget_too_small";
// Runtime, after open:
inline constexpr const char* kHeaderDrift = "clickhouse.header_drift";
inline constexpr const char* kConversionFailed = "clickhouse.conversion_failed";
inline constexpr const char* kTooManyPartitions = "clickhouse.too_many_partitions";
inline constexpr const char* kInsertFailed = "clickhouse.insert_failed";
inline constexpr const char* kRetryWindowExhausted = "clickhouse.retry_window_exhausted";
inline constexpr const char* kCancelled = "clickhouse.cancelled";
}  // namespace code

// Thrown by the stub bodies of the skeleton, so that a path nobody has
// implemented yet fails loudly instead of returning a default.
[[noreturn]] void not_implemented(const char* what);

}  // namespace clink::clickhouse::native
