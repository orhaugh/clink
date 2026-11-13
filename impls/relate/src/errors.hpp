#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace clink::relate {

// A refusal with a stable code ("relate.option_invalid", ...) for a test or a
// dashboard to key on, beside a message for a person.
class RelateError : public std::runtime_error {
public:
    RelateError(std::string code, const std::string& message)
        : std::runtime_error(message), code_(std::move(code)) {}
    [[nodiscard]] const std::string& code() const noexcept { return code_; }

private:
    std::string code_;
};

namespace code {
inline constexpr char kOptionInvalid[] = "relate.option_invalid";
inline constexpr char kGuaranteeUnsupported[] = "relate.guarantee_unsupported";
inline constexpr char kTypeUnsupported[] = "relate.type_unsupported";
inline constexpr char kStoreUnavailable[] = "relate.store_unavailable";
inline constexpr char kSchemaRefused[] = "relate.schema_refused";
inline constexpr char kStatementRefused[] = "relate.statement_refused";
inline constexpr char kValueInvalid[] = "relate.value_invalid";
inline constexpr char kRowTooLarge[] = "relate.row_too_large";
inline constexpr char kBatchRefused[] = "relate.batch_refused";
inline constexpr char kRetriesExhausted[] = "relate.retries_exhausted";
inline constexpr char kEngineFailure[] = "relate.engine_failure";
inline constexpr char kCancelled[] = "relate.cancelled";
}  // namespace code

// What Relate reports with a call that failed: its status code, the stage that
// failed, whether running the same thing again can succeed, and its message.
struct EngineError {
    std::string code;   // such as Neo.ClientError.Schema.ConstraintValidationFailed
    std::string stage;  // compile, runtime or internal
    bool retryable{false};
    std::string message;

    // "<message> (<code>)", or the message alone when there is no code.
    [[nodiscard]] std::string describe() const;
};

// Reads the JSON error shape Relate returns. Text that is not that shape is
// kept whole as the message, so nothing Relate said is lost.
[[nodiscard]] EngineError parse_engine_error(std::string_view json);

}  // namespace clink::relate
