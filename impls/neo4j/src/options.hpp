#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "types.hpp"

namespace clink::neo4j {

// A refusal with a stable code ("neo4j.option_invalid", ...) for a test or a
// dashboard to key on, beside a message for a person.
class Neo4jError : public std::runtime_error {
public:
    Neo4jError(std::string code, const std::string& message)
        : std::runtime_error(message), code_(std::move(code)) {}
    [[nodiscard]] const std::string& code() const noexcept { return code_; }

private:
    std::string code_;
};

namespace code {
inline constexpr char kOptionInvalid[] = "neo4j.option_invalid";
inline constexpr char kSecretUnset[] = "neo4j.secret_unset";
inline constexpr char kGuaranteeUnsupported[] = "neo4j.guarantee_unsupported";
inline constexpr char kTypeUnsupported[] = "neo4j.type_unsupported";
inline constexpr char kTlsUnavailable[] = "neo4j.tls_unavailable";
inline constexpr char kTlsVerify[] = "neo4j.tls_verify";
inline constexpr char kStatementRefused[] = "neo4j.statement_refused";
inline constexpr char kAuthentication[] = "neo4j.authentication";
inline constexpr char kAuthorisation[] = "neo4j.authorisation";
inline constexpr char kServerRefused[] = "neo4j.server_refused";
inline constexpr char kRetriesExhausted[] = "neo4j.retries_exhausted";
inline constexpr char kOutcomeUnknown[] = "neo4j.outcome_unknown";
inline constexpr char kValueInvalid[] = "neo4j.value_invalid";
inline constexpr char kRowTooLarge[] = "neo4j.row_too_large";
inline constexpr char kCancelled[] = "neo4j.cancelled";
inline constexpr char kMultipleRows[] = "neo4j.multiple_rows";
inline constexpr char kResponseTooLarge[] = "neo4j.response_too_large";
inline constexpr char kMalformedReply[] = "neo4j.malformed_reply";
inline constexpr char kTimeout[] = "neo4j.timeout";
}  // namespace code

// What the sink and the lookup share: where the server is, who connects, and
// how long a request may take. Parsed by the same rules for both.
struct ConnectionOptions {
    std::string endpoint;  // scheme://host[:port], no path
    bool https{false};
    std::string database{"neo4j"};
    // Exactly one of: basic (username + password), bearer, none.
    std::string username;
    std::string password;
    std::string bearer_token;
    // https: the PEM CAs that may sign the server's certificate, in place of
    // the system's. The certificate must also name the endpoint's host; that
    // check has no off switch.
    std::string tls_ca_file;
    std::uint64_t connect_timeout_ms{5000};
    std::uint64_t request_timeout_ms{30000};
    std::uint64_t retry_window_ms{60000};
    std::uint64_t max_response_bytes{1ULL << 20};

    // The request path, the database percent-encoded.
    [[nodiscard]] std::string query_path() const;
};

struct SinkOptions : ConnectionOptions {
    std::string statement;
    std::uint64_t batch_size{1000};
    std::uint64_t batch_bytes{4ULL << 20};
    std::uint64_t flush_interval_ms{1000};
    std::vector<Column> columns;
    std::uint32_t subtask{0};
    std::uint32_t parallelism{1};
};

// Every option the sink reads, refused when malformed, out of range or
// meaningless here; keys the SQL planner puts on every sink op are accepted
// and ignored. A value may be written env://VAR and is then read from the
// environment; a refusal names such a value by its reference only.
[[nodiscard]] SinkOptions parse_options(const std::map<std::string, std::string>& params,
                                        std::uint32_t subtask,
                                        std::uint32_t parallelism);

// The options a refusal lists as accepted.
[[nodiscard]] const std::vector<std::string>& option_names();

// A lookup join against Neo4j: the SQL planner's description of the join
// (which probe column is the key, the lookup table's declared columns, the
// aliases) and the lookup table's own options, which it passes prefixed with
// "neo4j.".
struct LookupOptions : ConnectionOptions {
    std::string statement;
    // The join.
    std::string probe_alias;
    std::string dim_alias;
    std::vector<std::string> probe_columns;
    std::string probe_key_column;
    SqlType key_type;  // the probe key column's declared type: $key's type
    std::string dim_key_column;
    std::vector<Column> dim_columns;     // every declared lookup column
    std::vector<Column> result_columns;  // the ones the statement RETURNs
    bool outer{false};
    // Metadata columns, each optional: true on a match and false on none; the
    // client's clock when the reply arrived (epoch ms); the reply's bookmark.
    std::string matched_column;
    std::string observed_at_column;
    std::string bookmark_column;
    // Bounds. A lookup fails once max_retries retries or retry_window_ms have
    // been spent, whichever comes first.
    std::uint64_t max_retries{3};
    std::uint64_t server_timeout_s{0};  // 0: maxExecutionTime is not sent
    std::uint64_t max_in_flight{64};
    std::uint64_t max_concurrent_requests{4};
    std::uint32_t subtask{0};
    std::uint32_t parallelism{1};
};

// Parses the op params of neo4j_lookup_join_row. The lookup table's options
// arrive as "neo4j.<key>"; any other key must be one the planner sets.
// Refusals are worded for the lookup ("neo4j lookup: ...").
[[nodiscard]] LookupOptions parse_lookup_options(const std::map<std::string, std::string>& params,
                                                 std::uint32_t subtask,
                                                 std::uint32_t parallelism);

// The lookup table options a refusal lists as accepted (without the prefix).
[[nodiscard]] const std::vector<std::string>& lookup_option_names();

// `e` with its leading "neo4j sink: " replaced by `prefix`: a shared helper
// words its refusals for the sink, and the lookup rewords them as its own.
[[nodiscard]] Neo4jError relabelled(const Neo4jError& e, std::string_view prefix);

}  // namespace clink::neo4j
