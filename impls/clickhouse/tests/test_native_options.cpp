#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "native/errors.hpp"
#include "native/sink_options.hpp"

namespace clink::clickhouse::native {
namespace {

using OptsParams = std::map<std::string, std::string>;

constexpr const char* kOptsColumnTypes = "id:BIGINT;name:VARCHAR";

// The smallest set the factory accepts: what the planner always supplies,
// and the table.
OptsParams opts_minimal() {
    return {{"table", "events"}, {"sql_column_types", kOptsColumnTypes}};
}

OptsParams opts_with(std::initializer_list<std::pair<const std::string, std::string>> extra) {
    OptsParams p = opts_minimal();
    for (const auto& [k, v] : extra) {
        p[k] = v;
    }
    return p;
}

SinkOptions opts_parse(const OptsParams& p) {
    return parse_sink_options(p, 0, 1);
}

struct OptsRefusal {
    std::string code;
    std::string what;
};

// The refusal parse_sink_options raises. Anything other than a
// NativeSinkError escapes and fails the test on its own.
OptsRefusal opts_refusal(const OptsParams& p) {
    try {
        (void)parse_sink_options(p, 0, 1);
    } catch (const NativeSinkError& e) {
        return {e.code(), e.what()};
    }
    ADD_FAILURE() << "parse_sink_options accepted the options";
    return {};
}

std::string opts_message(const char* code, const std::string& text) {
    return std::string("[") + code + "] clickhouse_native_sink: " + text;
}

// Sets, or unsets, one environment variable for the length of a test and
// puts back what was there.
class OptsScopedEnv {
public:
    OptsScopedEnv(std::string name, std::optional<std::string> value) : name_(std::move(name)) {
        if (const char* old = std::getenv(name_.c_str())) {
            old_ = old;
        }
        if (value) {
            ::setenv(name_.c_str(), value->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }
    ~OptsScopedEnv() {
        if (old_) {
            ::setenv(name_.c_str(), old_->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }
    OptsScopedEnv(const OptsScopedEnv&) = delete;
    OptsScopedEnv& operator=(const OptsScopedEnv&) = delete;

private:
    std::string name_;
    std::optional<std::string> old_;
};

const std::vector<std::string> kOptsExpectedOwnKeys = {
    "batch_bytes",
    "batch_interval_ms",
    "batch_rows",
    "compression",
    "connect_timeout_ms",
    "database",
    "endpoints",
    "host",
    "insert_format",
    "password",
    "port",
    "receive_timeout_ms",
    "retry_window_ms",
    "secure",
    "send_timeout_ms",
    "sql_column_types",
    "table",
    "tls_ca_dir",
    "tls_ca_file",
    "tls_verify",
    "user",
};

const std::vector<std::string> kOptsExpectedPassThroughKeys = {
    "batch_size",
    "bounded",
    "changelog",
    "columnar_decode",
    "commit_group",
    "decimal_columns",
    "definition_sql",
    "delim",
    "delivery_guarantee",
    "forced_singleton",
    "freshness",
    "freshness_ms",
    "mode",
    "partition_by",
    "primary_key",
    "query",
    "refresh_arm",
    "schema_columns",
    "state_ttl",
    "state_ttl_domain",
    "view_kind",
    "watermark_delay_ms",
    "write_mode",
};

const char* const kOptsUnknownTlsServerName =
    "[clickhouse.unknown_option] clickhouse_native_sink: unknown option 'tls_server_name'. "
    "Accepted options: batch_bytes, batch_interval_ms, batch_rows, compression, "
    "connect_timeout_ms, database, endpoints, host, insert_format, password, port, "
    "receive_timeout_ms, retry_window_ms, secure, send_timeout_ms, table, tls_ca_dir, "
    "tls_ca_file, tls_verify, user.";

// ---- Defaults --------------------------------------------------------------

TEST(NativeOptions, TheMinimalOptionsTakeEveryDefault) {
    const SinkOptions o = opts_parse(opts_minimal());
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"localhost", 9000}}));
    EXPECT_EQ(o.database, "default");
    EXPECT_EQ(o.table, "events");
    EXPECT_EQ(o.user, "default");
    EXPECT_EQ(o.password, "");
    EXPECT_FALSE(o.tls.enabled);
    EXPECT_EQ(o.tls.ca_file, "");
    EXPECT_EQ(o.tls.ca_dir, "");
    EXPECT_TRUE(o.tls.verify);
    EXPECT_EQ(o.batch_rows, 1'048'449U);
    EXPECT_EQ(o.batch_bytes, 67'108'864U);
    EXPECT_EQ(o.batch_interval, std::chrono::milliseconds(1000));
    EXPECT_EQ(o.compression, Compression::Lz4);
    EXPECT_EQ(o.connect_timeout, std::chrono::milliseconds(5000));
    EXPECT_EQ(o.send_timeout, std::chrono::milliseconds(30000));
    EXPECT_EQ(o.receive_timeout, std::chrono::milliseconds(30000));
    EXPECT_EQ(o.retry_window, std::chrono::milliseconds(600000));
    EXPECT_EQ(o.sql_column_types, kOptsColumnTypes);
    EXPECT_EQ(o.subtask_idx, 0U);
    EXPECT_EQ(o.parallelism, 1U);
    EXPECT_TRUE(o.passed_through.empty());
}

TEST(NativeOptions, TheSubtaskAndParallelismAreCarriedThrough) {
    const SinkOptions o = parse_sink_options(opts_minimal(), 3, 8);
    EXPECT_EQ(o.subtask_idx, 3U);
    EXPECT_EQ(o.parallelism, 8U);
}

// ---- Every own key ---------------------------------------------------------

TEST(NativeOptions, EveryOwnKeyIsParsed) {
    const SinkOptions o = opts_parse(opts_with({
        {"host", "ch-1.internal"},
        {"port", "9001"},
        {"database", "analytics"},
        {"table", "clicks"},
        {"user", "writer"},
        {"password", "pw-literal"},
        {"insert_format", "native"},
        {"secure", "false"},
        {"batch_rows", "5000"},
        {"batch_bytes", "2097152"},
        {"batch_interval_ms", "250"},
        {"compression", "zstd"},
        {"connect_timeout_ms", "1500"},
        {"send_timeout_ms", "2500"},
        {"receive_timeout_ms", "3500"},
        {"retry_window_ms", "45000"},
        {"sql_column_types", "a:INTEGER"},
    }));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-1.internal", 9001}}));
    EXPECT_EQ(o.database, "analytics");
    EXPECT_EQ(o.table, "clicks");
    EXPECT_EQ(o.user, "writer");
    EXPECT_EQ(o.password, "pw-literal");
    EXPECT_FALSE(o.tls.enabled);
    EXPECT_EQ(o.batch_rows, 5000U);
    EXPECT_EQ(o.batch_bytes, 2'097'152U);
    EXPECT_EQ(o.batch_interval, std::chrono::milliseconds(250));
    EXPECT_EQ(o.compression, Compression::Zstd);
    EXPECT_EQ(o.connect_timeout, std::chrono::milliseconds(1500));
    EXPECT_EQ(o.send_timeout, std::chrono::milliseconds(2500));
    EXPECT_EQ(o.receive_timeout, std::chrono::milliseconds(3500));
    EXPECT_EQ(o.retry_window, std::chrono::milliseconds(45000));
    EXPECT_EQ(o.sql_column_types, "a:INTEGER");
    EXPECT_TRUE(o.passed_through.empty());
}

TEST(NativeOptions, UserAndPasswordAcceptAnyValueIncludingEmpty) {
    const SinkOptions o = opts_parse(opts_with({{"user", ""}, {"password", ""}}));
    EXPECT_EQ(o.user, "");
    EXPECT_EQ(o.password, "");
}

TEST(NativeOptions, EachCompressionValueMapsToItsMethod) {
    EXPECT_EQ(opts_parse(opts_with({{"compression", "lz4"}})).compression, Compression::Lz4);
    EXPECT_EQ(opts_parse(opts_with({{"compression", "zstd"}})).compression, Compression::Zstd);
    EXPECT_EQ(opts_parse(opts_with({{"compression", "none"}})).compression, Compression::None);
}

TEST(NativeOptions, AnyOtherCompressionIsRefused) {
    for (const char* bad : {"LZ4", "gzip", "", "lz4 "}) {
        const auto r = opts_refusal(opts_with({{"compression", bad}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << bad;
        EXPECT_EQ(r.what,
                  opts_message(code::kOptionInvalid,
                               std::string("option 'compression' must be 'lz4', 'zstd' or "
                                           "'none'; got '") +
                                   bad + "'"));
    }
}

TEST(NativeOptions, InsertFormatAcceptsOnlyNative) {
    EXPECT_NO_THROW((void)opts_parse(opts_with({{"insert_format", "native"}})));
    const auto r = opts_refusal(opts_with({{"insert_format", "jsoneachrow"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what,
              opts_message(code::kOptionInvalid,
                           "option 'insert_format' must be 'native' for this sink; got "
                           "'jsoneachrow'"));
    EXPECT_EQ(opts_refusal(opts_with({{"insert_format", "Native"}})).code, code::kOptionInvalid);
    EXPECT_EQ(opts_refusal(opts_with({{"insert_format", ""}})).code, code::kOptionInvalid);
}

// ---- Integers --------------------------------------------------------------

struct OptsIntegerRule {
    const char* key;
    unsigned long long lo;
    unsigned long long hi;
};

const OptsIntegerRule kOptsIntegerRules[] = {
    {"port", 1, 65535},
    {"batch_rows", 1, 2147483647},
    {"batch_bytes", 1048576, 1099511627776},
    {"batch_interval_ms", 1, 3600000},
    {"connect_timeout_ms", 1, 600000},
    {"send_timeout_ms", 1, 600000},
    {"receive_timeout_ms", 1, 600000},
    {"retry_window_ms", 1000, 86400000},
};

std::string opts_integer_refusal(const OptsIntegerRule& rule, const std::string& got) {
    return opts_message(code::kOptionInvalid,
                        std::string("option '") + rule.key + "' must be an integer from " +
                            std::to_string(rule.lo) + " to " + std::to_string(rule.hi) + "; got '" +
                            got + "'");
}

TEST(NativeOptions, EveryIntegerIsAcceptedAtItsBounds) {
    for (const auto& rule : kOptsIntegerRules) {
        EXPECT_NO_THROW((void)opts_parse(opts_with({{rule.key, std::to_string(rule.lo)}})))
            << rule.key;
        EXPECT_NO_THROW((void)opts_parse(opts_with({{rule.key, std::to_string(rule.hi)}})))
            << rule.key;
    }
    const SinkOptions o = opts_parse(opts_with({{"port", "65535"},
                                                {"batch_rows", "2147483647"},
                                                {"batch_bytes", "1048576"},
                                                {"retry_window_ms", "86400000"}}));
    EXPECT_EQ(o.endpoints.at(0).port, 65535);
    EXPECT_EQ(o.batch_rows, 2147483647U);
    EXPECT_EQ(o.batch_bytes, 1048576U);
    EXPECT_EQ(o.retry_window, std::chrono::milliseconds(86400000));
}

TEST(NativeOptions, EveryIntegerOutsideItsBoundsIsRefusedWithTheRange) {
    for (const auto& rule : kOptsIntegerRules) {
        const std::string below = std::to_string(rule.lo - 1);
        const auto r = opts_refusal(opts_with({{rule.key, below}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << rule.key;
        EXPECT_EQ(r.what, opts_integer_refusal(rule, below));

        const std::string above = std::to_string(rule.hi + 1);
        const auto r2 = opts_refusal(opts_with({{rule.key, above}}));
        EXPECT_EQ(r2.code, code::kOptionInvalid) << rule.key;
        EXPECT_EQ(r2.what, opts_integer_refusal(rule, above));
    }
}

// Later code closes an INSERT at twice batch_bytes and adds the writer's
// queue to twice it for the memory cap. Without an upper bound, a value at or
// above 2^63 wrapped both sums to almost nothing.
TEST(NativeOptions, TheLargestBatchBytesCanBeDoubledWithoutWrapping) {
    const SinkOptions o = opts_parse(opts_with({{"batch_bytes", "1099511627776"}}));
    EXPECT_EQ(o.batch_bytes, 1ULL << 40);
    EXPECT_LE(o.batch_bytes, std::numeric_limits<std::uint64_t>::max() / 4);

    for (const char* huge :
         {"1099511627777", "9223372036854775808", "18446744073709551615", "18446744073709551616"}) {
        const auto r = opts_refusal(opts_with({{"batch_bytes", huge}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << huge;
        EXPECT_EQ(r.what,
                  opts_message(code::kOptionInvalid,
                               std::string("option 'batch_bytes' must be an integer from 1048576 "
                                           "to 1099511627776; got '") +
                                   huge + "'"));
    }
}

TEST(NativeOptions, AnIntegerMustBeTheWholeStringWithNoFallback) {
    for (const auto& rule : kOptsIntegerRules) {
        for (const char* bad :
             {"", "abc", "1200abc", " 1200", "1200 ", "+1200", "-1", "1200.5", "0x10", "1e4"}) {
            const auto r = opts_refusal(opts_with({{rule.key, bad}}));
            EXPECT_EQ(r.code, code::kOptionInvalid) << rule.key << "='" << bad << "'";
            EXPECT_EQ(r.what, opts_integer_refusal(rule, bad));
        }
    }
}

// ---- Booleans --------------------------------------------------------------

TEST(NativeOptions, ABooleanIsExactlyTrueOrFalse) {
    for (const char* bad : {"True", "FALSE", "1", "0", "yes", "", " true"}) {
        const auto r = opts_refusal(opts_with({{"secure", bad}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << bad;
        EXPECT_EQ(r.what,
                  opts_message(
                      code::kOptionInvalid,
                      std::string("option 'secure' must be 'true' or 'false'; got '") + bad + "'"));
    }
    const auto r = opts_refusal(opts_with({{"secure", "true"}, {"tls_verify", "no"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what,
              opts_message(code::kOptionInvalid,
                           "option 'tls_verify' must be 'true' or 'false'; got 'no'"));
    EXPECT_FALSE(opts_parse(opts_with({{"secure", "false"}})).tls.enabled);
}

// ---- Required and non-empty values -----------------------------------------

TEST(NativeOptions, AMissingTableIsRefused) {
    OptsParams p = opts_minimal();
    p.erase("table");
    const auto r = opts_refusal(p);
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what, opts_message(code::kOptionInvalid, "option 'table' is required"));
}

TEST(NativeOptions, EmptyTableDatabaseOrHostIsRefused) {
    for (const char* key : {"table", "database", "host"}) {
        const auto r = opts_refusal(opts_with({{key, ""}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << key;
        EXPECT_EQ(r.what,
                  opts_message(code::kOptionInvalid,
                               std::string("option '") + key + "' must not be empty"));
    }
}

TEST(NativeOptions, AMalformedSqlColumnTypesIsRefusedAtTheFactory) {
    const auto r = opts_refusal(opts_with({{"sql_column_types", "no-colon-here;;"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what,
              opts_message(code::kOptionInvalid,
                           "option 'sql_column_types' entry 'no-colon-here' is not name:TYPE"));

    // The grammar's own wording belongs to its parser, so these are held only
    // to the code and to naming the key.
    const std::string prefix = opts_message(code::kOptionInvalid, "option 'sql_column_types' ");
    for (const char* bad :
         {"a:BIGINT;;b:VARCHAR", "a:BIGINT;", ":BIGINT", "a:", "a:BIGINT;a:INTEGER"}) {
        const auto refused = opts_refusal(opts_with({{"sql_column_types", bad}}));
        EXPECT_EQ(refused.code, code::kOptionInvalid) << bad;
        EXPECT_TRUE(refused.what.starts_with(prefix)) << bad << ": " << refused.what;
    }
}

TEST(NativeOptions, AnUnknownSqlSpellingIsLeftToTheColumnPlan) {
    // Not malformed: it parses as an unsupported type, which the column plan
    // refuses by name at open alongside any other problem.
    const SinkOptions o = opts_parse(opts_with({{"sql_column_types", "id:BIGINT;g:GEOMETRY"}}));
    EXPECT_EQ(o.sql_column_types, "id:BIGINT;g:GEOMETRY");
}

TEST(NativeOptions, AMissingOrEmptySqlColumnTypesSaysTheSinkIsBuiltFromSql) {
    const std::string expected =
        opts_message(code::kOptionInvalid,
                     "the native sink is built from SQL; a Dag-direct job must pass "
                     "sql_column_types");
    OptsParams p = opts_minimal();
    p.erase("sql_column_types");
    const auto missing = opts_refusal(p);
    EXPECT_EQ(missing.code, code::kOptionInvalid);
    EXPECT_EQ(missing.what, expected);

    const auto empty = opts_refusal(opts_with({{"sql_column_types", ""}}));
    EXPECT_EQ(empty.code, code::kOptionInvalid);
    EXPECT_EQ(empty.what, expected);
}

// ---- Endpoints -------------------------------------------------------------

TEST(NativeOptions, EndpointsListsEveryServerInOrder) {
    const SinkOptions o = opts_parse(opts_with({{"endpoints", "ch-1:9001,ch-2:9002"}}));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-1", 9001}, {"ch-2", 9002}}));
}

TEST(NativeOptions, EndpointsToleratesSpaceAroundEntries) {
    const SinkOptions o = opts_parse(opts_with({{"endpoints", " ch-1:9001 ,\tch-2:9002 "}}));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-1", 9001}, {"ch-2", 9002}}));
}

TEST(NativeOptions, EndpointsTakesBracketedIpv6Addresses) {
    const SinkOptions o =
        opts_parse(opts_with({{"endpoints", "[::1]:9000,[fe80::1]:9440,10.0.0.7:9000"}}));
    EXPECT_EQ(o.endpoints,
              (std::vector<Endpoint>{{"::1", 9000}, {"fe80::1", 9440}, {"10.0.0.7", 9000}}));
}

TEST(NativeOptions, EndpointsAcceptsSixteenServersAndRefusesSeventeen) {
    std::string list;
    for (int i = 1; i <= 16; ++i) {
        list += (i == 1 ? "" : ",") + std::string("ch-") + std::to_string(i) + ":9000";
    }
    EXPECT_EQ(opts_parse(opts_with({{"endpoints", list}})).endpoints.size(), 16U);

    const auto r = opts_refusal(opts_with({{"endpoints", list + ",ch-17:9000"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what,
              opts_message(code::kOptionInvalid,
                           "option 'endpoints' lists 17 servers; at most 16 are accepted"));
}

TEST(NativeOptions, AMalformedEndpointEntryIsRefusedByName) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"", "option 'endpoints' must not be empty"},
        {"ch-1:9000,,ch-2:9000", "option 'endpoints' has an empty entry at position 2"},
        {"ch-1:9000,", "option 'endpoints' has an empty entry at position 2"},
        {"ch-1", "option 'endpoints' entry 'ch-1' is not host:port"},
        {"[::1]", "option 'endpoints' entry '[::1]' is not host:port"},
        {"[::1:9000", "option 'endpoints' entry '[::1:9000' is not host:port"},
        {":9000", "option 'endpoints' entry ':9000' has an empty host"},
        {"[]:9000", "option 'endpoints' entry '[]:9000' has an empty host"},
        {"ch-1:0", "option 'endpoints' entry 'ch-1:0' must have a port from 1 to 65535"},
        {"ch-1:65536", "option 'endpoints' entry 'ch-1:65536' must have a port from 1 to 65535"},
        {"ch-1:", "option 'endpoints' entry 'ch-1:' must have a port from 1 to 65535"},
        {"ch-1:90x", "option 'endpoints' entry 'ch-1:90x' must have a port from 1 to 65535"},
        {"[::1]:", "option 'endpoints' entry '[::1]:' must have a port from 1 to 65535"},
        {"::1:9000",
         "option 'endpoints' entry '::1:9000' has more than one ':'; write an IPv6 address in "
         "brackets, as [addr]:port"},
    };
    for (const auto& [value, text] : cases) {
        const auto r = opts_refusal(opts_with({{"endpoints", value}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << value;
        EXPECT_EQ(r.what, opts_message(code::kOptionInvalid, text)) << value;
    }
}

TEST(NativeOptions, EndpointsWithHostOrPortConflicts) {
    const auto with_host = opts_refusal(opts_with({{"endpoints", "ch-1:9000"}, {"host", "ch-2"}}));
    EXPECT_EQ(with_host.code, code::kOptionConflict);
    EXPECT_EQ(with_host.what,
              opts_message(code::kOptionConflict,
                           "options 'endpoints' and 'host' are both set; list every server in "
                           "'endpoints' instead"));

    const auto with_port = opts_refusal(opts_with({{"endpoints", "ch-1:9000"}, {"port", "9000"}}));
    EXPECT_EQ(with_port.code, code::kOptionConflict);
    EXPECT_EQ(with_port.what,
              opts_message(code::kOptionConflict,
                           "options 'endpoints' and 'port' are both set; list every server in "
                           "'endpoints' instead"));

    // Even a host equal to the default counts as set.
    EXPECT_EQ(opts_refusal(opts_with({{"endpoints", "ch-1:9000"}, {"host", "localhost"}})).code,
              code::kOptionConflict);
}

// ---- TLS -------------------------------------------------------------------

TEST(NativeOptions, TlsOptionsWithoutSecureConflict) {
    for (const char* key : {"tls_ca_file", "tls_ca_dir", "tls_verify"}) {
        const std::string value = std::string(key) == "tls_verify" ? "true" : "/etc/ch/ca.pem";
        const std::string expected = opts_message(
            code::kOptionConflict, std::string("option '") + key + "' needs secure='true'");

        const auto unset = opts_refusal(opts_with({{key, value}}));
        EXPECT_EQ(unset.code, code::kOptionConflict) << key;
        EXPECT_EQ(unset.what, expected);

        const auto off = opts_refusal(opts_with({{key, value}, {"secure", "false"}}));
        EXPECT_EQ(off.code, code::kOptionConflict) << key;
        EXPECT_EQ(off.what, expected);
    }
}

TEST(NativeOptions, AnEmptyCaPathIsRefused) {
    for (const char* key : {"tls_ca_file", "tls_ca_dir"}) {
        const auto r = opts_refusal(opts_with({{"secure", "true"}, {key, ""}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << key;
        EXPECT_EQ(r.what,
                  opts_message(code::kOptionInvalid,
                               std::string("option '") + key + "' must not be empty"));
    }
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)

TEST(NativeOptions, SecureParsesEveryTlsOption) {
    const SinkOptions o = opts_parse(opts_with({{"secure", "true"},
                                                {"tls_ca_file", "/etc/ch/ca.pem"},
                                                {"tls_ca_dir", "/etc/ch/certs"},
                                                {"tls_verify", "false"}}));
    EXPECT_TRUE(o.tls.enabled);
    EXPECT_EQ(o.tls.ca_file, "/etc/ch/ca.pem");
    EXPECT_EQ(o.tls.ca_dir, "/etc/ch/certs");
    EXPECT_FALSE(o.tls.verify);
}

TEST(NativeOptions, SecureDefaultsThePortTo9440) {
    const SinkOptions o = opts_parse(opts_with({{"secure", "true"}, {"host", "ch-1"}}));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-1", 9440}}));
    EXPECT_TRUE(o.tls.verify);
}

TEST(NativeOptions, SecureKeepsAnExplicitPort) {
    const SinkOptions o = opts_parse(opts_with({{"secure", "true"}, {"port", "9000"}}));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"localhost", 9000}}));
}

TEST(NativeOptions, SecureLeavesEndpointPortsAsListed) {
    const SinkOptions o =
        opts_parse(opts_with({{"secure", "true"}, {"endpoints", "ch-1:9000,ch-2:9441"}}));
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-1", 9000}, {"ch-2", 9441}}));
}

#else

TEST(NativeOptions, SecureIsRefusedOnABuildWithoutTls) {
    const auto r = opts_refusal(opts_with({{"secure", "true"}}));
    EXPECT_EQ(r.code, code::kTlsUnavailable);
    EXPECT_EQ(r.what,
              opts_message(code::kTlsUnavailable,
                           "secure='true' is not available: this build of the native sink was "
                           "compiled without TLS support"));
    EXPECT_FALSE(opts_parse(opts_with({{"secure", "false"}})).tls.enabled);
}

#endif

// ---- env:// ----------------------------------------------------------------

TEST(NativeOptions, EnvReferencesResolveOnAnyKey) {
    const OptsScopedEnv pw("CLINK_NATIVE_OPTS_TEST_PW", "s3cr3t-from-env");
    const OptsScopedEnv rows("CLINK_NATIVE_OPTS_TEST_ROWS", "7000");
    const OptsScopedEnv host("CLINK_NATIVE_OPTS_TEST_HOST", "ch-env");
    const SinkOptions o = opts_parse(opts_with({{"password", "env://CLINK_NATIVE_OPTS_TEST_PW"},
                                                {"batch_rows", "env://CLINK_NATIVE_OPTS_TEST_ROWS"},
                                                {"host", "env://CLINK_NATIVE_OPTS_TEST_HOST"}}));
    EXPECT_EQ(o.password, "s3cr3t-from-env");
    EXPECT_EQ(o.batch_rows, 7000U);
    EXPECT_EQ(o.endpoints, (std::vector<Endpoint>{{"ch-env", 9000}}));
}

TEST(NativeOptions, AnUnsetEnvReferenceIsRefusedNamingTheKeyAndTheVariable) {
    const OptsScopedEnv unset("CH_PASSWORD", std::nullopt);
    const auto r = opts_refusal(opts_with({{"password", "env://CH_PASSWORD"}}));
    EXPECT_EQ(r.code, code::kSecretUnset);
    EXPECT_EQ(r.what,
              "[clickhouse.secret_unset] clickhouse_native_sink: option 'password' names "
              "env://CH_PASSWORD, which is unset in this worker's environment");
}

TEST(NativeOptions, AnEmptyEnvVariableIsRefusedLikeAnUnsetOne) {
    const OptsScopedEnv empty("CLINK_NATIVE_OPTS_TEST_EMPTY", "");
    const auto r = opts_refusal(opts_with({{"user", "env://CLINK_NATIVE_OPTS_TEST_EMPTY"}}));
    EXPECT_EQ(r.code, code::kSecretUnset);
    EXPECT_EQ(r.what,
              opts_message(code::kSecretUnset,
                           "option 'user' names env://CLINK_NATIVE_OPTS_TEST_EMPTY, which is "
                           "unset in this worker's environment"));
}

TEST(NativeOptions, AnEnvReferenceWithNoVariableNameIsRefused) {
    const auto r = opts_refusal(opts_with({{"password", "env://"}}));
    EXPECT_EQ(r.code, code::kSecretUnset);
    EXPECT_EQ(
        r.what,
        opts_message(code::kSecretUnset, "option 'password' is 'env://' with no variable name"));
}

TEST(NativeOptions, NoRefusalShowsThePassword) {
    const std::string secret = "hunter2-literal-secret";
    const OptsScopedEnv unset("CLINK_NATIVE_OPTS_TEST_UNSET", std::nullopt);
    const std::vector<OptsParams> refused = {
        opts_with({{"password", secret}, {"user", "env://CLINK_NATIVE_OPTS_TEST_UNSET"}}),
        opts_with({{"password", secret}, {"batch_rows", "0"}}),
        opts_with({{"password", secret}, {"endpoints", "ch-1"}}),
        opts_with({{"password", secret}, {"tls_verify", "true"}}),
        opts_with({{"password", secret}, {"mode", "upsert"}}),
        opts_with({{"password", secret}, {"tls_server_name", "ch-1"}}),
        // A misspelt password key is unknown, and its value stays out of the
        // message too.
        opts_with({{"passwrd", secret}}),
    };
    for (const auto& p : refused) {
        const auto r = opts_refusal(p);
        EXPECT_FALSE(r.code.empty());
        EXPECT_EQ(r.what.find("hunter2"), std::string::npos) << r.what;
    }
}

// Whoever submits a job sees its refusal, and env:// exists to keep a worker's
// secrets out of job specs. A key pointed at a variable that holds a secret
// must not hand the secret back in the message.
TEST(NativeOptions, ARefusalNamesAnEnvReferenceButNeverItsValue) {
    const std::string secret = "AKIA-worker-secret-value";
    const std::string ref = "env://CLINK_NATIVE_OPTS_TEST_SECRET";
    const OptsScopedEnv env("CLINK_NATIVE_OPTS_TEST_SECRET", secret);

    const std::vector<OptsParams> refused = {
        opts_with({{"batch_rows", ref}}),
        opts_with({{"batch_bytes", ref}}),
        opts_with({{"port", ref}}),
        opts_with({{"connect_timeout_ms", ref}}),
        opts_with({{"secure", ref}}),
        opts_with({{"secure", "true"}, {"tls_verify", ref}}),
        opts_with({{"compression", ref}}),
        opts_with({{"insert_format", ref}}),
        opts_with({{"endpoints", ref}}),
        opts_with({{"sql_column_types", ref}}),
        opts_with({{"mode", ref}}),
        opts_with({{"delivery_guarantee", ref}}),
        opts_with({{"changelog", ref}}),
    };
    for (const auto& p : refused) {
        const auto r = opts_refusal(p);
        EXPECT_FALSE(r.code.empty());
        EXPECT_EQ(r.what.find(secret), std::string::npos) << r.what;
        EXPECT_NE(r.what.find(ref), std::string::npos) << r.what;
    }

    EXPECT_EQ(opts_refusal(opts_with({{"batch_rows", ref}})).what,
              opts_message(code::kOptionInvalid,
                           "option 'batch_rows' must be an integer from 1 to 2147483647; got the "
                           "value of env://CLINK_NATIVE_OPTS_TEST_SECRET"));
    EXPECT_EQ(opts_refusal(opts_with({{"compression", ref}})).what,
              opts_message(code::kOptionInvalid,
                           "option 'compression' must be 'lz4', 'zstd' or 'none'; got the value "
                           "of env://CLINK_NATIVE_OPTS_TEST_SECRET"));
    EXPECT_EQ(opts_refusal(opts_with({{"endpoints", ref}})).what,
              opts_message(code::kOptionInvalid,
                           "option 'endpoints' entry at position 1 of the value of "
                           "env://CLINK_NATIVE_OPTS_TEST_SECRET is not host:port"));
    EXPECT_EQ(opts_refusal(opts_with({{"sql_column_types", ref}})).what,
              opts_message(code::kOptionInvalid,
                           "option 'sql_column_types' is not a valid column list; got the value "
                           "of env://CLINK_NATIVE_OPTS_TEST_SECRET"));
    EXPECT_EQ(opts_refusal(opts_with({{"delivery_guarantee", ref}})).what,
              opts_message(code::kOptionInvalid,
                           "delivery_guarantee from env://CLINK_NATIVE_OPTS_TEST_SECRET is not a "
                           "recognised guarantee"));
}

TEST(NativeOptions, EveryEndpointsRefusalKeepsAnEnvValueOut) {
    const std::string ref = "env://CLINK_NATIVE_OPTS_TEST_ENDPOINTS";
    // One value per entry check, each carrying text that must not come back.
    for (const char* value : {"ok-host:9000,secret-host",
                              "ok-host:9000,::secret:9000",
                              "ok-host:9000,:9000",
                              "ok-host:9000,secret-host:99999",
                              "ok-host:9000,[secret::1"}) {
        const OptsScopedEnv env("CLINK_NATIVE_OPTS_TEST_ENDPOINTS", std::string(value));
        const auto r = opts_refusal(opts_with({{"endpoints", ref}}));
        EXPECT_EQ(r.code, code::kOptionInvalid) << value;
        EXPECT_EQ(r.what.find("secret"), std::string::npos) << r.what;
        EXPECT_EQ(r.what.find("ok-host"), std::string::npos) << r.what;
        EXPECT_NE(r.what.find("entry at position 2 of the value of " + ref), std::string::npos)
            << r.what;
    }
}

// ---- Unknown keys ----------------------------------------------------------

TEST(NativeOptions, TlsServerNameIsUnknown) {
    const auto r = opts_refusal(opts_with({{"tls_server_name", "ch-1.internal"}}));
    EXPECT_EQ(r.code, code::kUnknownOption);
    EXPECT_EQ(r.what, kOptsUnknownTlsServerName);
}

TEST(NativeOptions, TheUnknownOptionMessageListsTheUserFacingOwnKeysOnly) {
    const auto r = opts_refusal(opts_with({{"tls_server_name", "x"}}));
    const std::string marker = "Accepted options: ";
    const auto start = r.what.find(marker);
    ASSERT_NE(start, std::string::npos) << r.what;
    std::string list = r.what.substr(start + marker.size());
    ASSERT_FALSE(list.empty());
    ASSERT_EQ(list.back(), '.');
    list.pop_back();

    std::vector<std::string> named;
    for (std::size_t pos = 0;;) {
        const auto comma = list.find(", ", pos);
        named.push_back(list.substr(pos, comma - pos));
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 2;
    }
    std::vector<std::string> expected;
    for (const auto& k : kOptsExpectedOwnKeys) {
        if (k != "sql_column_types") {
            expected.push_back(k);
        }
    }
    EXPECT_EQ(named, expected);
    EXPECT_EQ(r.what.find("sql_column_types"), std::string::npos);
    for (const auto& k : pass_through_keys()) {
        EXPECT_EQ(std::find(named.begin(), named.end(), k), named.end()) << k;
    }
}

TEST(NativeOptions, AnUnknownKeyIsReportedBeforeAnyValueProblem) {
    const OptsScopedEnv unset("CLINK_NATIVE_OPTS_TEST_UNSET", std::nullopt);
    const auto r = opts_refusal(opts_with({{"batch_rows", "abc"},
                                           {"password", "env://CLINK_NATIVE_OPTS_TEST_UNSET"},
                                           {"mode", "upsert"},
                                           {"tls_server_name", "ch-1"}}));
    EXPECT_EQ(r.code, code::kUnknownOption);
    EXPECT_EQ(r.what, kOptsUnknownTlsServerName);

    OptsParams no_table = {{"tls_server_name", "ch-1"}};
    EXPECT_EQ(opts_refusal(no_table).code, code::kUnknownOption);
}

TEST(NativeOptions, KeysAreMatchedExactly) {
    for (const char* key : {"Host", "TABLE", "batch_rows ", "format", "connector"}) {
        const auto r = opts_refusal(opts_with({{key, "x"}}));
        EXPECT_EQ(r.code, code::kUnknownOption) << key;
        EXPECT_NE(r.what.find(std::string("unknown option '") + key + "'"), std::string::npos)
            << r.what;
    }
}

// ---- The key lists ---------------------------------------------------------

TEST(NativeOptions, TheOwnKeysAreExactlyTheDocumentedSetSorted) {
    EXPECT_EQ(own_option_keys(), kOptsExpectedOwnKeys);
    EXPECT_TRUE(std::is_sorted(own_option_keys().begin(), own_option_keys().end()));
}

TEST(NativeOptions, ThePassThroughKeysAreExactlyTheToleratedSetSorted) {
    EXPECT_EQ(pass_through_keys(), kOptsExpectedPassThroughKeys);
    EXPECT_TRUE(std::is_sorted(pass_through_keys().begin(), pass_through_keys().end()));
}

TEST(NativeOptions, NoKeyIsInBothLists) {
    for (const auto& k : own_option_keys()) {
        EXPECT_EQ(std::find(pass_through_keys().begin(), pass_through_keys().end(), k),
                  pass_through_keys().end())
            << k;
    }
}

// ---- Tolerated keys --------------------------------------------------------

TEST(NativeOptions, EveryToleratedKeyIsAcceptedAndRecorded) {
    OptsParams p = opts_minimal();
    for (const auto& k : pass_through_keys()) {
        p[k] = "planner-value";
    }
    // The four the sink reads need values that keep the guarantee.
    p["mode"] = "append";
    p["delivery_guarantee"] = "at_least_once";
    p["changelog"] = "false";
    p["write_mode"] = "append";
    const SinkOptions o = opts_parse(p);
    EXPECT_EQ(o.passed_through, kOptsExpectedPassThroughKeys);
    EXPECT_EQ(o.table, "events");
}

TEST(NativeOptions, TheSourceKeysOfATableThatIsAlsoReadAreTolerated) {
    const SinkOptions o = opts_parse(opts_with(
        {{"query", "SELECT id, name FROM events"}, {"batch_size", "1024"}, {"delim", "|"}}));
    EXPECT_EQ(o.passed_through, (std::vector<std::string>{"batch_size", "delim", "query"}));
}

TEST(NativeOptions, PlannerAndViewKeysAreTolerated) {
    const SinkOptions o = opts_parse(opts_with({{"schema_columns", "id:i64,name:str"},
                                                {"decimal_columns", "amount:2"},
                                                {"forced_singleton", "true"},
                                                {"view_kind", "materialized"},
                                                {"definition_sql", "SELECT 1"},
                                                {"freshness", "0"},
                                                {"mode", "append"}}));
    EXPECT_EQ(o.passed_through,
              (std::vector<std::string>{"decimal_columns",
                                        "definition_sql",
                                        "forced_singleton",
                                        "freshness",
                                        "mode",
                                        "schema_columns",
                                        "view_kind"}));
}

TEST(NativeOptions, AnIgnoredToleratedKeyIsNotResolved) {
    const OptsScopedEnv unset("CLINK_NATIVE_OPTS_TEST_UNSET", std::nullopt);
    const SinkOptions o = opts_parse(opts_with({{"query", "env://CLINK_NATIVE_OPTS_TEST_UNSET"}}));
    EXPECT_EQ(o.passed_through, (std::vector<std::string>{"query"}));
}

// ---- Values that would change the guarantee --------------------------------

TEST(NativeOptions, AModeOtherThanAppendIsRefused) {
    const auto r = opts_refusal(opts_with({{"mode", "upsert"}}));
    EXPECT_EQ(r.code, code::kDeliveryUnsupported);
    EXPECT_EQ(r.what,
              opts_message(code::kDeliveryUnsupported,
                           "mode='upsert' is not supported; this sink appends rows, so mode must "
                           "be 'append'"));
    EXPECT_EQ(opts_refusal(opts_with({{"mode", "cdc"}})).code, code::kDeliveryUnsupported);
    EXPECT_EQ(opts_refusal(opts_with({{"mode", ""}})).code, code::kDeliveryUnsupported);
    EXPECT_NO_THROW((void)opts_parse(opts_with({{"mode", "append"}})));
}

TEST(NativeOptions, ADeliveryGuaranteeStrongerThanAtLeastOnceIsRefused) {
    const auto r = opts_refusal(opts_with({{"delivery_guarantee", "exactly_once"}}));
    EXPECT_EQ(r.code, code::kDeliveryUnsupported);
    EXPECT_EQ(r.what,
              opts_message(code::kDeliveryUnsupported,
                           "delivery_guarantee='exactly_once' is stronger than this sink "
                           "provides; it delivers at least once"));
    for (const char* stronger : {"exactly_once_two_phase_commit",
                                 "exactly_once_atomic_publish",
                                 "effectively_once_idempotent"}) {
        EXPECT_EQ(opts_refusal(opts_with({{"delivery_guarantee", stronger}})).code,
                  code::kDeliveryUnsupported)
            << stronger;
    }
    for (const char* weaker : {"at_least_once", "at_most_once", "no_durable_restart_guarantee"}) {
        EXPECT_NO_THROW((void)opts_parse(opts_with({{"delivery_guarantee", weaker}}))) << weaker;
    }
}

TEST(NativeOptions, AnUnrecognisedDeliveryGuaranteeIsRefused) {
    const auto r = opts_refusal(opts_with({{"delivery_guarantee", "exactly-once"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_EQ(r.what,
              opts_message(code::kOptionInvalid,
                           "delivery_guarantee='exactly-once' is not a recognised guarantee"));
}

TEST(NativeOptions, ChangelogTrueIsRefused) {
    const auto r = opts_refusal(opts_with({{"changelog", "true"}}));
    EXPECT_EQ(r.code, code::kDeliveryUnsupported);
    EXPECT_EQ(r.what,
              opts_message(code::kDeliveryUnsupported,
                           "changelog='true' is not supported; this sink writes inserts only"));
    EXPECT_NO_THROW((void)opts_parse(opts_with({{"changelog", "false"}})));
    EXPECT_EQ(opts_refusal(opts_with({{"changelog", "TRUE"}})).code, code::kOptionInvalid);
}

TEST(NativeOptions, WriteModeOverwriteIsRefused) {
    const auto r = opts_refusal(opts_with({{"write_mode", "overwrite"}}));
    EXPECT_EQ(r.code, code::kDeliveryUnsupported);
    EXPECT_EQ(r.what,
              opts_message(code::kDeliveryUnsupported,
                           "write_mode='overwrite' is not supported; each refresh would append "
                           "the whole result again"));
    EXPECT_NO_THROW((void)opts_parse(opts_with({{"write_mode", "append"}})));
}

TEST(NativeOptions, AGuaranteeValueFromTheEnvironmentIsStillChecked) {
    const OptsScopedEnv mode("CLINK_NATIVE_OPTS_TEST_MODE", "upsert");
    const OptsScopedEnv guarantee("CLINK_NATIVE_OPTS_TEST_GUARANTEE", "exactly_once");
    const OptsScopedEnv changelog("CLINK_NATIVE_OPTS_TEST_CHANGELOG", "true");
    const OptsScopedEnv write_mode("CLINK_NATIVE_OPTS_TEST_WRITE_MODE", "overwrite");

    const auto r = opts_refusal(opts_with({{"mode", "env://CLINK_NATIVE_OPTS_TEST_MODE"}}));
    EXPECT_EQ(r.code, code::kDeliveryUnsupported);
    EXPECT_EQ(r.what,
              opts_message(code::kDeliveryUnsupported,
                           "mode from env://CLINK_NATIVE_OPTS_TEST_MODE is not supported; this "
                           "sink appends rows, so mode must be 'append'"));

    const auto g =
        opts_refusal(opts_with({{"delivery_guarantee", "env://CLINK_NATIVE_OPTS_TEST_GUARANTEE"}}));
    EXPECT_EQ(g.code, code::kDeliveryUnsupported);
    EXPECT_EQ(g.what,
              opts_message(code::kDeliveryUnsupported,
                           "delivery_guarantee from env://CLINK_NATIVE_OPTS_TEST_GUARANTEE is "
                           "stronger than this sink provides; it delivers at least once"));

    const auto c =
        opts_refusal(opts_with({{"changelog", "env://CLINK_NATIVE_OPTS_TEST_CHANGELOG"}}));
    EXPECT_EQ(c.code, code::kDeliveryUnsupported);
    EXPECT_EQ(c.what,
              opts_message(code::kDeliveryUnsupported,
                           "changelog from env://CLINK_NATIVE_OPTS_TEST_CHANGELOG is not "
                           "supported; this sink writes inserts only"));

    const auto w =
        opts_refusal(opts_with({{"write_mode", "env://CLINK_NATIVE_OPTS_TEST_WRITE_MODE"}}));
    EXPECT_EQ(w.code, code::kDeliveryUnsupported);
    EXPECT_EQ(w.what,
              opts_message(code::kDeliveryUnsupported,
                           "write_mode from env://CLINK_NATIVE_OPTS_TEST_WRITE_MODE is not "
                           "supported; each refresh would append the whole result again"));
}

// ---- describe --------------------------------------------------------------

TEST(NativeOptions, DescribeRendersEveryDefault) {
    EXPECT_EQ(describe(opts_parse(opts_minimal())),
              "endpoints=localhost:9000\n"
              "database=default\n"
              "table=events\n"
              "user=default\n"
              "password=unset\n"
              "secure=false\n"
              "tls_ca_file=none\n"
              "tls_ca_dir=none\n"
              "tls_verify=true\n"
              "batch_rows=1048449\n"
              "batch_bytes=67108864\n"
              "batch_interval_ms=1000\n"
              "compression=lz4\n"
              "connect_timeout_ms=5000\n"
              "send_timeout_ms=30000\n"
              "receive_timeout_ms=30000\n"
              "retry_window_ms=600000\n"
              "sql_column_types=id:BIGINT;name:VARCHAR\n"
              "passed_through=none");
}

TEST(NativeOptions, DescribeRendersEveryValueButThePassword) {
    SinkOptions o;
    o.endpoints = {{"ch-1", 9440}, {"fe80::1", 9441}};
    o.database = "analytics";
    o.table = "clicks";
    o.user = "writer";
    o.password = "correct-horse-battery";
    o.tls = TlsOptions{true, "/etc/ch/ca.pem", "/etc/ch/certs", false};
    o.batch_rows = 5000;
    o.batch_bytes = 2'097'152;
    o.batch_interval = std::chrono::milliseconds(250);
    o.compression = Compression::None;
    o.connect_timeout = std::chrono::milliseconds(1500);
    o.send_timeout = std::chrono::milliseconds(2500);
    o.receive_timeout = std::chrono::milliseconds(3500);
    o.retry_window = std::chrono::milliseconds(45000);
    o.sql_column_types = "a:INTEGER";
    o.passed_through = {"mode", "schema_columns"};

    const std::string text = describe(o);
    EXPECT_EQ(text,
              "endpoints=ch-1:9440,[fe80::1]:9441\n"
              "database=analytics\n"
              "table=clicks\n"
              "user=writer\n"
              "password=set\n"
              "secure=true\n"
              "tls_ca_file=/etc/ch/ca.pem\n"
              "tls_ca_dir=/etc/ch/certs\n"
              "tls_verify=false\n"
              "batch_rows=5000\n"
              "batch_bytes=2097152\n"
              "batch_interval_ms=250\n"
              "compression=none\n"
              "connect_timeout_ms=1500\n"
              "send_timeout_ms=2500\n"
              "receive_timeout_ms=3500\n"
              "retry_window_ms=45000\n"
              "sql_column_types=a:INTEGER\n"
              "passed_through=mode,schema_columns");
    EXPECT_EQ(text.find("correct-horse"), std::string::npos);
}

TEST(NativeOptions, DescribeNeverRendersAPasswordFromTheEnvironment) {
    const OptsScopedEnv pw("CLINK_NATIVE_OPTS_TEST_PW", "env-held-secret-value");
    const std::string text =
        describe(opts_parse(opts_with({{"password", "env://CLINK_NATIVE_OPTS_TEST_PW"}})));
    EXPECT_NE(text.find("\npassword=set\n"), std::string::npos) << text;
    EXPECT_EQ(text.find("env-held-secret-value"), std::string::npos) << text;
    EXPECT_EQ(text.find("CLINK_NATIVE_OPTS_TEST_PW"), std::string::npos) << text;
}

}  // namespace
}  // namespace clink::clickhouse::native
