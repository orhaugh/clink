#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace clink::clickhouse::native {

enum class Compression : std::uint8_t { None, Lz4, Zstd };

struct Endpoint {
    std::string host;
    std::uint16_t port{9000};
    bool operator==(const Endpoint&) const = default;
    auto operator<=>(const Endpoint&) const = default;
};

struct TlsOptions {
    bool enabled{false};  // secure
    std::string ca_file;  // tls_ca_file
    std::string ca_dir;   // tls_ca_dir
    bool verify{true};    // tls_verify
};

struct SinkOptions {
    std::vector<Endpoint> endpoints;  // from host/port, or endpoints=; never empty
    std::string database{"default"};
    std::string table;
    std::string user{"default"};
    std::string password;  // resolved; never logged or rendered
    TlsOptions tls;
    std::uint64_t batch_rows{1'048'449};
    std::uint64_t batch_bytes{64ULL << 20};
    std::chrono::milliseconds batch_interval{1000};
    Compression compression{Compression::Lz4};
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds send_timeout{30000};
    std::chrono::milliseconds receive_timeout{30000};
    std::chrono::milliseconds retry_window{600000};
    std::string sql_column_types;  // planner-supplied, required
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::vector<std::string> passed_through;  // tolerated keys seen, for the open report
};

// Parses BuildContext::params. Every key must be in one of the two lists
// below. Resolves env:// on every value the sink reads with
// BuildContext::resolve_secret (include/clink/plugin/plugin.hpp) before
// parsing; an env:// naming an unset or empty variable refuses
// clickhouse.secret_unset, naming the key and the variable, never a value.
// Of the tolerated keys, only mode, delivery_guarantee, changelog and
// write_mode are read, to refuse a value that would change the guarantee.
// sql_column_types is checked for presence only: parse_sql_column_types owns
// its grammar. Throws NativeSinkError.
[[nodiscard]] SinkOptions parse_sink_options(const std::map<std::string, std::string>& params,
                                             std::uint32_t subtask_idx,
                                             std::uint32_t parallelism);

// The sink's own keys, and the keys it tolerates because the planner, the
// materialised-view code or the binder put them on the op.
[[nodiscard]] const std::vector<std::string>& own_option_keys();
[[nodiscard]] const std::vector<std::string>& pass_through_keys();

// One key=value line per option with its effective value, for the open
// report, joined by newlines with none at the end. The password is shown only
// as "set" or "unset", and tolerated keys by name only, as passed_through.
[[nodiscard]] std::string describe(const SinkOptions&);

}  // namespace clink::clickhouse::native
