#pragma once

// The PostgreSQL sinks' connection to a server, behind an interface so their
// failure paths can be tested against a scripted one.

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/postgres/pg_sink_rules.hpp"

namespace clink::postgres {

// Why a statement failed.
struct PgError {
    std::string sqlstate;  // five characters; empty when the server sent none
    std::string message;   // the primary message only, never the DETAIL
    bool lost{false};      // the connection is gone, so what the server did is unknown
};

// What a statement left: its error, or its command tag ("INSERT 0 3", "COMMIT",
// or "ROLLBACK" when a COMMIT found the transaction already failed).
struct PgOutcome {
    std::optional<PgError> error;
    std::string tag;
};

// How a sink's connection is set up beyond its conninfo, which overrides each.
// The keepalives and the send timeout bound how long a statement waits on a
// server that has gone away.
struct PgConnectSettings {
    int connect_timeout_s{10};
    int keepalives_idle_s{30};
    int keepalives_interval_s{10};
    int keepalives_count{3};
    int tcp_user_timeout_ms{60000};
};

class PgSinkServer {
public:
    virtual ~PgSinkServer() = default;
    virtual PgOutcome exec(std::string_view sql) = 0;
    // One statement with text parameters ($1, $2, ...); nullopt is NULL.
    virtual PgOutcome exec_params(std::string_view sql,
                                  const std::vector<std::optional<std::string>>& params) = 0;
    virtual std::string escape(std::string_view value) = 0;
    virtual PgTable describe(const std::string& table) = 0;
    // Ends the statement in flight. Safe to call from another thread.
    virtual void cancel() noexcept = 0;
};

using PgSinkServerFactory = std::function<std::unique_ptr<PgSinkServer>(
    const std::string& conninfo, const PgConnectSettings& settings, const std::string& who)>;

// A server over libpq. Throws when the connection cannot be made, or when it is
// not as encrypted as the conninfo asks.
[[nodiscard]] std::unique_ptr<PgSinkServer> connect_pg_sink_server(
    const std::string& conninfo, const PgConnectSettings& settings, const std::string& who);

}  // namespace clink::postgres
