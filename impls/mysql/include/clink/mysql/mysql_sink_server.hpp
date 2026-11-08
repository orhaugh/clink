#pragma once

// What the MySQL sinks need of a server, behind an interface so that every way
// a write can fail (a statement refused, a lost reply, a cancel, a COMMIT whose
// outcome is unknown) can be driven without one. connect_mysql_sink_server
// opens the real thing over mariadb-connector-c.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sink_rules.hpp"

namespace clink::mysql {

struct ServerError {
    unsigned code{0};
    std::string sqlstate;
    std::string message;  // as the server or the client library gave it
    // A client-side error (2000 to 2999): the request or its reply was lost, so
    // the statement it was for may or may not have run on the server.
    [[nodiscard]] bool lost() const noexcept { return code >= 2000 && code < 3000; }
};

struct ServerWarning {
    std::string level;  // "Note", "Warning" or "Error"
    unsigned code{0};
    std::string message;
};

class MysqlSinkServer {
public:
    virtual ~MysqlSinkServer() = default;
    // Runs one statement; nullopt when it succeeded.
    virtual std::optional<ServerError> exec(std::string_view sql) = 0;
    // The notes and warnings the last statement raised.
    virtual std::vector<ServerWarning> warnings() = 0;
    // A string value escaped for a single-quoted literal on this connection.
    virtual std::string escape(std::string_view value) = 0;
    // The table as information_schema describes it. Throws on failure.
    virtual TableSchema describe(const std::string& table) = 0;
    // The session's max_allowed_packet. Throws on failure.
    virtual std::uint64_t max_allowed_packet() = 0;
    // From another thread: ends the statement in flight, and the connection
    // with it.
    virtual void cancel() noexcept = 0;
};

using MysqlSinkServerFactory =
    std::function<std::unique_ptr<MysqlSinkServer>(const ConnectOptions&)>;

// Connects to the server the options name. Throws on failure.
[[nodiscard]] std::unique_ptr<MysqlSinkServer> connect_mysql_sink_server(const ConnectOptions& o);

}  // namespace clink::mysql
