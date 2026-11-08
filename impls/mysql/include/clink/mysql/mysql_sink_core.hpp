#pragma once

// What the MySQL sinks share: the connection and its session (UTC, strict SQL
// mode, bounded lock waits), the check of the destination table when the sink
// opens, the failure state, and the writing of one flush as one transaction.
//
// Failure. Once a record is refused or a write fails, the sink is failed: it
// refuses every later record, flush and checkpoint barrier until it is opened
// again, so no checkpoint completes as if the failed write had landed. close()
// releases the connection, rolling back a transaction a failure left open,
// and never throws, so it cannot hide the failure that preceded it.
//
// A flush. Its statements run inside one transaction. A deadlock or a lock
// wait timeout rolls it back and it is tried again, at most max_retries times
// with a growing pause a cancel cuts short; any other error, and any warning
// or note a statement raises, rolls it back and fails the sink. A COMMIT the
// server refuses committed nothing; a COMMIT whose reply is lost (the
// connection dropped, the task was cancelled) may or may not have committed,
// and the failure says so: the job restarts from its last checkpoint and
// writes those records again either way.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/mysql/mysql_sink_rules.hpp"
#include "clink/mysql/mysql_sink_server.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::mysql {

struct SinkCoreOptions {
    ConnectOptions conn;
    SinkTarget target;
    // The longest statement the sink sends, before the server's own
    // max_allowed_packet lowers it.
    std::size_t max_statement_bytes{4U << 20};
    // innodb_lock_wait_timeout and lock_wait_timeout for the sink's session.
    std::chrono::seconds lock_wait_timeout{30};
    // How many times a flush is tried again after a deadlock or a lock wait
    // timeout.
    int max_retries{3};
    std::string name;
    // Opens the server; unset, a real connection (connect_mysql_sink_server).
    MysqlSinkServerFactory server_factory;
};

class SinkCore {
public:
    explicit SinkCore(SinkCoreOptions o);
    ~SinkCore();
    SinkCore(const SinkCore&) = delete;
    SinkCore& operator=(const SinkCore&) = delete;

    // Connects, sets the session up, reads the table and checks it against the
    // target. Clears an earlier failure. Throws when any of it fails.
    void open(RuntimeContext* rt);
    // Releases the connection, rolling back an open transaction. Never throws.
    void close() noexcept;

    // Throws unless the sink is open and not failed. `what` names the refused
    // request, e.g. "a checkpoint barrier".
    void require_usable(std::string_view what) const;
    // Throws when the sink failed; a sink never opened passes.
    void require_not_failed(std::string_view what) const;
    // Records the failure and throws it.
    [[noreturn]] void fail(const std::string& why);
    [[nodiscard]] bool failed() const noexcept { return !failure_.empty(); }

    [[nodiscard]] std::string escape(std::string_view value);
    [[nodiscard]] const TableSchema& schema() const noexcept { return schema_; }
    // The longest statement this connection takes: max_statement_bytes, or
    // less when the server's max_allowed_packet is lower.
    [[nodiscard]] std::size_t statement_bytes() const noexcept { return statement_bytes_; }

    // Writes `statements` as one transaction, as the header describes.
    // `records` and `bytes` are for the metrics.
    void write(const std::vector<std::string>& statements, std::size_t records, std::size_t bytes);

private:
    class Watch;
    enum class Attempt : std::uint8_t { Done, Retry };

    Attempt attempt_(const std::vector<std::string>& statements);
    std::optional<ServerError> exec_(std::string_view sql);
    void rollback_() noexcept;
    [[noreturn]] void statement_failed_(const ServerError& e, std::string_view during);
    [[nodiscard]] std::string describe_(const ServerError& e) const;
    // What a failed flush left in the table, by whether its engine has transactions.
    [[nodiscard]] std::string left_behind_() const;
    [[nodiscard]] bool cancel_requested_() const noexcept;
    void pause_(std::chrono::milliseconds d);

    SinkCoreOptions opts_;
    RuntimeContext* rt_{nullptr};
    std::unique_ptr<MysqlSinkServer> server_;
    std::unique_ptr<Watch> watch_;
    TableSchema schema_;
    std::size_t statement_bytes_{0};
    bool open_{false};
    bool in_transaction_{false};
    std::string failure_;
    std::string last_retry_;  // why the last attempt was tried again
};

}  // namespace clink::mysql
