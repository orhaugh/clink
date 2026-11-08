#pragma once

// What the PostgreSQL sinks share: the connection and its session, the table
// check, one transaction per flush, the failure state, retries, cancellation
// and the statement bound.
//
// Session. Each connection runs with time zone UTC, client encoding UTF8,
// standard_conforming_strings on, lock_timeout set to the lock wait bound and
// statement_timeout a minute past it, whatever the server's defaults.
//
// Transactions. A flush is BEGIN, its statements in order, COMMIT. A deadlock,
// a serialization failure or a lock wait past lock_timeout rolls the flush back
// and tries it again, at most max_retries times, waiting longer each time. A
// flush whose upserts PostgreSQL refuses for naming one row twice (two keys the
// stream holds apart and the table holds as one) is rolled back and written
// again a row to a statement, in the same order. Any other failure rolls the
// flush back and fails the sink.
//
// COMMIT. A COMMIT the server refuses committed nothing. A COMMIT whose reply
// is lost (the connection went away, or the task was cancelled while it was in
// flight) is an unknown outcome: the flush may or may not have committed, and
// the sink says so; it is never reported as rolled back.
//
// Failure state. A failure fails the sink: every later record, barrier and
// flush is refused, an empty flush included, until open() is called again.
// close() rolls back a transaction left open, releases the connection and never
// throws.
//
// Cancellation. When the task's cancel is requested during a statement, a
// watch sends the server a cancel request, so a cancelled task does not wait
// out a lock.
//
// Errors carry the server's SQLSTATE and primary message, with the values it
// quotes replaced by <value>; never its DETAIL, which can quote a whole row.

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/postgres/pg_sink_rules.hpp"
#include "clink/postgres/pg_sink_server.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::postgres {

// One statement of a flush: SQL text, or SQL with $N text parameters.
struct PgStatement {
    std::string sql;
    std::optional<std::vector<std::optional<std::string>>> params;
};

struct PgSinkCoreOptions {
    std::string conninfo;
    PgConnectSettings settings;
    // The table and what is written to it, checked at open; an empty table name
    // checks nothing (PostgresSink writes statements of its own).
    PgSinkTarget target;
    std::size_t max_statement_bytes{4U << 20};
    std::chrono::seconds lock_wait_timeout{30};
    int max_retries{3};
    std::string name;
    PgSinkServerFactory server_factory;  // unset: a real server
};

class PgSinkCore {
public:
    explicit PgSinkCore(PgSinkCoreOptions o);
    ~PgSinkCore();
    PgSinkCore(const PgSinkCore&) = delete;
    PgSinkCore& operator=(const PgSinkCore&) = delete;

    // Connects, sets the session up and checks the table. Clears a failure.
    void open(RuntimeContext* rt);
    void close() noexcept;

    // Throws unless the sink is open and has not failed.
    void require_usable(std::string_view what) const;
    // Throws when the sink has failed (an unopened sink passes).
    void require_not_failed(std::string_view what) const;
    // Fails the sink and throws `why`.
    [[noreturn]] void fail(const std::string& why);
    [[nodiscard]] bool failed() const noexcept { return !failure_.empty(); }

    [[nodiscard]] std::string escape(std::string_view value);
    [[nodiscard]] const PgTable& table() const noexcept { return table_; }
    [[nodiscard]] std::size_t statement_bytes() const noexcept { return opts_.max_statement_bytes; }

    // A flush's statements; with `one_row_each` it writes each row in a
    // statement of its own.
    using Plan = std::function<std::vector<PgStatement>(bool one_row_each)>;
    // Writes one flush as one transaction (see the header comment). Throws, and
    // fails the sink, when it cannot.
    void write(const Plan& plan, std::size_t records, std::size_t bytes);

private:
    class Watch;
    enum class Attempt { Done, Retry, OneRowEach };

    Attempt attempt_(const std::vector<PgStatement>& statements, bool one_row_each);
    PgOutcome exec_(const PgStatement& s);
    PgOutcome exec_(std::string_view sql);
    void rollback_() noexcept;
    [[noreturn]] void statement_failed_(const PgError& e, std::string_view during);
    [[nodiscard]] std::string describe_(const PgError& e) const;
    [[nodiscard]] bool cancel_requested_() const noexcept;
    void pause_(std::chrono::milliseconds d);

    PgSinkCoreOptions opts_;
    std::unique_ptr<PgSinkServer> server_;
    std::unique_ptr<Watch> watch_;
    RuntimeContext* rt_{nullptr};
    PgTable table_;
    std::string failure_;
    std::string last_retry_;
    bool open_{false};
    bool in_transaction_{false};
};

}  // namespace clink::postgres
