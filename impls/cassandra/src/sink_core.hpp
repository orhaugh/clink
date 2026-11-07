#pragma once

// What the two Cassandra sinks share: the session and the table check at open,
// the window of writes in flight with its bounds, the writes' timestamps, the
// failed state, cancellation, the fault points and the metrics.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/cassandra/cassandra_rules.hpp"
#include "clink/cassandra/cassandra_service.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::cassandra::detail {

// The option bounds both sinks and their factories check.
inline constexpr std::size_t kMaxInFlightLimit = 8192;  // the driver's pending-request queue
inline constexpr std::size_t kMinBytesBound = 1024;
inline constexpr std::size_t kMaxBytesBound = std::size_t{1} << 30U;
inline constexpr std::size_t kMaxBufferedRecordsLimit = 1'000'000;
inline constexpr int kMaxTimeoutMs = 600'000;

struct SinkCoreOptions {
    std::string name;  // the sink's name: error messages, the log and the fault points
    CassandraConnParams conn;
    std::string keyspace;  // folded
    std::string table;     // folded
    // The key columns as given (folded), empty for the plain sink, which writes
    // whatever key its records carry.
    std::vector<std::string> key_columns;
    std::size_t max_in_flight{1024};
    std::size_t max_in_flight_bytes{64U << 20U};
    std::shared_ptr<CassandraWriteService> service;  // empty: the driver
};

// Checks the options both sinks take and folds the names, throwing at the first
// that is out of bounds. `keyed` is true for the upsert sink, which needs key
// columns.
[[nodiscard]] SinkCoreOptions checked_options(const std::string& name,
                                              const CassandraConnParams& conn,
                                              const std::string& keyspace,
                                              const std::string& table,
                                              const std::vector<std::string>& key_columns,
                                              bool keyed,
                                              std::size_t max_in_flight,
                                              std::size_t max_in_flight_bytes,
                                              std::shared_ptr<CassandraWriteService> service);

class SinkCore {
public:
    explicit SinkCore(SinkCoreOptions opts);
    ~SinkCore();
    SinkCore(const SinkCore&) = delete;
    SinkCore& operator=(const SinkCore&) = delete;
    SinkCore(SinkCore&&) = delete;
    SinkCore& operator=(SinkCore&&) = delete;

    // Connects, reads the table and checks the key columns against its primary
    // key, and prepares the insert (and the delete when there are key columns).
    // Refuses to run on an instance that is open and has not failed; an instance
    // that failed, or was closed, starts afresh.
    void open(RuntimeContext* rt);

    // Throws unless open and not failed.
    void check_usable() const;

    // The table's key, in key order, once open (for the upsert sink).
    [[nodiscard]] const std::vector<CassandraKeyColumn>& key() const { return key_; }

    // Hands one write to the service once the window has room for `bytes`, so
    // the writes in flight never exceed max_in_flight or, unless this one is
    // alone, max_in_flight_bytes. The write's timestamp is taken here.
    void submit(RuntimeContext* rt, CassandraWrite write, std::size_t bytes);

    // Waits for every write in flight to be acknowledged. Throws, after failing
    // the sink, at the first write that is not.
    void drain(RuntimeContext* rt);

    // Fails the sink: drops what is in flight (outcomes unknown), records why,
    // and throws. Every later call that would write or acknowledge a barrier
    // throws until open() starts afresh.
    [[noreturn]] void fail(const std::string& why, std::size_t dropped_buffered = 0);

    // How many buffered rows a failure now would drop, for its message.
    void note_buffered(std::size_t rows) noexcept { buffered_ = rows; }

    // The fault point "<name>.<suffix>".
    void reach(const char* suffix) const;

    // Releases what is in flight without waiting for it and closes the session.
    // Writes nothing. Never throws.
    void close() noexcept;

    [[nodiscard]] bool failed() const noexcept { return failure_.has_value(); }
    [[nodiscard]] bool is_open() const noexcept { return open_; }
    [[nodiscard]] std::size_t in_flight() const noexcept { return window_.size(); }
    [[nodiscard]] std::size_t in_flight_bytes() const noexcept { return window_bytes_; }
    [[nodiscard]] const std::string& name() const noexcept { return opts_.name; }

private:
    struct InFlight {
        std::uint64_t ticket{0};
        std::size_t bytes{0};
        std::chrono::steady_clock::time_point submitted;
    };

    // Takes the oldest write's outcome, waiting up to `wait`; false while it is
    // unresolved. Fails the sink on an outcome that is not ok.
    bool settle_oldest(std::chrono::microseconds wait);
    void check_cancel(RuntimeContext* rt, const char* doing);
    [[nodiscard]] std::int64_t next_timestamp();
    // Releases every write in flight without waiting for it.
    void release_window() noexcept;

    SinkCoreOptions opts_;
    std::shared_ptr<CassandraWriteService> service_;
    std::vector<CassandraKeyColumn> key_;
    std::deque<InFlight> window_;
    std::size_t window_bytes_{0};
    std::int64_t last_timestamp_{0};
    bool open_{false};
    std::optional<std::string> failure_;
    std::uint64_t submitted_{0};     // since open
    std::uint64_t acknowledged_{0};  // since open
    std::size_t buffered_{0};
};

}  // namespace clink::cassandra::detail
