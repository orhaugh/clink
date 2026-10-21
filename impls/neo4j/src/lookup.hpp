#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/async_lookup_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/lookup_join.hpp"
#include "clink/sql/row.hpp"

#include "connection.hpp"
#include "options.hpp"

namespace clink::neo4j {

// What one lookup found: a match, with one value per result column, or no
// match. A failure is never a result.
struct LookupResult {
    bool matched{false};
    std::vector<clink::config::JsonValue> values;  // LookupOptions::result_columns order
    // The client's wall clock (epoch ms) when the reply arrived, and the
    // reply's bookmark. Neither says when the graph was in the state read.
    std::int64_t observed_at_ms{0};
    std::string bookmark;
};

// One lookup on its way through the pool. The request thread that takes it
// sets `result` or `error`, then `done`; the coroutine waiting on it reads
// them only once it sees `done`.
struct LookupCall {
    std::string key;  // $key as typed JSON
    std::atomic<bool> done{false};
    LookupResult result;
    std::exception_ptr error;
    std::chrono::steady_clock::time_point queued_at;
};

// The requests of one lookup subtask: max_concurrent_requests threads, each
// with its own client and RequestWatch, taking calls from one queue in the
// order they arrived. Every request is a read: it carries accessMode READ,
// and the statement passed the open-time check.
class LookupPool {
public:
    LookupPool(LookupOptions options,
               CancelSignal cancel,
               MetricsRegistry* metrics,
               std::uint64_t op_id);
    ~LookupPool();
    LookupPool(const LookupPool&) = delete;
    LookupPool& operator=(const LookupPool&) = delete;

    // Checks the statement with an EXPLAIN (retried within the retry budget),
    // then starts the request threads. Throws Neo4jError when the statement
    // is refused, the server answers with a refusal, or the budget runs out.
    void open();

    // Queues `call`. The queue holds at most one call per lookup the operator
    // has in flight, so the operator's max_in_flight bounds it.
    void submit(std::shared_ptr<LookupCall> call);

    // Ends the pool: calls still queued fail as cancelled, a request in flight
    // is interrupted, and every thread is joined. Idempotent.
    void stop();

    struct Counts {
        std::uint64_t requests{0};
        std::uint64_t retries{0};
        std::uint64_t matches{0};
        std::uint64_t no_matches{0};
        std::uint64_t failures{0};
    };
    [[nodiscard]] Counts counts() const;
    [[nodiscard]] const std::string& server_version() const noexcept { return server_version_; }
    [[nodiscard]] std::string where() const;

private:
    struct Worker {
        std::unique_ptr<clink::http_connector::HttpRequest> http;
        std::unique_ptr<RequestWatch> watch;
        std::thread thread;
        std::atomic<bool> exited{true};  // false from the thread's start until it leaves
    };

    void check_statement_(Worker& w);
    void run_(Worker& w);
    void serve_(Worker& w, LookupCall& call);
    [[nodiscard]] LookupResult read_(Worker& w, const std::string& key);
    [[nodiscard]] LookupResult answer_(const Reply& reply) const;
    [[noreturn]] void fail_(const char* code, const std::string& message) const;
    void publish_queue_gauges_(std::size_t queued);

    LookupOptions opts_;
    CancelSignal cancel_;
    MetricsRegistry* metrics_;
    std::uint64_t op_id_;
    std::string path_;
    std::string body_prefix_;  // {"statement":...,"accessMode":"READ",...,"parameters":{"key":
    clink::http_connector::BoundedRequest request_;
    std::vector<std::string> result_names_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::string server_version_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<LookupCall>> queue_;
    std::atomic<bool> stopping_{false};
    bool started_{false};

    std::atomic<std::uint64_t> requests_{0};
    std::atomic<std::uint64_t> retries_{0};
    std::atomic<std::uint64_t> matches_{0};
    std::atomic<std::uint64_t> no_matches_{0};
    std::atomic<std::uint64_t> failures_{0};
};

// neo4j_lookup_join_row: a SQL lookup join against Neo4j. Each probe row's key
// is sent as $key to the lookup table's statement through the pool; the
// result, or the no-match, is merged as the generic lookup join merges it.
// Emission is in input order, and every lookup started before a watermark or
// checkpoint barrier finishes and is emitted before the marker is forwarded
// (AsyncLookupOperator), so no lookup is in flight across a checkpoint: a
// restore replays the input after it and runs those lookups again. A lookup
// that fails fails the task; it never becomes a no-match.
class Neo4jLookupJoin final : public Operator<clink::sql::Row, clink::sql::Row> {
public:
    explicit Neo4jLookupJoin(LookupOptions options);
    ~Neo4jLookupJoin() override;

    void open() override;
    void close() override;
    void process(const StreamElement<clink::sql::Row>& element,
                 Emitter<clink::sql::Row>& out) override;
    void flush(Emitter<clink::sql::Row>& out) override;
    // While lookups are in flight, the runner wakes the operator every few
    // milliseconds to emit those that have finished, so a result never waits
    // for the next input element.
    void fire_due_timers(Emitter<clink::sql::Row>& out, std::int64_t now_ms) override;
    [[nodiscard]] std::optional<std::int64_t> next_timer_deadline_ms() const override;
    [[nodiscard]] std::string name() const override { return "neo4j_lookup_join_row"; }

    [[nodiscard]] std::size_t in_flight() const noexcept {
        return inner_ ? inner_->in_flight_count() : 0;
    }
    [[nodiscard]] const LookupPool* pool() const noexcept { return pool_.get(); }

private:
    clink::async::Task<clink::sql::Row> lookup_(clink::sql::Row probe);
    void publish_in_flight_();

    LookupOptions opts_;
    clink::sql::LookupJoinShape shape_;
    std::unique_ptr<LookupPool> pool_;
    std::unique_ptr<AsyncLookupOperator<clink::sql::Row, clink::sql::Row>> inner_;
    std::uint64_t null_keys_{0};
    bool closed_{false};
};

}  // namespace clink::neo4j
