#pragma once

// The collect sink's in-process plumbing: how connector='collect' rows
// reach the host application as typed Arrow batches.
//
// Topology per engine: EmbeddedEngine owns one CollectHub; the hub owns one
// CollectQueue per collect table. Sink subtasks (producers) convert their
// Row batches to typed Arrow RecordBatches via the same schema-driven
// batcher the wire uses (make_row_columnar_arrow_batcher) and push them
// into the queue; exactly one consumer per table drains it through an
// arrow::RecordBatchReader (exported over the Arrow C stream interface by
// libclink, or read directly in C++).
//
// The factory problem: sink factories are process-wide, but queues are
// per-engine. The embedded engine registers its hub under a fresh scope
// token in the process-wide CollectScopeRegistry and stamps that token
// onto every collect_sink_row op at submit time; the sink instance
// resolves its hub through the registry at open(). connector='collect' is
// therefore embedded-only by design: a cluster Worker has no scope
// (and no factory) for it.
//
// End-of-stream: the stream ends when the queue is drained, no producer is
// open, and every job writing the table has ended. Producers alone cannot
// say so: subtasks open and close at different moments, all close during a
// restart, and none opens at all when the job fails at deploy. The engine
// records which jobs write a table (CollectQueue::add_feed) and answers
// whether they have ended (CollectHub::set_feed_state). A job that ended
// failing ends its stream with that failure, after every row it delivered,
// unless the user cancelled it. abort() (engine close) wakes blocked
// readers with a Cancelled status instead.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <arrow/api.h>

namespace clink::embed {

// One collect table's producer/consumer bridge. Thread-safe; created by
// whichever side (sink or reader) touches the table first.
class CollectQueue {
public:
    // Sink lifecycle. The last producer_close() after any producer opened
    // marks end-of-stream.
    void producer_open();
    void producer_close();
    void push(std::shared_ptr<arrow::RecordBatch> batch);

    // The next queued batch, waiting at most `timeout`: a batch, Cancelled
    // after abort(), or nullptr when none arrived. Whether the stream has
    // ended is the reader's question, answered with the jobs' state.
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> next_for(std::chrono::milliseconds timeout);

    // No batch queued and no producer open.
    bool quiet();

    // The jobs writing this table, recorded as the engine submits them.
    void add_feed(std::uint64_t job_id);
    std::vector<std::uint64_t> feeds();

    // Wake every blocked reader with Cancelled and refuse further pushes.
    // Idempotent; called when the owning engine closes.
    void abort();

    // Exactly one consumer per table: the first claim wins.
    bool claim_consumer();

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<arrow::RecordBatch>> q_;
    std::vector<std::uint64_t> feeds_;
    int open_producers_ = 0;
    bool aborted_ = false;
    bool consumer_claimed_ = false;
};

// Whether the jobs writing a collect table have all ended, and if so with
// what: OK, or the failure that ends the stream.
struct FeedState {
    bool ended = false;
    arrow::Status status;
};

// Per-engine table -> queue map.
class CollectHub {
public:
    std::shared_ptr<CollectQueue> queue(const std::string& table);
    void abort_all();

    // The engine's answers about a job: has it ended, and what errors did it
    // record. Set once by the engine; detach() (engine close) clears them,
    // after which every stream reads as ended by the close.
    void set_job_queries(std::function<bool(std::uint64_t)> ended,
                         std::function<std::vector<std::string>(std::uint64_t)> errors);
    void detach();
    FeedState feed_state(const std::vector<std::uint64_t>& jobs);

    // A job the user cancelled ends its streams normally: the errors its
    // teardown records are not a failure of the job.
    void note_user_cancel(std::uint64_t job_id);

private:
    std::mutex m_;
    std::map<std::string, std::shared_ptr<CollectQueue>> queues_;
    std::function<bool(std::uint64_t)> job_ended_;
    std::function<std::vector<std::string>(std::uint64_t)> job_errors_;
    std::set<std::uint64_t> cancelled_;
};

// Process-wide scope token -> hub registry (weak: the engine owns the hub).
class CollectScopeRegistry {
public:
    static CollectScopeRegistry& instance();
    std::string register_hub(const std::shared_ptr<CollectHub>& hub);
    void unregister(const std::string& scope);
    std::shared_ptr<CollectHub> find(const std::string& scope);

private:
    std::mutex m_;
    std::uint64_t seq_ = 0;
    std::map<std::string, std::weak_ptr<CollectHub>> hubs_;
};

// Register the collect_sink_row factory into the process registries.
// Idempotent; called by the embedded engine's factory bootstrap.
void install_collect_sink();

}  // namespace clink::embed
