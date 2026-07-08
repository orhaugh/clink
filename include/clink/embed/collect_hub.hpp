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
// therefore embedded-only by design: a cluster TaskManager has no scope
// (and no factory) for it.
//
// End-of-stream: the queue counts open producers. Once at least one
// producer has opened and the count returns to zero (the job's sink
// subtasks closed - completion, cancellation, or failure all close), the
// stream ends. abort() (engine close) wakes blocked readers with a
// Cancelled status instead.

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>

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

    // Blocks until a batch is available, the stream ended (returns nullptr),
    // or abort() was called (returns Cancelled).
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> next();

    // Wake every blocked reader with Cancelled and refuse further pushes.
    // Idempotent; called when the owning engine closes.
    void abort();

    // Exactly one consumer per table: the first claim wins.
    bool claim_consumer();

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<arrow::RecordBatch>> q_;
    int open_producers_ = 0;
    bool saw_producer_ = false;
    bool aborted_ = false;
    bool consumer_claimed_ = false;
};

// Per-engine table -> queue map.
class CollectHub {
public:
    std::shared_ptr<CollectQueue> queue(const std::string& table);
    void abort_all();

private:
    std::mutex m_;
    std::map<std::string, std::shared_ptr<CollectQueue>> queues_;
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
