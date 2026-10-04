#include "clink/embed/collect_hub.hpp"

#include <mutex>
#include <stdexcept>
#include <utility>

#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_kind.hpp"

namespace clink::embed {

// ---- CollectQueue ----

void CollectQueue::producer_open() {
    {
        std::lock_guard lk(m_);
        ++open_producers_;
    }
    cv_.notify_all();
}

void CollectQueue::producer_close() {
    {
        std::lock_guard lk(m_);
        if (open_producers_ > 0) {
            --open_producers_;
        }
    }
    cv_.notify_all();
}

void CollectQueue::push(std::shared_ptr<arrow::RecordBatch> batch) {
    if (!batch) {
        return;
    }
    {
        std::lock_guard lk(m_);
        if (aborted_) {
            return;
        }
        q_.push_back(std::move(batch));
    }
    cv_.notify_all();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CollectQueue::next_for(
    std::chrono::milliseconds timeout) {
    std::unique_lock lk(m_);
    cv_.wait_for(lk, timeout, [&] { return !q_.empty() || aborted_; });
    if (!q_.empty()) {
        auto b = std::move(q_.front());
        q_.pop_front();
        return b;
    }
    if (aborted_) {
        return arrow::Status::Cancelled("collect stream aborted: the engine closed");
    }
    return std::shared_ptr<arrow::RecordBatch>{};
}

bool CollectQueue::quiet() {
    std::lock_guard lk(m_);
    return q_.empty() && open_producers_ == 0;
}

void CollectQueue::add_feed(std::uint64_t job_id) {
    std::lock_guard lk(m_);
    feeds_.push_back(job_id);
}

std::vector<std::uint64_t> CollectQueue::feeds() {
    std::lock_guard lk(m_);
    return feeds_;
}

void CollectQueue::abort() {
    {
        std::lock_guard lk(m_);
        aborted_ = true;
    }
    cv_.notify_all();
}

bool CollectQueue::claim_consumer() {
    std::lock_guard lk(m_);
    if (consumer_claimed_) {
        return false;
    }
    consumer_claimed_ = true;
    return true;
}

// ---- CollectHub ----

std::shared_ptr<CollectQueue> CollectHub::queue(const std::string& table) {
    std::lock_guard lk(m_);
    auto& q = queues_[table];
    if (!q) {
        q = std::make_shared<CollectQueue>();
    }
    return q;
}

void CollectHub::abort_all() {
    std::lock_guard lk(m_);
    for (auto& [name, q] : queues_) {
        q->abort();
    }
}

void CollectHub::set_job_queries(std::function<bool(std::uint64_t)> ended,
                                 std::function<std::vector<std::string>(std::uint64_t)> errors) {
    std::lock_guard lk(m_);
    job_ended_ = std::move(ended);
    job_errors_ = std::move(errors);
}

void CollectHub::detach() {
    std::lock_guard lk(m_);
    job_ended_ = nullptr;
    job_errors_ = nullptr;
}

FeedState CollectHub::feed_state(const std::vector<std::uint64_t>& jobs) {
    // Held across the queries so detach() cannot complete while the engine
    // is being asked; the engine's answers take no lock of the hub's.
    std::lock_guard lk(m_);
    if (!job_ended_ || !job_errors_) {
        return FeedState{true,
                         arrow::Status::Cancelled("collect stream aborted: the engine closed")};
    }
    if (jobs.empty()) {
        return FeedState{};  // no job writes the table yet
    }
    for (const auto id : jobs) {
        if (!job_ended_(id)) {
            return FeedState{};
        }
    }
    std::string failures;
    for (const auto id : jobs) {
        if (cancelled_.count(id) != 0) {
            continue;
        }
        for (const auto& e : job_errors_(id)) {
            failures += e;
            failures += "; ";
        }
    }
    FeedState state;
    state.ended = true;
    if (!failures.empty()) {
        state.status = arrow::Status::Invalid("the job writing this table failed: ", failures);
    }
    return state;
}

void CollectHub::note_user_cancel(std::uint64_t job_id) {
    std::lock_guard lk(m_);
    cancelled_.insert(job_id);
}

// ---- CollectScopeRegistry ----

CollectScopeRegistry& CollectScopeRegistry::instance() {
    static CollectScopeRegistry reg;
    return reg;
}

std::string CollectScopeRegistry::register_hub(const std::shared_ptr<CollectHub>& hub) {
    std::lock_guard lk(m_);
    const std::string scope = "collect-scope-" + std::to_string(seq_++);
    hubs_[scope] = hub;
    return scope;
}

void CollectScopeRegistry::unregister(const std::string& scope) {
    std::lock_guard lk(m_);
    hubs_.erase(scope);
}

std::shared_ptr<CollectHub> CollectScopeRegistry::find(const std::string& scope) {
    std::lock_guard lk(m_);
    auto it = hubs_.find(scope);
    return it == hubs_.end() ? nullptr : it->second.lock();
}

// ---- The sink ----

namespace {

// Converts each Row batch to a typed Arrow RecordBatch (the same
// schema-driven batcher the wire and Parquet sinks use) and pushes it into
// the owning engine's queue for this table. Producer refcounts drive
// end-of-stream: close() fires on completion, cancellation and failure
// alike, so a reader never waits on a dead job.
class CollectSink final : public Sink<sql::Row> {
public:
    CollectSink(std::string scope,
                std::string table,
                ArrowBatcher<sql::Row> batcher,
                bool changelog)
        : scope_(std::move(scope)),
          table_(std::move(table)),
          batcher_(std::move(batcher)),
          changelog_(changelog) {}

    void open() override {
        auto hub = CollectScopeRegistry::instance().find(scope_);
        if (!hub) {
            throw std::runtime_error(
                "collect sink: engine scope '" + scope_ +
                "' not found - connector='collect' only works in embedded execution "
                "(EmbeddedEngine / libclink), not on a cluster");
        }
        queue_ = hub->queue(table_);
        queue_->producer_open();
    }

    void on_data(const Batch<sql::Row>& batch) override {
        if (batch.empty() || !queue_) {
            return;
        }
        if (changelog_) {
            // Surface each row's changelog kind as the declared leading
            // row_kind column (insert when unmarked) so the host sees the
            // changelog instead of a silent flatten into inserts.
            Batch<sql::Row> tagged;
            for (const auto& rec : batch) {
                sql::Row row = rec.value();
                row.values["row_kind"] = config::JsonValue{sql::row_kind_of(row)};
                if (rec.event_time().has_value()) {
                    tagged.emplace(std::move(row), *rec.event_time());
                } else {
                    tagged.emplace(std::move(row));
                }
            }
            push_batch_(tagged);
            return;
        }
        push_batch_(batch);
    }

    void close() override {
        if (queue_) {
            queue_->producer_close();
            queue_.reset();
        }
    }

private:
    void push_batch_(const Batch<sql::Row>& batch) {
        if (auto rb = batcher_.build(batch)) {
            // The schema-driven batcher prepends the engine's event-time
            // column (the wire layout); the host must see exactly the
            // declared columns, so strip it. The reader strips the same
            // field from the batcher schema, keeping both sides identical.
            auto stripped = rb->RemoveColumn(0);
            if (stripped.ok()) {
                queue_->push(std::move(*stripped));
            }
        }
    }

    std::string scope_;
    std::string table_;
    ArrowBatcher<sql::Row> batcher_;
    bool changelog_{false};
    std::shared_ptr<CollectQueue> queue_;
};

}  // namespace

void install_collect_sink() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        clink::plugin::PluginRegistry reg;
        reg.register_sink<sql::Row>(
            "collect_sink_row",
            [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<sql::Row>> {
                const auto scope = ctx.param_or("collect_scope", "");
                const auto table = ctx.param_or("collect_table", "");
                if (scope.empty()) {
                    throw std::runtime_error(
                        "collect_sink_row: no 'collect_scope' - connector='collect' only "
                        "works in embedded execution (EmbeddedEngine / libclink)");
                }
                if (table.empty()) {
                    throw std::runtime_error("collect_sink_row: 'collect_table' is required");
                }
                const bool changelog = ctx.param_or("collect_changelog", "") == "true";
                auto cols = sql::parse_row_schema(ctx.param_or("schema_columns"));
                if (changelog) {
                    // The leading changelog-kind column the host sees; the
                    // reader prepends the identical field to its schema.
                    cols.insert(cols.begin(), sql::RowColumn{"row_kind", arrow::utf8()});
                }
                auto batcher = sql::make_row_columnar_arrow_batcher(std::move(cols));
                return std::make_shared<CollectSink>(scope, table, std::move(batcher), changelog);
            });
    });
}

}  // namespace clink::embed
