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
        saw_producer_ = true;
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CollectQueue::next() {
    std::unique_lock lk(m_);
    cv_.wait(lk,
             [&] { return !q_.empty() || aborted_ || (saw_producer_ && open_producers_ == 0); });
    if (!q_.empty()) {
        auto b = std::move(q_.front());
        q_.pop_front();
        return b;
    }
    if (aborted_) {
        return arrow::Status::Cancelled("collect stream aborted: the engine closed");
    }
    // End of stream: every producer closed and the queue drained.
    return std::shared_ptr<arrow::RecordBatch>{};
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
