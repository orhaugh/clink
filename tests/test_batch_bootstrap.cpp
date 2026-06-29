// BATCH-4: bootstrap streaming state from a bounded run.
//
// A bounded backfill job builds keyed state, that state is captured as a
// savepoint via LocalExecutor::take_savepoint(), and a live streaming job
// restores it via JobConfig::restore_from and continues. The two jobs share the
// operator uid so the OperatorId (and thus the keyed state) lines up across the
// boundary. This is the single-engine batch-to-stream seam: no second system,
// the same savepoint restore path used for failover.

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

using KV = std::pair<std::string, std::int64_t>;

std::string encode_i64(std::int64_t v) {
    std::array<char, 8> b{};
    auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        b[static_cast<std::size_t>(i)] = static_cast<char>((u >> (i * 8)) & 0xFF);
    }
    return std::string(b.data(), b.size());
}

std::int64_t decode_i64(const StateBackend::Value& v) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8 && i < static_cast<int>(v.size()); ++i) {
        u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(v[static_cast<std::size_t>(i)]))
             << (i * 8);
    }
    return static_cast<std::int64_t>(u);
}

// Per-key running sum, stored as keyed state under the operator's id. Reads the
// current sum, adds the record value, writes it back, and emits (key, new sum).
// Stateless across restart except through the backend, so its whole state is in
// the savepoint.
class KeyedSumOperator final : public Operator<KV, KV> {
public:
    void process(const StreamElement<KV>& element, Emitter<KV>& out) override {
        if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
            return;
        }
        if (element.is_barrier()) {
            this->on_barrier(element.as_barrier(), out);
            return;
        }
        if (!element.is_data()) {
            return;
        }
        auto* backend = this->runtime()->state_backend();
        Batch<KV> out_batch;
        for (const auto& r : element.as_data()) {
            const std::string& key = r.value().first;
            std::int64_t sum = 0;
            if (auto cur = backend->get(this->id(), key); cur.has_value()) {
                sum = decode_i64(*cur);
            }
            sum += r.value().second;
            backend->put(this->id(), key, encode_i64(sum));
            out_batch.emplace(KV{key, sum});
        }
        out.emit_data(std::move(out_batch));
    }

    std::string name() const override { return "keyed_sum"; }
};

std::shared_ptr<VectorSource<KV>> kv_source(std::vector<KV> rows) {
    std::vector<Record<KV>> records;
    records.reserve(rows.size());
    for (auto& kv : rows) {
        records.emplace_back(std::move(kv));
    }
    return std::make_shared<VectorSource<KV>>(std::move(records));
}

std::int64_t state_for(StateBackend& backend, OperatorId op, const std::string& key) {
    auto v = backend.get(op, key);
    return v.has_value() ? decode_i64(*v) : -1;
}

}  // namespace

TEST(BatchBootstrap, BoundedRunSeedsStreamingState) {
    // ---- Stage 1: bounded backfill builds keyed state, then savepoint. ----
    auto backfill_backend = std::make_shared<InMemoryStateBackend>();
    OperatorId sum_op_id{0};
    Snapshot savepoint;
    {
        Dag dag;
        auto src = dag.add_source<KV>(kv_source({{"a", 1}, {"b", 10}, {"a", 2}}));
        auto sum = std::make_shared<KeyedSumOperator>();
        sum->set_uid("keyed_sum");  // stable identity across both jobs
        auto h = dag.add_operator<KV, KV>(src, sum);
        dag.add_sink<KV>(h, std::make_shared<CollectingSink<KV>>());
        sum_op_id = sum->id();

        JobConfig cfg;
        cfg.state_backend = backfill_backend;
        cfg.execution_mode = JobConfig::ExecutionMode::Batch;
        LocalExecutor exec(std::move(dag), cfg);
        exec.run_to_completion();
        ASSERT_TRUE(exec.operator_errors().empty());

        // Batch-built state: a = 1+2 = 3, b = 10.
        EXPECT_EQ(state_for(*backfill_backend, sum_op_id, "a"), 3);
        EXPECT_EQ(state_for(*backfill_backend, sum_op_id, "b"), 10);

        savepoint = exec.take_savepoint(CheckpointId{1});
    }

    // ---- Stage 2: streaming job restores the savepoint and continues. ----
    auto live_backend = std::make_shared<InMemoryStateBackend>();
    {
        Dag dag;
        // The live stream sees new records for "a" and a brand-new key "c";
        // it never sees "b", which must survive untouched from the backfill.
        auto src = dag.add_source<KV>(kv_source({{"a", 100}, {"c", 1000}}));
        auto sum = std::make_shared<KeyedSumOperator>();
        sum->set_uid("keyed_sum");  // same uid => same OperatorId => state lines up
        auto h = dag.add_operator<KV, KV>(src, sum);
        auto sink = std::make_shared<CollectingSink<KV>>();
        dag.add_sink<KV>(h, sink);
        ASSERT_EQ(sum->id(), sum_op_id);  // identity matches the backfill operator

        JobConfig cfg;
        cfg.state_backend = live_backend;
        cfg.restore_from = savepoint;  // bootstrap from the bounded run
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();
        ASSERT_TRUE(exec.operator_errors().empty());

        // "a" continued from the backfilled 3: 3 + 100 = 103.
        EXPECT_EQ(state_for(*live_backend, sum_op_id, "a"), 103);
        // "b" was restored from the backfill and never touched by the stream.
        EXPECT_EQ(state_for(*live_backend, sum_op_id, "b"), 10);
        // "c" is a fresh key seen only by the stream.
        EXPECT_EQ(state_for(*live_backend, sum_op_id, "c"), 1000);
    }
}

TEST(BatchBootstrap, TakeSavepointRejectsMissingBackend) {
    Dag dag;
    dag.add_source<KV>(kv_source({}));
    JobConfig cfg;  // no state backend
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();
    EXPECT_THROW(exec.take_savepoint(), std::logic_error);
}
