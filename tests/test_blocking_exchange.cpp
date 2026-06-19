// BATCH-2: blocking-exchange stage boundary.
//
// Covers:
//   1. Materialise-then-consume: process() emits nothing; the buffer is replayed
//      only on flush(), in arrival order.
//   2. Spill: a low byte threshold forces overflow batches to an Arrow IPC file
//      on disk; the replayed output is byte-identical and ordered, and the spill
//      file is cleaned up.
//   3. Control-element preservation: watermarks (incl. the BATCH-1 end-of-input
//      max watermark) flow through the exchange in order, so a downstream
//      watermark-driven consumer still fires.
//   4. End-to-end through LocalExecutor with the spill path active, and the
//      blocking-edge index recorded on the Dag for the batch scheduler.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/blocking_exchange.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

using namespace clink;

namespace {

// Drains a channel into a flat list of (value, event_time) pairs plus the
// largest watermark timestamp observed, in pop order.
struct Drained {
    std::vector<std::pair<std::int64_t, std::int64_t>> records;  // (value, event_time_ms)
    std::int64_t max_wm{std::numeric_limits<std::int64_t>::min()};
    std::size_t data_elements{0};
    std::size_t watermark_elements{0};
};

Drained drain_channel(BoundedChannel<StreamElement<std::int64_t>>& ch) {
    Drained out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            ++out.data_elements;
            for (const auto& r : e->as_data()) {
                const auto ts = r.event_time().has_value() ? r.event_time()->millis() : 0;
                out.records.emplace_back(r.value(), ts);
            }
        } else if (e->is_watermark()) {
            ++out.watermark_elements;
            out.max_wm = std::max(out.max_wm, e->as_watermark().timestamp().millis());
        }
    }
    return out;
}

Batch<std::int64_t> make_batch(std::vector<std::int64_t> values) {
    Batch<std::int64_t> b;
    for (auto v : values) {
        b.emplace(v, EventTime::from_millis(v));
    }
    return b;
}

// A batch of `n` sequential int64 records starting at `start`.
Batch<std::int64_t> make_range_batch(std::int64_t start, std::int64_t n) {
    Batch<std::int64_t> b;
    for (std::int64_t i = 0; i < n; ++i) {
        b.emplace(start + i, EventTime::from_millis(start + i));
    }
    return b;
}

// Emits `num_batches` data batches of `per_batch` int64 records (values dense
// from 0), no watermark of its own. Bounded, so the source runner appends the
// end-of-input max watermark.
class MultiBatchSource final : public Source<std::int64_t> {
public:
    MultiBatchSource(int num_batches, int per_batch)
        : num_batches_(num_batches), per_batch_(per_batch) {}

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    bool produce(Emitter<std::int64_t>& out) override {
        if (next_batch_ >= num_batches_ || this->cancelled()) {
            return false;
        }
        Batch<std::int64_t> b;
        for (int i = 0; i < per_batch_; ++i) {
            const std::int64_t v = static_cast<std::int64_t>(next_batch_ * per_batch_ + i);
            b.emplace(v, EventTime::from_millis(v));
        }
        out.emit_data(std::move(b));
        ++next_batch_;
        return next_batch_ < num_batches_;
    }

    std::string name() const override { return "multi_batch_source"; }

private:
    int num_batches_;
    int per_batch_;
    int next_batch_{0};
};

// Collects every record and the largest watermark it receives.
class RecordingSink final : public Sink<std::int64_t> {
public:
    RecordingSink(std::shared_ptr<std::vector<std::int64_t>> values,
                  std::shared_ptr<std::atomic<std::int64_t>> max_wm)
        : values_(std::move(values)), max_wm_(std::move(max_wm)) {}

    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            values_->push_back(r.value());
        }
    }

    void on_watermark(Watermark wm) override {
        const auto ts = wm.timestamp().millis();
        std::int64_t prev = max_wm_->load();
        while (ts > prev && !max_wm_->compare_exchange_weak(prev, ts)) {
        }
    }

    std::string name() const override { return "recording_sink"; }

private:
    std::shared_ptr<std::vector<std::int64_t>> values_;
    std::shared_ptr<std::atomic<std::int64_t>> max_wm_;
};

std::filesystem::path make_temp_spill_dir(const std::string& tag) {
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_blocking_exchange_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    return dir;
}

const std::int64_t kMaxWmMillis = EventTime::max().millis();

}  // namespace

// process() must not emit; the whole buffer is replayed only on flush(), in
// arrival order, with the watermark preserved relative to the data.
TEST(BlockingExchange, MaterialisesThenReplaysOnFlush) {
    BoundedChannel<StreamElement<std::int64_t>> ch(1024);
    Emitter<std::int64_t> em(&ch);

    BlockingExchangeOperator<std::int64_t> op(int64_arrow_batcher(), /*opts=*/{});
    op.set_id(OperatorId{1});

    op.process(StreamElement<std::int64_t>::data(make_batch({1, 2, 3})), em);
    op.process(StreamElement<std::int64_t>::watermark(Watermark(EventTime::from_millis(5))), em);
    op.process(StreamElement<std::int64_t>::data(make_batch({6, 7})), em);

    // Nothing has crossed the boundary yet.
    EXPECT_EQ(ch.size(), 0u);
    EXPECT_EQ(op.buffered_entry_count(), 3u);
    EXPECT_EQ(op.spilled_batch_count(), 0u);

    op.flush(em);

    const auto d = drain_channel(ch);
    ASSERT_EQ(d.records.size(), 5u);
    std::vector<std::int64_t> values;
    for (const auto& [v, ts] : d.records) {
        values.push_back(v);
        EXPECT_EQ(v, ts);  // event_time == value by construction
    }
    EXPECT_EQ(values, (std::vector<std::int64_t>{1, 2, 3, 6, 7}));
    EXPECT_EQ(d.watermark_elements, 1u);
    EXPECT_EQ(d.max_wm, 5);
}

// A tiny byte threshold forces every batch past the first onto disk. The replay
// is identical and ordered, and the spill file is removed afterwards.
TEST(BlockingExchange, SpillsOverThresholdAndReplaysInOrder) {
    const auto dir = make_temp_spill_dir("spill");
    BlockingExchangeOptions opts;
    opts.spill_threshold_bytes = 1;  // force spill after the first batch
    opts.spill_dir = dir.string();

    BoundedChannel<StreamElement<std::int64_t>> ch(1024);
    Emitter<std::int64_t> em(&ch);
    BlockingExchangeOperator<std::int64_t> op(int64_arrow_batcher(), opts);
    op.set_id(OperatorId{2});

    op.process(StreamElement<std::int64_t>::data(make_batch({10, 11})), em);
    op.process(StreamElement<std::int64_t>::data(make_batch({12, 13})), em);
    op.process(StreamElement<std::int64_t>::data(make_batch({14, 15})), em);
    EXPECT_GT(op.spilled_batch_count(), 0u);
    EXPECT_EQ(ch.size(), 0u);

    op.flush(em);

    const auto d = drain_channel(ch);
    std::vector<std::int64_t> values;
    for (const auto& [v, ts] : d.records) {
        values.push_back(v);
    }
    EXPECT_EQ(values, (std::vector<std::int64_t>{10, 11, 12, 13, 14, 15}));

    // Spill file cleaned up on flush.
    bool any_spill_file = false;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".arrow") {
            any_spill_file = true;
        }
    }
    EXPECT_FALSE(any_spill_file);
    std::filesystem::remove_all(dir);
}

// With spill disabled (no dir) the threshold is ignored and everything stays in
// memory - still blocking, still ordered.
TEST(BlockingExchange, NoSpillDirKeepsEverythingInMemory) {
    BlockingExchangeOptions opts;
    opts.spill_threshold_bytes = 1;  // would spill, but no dir => disabled
    opts.spill_dir.clear();

    BoundedChannel<StreamElement<std::int64_t>> ch(1024);
    Emitter<std::int64_t> em(&ch);
    BlockingExchangeOperator<std::int64_t> op(int64_arrow_batcher(), opts);

    op.process(StreamElement<std::int64_t>::data(make_batch({1})), em);
    op.process(StreamElement<std::int64_t>::data(make_batch({2})), em);
    EXPECT_EQ(op.spilled_batch_count(), 0u);
    op.flush(em);

    const auto d = drain_channel(ch);
    ASSERT_EQ(d.records.size(), 2u);
    EXPECT_EQ(d.records[0].first, 1);
    EXPECT_EQ(d.records[1].first, 2);
}

// Once spilling starts it stays on disk: a small batch arriving after overflow
// must NOT slip back into memory (it would break the post-spill memory bound).
TEST(BlockingExchange, SpillLatchesSoLaterSmallBatchStaysOnDisk) {
    const auto dir = make_temp_spill_dir("latch");
    BlockingExchangeOptions opts;
    opts.spill_threshold_bytes = 5000;  // a small int64 batch fits; a 2000-row one doesn't
    opts.spill_dir = dir.string();

    BoundedChannel<StreamElement<std::int64_t>> ch(1024);
    Emitter<std::int64_t> em(&ch);
    BlockingExchangeOperator<std::int64_t> op(int64_arrow_batcher(), opts);
    op.set_id(OperatorId{3});

    // Small batch: stays in memory.
    op.process(StreamElement<std::int64_t>::data(make_range_batch(0, 2)), em);
    EXPECT_EQ(op.spilled_batch_count(), 0u);
    const auto bytes_after_small = op.buffered_in_memory_bytes();
    EXPECT_GT(bytes_after_small, 0u);

    // Large batch: crosses the threshold and spills.
    op.process(StreamElement<std::int64_t>::data(make_range_batch(100, 2000)), em);
    EXPECT_EQ(op.spilled_batch_count(), 1u);

    // Another small batch: the latch keeps it on disk; in-memory bytes unchanged.
    op.process(StreamElement<std::int64_t>::data(make_range_batch(5000, 2)), em);
    EXPECT_EQ(op.spilled_batch_count(), 2u);
    EXPECT_EQ(op.buffered_in_memory_bytes(), bytes_after_small);

    op.flush(em);
    const auto d = drain_channel(ch);
    ASSERT_EQ(d.records.size(), 2u + 2000u + 2u);
    EXPECT_EQ(d.records.front().first, 0);    // first small batch
    EXPECT_EQ(d.records.back().first, 5001);  // last record of the trailing small batch
    std::filesystem::remove_all(dir);
}

// End-to-end: bounded source -> blocking exchange (spill on) -> sink, run to
// completion. All records arrive in order, the end-of-input max watermark
// survives the exchange and reaches the sink, and the Dag records the edge.
TEST(BlockingExchange, EndToEndThroughExecutorWithSpill) {
    const auto dir = make_temp_spill_dir("e2e");
    auto values = std::make_shared<std::vector<std::int64_t>>();
    auto max_wm =
        std::make_shared<std::atomic<std::int64_t>>(std::numeric_limits<std::int64_t>::min());

    BlockingExchangeOptions opts;
    opts.spill_threshold_bytes = 1;  // exercise the disk path
    opts.spill_dir = dir.string();

    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<MultiBatchSource>(/*num_batches=*/8,
                                                                               /*per_batch=*/16));
    auto ex = dag.add_blocking_exchange<std::int64_t>(src, int64_arrow_batcher(), opts);
    dag.add_sink<std::int64_t>(ex, std::make_shared<RecordingSink>(values, max_wm));

    ASSERT_EQ(dag.blocking_edge_runner_indices().size(), 1u);

    JobConfig config;
    config.execution_mode = JobConfig::ExecutionMode::Batch;
    LocalExecutor exec(std::move(dag), config);
    exec.run_to_completion();
    ASSERT_TRUE(exec.operator_errors().empty())
        << exec.operator_errors().front().first << ": " << exec.operator_errors().front().second;

    ASSERT_EQ(values->size(), 128u);
    for (std::int64_t i = 0; i < 128; ++i) {
        EXPECT_EQ((*values)[static_cast<std::size_t>(i)], i) << "at index " << i;
    }
    EXPECT_EQ(max_wm->load(), kMaxWmMillis);

    std::filesystem::remove_all(dir);
}
