// clink_bench - single-process wall-clock throughput harness.
//
// Runs a parameterised in-process pipeline (no coordinator / worker / wire) and
// emits a JSON line summarising throughput + per-record latency.
// Establishes a baseline so later changes (backpressure rework,
// rescaling work, etc.) can measure regressions.
//
// Scenario `pipe_throughput`:
//     VectorSource<int64_t>(N records)
//       -> MapOperator(x*2)
//       -> MapOperator(x+1)
//       -> CountingSink
//
// Why this scenario: exercises the operator runner hot path
// (Emitter::emit_data, MapOperator::process, BoundedChannel push/pop)
// without dragging in keyed state, windowing, or watermarks - those
// add fixed overhead that obscures the per-record cost we care about
// for V1. Future scenarios (keyed_reduce, windowed_aggregate) can be
// added when those code paths matter.
//
// Usage:
//   clink_bench --scenario=pipe_throughput --records=1000000 \
//                 [--warmup=100000] [--format=json|human]
//
// JSON output (one line, suitable for diffing across runs):
//   {"scenario":"pipe_throughput","records":1000000,"wall_ms":42.3,
//    "throughput_per_sec":23640661,"ns_per_record":42}

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/columnar_filter_operator.hpp"
#include "clink/operators/columnar_keyed_aggregate_operator.hpp"
#include "clink/operators/columnar_keyed_vector_source.hpp"
#include "clink/operators/columnar_string_filter_operator.hpp"
#include "clink/operators/columnar_string_vector_source.hpp"
#include "clink/operators/columnar_vector_source.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/sharded_keyed_stage.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/sharded_in_memory_state_backend.hpp"

namespace {

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

struct BenchResult {
    std::string scenario;
    std::int64_t records{0};
    double wall_ms{0};
    double throughput_per_sec{0};
    double ns_per_record{0};
};

BenchResult run_pipe_throughput(std::int64_t records, std::int64_t warmup) {
    using namespace clink;

    // Build the input vector once (the alloc cost is excluded from the
    // measurement). Warmup records are prepended so the first inputs
    // amortise allocation / first-run cache effects.
    std::vector<Record<std::int64_t>> input;
    input.reserve(static_cast<std::size_t>(records + warmup));
    for (std::int64_t i = 0; i < warmup + records; ++i) {
        input.emplace_back(Record<std::int64_t>{i});
    }

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(input));
    auto map1 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 2; }, "map_x2");
    auto map2 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v + 1; }, "map_plus1");

    std::int64_t consumed = 0;
    auto sink = std::make_shared<FunctionSink<std::int64_t>>(
        [&consumed](const std::int64_t&) { ++consumed; });

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, map1);
    auto h2 = dag.add_operator<std::int64_t, std::int64_t>(h1, map2);
    dag.add_sink<std::int64_t>(h2, sink);

    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();

    const auto total = consumed;
    if (total != warmup + records) {
        std::cerr << "clink_bench: sink consumed " << total << " records, expected "
                  << (warmup + records) << "\n";
    }

    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    BenchResult r;
    r.scenario = "pipe_throughput";
    r.records = records;
    r.wall_ms = static_cast<double>(wall_ns) / 1'000'000.0;
    // Throughput is computed over the MEASURED records only - warmup is
    // included in wall time but reported as part of the same run, so
    // setting warmup=0 (the default) keeps the math simple. For larger
    // warmups the reported throughput slightly underestimates because
    // the warmup share of wall time is attributed to the measured slice.
    const double seconds = static_cast<double>(wall_ns) / 1'000'000'000.0;
    r.throughput_per_sec = seconds > 0 ? static_cast<double>(records) / seconds : 0;
    r.ns_per_record = records > 0 ? static_cast<double>(wall_ns) / static_cast<double>(records) : 0;
    return r;
}

// Counts rows via Batch::size(), which reads the columnar row count WITHOUT
// materializing rows - so on the columnar arm the sink itself decodes zero
// rows, keeping the whole pipeline columnar.
class SizeCountingSink final : public clink::Sink<std::int64_t> {
public:
    using clink::Sink<std::int64_t>::on_data;
    void on_data(const clink::Batch<std::int64_t>& batch) override { count_ += batch.size(); }
    std::string name() const override { return "size_count"; }
    std::size_t count() const noexcept { return count_; }

private:
    std::size_t count_{0};
};

// One arm of the columnar_filter comparison.
struct FilterArm {
    double wall_ms{0};
    double ns_per_record{0};
    std::size_t rows_out{0};
    std::uint64_t materializations{0};
};

template <typename MakePipeline>
FilterArm run_filter_arm(std::int64_t records, MakePipeline make) {
    using namespace clink;
    detail::batch_materialize_counter().store(0, std::memory_order_relaxed);
    auto sink = std::make_shared<SizeCountingSink>();
    Dag dag = make(sink);
    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    FilterArm a;
    a.wall_ms = static_cast<double>(wall_ns) / 1'000'000.0;
    a.ns_per_record = records > 0 ? static_cast<double>(wall_ns) / static_cast<double>(records) : 0;
    a.rows_out = sink->count();
    a.materializations = detail::batch_materialize_counter().load(std::memory_order_relaxed);
    return a;
}

// columnar_filter: same ColumnarVectorSource feeding (a) a ROW
// FilterOperator (which lazily decodes every batch to rows) vs (b) the
// ColumnarFilterOperator (which scans the Arrow value buffer, zero decode).
// The win is the wall-clock delta; the materialization counts prove the
// columnar arm did no row decode and the rows_out prove both did equal work.
void run_columnar_filter(std::int64_t records, std::int64_t batch_size, std::int64_t threshold) {
    using namespace clink;
    std::vector<std::int64_t> input;
    input.reserve(static_cast<std::size_t>(records));
    for (std::int64_t i = 0; i < records; ++i) {
        input.push_back(i);
    }
    const auto bs = static_cast<std::size_t>(batch_size);

    const FilterArm row = run_filter_arm(records, [&](std::shared_ptr<SizeCountingSink> sink) {
        Dag dag;
        auto src = std::make_shared<ColumnarVectorSource>(input, bs);
        auto filt = std::make_shared<FilterOperator<std::int64_t>>(
            [threshold](const std::int64_t& v) { return v >= threshold; }, "row_filter");
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, filt);
        dag.add_sink<std::int64_t>(h1, sink);
        return dag;
    });

    const FilterArm col = run_filter_arm(records, [&](std::shared_ptr<SizeCountingSink> sink) {
        Dag dag;
        auto src = std::make_shared<ColumnarVectorSource>(input, bs);
        auto filt = std::make_shared<ColumnarFilterOperator>(threshold);
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, filt);
        dag.add_sink<std::int64_t>(h1, sink);
        return dag;
    });

    const double speedup = col.wall_ms > 0 ? row.wall_ms / col.wall_ms : 0;
    std::printf("scenario               columnar_filter\n");
    std::printf("records                %lld  (batch_size=%lld, threshold=%lld)\n",
                static_cast<long long>(records),
                static_cast<long long>(batch_size),
                static_cast<long long>(threshold));
    std::printf("row arm    wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                row.wall_ms,
                row.ns_per_record,
                row.rows_out,
                static_cast<unsigned long long>(row.materializations));
    std::printf("columnar   wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                col.wall_ms,
                col.ns_per_record,
                col.rows_out,
                static_cast<unsigned long long>(col.materializations));
    std::printf("speedup (row/columnar) %.2fx\n", speedup);
    if (row.rows_out != col.rows_out) {
        std::printf("WARNING: rows_out mismatch - arms did different work\n");
    }
    if (col.materializations != 0) {
        std::printf("WARNING: columnar arm decoded rows (materializations != 0)\n");
    }
}

// String analogue of SizeCountingSink (counts via Batch::size(), no decode).
class StringSizeCountingSink final : public clink::Sink<std::string> {
public:
    using clink::Sink<std::string>::on_data;
    void on_data(const clink::Batch<std::string>& batch) override { count_ += batch.size(); }
    std::string name() const override { return "string_size_count"; }
    std::size_t count() const noexcept { return count_; }

private:
    std::size_t count_{0};
};

template <typename MakePipeline>
FilterArm run_string_filter_arm(std::int64_t records, MakePipeline make) {
    using namespace clink;
    detail::batch_materialize_counter().store(0, std::memory_order_relaxed);
    auto sink = std::make_shared<StringSizeCountingSink>();
    Dag dag = make(sink);
    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    FilterArm a;
    a.wall_ms = static_cast<double>(wall_ns) / 1'000'000.0;
    a.ns_per_record = records > 0 ? static_cast<double>(wall_ns) / static_cast<double>(records) : 0;
    a.rows_out = sink->count();
    a.materializations = detail::batch_materialize_counter().load(std::memory_order_relaxed);
    return a;
}

// columnar_string_filter: the same ColumnarStringVectorSource feeding (a) a ROW
// FilterOperator<string> (which materializes a std::string per record) vs (b)
// the ColumnarStringFilterOperator (which scans the Arrow StringArray via
// string_view, zero allocations). The string win is larger than the int64
// filter's because materialization is a heap alloc per record, not a copy.
void run_columnar_string_filter(std::int64_t records, std::int64_t batch_size, std::string prefix) {
    using namespace clink;
    // Long values (> the ~22-byte small-string-optimization buffer) so a row
    // materialization is a real heap alloc - that is the cost the columnar scan
    // avoids. Short SSO strings show ~no win (materialization is just an inline
    // copy); the columnar win for strings is entirely about dodging the alloc.
    std::vector<std::string> input;
    input.reserve(static_cast<std::size_t>(records));
    for (std::int64_t i = 0; i < records; ++i) {
        input.push_back("event_category_long_label_" + std::to_string(i % 100));  // ~29 chars
    }
    const auto bs = static_cast<std::size_t>(batch_size);

    const FilterArm row =
        run_string_filter_arm(records, [&](std::shared_ptr<StringSizeCountingSink> sink) {
            Dag dag;
            auto src = std::make_shared<ColumnarStringVectorSource>(input, bs);
            auto filt = std::make_shared<FilterOperator<std::string>>(
                [prefix](const std::string& s) { return s.rfind(prefix, 0) == 0; },
                "row_str_filter");
            auto h0 = dag.add_source<std::string>(src);
            auto h1 = dag.add_operator<std::string, std::string>(h0, filt);
            dag.add_sink<std::string>(h1, sink);
            return dag;
        });

    const FilterArm col =
        run_string_filter_arm(records, [&](std::shared_ptr<StringSizeCountingSink> sink) {
            Dag dag;
            auto src = std::make_shared<ColumnarStringVectorSource>(input, bs);
            auto filt = std::make_shared<ColumnarStringFilterOperator>(prefix);
            auto h0 = dag.add_source<std::string>(src);
            auto h1 = dag.add_operator<std::string, std::string>(h0, filt);
            dag.add_sink<std::string>(h1, sink);
            return dag;
        });

    const double speedup = col.wall_ms > 0 ? row.wall_ms / col.wall_ms : 0;
    std::printf("scenario               columnar_string_filter\n");
    std::printf("records                %lld  (batch_size=%lld, prefix=%s)\n",
                static_cast<long long>(records),
                static_cast<long long>(batch_size),
                prefix.c_str());
    std::printf("row arm    wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                row.wall_ms,
                row.ns_per_record,
                row.rows_out,
                static_cast<unsigned long long>(row.materializations));
    std::printf("columnar   wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                col.wall_ms,
                col.ns_per_record,
                col.rows_out,
                static_cast<unsigned long long>(col.materializations));
    std::printf("speedup (row/columnar) %.2fx\n", speedup);
    if (row.rows_out != col.rows_out) {
        std::printf("WARNING: rows_out mismatch - arms did different work\n");
    }
    if (col.materializations != 0) {
        std::printf("WARNING: columnar arm decoded rows (materializations != 0)\n");
    }
    if (row.materializations == 0) {
        std::printf("WARNING: row arm decoded 0 rows - it should have materialized every batch\n");
    }
}

// Counts pair rows via Batch::size() (no row decode), the keyed-agg analogue
// of SizeCountingSink.
class PairSizeCountingSink final : public clink::Sink<std::pair<std::int64_t, std::int64_t>> {
public:
    using clink::Sink<std::pair<std::int64_t, std::int64_t>>::on_data;
    void on_data(const clink::Batch<std::pair<std::int64_t, std::int64_t>>& batch) override {
        count_ += batch.size();
    }
    std::string name() const override { return "pair_size_count"; }
    std::size_t count() const noexcept { return count_; }

private:
    std::size_t count_{0};
};

template <typename MakePipeline>
FilterArm run_agg_arm(std::int64_t records, MakePipeline make) {
    using namespace clink;
    detail::batch_materialize_counter().store(0, std::memory_order_relaxed);
    auto sink = std::make_shared<PairSizeCountingSink>();
    Dag dag = make(sink);
    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    FilterArm a;
    a.wall_ms = static_cast<double>(wall_ns) / 1'000'000.0;
    a.ns_per_record = records > 0 ? static_cast<double>(wall_ns) / static_cast<double>(records) : 0;
    a.rows_out = sink->count();
    a.materializations = detail::batch_materialize_counter().load(std::memory_order_relaxed);
    return a;
}

// columnar_aggregation: the same ColumnarKeyedVectorSource feeding the SAME
// ColumnarKeyedAggregateOperator twice, once with the columnar fast path disabled (it
// falls back to process(), decoding every batch to rows) and once enabled (it
// folds the key+value Arrow buffers straight into the accumulator). Isolates
// ingest as the only variable. The materialization counts prove the columnar
// arm did zero row decode; rows_out (one row per distinct key on flush) proves
// both arms produced the same grouped result.
void run_columnar_aggregation(std::int64_t records, std::int64_t batch_size, std::int64_t keys) {
    using namespace clink;
    using KV = std::pair<std::int64_t, std::int64_t>;
    const auto nkeys = keys > 0 ? keys : 1;
    std::vector<KV> input;
    input.reserve(static_cast<std::size_t>(records));
    for (std::int64_t i = 0; i < records; ++i) {
        input.emplace_back(i % nkeys, i);
    }
    const auto bs = static_cast<std::size_t>(batch_size);

    const FilterArm row = run_agg_arm(records, [&](std::shared_ptr<PairSizeCountingSink> sink) {
        Dag dag;
        auto src = std::make_shared<ColumnarKeyedVectorSource>(input, bs);
        auto agg = std::make_shared<ColumnarKeyedAggregateOperator>(AggKind::Sum,
                                                                    "row_keyed_sum",
                                                                    /*enable_columnar=*/false);
        auto h0 = dag.add_source<KV>(src);
        auto h1 = dag.add_operator<KV, KV>(h0, agg);
        dag.add_sink<KV>(h1, sink);
        return dag;
    });

    const FilterArm col = run_agg_arm(records, [&](std::shared_ptr<PairSizeCountingSink> sink) {
        Dag dag;
        auto src = std::make_shared<ColumnarKeyedVectorSource>(input, bs);
        auto agg = std::make_shared<ColumnarKeyedAggregateOperator>(AggKind::Sum,
                                                                    "columnar_keyed_sum",
                                                                    /*enable_columnar=*/true);
        auto h0 = dag.add_source<KV>(src);
        auto h1 = dag.add_operator<KV, KV>(h0, agg);
        dag.add_sink<KV>(h1, sink);
        return dag;
    });

    const double speedup = col.wall_ms > 0 ? row.wall_ms / col.wall_ms : 0;
    std::printf("scenario               columnar_aggregation\n");
    std::printf("records                %lld  (batch_size=%lld, keys=%lld)\n",
                static_cast<long long>(records),
                static_cast<long long>(batch_size),
                static_cast<long long>(keys));
    std::printf("row arm    wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                row.wall_ms,
                row.ns_per_record,
                row.rows_out,
                static_cast<unsigned long long>(row.materializations));
    std::printf("columnar   wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                col.wall_ms,
                col.ns_per_record,
                col.rows_out,
                static_cast<unsigned long long>(col.materializations));
    std::printf("speedup (row/columnar) %.2fx\n", speedup);
    if (row.rows_out != col.rows_out) {
        std::printf("WARNING: rows_out mismatch - arms did different work\n");
    }
    if (row.rows_out == 0) {
        std::printf("WARNING: rows_out is 0 - the aggregation emitted nothing\n");
    }
    if (col.materializations != 0) {
        std::printf("WARNING: columnar arm decoded rows (materializations != 0)\n");
    }
    if (row.materializations == 0) {
        std::printf("WARNING: row arm decoded 0 rows - it should have materialized every batch\n");
    }
}

// columnar_window_aggregation: the SAME timed columnar source feeds the SAME
// TumblingWindowOperator twice - columnar-disabled (row decode every batch) vs
// enabled (folds key+value+event_time buffers straight into the per-(window,key)
// accumulators). Windows fire at end-of-stream flush. Isolates ingest; the
// columnar path calls the operator's exact same per-record logic, so the only
// difference is the Record<pair> decode the row arm pays.
void run_columnar_window_aggregation(std::int64_t records,
                                     std::int64_t batch_size,
                                     std::int64_t keys,
                                     std::int64_t windows) {
    using namespace clink;
    using KV = std::pair<std::int64_t, std::int64_t>;
    using WinOp = TumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>;
    const auto nkeys = keys > 0 ? keys : 1;
    const auto nwin = windows > 0 ? windows : 1;
    constexpr std::int64_t kWindowMs = 1000;
    std::vector<KV> input;
    std::vector<std::int64_t> ts;
    input.reserve(static_cast<std::size_t>(records));
    ts.reserve(static_cast<std::size_t>(records));
    for (std::int64_t i = 0; i < records; ++i) {
        input.emplace_back(i % nkeys, i);
        ts.push_back((i % nwin) * kWindowMs + (i % kWindowMs));  // within window i%nwin
    }
    const auto bs = static_cast<std::size_t>(batch_size);

    auto make_arm = [&](bool columnar) {
        return [&, columnar](std::shared_ptr<PairSizeCountingSink> sink) {
            Dag dag;
            auto src = std::make_shared<ColumnarKeyedVectorSource>(input, bs, ts);
            auto op = std::make_shared<WinOp>(
                std::chrono::milliseconds{kWindowMs},
                [] { return std::int64_t{0}; },
                [](std::int64_t a, std::int64_t v) { return a + v; });
            op->set_columnar_enabled(columnar);
            auto h0 = dag.add_source<KV>(src);
            auto h1 = dag.add_operator<KV, KV>(h0, op);
            dag.add_sink<KV>(h1, sink);
            return dag;
        };
    };

    const FilterArm row = run_agg_arm(records, make_arm(/*columnar=*/false));
    const FilterArm col = run_agg_arm(records, make_arm(/*columnar=*/true));

    const double speedup = col.wall_ms > 0 ? row.wall_ms / col.wall_ms : 0;
    std::printf("scenario               columnar_window_aggregation\n");
    std::printf("records                %lld  (batch_size=%lld, keys=%lld, windows=%lld)\n",
                static_cast<long long>(records),
                static_cast<long long>(batch_size),
                static_cast<long long>(keys),
                static_cast<long long>(windows));
    std::printf("row arm    wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                row.wall_ms,
                row.ns_per_record,
                row.rows_out,
                static_cast<unsigned long long>(row.materializations));
    std::printf("columnar   wall_ms=%.3f ns/rec=%.2f rows_out=%zu materializations=%llu\n",
                col.wall_ms,
                col.ns_per_record,
                col.rows_out,
                static_cast<unsigned long long>(col.materializations));
    std::printf("speedup (row/columnar) %.2fx\n", speedup);
    if (row.rows_out != col.rows_out) {
        std::printf("WARNING: rows_out mismatch - arms did different work\n");
    }
    if (row.rows_out == 0) {
        std::printf("WARNING: rows_out is 0 - the window aggregation emitted nothing\n");
    }
    if (col.materializations != 0) {
        std::printf("WARNING: columnar arm decoded rows (materializations != 0)\n");
    }
    if (row.materializations == 0) {
        std::printf("WARNING: row arm decoded 0 rows - it should have materialized every batch\n");
    }
}

// keyed_contention: T threads doing read-modify-write keyed state ops on ONE
// shared backend, spread across all 128 key groups. Throughput-only (no
// per-op clock, which would add ~40ns/op and mask the lock signal). The win:
// the sharded backend's throughput scales with T while the single-mutex mono
// backend flattens at T>=2 (lock-bound). This is the workload the par=1
// pipeline benches cannot see; the premise (many keys, RMW hot path,
// concurrent threads on one backend) is the only condition under which
// sharding can win and is stated here so the result is not over-read.
double run_contention_arm(clink::StateBackend& backend,
                          int threads,
                          std::int64_t ops_total,
                          const std::vector<std::string>& keys) {
    using namespace clink;
    const OperatorId op{1};
    const std::int64_t per = ops_total / threads;
    std::atomic<bool> go{false};

    auto worker = [&](int t) {
        while (!go.load(std::memory_order_acquire)) {
        }
        std::size_t k = static_cast<std::size_t>(t) * 7919u;  // per-thread phase offset
        for (std::int64_t i = 0; i < per; ++i) {
            const std::string& key = keys[(k + static_cast<std::size_t>(i)) % keys.size()];
            const StateBackend::KeyView kv{key};
            auto v = backend.get(op, kv);
            std::int64_t cur = 0;
            if (v && v->size() >= sizeof(cur)) {
                std::memcpy(&cur, v->data(), sizeof(cur));
            }
            ++cur;
            backend.put(
                op, kv, StateBackend::ValueView{reinterpret_cast<const char*>(&cur), sizeof(cur)});
        }
    };

    std::vector<std::thread> ts;
    ts.reserve(static_cast<std::size_t>(threads));
    for (int t = 0; t < threads; ++t) {
        ts.emplace_back(worker, t);
    }
    const auto start = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : ts) {
        th.join();
    }
    const auto end = std::chrono::steady_clock::now();
    const double secs = std::chrono::duration<double>(end - start).count();
    const auto total = static_cast<double>(per * threads);
    return secs > 0 ? total / secs / 1e6 : 0.0;  // Mops/sec
}

void run_keyed_contention(std::int64_t ops_total, std::int64_t num_keys) {
    using namespace clink;
    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(num_keys));
    for (std::int64_t i = 0; i < num_keys; ++i) {
        std::string u = "key-" + std::to_string(i);
        const auto kg = key_group_for_key(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(u.data()), u.size()});
        std::string k;
        k.push_back(static_cast<char>(kg & 0xFF));  // leading key-group byte (as the encoder sets)
        k.append(u);
        keys.push_back(std::move(k));
    }
    std::printf("scenario               keyed_contention (ops=%lld, keys=%lld)\n",
                static_cast<long long>(ops_total),
                static_cast<long long>(num_keys));
    std::printf("%-9s %-8s %12s\n", "backend", "threads", "Mops/sec");
    for (int t : {1, 2, 4, 8}) {
        {
            InMemoryStateBackend mono;
            std::printf(
                "%-9s %-8d %12.2f\n", "mono", t, run_contention_arm(mono, t, ops_total, keys));
        }
        {
            ShardedInMemoryStateBackend sharded;
            std::printf("%-9s %-8d %12.2f\n",
                        "sharded",
                        t,
                        run_contention_arm(sharded, t, ops_total, keys));
        }
    }
}

// sharded_stage: end-to-end throughput of ShardedKeyedStage (the share-nothing
// execution model) vs a single-threaded keyed operator baseline, SAME per-record
// work (a keyed running-count: get -> +1 -> put). Each stage worker owns a
// private InMemoryStateBackend and is the only thread that touches it (single-
// writer), so the per-shard mutex is uncontended and the key set stays core-hot.
//
// Premise (so the number is not over-read): this measures END-TO-END stage
// throughput - the single producer thread's demux (split each batch by key
// group) and the shared downstream sink are SERIAL costs shared by every arm,
// so the speedup is bounded by Amdahl on those, not just the parallel keyed
// work. The honest read is "what the whole stage delivers", not "perfect S-way
// scaling of the RMW". emit counts are printed to prove neither arm no-ops.
class BenchCountingOp final : public clink::Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", clink::int64_codec(), clink::int64_codec()));
    }
    void process(const clink::StreamElement<std::int64_t>& el,
                 clink::Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> state_;
};

void run_sharded_stage(std::int64_t ops_total, std::int64_t num_keys) {
    using namespace clink;
    constexpr std::int64_t kBatch = 4096;
    auto key_bytes = KeyBytesOf<std::int64_t>(
        [kc = int64_codec()](const std::int64_t& v) { return kc.encode(v); });

    std::printf("scenario               sharded_stage (ops=%lld, keys=%lld)\n",
                static_cast<long long>(ops_total),
                static_cast<long long>(num_keys));
    std::printf("%-14s %-8s %12s %14s\n", "arm", "shards", "Mrec/sec", "emits");

    // Baseline: one keyed operator on one thread over one private backend.
    {
        InMemoryStateBackend backend;
        auto ctx = std::make_unique<RuntimeContext>(OperatorId{9}, "baseline", &backend, nullptr);
        BenchCountingOp op;
        op.set_id(OperatorId{9});
        op.attach_runtime(ctx.get());
        std::int64_t emits = 0;
        Emitter<std::int64_t> out(
            typename Emitter<std::int64_t>::Forward([&emits](StreamElement<std::int64_t> e) {
                if (e.is_data()) {
                    emits += static_cast<std::int64_t>(e.as_data().size());
                }
                return true;
            }));
        op.open();
        const auto start = std::chrono::steady_clock::now();
        std::int64_t produced = 0;
        while (produced < ops_total) {
            Batch<std::int64_t> b;
            const std::int64_t n = std::min(kBatch, ops_total - produced);
            for (std::int64_t i = 0; i < n; ++i) {
                b.emplace((produced + i) % num_keys);
            }
            op.process(StreamElement<std::int64_t>::data(std::move(b)), out);
            produced += n;
        }
        const auto end = std::chrono::steady_clock::now();
        const double secs = std::chrono::duration<double>(end - start).count();
        const double mrec = secs > 0 ? static_cast<double>(ops_total) / secs / 1e6 : 0.0;
        std::printf(
            "%-14s %-8d %12.2f %14lld\n", "baseline", 1, mrec, static_cast<long long>(emits));
    }

    // Stage: S single-writer workers, records demuxed by key group.
    for (int s : {1, 2, 4, 8}) {
        std::atomic<std::int64_t> emits{0};
        ShardedKeyedStage<std::int64_t, std::int64_t> stage(
            static_cast<std::size_t>(s),
            OperatorId{9},
            [](std::size_t) { return std::make_unique<BenchCountingOp>(); },
            key_bytes,
            [&emits](StreamElement<std::int64_t> e) {
                if (e.is_data()) {
                    emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                    std::memory_order_relaxed);
                }
                return true;
            });
        stage.start();
        const auto start = std::chrono::steady_clock::now();
        std::int64_t produced = 0;
        while (produced < ops_total) {
            Batch<std::int64_t> b;
            const std::int64_t n = std::min(kBatch, ops_total - produced);
            for (std::int64_t i = 0; i < n; ++i) {
                b.emplace((produced + i) % num_keys);
            }
            stage.submit(std::move(b));
            produced += n;
        }
        stage.close_input();
        stage.await();
        const auto end = std::chrono::steady_clock::now();
        const double secs = std::chrono::duration<double>(end - start).count();
        const double mrec = secs > 0 ? static_cast<double>(ops_total) / secs / 1e6 : 0.0;
        std::printf(
            "%-14s %-8d %12.2f %14lld\n", "stage", s, mrec, static_cast<long long>(emits.load()));
    }
}

void emit_json(const BenchResult& r) {
    std::printf(
        "{\"scenario\":\"%s\",\"records\":%lld,\"wall_ms\":%.3f,"
        "\"throughput_per_sec\":%.0f,\"ns_per_record\":%.1f}\n",
        r.scenario.c_str(),
        static_cast<long long>(r.records),
        r.wall_ms,
        r.throughput_per_sec,
        r.ns_per_record);
}

void emit_human(const BenchResult& r) {
    std::printf("scenario              %s\n", r.scenario.c_str());
    std::printf("records               %lld\n", static_cast<long long>(r.records));
    std::printf("wall_ms               %.3f\n", r.wall_ms);
    std::printf("throughput_per_sec    %.0f\n", r.throughput_per_sec);
    std::printf("ns_per_record         %.1f\n", r.ns_per_record);
}

void usage() {
    std::cerr
        << "Usage: clink_bench "
           "[--scenario=pipe_throughput|columnar_filter|columnar_string_filter|"
           "columnar_aggregation|columnar_window_aggregation|keyed_contention|sharded_stage]\n"
        << "                     [--records=N] [--warmup=N] [--format=json|human]\n"
        << "       columnar_filter extra: [--batch-size=N] [--threshold=N]\n"
        << "       columnar_string_filter extra: [--batch-size=N] [--prefix=STR]\n"
        << "       columnar_aggregation extra: [--batch-size=N] [--keys=N]\n"
        << "       columnar_window_aggregation extra: [--batch-size=N] [--keys=N] "
           "[--windows=N]\n"
        << "       keyed_contention extra: [--keys=N]\n"
        << "       sharded_stage extra: [--keys=N]\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        usage();
        return 0;
    }
    const auto scenario = get_arg(argc, argv, "scenario", "pipe_throughput");
    const auto records_str = get_arg(argc, argv, "records", "1000000");
    const auto warmup_str = get_arg(argc, argv, "warmup", "0");
    const auto format = get_arg(argc, argv, "format", "json");

    const auto records = static_cast<std::int64_t>(std::stoll(records_str));
    const auto warmup = static_cast<std::int64_t>(std::stoll(warmup_str));

    // keyed_contention is a mono-vs-sharded scaling sweep with its own report.
    if (scenario == "keyed_contention") {
        const auto keys =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "keys", "200000")));
        run_keyed_contention(records, keys);
        return 0;
    }

    // sharded_stage is the share-nothing-vs-baseline sweep with its own report.
    if (scenario == "sharded_stage") {
        const auto keys =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "keys", "200000")));
        run_sharded_stage(records, keys);
        return 0;
    }

    // columnar_filter is a two-arm comparison with its own report.
    if (scenario == "columnar_filter") {
        const auto batch_size =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "batch-size", "4096")));
        // Default threshold = records/2 => ~50% selectivity over input 0..N.
        const auto threshold = static_cast<std::int64_t>(
            std::stoll(get_arg(argc, argv, "threshold", std::to_string(records / 2))));
        run_columnar_filter(records, batch_size, threshold);
        return 0;
    }

    // columnar_string_filter is a two-arm string-prefix-filter comparison.
    if (scenario == "columnar_string_filter") {
        const auto batch_size =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "batch-size", "4096")));
        const auto prefix = get_arg(argc, argv, "prefix", "event_category_long_label_7");
        run_columnar_string_filter(records, batch_size, prefix);
        return 0;
    }

    // columnar_aggregation is a two-arm grouped-sum comparison with its own report.
    if (scenario == "columnar_aggregation") {
        const auto batch_size =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "batch-size", "4096")));
        const auto keys =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "keys", "10000")));
        run_columnar_aggregation(records, batch_size, keys);
        return 0;
    }

    // columnar_window_aggregation is a two-arm windowed grouped-sum comparison.
    if (scenario == "columnar_window_aggregation") {
        const auto batch_size =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "batch-size", "4096")));
        const auto keys =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "keys", "1000")));
        const auto windows =
            static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "windows", "100")));
        run_columnar_window_aggregation(records, batch_size, keys, windows);
        return 0;
    }

    BenchResult r;
    if (scenario == "pipe_throughput") {
        r = run_pipe_throughput(records, warmup);
    } else {
        std::cerr << "clink_bench: unknown scenario '" << scenario << "'\n";
        usage();
        return 2;
    }

    if (format == "human") {
        emit_human(r);
    } else {
        emit_json(r);
    }
    return 0;
}
