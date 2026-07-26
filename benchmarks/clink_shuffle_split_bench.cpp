// Keyed-shuffle split cost, in isolation.
//
// WHY. On the two-node rig the `hash` partitioner was 19.3% of all worker CPU on nexmark
// q12 - the largest non-library compute item in the engine, ahead of the window
// aggregation it feeds (9.4%). A whole-pipeline number cannot attribute a change to it,
// so this drives gather_columnar_by_target directly over a pre-built Arrow batch.
//
// WHAT THE SPLIT HAS TO DO. Given a batch of N rows and a per-row target subtask, produce
// one sub-batch per target containing that target's rows, preserving order, without
// materialising rows.
//
// Reports ns per ROW (not per batch), because that is what scales with throughput, and
// CPU alongside wall for the same reason the other benches do: this runs inside an
// operator thread and its cost is charged to the engine's CPU budget.
//
//   clink_shuffle_split_bench [rows_per_batch] [batches] [targets]
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <sys/resource.h>

#include "clink/core/record.hpp"
#include "clink/runtime/columnar_split.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

using clink::Batch;
using clink::sql::Row;

namespace {

double cpu_seconds() {
    struct rusage ru{};
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0.0;
    }
    const auto to_s = [](const struct timeval& tv) {
        return static_cast<double>(tv.tv_sec) + static_cast<double>(tv.tv_usec) / 1e6;
    };
    return to_s(ru.ru_utime) + to_s(ru.ru_stime);
}

// A batch shaped like the one q12's shuffle actually carries: the mandatory event-time
// column at index 0, an int64 key, an int64 measure and a string, all nullable.
std::shared_ptr<arrow::RecordBatch> make_batch(std::int64_t rows) {
    arrow::Int64Builder ts, key, price;
    arrow::StringBuilder channel;
    std::mt19937_64 rng(11);
    for (std::int64_t i = 0; i < rows; ++i) {
        (void)ts.Append(1'700'000'000'000 + i);
        (void)key.Append(static_cast<std::int64_t>(rng() % 100000));
        (void)price.Append(static_cast<std::int64_t>(rng() % 10000));
        (void)channel.Append("channel-" + std::to_string(i % 8));
    }
    std::shared_ptr<arrow::Array> a_ts, a_key, a_price, a_ch;
    (void)ts.Finish(&a_ts);
    (void)key.Finish(&a_key);
    (void)price.Finish(&a_price);
    (void)channel.Finish(&a_ch);
    auto schema = arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                 arrow::field("bidder", arrow::int64(), true),
                                 arrow::field("price", arrow::int64(), true),
                                 arrow::field("channel", arrow::utf8(), true)});
    return arrow::RecordBatch::Make(schema, rows, {a_ts, a_key, a_price, a_ch});
}

}  // namespace

int main(int argc, char** argv) {
    const std::int64_t rows = argc > 1 ? std::strtoll(argv[1], nullptr, 10) : 1024;
    const std::int64_t batches = argc > 2 ? std::strtoll(argv[2], nullptr, 10) : 20000;
    const std::size_t n_out =
        argc > 3 ? static_cast<std::size_t>(std::strtoull(argv[3], nullptr, 10)) : 4;

    std::printf("shuffle split bench: rows/batch=%lld batches=%lld targets=%zu\n",
                static_cast<long long>(rows),
                static_cast<long long>(batches),
                n_out);

    auto rb = make_batch(rows);
    Batch<Row> batch{rb, static_cast<std::size_t>(rows), clink::sql::row_materialize_fn()};

    // A spread of targets, as a real key hash produces. Built once so the measurement is
    // the split and not the routing decision.
    std::vector<int> targets(static_cast<std::size_t>(rows));
    std::mt19937_64 rng(7);
    for (auto& t : targets) {
        t = static_cast<int>(rng() % n_out);
    }

    // Warm the allocator and Arrow's kernel lookup so the first batch is not charged for
    // both.
    for (int i = 0; i < 50; ++i) {
        (void)clink::gather_columnar_by_target<Row>(batch, targets, n_out);
    }

    double best_cpu_ns = 0.0;
    for (int trial = 0; trial < 3; ++trial) {
        std::uint64_t rows_out = 0;
        const double cpu0 = cpu_seconds();
        const auto t0 = std::chrono::steady_clock::now();
        for (std::int64_t b = 0; b < batches; ++b) {
            auto parts = clink::gather_columnar_by_target<Row>(batch, targets, n_out);
            if (!parts) {
                std::fprintf(stderr, "split returned nullopt - nothing measured\n");
                return 1;
            }
            for (const auto& p : *parts) {
                rows_out += p.size();
            }
        }
        const auto t1 = std::chrono::steady_clock::now();
        const double cpu1 = cpu_seconds();

        const double secs = std::chrono::duration<double>(t1 - t0).count();
        const auto total_rows = static_cast<double>(rows) * static_cast<double>(batches);
        // Every input row must appear in exactly one output sub-batch. A split that
        // dropped or duplicated rows would otherwise post a flattering number.
        if (rows_out != static_cast<std::uint64_t>(total_rows)) {
            std::fprintf(stderr,
                         "WRONG ROW COUNT out=%llu expected=%.0f - result void\n",
                         static_cast<unsigned long long>(rows_out),
                         total_rows);
            return 1;
        }
        const double cpu_ns = (cpu1 - cpu0) / total_rows * 1e9;
        best_cpu_ns = best_cpu_ns > 0 ? std::min(best_cpu_ns, cpu_ns) : cpu_ns;
        std::printf("  trial %d: %7.3fs wall  %7.3fs cpu  %6.1f ns/row cpu  %10.0f rows/s\n",
                    trial + 1,
                    secs,
                    cpu1 - cpu0,
                    cpu_ns,
                    total_rows / secs);
    }
    std::printf("\nbest: %.1f ns/row CPU\n", best_cpu_ns);
    return 0;
}
