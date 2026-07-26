// Operator fusion A/B: the SAME three-stage chain run as separate Dag operators
// (one thread and one BoundedChannel per boundary) versus fused into a single
// ChainedOperator (one thread, no channel between stages).
//
// WHY THIS EXISTS. The split cloud rig
// (benchmarks/nexmark_compare/cloud/README.md) found that on nexmark q0 a
// blackhole sink which does nothing but increment a counter still cost 0.37us per
// record - 16% of all worker CPU, seven times the projection the query actually
// asked for. That is not work, it is the price of an operator boundary, and q0
// crosses three of them.
//
// The engine has the fix already: ChainedOperator<A,B,C> calls the second
// operator's process() directly from the first operator's emit, with no channel.
// But the cluster's chain dispatch stopped using it. The worker's chain path was
// generalised to DagBuilders so user-registered channel types could be dispatched,
// and each chain op became its own Dag runner - so a "chained" pair now shares a
// SLOT while still getting a thread and a channel each. Task counts fell; nothing
// was fused. The rig shows the consequence directly: a 4-op q0 plan the coordinator
// counts as 12 tasks runs as 16 operator threads plus 24 bridge threads.
//
// So this measures the prize before the plumbing gets rebuilt around it.
//
// TWO COSTS ARE IN PLAY and the bench separates neither deliberately, because both
// are real and both are removed by the same change:
//   1. The channel handoff itself - lock, condvar, wakeup, per batch.
//   2. CROSS-THREAD allocate/free. Stage 1 allocates the rows, stage 2 frees them.
//      An allocator serves that far worse than the alloc/free pair happening on one
//      thread, and it is charged to whichever thread does the freeing - which is why
//      a counting sink looked expensive.
//
//   clink_fusion_bench [records] [batch]
//
// Reports records/sec and CPU per record for both shapes. CPU is the headline for
// the same reason it was in the Kafka source bench: wall-clock on the split shape
// benefits from three threads running concurrently, so it can look competitive
// while burning far more machine to get there.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sys/resource.h>

#include "clink/config/json.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/row.hpp"

using clink::Batch;
using clink::ChainedOperator;
using clink::Dag;
using clink::Emitter;
using clink::LocalExecutor;
using clink::Operator;
using clink::Record;
using clink::Sink;
using clink::Source;
using clink::StreamElement;
using clink::config::JsonValue;
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

// A Row carrying a STRING field, on purpose. A pure-int64 row would live entirely
// in the Row's inline storage and there would be no cross-thread free to measure -
// which is the very cost that makes a counting sink expensive. Nexmark's bid rows
// carry channel and url strings, so this matches the shape being modelled.
Row make_row(std::int64_t i) {
    Row r;
    r.values.emplace("auction", JsonValue{i});
    r.values.emplace("price", JsonValue{i * 3});
    r.values.emplace("channel", JsonValue{std::string("channel-") + std::to_string(i % 1000)});
    r.values.emplace(
        "url", JsonValue{std::string("https://example.com/item/") + std::to_string(i % 10000)});
    return r;
}

// Stage 0: hands out pre-shaped batches. Generation cost is identical on both
// shapes, so it cancels out of the comparison.
class RowSource final : public Source<Row> {
public:
    RowSource(std::uint64_t total, std::size_t batch) : total_(total), batch_(batch) {}

    bool produce(Emitter<Row>& out) override {
        if (emitted_ >= total_) {
            return false;
        }
        Batch<Row> b;
        b.reserve(batch_);
        const std::uint64_t n = std::min<std::uint64_t>(batch_, total_ - emitted_);
        for (std::uint64_t k = 0; k < n; ++k) {
            b.emplace(make_row(static_cast<std::int64_t>(emitted_ + k)));
        }
        emitted_ += n;
        out.emit_data(std::move(b));
        return emitted_ < total_;
    }

    std::string name() const override { return "row_source"; }

private:
    std::uint64_t total_;
    std::size_t batch_;
    std::uint64_t emitted_{0};
};

// Stage 1: projection. Builds a NEW Row per record, as project_row does, so the
// allocation the next stage has to free is real.
class ProjectOp final : public Operator<Row, Row> {
public:
    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data()) {
            return;
        }
        const auto& in = element.as_data();
        Batch<Row> b;
        b.reserve(in.size());
        for (const auto& rec : in) {
            const auto& src = rec.value();
            Row r;
            r.values.emplace("auction", src.values.at("auction"));
            r.values.emplace("price", src.values.at("price"));
            r.values.emplace("url", src.values.at("url"));
            b.emplace(std::move(r));
        }
        out.emit_data(std::move(b));
    }

    std::string name() const override { return "project_row"; }
};

// Stage 2: a second Row->Row stage, so the split shape has two boundaries after the
// source rather than one. Cheap on purpose - it filters nothing and rewrites
// nothing - because the question is what the BOUNDARY costs, not the operator.
class PassOp final : public Operator<Row, Row> {
public:
    void process(const StreamElement<Row>& element, Emitter<Row>& out) override {
        if (!element.is_data()) {
            return;
        }
        const auto& in = element.as_data();
        Batch<Row> b;
        b.reserve(in.size());
        for (const auto& rec : in) {
            b.emplace(rec.value());
        }
        out.emit_data(std::move(b));
    }

    std::string name() const override { return "pass_row"; }
};

// Terminal: counts and drops, which is exactly the blackhole sink whose 0.37us per
// record started this. Whatever it costs above an increment is the boundary.
class CountingSink final : public Sink<Row> {
public:
    explicit CountingSink(std::atomic<std::uint64_t>* seen) : seen_(seen) {}

    void on_data(const Batch<Row>& batch) override {
        seen_->fetch_add(batch.size(), std::memory_order_relaxed);
    }

    std::string name() const override { return "counting_sink"; }

private:
    std::atomic<std::uint64_t>* seen_;
};

struct Result {
    double secs{0};
    double cpu{0};
    std::uint64_t records{0};
};

// Shape A: source -> project -> pass -> sink as four Dag stages. Three boundaries,
// each a thread handoff over a BoundedChannel, which is what the cluster's chain
// dispatch produces today even for ops the planner reports as chained.
Result run_split(std::uint64_t total, std::size_t batch) {
    std::atomic<std::uint64_t> seen{0};
    Dag dag;
    auto src =
        dag.add_source(std::shared_ptr<Source<Row>>(std::make_shared<RowSource>(total, batch)));
    auto p1 =
        dag.add_operator(src, std::shared_ptr<Operator<Row, Row>>(std::make_shared<ProjectOp>()));
    auto p2 = dag.add_operator(p1, std::shared_ptr<Operator<Row, Row>>(std::make_shared<PassOp>()));
    dag.add_sink(p2, std::shared_ptr<Sink<Row>>(std::make_shared<CountingSink>(&seen)));

    const double cpu0 = cpu_seconds();
    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();
    const double cpu1 = cpu_seconds();
    return {std::chrono::duration<double>(t1 - t0).count(),
            cpu1 - cpu0,
            seen.load(std::memory_order_relaxed)};
}

// Shape B: the two operators FUSED into one ChainedOperator, so the middle boundary
// is a direct call and both allocate and free happen on one thread.
Result run_fused(std::uint64_t total, std::size_t batch) {
    std::atomic<std::uint64_t> seen{0};
    Dag dag;
    auto src =
        dag.add_source(std::shared_ptr<Source<Row>>(std::make_shared<RowSource>(total, batch)));
    std::shared_ptr<Operator<Row, Row>> fused = std::make_shared<ChainedOperator<Row, Row, Row>>(
        std::shared_ptr<Operator<Row, Row>>(std::make_shared<ProjectOp>()),
        std::shared_ptr<Operator<Row, Row>>(std::make_shared<PassOp>()),
        "project+pass");
    auto p = dag.add_operator(src, fused);
    dag.add_sink(p, std::shared_ptr<Sink<Row>>(std::make_shared<CountingSink>(&seen)));

    const double cpu0 = cpu_seconds();
    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();
    const double cpu1 = cpu_seconds();
    return {std::chrono::duration<double>(t1 - t0).count(),
            cpu1 - cpu0,
            seen.load(std::memory_order_relaxed)};
}

void report(const char* label, const Result& r, std::uint64_t expected) {
    if (r.records != expected) {
        std::printf("  %-22s WRONG RECORD COUNT %llu (expected %llu) - result void\n",
                    label,
                    static_cast<unsigned long long>(r.records),
                    static_cast<unsigned long long>(expected));
        return;
    }
    const double rate = r.secs > 0 ? static_cast<double>(r.records) / r.secs : 0.0;
    const double cpu_ns = r.records > 0 ? r.cpu / static_cast<double>(r.records) * 1e9 : 0.0;
    std::printf("  %-22s %8.3fs  %10.0f rec/s  %6.0f ns/rec CPU  %5.2f cores\n",
                label,
                r.secs,
                rate,
                cpu_ns,
                r.secs > 0 ? r.cpu / r.secs : 0.0);
}

}  // namespace

int main(int argc, char** argv) {
    const std::uint64_t total = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 4'000'000;
    const std::size_t batch =
        argc > 2 ? static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10)) : 1024;

    std::printf(
        "fusion bench: records=%llu batch=%zu\n\n", static_cast<unsigned long long>(total), batch);

    // Warm the allocator and the page cache so the first shape measured is not
    // charged for both.
    (void)run_split(total / 10, batch);
    (void)run_fused(total / 10, batch);

    // Interleaved trials, not all of one then all of the other: a thermal or
    // frequency drift over the run would otherwise land entirely on whichever
    // shape ran second and read as a difference between the shapes.
    Result best_split{1e9, 1e9, 0};
    Result best_fused{1e9, 1e9, 0};
    for (int trial = 0; trial < 3; ++trial) {
        const auto s = run_split(total, batch);
        const auto f = run_fused(total, batch);
        if (s.records == total && s.cpu < best_split.cpu) {
            best_split = s;
        }
        if (f.records == total && f.cpu < best_fused.cpu) {
            best_fused = f;
        }
    }

    std::printf("best of 3 (lowest CPU):\n");
    report("split (3 boundaries)", best_split, total);
    report("fused (2 boundaries)", best_fused, total);

    if (best_split.records == total && best_fused.records == total && best_fused.cpu > 0) {
        const double cpu_gain = best_split.cpu / best_fused.cpu;
        const double wall_gain = best_split.secs > 0 ? best_split.secs / best_fused.secs : 0.0;
        std::printf(
            "\nfusing ONE of three boundaries: %.2fx less CPU, %.2fx wall\n"
            "per-boundary CPU saved: %.0f ns/record\n",
            cpu_gain,
            wall_gain,
            (best_split.cpu - best_fused.cpu) / static_cast<double>(total) * 1e9);
    }
    return 0;
}
