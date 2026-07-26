// Host-side hot-path rig for the nexmark shapes, built to be PROFILED.
//
// The containerised harness measures the whole system - broker, network, deploy,
// four containers - which is what a cross-engine claim needs but is useless for
// finding a hotspot: you cannot attach a sampling profiler to a worker inside a
// Linux VM from a macOS host, and the signal is buried under Kafka and scheduling
// anyway. This runs the same OPERATOR CHAIN in one process over pre-materialised
// input, so `sample` (or perf) sees only engine code.
//
// Deliberately NOT a throughput claim. There is no broker, no network hop and no
// deploy here, so its absolute numbers are not comparable with the harness's.
// Its purpose is A/B under a profiler: run it, change one thing, run it again.
//
//   clink_hotpath_bench q0   [lines]   decode -> project -> sink
//   clink_hotpath_bench q12  [lines]   decode -> STRING key -> windowed COUNT fold
//   clink_hotpath_bench q12typed [n]   the same fold with a typed int64 key
//   clink_hotpath_bench decode [lines] the JSON bridge alone
//   clink_hotpath_bench buildonly [n]  Batch<std::string> assembly ALONE, to subtract
//
// A 4th argument repeats the measured loop (`decode 2000000 256 20`), which is how to
// give `sample` a window that outlasts input generation.
//
// Records/sec is printed per stage so a change can be attributed.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/sql/json_string_to_row_columnar.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

using clink::Batch;
using clink::BoundedChannel;
using clink::Emitter;
using clink::Record;
using clink::StreamElement;
using clink::sql::Row;
using clink::sql::RowColumn;

namespace {

// The nexmark bid shape the harness generates.
std::vector<RowColumn> bid_schema() {
    return {
        {"auction", arrow::int64()},
        {"bidder", arrow::int64()},
        {"price", arrow::int64()},
        {"channel", arrow::utf8()},
        {"url", arrow::utf8()},
        {"datetime", arrow::int64()},
    };
}

std::vector<std::string> generate(std::size_t n) {
    std::vector<std::string> out;
    out.reserve(n);
    std::mt19937_64 rng(7);
    for (std::size_t i = 0; i < n; ++i) {
        char buf[256];
        std::snprintf(buf,
                      sizeof(buf),
                      R"({"auction":%llu,"bidder":%llu,"price":%llu,"channel":"ch%d",)"
                      R"("url":"https://x.test/%llu","datetime":%llu})",
                      static_cast<unsigned long long>(rng() % 500000),
                      static_cast<unsigned long long>(rng() % 100000),
                      static_cast<unsigned long long>(rng() % 100000),
                      static_cast<int>(i % 8),
                      static_cast<unsigned long long>(i % 1000),
                      static_cast<unsigned long long>(1000 + (i / 400) * 10));
        out.emplace_back(buf);
    }
    return out;
}

struct Timer {
    std::chrono::steady_clock::time_point t0{std::chrono::steady_clock::now()};
    double elapsed() const {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    }
};

void report(const char* stage, std::size_t n, double secs, std::size_t sink) {
    const double rate = secs > 0 ? static_cast<double>(n) / secs : 0.0;
    std::printf("  %-22s %7.3fs  %10.0f rec/s   (sink saw %zu)\n", stage, secs, rate, sink);
}

// Drive the columnar JSON bridge over the input in batches, handing each emitted
// element to `consume`. Mirrors how the runner feeds an operator.
template <typename Consume>
void run_decode(const std::vector<std::string>& lines, std::size_t batch, Consume&& consume) {
    clink::sql::JsonStringToRowColumnarOperator op{bid_schema()};
    for (std::size_t i = 0; i < lines.size(); i += batch) {
        Batch<std::string> in;
        in.reserve(batch);
        for (std::size_t j = i; j < std::min(i + batch, lines.size()); ++j) {
            in.emplace(std::string(lines[j]));
        }
        BoundedChannel<StreamElement<Row>> ch(8);
        Emitter<Row> em(&ch);
        op.process(StreamElement<std::string>::data(std::move(in)), em);
        while (auto e = ch.try_pop()) {
            if (e->is_data()) {
                consume(e->as_data());
            }
        }
    }
}

int bench_decode(const std::vector<std::string>& lines, std::size_t batch, std::size_t repeats) {
    std::size_t rows = 0, columnar = 0;
    Timer t;
    for (std::size_t r = 0; r < repeats; ++r) {
        run_decode(lines, batch, [&](const Batch<Row>& b) {
            rows += b.size();  // size() answers from the sidecar; no materialisation
            if (b.is_columnar())
                ++columnar;
        });
    }
    report("decode only", lines.size() * repeats, t.elapsed(), rows);
    std::printf("  (columnar batches: %zu)\n", columnar);
    return 0;
}

// Batch construction WITHOUT the decode: the same Batch<std::string> assembly
// run_decode does, and nothing else. Its cost belongs to the runner rather than to
// the bridge, so subtracting it is what turns "decode only" into the decode's own
// per-record cost. It is not small - each record copies a line into a fresh
// std::string, which for a ~120 byte nexmark bid line is a malloc and a memcpy.
int bench_build_only(const std::vector<std::string>& lines,
                     std::size_t batch,
                     std::size_t repeats) {
    std::size_t seen = 0;
    Timer t;
    for (std::size_t r = 0; r < repeats; ++r) {
        for (std::size_t i = 0; i < lines.size(); i += batch) {
            Batch<std::string> in;
            in.reserve(batch);
            for (std::size_t j = i; j < std::min(i + batch, lines.size()); ++j) {
                in.emplace(std::string(lines[j]));
            }
            seen += in.size();
        }
    }
    report("batch build only", lines.size() * repeats, t.elapsed(), seen);
    return 0;
}

// q0: decode -> project -> sink. The sink is the harness's blackhole: a per-record
// callback with an empty body. Touching a row accessor materialises the sidecar,
// which is exactly the cost under scrutiny.
int bench_q0(const std::vector<std::string>& lines, std::size_t batch) {
    std::size_t seen = 0;
    Timer t;
    run_decode(lines, batch, [&](const Batch<Row>& b) {
        for (const auto& rec : b) {
            // What FunctionSink<Row> does: iterate rows, call a lambda per row.
            const Row& r = rec.value();
            (void)r;
            ++seen;
        }
    });
    report("q0 decode+sink", lines.size(), t.elapsed(), seen);
    return 0;
}

// q12: decode -> group by bidder into a tumbling window, COUNT(*). Models the
// state shape the SQL window operator uses today - a STRING key per group and an
// ordered map per group - so a typed-key variant can be A/B'd against it.
int bench_q12(const std::vector<std::string>& lines, std::size_t batch) {
    std::unordered_map<std::string, std::map<std::int64_t, std::int64_t>> state;
    std::size_t folded = 0;
    Timer t;
    run_decode(lines, batch, [&](const Batch<Row>& b) {
        for (const auto& rec : b) {
            const Row& r = rec.value();
            auto bit = r.values.find("bidder");
            auto tit = r.values.find("datetime");
            if (bit == r.values.end() || tit == r.values.end())
                continue;
            std::string key = bit->second.serialize(0);  // the string group key
            const auto ts = static_cast<std::int64_t>(tit->second.as_number());
            const std::int64_t win_end = ((ts / 10000) + 1) * 10000;
            ++state[key][win_end];
            ++folded;
        }
    });
    report("q12 decode+fold", lines.size(), t.elapsed(), folded);
    std::printf("  (distinct group keys: %zu)\n", state.size());
    return 0;
}

// The same fold with a TYPED int64 group key instead of the serialised string
// one the window operator uses today. Isolates what the key representation costs:
// the profile of bench_q12 ranks serialize_append + string::append + itoa at 587
// self-weight, plus a string-keyed hash insert at 328, to group by an integer
// that was already an integer in the input.
int bench_q12_typed(const std::vector<std::string>& lines, std::size_t batch) {
    std::unordered_map<std::int64_t, std::map<std::int64_t, std::int64_t>> state;
    std::size_t folded = 0;
    Timer t;
    run_decode(lines, batch, [&](const Batch<Row>& b) {
        for (const auto& rec : b) {
            const Row& r = rec.value();
            auto bit = r.values.find("bidder");
            auto tit = r.values.find("datetime");
            if (bit == r.values.end() || tit == r.values.end())
                continue;
            const auto key = static_cast<std::int64_t>(bit->second.as_number());
            const auto ts = static_cast<std::int64_t>(tit->second.as_number());
            const std::int64_t win_end = ((ts / 10000) + 1) * 10000;
            ++state[key][win_end];
            ++folded;
        }
    });
    report("q12 typed-key fold", lines.size(), t.elapsed(), folded);
    std::printf("  (distinct group keys: %zu)\n", state.size());
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string shape = argc > 1 ? argv[1] : "q0";
    const std::size_t n = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 2000000;
    const std::size_t batch = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 256;
    // Repeats keep the measured loop running long enough for a sampling profiler to
    // see it. The first attempt at profiling the decode sampled the input GENERATOR
    // instead, because generating 20M lines outlasted the sample window and snprintf
    // dominated the result.
    const std::size_t repeats = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 1;

    std::printf("clink_hotpath_bench: shape=%s lines=%zu batch=%zu\n", shape.c_str(), n, batch);
    const auto lines = generate(n);

    if (shape == "decode")
        return bench_decode(lines, batch, repeats);
    if (shape == "buildonly")
        return bench_build_only(lines, batch, repeats);
    if (shape == "q0")
        return bench_q0(lines, batch);
    if (shape == "q12")
        return bench_q12(lines, batch);
    if (shape == "q12typed")
        return bench_q12_typed(lines, batch);
    std::fprintf(stderr, "unknown shape '%s' (decode|buildonly|q0|q12|q12typed)\n", shape.c_str());
    return 2;
}
