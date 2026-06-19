// Process-function clink-vs-Flink bench: the clink side.
//
// Pipeline:
//   in-proc source -> assign_timestamps_monotonic -> key_by ->
//   process(StatefulFn) -> counting sink.
//
// StatefulFn is a KeyedProcessFunction that maintains two state slots
// per key:
//   - ValueState<int64> running_count
//   - ListState<int64> last 8 values (as KeyedState<int64, vector<int64>>)
//
// Each record reads + updates both slots, then emits a ProcessStats
// record. Per-record cost is dominated by the 4 keyed-state ops + the
// emit. Mirrors the canonical KeyedProcessFunction workhorse pattern
// for non-windowed business logic.
//
// Expected emit count: BENCH_RECORDS (one emit per input record).

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/rocksdb/install.hpp"
#include "clink/time/event_time.hpp"

namespace bench {

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Event / ProcessStats records
// ---------------------------------------------------------------------------

// Heavyweight payload split out so we can alias-share it across records
// rather than copying 1.5KB per record. Same shape as the inproc_compare
// bench so the workload is comparable.
struct EventBody {
    std::string payload;
};
using BodyPtr = std::shared_ptr<EventBody>;

struct Event {
    std::int64_t ts_ms{0};
    std::int64_t key{0};
    std::int64_t value{0};
    BodyPtr body;
};

struct ProcessStats {
    std::int64_t key{0};
    std::int64_t count{0};
    double last8_avg{0.0};
    // Aliased pointer back into the source event's payload via the
    // shared control block - one ref-count increment, no string copy.
    std::shared_ptr<const std::string> latest_payload;
};

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

inline void put_i64_le(std::byte*& p, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
    }
}
inline std::int64_t read_i64_le(std::span<const std::byte> b, std::size_t& pos) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(b[pos + i])) << (i * 8);
    }
    pos += 8;
    return static_cast<std::int64_t>(u);
}
inline void put_double_le(std::byte*& p, double v) {
    std::uint64_t u;
    std::memcpy(&u, &v, 8);
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
    }
}
inline double read_double_le(std::span<const std::byte> b, std::size_t& pos) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(b[pos + i])) << (i * 8);
    }
    pos += 8;
    double v;
    std::memcpy(&v, &u, 8);
    return v;
}
inline void put_bytes(std::byte*& p, std::string_view s) {
    const auto n = static_cast<std::uint32_t>(s.size());
    for (int i = 0; i < 4; ++i) {
        *p++ = static_cast<std::byte>((n >> (i * 8)) & 0xFF);
    }
    if (!s.empty()) {
        std::memcpy(p, s.data(), s.size());
        p += s.size();
    }
}
inline std::string read_string(std::span<const std::byte> b, std::size_t& pos) {
    std::uint32_t n = 0;
    for (int i = 0; i < 4; ++i) {
        n |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[pos + i])) << (i * 8);
    }
    pos += 4;
    std::string out(n, '\0');
    if (n > 0) {
        std::memcpy(out.data(), &b[pos], n);
        pos += n;
    }
    return out;
}

inline clink::Codec<Event> event_codec() {
    return clink::Codec<Event>{
        .encode =
            [](const Event& e) {
                const std::string& payload = e.body ? e.body->payload : std::string{};
                std::vector<std::byte> out(8 + 8 + 8 + 4 + payload.size());
                std::byte* p = out.data();
                put_i64_le(p, e.ts_ms);
                put_i64_le(p, e.key);
                put_i64_le(p, e.value);
                put_bytes(p, payload);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<Event> {
            std::size_t pos = 0;
            Event e;
            e.ts_ms = read_i64_le(b, pos);
            e.key = read_i64_le(b, pos);
            e.value = read_i64_le(b, pos);
            auto body = std::make_shared<EventBody>();
            body->payload = read_string(b, pos);
            e.body = std::move(body);
            return e;
        }};
}

inline clink::Codec<ProcessStats> process_stats_codec() {
    return clink::Codec<ProcessStats>{
        .encode =
            [](const ProcessStats& s) {
                const std::string& payload = s.latest_payload ? *s.latest_payload : std::string{};
                std::vector<std::byte> out(8 + 8 + 8 + 4 + payload.size());
                std::byte* p = out.data();
                put_i64_le(p, s.key);
                put_i64_le(p, s.count);
                put_double_le(p, s.last8_avg);
                put_bytes(p, payload);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<ProcessStats> {
            std::size_t pos = 0;
            ProcessStats s;
            s.key = read_i64_le(b, pos);
            s.count = read_i64_le(b, pos);
            s.last8_avg = read_double_le(b, pos);
            s.latest_payload = std::make_shared<std::string>(read_string(b, pos));
            return s;
        }};
}

// ---------------------------------------------------------------------------
// Synthetic source
// ---------------------------------------------------------------------------

constexpr std::size_t kBatchSize = 256;

class SyntheticEventSource final : public clink::Source<Event> {
public:
    SyntheticEventSource(std::int64_t total,
                         std::int64_t keys,
                         std::int64_t windows,
                         std::size_t payload_size)
        : total_(total), keys_(keys), windows_(windows) {
        body_template_ = std::make_shared<EventBody>();
        body_template_->payload = std::string(payload_size, 'x');
    }

    bool produce(clink::Emitter<Event>& out) override {
        if (this->cancelled() || cursor_ >= total_) {
            if (!eos_sent_) {
                out.emit_watermark(clink::Watermark::max());
                eos_sent_ = true;
            }
            return false;
        }
        const std::int64_t end = std::min<std::int64_t>(
            cursor_ + static_cast<std::int64_t>(kBatchSize), total_);
        clink::Batch<Event> batch;
        batch.reserve(static_cast<std::size_t>(end - cursor_));
        const double step_ms = static_cast<double>(windows_ * 1000) / static_cast<double>(total_);
        for (std::int64_t i = cursor_; i < end; ++i) {
            Event e;
            e.ts_ms = static_cast<std::int64_t>(static_cast<double>(i) * step_ms);
            e.key = i % keys_;
            e.value = (i % 7) + 1;
            e.body = body_template_;
            clink::Record<Event> r{std::move(e), clink::EventTime{e.ts_ms}};
            batch.push(std::move(r));
        }
        cursor_ = end;
        out.emit_data(std::move(batch));
        return true;
    }

    std::string name() const override { return "synthetic_event_source"; }

private:
    std::int64_t total_;
    std::int64_t keys_;
    std::int64_t windows_;
    BodyPtr body_template_;
    std::int64_t cursor_{0};
    bool eos_sent_{false};
};

// ---------------------------------------------------------------------------
// KeyedProcessFunction with ValueState + ListState
// ---------------------------------------------------------------------------

class StatefulFn final
    : public clink::KeyedProcessFunction<std::int64_t, Event, ProcessStats> {
public:
    void open(clink::RuntimeContext& ctx) override {
        count_state_ = std::make_unique<clink::KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>(
                "count", clink::int64_codec(), clink::int64_codec()));
        last8_state_ =
            std::make_unique<clink::KeyedState<std::int64_t, std::vector<std::int64_t>>>(
                ctx.keyed_state<std::int64_t, std::vector<std::int64_t>>(
                    "last8",
                    clink::int64_codec(),
                    clink::vector_codec<std::int64_t>(clink::int64_codec())));
    }

    void process_element(const Event& e,
                         clink::ProcessFunctionContext<ProcessStats>& /*ctx*/,
                         clink::Collector<ProcessStats>& out) override {
        // ValueState read+update.
        const std::int64_t k = current_key();
        const std::int64_t next_count = count_state_->get(k).value_or(0) + 1;
        count_state_->put(k, next_count);

        // ListState append+truncate to last 8.
        auto last8 = last8_state_->get(k).value_or(std::vector<std::int64_t>{});
        last8.push_back(e.value);
        if (last8.size() > 8) {
            last8.erase(last8.begin(), last8.begin() + (last8.size() - 8));
        }
        last8_state_->put(k, last8);

        // Compute the running last-8 average and emit.
        double sum = 0.0;
        for (const auto v : last8) {
            sum += static_cast<double>(v);
        }
        ProcessStats stats;
        stats.key = k;
        stats.count = next_count;
        stats.last8_avg = last8.empty() ? 0.0 : sum / static_cast<double>(last8.size());
        if (e.body) {
            // Aliasing shared_ptr piggybacks on the body's control block
            // - no string copy on the emit path.
            stats.latest_payload =
                std::shared_ptr<const std::string>(e.body, &e.body->payload);
        }
        out.collect(stats);
    }

    std::string name() const override { return "stateful_process"; }

private:
    std::unique_ptr<clink::KeyedState<std::int64_t, std::int64_t>> count_state_;
    std::unique_ptr<clink::KeyedState<std::int64_t, std::vector<std::int64_t>>> last8_state_;
};

// ---------------------------------------------------------------------------
// Counting sink
// ---------------------------------------------------------------------------

class CountingSink final : public clink::Sink<ProcessStats> {
public:
    explicit CountingSink(std::string label) : label_(std::move(label)) {}

    void on_data(const clink::Batch<ProcessStats>& b) override {
        count_ += static_cast<std::int64_t>(b.size());
    }

    void close() override {
        std::fprintf(
            stderr, "[%s] sink final count: %lld\n", label_.c_str(), static_cast<long long>(count_));
    }

    std::string name() const override { return "counting_sink"; }

private:
    std::string label_;
    std::int64_t count_{0};
};

// ---------------------------------------------------------------------------
// Job entry point
// ---------------------------------------------------------------------------

void define_job(clink::api::StreamExecutionEnvironment& env) {
    clink::rocksdb::install();
    const auto envv_int64 = [](const char* k, std::int64_t def) -> std::int64_t {
        const char* v = std::getenv(k);
        return v ? std::atoll(v) : def;
    };
    const auto envv_size = [](const char* k, std::size_t def) -> std::size_t {
        const char* v = std::getenv(k);
        return v ? static_cast<std::size_t>(std::atoll(v)) : def;
    };

    const std::int64_t total = envv_int64("BENCH_RECORDS", 10'000'000);
    const std::int64_t keys = envv_int64("BENCH_KEYS", 1000);
    const std::int64_t windows = envv_int64("BENCH_WINDOWS", 100);
    const std::size_t payload_size = envv_size("BENCH_PAYLOAD_BYTES", 1500);

    env.registry().register_type<Event>("bench.process.event", event_codec());
    env.registry().register_type<ProcessStats>("bench.process.stats", process_stats_codec());

    const std::string src_op = "bench.process.source";
    env.registry().register_source<Event>(
        src_op, [total, keys, windows, payload_size](const clink::plugin::BuildContext&) {
            return std::make_shared<SyntheticEventSource>(total, keys, windows, payload_size);
        });

    const std::string sink_op = "bench.process.sink";
    env.registry().register_sink<ProcessStats>(
        sink_op, [](const clink::plugin::BuildContext& bctx) {
            return std::make_shared<CountingSink>("clink-subtask-" +
                                                  std::to_string(bctx.subtask_idx));
        });

    clink::api::SourceDescriptor src_desc;
    src_desc.op_type = src_op;
    src_desc.channel_type = "bench.process.event";
    src_desc.parallelism = 1;

    clink::api::SinkDescriptor sink_desc;
    sink_desc.op_type = sink_op;
    sink_desc.channel_type = "bench.process.stats";
    sink_desc.parallelism = 1;

    auto fn = std::make_shared<StatefulFn>();

    env.source<Event>(src_desc)
        .assign_timestamps_monotonic([](const Event& e) { return clink::EventTime{e.ts_ms}; })
        .key_by([](const Event& e) { return e.key; })
        .process<ProcessStats>(fn)
        .sink(sink_desc);
}

}  // namespace bench

CLINK_REGISTER_JOB("process-function-bench-pipeline",
                   "1.0",
                   "in-proc source -> key_by -> KeyedProcessFunction(ValueState+ListState) -> sink",
                   bench::define_job);
