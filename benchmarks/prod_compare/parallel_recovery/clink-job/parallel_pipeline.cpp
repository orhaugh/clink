// Parallel-recovery clink-vs-Flink bench: the clink side.
//
// Same workload as inproc_compare/ (synthetic source -> keyed tumbling
// window -> aggregate -> counting sink), but driven at parallelism N
// (default 4 via BENCH_PARALLELISM). Exercises hash partitioning
// across subtasks, watermark alignment, and checkpoint-barrier
// propagation through a fanned-out aggregate. v1 has no induced
// crash + restore; v2 will kill the TM mid-run and verify
// exactly-once after recovery.
//
// State backend is selected by the submit CLI (use --state-backend=
// rocksdb:<dir> to exercise RocksDB writes per the bench spec).

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/rocksdb/install.hpp"
#include "clink/time/event_time.hpp"

namespace bench {

// ---------------------------------------------------------------------------
// Event: the rich record that flows through the pipeline.
// ~2 KB serialised (1 KB payload + tags + attributes).
// ---------------------------------------------------------------------------

// PayloadPtr: shared_ptr<const string> models Java String's reference
// semantics. Java code that writes `acc.latestPayload = e.payload`
// just copies a reference; C++ value semantics would deep-copy every
// byte. For an apples-to-apples comparison of engine throughput (not
// language-level data semantics) the C++ bench uses shared_ptr so
// payload bytes are produced once at the source and shared by
// reference downstream.
using PayloadPtr = std::shared_ptr<const std::string>;
using TagsPtr = std::shared_ptr<const std::vector<std::int64_t>>;
using AttrsPtr = std::shared_ptr<const std::map<std::string, std::string>>;

// EventBody bundles the three large heap-allocated fields under one
// shared_ptr so a Record<Event>'s destructor only has to do one
// atomic refcount decrement instead of three. On the 10M-record bench
// that drops ~20M atomic ops from the hot-path teardown.
struct EventBody {
    std::string payload;                            // ~1.5 KB
    std::vector<std::int64_t> tags;                 // 50 int64s = ~400 B
    std::map<std::string, std::string> attributes;  // 4 entries = ~100 B
};
using BodyPtr = std::shared_ptr<const EventBody>;

struct Event {
    std::int64_t ts_ms{0};
    std::int64_t key{0};
    std::int64_t value{0};
    BodyPtr body;  // payload + tags + attributes, shared
};

// EventStats: per-(key, window) aggregate. Holds the latest payload so
// per-key state is rich enough that RocksDB writes are non-trivial.
struct EventStats {
    std::int64_t sum_value{0};
    std::int64_t count{0};
    PayloadPtr latest_payload;
};

// ---------------------------------------------------------------------------
// Codec helpers
// ---------------------------------------------------------------------------

// Codec helpers use raw-pointer memcpy into a pre-sized buffer rather
// than push_back loops. push_back-per-byte is the dominant cost on the
// 10M-record bench - each int64 push_back loop runs 8 increment+write
// ops with bounds-check overhead vs a single 8-byte store.
inline void put_u32_le(std::byte*& p, std::uint32_t v) {
    std::memcpy(p, &v, 4);
    p += 4;
}
inline void put_i64_le(std::byte*& p, std::int64_t v) {
    std::memcpy(p, &v, 8);
    p += 8;
}
inline void put_bytes(std::byte*& p, const std::string& s) {
    const auto n = static_cast<std::uint32_t>(s.size());
    std::memcpy(p, &n, 4);
    p += 4;
    std::memcpy(p, s.data(), n);
    p += n;
}

inline std::uint32_t read_u32_le(std::span<const std::byte> b, std::size_t& pos) {
    std::uint32_t v;
    std::memcpy(&v, b.data() + pos, 4);
    pos += 4;
    return v;
}
inline std::int64_t read_i64_le(std::span<const std::byte> b, std::size_t& pos) {
    std::int64_t v;
    std::memcpy(&v, b.data() + pos, 8);
    pos += 8;
    return v;
}
inline std::string read_string(std::span<const std::byte> b, std::size_t& pos) {
    const auto n = read_u32_le(b, pos);
    std::string s(reinterpret_cast<const char*>(b.data() + pos), n);
    pos += n;
    return s;
}

inline clink::Codec<Event> event_codec() {
    return clink::Codec<Event>{
        .encode =
            [](const Event& e) {
                static const EventBody kEmptyBody;
                const EventBody& body = e.body ? *e.body : kEmptyBody;
                const std::string& payload = body.payload;
                const auto& tags = body.tags;
                const auto& attrs = body.attributes;
                std::size_t total = 8 * 3                  // ts, key, value
                                    + 4 + payload.size()    // payload
                                    + 4 + 8 * tags.size()   // tags
                                    + 4;                   // attr count
                for (const auto& [k, v] : attrs) {
                    total += 4 + k.size() + 4 + v.size();
                }
                std::vector<std::byte> out(total);
                std::byte* p = out.data();
                put_i64_le(p, e.ts_ms);
                put_i64_le(p, e.key);
                put_i64_le(p, e.value);
                put_bytes(p, payload);
                const auto n_tags = static_cast<std::uint32_t>(tags.size());
                std::memcpy(p, &n_tags, 4);
                p += 4;
                std::memcpy(p, tags.data(), n_tags * sizeof(std::int64_t));
                p += n_tags * sizeof(std::int64_t);
                const auto n_attrs = static_cast<std::uint32_t>(attrs.size());
                std::memcpy(p, &n_attrs, 4);
                p += 4;
                for (const auto& [k, v] : attrs) {
                    put_bytes(p, k);
                    put_bytes(p, v);
                }
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<Event> {
            if (b.size() < 24) {
                return std::nullopt;
            }
            std::size_t pos = 0;
            Event e;
            e.ts_ms = read_i64_le(b, pos);
            e.key = read_i64_le(b, pos);
            e.value = read_i64_le(b, pos);
            // Decode into a fresh EventBody, then hand it to a single
            // shared_ptr<const EventBody>. One heap object instead of
            // three, one atomic refcount instead of three on every
            // copy/destroy.
            auto body = std::make_shared<EventBody>();
            body->payload = read_string(b, pos);
            const auto n_tags = read_u32_le(b, pos);
            body->tags.reserve(n_tags);
            for (std::uint32_t i = 0; i < n_tags; ++i) {
                body->tags.push_back(read_i64_le(b, pos));
            }
            const auto n_attrs = read_u32_le(b, pos);
            for (std::uint32_t i = 0; i < n_attrs; ++i) {
                auto k = read_string(b, pos);
                auto v = read_string(b, pos);
                body->attributes.emplace(std::move(k), std::move(v));
            }
            e.body = std::move(body);
            return e;
        }};
}

inline clink::Codec<EventStats> event_stats_codec() {
    const auto write_payload = [](const EventStats& s, std::byte* p) {
        const std::string& payload = s.latest_payload ? *s.latest_payload : std::string{};
        put_i64_le(p, s.sum_value);
        put_i64_le(p, s.count);
        put_bytes(p, payload);
    };
    return clink::Codec<EventStats>{
        .encode =
            [write_payload](const EventStats& s) {
                const std::string& payload =
                    s.latest_payload ? *s.latest_payload : std::string{};
                const std::size_t total = 8 + 8 + 4 + payload.size();
                std::vector<std::byte> out(total);
                write_payload(s, out.data());
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<EventStats> {
            if (b.size() < 20) {
                return std::nullopt;
            }
            std::size_t pos = 0;
            EventStats s;
            s.sum_value = read_i64_le(b, pos);
            s.count = read_i64_le(b, pos);
            s.latest_payload = std::make_shared<std::string>(read_string(b, pos));
            return s;
        },
        .encode_into = [write_payload](const EventStats& s, std::vector<std::byte>& out) {
            // Zero-alloc on the hot path after warm-up: caller-owned
            // (thread_local in KeyedState::put) buffer is resized in
            // place, then written into. The 1.5KB payload memcpy is
            // unavoidable; the std::vector allocation that the
            // returning-by-value encode() shape forced isn't.
            const std::string& payload = s.latest_payload ? *s.latest_payload : std::string{};
            const std::size_t total = 8 + 8 + 4 + payload.size();
            out.resize(total);
            write_payload(s, out.data());
        }};
}

// ---------------------------------------------------------------------------
// Synthetic source: emits Event records at full speed in batches of
// kBatchSize. Bounded - returns nullopt after `total_` records so the
// pipeline drains naturally.
// ---------------------------------------------------------------------------

constexpr std::size_t kBatchSize = 256;

class SyntheticEventSource final : public clink::Source<Event> {
public:
    // total = records this shard will emit; offset = where this shard
    // starts in the global cursor; global_total = the unsharded
    // total (used so all shards space their event-time stamps over
    // the same window range and watermarks line up).
    SyntheticEventSource(std::int64_t total,
                         std::int64_t keys,
                         std::int64_t windows,
                         std::size_t payload_size,
                         std::int64_t offset = 0,
                         std::int64_t global_total = 0)
        : total_(total),
          keys_(keys),
          windows_(windows),
          payload_size_(payload_size),
          offset_(offset),
          global_total_(global_total == 0 ? total : global_total) {
        auto body = std::make_shared<EventBody>();
        body->payload = std::string(payload_size, 'x');
        body->tags.reserve(50);
        for (int i = 0; i < 50; ++i) {
            body->tags.push_back(i * 7);
        }
        body->attributes.emplace("region", "us-west-2");
        body->attributes.emplace("env", "prod");
        body->attributes.emplace("service", "events");
        body->attributes.emplace("version", "v1.42.0");
        body_ = std::move(body);
    }

    bool produce(clink::Emitter<Event>& out) override {
        if (this->cancelled() || cursor_ >= total_) {
            if (!eos_sent_) {
                out.emit_watermark(clink::Watermark::max());
                eos_sent_ = true;
            }
            return false;
        }
        const std::int64_t end =
            std::min<std::int64_t>(cursor_ + static_cast<std::int64_t>(kBatchSize), total_);
        clink::Batch<Event> batch;
        // Reserve up-front so push_back doesn't realloc as the
        // batch fills - each realloc copies every previously-pushed
        // Record, and Record carries shared_ptr fields whose copy is
        // an atomic refcount bump. On a 256-record batch that's
        // ~8 reallocations × ~128 mean copies = ~1k Record copies
        // per batch we can avoid with one reserve call.
        batch.reserve(static_cast<std::size_t>(end - cursor_));
        // Map shard-local cursor (0..total_) to the global cursor
        // (offset_..offset_+total_) so event-time and key distribution
        // are consistent across shards.
        const double step_ms =
            static_cast<double>(windows_ * 1000) / static_cast<double>(global_total_);
        for (std::int64_t i = cursor_; i < end; ++i) {
            const std::int64_t g = offset_ + i;
            Event e;
            e.ts_ms = static_cast<std::int64_t>(static_cast<double>(g) * step_ms);
            e.key = g % keys_;
            e.value = (g % 7) + 1;
            e.body = body_;  // one refcount bump - same shared bytes per record
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
    std::size_t payload_size_;
    std::int64_t offset_;
    std::int64_t global_total_;
    BodyPtr body_;
    std::int64_t cursor_{0};
    bool eos_sent_{false};
};

// ---------------------------------------------------------------------------
// Counting sink: increments a process-wide counter and prints summary
// on close so the bench can pick up the final count.
// ---------------------------------------------------------------------------

class CountingSink final : public clink::Sink<EventStats> {
public:
    explicit CountingSink(std::string label) : label_(std::move(label)) {}

    void on_data(const clink::Batch<EventStats>& b) override {
        count_ += static_cast<std::int64_t>(b.size());
    }

    void close() override {
        std::fprintf(stderr,
                     "[%s] sink final count: %lld\n",
                     label_.c_str(),
                     static_cast<long long>(count_));
    }

    std::string name() const override { return "counting_sink"; }

private:
    std::string label_;
    std::int64_t count_{0};
};

// ---------------------------------------------------------------------------
// Env-driven config
// ---------------------------------------------------------------------------

inline std::int64_t env_i64(const char* name, std::int64_t fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return fallback;
}

inline void define_job(clink::api::StreamExecutionEnvironment& env) {
    using namespace std::chrono_literals;

    // The .so dlopens with RTLD_LOCAL so its StateBackendFactory
    // singleton is private; the TM-side install() never reaches it.
    // Calling install() here registers "rocksdb" scheme on the .so's
    // own factory so make_subtask_job_config() can build the backend.
    clink::rocksdb::install();

    const auto records = env_i64("BENCH_RECORDS", 10'000'000);
    const auto keys = env_i64("BENCH_KEYS", 1000);
    const auto windows = env_i64("BENCH_WINDOWS", 100);
    const auto payload_size = env_i64("BENCH_PAYLOAD_BYTES", 1500);
    const auto parallelism =
        static_cast<std::uint32_t>(env_i64("BENCH_PARALLELISM", 4));

    // Register typed channels for Event + EventStats so the pipeline
    // can carry them on the wire and through state.
    env.registry().register_type<Event>("inproc.event", event_codec());
    env.registry().register_type<EventStats>("inproc.event_stats", event_stats_codec());

    // Register the synthetic source as a custom op_type. Builds one
    // sharded source per subtask: subtask_idx I of parallelism P owns
    // the i-th 1/P slice of [0, records). Each shard emits its slice
    // of records spaced over the full event-time span, so watermarks
    // at every subtask reach the same final value.
    const std::string source_op = "bench.parallel.source";
    env.registry().register_source<Event>(
        source_op,
        [records, keys, windows, payload_size, parallelism](
            const clink::plugin::BuildContext& ctx) {
            const auto idx = static_cast<std::int64_t>(ctx.subtask_idx);
            const auto par = std::max<std::int64_t>(1, static_cast<std::int64_t>(parallelism));
            const auto per_shard = records / par;
            const auto shard_start = idx * per_shard;
            const auto shard_end = (idx + 1 == par) ? records : shard_start + per_shard;
            return std::make_shared<SyntheticEventSource>(
                shard_end - shard_start, keys, windows, payload_size, shard_start, records);
        });

    // Register a stable key extractor by name so all subtasks at
    // parallelism > 1 hash-partition consistently. (Inline-lambda
    // .key_by(fn) mints a unique per-call name which is fine for
    // par=1 but doesn't survive a multi-subtask plan.)
    env.registry().register_key_extractor<Event>(
        "by_key", [](const Event& e) { return e.key; });

    // Register the counting sink as a custom op_type, similarly.
    const std::string sink_op = "bench.parallel.sink";
    env.registry().register_sink<EventStats>(
        sink_op,
        [](const clink::plugin::BuildContext& ctx) {
            return std::make_shared<CountingSink>("clink-subtask-" +
                                                  std::to_string(ctx.subtask_idx));
        });

    env.set_parallelism(parallelism);

    clink::api::SourceDescriptor src_desc;
    src_desc.op_type = source_op;
    src_desc.channel_type = "inproc.event";
    src_desc.parallelism = parallelism;

    clink::api::SinkDescriptor sink_desc;
    sink_desc.op_type = sink_op;
    sink_desc.channel_type = "inproc.event_stats";
    sink_desc.parallelism = parallelism;

    env.source<Event>(src_desc)
        .assign_timestamps_monotonic([](const Event& e) { return clink::EventTime{e.ts_ms}; })
        .key_by(std::string{"by_key"})
        .tumbling_window(1000ms)
        .aggregate<EventStats, EventStats>(
            []() -> EventStats { return EventStats{}; },
            [](const EventStats& acc, const Event& e) {
                EventStats next;
                next.sum_value = acc.sum_value + e.value;
                next.count = acc.count + 1;
                // Aliasing shared_ptr: piggyback on the body's control
                // block so latest_payload is one heap allocation
                // worth of refcount, not a separate one.
                if (e.body) {
                    next.latest_payload = PayloadPtr(e.body, &e.body->payload);
                }
                return next;
            },
            [](std::int64_t /*key*/,
               const clink::TimeWindow& /*window*/,
               const EventStats& s) { return s; })
        .sink(sink_desc);
}

}  // namespace bench

CLINK_REGISTER_JOB("parallel-recovery-bench-pipeline",
                   "1.0",
                   "sharded source -> hash-keyed tumbling window at par=N",
                   bench::define_job);
