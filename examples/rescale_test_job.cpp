// rescale_test_job - long-running keyed pipeline used by the rescale
// integration test. The shape is deliberately tiny:
//
//   slow_source<int64_t>(N counts, sleep K ms per record)
//     -> map<KV>(value v -> KV{key = v % 8, count = 1})
//     -> key_by(KV::key)
//     -> reduce(KV{k, a.count + b.count})
//     -> map<string>("key|count")
//     -> file_text_sink(parallelism configurable, one file per subtask)
//
// The source is intentionally slow so the test has time to:
//   1. let the JM trigger at least one checkpoint,
//   2. call clink_rescale_job to expand reduce + sink parallelism,
//   3. observe that the redeploy lands on the new parallelism.
//
// Configurable via env (read once on .so load, propagated to all
// processes by the caller):
//   CLINK_RESCALE_COUNT       - total records to emit (default 200)
//   CLINK_RESCALE_TICK_MS     - ms to sleep between records (default 25)
//   CLINK_RESCALE_OUT_BASE    - sink path prefix; sinks append ".<idx>"
//   CLINK_RESCALE_INITIAL_P   - initial reduce + sink parallelism (default 2)
//
// State preservation across rescale is what the test ultimately
// exercises: the reducer keeps a per-key count, and after rescale
// each new reduce subtask reads its assigned key-group slice from
// the appropriate parent's checkpoint file.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/source_operator.hpp"

namespace rescale_test {

struct KV {
    std::int64_t key{0};
    std::int64_t count{0};
};

inline clink::Codec<KV> kv_codec() {
    return clink::Codec<KV>{
        .encode =
            [](const KV& v) {
                std::vector<std::byte> out;
                out.reserve(16);
                const auto put_i64 = [&](std::int64_t x) {
                    const auto u = static_cast<std::uint64_t>(x);
                    for (int i = 0; i < 8; ++i) {
                        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                    }
                };
                put_i64(v.key);
                put_i64(v.count);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<KV> {
            if (b.size() < 16) {
                return std::nullopt;
            }
            const auto read_i64 = [&](std::size_t pos) -> std::int64_t {
                std::uint64_t u = 0;
                for (int i = 0; i < 8; ++i) {
                    u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[pos + i]))
                         << (i * 8);
                }
                return static_cast<std::int64_t>(u);
            };
            KV v;
            v.key = read_i64(0);
            v.count = read_i64(8);
            return v;
        }};
}

inline std::int64_t env_i64(const char* name, std::int64_t fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return fallback;
}

inline std::string env_str(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return p;
    }
    return fallback;
}

// Slow source: emits sequential int64 values 0..count-1, with a
// sleep_for between each so the pipeline stays alive long enough for
// the rescale-driving test to land its CLI invocation between
// checkpoints.
inline std::shared_ptr<clink::Source<std::int64_t>> make_slow_source(std::int64_t count,
                                                                     std::int64_t tick_ms,
                                                                     std::int64_t parallelism,
                                                                     std::int64_t subtask_idx) {
    auto state = std::make_shared<std::int64_t>(subtask_idx);
    auto gen =
        [state, count, tick_ms, parallelism]() -> std::optional<clink::Record<std::int64_t>> {
        if (*state >= count) {
            return std::nullopt;
        }
        if (tick_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds{tick_ms});
        }
        clink::Record<std::int64_t> r{*state};
        *state += parallelism;
        return r;
    };
    return std::make_shared<clink::GeneratorSource<std::int64_t>>(std::move(gen),
                                                                  "rescale_slow_source");
}

inline void define_job(clink::api::StreamExecutionEnvironment& env) {
    const auto count = env_i64("CLINK_RESCALE_COUNT", 200);
    const auto tick_ms = env_i64("CLINK_RESCALE_TICK_MS", 25);
    const auto initial_p = env_i64("CLINK_RESCALE_INITIAL_P", 2);
    const auto out_base = env_str("CLINK_RESCALE_OUT_BASE", "/tmp/clink_rescale_out");

    // Register int64_t into the per-job bundle so register_source<int64_t>
    // below can find the typed bridge factories. The built-ins
    // singleton has it, but env.registry() is a bundle-scoped overlay
    // and won't see the singleton's typed entry for source-side
    // construction.
    env.registry().register_type<std::int64_t>("int64", clink::int64_codec());
    env.registry().register_type<KV>("rescale.kv", kv_codec());

    // Register the slow source as a plugin op so the per-subtask
    // factory can read parallelism + subtask_idx from BuildContext
    // and partition the count range across subtasks.
    const std::string source_op_type = "rescale_slow_source";
    env.registry().register_source<std::int64_t>(
        source_op_type, [count, tick_ms](const clink::plugin::BuildContext& ctx) {
            const auto par = static_cast<std::int64_t>(ctx.parallelism == 0 ? 1 : ctx.parallelism);
            return make_slow_source(
                count, tick_ms, par, static_cast<std::int64_t>(ctx.subtask_idx));
        });

    clink::api::SourceDescriptor src_desc;
    src_desc.op_type = source_op_type;
    src_desc.channel_type = clink::api::ChannelName<std::int64_t>::get();
    src_desc.parallelism = 1;

    env.source<std::int64_t>(src_desc)
        .map<KV>([](const std::int64_t& v) { return KV{v % 8, 1}; })
        .key_by([](const KV& kv) -> std::int64_t { return kv.key; })
        .reduce([](const KV& a, const KV& b) { return KV{a.key, a.count + b.count}; })
        // Stable uid: the reduce is stateful (per-key accumulator), so it must
        // pin an OperatorId for keyed-state restore across checkpoint/rescale.
        .uid("rescale-reduce")
        .map<std::string>(
            [](const KV& kv) { return std::to_string(kv.key) + "|" + std::to_string(kv.count); })
        .sink(clink::api::FileTextSink::builder()
                  .path(out_base)
                  .parallelism(static_cast<std::size_t>(initial_p))
                  .build());
}

}  // namespace rescale_test

CLINK_REGISTER_JOB("rescale-test",
                   "1.0",
                   "slow source -> map -> keyBy(8) -> reduce sum -> file sink",
                   rescale_test::define_job);
