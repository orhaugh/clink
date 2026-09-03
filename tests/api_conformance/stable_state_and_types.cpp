// Stable tier: keyed and broadcast state, the typed state views, backends and
// schema versioning. Compile-only; frozen (see README.md). Additions only.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/derived_codec.hpp"
#include "clink/core/fields.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/broadcast_state.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state/typed_state.hpp"

struct CounterV2 {
    std::int64_t count{};
    std::int64_t last_seen_ms{};
};
CLINK_FIELDS(CounterV2, count, last_seen_ms);

// Bumping a type's schema version is the user's signal that its bytes changed.
template <>
struct clink::SchemaVersionTrait<CounterV2> {
    static constexpr std::uint32_t value = 2;
};
static_assert(clink::schema_version_v<CounterV2> == 2);

namespace {

[[maybe_unused]] void keyed_state_slots() {
    clink::InMemoryStateBackend backend;
    clink::RuntimeContext ctx(clink::OperatorId{1}, "state", &backend, nullptr);

    clink::KeyedState<std::int64_t, std::string> plain = ctx.keyed_state<std::int64_t, std::string>(
        "plain", clink::int64_codec(), clink::string_codec());
    plain.put(1, "a");
    (void)plain.get(1);
    plain.erase(1);
    plain.scan([](const std::int64_t&, const std::string&) {});

    clink::TtlConfig ttl;
    ttl.ttl = std::chrono::milliseconds{60'000};
    ttl.refresh_on_write = true;
    ttl.refresh_on_read = false;
    ttl.domain = clink::TtlTimeDomain::EventTime;
    clink::KeyedState<std::int64_t, std::string> expiring =
        ctx.keyed_state<std::int64_t, std::string>(
            "expiring", clink::int64_codec(), clink::string_codec(), ttl);
    (void)expiring.ttl_stats().unscanned_backlog;

    clink::KeyedState<std::int64_t, CounterV2> declared = ctx.keyed_state<std::int64_t, CounterV2>(
        "declared", clink::int64_codec(), clink::derived_codec<CounterV2>());
    declared.put(1, CounterV2{.count = 1, .last_seen_ms = 2});

    clink::BroadcastState<std::string> rules =
        ctx.broadcast_state<std::string>("rules", clink::string_codec());
    rules.put("r");
    (void)rules.get();

    clink::ListState<std::int64_t, std::string> list = ctx.list_state<std::int64_t, std::string>(
        "list", clink::int64_codec(), clink::string_codec());
    list.add(1, "x");
    list.add_all(1, {"y", "z"});
    (void)list.get(1);
    (void)list.empty(1);
    list.update(1, {"only"});
    list.advance_watermark(0);

    clink::MapState<std::int64_t, std::string, std::int64_t> map =
        ctx.map_state<std::int64_t, std::string, std::int64_t>(
            "map", clink::int64_codec(), clink::string_codec(), clink::int64_codec());
    map.put(1, "k", 2);
    (void)map.get(1, "k");
    (void)map.contains(1, "k");
    (void)map.entries(1);
    map.remove(1, "k");

    clink::AggregatingState<std::int64_t, std::int64_t, std::int64_t, std::int64_t> agg =
        ctx.aggregating_state<std::int64_t, std::int64_t, std::int64_t, std::int64_t>(
            "agg",
            clink::int64_codec(),
            clink::int64_codec(),
            []() -> std::int64_t { return 0; },
            [](const std::int64_t& acc, const std::int64_t& in) { return acc + in; },
            [](const std::int64_t& acc) { return acc; });
    agg.add(1, 5);
    (void)agg.get(1);
    (void)agg.accumulator(1);

    clink::ReducingState<std::int64_t, std::int64_t> red =
        ctx.reducing_state<std::int64_t, std::int64_t>(
            "red",
            clink::int64_codec(),
            clink::int64_codec(),
            [](const std::int64_t& a, const std::int64_t& b) { return a + b; });
    red.add(1, 1);
    (void)red.get(1);
}

[[maybe_unused]] void backends_and_factories() {
    clink::InMemoryStateBackend backend;
    backend.put(clink::OperatorId{1}, "k", "v");
    (void)backend.get(clink::OperatorId{1}, "k");
    backend.erase(clink::OperatorId{1}, "k");
    const clink::Snapshot snap = backend.snapshot(clink::CheckpointId{1});
    backend.restore(snap);
    backend.clear();

    clink::StateBackendFactory& factory = clink::StateBackendFactory::default_instance();
    factory.register_scheme("conformance", [](const clink::StateBackendSpec& spec) {
        (void)spec.uri;
        (void)spec.subtask_idx;
        (void)spec.restore_uri;
        (void)spec.restore_checkpoint_id;
        return clink::BuiltStateBackend{.backend = std::make_shared<clink::InMemoryStateBackend>(),
                                        .restore_from = std::nullopt};
    });
    (void)factory.has_scheme("conformance");
    clink::StateBackendSpec spec;
    spec.uri = "memory://";
    (void)factory.build(spec).backend;
}

[[maybe_unused]] void schema_migrations() {
    clink::StateMigrationRegistry::global().register_migration(
        "CounterV2", /*from_version=*/1, /*to_version=*/2, [](std::span<const std::byte> in) {
            return std::vector<std::byte>(in.begin(), in.end());
        });
    (void)clink::StateMigrationRegistry::global().has_path("CounterV2", 1, 2);
    (void)clink::StateMigrationRegistry::global().edges_for("CounterV2");

    clink::StateVersionMap versions;
    versions.set(clink::OperatorId{1}, "CounterV2", 2, "declared");
    const std::string packed = versions.pack();
    (void)clink::StateVersionMap::unpack(packed);
}

}  // namespace
