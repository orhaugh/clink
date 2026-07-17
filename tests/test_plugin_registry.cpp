// In-process exercise of the PluginRegistry API. No dlopen yet;
// these tests instantiate the templates in the cluster's
// own compilation unit and verify they wire into the singletons
// correctly. A real plugin .so would produce the same registrations.

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/install_defaults.hpp"
#include "clink/plugin/plugin.hpp"

using namespace clink;

namespace {

// Custom record type the "plugin" defines.
struct Greeting {
    std::int64_t id{0};
    std::string message;
};

// Minimal codec: encode the int64 + length-prefixed message.
Codec<Greeting> greeting_codec() {
    return Codec<Greeting>{
        .encode = [](const Greeting& g) -> std::vector<std::byte> {
            std::vector<std::byte> out;
            const auto put_u64 = [&](std::uint64_t v) {
                for (int i = 0; i < 8; ++i) {
                    out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
                }
            };
            const auto put_u32 = [&](std::uint32_t v) {
                for (int i = 0; i < 4; ++i) {
                    out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
                }
            };
            put_u64(static_cast<std::uint64_t>(g.id));
            put_u32(static_cast<std::uint32_t>(g.message.size()));
            for (char c : g.message) {
                out.push_back(static_cast<std::byte>(c));
            }
            return out;
        },
        .decode = [](std::span<const std::byte> b) -> std::optional<Greeting> {
            if (b.size() < 12)
                return std::nullopt;
            std::uint64_t id_raw = 0;
            for (int i = 0; i < 8; ++i) {
                id_raw |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[i])) << (i * 8);
            }
            std::uint32_t len = 0;
            for (int i = 0; i < 4; ++i) {
                len |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[8 + i])) << (i * 8);
            }
            if (b.size() != 12 + len)
                return std::nullopt;
            std::string msg(reinterpret_cast<const char*>(b.data() + 12), len);
            return Greeting{static_cast<std::int64_t>(id_raw), std::move(msg)};
        }};
}

class GreetingSource final : public Source<Greeting> {
    bool emitted_{false};

public:
    bool produce(Emitter<Greeting>& out) override {
        if (emitted_)
            return false;
        Batch<Greeting> b;
        b.emplace(Greeting{1, "hello"});
        b.emplace(Greeting{2, "world"});
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark::max());
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "greeting_source"; }
};

class CollectingGreetingSink final : public Sink<Greeting> {
public:
    void on_data(const Batch<Greeting>& batch) override {
        for (const auto& r : batch) {
            collected_.push_back(r.value());
        }
    }
    const std::vector<Greeting>& collected() const { return collected_; }
    std::string name() const override { return "collecting_greeting_sink"; }

private:
    std::vector<Greeting> collected_;
};

}  // namespace

TEST(PluginRegistry, RegisterTypeStampsBridgeBuilders) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    reg.register_type<Greeting>("test.Greeting", greeting_codec());

    // Channel ops are now in TypeRegistry under "test.Greeting".
    const auto* ops = tr.find("test.Greeting");
    ASSERT_NE(ops, nullptr);
    EXPECT_EQ(ops->channel_name, "test.Greeting");
    EXPECT_TRUE(static_cast<bool>(ops->bind_inbound_bridge));
    EXPECT_TRUE(static_cast<bool>(ops->connect_outbound_bridge));

    // typeid reverse map populated.
    EXPECT_EQ(tr.channel_for_typeid(typeid(Greeting).name()), "test.Greeting");
}

TEST(PluginRegistry, RegisterSourceWithoutRegisterTypeIsRejected) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    EXPECT_THROW(reg.register_source<Greeting>("test.greeting_src",
                                               [](const clink::plugin::BuildContext&) {
                                                   return std::make_shared<GreetingSource>();
                                               }),
                 std::runtime_error);
}

TEST(PluginRegistry, RegisterSourceInstallsRunner) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    reg.register_type<Greeting>("test.Greeting", greeting_codec());
    reg.register_source<Greeting>("test.greeting_src", [](const clink::plugin::BuildContext&) {
        return std::make_shared<GreetingSource>();
    });

    // Runner stored under (op_type, channel-name-for-T).
    const auto* runner = rr.find_source("test.greeting_src", "test.Greeting");
    EXPECT_NE(runner, nullptr);
}

TEST(PluginRegistry, RegisterSinkInstallsRunner) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    reg.register_type<Greeting>("test.Greeting", greeting_codec());
    reg.register_sink<Greeting>("test.greeting_sink", [](const clink::plugin::BuildContext&) {
        return std::make_shared<CollectingGreetingSink>();
    });

    const auto* runner = rr.find_sink("test.greeting_sink", "test.Greeting");
    EXPECT_NE(runner, nullptr);
}

TEST(PluginRegistry, BuildContextCarriesParamsAndSubtaskInfo) {
    // Verify the BuildContext that runners hand to factories is shaped
    // correctly. Construct one directly (mimics what the runner builds
    // from a RunnerContext).
    clink::plugin::BuildContext bctx;
    bctx.params["count"] = "5";
    bctx.params["start"] = "100";
    bctx.subtask_idx = 2;
    bctx.parallelism = 4;

    EXPECT_EQ(bctx.param_int64_or("count", 0), 5);
    EXPECT_EQ(bctx.param_int64_or("start", 0), 100);
    EXPECT_EQ(bctx.param_int64_or("missing", 42), 42);
    EXPECT_EQ(bctx.param_or("missing", "default"), "default");
}

// Schema evolution (cluster-C part 2): make_subtask_job_config unpacks the
// RunnerContext's packed expected state-version map onto the JobConfig so
// the LocalExecutor auto-migrates restored state. This is the worker-side
// terminus of the deploy wire DeployMsg.expected_state_versions_packed ->
// RunnerContext -> JobConfig.expected_state_versions.
TEST(PluginRegistry, MakeSubtaskJobConfigUnpacksExpectedStateVersions) {
    clink::cluster::OperatorChainSpec chain;  // default: subtask_idx 0

    // Build a valid packed map via the real API (op_id is numeric).
    StateVersionMap vmap;
    vmap.set(operator_id_from_uid("agg-sum"), "i64_sum", 3);
    vmap.set(operator_id_from_uid("win-cnt"), "i64_cnt", 2);
    const std::string packed = vmap.pack();

    clink::cluster::RunnerContext rctx{.chain = chain};
    rctx.expected_state_versions_packed = packed;
    auto cfg = clink::plugin::detail::make_subtask_job_config(rctx);
    ASSERT_TRUE(cfg.expected_state_versions.has_value());
    // Unpacked map round-trips back to the same packed string.
    EXPECT_EQ(cfg.expected_state_versions->pack(), packed);

    // Empty packed string -> no declared versions -> restore verbatim.
    clink::cluster::RunnerContext rctx_empty{.chain = chain};
    auto cfg_empty = clink::plugin::detail::make_subtask_job_config(rctx_empty);
    EXPECT_FALSE(cfg_empty.expected_state_versions.has_value());

    // Slot survives the unpack onto JobConfig. A slotless input is a
    // pack()-fixpoint, so assert the slot explicitly via entries().
    StateVersionMap slotted;
    slotted.set(operator_id_from_uid("join"), "left", 2, "left_buf");
    clink::cluster::RunnerContext rctx_slot{.chain = chain};
    rctx_slot.expected_state_versions_packed = slotted.pack();
    auto cfg_slot = clink::plugin::detail::make_subtask_job_config(rctx_slot);
    ASSERT_TRUE(cfg_slot.expected_state_versions.has_value());
    const auto slot_entries = cfg_slot.expected_state_versions->entries();
    ASSERT_EQ(slot_entries.size(), 1u);
    EXPECT_EQ(slot_entries[0].slot, "left_buf");
    EXPECT_EQ(slot_entries[0].version, 2u);
}

// A trivial CoOperator used only to exercise the registry; the
// process_element methods are no-ops because this test cares about
// REGISTRATION + LOOKUP shape, not runtime behaviour.
class DummyCoOp final : public clink::CoOperator<std::string, std::int64_t, std::int64_t> {
public:
    void process_element1(const clink::StreamElement<std::string>&,
                          clink::Emitter<std::int64_t>&) override {}
    void process_element2(const clink::StreamElement<std::int64_t>&,
                          clink::Emitter<std::int64_t>&) override {}
    std::string name() const override { return "dummy_co_op"; }
};

// Regression test for the gap-#5 symmetry hack. Before the fix, the
// planner had to try BOTH (in1, in2) and (in2, in1) orderings of
// find_co_operator because the user's template <In1=A, In2=B>
// determined the registration order while the graph input order
// determined the lookup order. After the fix, RunnerRegistry::
// register_co_operator and find_co_operator both canonicalize
// (in1, in2) - sorted lexicographically - so order is irrelevant.
TEST(PluginRegistry, RegisterCoOperatorLookupIsOrderIndependent) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    // string sorts BEFORE int64 lexicographically, so the user
    // registering <In1=string, In2=int64> and the planner looking up
    // <int64, string> exercises the swap.
    reg.register_type<std::string>("string", clink::string_codec());
    reg.register_type<std::int64_t>("int64", clink::int64_codec());
    reg.register_co_operator<std::string, std::int64_t, std::int64_t>(
        "test.dummy_co_op",
        [](const clink::plugin::BuildContext&) { return std::make_shared<DummyCoOp>(); });

    // Lookup in the order the user templated: (string, int64).
    EXPECT_NE(rr.find_co_operator("test.dummy_co_op", "string", "int64", "int64"), nullptr);

    // Lookup in the opposite order (matches what the planner would do
    // if the graph wired the int64 source as input[0] and string source
    // as input[1]). Pre-fix this returned nullptr; post-fix it finds
    // the same registration.
    EXPECT_NE(rr.find_co_operator("test.dummy_co_op", "int64", "string", "int64"), nullptr);
}

// A trivial KeyedProcessFunction used to exercise the keyed-registration
// sugar. Tracks the most recent current_key seen so a registration-level
// test can verify the adapter is wired in correctly.
class StringKeyedCounter final
    : public clink::KeyedProcessFunction<std::string, std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t&,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>&) override {}
    std::string name() const override { return "string_keyed_counter"; }
};

TEST(PluginRegistry, RegisterKeyedOperatorWiresAdapter) {
    // The sugar should register an Operator<In, Out> under the given
    // op_type, looking up successfully in the runner registry with the
    // matching (In, Out) channel pair. We don't run the operator here;
    // the registration shape is the contract.
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    reg.register_type<std::int64_t>("int64", clink::int64_codec());

    reg.register_keyed_operator<std::string, std::int64_t, std::int64_t>(
        "test.string_keyed_counter",
        [](const clink::plugin::BuildContext&) { return std::make_shared<StringKeyedCounter>(); },
        [](const std::int64_t& v) { return std::to_string(v % 4); });

    EXPECT_NE(rr.find_operator("test.string_keyed_counter", "int64", "int64"), nullptr);
}

// A trivial KeyedCoProcessFunction for the keyed_co_operator sugar test.
class StringKeyedCoCounter final
    : public clink::KeyedCoProcessFunction<std::string, std::string, std::int64_t, std::int64_t> {
public:
    void process_element1(const std::string&,
                          clink::ProcessFunctionContext<std::int64_t>&,
                          clink::Collector<std::int64_t>&) override {}
    void process_element2(const std::int64_t&,
                          clink::ProcessFunctionContext<std::int64_t>&,
                          clink::Collector<std::int64_t>&) override {}
    std::string name() const override { return "string_keyed_co_counter"; }
};

TEST(PluginRegistry, RegisterKeyedCoOperatorWiresAdapter) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    reg.register_type<std::string>("string", clink::string_codec());
    reg.register_type<std::int64_t>("int64", clink::int64_codec());

    reg.register_keyed_co_operator<std::string, std::string, std::int64_t, std::int64_t>(
        "test.string_keyed_co_counter",
        [](const clink::plugin::BuildContext&) { return std::make_shared<StringKeyedCoCounter>(); },
        [](const std::string& v) { return v; },
        [](const std::int64_t& v) { return std::to_string(v); });

    // Same canonicalization as register_co_operator - order-independent.
    EXPECT_NE(rr.find_co_operator("test.string_keyed_co_counter", "string", "int64", "int64"),
              nullptr);
    EXPECT_NE(rr.find_co_operator("test.string_keyed_co_counter", "int64", "string", "int64"),
              nullptr);
}

TEST(PluginRegistry, InstallDefaultsRegistersBuiltIns) {
    // install_defaults is a one-call replacement for the
    // ensure_built_ins_registered + clink::<impl>::install(...) sequence
    // users would otherwise have to repeat. The test target links only
    // clink::core (per tests/CMakeLists.txt), so the impl install hooks
    // aren't visible - but the built-ins ARE visible regardless of
    // impl-link state, so this test pins that contract.
    //
    // Idempotent: calling twice must not throw.
    clink::plugin::PluginRegistry reg;  // attaches to default singletons
    clink::plugin::install_defaults(reg);
    clink::plugin::install_defaults(reg);  // second call is a no-op

    auto& rr = clink::cluster::RunnerRegistry::default_instance();
    // int64_range_source is a built-in registered by
    // ensure_built_ins_registered().
    EXPECT_NE(rr.find_source("int64_range_source", "int64"), nullptr);
}

// env:// secret indirection (roadmap F6): a connector option may reference a
// secret as `env://VAR` so a spec / catalog stores a reference, not a plaintext
// password. BuildContext::param_or resolves it from the environment at build
// time; a literal is returned verbatim; an unset variable yields empty (a clear
// failure, never a leak).
TEST(BuildContextSecrets, EnvUriIsResolvedFromTheEnvironment) {
    ::setenv("CLINK_TEST_SECRET", "hunter2", /*overwrite=*/1);
    ::unsetenv("CLINK_TEST_UNSET");

    clink::plugin::BuildContext ctx;
    ctx.params["password"] = "env://CLINK_TEST_SECRET";
    ctx.params["plain"] = "literal-value";
    ctx.params["missing"] = "env://CLINK_TEST_UNSET";

    EXPECT_EQ(ctx.param_or("password"), "hunter2");             // resolved from the env
    EXPECT_EQ(ctx.param_or("plain"), "literal-value");          // non-env value verbatim
    EXPECT_EQ(ctx.param_or("missing"), "");                     // unset -> empty, not a leak
    EXPECT_EQ(ctx.param_or("absent", "fallback"), "fallback");  // fallback path intact

    EXPECT_EQ(clink::plugin::BuildContext::resolve_secret("env://CLINK_TEST_SECRET"), "hunter2");
    EXPECT_EQ(clink::plugin::BuildContext::resolve_secret("not-a-secret"), "not-a-secret");

    ::unsetenv("CLINK_TEST_SECRET");
}
