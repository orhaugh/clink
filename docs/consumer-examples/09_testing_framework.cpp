// 09 - Testing a stateful operator with clink::test_support.
//
// Demonstrates the PUBLIC testing framework (docs/internals/testing-framework.md)
// that ships as the installed `clink::test_support` target. A consumer links it
// alongside `clink::core` and drives its own operators through a harness - no
// cluster, no JM, no gtest dependency (this file is a plain main() that returns
// non-zero if any check fails, so CTest or CI can gate on it).
//
// What it shows:
//   * a keyed stateful KeyedProcessFunction (count purchases per user, emit on
//     an event-time timer),
//   * the keyed harness driving elements + watermarks and firing key-scoped
//     timers,
//   * inspecting per-key state through the production read path,
//   * a snapshot -> restore-into-a-fresh-harness round trip proving the state
//     and timers survive a checkpoint.
//
// Build:  cmake --build build --target 09_testing_framework
// Run:    ./build/09_testing_framework   (exit 0 = all checks passed)

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <clink/core/codec.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/test/keyed_harness.hpp>

namespace {

int g_failures = 0;

template <typename A, typename B>
void check_eq(const A& got, const B& want, const char* what, int line) {
    if (!(got == want)) {
        std::cerr << "FAIL (line " << line << "): " << what << "\n";
        ++g_failures;
    }
}
#define CHECK_EQ(got, want) check_eq((got), (want), #got " == " #want, __LINE__)

struct Purchase {
    std::string user;
    std::int64_t amount;
};

// Counts purchases per user in keyed state; each record arms an event-time
// timer 1000ms later that emits "<user>:<count>" when the watermark passes it.
class CountPerUser final : public clink::KeyedProcessFunction<std::string, Purchase, std::string> {
public:
    void open(clink::RuntimeContext& ctx) override {
        counts_.emplace(ctx.keyed_state<std::string, std::int64_t>(
            "count", clink::string_codec(), clink::int64_codec()));
    }
    void process_element(const Purchase& p,
                         clink::ProcessFunctionContext<std::string>& ctx,
                         clink::Collector<std::string>& /*out*/) override {
        counts_->put(p.user, counts_->get(p.user).value_or(0) + 1);
        const auto ts = ctx.timestamp() ? ctx.timestamp()->millis() : 0;
        ctx.timer_service()->register_event_time_timer(ts + 1000, p.user);
    }
    void on_timer(std::int64_t /*ts*/,
                  clink::OnTimerContext<std::string>& /*ctx*/,
                  clink::Collector<std::string>& out) override {
        out.collect(current_key() + ":" + std::to_string(counts_->get(current_key()).value_or(0)));
    }

private:
    std::optional<clink::KeyedState<std::string, std::int64_t>> counts_;
};

}  // namespace

int main() {
    namespace test = clink::test;
    auto key_fn = [](const Purchase& p) { return p.user; };
    auto timer_key_fn = [](const std::string& k) { return k; };

    auto h = test::make_keyed_process_function_harness(CountPerUser{}, key_fn, timer_key_fn);
    h.open();

    h.process_element(Purchase{"alice", 10}, 1000);
    h.process_element(Purchase{"bob", 20}, 1100);
    h.process_element(Purchase{"alice", 30}, 1200);

    // State is isolated per key and read through the production path.
    CHECK_EQ(h.state_value<std::int64_t>("alice", "count"), std::optional<std::int64_t>{2});
    CHECK_EQ(h.state_value<std::int64_t>("bob", "count"), std::optional<std::int64_t>{1});
    CHECK_EQ(h.state_value<std::int64_t>("carol", "count"), std::optional<std::int64_t>{});

    // Take a checkpoint, then diverge - the divergence must not leak into a
    // harness restored from the snapshot.
    const auto snap = h.snapshot(/*checkpoint_id=*/1);
    h.process_element(Purchase{"alice", 5}, 1300);
    CHECK_EQ(h.state_value<std::int64_t>("alice", "count"), std::optional<std::int64_t>{3});

    // A watermark at 2050 fires only alice's first timer (@2000); her later
    // timers (@2200 from the 1200 record, @2300 from the 1300 record) are not
    // yet due. The fire emits her current running count (3).
    h.process_watermark(2050);
    CHECK_EQ(h.output_values(), (std::vector<std::string>{"alice:3"}));

    // Restore a fresh harness from the checkpoint: state is back to snapshot
    // time (alice=2), and the restored timers fire with that restored state. At
    // snapshot time the pending timers were alice@2000, bob@2100, alice@2200;
    // a watermark at 3000 fires all three in timestamp order.
    auto h2 = test::make_keyed_process_function_harness(CountPerUser{}, key_fn, timer_key_fn);
    h2.restore_from(snap);
    h2.open();
    CHECK_EQ(h2.state_value<std::int64_t>("alice", "count"), std::optional<std::int64_t>{2});
    CHECK_EQ(h2.state_value<std::int64_t>("bob", "count"), std::optional<std::int64_t>{1});
    h2.process_watermark(3000);
    CHECK_EQ(h2.output_values(), (std::vector<std::string>{"alice:2", "bob:1", "alice:2"}));

    if (g_failures == 0) {
        std::cout << "09_testing_framework: all checks passed\n";
        return 0;
    }
    std::cerr << "09_testing_framework: " << g_failures << " check(s) failed\n";
    return 1;
}
