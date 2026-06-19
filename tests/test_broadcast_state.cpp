#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/broadcast_state.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// BroadcastState typed-view round-trip
// ---------------------------------------------------------------------------

TEST(BroadcastState, PutGetEraseRoundTrip) {
    InMemoryStateBackend backend;
    BroadcastState<std::string> bs(backend, OperatorId{1}, "config", string_codec());

    EXPECT_FALSE(bs.get().has_value());
    bs.put("v1");
    auto v = bs.get();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "v1");

    bs.put("v2");  // overwrite
    v = bs.get();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "v2");

    bs.erase();
    EXPECT_FALSE(bs.get().has_value());
}

TEST(BroadcastState, SlotNamespacingPreventsCollisions) {
    InMemoryStateBackend backend;
    BroadcastState<std::string> a(backend, OperatorId{1}, "slot_a", string_codec());
    BroadcastState<std::string> b(backend, OperatorId{1}, "slot_b", string_codec());

    a.put("alpha");
    b.put("beta");

    EXPECT_EQ(*a.get(), "alpha");
    EXPECT_EQ(*b.get(), "beta");
}

// ---------------------------------------------------------------------------
// End-to-end: feature-flag pipeline via broadcast_connect
// ---------------------------------------------------------------------------

namespace {

// A main source that delays its emission slightly so the broadcast stream
// has time to populate the operator's broadcast state before main records
// arrive. This makes the test deterministic without rendezvous primitives.
class DelayedStringSource final : public Source<std::string> {
public:
    explicit DelayedStringSource(std::vector<std::string> records,
                                 std::string n,
                                 std::chrono::milliseconds delay)
        : records_(std::move(records)), name_(std::move(n)), delay_(delay) {}

    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        std::this_thread::sleep_for(delay_);
        Batch<std::string> b;
        for (const auto& s : records_) {
            b.emplace(s);
        }
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark::max());
        done_ = true;
        return false;
    }

    std::string name() const override { return name_; }

private:
    std::vector<std::string> records_;
    std::string name_;
    std::chrono::milliseconds delay_;
    bool done_{false};
};

}  // namespace

TEST(BroadcastConnect, FilterMainStreamByBroadcastedBlocklist) {
    Dag dag;

    // Convert raw strings to Records for sources.
    auto records = [](std::vector<std::string> v) {
        std::vector<Record<std::string>> out;
        out.reserve(v.size());
        for (auto& s : v) {
            out.emplace_back(std::move(s));
        }
        return out;
    };

    // Broadcast stream: a single update that adds "alice" to the blocklist.
    // We use VectorSource (emits immediately) - broadcast settles before
    // the delayed main source fires.
    auto src_brod = std::make_shared<VectorSource<std::string>>(records({"alice"}), "broadcast");

    // Main stream: three users, two of which are not blocked. Wait 50ms
    // before emitting so the broadcast update is already in state.
    auto src_main = std::make_shared<DelayedStringSource>(
        std::vector<std::string>{"alice", "bob", "carol"}, "main", 50ms);

    auto h_brod = dag.add_source<std::string>(src_brod);
    auto h_main = dag.add_source<std::string>(src_main);

    auto h_filt = dag.broadcast_connect<std::string, std::string, std::string, std::string>(
        h_main,
        h_brod,
        // on_broadcast: overwrite the single blocked-user slot.
        [](const std::string& user, BroadcastState<std::string>& s) { s.put(user); },
        // on_main: emit iff user differs from current blocked-user.
        [](const std::string& user, BroadcastState<std::string>& s) -> std::optional<std::string> {
            auto blocked = s.get();
            if (blocked.has_value() && *blocked == user) {
                return std::nullopt;
            }
            return user;
        },
        string_codec(),
        "blocked_user",
        "filter_blocked");

    auto sink = std::make_shared<CollectingSink<std::string>>();
    dag.add_sink<std::string>(h_filt, sink);

    // Capture the broadcast_connect operator's id before moving the dag
    // into the executor.
    const OperatorId broadcast_op_id = dag.runners()[h_filt.runner_index].id;

    auto backend = std::make_shared<InMemoryStateBackend>();
    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto got = sink->collected();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"bob", "carol"}));

    // The state backend should still hold the broadcast value at end of run.
    BroadcastState<std::string> verify(*backend, broadcast_op_id, "blocked_user", string_codec());
    auto residual = verify.get();
    ASSERT_TRUE(residual.has_value());
    EXPECT_EQ(*residual, "alice");
}

TEST(BroadcastConnect, ThrowsWhenStateBackendMissing) {
    Dag dag;
    auto src_brod = std::make_shared<VectorSource<std::string>>(std::vector<Record<std::string>>{},
                                                                "broadcast");
    auto src_main =
        std::make_shared<VectorSource<std::string>>(std::vector<Record<std::string>>{}, "main");
    auto h_brod = dag.add_source<std::string>(src_brod);
    auto h_main = dag.add_source<std::string>(src_main);

    auto h_filt = dag.broadcast_connect<std::string, std::string, std::string, std::string>(
        h_main,
        h_brod,
        [](const std::string&, BroadcastState<std::string>&) {},
        [](const std::string& s, BroadcastState<std::string>&) -> std::optional<std::string> {
            return s;
        },
        string_codec());
    auto sink = std::make_shared<CollectingSink<std::string>>();
    dag.add_sink<std::string>(h_filt, sink);

    LocalExecutor exec(std::move(dag));  // no state backend
    exec.run();

    // The operator-thread exception should have been captured rather than
    // terminating the process. Look for the broadcast_connect's error.
    auto errors = exec.operator_errors();
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (op_name == "broadcast_connect" &&
            msg.find("requires JobConfig::state_backend") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "expected broadcast_connect to record a state-backend error";
}
