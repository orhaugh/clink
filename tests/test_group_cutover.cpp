// The upstream hold-and-swap (hot rescale, design record 008, increment 3).
//
// An output group feeding a rescale-eligible operator is built with one
// branch per unit of the downstream's max_parallelism; branches above the
// live count are parked. At the armed cutover barrier C the split broadcasts
// C and HOLDS - nothing may be routed with the old divisor past C - while
// each branch sink reports "flushed" once C has been pushed to its peer.
// The control side (the worker, played here by the test) then swaps the
// branch endpoints to the post-cutover peers, installs the new live count,
// and releases; the split re-broadcasts its last watermark so the fresh
// peers start with the group's event-time position.
//
// Everything is scripted: the source emits the barrier inline, the hold is
// proven by the flushed streams ending at exactly C, and the swap is proven
// by where the post-release records land. No step waits on time.
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/cutover_gate.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/local_data_plane.hpp"
#include "clink/runtime/network/network_bridge.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

struct CutoverStepGate {
    std::mutex mu;
    std::condition_variable cv;
    bool open{false};
    void signal() {
        {
            std::lock_guard lock(mu);
            open = true;
        }
        cv.notify_all();
    }
    [[nodiscard]] bool await(std::chrono::seconds bound = std::chrono::seconds{30}) {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, bound, [&] { return open; });
    }
};

// Emits: keys 0..7, watermark 100, barrier C(=1), then IMMEDIATELY keys
// 8..15 - the post-cutover records are in flight against the held split,
// so only the hold keeps them off the old peers. Then parks until the test
// has asserted, then end-of-input.
class CutoverScriptedSource final : public Source<std::int64_t> {
public:
    CutoverScriptedSource(CutoverStepGate& fed_all, CutoverStepGate& resume)
        : fed_all_(fed_all), resume_(resume) {}

    bool produce(Emitter<std::int64_t>& out) override {
        if (this->cancelled()) {
            return false;
        }
        const int call = ++calls_;
        if (call <= 8) {
            emit_one_(out, call - 1);
            return true;
        }
        if (call == 9) {
            out.emit_watermark(Watermark{EventTime{100}});
            return true;
        }
        if (call == 10) {
            out.emit_barrier(CheckpointBarrier{CheckpointId{1}});
            return true;
        }
        if (call <= 18) {
            emit_one_(out, call - 3);  // 8..15, queued behind the held split
            return true;
        }
        if (call == 19) {
            fed_all_.signal();
            (void)resume_.await();
            return true;
        }
        return false;
    }

    std::string name() const override { return name_; }
    void set_name(std::string n) { name_ = std::move(n); }

private:
    void emit_one_(Emitter<std::int64_t>& out, std::int64_t v) {
        Batch<std::int64_t> b;
        b.emplace(v);
        out.emit_data(std::move(b));
    }
    CutoverStepGate& fed_all_;
    CutoverStepGate& resume_;
    std::atomic<int> calls_{0};
    std::string name_{"cutover.source"};
};

struct PeerEvent {
    enum class Kind { Data, Barrier, Watermark } kind;
    std::int64_t value{0};
};

// One in-process peer: a listening relay whose received stream can be
// drained on demand. Receives over the LocalDataPlane fast path.
struct Peer {
    std::shared_ptr<network::NetworkBridgeSource<std::int64_t>> relay;
    std::uint16_t port{0};

    explicit Peer(const std::string& name)
        : relay(std::make_shared<network::NetworkBridgeSource<std::int64_t>>(
              0, int64_codec(), name)) {
        port = relay->prepare_listen();
    }

    // Drain everything currently relayable. The feeder side must have
    // half-closed (or be held) for this to terminate: pop() blocks on an
    // open-and-quiet channel, so callers only drain peers whose upstream
    // has closed - which the choreography guarantees at the points the
    // test drains.
    std::vector<PeerEvent> drain() {
        relay->open();
        auto ch = std::make_shared<BoundedChannel<StreamElement<std::int64_t>>>(256);
        Emitter<std::int64_t> em(ch.get());
        while (relay->produce(em)) {
        }
        std::vector<PeerEvent> out;
        while (auto e = ch->try_pop()) {
            if (e->is_data()) {
                for (const auto& rec : e->as_data()) {
                    out.push_back({PeerEvent::Kind::Data, rec.value()});
                }
            } else if (e->is_barrier()) {
                out.push_back({PeerEvent::Kind::Barrier,
                               static_cast<std::int64_t>(e->as_barrier().id().value())});
            } else if (e->is_watermark()) {
                out.push_back({PeerEvent::Kind::Watermark, e->as_watermark().timestamp().millis()});
            }
        }
        return out;
    }
};

std::string cutover_tag() {
    static std::atomic<unsigned> seq{0};
    return "cut" + std::to_string(seq.fetch_add(1));
}

}  // namespace

TEST(GroupCutover, TheSplitHoldsAtTheArmedBarrierAndResumesOnTheNewPeerSet) {
    network::LocalDataPlane::instance().clear_for_testing();
    const auto tag = cutover_tag();

    // Old peers (live = 2) and new peers (live = 4 after the swap).
    Peer a0("cut.a0." + tag), a1("cut.a1." + tag);
    Peer b0("cut.b0." + tag), b1("cut.b1." + tag), b2("cut.b2." + tag), b3("cut.b3." + tag);

    auto gate = std::make_shared<GroupCutoverGate>(2);
    constexpr std::size_t kMaxBranches = 4;

    CutoverStepGate fed_all;
    CutoverStepGate resume;
    auto src = std::make_shared<CutoverScriptedSource>(fed_all, resume);
    src->set_name("cutover.source." + tag);

    Dag dag;
    auto h0 = dag.add_source<std::int64_t>(src);
    // Selector reads the gate's live divisor - the whole point of the swap.
    auto branches = dag.add_split<std::int64_t>(
        h0,
        [gate](const std::int64_t& v) {
            return static_cast<int>(v % static_cast<std::int64_t>(gate->live()));
        },
        kMaxBranches,
        "cutover.split." + tag,
        {},
        gate);
    std::vector<std::shared_ptr<network::SwappableBridgeSink<std::int64_t>>> sinks;
    for (std::size_t i = 0; i < kMaxBranches; ++i) {
        std::optional<network::SwappableBridgeSink<std::int64_t>::Endpoint> ep;
        if (i == 0) {
            ep = {"127.0.0.1", a0.port};
        } else if (i == 1) {
            ep = {"127.0.0.1", a1.port};
        }
        auto sink = std::make_shared<network::SwappableBridgeSink<std::int64_t>>(
            int64_codec(),
            ArrowBatcher<std::int64_t>{},
            ep,
            gate,
            "cutover.branch" + std::to_string(i) + "." + tag);
        sinks.push_back(sink);
        dag.add_sink<std::int64_t>(branches[i], sink);
    }
    EXPECT_TRUE(sinks[2]->parked());
    EXPECT_TRUE(sinks[3]->parked());

    // Armed BEFORE the executor starts: the choreography's rule 1 is that
    // the arm precedes the barrier, and here that ordering is structural
    // rather than raced.
    ASSERT_TRUE(gate->arm(1));

    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    // Everything - including the post-cutover keys 8..15 - is now inside
    // the task: pre-C records at the old peers, C broadcast, the rest
    // queued against the held split. Only the hold keeps them there.
    ASSERT_TRUE(fed_all.await()) << "the source never fed its full script";

    // Every branch flushes C - including the parked ones, whose streams are
    // trivially at C. This wait is the worker's precondition for swapping.
    ASSERT_TRUE(gate->await_all_flushed(kMaxBranches, 30s, nullptr))
        << "not every branch pushed the armed barrier: flushed=" << gate->flushed_count();

    // With the split held and the branches flushed, the OLD peers' streams
    // end at exactly C. Their feeder sinks are about to be closed by the
    // swap, which half-closes the local channels and lets drain terminate.
    sinks[0]->swap({{"127.0.0.1", b0.port}});
    sinks[1]->swap({{"127.0.0.1", b1.port}});
    sinks[2]->swap({{"127.0.0.1", b2.port}});
    sinks[3]->swap({{"127.0.0.1", b3.port}});

    const auto a0_events = a0.drain();
    const auto a1_events = a1.drain();
    auto ends_at_barrier = [](const std::vector<PeerEvent>& ev) {
        return !ev.empty() && ev.back().kind == PeerEvent::Kind::Barrier && ev.back().value == 1;
    };
    EXPECT_TRUE(ends_at_barrier(a0_events))
        << "old peer a0's stream does not end at the cutover barrier";
    EXPECT_TRUE(ends_at_barrier(a1_events))
        << "old peer a1's stream does not end at the cutover barrier";
    std::vector<std::int64_t> a0_keys, a1_keys;
    for (const auto& e : a0_events) {
        if (e.kind == PeerEvent::Kind::Data) {
            a0_keys.push_back(e.value);
        }
    }
    for (const auto& e : a1_events) {
        if (e.kind == PeerEvent::Kind::Data) {
            a1_keys.push_back(e.value);
        }
    }
    EXPECT_EQ(a0_keys, (std::vector<std::int64_t>{0, 2, 4, 6}));
    EXPECT_EQ(a1_keys, (std::vector<std::int64_t>{1, 3, 5, 7}));

    // Release with the new divisor and let the source finish.
    gate->release(4);
    resume.signal();
    exec.await_termination();

    // Post-release records route modulo 4 onto the NEW peers, each of which
    // first received the re-broadcast watermark.
    const std::vector<Peer*> new_peers{&b0, &b1, &b2, &b3};
    for (std::size_t i = 0; i < new_peers.size(); ++i) {
        const auto events = new_peers[i]->drain();
        ASSERT_FALSE(events.empty()) << "new peer b" << i << " received nothing";
        EXPECT_EQ(events.front().kind, PeerEvent::Kind::Watermark)
            << "new peer b" << i
            << " did not receive the re-broadcast watermark before its first record, so its "
               "downstream event-time position would stall until the next organic watermark";
        EXPECT_EQ(events.front().value, 100);
        std::vector<std::int64_t> keys;
        for (const auto& e : events) {
            if (e.kind == PeerEvent::Kind::Data) {
                keys.push_back(e.value);
                EXPECT_EQ(static_cast<std::size_t>(e.value % 4), i)
                    << "a post-cutover record landed on the wrong peer for its key";
                EXPECT_GE(e.value, 8) << "a pre-cutover record leaked to a new peer";
            }
            EXPECT_NE(e.kind, PeerEvent::Kind::Barrier)
                << "no barrier may reach a new peer here: C went to the old set and the "
                   "checkpoint clock is paused during the window";
        }
        EXPECT_EQ(keys.size(), 2u) << "keys 8..15 modulo 4 give each new peer exactly two";
    }
}

TEST(GroupCutover, AnAbortReleasesTheHeldSplitToWindDown) {
    network::LocalDataPlane::instance().clear_for_testing();
    const auto tag = cutover_tag();

    Peer a0("cutabort.a0." + tag);

    auto gate = std::make_shared<GroupCutoverGate>(1);
    CutoverStepGate fed_all;
    CutoverStepGate resume;
    // The script parks after feeding everything; on abort the split breaks
    // and closes its branches, and the source's next produce hits a closed
    // channel - the executor tears down rather than resuming.
    auto src = std::make_shared<CutoverScriptedSource>(fed_all, resume);
    src->set_name("cutabort.source." + tag);

    Dag dag;
    auto h0 = dag.add_source<std::int64_t>(src);
    auto branches = dag.add_split<std::int64_t>(
        h0, [](const std::int64_t&) { return 0; }, 1, "cutabort.split." + tag, {}, gate);
    auto sink = std::make_shared<network::SwappableBridgeSink<std::int64_t>>(
        int64_codec(),
        ArrowBatcher<std::int64_t>{},
        network::SwappableBridgeSink<std::int64_t>::Endpoint{"127.0.0.1", a0.port},
        gate,
        "cutabort.branch0." + tag);
    dag.add_sink<std::int64_t>(branches[0], sink);

    ASSERT_TRUE(gate->arm(1));
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    ASSERT_TRUE(fed_all.await());
    ASSERT_TRUE(gate->await_all_flushed(1, 30s, nullptr));

    // Abort instead of swapping: the held split must wind down cleanly, not
    // resume routing and not throw.
    gate->abort();
    resume.signal();  // let the parked source proceed into the closed split
    exec.cancel();
    exec.await_termination();

    sink->close();
    const auto events = a0.drain();
    for (std::size_t i = 1; i < events.size(); ++i) {
        EXPECT_FALSE(events[i - 1].kind == PeerEvent::Kind::Barrier && events[i - 1].value == 1)
            << "an element followed the cutover barrier on an aborted hold";
    }
}

TEST(GroupCutover, TheGatePinsItsOwnBookkeeping) {
    GroupCutoverGate gate(3);
    EXPECT_EQ(gate.live(), 3u);

    // Arming: once at a time, never for checkpoint 0.
    EXPECT_FALSE(gate.arm(0));
    EXPECT_TRUE(gate.arm(7));
    EXPECT_FALSE(gate.arm(8)) << "a second cutover was armed over a live one";
    EXPECT_TRUE(gate.is_armed_for(7));
    EXPECT_FALSE(gate.is_armed_for(8));

    // Flushes count only for the armed id.
    gate.mark_branch_flushed(6);
    EXPECT_EQ(gate.flushed_count(), 0u);
    gate.mark_branch_flushed(7);
    gate.mark_branch_flushed(7);
    EXPECT_EQ(gate.flushed_count(), 2u);
    EXPECT_TRUE(gate.await_all_flushed(2, std::chrono::milliseconds{50}, nullptr));
    EXPECT_FALSE(gate.await_all_flushed(3, std::chrono::milliseconds{50}, nullptr))
        << "await_all_flushed reported more branches flushed than marked";

    // Release installs the divisor and disarms.
    gate.release(6);
    EXPECT_EQ(gate.live(), 6u);
    EXPECT_FALSE(gate.is_armed_for(7));
    EXPECT_TRUE(gate.arm(9)) << "the gate did not re-arm after release";
}
