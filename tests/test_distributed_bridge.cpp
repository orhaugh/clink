#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/network_bridge.hpp"

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;

// Two LocalExecutors in two threads, separate DAGs, bridged by a single
// NetworkChannel:
//
//   Thread A (sender):
//     VectorSource<int64> → MapOperator (×2) → NetworkBridgeSink → TCP
//
//   Thread B (receiver):
//     TCP → NetworkBridgeSource → CollectingSink<int64>
//
// Validates that the bridge connectors plug into the existing DAG and
// LocalExecutor seamlessly: both halves are normal clink jobs.
TEST(Distributed, TwoDagsBridgedByNetworkChannel) {
    const std::vector<std::int64_t> input = {10, 20, 30, 40, 50};

    // Receiver first: bind the listener and capture the OS-assigned port
    // before the sender tries to connect.
    auto bridge_source =
        std::make_shared<NetworkBridgeSource<std::int64_t>>(/*port*/ 0, int64_codec());
    const std::uint16_t bridge_port = bridge_source->prepare_listen();

    auto sink = std::make_shared<CollectingSink<std::int64_t>>();

    Dag receiver_dag;
    auto h_src = receiver_dag.add_source<std::int64_t>(bridge_source);
    receiver_dag.add_sink<std::int64_t>(h_src, sink);

    LocalExecutor receiver_exec(std::move(receiver_dag));
    receiver_exec.start();

    // Sender side runs on its own thread.
    std::thread sender_thread([input, bridge_port] {
        std::vector<Record<std::int64_t>> records;
        records.reserve(input.size());
        for (auto v : input) {
            records.emplace_back(Record<std::int64_t>{v, EventTime{v * 10}});
        }

        auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
        auto doubler = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
            [](const std::int64_t& v) { return v * 2; }, "doubler");
        auto bridge_sink = std::make_shared<NetworkBridgeSink<std::int64_t>>(
            "127.0.0.1", bridge_port, int64_codec());

        Dag sender_dag;
        auto h0 = sender_dag.add_source<std::int64_t>(src);
        auto h1 = sender_dag.add_operator<std::int64_t, std::int64_t>(h0, doubler);
        sender_dag.add_sink<std::int64_t>(h1, bridge_sink);

        LocalExecutor sender_exec(std::move(sender_dag));
        sender_exec.run();  // blocks until source is exhausted + bridge close
    });

    sender_thread.join();
    receiver_exec.await_termination();

    // Doubled values should appear at the receiver's sink in order.
    auto got = sink->collected();
    EXPECT_EQ(got, (std::vector<std::int64_t>{20, 40, 60, 80, 100}));

    // The watermark::max emitted by VectorSource at end-of-stream propagates
    // through the bridge and reaches the receiver's sink.
    EXPECT_EQ(sink->last_watermark(), Watermark::max());
}

// Connection refused is a likely real-world failure mode: the receiver
// hasn't started, has crashed, or is on the wrong port. The bridge
// surfaces this as an operator error via the executor's normal
// exception-capture path; no silent swallow.
TEST(Distributed, BridgeReportsErrorWhenReceiverNotListening) {
    // Find an unused port: bind, capture, immediately release. Tiny race
    // window but acceptable for a unit test.
    std::uint16_t dead_port = 0;
    {
        NetworkChannelSource<std::int64_t> probe(0, int64_codec());
        dead_port = probe.listen();
    }
    // Probe destructor releases the port; nothing is now listening on it.

    std::vector<Record<std::int64_t>> records;
    records.emplace_back(Record<std::int64_t>{1});
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
    auto bridge_sink = std::make_shared<NetworkBridgeSink<std::int64_t>>(
        "127.0.0.1", dead_port, int64_codec(), "dead_bridge");

    Dag dag;
    auto h0 = dag.add_source<std::int64_t>(src);
    dag.add_sink<std::int64_t>(h0, bridge_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();  // does not throw - error captured

    const auto errors = exec.operator_errors();
    ASSERT_FALSE(errors.empty());
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (op_name == "dead_bridge" && msg.find("connect failed") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// Listening on a port that's already bound must fail loudly so the
// operator runner records an error - silently grabbing a different port
// or hanging would leave the peer connecting to the wrong place.
TEST(Distributed, BridgeListenFailsWhenPortIsAlreadyBound) {
    auto first = std::make_shared<NetworkBridgeSource<std::int64_t>>(0, int64_codec());
    const std::uint16_t port = first->prepare_listen();

    NetworkBridgeSource<std::int64_t> conflict(port, int64_codec());
    EXPECT_THROW(conflict.prepare_listen(), std::runtime_error);
}

// A second test: the bridge survives a barrier passing through. Confirms
// that checkpoint barriers cross the wire faithfully and reach the
// receiving sink's on_barrier hook.
TEST(Distributed, BridgeForwardsCheckpointBarrier) {
    auto bridge_source =
        std::make_shared<NetworkBridgeSource<std::int64_t>>(/*port*/ 0, int64_codec());
    const std::uint16_t bridge_port = bridge_source->prepare_listen();

    auto sink = std::make_shared<CollectingSink<std::int64_t>>();

    Dag receiver_dag;
    auto h_src = receiver_dag.add_source<std::int64_t>(bridge_source);
    receiver_dag.add_sink<std::int64_t>(h_src, sink);

    LocalExecutor receiver_exec(std::move(receiver_dag));
    receiver_exec.start();

    // Custom source that emits one batch, one barrier, then closes.
    class BarrierCarryingSource final : public Source<std::int64_t> {
    public:
        bool produce(Emitter<std::int64_t>& out) override {
            if (this->cancelled() || step_ > 1) {
                return false;
            }
            if (step_ == 0) {
                Batch<std::int64_t> b;
                b.emplace(7);
                out.emit_data(std::move(b));
                ++step_;
                return true;
            }
            out.emit_barrier(CheckpointBarrier{CheckpointId{99}});
            ++step_;
            return false;
        }
        std::string name() const override { return "barrier_source"; }

    private:
        int step_{0};
    };

    std::thread sender_thread([bridge_port] {
        auto src = std::make_shared<BarrierCarryingSource>();
        auto bridge_sink = std::make_shared<NetworkBridgeSink<std::int64_t>>(
            "127.0.0.1", bridge_port, int64_codec());
        Dag sender_dag;
        auto h0 = sender_dag.add_source<std::int64_t>(src);
        sender_dag.add_sink<std::int64_t>(h0, bridge_sink);

        LocalExecutor sender_exec(std::move(sender_dag));
        sender_exec.run();
    });

    sender_thread.join();
    receiver_exec.await_termination();

    EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{7}));
    EXPECT_EQ(sink->last_barrier().id().value(), 99u);
}
