#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/wire.hpp"

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#endif

using namespace clink;
using namespace clink::network;

// End-to-end TCP transport. The source listens on an OS-assigned port,
// the sink connects from a worker thread, the sink sends a heterogeneous
// stream (data + watermark + barrier + more data + close), and the source
// pops each frame and verifies the bytes round-trip with full fidelity.
TEST(NetworkChannel, RoundTripsMixedStreamElements) {
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();

        Batch<std::int64_t> first;
        first.emplace(1, EventTime{100});
        first.emplace(2, EventTime{200});
        first.emplace(3, EventTime{300});
        sink.push(StreamElement<std::int64_t>::data(std::move(first)));

        sink.push(StreamElement<std::int64_t>::watermark(Watermark{EventTime{500}}));
        sink.push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{CheckpointId{7}}));

        Batch<std::int64_t> second;
        second.emplace(42);  // no event time
        sink.push(StreamElement<std::int64_t>::data(std::move(second)));

        sink.close_send();
    });

    source.accept();

    // Frame 1: data batch with 3 timestamped records.
    auto e1 = source.pop();
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(e1->is_data());
    {
        const auto& b = e1->as_data();
        ASSERT_EQ(b.size(), 3u);
        EXPECT_EQ(b[0].value(), 1);
        ASSERT_TRUE(b[0].event_time().has_value());
        EXPECT_EQ(b[0].event_time()->millis(), 100);
        EXPECT_EQ(b[2].value(), 3);
        EXPECT_EQ(b[2].event_time()->millis(), 300);
    }

    // Frame 2: watermark.
    auto e2 = source.pop();
    ASSERT_TRUE(e2.has_value());
    ASSERT_TRUE(e2->is_watermark());
    EXPECT_EQ(e2->as_watermark().timestamp().millis(), 500);

    // Frame 3: barrier. Default-constructed barriers carry Mode::Aligned.
    auto e3 = source.pop();
    ASSERT_TRUE(e3.has_value());
    ASSERT_TRUE(e3->is_barrier());
    EXPECT_EQ(e3->as_barrier().id().value(), 7u);
    EXPECT_EQ(e3->as_barrier().mode(), CheckpointBarrier::Mode::Aligned);

    // Frame 4: data batch with one untimestamped record.
    auto e4 = source.pop();
    ASSERT_TRUE(e4.has_value());
    ASSERT_TRUE(e4->is_data());
    {
        const auto& b = e4->as_data();
        ASSERT_EQ(b.size(), 1u);
        EXPECT_EQ(b[0].value(), 42);
        EXPECT_FALSE(b[0].event_time().has_value());
    }

    // Frame 5: close - pop returns nullopt and closed() flips true.
    auto e5 = source.pop();
    EXPECT_FALSE(e5.has_value());
    EXPECT_TRUE(source.closed());

    sender.join();
}

TEST(NetworkChannel, BindsToAllInterfacesWhenAsked) {
    // Verifies that NetworkChannelSource accepts a 0.0.0.0 bind_host and
    // still answers loopback connections - the foundation for multi-
    // machine deployment.
    NetworkChannelSource<std::int64_t> source(/*port*/ 0,
                                              int64_codec(),
                                              /*bind_host*/ "0.0.0.0");
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();
        Batch<std::int64_t> b;
        b.emplace(42);
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.close_send();
    });

    source.accept();
    auto e = source.pop();
    ASSERT_TRUE(e.has_value());
    ASSERT_TRUE(e->is_data());
    EXPECT_EQ(e->as_data()[0].value(), 42);
    sender.join();
}

#ifdef CLINK_HAS_ARROW
// Regression test for the cluster-path schema-mismatch bug fixed
// 2026-05-15. Previously, the cluster's attach_group_output<T> built
// NetworkBridgeSink with the codec-only ctor (binary-fallback batcher)
// while the receiver's TypeRegistry handed back a columnar batcher.
// Sender and receiver Arrow schemas disagreed, and the receiver's
// parse() did static_cast<Int64Array*> on a BinaryArray column - UB
// that happened to read correct int64 values due to coincidental
// buffer layout overlap.
//
// Now the source-side schema check (network_channel.hpp's pop()
// dispatch under Kind::ArrowBatch) catches this and returns nullopt
// (treated as a malformed frame) rather than letting the downcast
// produce silently-corrupt records. This test exercises the
// asymmetric case explicitly.
TEST(NetworkChannel, MismatchedBatcherIsRejectedCleanly) {
    // Schema validation lives on the wire-protocol path; force the
    // socket round-trip so the receiver's batcher actually parses
    // bytes from the sender's schema (vs. typed direct push on the
    // LocalDataPlane fast path, which has no schema concept).
    clink::network::ScopedDisableLocalDataPlane no_local;
    NetworkChannelSource<std::int64_t> source(
        /*port*/ 0,
        int64_codec(),
        // Receiver expects the columnar batcher (schema:
        // {event_time:int64(null), value:int64}).
        int64_arrow_batcher());
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        // Sender uses the binary-fallback batcher (schema:
        // {event_time:int64(null), value_bytes:binary}). Mismatch.
        NetworkChannelSink<std::int64_t> sink(
            "127.0.0.1",
            port,
            int64_codec(),
            make_default_arrow_batcher<std::int64_t>(int64_codec()));
        sink.connect();
        Batch<std::int64_t> b;
        b.emplace(42, EventTime{100});
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.close_send();
    });

    source.accept();
    // First pop sees a frame, fails the schema check, returns nullopt.
    // Subsequent pops see Close, also nullopt. closed() becomes true.
    auto e1 = source.pop();
    EXPECT_FALSE(e1.has_value()) << "schema mismatch must be rejected, not parsed";
    sender.join();
}
#endif

#ifdef CLINK_HAS_ARROW
// Lock in the wire contract: data frames go out as Kind::ArrowBatch
// with a valid Arrow IPC stream payload. Reads the raw socket bytes
// rather than going through NetworkChannelSource so the assertion
// is on the actual bytes-on-the-wire layout.
TEST(NetworkChannel, DataFramesUseArrowIPCWithKindArrowBatch) {
    using namespace clink::network;

    // Raw TCP listener (no NetworkChannelSource - we want to see the
    // frame bytes directly).
    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);
    ASSERT_GT(port, 0u);

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink(
            "127.0.0.1", port, int64_codec(), int64_arrow_batcher());
        sink.connect();
        Batch<std::int64_t> b;
        b.emplace(11, EventTime{100});
        b.emplace(22, EventTime{200});
        b.emplace(33);  // no event-time
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.close_send();
    });

    const int peer_fd = NetworkSocket::accept_one(listener_fd);
    ASSERT_GE(peer_fd, 0);
    NetworkSocket::close(listener_fd);

    // The receiver's first action is to send the initial credit grant
    // (see NetworkChannelSource::accept). Without it, the sender's
    // push() would block on credit forever. Reproduce that here.
    {
        std::vector<std::byte> credit_payload;
        credit_payload.push_back(static_cast<std::byte>(Kind::CreditUpdate));
        put_u32_be(credit_payload, kInitialNetworkCredit);
        std::vector<std::byte> credit_header;
        put_u32_be(credit_header, static_cast<std::uint32_t>(credit_payload.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_header.data(), credit_header.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_payload.data(), credit_payload.size()));
    }

    // Read one frame: [u32 len][kind byte][...]
    std::array<std::byte, 4> hdr_buf{};
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, hdr_buf.data(), hdr_buf.size()));
    const std::uint32_t frame_len = read_u32_be(hdr_buf.data());
    ASSERT_GT(frame_len, 0u);
    std::vector<std::byte> body(frame_len);
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, body.data(), body.size()));

    // Kind byte == ArrowBatch (7), not the legacy Data (0).
    ASSERT_EQ(static_cast<Kind>(body[0]), Kind::ArrowBatch);

    // The rest of the body must be a valid Arrow IPC stream with the
    // int64 columnar schema {event_time:int64(null), value:int64}.
    auto record_batch = clink::arrow_batch_from_ipc(body.data() + 1, body.size() - 1);
    ASSERT_NE(record_batch, nullptr);
    ASSERT_EQ(record_batch->num_columns(), 2);
    EXPECT_EQ(record_batch->schema()->field(0)->name(), "event_time");
    EXPECT_EQ(record_batch->schema()->field(0)->type()->id(), arrow::Type::INT64);
    EXPECT_TRUE(record_batch->schema()->field(0)->nullable());
    EXPECT_EQ(record_batch->schema()->field(1)->name(), "value");
    EXPECT_EQ(record_batch->schema()->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(record_batch->num_rows(), 3);

    const auto* t_col = static_cast<const arrow::Int64Array*>(record_batch->column(0).get());
    const auto* v_col = static_cast<const arrow::Int64Array*>(record_batch->column(1).get());
    EXPECT_EQ(v_col->Value(0), 11);
    EXPECT_EQ(v_col->Value(1), 22);
    EXPECT_EQ(v_col->Value(2), 33);
    EXPECT_FALSE(t_col->IsNull(0));
    EXPECT_EQ(t_col->Value(0), 100);
    EXPECT_FALSE(t_col->IsNull(1));
    EXPECT_EQ(t_col->Value(1), 200);
    EXPECT_TRUE(t_col->IsNull(2));

    NetworkSocket::close(peer_fd);
    sender.join();
}

// Binary-fallback path: a type registered without a specialised
// ArrowBatcher rides Arrow IPC framing with a value_bytes:binary
// column carrying the existing Codec<T> output.
TEST(NetworkChannel, UnknownTypeUsesBinaryFallbackArrowBatcher) {
    using namespace clink::network;

    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);
    ASSERT_GT(port, 0u);

    std::thread sender([port] {
        // Codec-only ctor → NetworkChannelSink builds default arrow
        // batcher internally (value_bytes:binary schema).
        NetworkChannelSink<std::string> sink("127.0.0.1", port, string_codec());
        sink.connect();
        Batch<std::string> b;
        b.emplace("alpha");
        sink.push(StreamElement<std::string>::data(std::move(b)));
        sink.close_send();
    });

    const int peer_fd = NetworkSocket::accept_one(listener_fd);
    ASSERT_GE(peer_fd, 0);
    NetworkSocket::close(listener_fd);

    // Send initial credit grant so the sender's push doesn't block.
    {
        std::vector<std::byte> credit_payload;
        credit_payload.push_back(static_cast<std::byte>(Kind::CreditUpdate));
        put_u32_be(credit_payload, kInitialNetworkCredit);
        std::vector<std::byte> credit_header;
        put_u32_be(credit_header, static_cast<std::uint32_t>(credit_payload.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_header.data(), credit_header.size()));
        ASSERT_TRUE(NetworkSocket::send_all(peer_fd, credit_payload.data(), credit_payload.size()));
    }

    std::array<std::byte, 4> hdr_buf{};
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, hdr_buf.data(), hdr_buf.size()));
    const std::uint32_t frame_len = read_u32_be(hdr_buf.data());
    std::vector<std::byte> body(frame_len);
    ASSERT_TRUE(NetworkSocket::recv_all(peer_fd, body.data(), body.size()));
    ASSERT_EQ(static_cast<Kind>(body[0]), Kind::ArrowBatch);

    auto record_batch = clink::arrow_batch_from_ipc(body.data() + 1, body.size() - 1);
    ASSERT_NE(record_batch, nullptr);
    ASSERT_EQ(record_batch->num_columns(), 2);
    EXPECT_EQ(record_batch->schema()->field(1)->name(), "value_bytes");
    EXPECT_EQ(record_batch->schema()->field(1)->type()->id(), arrow::Type::BINARY);

    NetworkSocket::close(peer_fd);
    sender.join();
}
#endif  // CLINK_HAS_ARROW

TEST(NetworkChannel, ClosedConnectionPropagatesNullopt) {
    // Same protocol but the sender drops without sending Close - exercises
    // the connection-drop detection path (recv returns 0 → false → closed).
    NetworkChannelSource<std::string> source(/*port*/ 0, string_codec());
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        NetworkChannelSink<std::string> sink("127.0.0.1", port, string_codec());
        sink.connect();
        Batch<std::string> b;
        b.emplace("hello");
        sink.push(StreamElement<std::string>::data(std::move(b)));
        // Destructor closes the socket without a Close frame.
    });

    source.accept();

    auto e1 = source.pop();
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(e1->is_data());
    EXPECT_EQ(e1->as_data()[0].value(), "hello");

    auto e2 = source.pop();
    EXPECT_FALSE(e2.has_value());
    EXPECT_TRUE(source.closed());

    sender.join();
}

// Drain stream-element wire round-trip. New Kind::Drain
// frame carries (subtask_idx, target_parallelism) across the socket
// transport.
TEST(NetworkChannel, DrainMarkerRoundTripsAcrossWire) {
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();
        sink.push(StreamElement<std::int64_t>::drain(
            DrainMarker{.subtask_idx = 3, .target_parallelism = 8}));
        sink.close_send();
    });

    source.accept();
    auto e = source.pop();
    ASSERT_TRUE(e.has_value());
    ASSERT_TRUE(e->is_drain());
    EXPECT_EQ(e->as_drain().subtask_idx, 3u);
    EXPECT_EQ(e->as_drain().target_parallelism, 8u);

    auto eof = source.pop();
    EXPECT_FALSE(eof.has_value());
    sender.join();
}

// The bridge adapter (NetworkBridgeSource::produce) must FORWARD a wire-delivered
// drain marker, not route it into as_barrier() - the old else-branch did the
// latter and threw bad_variant_access, killing the cross-worker recv consumer the
// moment a rescale drained an upstream subtask on the sending worker. The channel-
// level round-trip above never goes through produce(); this exercises it.
TEST(NetworkBridge, ProduceForwardsDrainMarker) {
    NetworkBridgeSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.prepare_listen();

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();
        Batch<std::int64_t> b;
        b.emplace(7, EventTime{1});
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.push(StreamElement<std::int64_t>::drain(
            DrainMarker{.subtask_idx = 2, .target_parallelism = 4}));
        sink.close_send();
    });

    source.open();  // accept() the peer connection
    BoundedChannel<StreamElement<std::int64_t>> out_ch(64);
    Emitter<std::int64_t> em(&out_ch);

    ASSERT_TRUE(source.produce(em));  // the data record
    ASSERT_TRUE(source.produce(em));  // the drain marker - must not throw

    std::vector<StreamElement<std::int64_t>> got;
    while (auto e = out_ch.try_pop()) {
        got.push_back(std::move(*e));
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_TRUE(got[0].is_data());
    ASSERT_TRUE(got[1].is_drain()) << "bridge must forward the drain, not crash on it";
    EXPECT_EQ(got[1].as_drain().subtask_idx, 2u);
    EXPECT_EQ(got[1].as_drain().target_parallelism, 4u);

    source.cancel();
    sender.join();
}

// Alignment mode rides the Barrier and Terminal wire frames
// as one trailing byte after the 8-byte id. Both Aligned and Unaligned
// stamps must round-trip cleanly across the socket transport.
TEST(NetworkChannel, BarrierModeRoundTripsAcrossWire) {
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();

    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();

        sink.push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{
            CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned}));
        sink.push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{
            CheckpointId{2}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned}));
        // Terminal barriers carry mode too.
        sink.push(StreamElement<std::int64_t>::barrier(CheckpointBarrier{
            CheckpointId{3}, /*terminal=*/true, CheckpointBarrier::Mode::Unaligned}));
        sink.close_send();
    });

    source.accept();

    auto e1 = source.pop();
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(e1->is_barrier());
    EXPECT_EQ(e1->as_barrier().id().value(), 1u);
    EXPECT_EQ(e1->as_barrier().mode(), CheckpointBarrier::Mode::Aligned);
    EXPECT_FALSE(e1->as_barrier().is_terminal());

    auto e2 = source.pop();
    ASSERT_TRUE(e2.has_value());
    ASSERT_TRUE(e2->is_barrier());
    EXPECT_EQ(e2->as_barrier().id().value(), 2u);
    EXPECT_EQ(e2->as_barrier().mode(), CheckpointBarrier::Mode::Unaligned);

    auto e3 = source.pop();
    ASSERT_TRUE(e3.has_value());
    ASSERT_TRUE(e3->is_barrier());
    EXPECT_EQ(e3->as_barrier().id().value(), 3u);
    EXPECT_EQ(e3->as_barrier().mode(), CheckpointBarrier::Mode::Unaligned);
    EXPECT_TRUE(e3->as_barrier().is_terminal());

    auto e_eof = source.pop();
    EXPECT_FALSE(e_eof.has_value());
    sender.join();
}

// Per-operator byte attribution: when the bridge points the channel at an
// operator id + the host registry (set_op_bytes_target), the serialised wire
// bytes land on clink_op_bytes_sent_total{op_id} (sink) and
// clink_op_bytes_received_total{op_id} (source), alongside the per-process
// counters. This is what the per-operator overlay reads.
TEST(NetworkChannel, PerOperatorBytesAttributed) {
    using namespace clink::metrics;
    auto& reg = MetricsRegistry::global();
    const std::uint64_t op = 0xB17E5u;  // unique to this test
    const auto sent_name = op_metric_name("bytes_sent_total", op);
    const auto recv_name = op_metric_name("bytes_received_total", op);
    const auto sent_before = reg.counter(sent_name).value();
    const auto recv_before = reg.counter(recv_name).value();

    // Force the cross-process socket+serde path; per-op bytes are only counted
    // at the serialising boundary (same-process colocated subtasks take the
    // LocalDataPlane fast path, where per-op bytes are correctly absent).
    LocalDataPlane::instance().set_enabled(false);

    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    source.set_op_bytes_target(&reg, op);  // before accept() spawns the recv thread
    const std::uint16_t port = source.listen();

    std::thread sender([port, &reg, op] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.set_op_bytes_target(&reg, op);  // before any send
        sink.connect();
        Batch<std::int64_t> b;
        for (std::int64_t i = 0; i < 100; ++i) {
            b.emplace(i);
        }
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.close_send();
    });

    source.accept();
    while (source.pop().has_value()) {
        // drain until the Close frame
    }
    sender.join();

    LocalDataPlane::instance().set_enabled(true);  // restore for other tests

    EXPECT_GT(reg.counter(sent_name).value() - sent_before, 0u)
        << "sink should attribute serialised bytes to the operator";
    EXPECT_GT(reg.counter(recv_name).value() - recv_before, 0u)
        << "source should attribute received bytes to the operator";
}

// Security: a frame header is attacker-controllable, so an unbounded
// vector<byte>(frame_len) is a memory-amplification DoS - a peer that claims
// 4 GiB makes the reader allocate 4 GiB and OOM the Coordinator/Worker.
// wire.hpp caps the frame length; the source must reject an oversized header
// and drop the connection (so pop() drains to nullopt) rather than allocating
// it or blocking forever on a body that never comes. Before the cap this test
// would OOM or hang; after it, pop() returns promptly.
TEST(NetworkChannel, OversizedFrameHeaderIsRejectedNotAllocated) {
    using namespace clink::network;

    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();

    std::thread attacker([port] {
        const int fd = NetworkSocket::connect_to("127.0.0.1", port);
        if (fd < 0) {
            return;
        }
        // A data-frame header claiming just over the frame cap, then no body.
        std::vector<std::byte> hdr;
        put_u32_be(hdr, kMaxFrameBytes + 1u);
        NetworkSocket::send_all(fd, hdr.data(), hdr.size());
        // Hold the connection briefly so the source processes the header, then
        // close - the source should already have dropped its side.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        NetworkSocket::close(fd);
    });

    source.accept();
    // Reader hits the cap, breaks, and closes: the stream drains to nullopt.
    // (A hang here would mean the cap did not fire; an OOM would crash.)
    const auto e = source.pop();
    EXPECT_FALSE(e.has_value()) << "oversized frame must not yield a record";

    attacker.join();
}
