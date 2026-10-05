#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <functional>
#include <pthread.h>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/network/local_data_plane.hpp"
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

// The advertised-host alias. A worker BINDS its data-plane endpoints to
// CLINK_DATA_BIND_HOST (0.0.0.0 in a container, 127.0.0.1 by default) but peers
// address it by the host it advertised to the coordinator (--data-host, e.g.
// "clink-worker3"). Registering only under the bind address meant a lookup by the
// advertised name missed, the in-process fast path silently vanished, and two
// subtasks colocated in one process serialised to Arrow IPC and crossed a TCP
// socket to their own hostname. It went unnoticed because every non-container
// deployment has both sides defaulting to 127.0.0.1, where the keys coincide.
TEST(LocalDataPlaneAdvertisedHost, LookupByAdvertisedNameFindsAWildcardBoundEndpoint) {
    auto& ldp = LocalDataPlane::instance();
    const std::uint16_t port = 51999;
    ldp.set_advertised_host("clink-worker3");
    auto ch = std::make_shared<LocalEndpointChannel<std::int64_t>>(8);

    ldp.register_endpoint<std::int64_t>("0.0.0.0", port, ch);

    EXPECT_NE(ldp.lookup_endpoint<std::int64_t>("clink-worker3", port), nullptr)
        << "a peer resolving this worker by its advertised name must hit the fast path";
    EXPECT_NE(ldp.lookup_endpoint<std::int64_t>("0.0.0.0", port), nullptr)
        << "the bind identity must keep working";
    EXPECT_NE(ldp.lookup_endpoint<std::int64_t>("127.0.0.1", port), nullptr)
        << "a wildcard bind is reachable in-process as loopback";
    EXPECT_EQ(ldp.lookup_endpoint<std::int64_t>("clink-worker9", port), nullptr)
        << "a DIFFERENT worker must not resolve locally - that would cross-wire subtasks";

    // Every alias must be dropped, or a stale channel outlives its deploy and the
    // next job pushes records into a queue nobody reads.
    ldp.unregister_endpoint("0.0.0.0", port);
    EXPECT_EQ(ldp.lookup_endpoint<std::int64_t>("clink-worker3", port), nullptr);
    EXPECT_EQ(ldp.lookup_endpoint<std::int64_t>("0.0.0.0", port), nullptr);
    EXPECT_EQ(ldp.lookup_endpoint<std::int64_t>("127.0.0.1", port), nullptr);
    ldp.set_advertised_host("");
}

// ---------------------------------------------------------------------
// Protocol corruption must FAIL the channel, not end the stream.
//
// Found the hard way: a corrupted first frame (its Arrow IPC payload
// arrived as zeros) made the recv loop exit silently, the consumer saw
// an ordinary close, every task finished cleanly, and a three-worker
// pipeline reported "ok" having delivered zero of its 99 records. The
// contract pinned here: undecodable bytes leave failure_reason()
// non-null, and the bridge surfaces that as a task error instead of
// end-of-input.
// ---------------------------------------------------------------------

namespace {

// Raw TCP client that speaks just enough framing to deliver one
// deliberately-corrupt ArrowBatch frame: correct length prefix, valid
// kind byte, garbage (all-zero) IPC payload.
void send_corrupt_arrow_frame(std::uint16_t port) {
    const int fd = NetworkSocket::connect_to("127.0.0.1", port);
    ASSERT_GE(fd, 0) << "raw connect failed";
    std::vector<std::byte> body;
    body.push_back(static_cast<std::byte>(Kind::ArrowBatch));
    body.insert(body.end(), 64, std::byte{0});  // no Arrow stream starts with zeros
    std::vector<std::byte> header;
    put_u32_be(header, static_cast<std::uint32_t>(body.size()));
    ASSERT_TRUE(NetworkSocket::send_all(fd, header.data(), header.size()));
    ASSERT_TRUE(NetworkSocket::send_all(fd, body.data(), body.size()));
    NetworkSocket::close(fd);
}

}  // namespace

TEST(NetworkChannelFailure, ACorruptArrowFrameFailsTheChannelInsteadOfEndingIt) {
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();
    std::thread sender([port] { send_corrupt_arrow_frame(port); });
    source.accept();

    // The recv loop dies on the corrupt frame; the queue drains empty.
    auto e = source.pop();
    EXPECT_FALSE(e.has_value());
    ASSERT_NE(source.failure_reason(), nullptr)
        << "a corrupt frame must be recorded as a FAILURE, not a clean close";
    EXPECT_STREQ(source.failure_reason(), "Arrow IPC payload undecodable");
    sender.join();
}

TEST(NetworkChannelFailure, ACleanCloseLeavesNoFailureReason) {
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    const std::uint16_t port = source.listen();
    std::thread sender([port] {
        NetworkChannelSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
        sink.connect();
        Batch<std::int64_t> b;
        b.emplace(7);
        sink.push(StreamElement<std::int64_t>::data(std::move(b)));
        sink.close_send();
    });
    source.accept();

    auto e = source.pop();
    ASSERT_TRUE(e.has_value());
    EXPECT_TRUE(e->is_data());
    EXPECT_FALSE(source.pop().has_value());
    EXPECT_EQ(source.failure_reason(), nullptr) << "an ordinary close must NOT read as a failure";
    sender.join();
}

TEST(NetworkChannelFailure, TheBridgeSurfacesAFailedChannelAsATaskError) {
    NetworkBridgeSource<std::int64_t> bridge(/*port*/ 0, int64_codec());
    const std::uint16_t port = bridge.prepare_listen();
    std::thread sender([port] { send_corrupt_arrow_frame(port); });
    bridge.open();

    Emitter<std::int64_t> sink_nothing([](StreamElement<std::int64_t>) { return true; });
    // produce() must THROW once the channel records the corruption -
    // returning false here is exactly the silent-loss bug this pins.
    EXPECT_THROW(
        {
            while (bridge.produce(sink_nothing)) {
            }
        },
        std::runtime_error);
    sender.join();
}

// accept_one must retry past a signal (EINTR) rather than treat it as
// terminal. At graph width the treat-as-terminal path was item 72's silent
// cascade origin: one interrupted accept read as an orderly shutdown, the
// task saw a clean end-of-stream, and its upstream hit the reset backlog
// connection as "peer gone" - a whole-job restart from a transient a retry
// absorbs. The test parks a thread in accept_one, peppers it with a
// handler-installed signal (no SA_RESTART, so the accept genuinely returns
// EINTR), then connects; the accept must deliver the connection.
namespace {
void eintr_test_noop_handler(int) {}
}  // namespace

TEST(NetworkSocketAccept, ASignalInterruptedAcceptStillDeliversTheConnection) {
    struct sigaction sa{};
    sa.sa_handler = eintr_test_noop_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // deliberately NOT SA_RESTART: accept must see EINTR
    struct sigaction prev{};
    ASSERT_EQ(::sigaction(SIGUSR1, &sa, &prev), 0);

    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);

    std::atomic<int> accepted{-2};
    std::thread acceptor([&] { accepted.store(NetworkSocket::accept_one(listener_fd)); });

    // Interrupt the blocked accept repeatedly across a window that
    // comfortably covers the thread reaching accept(), then connect.
    const auto native = acceptor.native_handle();
    for (int i = 0; i < 20; ++i) {
        ::pthread_kill(native, SIGUSR1);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const int client_fd = NetworkSocket::connect_to("127.0.0.1", port);
    ASSERT_GE(client_fd, 0);

    acceptor.join();
    EXPECT_GE(accepted.load(), 0)
        << "an EINTR while blocked in accept was treated as terminal (item 72): the "
           "connection that arrived moments later was never delivered";

    if (accepted.load() >= 0) {
        NetworkSocket::close(accepted.load());
    }
    NetworkSocket::close(client_fd);
    NetworkSocket::close(listener_fd);
    ::sigaction(SIGUSR1, &prev, nullptr);
}

namespace {

// True while `fd` is still the listener bound to `port`: open, and not
// reissued to another socket. A woken listener counts (on Linux the wake
// shuts it down, which leaves it open and bound); a closed one does not.
bool accept_wake_test_is_listening_on(int fd, std::uint16_t port) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0 ||
        addr.sin_family != AF_INET || ntohs(addr.sin_port) != port) {
        return false;
    }
    // Not SO_ACCEPTCONN, which Darwin does not answer. A socket bound to
    // the port with no peer is the listener: an accepted socket shares the
    // local port but has a peer.
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    return ::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &peer_len) != 0 &&
           errno == ENOTCONN;
}

// The descriptor this process holds listening on `port`, or -1. Probes a
// bounded descriptor range rather than listing /dev/fd, which is enough for a
// test process.
int accept_wake_test_listener_for(std::uint16_t port) {
    for (int fd = 0; fd < 4096; ++fd) {
        if (accept_wake_test_is_listening_on(fd, port)) {
            return fd;
        }
    }
    return -1;
}

bool accept_wake_test_await(const std::function<bool()>& cond) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!cond()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

}  // namespace

// A wake that lands before the wait starts is not lost, and it ends the wait
// without closing the listener: the owner closes that, after joining.
TEST(NetworkSocketAccept, AWakeEndsTheWaitEvenWhenItCameFirst) {
    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);
    AcceptWake wake(listener_fd);
#if defined(__linux__)
    // On Linux the wake is a shutdown of the listener itself, so a receiver
    // waiting for a peer costs no descriptors beyond its listener.
    EXPECT_EQ(wake.fd(), -1) << "the Linux wake allocated a descriptor";
#endif
    wake.wake();
    wake.wake();  // idempotent

    std::atomic<int> result{-2};
    std::atomic<int> err{0};
    std::thread acceptor([&] {
        result.store(NetworkSocket::accept_one(listener_fd, wake));
        err.store(errno);
    });
    acceptor.join();
    EXPECT_EQ(result.load(), -1);
    EXPECT_EQ(err.load(), ECANCELED);
    EXPECT_TRUE(accept_wake_test_is_listening_on(listener_fd, port))
        << "the wait ended by closing the listener";
    NetworkSocket::close(listener_fd);
}

// The waiting accept puts the listener in non-blocking mode, and an accepted
// socket inherits that on Darwin and the BSDs (not on Linux). Every reader of
// an accepted fd - recv_all, the TLS handshake - expects a blocking one.
TEST(NetworkSocketAccept, AWaitingAcceptDeliversABlockingSocket) {
    std::uint16_t port = 0;
    const int listener_fd = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener_fd, 0);
    AcceptWake wake(listener_fd);
    std::atomic<int> accepted{-2};
    std::thread acceptor([&] { accepted.store(NetworkSocket::accept_one(listener_fd, wake)); });
    const int client_fd = NetworkSocket::connect_to("127.0.0.1", port);
    ASSERT_GE(client_fd, 0);
    acceptor.join();
    ASSERT_GE(accepted.load(), 0);
    EXPECT_NE(::fcntl(listener_fd, F_GETFL) & O_NONBLOCK, 0) << "the listener was left blocking";
    EXPECT_EQ(::fcntl(accepted.load(), F_GETFL) & O_NONBLOCK, 0)
        << "the accepted socket inherited the listener's O_NONBLOCK";
    NetworkSocket::close(accepted.load());
    NetworkSocket::close(client_fd);
    NetworkSocket::close(listener_fd);
}

// A receiver's teardown must not close its listener while the recv thread
// can still accept() on it. On Darwin, a close() that lands while that thread
// is entering accept() can miss it, and then both block until a connection
// arrives - which for the colocated case, where no peer ever connects, is
// never. On Linux, close() does not wake accept() at all, and a descriptor
// closed under the thread can be reissued to another receiver's listener in
// the same worker before the thread gets to it.
//
// The recv thread is parked just before it waits; shutdown_recv() must leave
// the listener open, and once the thread is let go it closes the listener
// itself, so the port is still given back without waiting for the destructor.
TEST(NetworkChannel, TeardownNeverClosesTheListenerUnderTheRecvThread) {
    namespace fault = clink::fault;
    // Before the fault guard, so an early exit resets the registry (releasing
    // the parked recv thread) before the destructor joins it.
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    fault::Registry::instance().reset();
    fault::ScopedFault park{fault::Rule{.point = fault::points::kNetworkChannelBeforeAccept,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    const std::uint16_t port = source.listen();
    source.accept();
    ASSERT_TRUE(accept_wake_test_await([] {
        return fault::Registry::instance().hits(fault::points::kNetworkChannelBeforeAccept) >= 1;
    })) << "the recv thread never started";
    const int listener = accept_wake_test_listener_for(port);
    ASSERT_GE(listener, 0) << "no descriptor in this process is listening on " << port;

    source.shutdown_recv();
    const bool open_while_thread_waits = accept_wake_test_is_listening_on(listener, port);
    // Not asserting the count: the thread can be counted as a hit before it
    // has parked. The release still reaches it (reach() captures the epoch).
    fault::Registry::instance().release(fault::points::kNetworkChannelBeforeAccept);

    EXPECT_TRUE(open_while_thread_waits)
        << "shutdown_recv() closed the listener while the recv thread could still accept() on it";
    EXPECT_TRUE(accept_wake_test_await([&] {
        return !accept_wake_test_is_listening_on(listener, port);
    })) << "the woken recv thread did not give the listener back";
    EXPECT_FALSE(source.pop().has_value());
}

// A peer that connects as teardown begins must not strand the recv thread.
// Teardown wakes the thread and shuts down any peer it can see; a connection
// the thread accepted but had not yet published is one it cannot see, so the
// thread must refuse it once woken. Kept instead, the thread would sit in
// recv_all on a peer nothing will ever shut down, and the destructor's join
// would wait for that peer to hang up.
//
// The recv thread is parked just after it accepts the test's connection;
// shutdown_recv() runs there, and once let go the thread must close the
// connection rather than serve it.
TEST(NetworkChannel, APeerAcceptedDuringTeardownIsRefused) {
    namespace fault = clink::fault;
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    // After the source, so it closes first: a kept connection then ends at
    // the client's close instead of hanging the destructor's join.
    struct ClientFd {
        int fd{-1};
        ~ClientFd() {
            if (fd >= 0) {
                NetworkSocket::close(fd);
            }
        }
    } client;
    fault::Registry::instance().reset();
    fault::ScopedFault park{fault::Rule{.point = fault::points::kNetworkChannelAfterAccept,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    const std::uint16_t port = source.listen();
    source.accept();
    client.fd = NetworkSocket::connect_to("127.0.0.1", port);
    ASSERT_GE(client.fd, 0);
    ASSERT_TRUE(accept_wake_test_await([] {
        return fault::Registry::instance().hits(fault::points::kNetworkChannelAfterAccept) >= 1;
    })) << "the recv thread never accepted the connection";

    source.shutdown_recv();
    // Not asserting the count: the thread can be counted as a hit before it
    // has parked. The release still reaches it (reach() captures the epoch).
    fault::Registry::instance().release(fault::points::kNetworkChannelAfterAccept);

    // Refused: the client sees the connection end. Served: it receives the
    // source's initial credit instead, or nothing until the bound.
    timeval bound{.tv_sec = 10, .tv_usec = 0};
    ASSERT_EQ(::setsockopt(client.fd, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof(bound)), 0);
    std::byte b{};
    const auto n = ::recv(client.fd, &b, 1, 0);
    const int err = errno;
    EXPECT_TRUE(n == 0 || (n < 0 && err != EAGAIN && err != EWOULDBLOCK))
        << "the recv thread kept a peer it accepted after teardown woke it (recv returned " << n
        << ", errno " << err << ")";
    EXPECT_FALSE(source.pop().has_value());
}

// Only a wake is an orderly end of the wait for a peer. The owner never closes
// or shuts down the listener to stop the recv thread any more, so an accept
// that fails EINVAL or EBADF means something else broke the listener, and
// ending the stream quietly would hand the task a clean end of input with
// nothing in it: the silent clean end-of-stream that turns into a whole-job
// restart once the upstream hits the dead connection. It must fail the stream
// instead, as any other accept failure does.
//
// The recv thread is parked before it waits, and the listener's descriptor
// number is swapped for a socket that is readable but not listening (one end
// of a socketpair, shut down for reading), so the thread's accept fails
// EINVAL. Swapped with dup2, so the number stays the source's to close and the
// listener is closed while no thread is using it.
TEST(NetworkChannel, AnAcceptFailureThatIsNotAWakeFailsTheStream) {
    namespace fault = clink::fault;
    NetworkChannelSource<std::int64_t> source(/*port*/ 0, int64_codec());
    fault::Registry::instance().reset();
    fault::ScopedFault park{fault::Rule{.point = fault::points::kNetworkChannelBeforeAccept,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    const std::uint16_t port = source.listen();
    source.accept();
    ASSERT_TRUE(accept_wake_test_await([] {
        return fault::Registry::instance().hits(fault::points::kNetworkChannelBeforeAccept) >= 1;
    })) << "the recv thread never started";
    const int listener = accept_wake_test_listener_for(port);
    ASSERT_GE(listener, 0) << "no descriptor in this process is listening on " << port;

    int pair[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
    ASSERT_EQ(::shutdown(pair[0], SHUT_RD), 0);
    ASSERT_EQ(::dup2(pair[0], listener), listener);
    ::close(pair[0]);
    ::close(pair[1]);
    // Not asserting the count: the thread can be counted as a hit before it
    // has parked. The release still reaches it (reach() captures the epoch).
    fault::Registry::instance().release(fault::points::kNetworkChannelBeforeAccept);

    EXPECT_FALSE(source.pop().has_value());
    ASSERT_NE(source.failure_reason(), nullptr)
        << "an accept on a broken listener ended the stream as if it were a clean end of input";
    EXPECT_NE(std::string{source.failure_reason()}.find("accept failed"), std::string::npos)
        << source.failure_reason();
}

// The SINK-side data-loss detector, which had no test at all.
//
// NetworkBridgeSink::require_sent_ throws when a push is refused, and it
// was made to throw from an incident rather than from a red test: a send
// whose false return was discarded left no error, no failed task and no log
// line, and the resulting short stream surfaced six operators downstream as
// a windowed top-N quietly disagreeing with another engine. The source side
// of that contract is pinned by TheBridgeSurfacesAFailedChannelAsATaskError;
// the send side was not pinned by anything, which is how a change that
// stopped it failing the task passed every suite (followups item 83).
//
// A co-located endpoint makes the refusal deterministic: the local data
// plane hands the sink a BoundedChannel, and a closed BoundedChannel
// refuses a push without any socket timing involved.
namespace {

// A registered local endpoint the test can close underneath the sink.
std::shared_ptr<clink::network::LocalEndpointChannel<std::int64_t>> register_local_peer(
    std::uint16_t port) {
    auto ch = std::make_shared<clink::network::LocalEndpointChannel<std::int64_t>>(16);
    clink::network::LocalDataPlane::instance().register_endpoint<std::int64_t>(
        "127.0.0.1", port, ch);
    return ch;
}

Batch<std::int64_t> one_record() {
    Batch<std::int64_t> b;
    b.emplace(7, EventTime{1});
    return b;
}

}  // namespace

TEST(NetworkBridgeSinkLoss, ARefusedSendThrowsRatherThanShorteningTheStream) {
    const std::uint16_t port = 53991;
    auto peer = register_local_peer(port);
    NetworkBridgeSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
    sink.open();

    // The peer goes away while the job is still running. Anything pushed now
    // is NOT in the stream, and silence about it is the defect.
    peer->close();

    EXPECT_THROW(sink.on_data(one_record()), std::runtime_error)
        << "a send the channel refused was accepted silently: the stream is now short by "
           "those records and nothing says so";
    clink::network::LocalDataPlane::instance().unregister_endpoint("127.0.0.1", port);
}

TEST(NetworkBridgeSinkLoss, AWatermarkRefusedAfterThePeerLeftAlsoThrows) {
    const std::uint16_t port = 53992;
    auto peer = register_local_peer(port);
    NetworkBridgeSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
    sink.open();
    peer->close();

    EXPECT_THROW(sink.on_watermark(Watermark{EventTime{5}}), std::runtime_error);
    clink::network::LocalDataPlane::instance().unregister_endpoint("127.0.0.1", port);
}

// The other half of the contract, and the reason the detector can be trusted:
// our OWN shutdown must not read as loss. close() sets closing_ before it
// closes the channel, so an element racing teardown is an orderly stop.
TEST(NetworkBridgeSinkLoss, AnElementRacingOurOwnCloseIsNotTreatedAsLoss) {
    const std::uint16_t port = 53993;
    auto peer = register_local_peer(port);
    NetworkBridgeSink<std::int64_t> sink("127.0.0.1", port, int64_codec());
    sink.open();

    sink.close();  // sets closing_, then closes the send side
    peer->close();

    EXPECT_NO_THROW(sink.on_data(one_record()))
        << "an orderly shutdown must not be reported as a lost record";
    clink::network::LocalDataPlane::instance().unregister_endpoint("127.0.0.1", port);
}
