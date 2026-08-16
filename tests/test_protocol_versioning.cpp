// Protocol version negotiation, and the snapshot format-version gate.
//
// Two surfaces carry a version, and until this change neither was checked.
//
// The CLUSTER WIRE PROTOCOL had no version at all. Additive tails let a
// message gain a field across a rolling upgrade, which covers the common
// case well - but they cover only that case. A field that changes meaning
// or width, a message kind repurposed, a semantic contract changed under
// an unchanged encoding: all of those are invisible on the wire, and the
// cluster would have run happily into whatever they caused.
//
// The SNAPSHOT FORMAT VERSION was written on every stream, documented as a
// hard gate ("readers MUST reject a version above the highest they know"),
// and read by nothing. A future version-2 stream would have been restored
// as version 1. Since a version bump means exactly "a change the previous
// reader misreads", and a bump need not change the column shape, the
// existing schema check did not stand in for it.

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include "clink/cluster/client_handshake.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"

using namespace clink::cluster;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = clink::MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

std::vector<std::byte> proto_body_of(const std::vector<std::byte>& framed) {
    if (framed.size() < 4) {
        return {};
    }
    return {framed.begin() + 4, framed.end()};
}

template <typename Msg, typename Decoder>
Msg proto_round_trip(MessageKind kind, const Msg& original, Decoder decode) {
    MessageReader r(proto_body_of(encode_frame(kind, original)));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), kind);
    return decode(r);
}

// Drop `n` bytes off the tail and repair the length prefix, producing the
// frame a peer built before those fields existed.
std::vector<std::byte> truncate_tail(std::vector<std::byte> framed, std::size_t n) {
    EXPECT_GT(framed.size(), n + 4);
    framed.resize(framed.size() - n);
    const auto body_len = static_cast<std::uint32_t>(framed.size() - 4);
    framed[0] = static_cast<std::byte>((body_len >> 24) & 0xFF);
    framed[1] = static_cast<std::byte>((body_len >> 16) & 0xFF);
    framed[2] = static_cast<std::byte>((body_len >> 8) & 0xFF);
    framed[3] = static_cast<std::byte>(body_len & 0xFF);
    return framed;
}

// Length-prefixed frame IO over a raw Connection. The cluster's own
// helpers are internal to coordinator.cpp, and the point of these two
// tests is to be a peer the engine did not build - so the framing is done
// here, the same way the CLI tools do it.
bool send_raw_frame(clink::network::Connection& c, const std::vector<std::byte>& framed) {
    return c.send_all(framed.data(), framed.size());
}

std::optional<std::vector<std::byte>> read_raw_frame(clink::network::Connection& c) {
    std::array<std::byte, 4> hdr{};
    if (!c.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (const auto b : hdr) {
        len = (len << 8) | static_cast<unsigned char>(b);
    }
    std::vector<std::byte> body(len);
    if (len > 0 && !c.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

std::shared_ptr<arrow::KeyValueMetadata> meta_with_version(const std::string& v) {
    auto m = std::make_shared<arrow::KeyValueMetadata>();
    m->Append(clink::kSnapshotFormatVersionKey, v);
    return m;
}

}  // namespace

// --- the compatibility rule ---------------------------------------------

TEST(ProtocolVersioning, MatchingVersionsAreCompatible) {
    const auto c = check_protocol_compatibility(
        kClusterProtocolVersion, kMinCompatibleClusterProtocolVersion, "worker 'w'");
    EXPECT_TRUE(c.compatible) << c.reason;
    EXPECT_TRUE(c.reason.empty());
}

TEST(ProtocolVersioning, APeerOlderThanThisBuildSupportsIsRefused) {
    // This build declares min=5; the peer speaks 3. Refusing is the point:
    // admitting it means failing later on a frame it cannot decode.
    const auto c = check_protocol_compatibility(
        /*peer_version=*/3, /*peer_min=*/1, "worker 'w'", /*own=*/7, /*own_min=*/5);
    EXPECT_FALSE(c.compatible);
    EXPECT_NE(c.reason.find("v3"), std::string::npos) << c.reason;
    EXPECT_NE(c.reason.find("v7"), std::string::npos) << c.reason;
    EXPECT_NE(c.reason.find("Upgrade the peer"), std::string::npos)
        << "the diagnostic must say which end to upgrade: " << c.reason;
}

TEST(ProtocolVersioning, APeerNewerThanThisBuildSupportsIsAlsoRefused) {
    // The other direction, and the one a single-sided check misses: the
    // peer requires at least v9 and this build only speaks v4.
    const auto c = check_protocol_compatibility(
        /*peer_version=*/9, /*peer_min=*/9, "coordinator", /*own=*/4, /*own_min=*/1);
    EXPECT_FALSE(c.compatible);
    EXPECT_NE(c.reason.find("Upgrade this node"), std::string::npos) << c.reason;
}

TEST(ProtocolVersioning, AnOverlappingRangeIsCompatibleEvenWhenVersionsDiffer) {
    // The case a rolling upgrade actually depends on: v2 and v3 nodes that
    // both still support v1. Refusing this would make every upgrade a
    // full-cluster outage, which is the failure mode that makes people
    // turn versioning off.
    const auto c = check_protocol_compatibility(
        /*peer_version=*/2, /*peer_min=*/1, "worker 'w'", /*own=*/3, /*own_min=*/1);
    EXPECT_TRUE(c.compatible) << c.reason;
}

TEST(ProtocolVersioning, APeerThatDeclaresNothingIsTreatedAsVersionOne) {
    // A node from before this change sends no version fields, so they
    // decode as 0. Zero must mean "the protocol as it stood at v1", not
    // "invalid" - otherwise deploying this change fences off every node
    // that has not restarted yet.
    const auto c = check_protocol_compatibility(/*peer_version=*/0, /*peer_min=*/0, "worker 'w'");
    EXPECT_TRUE(c.compatible) << c.reason;

    // And it IS refused once this build stops supporting v1, which is the
    // deliberate end-of-support decision rather than an accident.
    const auto dropped = check_protocol_compatibility(
        /*peer_version=*/0, /*peer_min=*/0, "worker 'w'", /*own=*/2, /*own_min=*/2);
    EXPECT_FALSE(dropped.compatible);
}

// --- the fields on the wire ---------------------------------------------

TEST(ProtocolVersioning, TheHandshakeMessagesCarryTheDeclaration) {
    {
        RegisterMsg in{.worker_id = "w", .data_host = "h", .slot_count = 2};
        in.protocol_version = 4;
        in.min_compatible_protocol_version = 2;
        const auto out = proto_round_trip(MessageKind::Register, in, decode_register);
        EXPECT_EQ(out.worker_id, "w");
        EXPECT_EQ(out.slot_count, 2U);
        EXPECT_EQ(out.protocol_version, 4U);
        EXPECT_EQ(out.min_compatible_protocol_version, 2U);
    }
    {
        RegisterAckMsg in{.ok = true, .message = "welcome"};
        in.coordinator_epoch = 8;
        in.protocol_version = 4;
        in.min_compatible_protocol_version = 2;
        const auto out = proto_round_trip(MessageKind::RegisterAck, in, decode_register_ack);
        EXPECT_EQ(out.coordinator_epoch, 8U) << "the fencing epoch must survive the new tail";
        EXPECT_EQ(out.protocol_version, 4U);
        EXPECT_EQ(out.min_compatible_protocol_version, 2U);
    }
    {
        HelloClientMsg in;
        in.protocol_version = 4;
        in.min_compatible_protocol_version = 2;
        const auto out = proto_round_trip(MessageKind::HelloClient, in, decode_hello_client);
        EXPECT_EQ(out.protocol_version, 4U);
        EXPECT_EQ(out.min_compatible_protocol_version, 2U);
    }
}

TEST(ProtocolVersioning, TheDeclarationDefaultsToWhatThisBuildSpeaks) {
    // A message constructed anywhere in the codebase declares the truth
    // without its author having to remember to. Every HelloClient sender in
    // tools/ relies on this rather than setting the fields itself.
    EXPECT_EQ(RegisterMsg{}.protocol_version, kClusterProtocolVersion);
    EXPECT_EQ(RegisterAckMsg{}.protocol_version, kClusterProtocolVersion);
    EXPECT_EQ(HelloClientMsg{}.protocol_version, kClusterProtocolVersion);
    EXPECT_EQ(RegisterMsg{}.min_compatible_protocol_version, kMinCompatibleClusterProtocolVersion);
}

TEST(ProtocolVersioning, AHandshakeFromAPreVersioningPeerStillDecodes) {
    // The frame a previous build actually puts on the wire: body ends
    // before the two u32s. It must decode, keep everything before them,
    // and report 0 - which the rule reads as v1.
    {
        RegisterMsg in{.worker_id = "w", .data_host = "h", .slot_count = 3};
        in.http_port = 8081;
        MessageReader r(proto_body_of(truncate_tail(encode_frame(MessageKind::Register, in), 8)));
        EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::Register);
        const auto out = decode_register(r);
        EXPECT_EQ(out.worker_id, "w");
        EXPECT_EQ(out.slot_count, 3U);
        EXPECT_EQ(out.http_port, 8081);
        EXPECT_EQ(out.protocol_version, 0U);
        EXPECT_EQ(out.min_compatible_protocol_version, 0U);
    }
    {
        // The RegisterAck case matters most: it now carries FOUR tail
        // fields (epoch, version, min, retryable). A peer that knows only the epoch
        // truncates after it, and the epoch must still arrive intact.
        RegisterAckMsg in{.ok = true, .message = "welcome"};
        in.coordinator_epoch = 12;
        MessageReader r(
            proto_body_of(truncate_tail(encode_frame(MessageKind::RegisterAck, in), 9)));
        EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::RegisterAck);
        const auto out = decode_register_ack(r);
        EXPECT_TRUE(out.ok);
        EXPECT_EQ(out.coordinator_epoch, 12U);
        EXPECT_EQ(out.protocol_version, 0U);
    }
    {
        // HelloClient used to be an EMPTY body. That is the exact frame an
        // older CLI sends, so it must decode rather than throw.
        MessageBuilder b;
        b.put_u8(static_cast<std::uint8_t>(MessageKind::HelloClient));
        MessageReader r(proto_body_of(b.finalize()));
        EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::HelloClient);
        HelloClientMsg out;
        EXPECT_NO_THROW(out = decode_hello_client(r));
        EXPECT_EQ(out.protocol_version, 0U);
    }
}

TEST(ProtocolVersioning, ARefusalIsLegibleToAToolExpectingADifferentAck) {
    // A refused client gets a SubmitJobAck, which is right for `clink
    // submit` and wrong for every other tool. Those report the reason
    // rather than the kind number.
    SubmitJobAckMsg nack{.job_id = 0, .ok = false, .message = "worker speaks protocol v9"};
    MessageReader r(proto_body_of(encode_frame(MessageKind::SubmitJobAck, nack)));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    const auto why = protocol_rejection_message(kind, r);
    ASSERT_TRUE(why.has_value());
    EXPECT_EQ(*why, "worker speaks protocol v9");
}

TEST(ProtocolVersioning, AGenuineUnexpectedFrameIsNotMistakenForARefusal) {
    // The helper must not turn every surprise into "the coordinator
    // refused you" - a successful SubmitJobAck arriving where a
    // ListJobsAck was expected is a different bug and must read as one.
    SubmitJobAckMsg ok_ack{.job_id = 7, .ok = true, .message = ""};
    MessageReader r(proto_body_of(encode_frame(MessageKind::SubmitJobAck, ok_ack)));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    EXPECT_FALSE(protocol_rejection_message(kind, r).has_value());

    HeartbeatMsg hb{"w"};
    MessageReader r2(proto_body_of(encode_frame(MessageKind::Heartbeat, hb)));
    const auto kind2 = static_cast<MessageKind>(r2.read_u8());
    EXPECT_FALSE(protocol_rejection_message(kind2, r2).has_value());
}

// --- negotiation against a real coordinator ------------------------------

TEST(ProtocolVersioning, ARealCoordinatorRefusesAWorkerItCannotSpeakTo) {
    // The rule tested through the actual handshake rather than in
    // isolation. A worker that declares a version this build does not
    // support must be turned away AT REGISTRATION, with the reason on the
    // nack - not admitted and left to fail later on a frame it cannot
    // decode, where the symptom is a job that will not deploy.
    //
    // Driven over a raw socket rather than through Worker, because Worker
    // necessarily declares the version this build speaks; the peer under
    // test here is one that does not.
    Coordinator coordinator;
    const auto port = coordinator.start();

    auto conn = clink::network::connect_plain("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RegisterMsg reg{.worker_id = "from-the-future", .data_host = "127.0.0.1", .slot_count = 1};
    reg.protocol_version = kClusterProtocolVersion + 5;
    reg.min_compatible_protocol_version = kClusterProtocolVersion + 5;
    const auto frame = encode_frame(MessageKind::Register, reg);
    ASSERT_TRUE(send_raw_frame(*conn, frame));

    auto reply = read_raw_frame(*conn);
    ASSERT_TRUE(reply.has_value()) << "the coordinator closed without saying why";
    MessageReader rr(std::move(*reply));
    ASSERT_EQ(static_cast<MessageKind>(rr.read_u8()), MessageKind::RegisterAck);
    const auto ack = decode_register_ack(rr);
    EXPECT_FALSE(ack.ok) << "a worker from an unsupported protocol version was admitted";
    EXPECT_NE(ack.message.find("protocol"), std::string::npos) << ack.message;
    EXPECT_NE(ack.message.find("from-the-future"), std::string::npos)
        << "the diagnostic must name the worker: " << ack.message;

    coordinator.stop();
}

TEST(ProtocolVersioning, ARealCoordinatorAdmitsAPeerThatDeclaresNothing) {
    // The rolling-upgrade case, end to end: a worker built before
    // versioning sends a Register with no version tail. It must register
    // normally. If this fails, deploying the change takes the cluster down.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"legacy"});

    auto conn = clink::network::connect_plain("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RegisterMsg reg{.worker_id = "legacy", .data_host = "127.0.0.1", .slot_count = 1};
    auto frame = encode_frame(MessageKind::Register, reg);
    frame = truncate_tail(std::move(frame), 8);  // what a pre-versioning worker sends
    ASSERT_TRUE(send_raw_frame(*conn, frame));

    auto reply = read_raw_frame(*conn);
    ASSERT_TRUE(reply.has_value());
    MessageReader rr(std::move(*reply));
    ASSERT_EQ(static_cast<MessageKind>(rr.read_u8()), MessageKind::RegisterAck);
    const auto ack = decode_register_ack(rr);
    EXPECT_TRUE(ack.ok) << "a pre-versioning worker was refused: " << ack.message;

    coordinator.stop();
}

// --- the snapshot format-version gate ------------------------------------

// Rewrite an Arrow IPC stream with a different format-version marker,
// leaving the data and the column shape untouched. This is precisely the
// shape a future version-2 writer would produce for a change the schema
// check cannot see - a key-layout change, say - which is why checking the
// schema is not a substitute for checking the version.
std::vector<std::byte> restamp_format_version(const std::vector<std::byte>& stream,
                                              const std::string& version) {
    auto buffer =
        std::make_shared<arrow::Buffer>(reinterpret_cast<const std::uint8_t*>(stream.data()),
                                        static_cast<std::int64_t>(stream.size()));
    auto reader =
        arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer))
            .ValueOrDie();

    auto meta = std::make_shared<arrow::KeyValueMetadata>();
    meta->Append(clink::kSnapshotFormatVersionKey, version);
    auto schema = reader->schema()->WithMetadata(meta);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        if (!reader->ReadNext(&batch).ok() || batch == nullptr) {
            break;
        }
        batches.push_back(batch->ReplaceSchemaMetadata(meta));
    }

    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
    for (const auto& b : batches) {
        EXPECT_TRUE(writer->WriteRecordBatch(*b).ok());
    }
    EXPECT_TRUE(writer->Close().ok());
    auto out = sink->Finish().ValueOrDie();
    return {reinterpret_cast<const std::byte*>(out->data()),
            reinterpret_cast<const std::byte*>(out->data()) + out->size()};
}

TEST(SnapshotFormatVersion, ARestoreActuallyRunsTheGate) {
    // The gate tested through a real restore rather than by calling it.
    //
    // Without this, every call site could be deleted and the unit tests
    // above would still pass - they exercise the check, not its wiring.
    // A gate nothing calls is the exact defect this work item exists to
    // fix, so it must not be reintroduced one call site at a time.
    clink::InMemoryStateBackend backend;
    backend.put(clink::OperatorId{1}, "k", "v");
    const auto snap = backend.snapshot(clink::CheckpointId{1});
    ASSERT_FALSE(snap.bytes.empty());

    // Round-tripping the untouched stream must still work, or the test
    // below would pass for the wrong reason.
    {
        clink::InMemoryStateBackend ok_backend;
        EXPECT_NO_THROW(ok_backend.restore(snap));
    }

    clink::Snapshot from_the_future{
        .checkpoint_id = clink::CheckpointId{1},
        .bytes = restamp_format_version(
            snap.bytes, std::to_string(clink::kMaxReadableSnapshotFormatVersion + 1))};
    EXPECT_THROW(
        {
            clink::InMemoryStateBackend future_backend;
            future_backend.restore(from_the_future);
        },
        std::runtime_error)
        << "a snapshot from a newer format was restored as though it were the current one";
}

TEST(SnapshotFormatVersion, AbsenceIsVersionOneForever) {
    // Streams written before the marker existed are valid and must stay
    // readable. This is not a transitional allowance; it is the contract.
    EXPECT_NO_THROW(clink::verify_snapshot_format_version(nullptr, "test"));
    auto empty = std::make_shared<arrow::KeyValueMetadata>();
    EXPECT_NO_THROW(clink::verify_snapshot_format_version(empty, "test"));
    // Metadata carrying other keys but no version is the same case.
    auto other = std::make_shared<arrow::KeyValueMetadata>();
    other->Append("clink.state_versions", "");
    EXPECT_NO_THROW(clink::verify_snapshot_format_version(other, "test"));
}

TEST(SnapshotFormatVersion, TheVersionThisBuildWritesIsOneItCanRead) {
    // Guards the bump-one-forget-the-other mistake: writing v2 while
    // still declaring v1 readable would make the engine unable to read
    // its own output.
    EXPECT_NO_THROW(clink::verify_snapshot_format_version(
        meta_with_version(clink::kSnapshotFormatVersion), "test"));
    EXPECT_LE(std::stoul(clink::kSnapshotFormatVersion), clink::kMaxReadableSnapshotFormatVersion)
        << "the writer emits a version this build cannot read";
}

TEST(SnapshotFormatVersion, ANewerFormatIsRefusedRatherThanGuessedAt) {
    const auto newer = std::to_string(clink::kMaxReadableSnapshotFormatVersion + 1);
    try {
        clink::verify_snapshot_format_version(meta_with_version(newer), "restore of job 7");
        FAIL() << "a version-" << newer << " snapshot was accepted";
    } catch (const std::exception& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("restore of job 7"), std::string::npos)
            << "the diagnostic does not say WHICH snapshot: " << what;
        EXPECT_NE(what.find(newer), std::string::npos) << what;
    }
}

TEST(SnapshotFormatVersion, AMalformedOrImpossibleVersionIsCorruption) {
    // Not a number, a number with trailing junk, and zero - which no
    // writer produces. Each is a corrupt or foreign stream, and reading
    // one as version 1 would be a guess.
    for (const char* bad : {"", "one", "1x", "1.0", " 1", "0"}) {
        EXPECT_THROW(clink::verify_snapshot_format_version(meta_with_version(bad), "test"),
                     std::runtime_error)
            << "format version '" << bad << "' was accepted";
    }
}

// --- The two enforcement sites nothing reached ------------------------
//
// Three places refuse an incompatible peer: the coordinator at Register, the
// coordinator at HelloClient, and the WORKER at RegisterAck. Only the first had a
// test that drove the real handshake. The other two were covered by predicate
// tests and by a decoder test for the rejection frame - neither of which proves
// the code that produces it ever runs.
//
// The worker direction is the one a single-sided check misses, and this header
// says so in its own comment: "the coordinator can read me" does not imply "I can
// read the coordinator".

namespace {

// A connection the test scripts, so a real Worker can be pointed at a
// coordinator that does not exist and told exactly what it replied. Same shape
// as FencingScriptedConnection in test_coordinator_fencing.cpp; kept local
// rather than shared because the two need different frames and a shared helper
// would have to grow options for both.
class VersionScriptedConnection final : public clink::network::Connection {
public:
    void deliver(const std::vector<std::byte>& framed) {
        {
            std::lock_guard lock(mu_);
            inbound_.insert(inbound_.end(), framed.begin(), framed.end());
        }
        cv_.notify_all();
    }

    bool send_all(const std::byte*, std::size_t) override { return true; }

    bool recv_all(std::byte* buf, std::size_t len) override {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return closed_ || inbound_.size() >= len; });
        if (closed_ && inbound_.size() < len) {
            return false;
        }
        std::copy_n(inbound_.begin(), len, buf);
        inbound_.erase(inbound_.begin(), inbound_.begin() + static_cast<std::ptrdiff_t>(len));
        return true;
    }

    void shutdown_write() override {}
    void shutdown_read() override { close(); }
    void close() override {
        {
            std::lock_guard lock(mu_);
            closed_ = true;
        }
        cv_.notify_all();
    }
    bool is_open() const noexcept override { return !closed_; }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<std::byte> inbound_;
    bool closed_{false};
};

}  // namespace

TEST(ProtocolVersioning, AWorkerRefusesACoordinatorItCannotSpeakTo) {
    using namespace std::chrono_literals;
    const auto before = counter_value(clink::metrics::kProtocolMismatches);

    Worker::Config cfg;
    cfg.heartbeat_interval = 0ms;
    Worker worker("w-ver", "127.0.0.1", cfg);
    // Atomic, because the factory runs on the calling thread and the seeder
    // below polls it from another. The equivalent helper in
    // test_coordinator_fencing.cpp uses a plain pointer here and TSan reports it;
    // copying that would have added a second instance of a known race rather than
    // one more test.
    std::atomic<VersionScriptedConnection*> conn{nullptr};
    worker.set_connect_factory([&conn](const std::string&, std::uint16_t) {
        auto c = std::make_unique<VersionScriptedConnection>();
        conn.store(c.get(), std::memory_order_release);
        return c;
    });

    // connect_to_coordinator blocks reading the ack, so it has to be queued from
    // another thread once the factory has handed the pointer over.
    std::thread seeder([&conn] {
        VersionScriptedConnection* c = nullptr;
        while ((c = conn.load(std::memory_order_acquire)) == nullptr) {
            std::this_thread::sleep_for(100us);
        }
        RegisterAckMsg ack;
        ack.ok = true;
        ack.protocol_version = kClusterProtocolVersion + 5;
        ack.min_compatible_protocol_version = kClusterProtocolVersion + 5;
        c->deliver(encode_frame(MessageKind::RegisterAck, ack));
    });

    std::string what;
    try {
        worker.connect_to_coordinator("127.0.0.1", 1);
        ADD_FAILURE() << "the worker joined a coordinator speaking a protocol it cannot read; a "
                         "half-compatible pairing surfaces later as a decode failure on some "
                         "control frame, a long way from the cause";
    } catch (const std::exception& e) {
        what = e.what();
    }
    seeder.join();

    EXPECT_NE(what.find("coordinator"), std::string::npos)
        << "the refusal does not say which end is incompatible: " << what;
    EXPECT_NE(counter_value(clink::metrics::kProtocolMismatches), before)
        << "a refused pairing was not counted, so a cluster half-refusing its peers is invisible "
           "to monitoring";

    if (auto* c = conn.load(std::memory_order_acquire); c != nullptr) {
        c->close();
    }
    worker.stop();
}

TEST(ProtocolVersioning, ARealCoordinatorRefusesAClientItCannotSpeakTo) {
    using namespace std::chrono_literals;
    Coordinator coordinator;
    const auto port = coordinator.start();
    const auto before = counter_value(clink::metrics::kProtocolMismatches);

    auto conn = clink::network::connect_plain("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);
    HelloClientMsg hello;
    hello.protocol_version = kClusterProtocolVersion + 5;
    hello.min_compatible_protocol_version = kClusterProtocolVersion + 5;
    const auto frame = encode_frame(MessageKind::HelloClient, hello);
    ASSERT_TRUE(conn->send_all(frame.data(), frame.size()));

    // The refusal arrives as a SubmitJobAck, which is what a client tool decodes
    // when it asked for something else - the shape protocol_rejection_message
    // exists for. That helper has a test; the path that PRODUCES the frame did
    // not.
    auto reply = clink::cluster::read_frame(*conn);
    ASSERT_TRUE(reply.has_value()) << "the coordinator closed without saying why";
    MessageReader r(std::move(*reply));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    ASSERT_EQ(kind, MessageKind::SubmitJobAck);
    const auto ack = decode_submit_job_ack(r);
    EXPECT_FALSE(ack.ok);
    EXPECT_NE(ack.message.find("protocol"), std::string::npos)
        << "the refusal does not name the incompatibility: " << ack.message;
    EXPECT_NE(counter_value(clink::metrics::kProtocolMismatches), before);

    conn->close();
    coordinator.stop();
}
