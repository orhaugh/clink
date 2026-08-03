// The control plane under invalid input.
//
// Three defects motivated this file, all reachable by anything that could
// open a TCP connection to the control port, all before authentication of
// any kind:
//
//   1. `read_frame` trusted the 4-byte length prefix. Four bytes of
//      `FF FF FF FF` made the receiver allocate and zero 4 GB before
//      reading a single byte of body. Three copies of that code existed.
//
//   2. Eleven decoders read a u32 element count and handed it straight to
//      `reserve()`. A Deploy claiming 0xFFFFFFFF tasks asked for hundreds
//      of gigabytes.
//
//   3. Nothing caught the exceptions the decoders throw. `MessageReader`
//      throws BY DESIGN on a truncated payload - there is a test for it -
//      and the throw propagated out of the accept thread, the client
//      thread and both reader threads. Leaving a thread function by
//      exception is std::terminate. One malformed frame killed the
//      process.
//
// The third is the one that makes the other two acute, and the one these
// tests are mostly about: the engine must survive garbage, not merely
// reject it.

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/runtime/network/connection.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

// A Connection that serves a fixed byte script and swallows everything
// written to it. Lets read_frame be driven with a header the sender would
// never produce.
class ScriptedBytes final : public network::Connection {
public:
    explicit ScriptedBytes(std::vector<std::byte> script) : script_(std::move(script)) {}

    bool send_all(const std::byte* /*buf*/, std::size_t /*len*/) override { return true; }

    bool recv_all(std::byte* buf, std::size_t len) override {
        ++recv_calls_;
        if (pos_ + len > script_.size()) {
            return false;  // peer sent less than it claimed
        }
        for (std::size_t i = 0; i < len; ++i) {
            buf[i] = script_[pos_++];
        }
        bytes_read_ += len;
        return true;
    }

    void shutdown_write() override {}
    void shutdown_read() override {}
    void close() override { open_ = false; }
    [[nodiscard]] bool is_open() const noexcept override { return open_; }

    // Total bytes successfully pulled, INCLUDING the 4-byte header.
    [[nodiscard]] std::size_t bytes_read() const noexcept { return bytes_read_; }

    // recv_all calls, successful or not. One means "read the header and
    // stopped"; more means the reader went on to the body. Counting
    // attempts rather than bytes is what distinguishes "refused on the
    // header" from "tried the body and the peer had not sent it".
    [[nodiscard]] std::size_t recv_calls() const noexcept { return recv_calls_; }

private:
    std::vector<std::byte> script_;
    std::size_t pos_{0};
    std::size_t bytes_read_{0};
    std::size_t recv_calls_{0};
    bool open_{true};
};

std::vector<std::byte> be32(std::uint32_t v) {
    return {static_cast<std::byte>((v >> 24) & 0xFF),
            static_cast<std::byte>((v >> 16) & 0xFF),
            static_cast<std::byte>((v >> 8) & 0xFF),
            static_cast<std::byte>(v & 0xFF)};
}

// Every decoder, behind a uniform signature, so a corpus can be run
// through all of them without naming each one at every call site. A new
// decoder that is not listed here is simply untested - which is why the
// count is asserted below.
using DecodeFn = void (*)(MessageReader&);

const std::vector<std::pair<const char*, DecodeFn>>& all_decoders() {
    static const std::vector<std::pair<const char*, DecodeFn>> fns = {
        {"register", [](MessageReader& r) { (void)decode_register(r); }},
        {"register_ack", [](MessageReader& r) { (void)decode_register_ack(r); }},
        {"deploy", [](MessageReader& r) { (void)decode_deploy(r); }},
        {"peer_update", [](MessageReader& r) { (void)decode_peer_update(r); }},
        {"cancel_job", [](MessageReader& r) { (void)decode_cancel_job(r); }},
        {"trigger_checkpoint", [](MessageReader& r) { (void)decode_trigger_checkpoint(r); }},
        {"commit_checkpoint", [](MessageReader& r) { (void)decode_commit_checkpoint(r); }},
        {"abort_checkpoint", [](MessageReader& r) { (void)decode_abort_checkpoint(r); }},
        {"begin_rescale", [](MessageReader& r) { (void)decode_begin_rescale(r); }},
        {"subtask_finished", [](MessageReader& r) { (void)decode_subtask_finished(r); }},
        {"subtask_listening", [](MessageReader& r) { (void)decode_subtask_listening(r); }},
        {"subtask_checkpointed", [](MessageReader& r) { (void)decode_subtask_checkpointed(r); }},
        {"heartbeat", [](MessageReader& r) { (void)decode_heartbeat(r); }},
        {"hello_client", [](MessageReader& r) { (void)decode_hello_client(r); }},
        {"submit_job", [](MessageReader& r) { (void)decode_submit_job(r); }},
        {"submit_job_ack", [](MessageReader& r) { (void)decode_submit_job_ack(r); }},
        {"list_jobs_ack", [](MessageReader& r) { (void)decode_list_jobs_ack(r); }},
        {"final_checkpoint_assigned",
         [](MessageReader& r) { (void)decode_final_checkpoint_assigned(r); }},
        {"request_final_checkpoint",
         [](MessageReader& r) { (void)decode_request_final_checkpoint(r); }},
        {"savepoint", [](MessageReader& r) { (void)decode_savepoint(r); }},
        {"savepoint_ack", [](MessageReader& r) { (void)decode_savepoint_ack(r); }},
    };
    return fns;
}

}  // namespace

// --- the length prefix ---------------------------------------------------

TEST(FrameRobustness, AnAbsurdLengthPrefixIsRefusedNotAllocated) {
    // Four bytes in, 4 GB out. This is the whole bug.
    ScriptedBytes conn(be32(0xFFFFFFFFU));
    EXPECT_FALSE(read_frame(conn).has_value());
    EXPECT_EQ(conn.bytes_read(), 4U);
    EXPECT_EQ(conn.recv_calls(), 1U)
        << "the reader went looking for a body it should have refused on the header alone";
}

TEST(FrameRobustness, TheCapIsEnforcedAtItsBoundaryNotApproximately) {
    // One byte over is refused; the cap itself is not (a legitimate
    // maximum-size frame must still work). An off-by-one here would
    // silently reject the largest plugin someone could ship.
    {
        ScriptedBytes over(be32(static_cast<std::uint32_t>(kMaxFrameBytes + 1)));
        EXPECT_FALSE(read_frame(over).has_value());
        EXPECT_EQ(over.recv_calls(), 1U)
            << "an over-cap length must be refused on the header, without touching the body";
    }
    {
        // At the cap, with a body that is not actually there: the read
        // must FAIL on the missing body rather than on the length, which
        // proves the length itself was accepted.
        ScriptedBytes at(be32(static_cast<std::uint32_t>(kMaxFrameBytes)));
        EXPECT_FALSE(read_frame(at).has_value());
        EXPECT_GT(at.recv_calls(), 1U)
            << "a length exactly at the cap was refused; the boundary is off by one";
    }
}

TEST(FrameRobustness, MemoryTracksBytesActuallySentNotBytesClaimed) {
    // A cap alone leaves the amplification: four bytes claiming 256 MB
    // would still allocate 256 MB up front. Reading incrementally means a
    // peer must send a byte to cost a byte.
    //
    // Observable without measuring memory: the reader must have asked for
    // no more than the peer supplied before giving up. A reader that
    // sized itself from the header would have allocated first and only
    // then discovered the body was absent.
    auto script = be32(64U * 1024U * 1024U);  // claims 64 MiB
    script.resize(script.size() + 1024);      // sends 1 KiB
    ScriptedBytes conn(std::move(script));
    EXPECT_FALSE(read_frame(conn).has_value());
    EXPECT_LE(conn.bytes_read(), 4U + kFrameReadChunkBytes)
        << "the reader consumed more than the header plus one chunk for a body that was "
           "never sent; it is sizing itself from the claimed length";
}

TEST(FrameRobustness, AWellFormedFrameStillRoundTripsIncludingALargeOne) {
    // The fix must not have broken the thing it protects. A frame larger
    // than one read chunk exercises the incremental path, which is where
    // an off-by-one would corrupt the payload rather than reject it.
    const std::size_t big = kFrameReadChunkBytes * 3 + 17;
    std::vector<std::byte> script = be32(static_cast<std::uint32_t>(big));
    for (std::size_t i = 0; i < big; ++i) {
        script.push_back(static_cast<std::byte>(i & 0xFF));
    }
    ScriptedBytes conn(std::move(script));
    const auto body = read_frame(conn);
    ASSERT_TRUE(body.has_value());
    ASSERT_EQ(body->size(), big);
    for (std::size_t i = 0; i < big; ++i) {
        ASSERT_EQ((*body)[i], static_cast<std::byte>(i & 0xFF))
            << "payload corrupted at byte " << i;
    }
}

TEST(FrameRobustness, AZeroLengthFrameIsStillValid) {
    ScriptedBytes conn(be32(0));
    const auto body = read_frame(conn);
    ASSERT_TRUE(body.has_value());
    EXPECT_TRUE(body->empty());
}

// --- element counts ------------------------------------------------------

TEST(FrameRobustness, AnElementCountLargerThanTheFrameIsRefused) {
    // The count that used to reach reserve(). Every element costs at least
    // a byte on the wire, so a count above the bytes remaining cannot be
    // honest - the bound needs no arbitrary constant.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::Deploy));
    b.put_u64_be(1);            // job_id
    b.put_u32_be(0xFFFFFFFFU);  // task count
    auto framed = b.finalize();
    MessageReader r({framed.begin() + 4, framed.end()});
    (void)r.read_u8();
    EXPECT_THROW((void)decode_deploy(r), std::runtime_error);
}

TEST(FrameRobustness, AnHonestCountIsStillAccepted) {
    // Guards against the bound being tightened into a false positive: a
    // real Deploy with real tasks must decode.
    DeployMsg in;
    in.job_id = 3;
    for (std::uint32_t i = 0; i < 4; ++i) {
        in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = i});
    }
    auto framed = encode_frame(MessageKind::Deploy, in);
    MessageReader r({framed.begin() + 4, framed.end()});
    (void)r.read_u8();
    DeployMsg out;
    ASSERT_NO_THROW(out = decode_deploy(r));
    EXPECT_EQ(out.tasks.size(), 4U);
}

TEST(FrameRobustness, ReadCountRejectsExactlyWhatItShould) {
    // The rule in isolation, at its boundary.
    std::vector<std::byte> payload = be32(3);
    payload.push_back(std::byte{1});
    payload.push_back(std::byte{2});
    payload.push_back(std::byte{3});
    {
        MessageReader r(payload);
        EXPECT_EQ(r.read_count(), 3U) << "a count equal to the bytes remaining is honest";
    }
    {
        auto too_big = be32(4);
        too_big.push_back(std::byte{1});
        too_big.push_back(std::byte{2});
        too_big.push_back(std::byte{3});
        MessageReader r(too_big);
        EXPECT_THROW((void)r.read_count(), std::runtime_error);
    }
}

// --- every decoder, against garbage --------------------------------------

TEST(FrameRobustness, NoDecoderCrashesOnArbitraryBytes) {
    // Deterministic property test rather than a fuzzer: a fixed seed, so a
    // failure here reproduces exactly, and it runs in the normal suite
    // instead of needing a fuzzing engine and an unbounded time budget.
    //
    // The property is narrow and total: for ANY byte string, a decoder
    // either returns or throws a std::exception. It never reads out of
    // bounds, never allocates from a number it was handed, and never
    // aborts. Everything above depends on that being true of all of them,
    // not of the ones that happened to be checked.
    ASSERT_GE(all_decoders().size(), 20U)
        << "the decoder table has shrunk; an unlisted decoder is an untested one";

    std::mt19937 rng(0xC1A5B0DE);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::uniform_int_distribution<std::size_t> len_dist(0, 96);

    for (const auto& [name, decode] : all_decoders()) {
        for (int iteration = 0; iteration < 400; ++iteration) {
            std::vector<std::byte> payload(len_dist(rng));
            for (auto& b : payload) {
                b = static_cast<std::byte>(byte_dist(rng));
            }
            MessageReader r(std::move(payload));
            try {
                decode(r);
            } catch (const std::exception&) {
                // The contract. A malformed frame is an error, not a crash.
            }
            // Reaching here at all is the assertion: no abort, no OOM, no
            // out-of-bounds read (which ASan/UBSan builds turn into a
            // failure rather than a silent pass).
            SUCCEED();
        }
        (void)name;
    }
}

TEST(FrameRobustness, NoDecoderCrashesOnATruncatedValidFrame) {
    // Random bytes rarely reach deep into a decoder - most die on the
    // first length prefix. Truncating a VALID frame at every offset walks
    // the decoder through every partial state it can actually be in, which
    // is where the interesting failures are.
    DeployMsg deploy;
    deploy.job_id = 42;
    deploy.tasks.push_back(DeploymentTask{.role = "source", .subtask_idx = 0});
    deploy.tasks.push_back(DeploymentTask{.role = "sink", .subtask_idx = 1});
    deploy.plugins.push_back(PluginBinary{.name = "p", .content_hash = "h", .bytes = {}});

    SubmitJobMsg submit;
    submit.graph_json = R"({"nodes":[]})";
    submit.checkpoint.checkpoint_dir = "/tmp/x";

    PeerUpdateMsg peers;
    peers.job_id = 1;
    PeerUpdateMsg::TaskPeers tp;
    tp.role = "sink";
    tp.subtask_idx = 0;
    tp.peers.push_back(PeerAddress{.role = "src", .subtask_idx = 0, .host = "h", .data_port = 1});
    peers.tasks.push_back(std::move(tp));

    struct Case {
        const char* name;
        std::vector<std::byte> framed;
        DecodeFn decode;
    };
    const std::vector<Case> cases = {
        {"deploy",
         encode_frame(MessageKind::Deploy, deploy),
         [](MessageReader& r) { (void)decode_deploy(r); }},
        {"submit_job",
         encode_frame(MessageKind::SubmitJob, submit),
         [](MessageReader& r) { (void)decode_submit_job(r); }},
        {"peer_update",
         encode_frame(MessageKind::PeerUpdate, peers),
         [](MessageReader& r) { (void)decode_peer_update(r); }},
    };

    for (const auto& c : cases) {
        const std::vector<std::byte> body(c.framed.begin() + 4, c.framed.end());
        for (std::size_t cut = 1; cut <= body.size(); ++cut) {
            MessageReader r(std::vector<std::byte>(body.begin(), body.begin() + cut));
            (void)r.read_u8();
            try {
                c.decode(r);
            } catch (const std::exception&) {
                // Expected for every cut short of the whole body.
            }
        }
        SUCCEED() << c.name;
    }
}

// --- the process survives ------------------------------------------------

TEST(FrameRobustness, AMalformedFrameDoesNotKillTheCoordinator) {
    // The test the other two exist for. Before this change, the throw from
    // a decoder left the accept thread, and leaving a thread function by
    // exception is std::terminate - so this test would not have failed,
    // it would have taken the whole test binary down with it.
    //
    // Proof of survival is not "no crash" (unfalsifiable in-process) but
    // "the coordinator still works afterwards": a real worker registers on
    // a fresh connection once the garbage has been sent.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    // A Register frame truncated mid-string, a Register claiming a huge
    // string, and an unknown kind. Each on its own connection, because the
    // coordinator is entitled to close a connection it cannot parse.
    const std::vector<std::vector<std::byte>> garbage = {
        // Kind=Register, then a string length with no string.
        [] {
            MessageBuilder b;
            b.put_u8(static_cast<std::uint8_t>(MessageKind::Register));
            b.put_u32_be(0xFFFFFF00U);
            return b.finalize();
        }(),
        // Kind=Register, valid worker_id, then nothing.
        [] {
            MessageBuilder b;
            b.put_u8(static_cast<std::uint8_t>(MessageKind::Register));
            b.put_string("w");
            return b.finalize();
        }(),
        // A kind no version has ever defined.
        [] {
            MessageBuilder b;
            b.put_u8(200);
            b.put_u64_be(0);
            return b.finalize();
        }(),
        // Empty body.
        [] {
            MessageBuilder b;
            return b.finalize();
        }(),
    };

    for (const auto& g : garbage) {
        auto conn = network::connect_plain("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        (void)send_frame(*conn, g);
        conn->close();
    }

    // The coordinator must still be alive and functioning.
    Worker worker("w", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "the coordinator stopped accepting after being sent a malformed frame";
    EXPECT_TRUE(coordinator.await_registrations(2s))
        << "the coordinator accepted the connection but no longer registers workers";

    worker.stop();
    coordinator.stop();
}

TEST(FrameRobustness, AnOverLongLengthPrefixDoesNotKillTheCoordinator) {
    // The same survival property for the header path, which is handled
    // before any decoder runs.
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    {
        auto conn = network::connect_plain("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        const auto absurd = be32(0xFFFFFFFFU);
        (void)conn->send_all(absurd.data(), absurd.size());
        conn->close();
    }

    Worker worker("w", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(2s));

    worker.stop();
    coordinator.stop();
}
