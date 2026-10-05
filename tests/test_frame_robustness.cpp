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
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>

#include "clink/cluster/admission_policy.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/network/network_socket.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

// Raises the soft descriptor limit to at least `want` for the life of a test,
// restoring it after. ok() is false when the hard limit leaves no room, which
// the caller skips on. macOS starts a process at a soft limit of 256.
class FrameRobustnessDescriptorLimit {
public:
    explicit FrameRobustnessDescriptorLimit(rlim_t want) {
        if (::getrlimit(RLIMIT_NOFILE, &saved_) != 0) {
            return;
        }
        if (saved_.rlim_cur != RLIM_INFINITY && saved_.rlim_cur < want) {
            if (saved_.rlim_max != RLIM_INFINITY && saved_.rlim_max < want) {
                return;
            }
            rlimit raised = saved_;
            raised.rlim_cur = want;
            if (::setrlimit(RLIMIT_NOFILE, &raised) != 0) {
                return;
            }
            restore_ = true;
        }
        ok_ = true;
    }
    ~FrameRobustnessDescriptorLimit() {
        if (restore_) {
            ::setrlimit(RLIMIT_NOFILE, &saved_);
        }
    }
    FrameRobustnessDescriptorLimit(const FrameRobustnessDescriptorLimit&) = delete;
    FrameRobustnessDescriptorLimit& operator=(const FrameRobustnessDescriptorLimit&) = delete;

    [[nodiscard]] bool ok() const { return ok_; }

private:
    rlimit saved_{};
    bool restore_{false};
    bool ok_{false};
};

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

// Wait for `pred` or give up. The deadline is a failure bound, not a
// synchronisation mechanism.
template <typename Pred>
bool fencing_await(Pred pred, std::chrono::milliseconds bound = std::chrono::seconds{2}) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return pred();
}

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

TEST(FrameRobustness, ClientConnectionsAreReapedRatherThanAccumulated) {
    // Every client used to leave a joinable std::thread and a shared_ptr
    // behind for the coordinator's whole lifetime - the two vectors
    // holding them were drained only by stop(). A script polling
    // `clink list` once a second grew the process by 86,400 thread
    // handles a day until thread creation started failing.
    //
    // Nothing crashed while that was true, which is why the assertion has
    // to be on the count rather than on survival.
    Coordinator coordinator;
    const auto port = coordinator.start();

    constexpr int kCycles = 40;
    for (int i = 0; i < kCycles; ++i) {
        auto conn = network::connect_plain("127.0.0.1", port);
        ASSERT_NE(conn, nullptr) << "connect failed on cycle " << i
                                 << "; the coordinator may already have run out of threads";
        HelloClientMsg hello;
        ASSERT_TRUE(send_frame(*conn, encode_frame(MessageKind::HelloClient, hello)));
        conn->close();
    }

    // Reaping is driven by ADMISSION - a session is joined and dropped when
    // the next client arrives - so waiting for the count to fall on its own
    // cannot work: after the last client is admitted there is nothing left
    // to do the reaping. The first version of this test waited anyway and
    // failed on Linux holding 6 of 40, which is reaping working and the
    // assertion being wrong.
    //
    // So drive it. Each probe admits a client, which reaps whatever has
    // finished; the loop is bounded, so what is proven is that reaping
    // makes progress rather than that it happened before a deadline. The
    // floor is 1 - the probe that has not been reaped yet - so 2 is the
    // headroom for one straggler.
    EXPECT_TRUE(fencing_await(
        [&] {
            auto probe = network::connect_plain("127.0.0.1", port);
            if (probe != nullptr) {
                (void)send_frame(*probe, encode_frame(MessageKind::HelloClient, HelloClientMsg{}));
                probe->close();
            }
            return coordinator.client_session_count() <= 2;
        },
        5s))
        << "after " << kCycles
        << " connect/disconnect cycles, and repeated admissions to drive the reaper, the "
           "coordinator still holds "
        << coordinator.client_session_count()
        << " client sessions; they are accumulating rather than being reaped";

    coordinator.stop();
}

TEST(FrameRobustness, TheCoordinatorRefusesClientsBeyondItsLimitRatherThanSpawningThem) {
    // The bound that makes the reaping meaningful: concurrent clients,
    // not total clients, and a refusal a caller can read rather than a
    // silent close or an unbounded thread pool.
    Coordinator::Config cfg;
    cfg.max_client_connections = 2;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    // Hold two open. These stay connected, so nothing is reapable.
    std::vector<std::unique_ptr<network::Connection>> held;
    for (int i = 0; i < 2; ++i) {
        auto conn = network::connect_plain("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        ASSERT_TRUE(send_frame(*conn, encode_frame(MessageKind::HelloClient, HelloClientMsg{})));
        held.push_back(std::move(conn));
    }
    ASSERT_TRUE(fencing_await([&] { return coordinator.client_session_count() == 2; }));

    // The third must be refused, and told why.
    auto extra = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(extra, nullptr);
    ASSERT_TRUE(send_frame(*extra, encode_frame(MessageKind::HelloClient, HelloClientMsg{})));
    auto reply = read_frame(*extra);
    ASSERT_TRUE(reply.has_value()) << "the coordinator closed on the third client without a reason";
    MessageReader rr(std::move(*reply));
    ASSERT_EQ(static_cast<MessageKind>(rr.read_u8()), MessageKind::SubmitJobAck);
    const auto ack = decode_submit_job_ack(rr);
    EXPECT_FALSE(ack.ok);
    EXPECT_NE(ack.message.find("client-connection limit"), std::string::npos) << ack.message;

    EXPECT_EQ(coordinator.client_session_count(), 2U) << "the refused client was admitted anyway";

    // And once one goes away, a new client fits again - the limit is on
    // concurrency, not a lifetime quota.
    held.front()->close();
    held.erase(held.begin());
    auto after = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(after, nullptr);
    ASSERT_TRUE(send_frame(*after, encode_frame(MessageKind::HelloClient, HelloClientMsg{})));
    EXPECT_TRUE(fencing_await([&] { return coordinator.client_session_count() == 2; }))
        << "a slot freed by a departing client was never reclaimed";

    after->close();
    for (auto& c : held) {
        c->close();
    }
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

// --- A silent connection must not wedge admission --------------------
//
// The coordinator reads a new peer's FIRST frame on the ACCEPT THREAD. That makes
// a connection which opens a socket and sends nothing the cheapest denial there
// is: one such connection parks the only thread that admits anything, so no
// client connects and no worker registers - and max_client_connections, the cap
// meant to bound exactly this, becomes unreachable, because reaching it requires
// being admitted.
//
// Not a rate problem, which is how the follow-up item framed it. A rate limit
// would not have helped: the peer sends no frames at all.

TEST(FrameRobustness, AConnectionThatSendsNothingDoesNotStopAdmission) {
    Coordinator::Config cfg;
    // Short, so the test does not wait out a production liveness window. This is
    // the same knob the fix reuses as its deadline, which is the point: the bound
    // is the coordinator's own definition of a live peer, not a new number.
    cfg.heartbeat_timeout = 300ms;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-silent"});

    // Open a socket and send NOTHING. Held open for the rest of the test.
    auto silent = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(silent, nullptr);

    // A second, well-behaved peer must still be admitted. Pre-fix this never
    // returns: the accept thread is blocked in the silent connection's first
    // read, so the registration is never even seen.
    Worker worker("w-silent", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "a peer that opened a connection and sent nothing blocked admission entirely";
    EXPECT_TRUE(coordinator.await_registrations(5s))
        << "the coordinator never registered a worker while one silent connection was open; the "
           "accept thread is parked on that connection's first frame";

    silent->close();
    worker.stop();
    coordinator.stop();
}

TEST(FrameRobustness, AFirstFrameHeaderWithNoBodyIsDroppedRatherThanHeld) {
    // The same wedge, one step further in: a peer that sends a length header and
    // then stalls. read_frame has the length and blocks for the body, still on the
    // accept thread. The connection must be dropped, and must not be counted as a
    // client session - a half-open peer that occupies a slot in the cap is a
    // slower version of the same denial.
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = 300ms;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    auto stalled = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(stalled, nullptr);
    // A 4-byte big-endian length claiming a 64-byte body, then nothing.
    const std::array<std::byte, 4> hdr{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{64}};
    ASSERT_TRUE(stalled->send_all(hdr.data(), hdr.size()));

    // Admission still works.
    Worker worker("w-stalled", "127.0.0.1");
    coordinator.expect_workers({"w-stalled"});
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "a peer that sent a header and stalled blocked admission";
    EXPECT_TRUE(coordinator.await_registrations(5s));

    // And the stalled peer was never admitted as a client.
    EXPECT_EQ(coordinator.client_session_count(), 0u)
        << "a peer that never completed its first frame was counted as a client session, so it "
           "occupies a slot in max_client_connections";

    stalled->close();
    worker.stop();
    coordinator.stop();
}

// --- Admission is bounded per connection, and in parallel -----------------
//
// The per-read bound above stops a peer that sends nothing. It does not stop a
// peer that sends a byte at a time, each inside heartbeat_timeout: every read
// succeeds, and read_frame never finishes. And while the accept thread read
// every first frame itself, one such peer, or a queue of silent ones each
// costing a full heartbeat_timeout, held up every registration behind it. Each
// connection is now admitted on a thread of its own, under a deadline on the
// whole of its admission.

TEST(FrameRobustness, AFirstFrameSentAByteAtATimeIsDroppedAtTheAdmissionDeadline) {
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = 300ms;
    cfg.handshake_timeout = 0ms;  // plain TCP: the admission deadline is heartbeat_timeout
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    auto dribbler = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(dribbler, nullptr);
    // A header claiming 32 KiB (under kMaxFirstFrameBytes, so it is read),
    // then one body byte every 50 ms: each read the coordinator makes
    // succeeds well inside the 300 ms per-read bound.
    const auto hdr = be32(32U * 1024U);
    ASSERT_TRUE(dribbler->send_all(hdr.data(), hdr.size()));
    const auto started = std::chrono::steady_clock::now();
    const std::byte one{0x01};
    bool closed = false;
    // Until the coordinator closes the connection (a send fails), or 10 s.
    for (int i = 0; i < 200 && !closed; ++i) {
        std::this_thread::sleep_for(50ms);
        closed = !dribbler->send_all(&one, 1);
    }
    EXPECT_TRUE(closed) << "a peer sending its first frame a byte at a time was never dropped: "
                           "the first frame is bounded per read, not as a whole";
    EXPECT_LT(std::chrono::steady_clock::now() - started, 5s);
    EXPECT_EQ(coordinator.client_session_count(), 0u);

    dribbler->close();
    coordinator.stop();
}

TEST(FrameRobustness, SilentConnectionsQueuedAheadOfAWorkerDoNotDelayIt) {
    Coordinator::Config cfg;
    // Each silent connection used to cost the accept thread this long, one
    // after another, before it reached the worker queued behind them.
    cfg.heartbeat_timeout = 3s;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-queued"});

    // Connected before the worker, so they are ahead of it in the listener's
    // queue. Four of them were twelve seconds of admission in sequence.
    std::vector<std::unique_ptr<network::Connection>> silent;
    for (int i = 0; i < 4; ++i) {
        silent.push_back(network::connect_plain("127.0.0.1", port));
        ASSERT_NE(silent.back(), nullptr);
    }

    Worker worker("w-queued", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    const auto started = std::chrono::steady_clock::now();
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(5s));
    EXPECT_LT(std::chrono::steady_clock::now() - started, cfg.heartbeat_timeout)
        << "a worker waited behind silent connections accepted before it: admission is "
           "serial, so each one costs every later connection a whole first-frame timeout";

    for (auto& c : silent) {
        c->close();
    }
    worker.stop();
    coordinator.stop();
}

// --- What an admission may hold, and for how long -------------------------
//
// Admission threads run in parallel, so whatever one of them may hold, the
// coordinator may hold max_pending_connections times over: a first frame
// read under the 256 MiB cap a plugin-carrying frame needs, a thread whose
// frame is in and that waits to be registered, an exception that leaves the
// thread function. Each of these was bounded, or caught, while the accept
// thread did all of it one connection at a time.

namespace {

// True if the peer closes `conn` well inside `bound`: its next read sees the
// end of the stream rather than the receive timeout.
bool closed_by_coordinator_within(network::Connection& conn, std::chrono::milliseconds bound) {
    if (!conn.set_recv_timeout(bound)) {
        return false;
    }
    std::byte b{};
    const auto from = std::chrono::steady_clock::now();
    const bool got = conn.recv_all(&b, 1);
    return !got && std::chrono::steady_clock::now() - from < bound / 2;
}

constexpr std::chrono::milliseconds kAdmissionBound{10000};

}  // namespace

TEST(FrameRobustness, AFirstFrameClaimingMoreThanTheFirstFrameCapIsDroppedUnread) {
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;  // only the cap can end this in time
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();

    auto big = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(big, nullptr);
    const auto hdr = be32(static_cast<std::uint32_t>(kMaxFirstFrameBytes + 1));
    ASSERT_TRUE(big->send_all(hdr.data(), hdr.size()));
    EXPECT_TRUE(closed_by_coordinator_within(*big, kAdmissionBound))
        << "a first frame claiming more than kMaxFirstFrameBytes was waited for: an "
           "unauthenticated peer may make the coordinator buffer up to the 256 MiB frame cap, on "
           "every connection being admitted at once";

    big->close();
    coordinator.stop();
}

TEST(FrameRobustness, FirstFramesWaitingToBeHandledAreBoundedByTheCeiling) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.max_pending_connections = 2;
    cfg.heartbeat_timeout = kAdmissionBound;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    fault::Registry::instance().reset();
    // The first frame handled is held inside the first-frame lock, as a
    // registration waiting a long time for the coordinator's lock would be.
    fault::ScopedFault hold{fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeHandle,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kCoordinatorAdmissionQueued,
                                                .action = fault::Action::Observe});
    const auto hello = encode_frame(MessageKind::HelloClient, HelloClientMsg{});

    std::vector<std::unique_ptr<network::Connection>> waiting;
    // The first is held inside the lock; the rest, their frames in, wait
    // behind it. From one address, whose admissions all wait only on
    // registration, they are admitted past the cap, up to the ceiling.
    const auto ceiling = admission_ceiling(cfg.max_pending_connections);
    for (std::size_t i = 1; i <= ceiling; ++i) {
        waiting.push_back(network::connect_plain("127.0.0.1", port));
        ASSERT_NE(waiting.back(), nullptr);
        ASSERT_TRUE(send_frame(*waiting.back(), hello));
        ASSERT_TRUE(fencing_await(
            [i] {
                return fault::Registry::instance().hits(
                           fault::points::kCoordinatorAdmissionQueued) >= i;
            },
            kAdmissionBound))
            << "connection " << i << " was not admitted below the ceiling";
    }

    // The ceiling is held by connections whose first frames are in, which
    // nothing may evict: the next is refused rather than given a thread.
    auto over = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(over, nullptr);
    EXPECT_TRUE(closed_by_coordinator_within(*over, kAdmissionBound))
        << "a connection was admitted beyond the ceiling while it was held by first frames "
           "waiting to be handled: those threads are not counted, so nothing bounds them while "
           "registration is slow";

    fault::Registry::instance().release(fault::points::kCoordinatorAdmissionBeforeHandle);
    over->close();
    for (auto& c : waiting) {
        c->close();
    }
    coordinator.stop();
}

TEST(FrameRobustness, ARegistrationFromAnOlderConnectionDoesNotReplaceANewerSession) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-twice"});
    fault::Registry::instance().reset();
    fault::ScopedFault park{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeFirstFrame,
                    .ordinal = 1,
                    .action = fault::Action::Block}};

    // The worker's first attempt: its Register is sent, and its admission is
    // held before reading it, as one behind a slow registration would be.
    auto stale = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(stale, nullptr);
    RegisterMsg reg;
    reg.worker_id = "w-twice";
    reg.data_host = "127.0.0.1";
    ASSERT_TRUE(send_frame(*stale, encode_frame(MessageKind::Register, reg)));
    ASSERT_TRUE(fencing_await(
        [] {
            return fault::Registry::instance().hits(
                       fault::points::kCoordinatorAdmissionBeforeFirstFrame) >= 1;
        },
        kAdmissionBound));

    // Its second attempt, on a newer connection, registers first.
    Worker worker("w-twice", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    ASSERT_TRUE(coordinator.await_registrations(5s));

    // Then the first attempt's frame is handled.
    fault::Registry::instance().release(fault::points::kCoordinatorAdmissionBeforeFirstFrame);
    ASSERT_TRUE(stale->set_recv_timeout(kAdmissionBound));
    auto answer = read_frame(*stale);
    ASSERT_TRUE(answer.has_value()) << "the older registration got no answer";
    MessageReader r(std::move(*answer));
    ASSERT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::RegisterAck);
    const auto ack = decode_register_ack(r);
    EXPECT_FALSE(ack.ok) << "a registration from a connection accepted before the live session's "
                            "replaced that session: the live worker is torn down for one that "
                            "had already given up";

    stale->close();
    worker.stop();
    coordinator.stop();
}

TEST(FrameRobustness, AThrowOnAnAdmissionThreadDropsOneConnectionNotTheCoordinator) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-after-throw"});
    fault::Registry::instance().reset();
    // Stands in for a std::bad_alloc while a first frame grows, or a throw
    // from a custom Connection: anything that leaves the admission thread
    // function by exception is std::terminate.
    fault::ScopedFault boom{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeFirstFrame,
                    .ordinal = 1,
                    .action = fault::Action::Throw}};

    auto victim = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(victim, nullptr);
    EXPECT_TRUE(closed_by_coordinator_within(*victim, kAdmissionBound))
        << "the connection whose admission threw was not dropped";

    Worker worker("w-after-throw", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(5s));

    victim->close();
    worker.stop();
    coordinator.stop();
}

TEST(FrameRobustness, AnAdmissionThreadThatCannotStartReleasesItsConnection) {
    namespace fault = clink::fault;
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-after-no-thread"});
    fault::Registry::instance().reset();
    // Stands in for std::thread failing to start (EAGAIN under thread
    // exhaustion), with the admission already published.
    fault::ScopedFault no_thread{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeThreadStart,
                    .ordinal = 1,
                    .action = fault::Action::Throw}};

    auto victim = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(victim, nullptr);
    EXPECT_TRUE(closed_by_coordinator_within(*victim, kAdmissionBound))
        << "a connection whose admission thread could not start was left open";

    Worker worker("w-after-no-thread", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(5s));

    victim->close();
    worker.stop();
    coordinator.stop();  // no pending admission is left for it to wait on
}

TEST(FrameRobustness, AHandshakeTimeoutTooLargeForTheClockIsNoDeadline) {
    // About 317 years: an operator's "no limit", and milliseconds that
    // overflow the clock's signed nanoseconds when added to it. The admission
    // deadline used to wrap into the past, closing every connection as soon
    // as it was accepted.
    Coordinator::Config cfg;
    cfg.handshake_timeout = std::chrono::milliseconds{10'000'000'000'000};
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-no-deadline"});

    Worker worker("w-no-deadline", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "a handshake timeout too large for the clock made every admission overdue at once";
    EXPECT_TRUE(coordinator.await_registrations(5s));

    worker.stop();
    coordinator.stop();
}

TEST(FrameRobustness, DeadlineAfterSaturatesRatherThanOverflowing) {
    using clock = std::chrono::steady_clock;
    EXPECT_EQ(network::NetworkSocket::deadline_after(std::chrono::milliseconds::max()),
              clock::time_point::max());
    EXPECT_EQ(network::NetworkSocket::deadline_after(std::chrono::milliseconds{99'999'999'999'999}),
              clock::time_point::max());
    const auto before = clock::now();
    const auto in_a_second = network::NetworkSocket::deadline_after(1s);
    EXPECT_GE(in_a_second, before + 1s);
    EXPECT_LE(in_a_second, clock::now() + 1s);
    // Each deadline is taken before the clock it is compared with: within one
    // EXPECT the compiler may read the clock first, and GCC on x86-64 does.
    const auto at_once = network::NetworkSocket::deadline_after(0ms);
    EXPECT_LE(at_once, clock::now());
    const auto in_the_past = network::NetworkSocket::deadline_after(-5ms);
    EXPECT_LE(in_the_past, clock::now());
}

// --- Who gives way when the admission set is full -------------------------
//
// Evicting the oldest, whoever sent it, let one host that could reach the
// port evict every worker with a connect loop that sent nothing, and let a
// stranger evict a worker whose TLS handshake was done.

TEST(AdmissionPolicy, AFloodFromOneAddressEvictsItsOwnConnections) {
    const std::vector<AdmissionCandidate> pending = {
        {.id = 1, .source = "10.0.0.1", .progress = AdmissionProgress::Accepted},  // a worker
        {.id = 2, .source = "10.0.0.9", .progress = AdmissionProgress::Accepted},
        {.id = 3, .source = "10.0.0.9", .progress = AdmissionProgress::Accepted},
        {.id = 4, .source = "10.0.0.9", .progress = AdmissionProgress::Accepted},
    };
    // The worker is the oldest, but its address holds one admission.
    EXPECT_EQ(choose_admission_to_evict(pending, "10.0.0.9"), std::optional<std::uint64_t>{2});
    EXPECT_EQ(choose_admission_to_evict(pending, "10.0.0.7"), std::optional<std::uint64_t>{2});
}

TEST(AdmissionPolicy, AConnectionFurtherAlongIsNotEvictedForOneThatHasDoneNothing) {
    const std::vector<AdmissionCandidate> mixed = {
        {.id = 1, .source = "127.0.0.1", .progress = AdmissionProgress::Handshaken},
        {.id = 2, .source = "127.0.0.1", .progress = AdmissionProgress::FrameStarted},
        {.id = 3, .source = "127.0.0.1", .progress = AdmissionProgress::Accepted},
    };
    EXPECT_EQ(choose_admission_to_evict(mixed, "127.0.0.1"), std::optional<std::uint64_t>{3});

    // Every one has completed its handshake: the new connection, which has
    // not, is refused rather than allowed to evict one.
    const std::vector<AdmissionCandidate> all_handshaken = {
        {.id = 1, .source = "127.0.0.1", .progress = AdmissionProgress::Handshaken},
        {.id = 2, .source = "127.0.0.1", .progress = AdmissionProgress::FrameStarted},
    };
    EXPECT_EQ(choose_admission_to_evict(all_handshaken, "127.0.0.1"), std::nullopt);
}

TEST(AdmissionPolicy, AFirstFrameWaitingToBeHandledIsNeverEvictedButCountsTowardsItsShare) {
    const std::vector<AdmissionCandidate> all_queued = {
        {.id = 1, .source = "10.0.0.1", .progress = AdmissionProgress::Queued},
        {.id = 2, .source = "10.0.0.2", .progress = AdmissionProgress::Queued},
    };
    EXPECT_EQ(choose_admission_to_evict(all_queued, "10.0.0.3"), std::nullopt);

    // 10.0.0.1 holds three admissions, two of them queued, so its one that
    // can be evicted goes before the older one from 10.0.0.2.
    const std::vector<AdmissionCandidate> shares = {
        {.id = 1, .source = "10.0.0.2", .progress = AdmissionProgress::Accepted},
        {.id = 2, .source = "10.0.0.1", .progress = AdmissionProgress::Queued},
        {.id = 3, .source = "10.0.0.1", .progress = AdmissionProgress::Queued},
        {.id = 4, .source = "10.0.0.1", .progress = AdmissionProgress::Accepted},
    };
    EXPECT_EQ(choose_admission_to_evict(shares, "10.0.0.3"), std::optional<std::uint64_t>{4});
}

TEST(AdmissionPolicy, AmongEqualsTheOldestGivesWayToTheNewConnection) {
    // One admission per address, none past its handshake: the oldest goes, so
    // a stream of stalling connections from many addresses cannot hold every
    // slot against a worker.
    const std::vector<AdmissionCandidate> pending = {
        {.id = 7, .source = "10.0.0.1", .progress = AdmissionProgress::Accepted},
        {.id = 5, .source = "10.0.0.2", .progress = AdmissionProgress::Accepted},
        {.id = 9, .source = "10.0.0.3", .progress = AdmissionProgress::Handshaken},
    };
    EXPECT_EQ(choose_admission_to_evict(pending, "10.0.0.4"), std::optional<std::uint64_t>{5});
}

TEST(AdmissionPolicy, FirstFramesQueuedFromOneAddressDoNotTurnAwayAnother) {
    // A slow registration holds the first-frame lock, and one host has filled
    // every slot with first frames that cannot be evicted. A worker from
    // another host was refused for as long as that lasted.
    std::vector<AdmissionCandidate> queued;
    for (std::uint64_t id = 1; id <= 64; ++id) {
        queued.push_back({.id = id, .source = "10.0.0.9", .progress = AdmissionProgress::Queued});
    }
    EXPECT_EQ(decide_admission(queued, "10.0.0.1", 64).kind, AdmissionDecision::Kind::Admit)
        << "a newcomer from an address with no admission was refused because another address "
           "held every slot with queued first frames";
}

TEST(AdmissionPolicy, AHostsOnlyAdmissionIsNotEvictedWhileAnotherHostsQueueFillsTheCap) {
    // One host's first frames, waiting on registration, fill all but one
    // slot; a worker from a host of its own holds the last, mid-handshake. A
    // worker from a third host evicted it, the two tying on one admission
    // each, and the next worker from elsewhere evicted that one in turn.
    std::vector<AdmissionCandidate> pending;
    for (std::uint64_t id = 1; id <= 63; ++id) {
        pending.push_back({.id = id, .source = "10.0.0.9", .progress = AdmissionProgress::Queued});
    }
    pending.push_back({.id = 64, .source = "10.0.0.1", .progress = AdmissionProgress::Accepted});
    EXPECT_EQ(decide_admission(pending, "10.0.0.2", 64).kind, AdmissionDecision::Kind::Admit)
        << "a host's only admission was evicted although another host held more";
    for (std::uint64_t id = 65; id <= 100; ++id) {
        pending.push_back({.id = id, .source = "10.0.0.9", .progress = AdmissionProgress::Queued});
    }
    EXPECT_EQ(decide_admission(pending, "10.0.0.2", 64).kind, AdmissionDecision::Kind::Admit);

    // The busy host's own connection still waiting on its peer goes first.
    pending.push_back({.id = 101, .source = "10.0.0.9", .progress = AdmissionProgress::Accepted});
    const auto own = decide_admission(pending, "10.0.0.2", 64);
    EXPECT_EQ(own.kind, AdmissionDecision::Kind::Evict);
    EXPECT_EQ(own.victim, 101u);

    // When every host holds one, the oldest that got least far still gives
    // way, so stalling connections from many hosts cannot hold every slot.
    std::vector<AdmissionCandidate> one_each;
    for (std::uint64_t id = 1; id <= 64; ++id) {
        one_each.push_back({.id = id,
                            .source = "10.0.1." + std::to_string(id),
                            .progress = AdmissionProgress::Accepted});
    }
    const auto spread = decide_admission(one_each, "10.0.0.2", 64);
    EXPECT_EQ(spread.kind, AdmissionDecision::Kind::Evict);
    EXPECT_EQ(spread.victim, 1u);
}

TEST(AdmissionPolicy, ABurstFromOneAddressWaitingOnRegistrationIsAdmittedUpToTheCeiling) {
    // Workers starting together on one host: their first frames are all in,
    // and wait while registration works through them. Everything past the
    // cap used to be refused, where the listen backlog had held it before.
    std::vector<AdmissionCandidate> queued;
    for (std::uint64_t id = 1; id <= 64; ++id) {
        queued.push_back({.id = id, .source = "10.0.0.9", .progress = AdmissionProgress::Queued});
    }
    EXPECT_EQ(decide_admission(queued, "10.0.0.9", 64).kind, AdmissionDecision::Kind::Admit)
        << "a connection from an address whose admissions all wait only on registration was "
           "refused at the cap";

    // Not while one of them still waits on its peer, and is further along
    // than the newcomer: that is what a peer stalling looks like.
    auto stalling = queued;
    stalling.back().progress = AdmissionProgress::FrameStarted;
    EXPECT_EQ(decide_admission(stalling, "10.0.0.9", 64).kind, AdmissionDecision::Kind::Refuse);

    // And only up to the ceiling, where the address is held to its share.
    for (std::uint64_t id = 65; id <= admission_ceiling(64); ++id) {
        queued.push_back({.id = id, .source = "10.0.0.9", .progress = AdmissionProgress::Queued});
    }
    const auto at_ceiling = decide_admission(queued, "10.0.0.9", 64);
    EXPECT_EQ(at_ceiling.kind, AdmissionDecision::Kind::Refuse);
    EXPECT_TRUE(at_ceiling.at_ceiling);
    // Which still admits a host holding nothing.
    EXPECT_EQ(decide_admission(queued, "10.0.0.1", 64).kind, AdmissionDecision::Kind::Admit);
}

TEST(AdmissionPolicy, InterruptedAdmissionsCountTowardsTheCeilingAndTheHardLimit) {
    // Interrupted admissions are on their way out but still hold a thread and
    // a socket until that thread notices; dropping them from the count let a
    // flood grow those without limit while their threads were blocked.
    const std::vector<AdmissionCandidate> below = {
        {.id = 1, .source = "10.0.0.9", .interrupted = true},
        {.id = 2, .source = "10.0.0.9"},
        {.id = 3, .source = "10.0.0.9"},
    };
    const auto evict = decide_admission(below, "10.0.0.9", 2);
    EXPECT_EQ(evict.kind, AdmissionDecision::Kind::Evict);
    EXPECT_EQ(evict.victim, 2u) << "an interrupted admission was chosen to evict again";

    auto at_ceiling = below;
    at_ceiling.push_back({.id = 4, .source = "10.0.0.9", .interrupted = true});
    const auto refuse = decide_admission(at_ceiling, "10.0.0.9", 2);
    EXPECT_EQ(refuse.kind, AdmissionDecision::Kind::Refuse)
        << "a newcomer was admitted with twice the cap still tracked";
    EXPECT_TRUE(refuse.at_ceiling);

    // At three times the cap, from any address: the hard limit bounds
    // threads, not shares.
    auto at_hard_limit = at_ceiling;
    at_hard_limit.push_back({.id = 5, .source = "10.0.0.1", .interrupted = true});
    at_hard_limit.push_back({.id = 6, .source = "10.0.0.2", .interrupted = true});
    EXPECT_EQ(admission_hard_limit(2), 6u);
    const auto hard = decide_admission(at_hard_limit, "10.0.0.3", 2);
    EXPECT_EQ(hard.kind, AdmissionDecision::Kind::Refuse)
        << "a newcomer was admitted with three times the cap still tracked";
    EXPECT_TRUE(hard.at_ceiling);

    // Below the cap, counting only admissions not interrupted, there is room.
    const std::vector<AdmissionCandidate> room = {
        {.id = 1, .source = "10.0.0.9", .interrupted = true},
        {.id = 2, .source = "10.0.0.9"},
    };
    EXPECT_EQ(decide_admission(room, "10.0.0.9", 2).kind, AdmissionDecision::Kind::Admit);
    EXPECT_EQ(admission_ceiling(0), 2u);
    EXPECT_EQ(admission_ceiling(static_cast<std::size_t>(-1)), static_cast<std::size_t>(-1));
    EXPECT_EQ(admission_hard_limit(0), 3u);
    EXPECT_EQ(admission_hard_limit(static_cast<std::size_t>(-1) / 2), static_cast<std::size_t>(-1));
}

TEST(AdmissionPolicy, OneAddressFillingTheCeilingWithInterruptedAdmissionsDoesNotTurnAwayAnother) {
    // One address reconnecting fast evicts its own oldest admission each
    // time, as it should, but under a busy TLS handshake pool the evicted
    // threads let go slowly, and their entries filled the ceiling. A worker
    // from another host, holding nothing, was then refused with the rest.
    const std::size_t cap = 4;
    std::vector<AdmissionCandidate> flooded;
    for (std::uint64_t id = 1; id <= 8; ++id) {
        flooded.push_back({.id = id,
                           .source = "10.0.0.9",
                           .progress = AdmissionProgress::Accepted,
                           .interrupted = id <= 4});
    }
    ASSERT_EQ(flooded.size(), admission_ceiling(cap));
    EXPECT_EQ(decide_admission(flooded, "10.0.0.1", cap).kind, AdmissionDecision::Kind::Admit)
        << "a newcomer from an address holding nothing was refused because one other address "
           "filled the ceiling with its own interrupted admissions";
    const auto flooder = decide_admission(flooded, "10.0.0.9", cap);
    EXPECT_EQ(flooder.kind, AdmissionDecision::Kind::Refuse)
        << "the address holding the whole ceiling was admitted again";
    EXPECT_TRUE(flooder.at_ceiling);

    // A second worker from that host still fits: its address holds one,
    // below its share of the ceiling (8 / 2 addresses).
    flooded.push_back({.id = 9, .source = "10.0.0.1", .progress = AdmissionProgress::Handshaken});
    EXPECT_EQ(decide_admission(flooded, "10.0.0.1", cap).kind, AdmissionDecision::Kind::Admit);
    // The flooding address is still at or over its share.
    EXPECT_EQ(decide_admission(flooded, "10.0.0.9", cap).kind, AdmissionDecision::Kind::Refuse);

    // And many addresses with one each fill only up to the hard limit.
    std::vector<AdmissionCandidate> many;
    for (std::uint64_t id = 1; id <= admission_hard_limit(cap); ++id) {
        many.push_back({.id = id,
                        .source = "10.1.0." + std::to_string(id),
                        .progress = AdmissionProgress::FrameStarted});
    }
    EXPECT_EQ(decide_admission(many, "10.2.0.1", cap).kind, AdmissionDecision::Kind::Refuse);
    many.pop_back();
    EXPECT_EQ(decide_admission(many, "10.2.0.1", cap).kind, AdmissionDecision::Kind::Admit);
}

TEST(AdmissionPolicy, AListenerFormConnectionEvictsTheOldestThatGotNoFurther) {
    // A listener-form factory hands over connections already handshaken, all
    // from the one unknown source. Ranking the newcomer as merely Accepted
    // left nothing below it, so at the cap every new connection was refused
    // until the existing ones reached their deadline: one client completing
    // handshakes and sending nothing kept every worker out.
    const std::vector<AdmissionCandidate> handshaken = {
        {.id = 3, .source = "", .progress = AdmissionProgress::Handshaken},
        {.id = 1, .source = "", .progress = AdmissionProgress::Handshaken},
        {.id = 2, .source = "", .progress = AdmissionProgress::FrameStarted},
    };
    EXPECT_EQ(choose_admission_to_evict(handshaken, "", AdmissionProgress::Handshaken),
              std::optional<std::uint64_t>{1});
    const auto decision = decide_admission(handshaken, "", 3, AdmissionProgress::Handshaken);
    EXPECT_EQ(decision.kind, AdmissionDecision::Kind::Evict)
        << "a listener-form connection at the cap was refused instead of evicting the oldest "
           "admission that had got no further";
    EXPECT_EQ(decision.victim, 1u);

    // Still never one further along: a first frame begun is kept.
    const std::vector<AdmissionCandidate> started = {
        {.id = 1, .source = "", .progress = AdmissionProgress::FrameStarted},
    };
    EXPECT_EQ(choose_admission_to_evict(started, "", AdmissionProgress::Handshaken), std::nullopt);
}

TEST(AdmissionLogLimiter, LogsABurstThenSummarisesTheRest) {
    using clock = AdmissionLogLimiter::Clock;
    AdmissionLogLimiter limiter{2, std::chrono::seconds{10}};
    const clock::time_point t0{std::chrono::hours{1}};
    EXPECT_EQ(limiter.flush_due(), clock::time_point::max());

    EXPECT_TRUE(limiter.note(t0, "10.0.0.9").log);
    EXPECT_TRUE(limiter.note(t0 + 1ms, "10.0.0.9").log);
    // Past the burst: counted, not logged.
    for (int i = 0; i < 3; ++i) {
        const auto v = limiter.note(t0 + 2ms, "10.0.0.9");
        EXPECT_FALSE(v.log);
        EXPECT_TRUE(v.summary.empty());
    }
    EXPECT_FALSE(limiter.note(t0 + 3ms, "10.0.0.1").log);
    EXPECT_EQ(limiter.flush_due(), t0 + std::chrono::seconds{10});
    EXPECT_TRUE(limiter.flush(t0 + std::chrono::seconds{5}).empty()) << "summarised early";

    // The interval over, the accept thread's clock gives the summary.
    const auto summary = limiter.flush(t0 + std::chrono::seconds{10});
    EXPECT_NE(summary.find("4 more connections"), std::string::npos) << summary;
    EXPECT_NE(summary.find("most from 10.0.0.9: 3"), std::string::npos) << summary;
    EXPECT_EQ(limiter.flush_due(), clock::time_point::max());
    EXPECT_TRUE(limiter.flush(t0 + std::chrono::seconds{30}).empty()) << "summarised twice";

    // A fresh interval logs again, and a summary owed when the next event
    // arrives comes with it.
    const auto t1 = t0 + std::chrono::seconds{40};
    EXPECT_TRUE(limiter.note(t1, "").log);
    EXPECT_TRUE(limiter.note(t1, "").log);
    EXPECT_FALSE(limiter.note(t1, "").log);
    const auto later = limiter.note(t1 + std::chrono::seconds{11}, "10.0.0.2");
    EXPECT_TRUE(later.log);
    EXPECT_NE(later.summary.find("1 more connections"), std::string::npos) << later.summary;
    EXPECT_NE(later.summary.find("an unknown address"), std::string::npos) << later.summary;
}

// An evicted admission's thread lets go only once it notices, and a factory
// may be blocked in something an eviction cannot reach. Counting only the
// admissions not yet interrupted let each new connection evict one more and
// start one more thread, without limit; the ceiling refuses them instead.
TEST(FrameRobustness, AdmissionsThatDoNotLetGoWhenEvictedAreBoundedByTheCeiling) {
    Coordinator::Config cfg;
    cfg.max_pending_connections = 2;
    cfg.handshake_timeout = 10min;
    cfg.heartbeat_timeout = 10min;
    Coordinator coordinator(cfg);
    std::mutex mu;
    std::condition_variable cv;
    bool release = false;
    std::atomic<int> entered{0};
    // A handshake that ignores the shutdown eviction does and the stop wake:
    // only the test lets it go.
    coordinator.set_accept_factory(
        [&](const Coordinator::AcceptRequest& req) -> std::unique_ptr<network::Connection> {
            entered.fetch_add(1);
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return release; });
            network::NetworkSocket::close(req.fd);
            throw std::runtime_error("released by the test");
        });
    const auto port = coordinator.start();
    const auto let_go = [&] {
        {
            std::lock_guard lock(mu);
            release = true;
        }
        cv.notify_all();
    };

    // Two fill the cap; each of the next two evicts one, which stays.
    std::vector<std::unique_ptr<network::Connection>> conns;
    for (int i = 1; i <= 4; ++i) {
        conns.push_back(network::connect_plain("127.0.0.1", port));
        ASSERT_NE(conns.back(), nullptr);
        ASSERT_TRUE(fencing_await([&] { return entered.load() == i; }, kAdmissionBound))
            << "connection " << i << " was not admitted below the ceiling";
    }
    // Four tracked, twice the cap: the next is refused rather than given a
    // thread, and nothing more is evicted for it.
    auto fifth = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(fifth, nullptr);
    const bool refused = closed_by_coordinator_within(*fifth, kAdmissionBound);
    EXPECT_TRUE(refused) << "a connection was admitted with twice max_pending_connections still "
                            "tracked: evicted admissions that have not let go are not counted, "
                            "so their threads and sockets grow without limit";
    EXPECT_EQ(entered.load(), 4) << "an admission thread was started beyond the ceiling";

    let_go();
    fifth->close();
    for (auto& c : conns) {
        c->close();
    }
    coordinator.stop();
}

// The bookkeeping that tracks a new admission (joining finished admission
// threads, reading the peer's address) ran after the accept had handed the
// socket over but before anything that would close it: a throw there left the
// descriptor open for the life of the process, and the peer connected to
// nothing that would ever read it.
TEST(FrameRobustness, AThrowBeforeAnAdmissionIsTrackedClosesItsConnection) {
    namespace fault = clink::fault;
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-after-track-throw"});
    fault::Registry::instance().reset();
    fault::ScopedFault boom{fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeTrack,
                                        .ordinal = 1,
                                        .action = fault::Action::Throw}};

    auto victim = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(victim, nullptr);
    EXPECT_TRUE(closed_by_coordinator_within(*victim, kAdmissionBound))
        << "a connection whose admission could not be tracked was left open";

    Worker worker("w-after-track-throw", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(5s));

    victim->close();
    worker.stop();
    coordinator.stop();
}

// A refusal costs a client one connect, so a line for each let whoever could
// reach the port write the coordinator's log at the accept loop's speed.
TEST(FrameRobustness, AFloodOfRefusedConnectionsLogsABurstNotALineEach) {
    Coordinator::Config cfg;
    cfg.max_pending_connections = 1;
    cfg.handshake_timeout = 10min;
    cfg.heartbeat_timeout = 10min;
    Coordinator coordinator(cfg);
    std::mutex mu;
    std::condition_variable cv;
    bool release = false;
    std::atomic<int> entered{0};
    // Holds every admission until the test lets go, so from the third
    // connection on (one admitted, one evicting it, both from 127.0.0.1, at
    // twice the cap) every one is refused.
    coordinator.set_accept_factory(
        [&](const Coordinator::AcceptRequest& req) -> std::unique_ptr<network::Connection> {
            entered.fetch_add(1);
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return release; });
            network::NetworkSocket::close(req.fd);
            throw std::runtime_error("released by the test");
        });
    const auto since_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count() -
                          1;
    const auto port = coordinator.start();
    // Lets the held admissions go on every way out, a failed assertion
    // included, so stop() never waits on them.
    struct Release {
        std::mutex& mu;
        std::condition_variable& cv;
        bool& release;
        void operator()() const {
            {
                std::lock_guard lock(mu);
                release = true;
            }
            cv.notify_all();
        }
        ~Release() { (*this)(); }
    };

    const Release let_go{.mu = mu, .cv = cv, .release = release};
    std::vector<std::unique_ptr<network::Connection>> held;
    for (int i = 1; i <= 2; ++i) {
        held.push_back(network::connect_plain("127.0.0.1", port));
        ASSERT_NE(held.back(), nullptr);
        ASSERT_TRUE(fencing_await([&] { return entered.load() == i; }, kAdmissionBound));
    }
    constexpr int kRefused = 20;
    for (int i = 0; i < kRefused; ++i) {
        auto c = network::connect_plain("127.0.0.1", port);
        ASSERT_NE(c, nullptr);
        ASSERT_TRUE(closed_by_coordinator_within(*c, kAdmissionBound)) << "connection " << i;
        c->close();
    }
    std::size_t lines = 0;
    for (const auto& rec : LogBuffer::global().tail(4000, "warn", since_ms, "coordinator.accept")) {
        if (rec.message.find("connection refused") != std::string::npos &&
            rec.message.find("max_pending_connections=1)") != std::string::npos) {
            ++lines;
        }
    }
    EXPECT_GE(lines, 1u) << "no refusal was logged at all";
    EXPECT_LE(lines, 5u) << kRefused << " refusals wrote " << lines
                         << " lines: one per refusal, at whatever rate the peer connects";
    EXPECT_EQ(entered.load(), 2);

    let_go();
    for (auto& c : held) {
        c->close();
    }
    coordinator.stop();
}

// --- Replies to a peer not yet admitted -------------------------------------
//
// handle_first_frame_ answers a peer before it is admitted (a refusal, or the
// RegisterAck), on the admission thread and under the first-frame lock, and
// for a registration that loses to a newer session it did so under mu_. No
// send was bounded, and the worker id was echoed into the reply, so a peer
// that never read could make a reply too large for the socket buffers and
// park that thread in send() with those locks held: every dispatcher, the
// watchdog and stop() waited on it.

namespace {

// A plain-TCP connection to the coordinator with as small a receive buffer as
// the platform allows, set before the connect so the window it advertises is
// small from the start: a peer that means not to read.
std::unique_ptr<network::Connection> connect_with_small_receive_buffer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return nullptr;
    }
    const int small = 1024;
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return nullptr;
    }
    return network::make_plain_connection(fd);
}

// A plain-TCP connection to a peer that never reads, whose socket buffers hold
// `buffered` bytes: a send larger than that writes far more than any real
// socket buffer holds instead, so it completes only if the peer reads, and
// fails once the socket is shut down (or a send bound set on it expires).
// With `buffered` zero every send does. Stands in for a reply too large for
// the buffers, where the platform's own would hold it anyway: Darwin grows a
// receive window that is never read from, Linux does not. With
// `can_bound_sends` false it also stands for a transport that cannot bound a
// send, recording what it was asked for.
class SendsUntilShutDown final : public network::Connection {
public:
    SendsUntilShutDown(int fd,
                       std::atomic<int>& sends_entered,
                       std::size_t buffered = 0,
                       bool can_bound_sends = false)
        : fd_(fd),
          inner_(network::make_plain_connection(fd)),
          sends_entered_(sends_entered),
          buffered_(buffered),
          can_bound_sends_(can_bound_sends) {}

    bool send_all(const std::byte* buf, std::size_t len) override {
        if (len <= buffered_) {
            return inner_->send_all(buf, len);
        }
        sends_entered_.fetch_add(1);
        const std::vector<std::byte> junk(std::size_t{64} * 1024 * 1024, std::byte{0x5a});
        return network::NetworkSocket::send_all(fd_, junk.data(), junk.size());
    }
    bool recv_all(std::byte* buf, std::size_t len) override { return inner_->recv_all(buf, len); }
    void shutdown_write() override { inner_->shutdown_write(); }
    void shutdown_read() override { inner_->shutdown_read(); }
    void close() override { inner_->close(); }
    [[nodiscard]] bool is_open() const noexcept override { return inner_->is_open(); }
    bool set_recv_timeout(std::chrono::milliseconds timeout) override {
        return inner_->set_recv_timeout(timeout);
    }
    bool set_send_timeout(std::chrono::milliseconds timeout) override {
        if (timeout.count() > 0) {
            requested_send_timeout.store(timeout.count());
        }
        return can_bound_sends_ && inner_->set_send_timeout(timeout);
    }

    static inline std::atomic<std::int64_t> requested_send_timeout{0};

private:
    int fd_;
    std::unique_ptr<network::Connection> inner_;
    std::atomic<int>& sends_entered_;
    std::size_t buffered_;
    bool can_bound_sends_;
};

// Runs `f` on a thread of its own and reports whether it returned within
// `bound`; join() waits for it however long it takes, for the cleanup after a
// failure.
class Bounded {
public:
    template <typename F>
    explicit Bounded(F f)
        : thread_([this, f = std::move(f)]() mutable {
              f();
              done_.store(true);
          }) {}
    ~Bounded() { join(); }
    Bounded(const Bounded&) = delete;
    Bounded& operator=(const Bounded&) = delete;
    [[nodiscard]] bool returns_within(std::chrono::milliseconds bound) const {
        return fencing_await([this] { return done_.load(); }, bound);
    }
    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    std::atomic<bool> done_{false};
    std::thread thread_;
};

}  // namespace

TEST(FrameRobustness, ARegisterNamingAWorkerOrHostLongerThanTheCapDoesNotDecode) {
    const auto decode = [](const RegisterMsg& m) {
        auto frame = encode_frame(MessageKind::Register, m);
        // Past the length prefix and the kind byte, as handle_first_frame_
        // hands it over.
        MessageReader r(std::vector<std::byte>(frame.begin() + 4, frame.end()));
        (void)r.read_u8();
        return decode_register(r);
    };
    RegisterMsg reg;
    reg.worker_id = std::string(kMaxRegisterStringBytes, 'w');
    reg.data_host = std::string(kMaxRegisterStringBytes, 'h');
    EXPECT_EQ(decode(reg).worker_id, reg.worker_id);
    reg.worker_id.push_back('w');
    EXPECT_THROW((void)decode(reg), std::runtime_error) << "an over-long worker_id decoded";
    reg.worker_id.pop_back();
    reg.data_host.push_back('h');
    EXPECT_THROW((void)decode(reg), std::runtime_error) << "an over-long data_host decoded";

    // The worker says why itself, before connecting, rather than reading the
    // coordinator's refusal as a coordinator that hung up.
    Coordinator coordinator;
    const auto port = coordinator.start();
    Worker worker(std::string(kMaxRegisterStringBytes + 1, 'w'), "127.0.0.1");
    try {
        worker.connect_to_coordinator("127.0.0.1", port);
        ADD_FAILURE() << "a worker with an over-long id connected";
    } catch (const WorkerConnectionError& e) {
        EXPECT_FALSE(e.retryable()) << e.what();
    }
    coordinator.stop();
}

TEST(FrameRobustness, AnEchoedWorkerIdFromAPeerThatNeverReadsHoldsUpNoRegistrationNorStop) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;
    Coordinator coordinator(cfg);
    // Socket buffers of 16 KiB to a peer that never reads, about what Linux
    // gives a fresh connection, on any platform: a reply of tens of
    // kilobytes does not fit them, one naming a worker in 255 bytes does.
    std::atomic<int> sends_entered{0};
    coordinator.set_accept_factory([&](const Coordinator::AcceptRequest& req) {
        return std::make_unique<SendsUntilShutDown>(
            req.fd, sends_entered, std::size_t{16} * 1024, /*can_bound_sends=*/true);
    });
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-behind-echo"});
    fault::Registry::instance().reset();
    fault::ScopedFault observe{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeHandle,
                    .action = fault::Action::Observe}};

    // A Register the coordinator refuses for its protocol version, naming a
    // worker id of 60,000 bytes, which the refusal used to echo back.
    auto silent = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(silent, nullptr);
    RegisterMsg reg;
    reg.worker_id = std::string(60'000, 'w');
    reg.data_host = "127.0.0.1";
    reg.protocol_version = kClusterProtocolVersion + 1000;
    reg.min_compatible_protocol_version = kClusterProtocolVersion + 1000;
    ASSERT_TRUE(send_frame(*silent, encode_frame(MessageKind::Register, reg)));
    ASSERT_TRUE(fencing_await(
        [] {
            return fault::Registry::instance().hits(
                       fault::points::kCoordinatorAdmissionBeforeHandle) >= 1;
        },
        kAdmissionBound));

    Worker worker("w-behind-echo", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    EXPECT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "a worker could not register behind a peer that does not read its refusal";
    EXPECT_TRUE(coordinator.await_registrations(1s));
    worker.stop();
    {
        Bounded stop{[&] { coordinator.stop(); }};
        EXPECT_TRUE(stop.returns_within(kAdmissionBound))
            << "stop() waited on a reply to a peer that does not read";
        silent.reset();  // a reset, which ends a send blocked on it, so the join returns
    }
    EXPECT_EQ(sends_entered.load(), 0) << "a reply too large for a fresh socket's buffers was sent";
}

TEST(FrameRobustness, ARefusalBlockedOnItsPeerHoldsNeitherTheCoordinatorLockNorStop) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;
    Coordinator coordinator(cfg);
    std::atomic<int> accepted{0};
    std::atomic<int> sends_entered{0};
    SendsUntilShutDown::requested_send_timeout.store(0);
    // The first connection's replies never complete; the rest are plain.
    coordinator.set_accept_factory(
        [&](const Coordinator::AcceptRequest& req) -> std::unique_ptr<network::Connection> {
            if (accepted.fetch_add(1) == 0) {
                return std::make_unique<SendsUntilShutDown>(req.fd, sends_entered);
            }
            return network::make_plain_connection(req.fd);
        });
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-held"});
    fault::Registry::instance().reset();
    fault::ScopedFault park{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeFirstFrame,
                    .ordinal = 1,
                    .action = fault::Action::Block}};

    // A worker's abandoned first attempt, held before its first frame is
    // read, while its second registers.
    auto stale = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(stale, nullptr);
    RegisterMsg reg;
    reg.worker_id = "w-held";
    reg.data_host = "127.0.0.1";
    ASSERT_TRUE(send_frame(*stale, encode_frame(MessageKind::Register, reg)));
    ASSERT_TRUE(fencing_await(
        [] {
            return fault::Registry::instance().hits(
                       fault::points::kCoordinatorAdmissionBeforeFirstFrame) >= 1;
        },
        kAdmissionBound));
    Worker worker("w-held", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    ASSERT_TRUE(coordinator.await_registrations(5s));

    // The first attempt's frame is handled and refused, and its refusal
    // cannot be delivered.
    fault::Registry::instance().release(fault::points::kCoordinatorAdmissionBeforeFirstFrame);
    ASSERT_TRUE(fencing_await([&] { return sends_entered.load() >= 1; }, kAdmissionBound));
    EXPECT_EQ(SendsUntilShutDown::requested_send_timeout.load(),
              std::min(cfg.heartbeat_timeout, Coordinator::kAdmissionReplySendBound).count())
        << "the refusal was sent without asking for a send bound";

    {
        Bounded locked{[&] { (void)coordinator.free_slots(); }};
        EXPECT_TRUE(locked.returns_within(kAdmissionBound))
            << "the coordinator's lock was held while a refusal waited on a peer that does not "
               "read: every dispatcher, the watchdog and stop() wait with it";
        worker.stop();
        Bounded stop{[&] { coordinator.stop(); }};
        EXPECT_TRUE(stop.returns_within(kAdmissionBound))
            << "stop() waited on a refusal to a peer that does not read: an admission whose first "
               "frame is in was out of its reach";
        stale.reset();  // a reset, which ends the send, so the joins return
    }
}

TEST(FrameRobustness, ASendToAPeerThatNeverReadsFailsWithinTheSendTimeout) {
    std::uint16_t port = 0;
    const int listener = network::NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);
    auto silent = connect_with_small_receive_buffer(port);
    ASSERT_NE(silent, nullptr);
    const int fd = network::NetworkSocket::accept_one(listener);
    network::NetworkSocket::close(listener);
    ASSERT_GE(fd, 0);
    auto conn = network::make_plain_connection(fd);
    ASSERT_TRUE(conn->set_send_timeout(300ms));
    const std::vector<std::byte> big(std::size_t{64} * 1024 * 1024, std::byte{0x5a});
    {
        Bounded send{[&] { EXPECT_FALSE(conn->send_all(big.data(), big.size())); }};
        EXPECT_TRUE(send.returns_within(kAdmissionBound))
            << "a send to a peer that never reads outlasted its send timeout";
        silent.reset();  // a reset, which ends the send, so the join returns
    }
}

// --- Out of descriptors -----------------------------------------------------
//
// accept() failing with EMFILE (or ENFILE, ENOBUFS, ENOMEM) leaves the
// connection pending and the listener readable, so the accept loop went
// straight back to a wait that returned at once and an accept that failed
// again: one core spinning, logging nothing, until a descriptor freed.

TEST(FrameRobustness, AnAcceptOutOfDescriptorsBacksOffRatherThanSpinning) {
    namespace fault = clink::fault;
    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-after-emfile"});
    fault::Registry::instance().reset();
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kCoordinatorAcceptOne,
                                                .action = fault::Action::Error,
                                                .arg = EMFILE});
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kCoordinatorAcceptBackoff,
                                                .action = fault::Action::Observe});

    // Pending, and every accept of it fails.
    auto pending = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(pending, nullptr);
    ASSERT_TRUE(fencing_await(
        [] { return fault::Registry::instance().hits(fault::points::kCoordinatorAcceptOne) >= 1; },
        kAdmissionBound));
    // From 10 ms, doubling: four backoffs take at least 150 ms, in which a
    // loop that retries at once makes many thousands of attempts.
    EXPECT_TRUE(fencing_await(
        [] {
            return fault::Registry::instance().hits(fault::points::kCoordinatorAcceptBackoff) >= 4;
        },
        kAdmissionBound))
        << "the accept loop never backed off after accept() ran out of descriptors";
    EXPECT_LE(fault::Registry::instance().hits(fault::points::kCoordinatorAcceptOne), 6u)
        << "the accept loop retried accept() at once, spinning while out of descriptors";

    // Descriptors free again: the waiting connection, and a worker after it,
    // are taken.
    fault::Registry::instance().reset();
    Worker worker("w-after-emfile", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    EXPECT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port));
    EXPECT_TRUE(coordinator.await_registrations(5s));

    pending->close();
    worker.stop();
    coordinator.stop();
}

// --- A burst from one address ----------------------------------------------
//
// Plain-TCP workers starting together on one host all share its address, and
// their first frames arrive within microseconds of connecting. Once the cap's
// worth were waiting on registration, every further connection from that host
// was closed unanswered, where the listen backlog had held it before.

TEST(FrameRobustness, MoreRegistrationsFromOneAddressThanThePendingCapAllRegister) {
    namespace fault = clink::fault;
    // Three descriptors a worker, all in this one process (its socket, the
    // accepted socket and that one's interrupt duplicate): past the soft
    // limit a macOS process starts with.
    const FrameRobustnessDescriptorLimit descriptors{1024};
    if (!descriptors.ok()) {
        GTEST_SKIP() << "the descriptor limit cannot be raised to 1024";
    }
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = kAdmissionBound;  // the raw peers below send no heartbeats
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    ASSERT_EQ(cfg.max_pending_connections, 64u);
    constexpr std::size_t kWorkers = 100;
    fault::Registry::instance().reset();
    // Registration held up, as behind a slow one: each first frame queues.
    fault::ScopedFault hold{fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeHandle,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kCoordinatorAdmissionQueued,
                                                .action = fault::Action::Observe});

    std::vector<std::unique_ptr<network::Connection>> workers;
    for (std::size_t i = 1; i <= kWorkers; ++i) {
        workers.push_back(network::connect_plain("127.0.0.1", port));
        ASSERT_NE(workers.back(), nullptr);
        RegisterMsg reg;
        reg.worker_id = "w-burst-" + std::to_string(i);
        reg.data_host = "127.0.0.1";
        ASSERT_TRUE(send_frame(*workers.back(), encode_frame(MessageKind::Register, reg)));
        // Each queued before the next connects, so none is caught mid-read.
        ASSERT_TRUE(fencing_await(
            [i] {
                return fault::Registry::instance().hits(
                           fault::points::kCoordinatorAdmissionQueued) >= i;
            },
            kAdmissionBound))
            << "registration " << i << " from the same address was not admitted while " << (i - 1)
            << " waited on registration (max_pending_connections=64)";
    }

    fault::Registry::instance().release(fault::points::kCoordinatorAdmissionBeforeHandle);
    for (std::size_t i = 0; i < kWorkers; ++i) {
        ASSERT_TRUE(workers[i]->set_recv_timeout(kAdmissionBound));
        auto frame = read_frame(*workers[i]);
        ASSERT_TRUE(frame.has_value()) << "registration " << (i + 1) << " got no answer";
        MessageReader r(std::move(*frame));
        ASSERT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::RegisterAck);
        EXPECT_TRUE(decode_register_ack(r).ok) << "registration " << (i + 1) << " was refused";
    }

    for (auto& w : workers) {
        w->close();
    }
    coordinator.stop();
}
