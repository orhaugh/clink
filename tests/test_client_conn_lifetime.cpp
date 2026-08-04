// A client leaving through an odd exit path must not take the coordinator
// with it.
//
// Background, and the reason this file exists. JobState::notify_client_conn
// is a RAW pointer into the Connection owned by the coordinator's per-client
// thread, and signal_job_completion_locked_ dereferences it to push
// JobCompleted. When that thread returns, the Connection is destroyed - so a
// job still holding the pointer holds freed memory. The client loop cleared
// it on exactly ONE of its three exit paths (the client hung up); an
// unhandled frame kind and an undecodable frame both returned without
// clearing, and both are reachable by anyone who can connect to the control
// port. That is now fixed with a scope guard, so no future exit path can
// forget.
//
// WHAT THESE TESTS DO AND DO NOT COVER, because the distinction matters more
// than the coverage. They drive the two previously-unguarded exit paths and
// assert the coordinator survives them and keeps serving. They do NOT
// reproduce the use-after-free, because that needs a job SUBMITTED over the
// wire on the connection (only the wire path sets notify_client_conn; the
// in-process submit_job API does not) and then COMPLETING after the client
// has gone. Confirmed by mutation: reverting the guard leaves both cases
// green.
//
// So the guard's own regression test does not exist yet, and saying so is the
// point - a file named for a lifetime bug that quietly tests something else
// is worse than an admitted gap. Writing it needs a wire submission plus a
// fake worker acking a final checkpoint, which is a fixture this file does
// not have. Recorded in F39.
//
// What they do catch is real: before the fix, both paths also closed the
// connection SILENTLY, so a submitter reported "connection closed by the
// coordinator" with nothing on the coordinator side to explain it. The
// unhandled-kind path now logs, and the cases below exercise it.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/connection.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

// Become a client, the way the real submitter does.
//
// The coordinator routes a connection on its FIRST frame: Register makes it a
// worker, HelloClient a client. Sending the unhandled frame first would test
// the worker path instead, and hang waiting for a close that never comes -
// which is exactly what the first draft of this test did.
[[nodiscard]] std::shared_ptr<network::Connection> connect_as_client(std::uint16_t port) {
    auto conn = network::connect_plain("127.0.0.1", port);
    if (conn == nullptr) {
        return nullptr;
    }
    if (!send_frame(*conn, encode_frame(MessageKind::HelloClient, HelloClientMsg{}))) {
        return nullptr;
    }
    return conn;
}

// A kind the client dispatch does not handle, which is the exit path under
// test. Register is a worker->coordinator kind, so a client sending one is
// precisely the "unhandled kind" case.
constexpr auto kUnhandledOnClientPath = MessageKind::Register;

}  // namespace

TEST(ClientConnLifetime, AClientLeavingOnAnUnhandledFrameDoesNotStopTheCoordinator) {
    Coordinator coordinator;
    const auto port = coordinator.start(0);
    ASSERT_GT(port, 0);

    // A client takes the exit path that used to leave a dangling pointer
    // behind. No job is submitted, so this does NOT reproduce the
    // use-after-free - see the header. What it asserts is that the path is
    // survivable and repeatable.
    {
        auto conn = connect_as_client(port);
        ASSERT_NE(conn, nullptr);
        RegisterMsg reg{.worker_id = "not-a-worker", .data_host = "127.0.0.1", .slot_count = 1};
        ASSERT_TRUE(send_frame(*conn, encode_frame(kUnhandledOnClientPath, reg)))
            << "could not send the frame that drives the exit path under test";
        // Deliberately NOT waiting for the coordinator's close. Whether the
        // socket is closed promptly depends on session bookkeeping holding a
        // reference, and that is not what this asserts - what matters is that
        // the coordinator took the exit path (it logs "unhandled frame kind")
        // and survives it. Waiting here made the first draft hang.
    }

    // Cycle several clients through the same path. One iteration proves
    // little - the interesting failure is the coordinator dereferencing a
    // pointer left by an EARLIER client, so there has to be an earlier one.
    for (int i = 0; i < 8; ++i) {
        auto conn = connect_as_client(port);
        ASSERT_NE(conn, nullptr) << "coordinator stopped accepting after " << i << " clients";
        RegisterMsg reg{
            .worker_id = "not-a-worker-" + std::to_string(i),
            .data_host = "127.0.0.1",
            .slot_count = 1,
        };
        (void)send_frame(*conn, encode_frame(kUnhandledOnClientPath, reg));
    }

    // Let the drops be processed before probing. A poll would be better, but
    // the observable (a session removed from a private list) is not exposed;
    // the assertion below is what actually matters and a short settle is
    // enough for it to mean something.
    std::this_thread::sleep_for(500ms);

    // Still serving: a coordinator that had crashed or corrupted its client
    // bookkeeping would fail here rather than in the loop above.
    auto probe = connect_as_client(port);
    EXPECT_NE(probe, nullptr) << "the coordinator stopped accepting clients after 9 of them left "
                                 "through the unhandled-frame path";

    coordinator.stop();
}

TEST(ClientConnLifetime, TheCoordinatorSurvivesAClientThatSendsRubbish) {
    // The other unguarded exit: a frame that throws while decoding. The
    // handler catches it, logs, and drops the client - and used to do so
    // without clearing the pointer either.
    Coordinator coordinator;
    const auto port = coordinator.start(0);
    ASSERT_GT(port, 0);

    for (int i = 0; i < 8; ++i) {
        auto conn = connect_as_client(port);
        ASSERT_NE(conn, nullptr) << "coordinator stopped accepting after " << i << " clients";
        // A SubmitJob kind with a truncated body: the kind is handled, so
        // dispatch runs, and the decode reads past the end and throws.
        std::vector<std::byte> body;
        body.push_back(static_cast<std::byte>(MessageKind::SubmitJob));
        body.push_back(std::byte{0xFF});
        body.push_back(std::byte{0xFF});
        (void)send_frame(*conn, body);
    }

    std::this_thread::sleep_for(500ms);
    auto probe = connect_as_client(port);
    EXPECT_NE(probe, nullptr)
        << "the coordinator stopped accepting clients after 8 sent undecodable frames";

    coordinator.stop();
}
