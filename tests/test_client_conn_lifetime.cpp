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
// Reproducing it needs FOUR things in order, and the order is the whole
// difficulty:
//
//   1. a job submitted over the WIRE - only that path sets
//      notify_client_conn; the in-process submit_job API does not;
//   2. the client leaving through one of the unguarded exits;
//   3. ANOTHER client connecting, because ClientSession holds a shared_ptr to
//      the Connection and it is only freed when reap_finished_clients_ drops
//      the session, which happens on the next admission;
//   4. and only then the job completing, so something dereferences it.
//
// Get step 3 wrong and nothing is freed, so nothing faults. The first draft
// of this test completed the job before any reap and passed under ASan with
// the fix reverted - a green test proving nothing.
//
// With the order right, ASan on the pre-fix code reports:
//
//   heap-use-after-free ... READ of size 8
//     clink::cluster::send_frame
//     Coordinator::signal_job_completion_locked_
//     Coordinator::handle_subtask_finished_
//
// which is the defect exactly. AJobCompletingAfterItsClientLeftDoesNotTouchFreedMemory
// below drives it.
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

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
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

// The case that actually reproduces the defect: wire submission, client
// leaves through an unguarded exit, job completes.
//
// Under ASan on the pre-fix code this is a heap-use-after-free in
// signal_job_completion_locked_. Without a sanitizer it is a crash or
// nothing, depending on what the allocator did with the freed Connection -
// so this test is worth most when run under ASan, and is still worth having
// without it because the crash it caused on Linux was real.
TEST(ClientConnLifetime, AJobCompletingAfterItsClientLeftDoesNotTouchFreedMemory) {
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_ccl_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator coordinator;
    const auto port = coordinator.start(0);
    ASSERT_GT(port, 0);
    coordinator.expect_workers({"w"});

    // A worker, so the job can be deployed and later reported finished.
    auto worker = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(worker, nullptr);
    RegisterMsg reg{.worker_id = "w", .data_host = "127.0.0.1", .slot_count = 4};
    ASSERT_TRUE(send_frame(*worker, encode_frame(MessageKind::Register, reg)));
    auto reg_ack = read_frame(*worker);
    ASSERT_TRUE(reg_ack.has_value()) << "no RegisterAck";

    // Submit over the wire. This is the only path that records the client
    // connection on the job, which is the pointer under test.
    auto client = connect_as_client(port);
    ASSERT_NE(client, nullptr);
    SubmitJobMsg sj;
    {
        JobGraphSpec g;
        OperatorSpec src;
        src.type = "int64_range_source";
        src.id = "src";
        src.parallelism = 1;
        src.out_channel = std::string{kChannelInt64};
        src.params = {{"count", "1000000"}};  // must not finish on its own
        g.ops.push_back(src);
        OperatorSpec snk;
        snk.type = "file_int64_sink";
        snk.id = "snk";
        snk.inputs = {"src"};
        snk.parallelism = 1;
        snk.out_channel = std::string{kChannelInt64};
        snk.params = {{"path", (dir / "out.txt").string()}};
        g.ops.push_back(snk);
        sj.graph_json = g.to_json();
    }
    ASSERT_TRUE(send_frame(*client, encode_frame(MessageKind::SubmitJob, sj)));
    auto ack = read_frame(*client);
    ASSERT_TRUE(ack.has_value()) << "no SubmitJobAck";
    MessageReader ack_r(std::move(*ack));
    ASSERT_EQ(static_cast<MessageKind>(ack_r.read_u8()), MessageKind::SubmitJobAck);
    const auto ack_msg = decode_submit_job_ack(ack_r);
    ASSERT_TRUE(ack_msg.ok) << "submission rejected: " << ack_msg.message;
    const auto job_id = ack_msg.job_id;
    ASSERT_GT(job_id, 0U);

    // Learn the deployed task set, so completion can be reported for the
    // keys the coordinator is actually tracking.
    auto deploy = read_frame(*worker);
    ASSERT_TRUE(deploy.has_value()) << "no Deploy reached the worker";
    MessageReader dep_r(std::move(*deploy));
    ASSERT_EQ(static_cast<MessageKind>(dep_r.read_u8()), MessageKind::Deploy);
    const auto deployed = decode_deploy(dep_r).tasks;
    ASSERT_FALSE(deployed.empty());

    // The client now leaves through the exit path that used to skip the
    // cleanup, leaving the job holding a pointer to a Connection that is
    // about to be destroyed.
    RegisterMsg unhandled{.worker_id = "not-a-worker", .data_host = "127.0.0.1", .slot_count = 1};
    ASSERT_TRUE(send_frame(*client, encode_frame(kUnhandledOnClientPath, unhandled)));
    std::this_thread::sleep_for(500ms);  // let the coordinator drop it

    // FORCE THE FREE, and this ordering is the whole test.
    //
    // The loop returning does not destroy the Connection: ClientSession holds
    // a shared_ptr to it, so it lives until reap_finished_clients_ removes the
    // session - which happens when the NEXT client is admitted. So a job's
    // pointer only becomes dangling after another client connects, and a test
    // that completes the job before that proves nothing. The first draft of
    // this test did exactly that and passed even under ASan with the fix
    // reverted.
    {
        auto reaper = connect_as_client(port);
        ASSERT_NE(reaper, nullptr) << "could not admit a client to trigger the reap";
        std::this_thread::sleep_for(300ms);
    }

    // And NOW the job completes, dereferencing a pointer to memory that has
    // been freed.
    for (const auto& t : deployed) {
        SubtaskFinishedMsg fin;
        fin.job_id = job_id;
        fin.worker_id = "w";
        fin.role = t.role;
        fin.subtask_idx = t.subtask_idx;
        fin.had_error = false;
        ASSERT_TRUE(send_frame(*worker, encode_frame(MessageKind::SubtaskFinished, fin)));
    }

    // Survival is the assertion. A coordinator that wrote to the freed
    // Connection either crashes here or is caught by ASan; one that cleared
    // the pointer keeps serving.
    std::this_thread::sleep_for(750ms);
    auto probe = connect_as_client(port);
    EXPECT_NE(probe, nullptr)
        << "the coordinator stopped serving after a job completed with its client gone - the "
           "completion path wrote to a Connection that had been destroyed";

    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
