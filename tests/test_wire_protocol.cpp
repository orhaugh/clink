// Wire-format invariants for the coordinator/worker cluster protocol.
//
// The wire format is the contract between every clink_node process in a
// cluster. Once two nodes are running different versions of the binary,
// any silent change to encode/decode breaks rolling deploys. These tests
// pin:
//   - Round-trip identity for every MessageKind.
//   - Length-prefix framing produced by MessageBuilder::finalize().
//   - Big-endian on-wire integers (snapshot a few bytes so an endian flip
//     is loud).
//   - String length-prefix protocol round-trips, including empty and
//     binary content.
//   - MessageReader truncation rejection.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"

using namespace clink::cluster;

namespace {

// Strip the 4-byte length header from an encode_frame() output so we can
// feed the body to MessageReader the way the cluster's read_frame does.
std::vector<std::byte> body_of(const std::vector<std::byte>& framed) {
    if (framed.size() < 4) {
        return {};
    }
    return {framed.begin() + 4, framed.end()};
}

}  // namespace

// ----- MessageBuilder primitives -----

TEST(MessageBuilder, FinalizePrependsBigEndianLength) {
    MessageBuilder b;
    b.put_u8(0xAB);
    b.put_u32_be(0x01020304);
    auto out = b.finalize();
    ASSERT_EQ(out.size(), 4u + 1u + 4u);

    // Length header = 5, big-endian.
    EXPECT_EQ(static_cast<unsigned char>(out[0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[1]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[2]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[3]), 0x05);

    // Payload bytes match what we put in.
    EXPECT_EQ(static_cast<unsigned char>(out[4]), 0xAB);
    EXPECT_EQ(static_cast<unsigned char>(out[5]), 0x01);
    EXPECT_EQ(static_cast<unsigned char>(out[6]), 0x02);
    EXPECT_EQ(static_cast<unsigned char>(out[7]), 0x03);
    EXPECT_EQ(static_cast<unsigned char>(out[8]), 0x04);
}

TEST(MessageBuilder, IntegersAreBigEndian) {
    MessageBuilder b;
    b.put_u16_be(0xCAFE);
    auto out = b.finalize();
    ASSERT_EQ(out.size(), 4u + 2u);
    EXPECT_EQ(static_cast<unsigned char>(out[4]), 0xCA);
    EXPECT_EQ(static_cast<unsigned char>(out[5]), 0xFE);
}

TEST(MessageBuilder, StringIsLengthPrefixed) {
    MessageBuilder b;
    b.put_string("hi");
    auto out = b.finalize();
    // 4 hdr + 4 strlen + 2 chars
    ASSERT_EQ(out.size(), 4u + 4u + 2u);
    // Payload starts at offset 4 - first the BE u32 length 2, then "hi".
    EXPECT_EQ(static_cast<unsigned char>(out[4]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[5]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[6]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(out[7]), 0x02);
    EXPECT_EQ(static_cast<char>(out[8]), 'h');
    EXPECT_EQ(static_cast<char>(out[9]), 'i');
}

// ----- MessageReader primitives -----

TEST(MessageReader, ReadsBackWhatBuilderWrote) {
    MessageBuilder b;
    b.put_u8(7);
    b.put_u16_be(0xABCD);
    b.put_u32_be(0x11223344);
    b.put_string("");
    b.put_string("payload");

    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(r.read_u8(), 7);
    EXPECT_EQ(r.read_u16_be(), 0xABCD);
    EXPECT_EQ(r.read_u32_be(), 0x11223344u);
    EXPECT_EQ(r.read_string(), "");
    EXPECT_EQ(r.read_string(), "payload");
    EXPECT_TRUE(r.eof());
}

TEST(MessageReader, ThrowsOnTruncatedBody) {
    MessageReader r(std::vector<std::byte>{});
    EXPECT_THROW((void)r.read_u8(), std::runtime_error);
}

TEST(MessageReader, ThrowsOnTruncatedString) {
    MessageBuilder b;
    b.put_u32_be(10);  // claims a 10-byte string follows
    b.put_u8('a');     // but only 1 byte present
    MessageReader r(body_of(b.finalize()));
    EXPECT_THROW((void)r.read_string(), std::runtime_error);
}

// ----- Round-trip tests for every MessageKind -----

namespace {

template <typename Msg, typename Decoder>
Msg round_trip(MessageKind kind, const Msg& original, Decoder decode) {
    MessageReader r(body_of(encode_frame(kind, original)));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), kind);
    return decode(r);
}

}  // namespace

TEST(WireProtocol, RegisterRoundTrips) {
    RegisterMsg in{.worker_id = "worker-a", .data_host = "10.0.0.7", .slot_count = 4};
    auto out = round_trip(MessageKind::Register, in, decode_register);
    EXPECT_EQ(out.worker_id, in.worker_id);
    EXPECT_EQ(out.data_host, in.data_host);
    EXPECT_EQ(out.slot_count, in.slot_count);
}

TEST(WireProtocol, RegisterAckRoundTrips) {
    RegisterAckMsg ok_msg{.ok = true, .message = "welcome"};
    auto ok_out = round_trip(MessageKind::RegisterAck, ok_msg, decode_register_ack);
    EXPECT_EQ(ok_out.ok, true);
    EXPECT_EQ(ok_out.message, "welcome");

    RegisterAckMsg bad_msg{.ok = false, .message = "connection limit", .retryable = true};
    auto bad_out = round_trip(MessageKind::RegisterAck, bad_msg, decode_register_ack);
    EXPECT_EQ(bad_out.ok, false);
    EXPECT_EQ(bad_out.message, "connection limit");
    EXPECT_TRUE(bad_out.retryable);
}

TEST(WireProtocol, DeployRoundTripsSimpleTask) {
    DeployMsg in;
    in.job_id = 42;
    in.tasks.push_back(DeploymentTask{
        .role = "consumer",
        .subtask_idx = 0,
        .data_port = 18000,
        .peers = {},
        .extra_config = "",
    });
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    EXPECT_EQ(out.job_id, 42u);
    ASSERT_EQ(out.tasks.size(), 1u);
    EXPECT_EQ(out.tasks[0].role, "consumer");
    EXPECT_EQ(out.tasks[0].subtask_idx, 0u);
    EXPECT_EQ(out.tasks[0].data_port, 18000);
    EXPECT_TRUE(out.tasks[0].peers.empty());
    EXPECT_EQ(out.tasks[0].extra_config, "");
}

TEST(WireProtocol, DeployRoundTripsMultiTaskWithPeers) {
    DeployMsg in;
    in.job_id = 7;
    in.tasks.push_back(DeploymentTask{
        .role = "producer",
        .subtask_idx = 0,
        .data_port = 0,
        .peers =
            {PeerAddress{
                 .role = "consumer", .subtask_idx = 0, .host = "192.0.2.1", .data_port = 18000},
             PeerAddress{
                 .role = "consumer", .subtask_idx = 1, .host = "192.0.2.2", .data_port = 18001}},
        .extra_config = "clink_attempt=2",
    });
    in.tasks.push_back(DeploymentTask{
        .role = "consumer",
        .subtask_idx = 1,
        .data_port = 18001,
        .peers = {},
        .extra_config = "",
    });
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    EXPECT_EQ(out.job_id, 7u);
    ASSERT_EQ(out.tasks.size(), 2u);

    EXPECT_EQ(out.tasks[0].role, "producer");
    EXPECT_EQ(out.tasks[0].extra_config, "clink_attempt=2");
    ASSERT_EQ(out.tasks[0].peers.size(), 2u);
    EXPECT_EQ(out.tasks[0].peers[0].host, "192.0.2.1");
    EXPECT_EQ(out.tasks[0].peers[0].data_port, 18000);
    EXPECT_EQ(out.tasks[0].peers[1].subtask_idx, 1u);
    EXPECT_EQ(out.tasks[0].peers[1].host, "192.0.2.2");

    EXPECT_EQ(out.tasks[1].role, "consumer");
    EXPECT_EQ(out.tasks[1].subtask_idx, 1u);
    EXPECT_EQ(out.tasks[1].data_port, 18001);
}

TEST(WireProtocol, StartJobAndCancelJobRoundTripWithEmptyBody) {
    auto a = round_trip(MessageKind::StartJob, StartJobMsg{}, decode_start_job);
    (void)a;

    auto b = round_trip(MessageKind::CancelJob, CancelJobMsg{}, decode_cancel_job);
    (void)b;

    // Body is empty for these - the only assertion is that encode/decode
    // don't trip and the kind byte parses back.
    SUCCEED();
}

TEST(WireProtocol, SubtaskFinishedRoundTrips) {
    SubtaskFinishedMsg in_ok{.job_id = 1,
                             .worker_id = "worker-a",
                             .role = "producer",
                             .subtask_idx = 0,
                             .had_error = false,
                             .error_message = ""};
    auto out_ok = round_trip(MessageKind::SubtaskFinished, in_ok, decode_subtask_finished);
    EXPECT_EQ(out_ok.job_id, 1u);
    EXPECT_EQ(out_ok.worker_id, "worker-a");
    EXPECT_FALSE(out_ok.had_error);
    EXPECT_EQ(out_ok.error_message, "");

    SubtaskFinishedMsg in_err{.job_id = 99,
                              .worker_id = "worker-b",
                              .role = "consumer",
                              .subtask_idx = 3,
                              .had_error = true,
                              .error_message = "stream closed early"};
    auto out_err = round_trip(MessageKind::SubtaskFinished, in_err, decode_subtask_finished);
    EXPECT_EQ(out_err.job_id, 99u);
    EXPECT_TRUE(out_err.had_error);
    EXPECT_EQ(out_err.subtask_idx, 3u);
    EXPECT_EQ(out_err.error_message, "stream closed early");
}

TEST(WireProtocol, HeartbeatRoundTrips) {
    HeartbeatMsg in{.worker_id = "worker-z", .sequence = 42};
    auto out = round_trip(MessageKind::Heartbeat, in, decode_heartbeat);
    EXPECT_EQ(out.worker_id, "worker-z");
    EXPECT_EQ(out.sequence, 42U);

    HeartbeatAckMsg ack{.worker_id = "worker-z", .sequence = 42, .coordinator_epoch = 7};
    auto ack_out = round_trip(MessageKind::HeartbeatAck, ack, decode_heartbeat_ack);
    EXPECT_EQ(ack_out.worker_id, "worker-z");
    EXPECT_EQ(ack_out.sequence, 42U);
    EXPECT_EQ(ack_out.coordinator_epoch, 7U);
}

TEST(WireProtocol, VersionOneHeartbeatDefaultsSequenceToZero) {
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::Heartbeat));
    b.put_string("old-worker");
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::Heartbeat);
    const auto heartbeat = decode_heartbeat(r);
    EXPECT_EQ(heartbeat.worker_id, "old-worker");
    EXPECT_EQ(heartbeat.sequence, 0U);
}

// ----- Backwards-compat: Register without slot_count must still parse -----

TEST(WireProtocol, RegisterWithoutSlotCountDefaultsToOne) {
    // Old peers built without slot_count - encode the body manually so it
    // ends after data_host. The decoder must accept and default to 1.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::Register));
    b.put_string("legacy-worker");
    b.put_string("10.0.0.99");
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::Register);
    auto out = decode_register(r);
    EXPECT_EQ(out.worker_id, "legacy-worker");
    EXPECT_EQ(out.data_host, "10.0.0.99");
    EXPECT_EQ(out.slot_count, 1u);
}

// ----- StopJob: the graceful-stop trio -----

TEST(WireProtocol, StopJobRoundTrips) {
    StopJobMsg in{.job_id = 7, .timeout_ms = 45'000};
    auto out = round_trip(MessageKind::StopJob, in, decode_stop_job);
    EXPECT_EQ(out.job_id, in.job_id);
    EXPECT_EQ(out.timeout_ms, in.timeout_ms);
}

TEST(WireProtocol, StopSubtasksCarriesTheCoordinatorEpoch) {
    // The worker fences this frame like every other coordinator->worker command,
    // so the epoch has to survive the round trip or a superseded coordinator
    // could stop a job it no longer owns.
    StopSubtasksMsg in{.job_id = 3, .coordinator_epoch = 9};
    auto out = round_trip(MessageKind::StopSubtasks, in, decode_stop_subtasks);
    EXPECT_EQ(out.job_id, in.job_id);
    EXPECT_EQ(out.coordinator_epoch, 9u);
}

TEST(WireProtocol, StopJobAckRoundTripsTheSavepointId) {
    StopJobAckMsg in{
        .job_id = 4, .ok = true, .savepoint_checkpoint_id = 42, .message = "stopped at 42"};
    auto out = round_trip(MessageKind::StopJobAck, in, decode_stop_job_ack);
    EXPECT_EQ(out.job_id, 4u);
    EXPECT_TRUE(out.ok);
    EXPECT_EQ(out.savepoint_checkpoint_id, 42u)
        << "the checkpoint id is the whole point of the ack - it is what an operator resubmits "
           "from";
    EXPECT_EQ(out.message, "stopped at 42");

    StopJobAckMsg refused{
        .job_id = 4, .ok = false, .savepoint_checkpoint_id = 0, .message = "no such job"};
    auto refused_out = round_trip(MessageKind::StopJobAck, refused, decode_stop_job_ack);
    EXPECT_FALSE(refused_out.ok);
    EXPECT_EQ(refused_out.savepoint_checkpoint_id, 0u);
    EXPECT_EQ(refused_out.message, "no such job");
}

// ----- ListJobsAck: terminal status rides an additive tail -----

TEST(WireProtocol, ListJobsAckRoundTripsTerminalStatusPerJob) {
    // Per-job, and in order: the tail is a parallel array, so a bug that
    // dropped or shifted it would be invisible with one job or one status.
    ListJobsAckMsg in;
    in.jobs.push_back({.job_id = 1,
                       .total_subtasks = 4,
                       .completed_subtasks = 4,
                       .completion_signalled = true,
                       .terminal_status = JobTerminalStatus::CompletedOk});
    in.jobs.push_back({.job_id = 2,
                       .total_subtasks = 2,
                       .completed_subtasks = 1,
                       .completion_signalled = false,
                       .terminal_status = JobTerminalStatus::Running});
    in.jobs.push_back({.job_id = 3,
                       .total_subtasks = 3,
                       .completed_subtasks = 3,
                       .completion_signalled = true,
                       .terminal_status = JobTerminalStatus::Failed});
    in.jobs.push_back({.job_id = 4,
                       .total_subtasks = 1,
                       .completed_subtasks = 0,
                       .completion_signalled = true,
                       .terminal_status = JobTerminalStatus::Cancelled});

    auto out = round_trip(MessageKind::ListJobsAck, in, decode_list_jobs_ack);
    ASSERT_EQ(out.jobs.size(), in.jobs.size());
    for (std::size_t i = 0; i < in.jobs.size(); ++i) {
        EXPECT_EQ(out.jobs[i].job_id, in.jobs[i].job_id) << "job " << i;
        EXPECT_EQ(out.jobs[i].completion_signalled, in.jobs[i].completion_signalled) << "job " << i;
        EXPECT_EQ(out.jobs[i].terminal_status, in.jobs[i].terminal_status) << "job " << i;
    }
}

TEST(WireProtocol, ListJobsAckWithoutTheTailDecodesAsRunning) {
    // A peer built before the tail existed sends the job group and stops. Every
    // field before the tail must still decode, and the status must read as
    // Running - "this peer cannot tell us" - rather than as a terminal verdict
    // nobody sent. Encoded by hand because the current encoder always writes
    // the tail.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::ListJobsAck));
    b.put_u32_be(2);
    b.put_u64_be(7);
    b.put_u32_be(3);
    b.put_u32_be(3);
    b.put_u8(1);
    b.put_u64_be(8);
    b.put_u32_be(5);
    b.put_u32_be(2);
    b.put_u8(0);
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::ListJobsAck);
    auto out = decode_list_jobs_ack(r);
    ASSERT_EQ(out.jobs.size(), 2u);
    EXPECT_EQ(out.jobs[0].job_id, 7u);
    EXPECT_EQ(out.jobs[0].completed_subtasks, 3u);
    EXPECT_TRUE(out.jobs[0].completion_signalled);
    EXPECT_EQ(out.jobs[0].terminal_status, JobTerminalStatus::Running);
    EXPECT_EQ(out.jobs[1].job_id, 8u);
    EXPECT_EQ(out.jobs[1].total_subtasks, 5u);
    EXPECT_FALSE(out.jobs[1].completion_signalled);
    EXPECT_EQ(out.jobs[1].terminal_status, JobTerminalStatus::Running);
}

TEST(WireProtocol, ListJobsAckRejectsAnOutOfRangeTerminalStatus) {
    // A future peer may send a status this build has no name for. It must not
    // become a garbage enum value that later compares equal to Failed and gets
    // reported as one.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::ListJobsAck));
    b.put_u32_be(1);
    b.put_u64_be(9);
    b.put_u32_be(1);
    b.put_u32_be(1);
    b.put_u8(1);
    b.put_u32_be(1);
    b.put_u8(200);  // not a status this build knows
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::ListJobsAck);
    auto out = decode_list_jobs_ack(r);
    ASSERT_EQ(out.jobs.size(), 1u);
    EXPECT_EQ(out.jobs[0].terminal_status, JobTerminalStatus::Running);
}

// ----- MessageKind values are stable -----

TEST(WireProtocol, MessageKindValuesArePinnedForCompatibility) {
    // Anyone changing these breaks every cross-version cluster. The test
    // exists so the change is loud and intentional.
    EXPECT_EQ(static_cast<int>(MessageKind::Register), 1);
    EXPECT_EQ(static_cast<int>(MessageKind::SubtaskFinished), 2);
    EXPECT_EQ(static_cast<int>(MessageKind::Heartbeat), 3);
    EXPECT_EQ(static_cast<int>(MessageKind::HelloClient), 4);
    EXPECT_EQ(static_cast<int>(MessageKind::SubmitJob), 5);
    EXPECT_EQ(static_cast<int>(MessageKind::SubtaskListening), 6);
    EXPECT_EQ(static_cast<int>(MessageKind::RescaleJob), 11);
    // StopJob shares the client->coordinator space with Savepoint=13 and
    // RescaleOperator=12. A duplicate there silently routes the frame to the
    // wrong handler, which is how Savepoint once collided with RescaleOperator
    // and aborted the coordinator on every savepoint - hence pinning it.
    EXPECT_EQ(static_cast<int>(MessageKind::StopJob), 14);
    EXPECT_EQ(static_cast<int>(MessageKind::StopSubtasks), 117);
    EXPECT_EQ(static_cast<int>(MessageKind::StopJobAck), 118);
    EXPECT_EQ(static_cast<int>(MessageKind::RegisterAck), 100);
    EXPECT_EQ(static_cast<int>(MessageKind::Deploy), 101);
    EXPECT_EQ(static_cast<int>(MessageKind::StartJob), 102);
    EXPECT_EQ(static_cast<int>(MessageKind::CancelJob), 103);
    EXPECT_EQ(static_cast<int>(MessageKind::PeerUpdate), 104);
    EXPECT_EQ(static_cast<int>(MessageKind::SubmitJobAck), 105);
    EXPECT_EQ(static_cast<int>(MessageKind::JobCompleted), 106);
    EXPECT_EQ(static_cast<int>(MessageKind::RescaleJobAck), 111);
    EXPECT_EQ(static_cast<int>(MessageKind::HeartbeatAck), 122);
}

TEST(WireProtocol, DeployRoundTripsRescaleDirectivesPerTask) {
    // The rescale directives ride at the end of the Deploy body so old
    // peers ignore them. Round-trip both an explicitly-rescaled task
    // and a default-init task in the same message to confirm the
    // per-task pairing is preserved.
    DeployMsg in;
    in.job_id = 99;
    in.tasks.push_back(DeploymentTask{
        .role = "agg",
        .subtask_idx = 2,
        .data_port = 0,
        .peers = {},
        .extra_config = "",
        .restore_from_subtask_idx = 1,  // scale-up: one parent
        .restore_from_parent_count = 1,
        .key_group_first = 64,
        .key_group_last = 96,
    });
    in.tasks.push_back(DeploymentTask{
        .role = "agg", .subtask_idx = 3, .data_port = 0, .peers = {}, .extra_config = "",
        // default rescale fields = no override
    });
    in.tasks.push_back(DeploymentTask{
        .role = "agg",
        .subtask_idx = 0,
        .data_port = 0,
        .peers = {},
        .extra_config = "",
        .restore_from_subtask_idx = 0,  // scale-down: 4 parents merged
        .restore_from_parent_count = 4,
        .key_group_first = 0,
        .key_group_last = 64,
    });
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    ASSERT_EQ(out.tasks.size(), 3u);
    EXPECT_EQ(out.tasks[0].restore_from_subtask_idx, 1u);
    EXPECT_EQ(out.tasks[0].restore_from_parent_count, 1u);
    EXPECT_EQ(out.tasks[0].key_group_first, 64);
    EXPECT_EQ(out.tasks[0].key_group_last, 96);
    EXPECT_EQ(out.tasks[1].restore_from_subtask_idx, kRestoreFromSelf);
    EXPECT_EQ(out.tasks[1].restore_from_parent_count, 1u);
    EXPECT_EQ(out.tasks[1].key_group_first, 0);
    EXPECT_EQ(out.tasks[1].key_group_last, 0);
    EXPECT_EQ(out.tasks[2].restore_from_subtask_idx, 0u);
    EXPECT_EQ(out.tasks[2].restore_from_parent_count, 4u);
    EXPECT_EQ(out.tasks[2].key_group_first, 0);
    EXPECT_EQ(out.tasks[2].key_group_last, 64);
}

// Two MessageKinds deliberately share the value 117, and this pins that
// as a decision rather than an accident.
//
// CommitConfirmed travels worker -> coordinator and StopSubtasks travels
// coordinator -> worker, so each dispatch loop sees only one of them and
// the value is unambiguous in context. That is sound, but it is the kind
// of soundness that survives only while someone remembers it: the file's
// own history records a duplicate that "aborted the coordinator on every
// savepoint" when Savepoint collided with RescaleOperator in the SAME
// direction. This test states the rule - a value may be shared ONLY
// across directions - so a third kind added at 117, or a future move of
// either of these onto the other's side, fails here instead of routing a
// frame into the wrong handler at runtime.
TEST(WireProtocol, MessageKindsMayShareAValueOnlyAcrossDirections) {
    // The pair that shares 117 today.
    EXPECT_EQ(static_cast<int>(MessageKind::CommitConfirmed), 117);
    EXPECT_EQ(static_cast<int>(MessageKind::StopSubtasks), 117);

    // Every kind the COORDINATOR dispatches (arriving from a worker or a
    // client) must be unique among itself.
    const std::vector<std::pair<MessageKind, const char*>> to_coordinator = {
        {MessageKind::Register, "Register"},
        {MessageKind::Heartbeat, "Heartbeat"},
        {MessageKind::SubtaskFinished, "SubtaskFinished"},
        {MessageKind::SubtaskListening, "SubtaskListening"},
        {MessageKind::SubtaskCheckpointed, "SubtaskCheckpointed"},
        {MessageKind::CommitConfirmed, "CommitConfirmed"},
        {MessageKind::RequestFinalCheckpoint, "RequestFinalCheckpoint"},
        {MessageKind::SubmitJob, "SubmitJob"},
        {MessageKind::CancelJob, "CancelJob"},
        {MessageKind::ListJobs, "ListJobs"},
        {MessageKind::Savepoint, "Savepoint"},
        {MessageKind::StopJob, "StopJob"},
        {MessageKind::BeginRescaleAck, "BeginRescaleAck"},
    };
    // Every kind the WORKER dispatches (arriving from the coordinator).
    const std::vector<std::pair<MessageKind, const char*>> to_worker = {
        {MessageKind::RegisterAck, "RegisterAck"},
        {MessageKind::Deploy, "Deploy"},
        {MessageKind::PeerUpdate, "PeerUpdate"},
        {MessageKind::TriggerCheckpoint, "TriggerCheckpoint"},
        {MessageKind::CommitCheckpoint, "CommitCheckpoint"},
        {MessageKind::AbortCheckpoint, "AbortCheckpoint"},
        {MessageKind::FinalCheckpointAssigned, "FinalCheckpointAssigned"},
        {MessageKind::BeginRescale, "BeginRescale"},
        {MessageKind::StopSubtasks, "StopSubtasks"},
        {MessageKind::CutoverPeerUpdate, "CutoverPeerUpdate"},
        {MessageKind::HeartbeatAck, "HeartbeatAck"},
        {MessageKind::CutoverRebind, "CutoverRebind"},
    };
    const auto assert_unique = [](const auto& kinds, const char* direction) {
        std::map<int, std::string> seen;
        for (const auto& [kind, name] : kinds) {
            const int value = static_cast<int>(kind);
            auto it = seen.find(value);
            ASSERT_EQ(it, seen.end())
                << direction << ": " << name << " and " << it->second << " both use " << value
                << ", so one of them will be routed to the other's handler";
            seen.emplace(value, name);
        }
    };
    assert_unique(to_coordinator, "coordinator-bound");
    assert_unique(to_worker, "worker-bound");
}

TEST(WireProtocol, SubmitJobCarriesAlignmentMode) {
    SubmitJobMsg in;
    in.graph_json = "{}";
    in.checkpoint.checkpoint_dir = "/var/clink/state";
    in.checkpoint.alignment = CheckpointAlignment::Unaligned;
    auto out = round_trip(MessageKind::SubmitJob, in, decode_submit_job);
    EXPECT_EQ(out.checkpoint.alignment, CheckpointAlignment::Unaligned);

    // Adaptive round-trips too; an OLDER decoder maps the unknown value
    // to Aligned, the safe default, which is the degradation this enum
    // was extended under.
    SubmitJobMsg in_adaptive;
    in_adaptive.graph_json = "{}";
    in_adaptive.checkpoint.checkpoint_dir = "/var/clink/state";
    in_adaptive.checkpoint.alignment = CheckpointAlignment::Adaptive;
    auto out_adaptive = round_trip(MessageKind::SubmitJob, in_adaptive, decode_submit_job);
    EXPECT_EQ(out_adaptive.checkpoint.alignment, CheckpointAlignment::Adaptive);

    // Default-init message round-trips as Aligned.
    SubmitJobMsg in_default;
    in_default.graph_json = "{}";
    auto out_default = round_trip(MessageKind::SubmitJob, in_default, decode_submit_job);
    EXPECT_EQ(out_default.checkpoint.alignment, CheckpointAlignment::Aligned);
}

TEST(WireProtocol, DeployCarriesUnalignedCheckpointsFlag) {
    DeployMsg in;
    in.job_id = 5;
    in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
    in.unaligned_checkpoints = true;
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    EXPECT_TRUE(out.unaligned_checkpoints);

    // Default-init Deploy decodes as aligned (the historic shape).
    DeployMsg in_default;
    in_default.job_id = 6;
    in_default.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
    auto out_default = round_trip(MessageKind::Deploy, in_default, decode_deploy);
    EXPECT_FALSE(out_default.unaligned_checkpoints);
    EXPECT_FALSE(out_default.adaptive_barrier_mode);
}

TEST(WireProtocol, DeployCarriesAdaptiveBarrierModeFlag) {
    DeployMsg in;
    in.job_id = 5;
    in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
    in.adaptive_barrier_mode = true;
    in.coordinator_epoch = 4;
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    EXPECT_TRUE(out.adaptive_barrier_mode);
    // The flag sits before the epoch on the wire (the epoch stays the
    // final field by contract); both must survive together.
    EXPECT_EQ(out.coordinator_epoch, 4U);
    EXPECT_FALSE(out.unaligned_checkpoints)
        << "adaptive deploys leave the static flag false - aligned is the default the "
           "per-trigger stamp overrides";
}

TEST(WireProtocol, DeployCarriesExpectedStateVersions) {
    DeployMsg in;
    in.job_id = 7;
    in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
    // Mix a slotted (4-field) and slotless (3-field) line so the wire is
    // proven to carry the slot suffix verbatim, not just the legacy shape.
    in.expected_state_versions_packed = "123|i64_sum|3|left_buf\n456|i64_cnt|2\n";
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    EXPECT_EQ(out.expected_state_versions_packed, in.expected_state_versions_packed);

    // Default-init Deploy decodes with no expected versions (older peers /
    // jobs that never declared a schema version).
    DeployMsg in_default;
    in_default.job_id = 8;
    in_default.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
    auto out_default = round_trip(MessageKind::Deploy, in_default, decode_deploy);
    EXPECT_TRUE(out_default.expected_state_versions_packed.empty());
}

TEST(WireProtocol, SavepointAndAckRoundTrip) {
    SavepointMsg in{.job_id = 42, .timeout_ms = 15000};
    auto out = round_trip(MessageKind::Savepoint, in, decode_savepoint);
    EXPECT_EQ(out.job_id, 42u);
    EXPECT_EQ(out.timeout_ms, 15000);

    SavepointAckMsg ack_in{
        .job_id = 42,
        .ok = true,
        .checkpoint_id = 17,
        .checkpoint_dir = "/var/clink/state",
        .message = "savepoint complete",
    };
    auto ack_out = round_trip(MessageKind::SavepointAck, ack_in, decode_savepoint_ack);
    EXPECT_EQ(ack_out.job_id, 42u);
    EXPECT_TRUE(ack_out.ok);
    EXPECT_EQ(ack_out.checkpoint_id, 17u);
    EXPECT_EQ(ack_out.checkpoint_dir, "/var/clink/state");
    EXPECT_EQ(ack_out.message, "savepoint complete");
}

TEST(WireProtocol, RescaleJobAndAckRoundTrip) {
    RescaleJobMsg in{
        .job_id = 42,
        .role_parallelism = {{"source", 4}, {"agg", 8}},
    };
    auto out = round_trip(MessageKind::RescaleJob, in, decode_rescale_job);
    EXPECT_EQ(out.job_id, 42u);
    ASSERT_EQ(out.role_parallelism.size(), 2u);
    EXPECT_EQ(out.role_parallelism[0].first, "source");
    EXPECT_EQ(out.role_parallelism[0].second, 4u);
    EXPECT_EQ(out.role_parallelism[1].first, "agg");
    EXPECT_EQ(out.role_parallelism[1].second, 8u);

    RescaleJobAckMsg ack_in{.job_id = 42, .ok = false, .message = "parallelism not a multiple"};
    auto ack_out = round_trip(MessageKind::RescaleJobAck, ack_in, decode_rescale_job_ack);
    EXPECT_EQ(ack_out.job_id, 42u);
    EXPECT_FALSE(ack_out.ok);
    EXPECT_EQ(ack_out.message, "parallelism not a multiple");
}

// ----- New v2 messages for client submission and port discovery -----

TEST(WireProtocol, SubmitJobAndAckRoundTrip) {
    SubmitJobMsg sj{.graph_json = R"({"ops":[]})"};
    auto sj_out = round_trip(MessageKind::SubmitJob, sj, decode_submit_job);
    EXPECT_EQ(sj_out.graph_json, sj.graph_json);

    SubmitJobAckMsg ack{.job_id = 17, .ok = true, .message = ""};
    auto ack_out = round_trip(MessageKind::SubmitJobAck, ack, decode_submit_job_ack);
    EXPECT_EQ(ack_out.job_id, 17u);
    EXPECT_TRUE(ack_out.ok);

    SubmitJobAckMsg rej{.job_id = 0, .ok = false, .message = "no available slots"};
    auto rej_out = round_trip(MessageKind::SubmitJobAck, rej, decode_submit_job_ack);
    EXPECT_EQ(rej_out.job_id, 0u);
    EXPECT_FALSE(rej_out.ok);
    EXPECT_EQ(rej_out.message, "no available slots");
}

TEST(WireProtocol, JobCompletedRoundTrips) {
    JobCompletedMsg ok{.job_id = 11, .ok = true, .errors = {}};
    auto ok_out = round_trip(MessageKind::JobCompleted, ok, decode_job_completed);
    EXPECT_EQ(ok_out.job_id, 11u);
    EXPECT_TRUE(ok_out.ok);
    EXPECT_TRUE(ok_out.errors.empty());

    JobCompletedMsg bad{.job_id = 12,
                        .ok = false,
                        .errors = {"worker-a/producer[0]: send failed", "worker-b: lost"}};
    auto bad_out = round_trip(MessageKind::JobCompleted, bad, decode_job_completed);
    EXPECT_EQ(bad_out.job_id, 12u);
    EXPECT_FALSE(bad_out.ok);
    ASSERT_EQ(bad_out.errors.size(), 2u);
    EXPECT_EQ(bad_out.errors[0], "worker-a/producer[0]: send failed");
}

TEST(WireProtocol, SubtaskListeningRoundTrips) {
    SubtaskListeningMsg in;
    in.job_id = 5;
    in.worker_id = "worker-b";
    in.role = "consumer";
    in.subtask_idx = 0;
    in.host = "10.0.0.7";
    in.edge_ports.push_back(
        {.upstream_role = "producer", .upstream_subtask_idx = 0, .port = 39444});
    in.edge_ports.push_back(
        {.upstream_role = "producer", .upstream_subtask_idx = 1, .port = 39445});
    auto out = round_trip(MessageKind::SubtaskListening, in, decode_subtask_listening);
    EXPECT_EQ(out.job_id, 5u);
    EXPECT_EQ(out.worker_id, "worker-b");
    EXPECT_EQ(out.host, "10.0.0.7");
    ASSERT_EQ(out.edge_ports.size(), 2u);
    EXPECT_EQ(out.edge_ports[0].upstream_role, "producer");
    EXPECT_EQ(out.edge_ports[0].upstream_subtask_idx, 0u);
    EXPECT_EQ(out.edge_ports[0].port, 39444);
    EXPECT_EQ(out.edge_ports[1].upstream_subtask_idx, 1u);
    EXPECT_EQ(out.edge_ports[1].port, 39445);
}

TEST(WireProtocol, PeerUpdateRoundTrips) {
    PeerUpdateMsg in;
    in.job_id = 5;
    in.tasks.push_back(PeerUpdateMsg::TaskPeers{
        .role = "producer",
        .subtask_idx = 0,
        .peers = {PeerAddress{
            .role = "consumer", .subtask_idx = 0, .host = "10.0.0.7", .data_port = 39444}},
    });
    auto out = round_trip(MessageKind::PeerUpdate, in, decode_peer_update);
    EXPECT_EQ(out.job_id, 5u);
    ASSERT_EQ(out.tasks.size(), 1u);
    EXPECT_EQ(out.tasks[0].role, "producer");
    ASSERT_EQ(out.tasks[0].peers.size(), 1u);
    EXPECT_EQ(out.tasks[0].peers[0].host, "10.0.0.7");
    EXPECT_EQ(out.tasks[0].peers[0].data_port, 39444);
}

TEST(WireProtocol, HelloClientHasEmptyBody) {
    auto out = round_trip(MessageKind::HelloClient, HelloClientMsg{}, decode_hello_client);
    (void)out;
    SUCCEED();
}

// AbortCheckpoint wire frame round-trips. Same payload
// shape as CommitCheckpoint (job_id + checkpoint_id); the kind byte
// is what distinguishes them.
TEST(WireProtocol, AbortCheckpointRoundTrips) {
    AbortCheckpointMsg in{.job_id = 42, .checkpoint_id = 17};
    auto out = round_trip(MessageKind::AbortCheckpoint, in, decode_abort_checkpoint);
    EXPECT_EQ(out.job_id, 42u);
    EXPECT_EQ(out.checkpoint_id, 17u);
}

TEST(WireProtocol, CommitCheckpointRoundTrips) {
    CommitCheckpointMsg in{.job_id = 5, .checkpoint_id = 99};
    auto out = round_trip(MessageKind::CommitCheckpoint, in, decode_commit_checkpoint);
    EXPECT_EQ(out.job_id, 5u);
    EXPECT_EQ(out.checkpoint_id, 99u);
}

// CommitConfirmed wire frame round-trips. worker -> coordinator report
// that one sink subtask's commit callbacks all executed for a checkpoint;
// the role + subtask_idx pair identifies which confirmation to retire.
TEST(WireProtocol, CommitConfirmedRoundTrips) {
    CommitConfirmedMsg in{.job_id = 5, .checkpoint_id = 99, .role = "sink", .subtask_idx = 3};
    auto out = round_trip(MessageKind::CommitConfirmed, in, decode_commit_confirmed);
    EXPECT_EQ(out.job_id, 5u);
    EXPECT_EQ(out.checkpoint_id, 99u);
    EXPECT_EQ(out.role, "sink");
    EXPECT_EQ(out.subtask_idx, 3u);
}

// BeginRescale message frame. coordinator -> worker signal that starts
// the dual-run rescale: target_parallelism + cutover_checkpoint
// pinpoint exactly which checkpoint the new subtasks load their
// state slice from.
TEST(WireProtocol, BeginRescaleRoundTrips) {
    BeginRescaleMsg in{
        .job_id = 42, .op_id = "join", .target_parallelism = 8, .cutover_checkpoint = 1234};
    auto out = round_trip(MessageKind::BeginRescale, in, decode_begin_rescale);
    EXPECT_EQ(out.job_id, 42u);
    EXPECT_EQ(out.op_id, "join");
    EXPECT_EQ(out.target_parallelism, 8u);
    EXPECT_EQ(out.cutover_checkpoint, 1234u);
}

TEST(WireProtocol, BeginRescaleHandlesEmptyOpId) {
    // Defensive: an empty op_id is invalid at the coordinator level but the
    // wire codec shouldn't choke on it. The coordinator dispatch is the
    // layer that validates / rejects.
    BeginRescaleMsg in{.job_id = 1, .op_id = "", .target_parallelism = 2, .cutover_checkpoint = 0};
    auto out = round_trip(MessageKind::BeginRescale, in, decode_begin_rescale);
    EXPECT_EQ(out.op_id, "");
    EXPECT_EQ(out.target_parallelism, 2u);
}

// Per-operator rescale request + ack.
TEST(WireProtocol, RescaleOperatorRoundTrips) {
    RescaleOperatorMsg in{.job_id = 7, .op_id = "join", .new_parallelism = 8};
    auto out = round_trip(MessageKind::RescaleOperator, in, decode_rescale_operator);
    EXPECT_EQ(out.job_id, 7u);
    EXPECT_EQ(out.op_id, "join");
    EXPECT_EQ(out.new_parallelism, 8u);
}

TEST(WireProtocol, RescaleOperatorAckOkRoundTrips) {
    RescaleOperatorAckMsg in{.job_id = 7, .ok = true, .accepted_target = 8, .message = ""};
    auto out = round_trip(MessageKind::RescaleOperatorAck, in, decode_rescale_operator_ack);
    EXPECT_EQ(out.job_id, 7u);
    EXPECT_TRUE(out.ok);
    EXPECT_EQ(out.accepted_target, 8u);
    EXPECT_EQ(out.message, "");
}

TEST(WireProtocol, RescaleOperatorAckRejectionCarriesReason) {
    RescaleOperatorAckMsg in{.job_id = 7,
                             .ok = false,
                             .accepted_target = 0,
                             .message = "requested parallelism 100 above max_parallelism 8"};
    auto out = round_trip(MessageKind::RescaleOperatorAck, in, decode_rescale_operator_ack);
    EXPECT_FALSE(out.ok);
    EXPECT_NE(out.message.find("above max_parallelism"), std::string::npos);
}

// Recovery-default resolution: the default (kRestartAuto) self-heals when
// checkpointing is enabled and fail-fasts otherwise; explicit values pass
// through unchanged so a user can force fail-fast or a specific cap.
TEST(RecoveryDefaults, EffectiveMaxRestartsResolution) {
    CheckpointConfig def;  // default-constructed -> kRestartAuto
    EXPECT_EQ(def.max_restarts_on_worker_loss, kRestartAuto);

    // auto + checkpointing -> self-heal default.
    CheckpointConfig ckpt = def;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    EXPECT_EQ(effective_max_restarts(ckpt), kDefaultSelfHealRestarts);

    // auto + no checkpointing -> fail-fast (nothing to restore from).
    EXPECT_EQ(effective_max_restarts(def), 0u);

    // explicit 0 -> fail-fast even with checkpointing.
    CheckpointConfig off = ckpt;
    off.max_restarts_on_worker_loss = 0;
    EXPECT_EQ(effective_max_restarts(off), 0u);

    // explicit N -> used verbatim.
    CheckpointConfig fixed = ckpt;
    fixed.max_restarts_on_worker_loss = 3;
    EXPECT_EQ(effective_max_restarts(fixed), 3u);
}

// The coordinator's client-loop dispatch matches on the MessageKind byte, so every
// client->coordinator request kind MUST have a distinct value. A collision routes a
// frame to the wrong handler: Savepoint (13) once shared value 12 with
// RescaleOperator, so a savepoint frame decoded as a RescaleOperatorMsg and
// then, with no `continue`, fell through to the savepoint handler on an
// already-consumed reader - throwing "truncated payload" and aborting the coordinator.
TEST(WireProtocol, ClientRequestKindsAreDistinct) {
    const std::vector<std::pair<const char*, MessageKind>> kinds{
        {"HelloClient", MessageKind::HelloClient},
        {"SubmitJob", MessageKind::SubmitJob},
        {"ListJobs", MessageKind::ListJobs},
        {"CancelJob", MessageKind::CancelJob},
        {"RescaleJob", MessageKind::RescaleJob},
        {"RescaleOperator", MessageKind::RescaleOperator},
        {"Savepoint", MessageKind::Savepoint},
    };
    for (std::size_t i = 0; i < kinds.size(); ++i) {
        for (std::size_t j = i + 1; j < kinds.size(); ++j) {
            EXPECT_NE(static_cast<int>(kinds[i].second), static_cast<int>(kinds[j].second))
                << kinds[i].first << " and " << kinds[j].first << " share a MessageKind value";
        }
    }
    // Pin the specific pair that regressed.
    EXPECT_NE(static_cast<int>(MessageKind::Savepoint),
              static_cast<int>(MessageKind::RescaleOperator));
}

// ----- fencing epoch on the coordinator -> worker control frames -----
//
// Every control frame that changes what a worker is DOING carries the
// sending coordinator's fencing epoch, so the worker can refuse a command
// from a coordinator that has since lost leadership. Two properties matter
// and are pinned separately below:
//
//   1. The epoch survives the round trip on every one of those frames. A
//      frame that silently loses it decodes as epoch 0, which is the
//      "unfenced" value - so a dropped field does not fail loudly, it
//      turns fencing off. That is exactly the failure a test must catch.
//
//   2. A frame produced by a PRE-FENCING peer - one whose body ends before
//      the epoch field - still decodes, and decodes as 0. Without this a
//      rolling upgrade would break the moment the first upgraded node
//      talked to a node that had not restarted yet.

namespace {

// Encode `original`, then chop the trailing 8-byte epoch off the body, so
// what MessageReader sees is byte-identical to what a pre-fencing peer
// would have sent.
template <typename Msg, typename Decoder>
Msg round_trip_without_epoch_field(MessageKind kind,
                                   const Msg& original,
                                   Decoder decode,
                                   std::size_t tail_bytes = 8) {
    auto body = body_of(encode_frame(kind, original));
    EXPECT_GT(body.size(), tail_bytes);
    body.resize(body.size() - tail_bytes);
    MessageReader r(std::move(body));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), kind);
    return decode(r);
}

}  // namespace

TEST(WireProtocolFencing, EveryControlFrameCarriesTheCoordinatorEpoch) {
    {
        RegisterAckMsg in{.ok = true, .message = "welcome", .coordinator_epoch = 9};
        EXPECT_EQ(round_trip(MessageKind::RegisterAck, in, decode_register_ack).coordinator_epoch,
                  9U);
    }
    {
        DeployMsg in;
        in.job_id = 1;
        in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 0});
        in.coordinator_epoch = 9;
        EXPECT_EQ(round_trip(MessageKind::Deploy, in, decode_deploy).coordinator_epoch, 9U);
    }
    {
        CancelJobMsg in;
        in.job_id = 1;
        in.coordinator_epoch = 9;
        EXPECT_EQ(round_trip(MessageKind::CancelJob, in, decode_cancel_job).coordinator_epoch, 9U);
    }
    {
        TriggerCheckpointMsg in{.job_id = 1,
                                .checkpoint_id = 2,
                                .coordinator_epoch = 9,
                                .generation = 7,
                                .barrier_mode_plus1 = 2};
        const auto out = round_trip(MessageKind::TriggerCheckpoint, in, decode_trigger_checkpoint);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.generation, 7U);
        EXPECT_EQ(out.barrier_mode_plus1, 2U);
    }
    {
        CommitCheckpointMsg in{
            .job_id = 1, .checkpoint_id = 2, .coordinator_epoch = 9, .retain_floor = 5};
        const auto out = round_trip(MessageKind::CommitCheckpoint, in, decode_commit_checkpoint);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.retain_floor, 5U);
    }
    {
        AbortCheckpointMsg in{.job_id = 1, .checkpoint_id = 2, .coordinator_epoch = 9};
        EXPECT_EQ(
            round_trip(MessageKind::AbortCheckpoint, in, decode_abort_checkpoint).coordinator_epoch,
            9U);
    }
    {
        FinalCheckpointAssignedMsg in;
        in.job_id = 1;
        in.role = "src";
        in.subtask_idx = 0;
        in.final_checkpoint_id = 4;
        in.coordinator_epoch = 9;
        EXPECT_EQ(
            round_trip(MessageKind::FinalCheckpointAssigned, in, decode_final_checkpoint_assigned)
                .coordinator_epoch,
            9U);
    }
    {
        BeginRescaleMsg in;
        in.job_id = 1;
        in.op_id = "agg";
        in.target_parallelism = 4;
        in.cutover_checkpoint = 7;
        in.coordinator_epoch = 9;
        EXPECT_EQ(round_trip(MessageKind::BeginRescale, in, decode_begin_rescale).coordinator_epoch,
                  9U);
    }
    {
        PeerUpdateMsg in;
        in.job_id = 1;
        in.coordinator_epoch = 9;
        auto out = round_trip(MessageKind::PeerUpdate, in, decode_peer_update);
        EXPECT_EQ(out.coordinator_epoch, 9U);

        // Again with a populated task list, so the epoch is proven to survive
        // AFTER a variable-length section rather than only on an empty body.
        PeerUpdateMsg with_peers;
        with_peers.job_id = 1;
        with_peers.coordinator_epoch = 9;
        PeerUpdateMsg::TaskPeers tp;
        tp.role = "sink";
        tp.subtask_idx = 1;
        tp.peers.push_back(
            PeerAddress{.role = "src", .subtask_idx = 0, .host = "h", .data_port = 5});
        with_peers.tasks.push_back(std::move(tp));
        auto out2 = round_trip(MessageKind::PeerUpdate, with_peers, decode_peer_update);
        ASSERT_EQ(out2.tasks.size(), 1U);
        EXPECT_EQ(out2.tasks[0].peers.at(0).data_port, 5);
        EXPECT_EQ(out2.coordinator_epoch, 9U);
    }
}

TEST(WireProtocolFencing, AFrameFromAPreFencingPeerDecodesAsUnfenced) {
    // Truncating the tail is what a node running the previous build
    // actually puts on the wire. Each of these must decode cleanly, keep
    // every field that came before the epoch, and report epoch 0.
    {
        RegisterAckMsg in{.ok = true, .message = "welcome", .coordinator_epoch = 9};
        auto out = round_trip_without_epoch_field(MessageKind::RegisterAck,
                                                  in,
                                                  decode_register_ack,
                                                  // RegisterAck's tail has grown: the fencing epoch
                                                  // (8 bytes) plus the protocol version declaration
                                                  // (two u32s) and retryable (one byte). A peer
                                                  // that predates all three sends none, so 17
                                                  // bytes come off, not 8. The
                                                  // 8-byte case - a peer that has fencing but not
                                                  // versioning - is covered in
                                                  // test_protocol_versioning.cpp.
                                                  /*tail_bytes=*/17);
        EXPECT_TRUE(out.ok);
        EXPECT_EQ(out.message, "welcome");
        EXPECT_EQ(out.coordinator_epoch, 0U);
        EXPECT_EQ(out.protocol_version, 0U);
    }
    {
        DeployMsg in;
        in.job_id = 11;
        in.tasks.push_back(DeploymentTask{.role = "r", .subtask_idx = 3});
        in.coordinator_epoch = 9;
        auto out = round_trip_without_epoch_field(MessageKind::Deploy, in, decode_deploy);
        EXPECT_EQ(out.job_id, 11U);
        ASSERT_EQ(out.tasks.size(), 1U);
        EXPECT_EQ(out.tasks[0].subtask_idx, 3U);
        EXPECT_EQ(out.coordinator_epoch, 0U);
    }
    {
        // CommitCheckpoint's tail has grown: the fencing epoch (8 bytes) plus
        // the commit-confirmed retention floor (8 bytes), then the pinned
        // savepoint list (item 74: a 4-byte count, empty here). A peer that
        // predates ALL of them sends none, so 20 bytes come off, not 8.
        CommitCheckpointMsg in{
            .job_id = 1, .checkpoint_id = 22, .coordinator_epoch = 9, .retain_floor = 5};
        auto out = round_trip_without_epoch_field(
            MessageKind::CommitCheckpoint, in, decode_commit_checkpoint, /*tail_bytes=*/20);
        EXPECT_EQ(out.checkpoint_id, 22U);
        EXPECT_EQ(out.coordinator_epoch, 0U);
        EXPECT_EQ(out.retain_floor, 0U);
    }
    {
        // The intermediate peer: has fencing, predates the retention floor
        // (and therefore the pinned list too). Chops the floor and the count;
        // the epoch must survive and the floor must read 0 = no retention
        // constraint, the pre-protocol behaviour.
        CommitCheckpointMsg in{
            .job_id = 1, .checkpoint_id = 23, .coordinator_epoch = 9, .retain_floor = 5};
        auto out = round_trip_without_epoch_field(
            MessageKind::CommitCheckpoint, in, decode_commit_checkpoint, /*tail_bytes=*/12);
        EXPECT_EQ(out.checkpoint_id, 23U);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.retain_floor, 0U);
        EXPECT_TRUE(out.pinned_checkpoint_ids.empty());
    }
    {
        // The next rung: has the retention floor, predates pinned savepoints
        // (item 74). Chops only the 4-byte count; floor survives, nothing
        // pinned - which is what that peer's workers always did.
        CommitCheckpointMsg in{
            .job_id = 1, .checkpoint_id = 24, .coordinator_epoch = 9, .retain_floor = 5};
        auto out = round_trip_without_epoch_field(
            MessageKind::CommitCheckpoint, in, decode_commit_checkpoint, /*tail_bytes=*/4);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.retain_floor, 5U);
        EXPECT_TRUE(out.pinned_checkpoint_ids.empty());
    }
    {
        // TriggerCheckpoint's tail has grown: the fencing epoch (8 bytes),
        // the F84 generation (8 bytes) and the adaptive barrier mode
        // (1 byte). A peer that predates ALL THREE sends none of them,
        // so 17 bytes come off, not 8.
        TriggerCheckpointMsg in{.job_id = 1,
                                .checkpoint_id = 33,
                                .coordinator_epoch = 9,
                                .generation = 7,
                                .barrier_mode_plus1 = 2};
        auto out = round_trip_without_epoch_field(
            MessageKind::TriggerCheckpoint, in, decode_trigger_checkpoint, /*tail_bytes=*/17);
        EXPECT_EQ(out.checkpoint_id, 33U);
        EXPECT_EQ(out.coordinator_epoch, 0U);
        EXPECT_EQ(out.generation, 0U);
        EXPECT_EQ(out.barrier_mode_plus1, 0U);
    }
    {
        // The intermediate peer: has fencing, predates the F84 generation
        // (and therefore the adaptive mode byte). Chops the generation and
        // the mode; the epoch must survive, the generation must read 0 =
        // accept-all at the worker's fence, and the mode 0 = not stamped.
        TriggerCheckpointMsg in{.job_id = 1,
                                .checkpoint_id = 34,
                                .coordinator_epoch = 9,
                                .generation = 7,
                                .barrier_mode_plus1 = 2};
        auto out = round_trip_without_epoch_field(
            MessageKind::TriggerCheckpoint, in, decode_trigger_checkpoint, /*tail_bytes=*/9);
        EXPECT_EQ(out.checkpoint_id, 34U);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.generation, 0U);
        EXPECT_EQ(out.barrier_mode_plus1, 0U);
    }
    {
        // The peer that has fencing and the generation but predates the
        // adaptive mode byte: only that byte comes off, and it reads 0 =
        // not stamped (deploy-static behaviour).
        TriggerCheckpointMsg in{.job_id = 1,
                                .checkpoint_id = 35,
                                .coordinator_epoch = 9,
                                .generation = 7,
                                .barrier_mode_plus1 = 2};
        auto out = round_trip_without_epoch_field(
            MessageKind::TriggerCheckpoint, in, decode_trigger_checkpoint, /*tail_bytes=*/1);
        EXPECT_EQ(out.checkpoint_id, 35U);
        EXPECT_EQ(out.coordinator_epoch, 9U);
        EXPECT_EQ(out.generation, 7U);
        EXPECT_EQ(out.barrier_mode_plus1, 0U);
    }
    {
        BeginRescaleMsg in;
        in.job_id = 1;
        in.op_id = "agg";
        in.target_parallelism = 8;
        in.cutover_checkpoint = 44;
        in.coordinator_epoch = 9;
        auto out =
            round_trip_without_epoch_field(MessageKind::BeginRescale, in, decode_begin_rescale);
        EXPECT_EQ(out.op_id, "agg");
        EXPECT_EQ(out.cutover_checkpoint, 44U);
        EXPECT_EQ(out.coordinator_epoch, 0U);
    }
    {
        PeerUpdateMsg in;
        in.job_id = 1;
        in.coordinator_epoch = 9;
        PeerUpdateMsg::TaskPeers tp;
        tp.role = "sink";
        tp.subtask_idx = 1;
        tp.peers.push_back(
            PeerAddress{.role = "src", .subtask_idx = 0, .host = "h", .data_port = 6});
        in.tasks.push_back(std::move(tp));
        auto out = round_trip_without_epoch_field(MessageKind::PeerUpdate, in, decode_peer_update);
        ASSERT_EQ(out.tasks.size(), 1U);
        EXPECT_EQ(out.tasks[0].peers.at(0).data_port, 6);
        EXPECT_EQ(out.coordinator_epoch, 0U);
    }
}

TEST(WireProtocolFencing, ADefaultConstructedControlFrameIsUnfenced) {
    // Zero is the value that means "no fencing", so every message must
    // default to it. A message that defaulted to 1 would fence off every
    // worker registered under an unfenced (non-HA) coordinator.
    EXPECT_EQ(RegisterAckMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(DeployMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(CancelJobMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(TriggerCheckpointMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(CommitCheckpointMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(AbortCheckpointMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(FinalCheckpointAssignedMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(BeginRescaleMsg{}.coordinator_epoch, 0U);
    EXPECT_EQ(PeerUpdateMsg{}.coordinator_epoch, 0U);
}

// Content-addressed plugin shipping (item 30): the ack's missing-hash list
// rides an eof-guarded tail, so a new reader accepts an old writer's frame
// (empty list) and the new field round-trips when present.
TEST(WireProtocol, SubmitJobAckMissingPluginHashesRoundTripAndOldFramesDecode) {
    SubmitJobAckMsg in{
        .job_id = 0, .ok = false, .message = "plugin bytes required for 2 referenced module(s)"};
    in.missing_plugin_hashes = {"00deadbeef00cafe", "1122334455667788"};
    auto out = round_trip(MessageKind::SubmitJobAck, in, decode_submit_job_ack);
    EXPECT_EQ(out.ok, false);
    EXPECT_EQ(out.message, in.message);
    ASSERT_EQ(out.missing_plugin_hashes.size(), 2u);
    EXPECT_EQ(out.missing_plugin_hashes[0], "00deadbeef00cafe");
    EXPECT_EQ(out.missing_plugin_hashes[1], "1122334455667788");

    // A frame from a pre-item-30 coordinator ends after `message`; the
    // decoder must stop at eof with an empty list rather than throw.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::SubmitJobAck));
    b.put_u64_be(7);
    b.put_u8(1);
    b.put_string("ok");
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::SubmitJobAck);
    const auto old = decode_submit_job_ack(r);
    EXPECT_TRUE(old.ok);
    EXPECT_EQ(old.job_id, 7u);
    EXPECT_TRUE(old.missing_plugin_hashes.empty());
}

// A PluginBinary with empty bytes and a non-empty hash is a REFERENCE and
// must survive the wire as one - bytes stay empty, hash intact.
TEST(WireProtocol, PluginBinaryReferenceRoundTripsInsideDeploy) {
    DeployMsg in;
    in.job_id = 42;
    in.plugins.push_back(
        PluginBinary{.name = "hello", .content_hash = "aabbccddeeff0011", .bytes = {}});
    ASSERT_TRUE(in.plugins[0].is_reference());
    auto out = round_trip(MessageKind::Deploy, in, decode_deploy);
    ASSERT_EQ(out.plugins.size(), 1u);
    EXPECT_TRUE(out.plugins[0].is_reference());
    EXPECT_EQ(out.plugins[0].content_hash, "aabbccddeeff0011");
    EXPECT_EQ(out.plugins[0].name, "hello");
}

// The transport_only tail field (item 83). It decides whether the
// coordinator may act on a subtask error directly or must wait for the
// cause behind it, so a frame that loses it in transit would turn a symptom
// back into a restart cause - the exact cascade the field exists to stop.
TEST(WireProtocol, SubtaskFinishedCarriesTransportOnly) {
    SubtaskFinishedMsg in{.job_id = 7,
                          .worker_id = "worker-c",
                          .role = "generic",
                          .subtask_idx = 2,
                          .had_error = true,
                          .error_message = "network_bridge_sink: failed to send a watermark",
                          .fatal = false,
                          .transport_only = true};
    auto out = round_trip(MessageKind::SubtaskFinished, in, decode_subtask_finished);
    EXPECT_TRUE(out.had_error);
    EXPECT_FALSE(out.fatal);
    EXPECT_TRUE(out.transport_only)
        << "a transport-only error that arrives without its marker is acted on as a cause";
    EXPECT_EQ(out.error_message, in.error_message);
}

// An ordinary operator failure must NOT come back marked transport-only:
// held pending a cause that is never coming, it would wait out the grace
// before the job reacted at all.
TEST(WireProtocol, SubtaskFinishedDefaultsTransportOnlyFalse) {
    SubtaskFinishedMsg in{.job_id = 8,
                          .worker_id = "worker-d",
                          .role = "generic",
                          .subtask_idx = 1,
                          .had_error = true,
                          .error_message = "json_value_expr: unknown op 'f'"};
    auto out = round_trip(MessageKind::SubtaskFinished, in, decode_subtask_finished);
    EXPECT_TRUE(out.had_error);
    EXPECT_FALSE(out.transport_only);
}

// Compatibility: a frame from a worker that predates the field stops before
// it, and the absent tail must read as "act on this now" - the behaviour
// that worker's coordinator would have given it.
TEST(WireProtocol, SubtaskFinishedFromAnOlderWorkerActsOnTheErrorImmediately) {
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::SubtaskFinished));
    b.put_u64_be(11);
    b.put_string("worker-old");
    b.put_string("generic");
    b.put_u32_be(4);
    b.put_u8(1);
    b.put_string("some failure");
    // Stops here: no fatal byte, no transport_only byte.
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::SubtaskFinished);
    auto out = decode_subtask_finished(r);
    EXPECT_TRUE(out.had_error);
    EXPECT_FALSE(out.fatal);
    EXPECT_FALSE(out.transport_only);
}

// Pinned checkpoints on CommitCheckpoint (item 74). The worker's retention
// sweep unlinks whatever this list does not protect, so a frame that lost
// the list in transit would turn every savepoint into a one-interval handle
// again - which is exactly the defect.
TEST(WireProtocol, CommitCheckpointCarriesPinnedCheckpoints) {
    CommitCheckpointMsg in{.job_id = 5, .checkpoint_id = 90};
    in.coordinator_epoch = 3;
    in.retain_floor = 88;
    in.pinned_checkpoint_ids = {12, 48, 77};
    auto out = round_trip(MessageKind::CommitCheckpoint, in, decode_commit_checkpoint);
    EXPECT_EQ(out.checkpoint_id, 90u);
    EXPECT_EQ(out.retain_floor, 88u);
    EXPECT_EQ(out.pinned_checkpoint_ids, (std::vector<std::uint64_t>{12, 48, 77}));
}

TEST(WireProtocol, CommitCheckpointFromAnOlderCoordinatorPinsNothing) {
    // A pre-item-74 coordinator stops after retain_floor. Nothing pinned is
    // the behaviour that coordinator's workers had, so it is what they keep.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::CommitCheckpoint));
    b.put_u64_be(5);
    b.put_u64_be(90);
    b.put_u64_be(3);   // epoch
    b.put_u64_be(88);  // retain_floor
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::CommitCheckpoint);
    auto out = decode_commit_checkpoint(r);
    EXPECT_EQ(out.retain_floor, 88u);
    EXPECT_TRUE(out.pinned_checkpoint_ids.empty());
}

// ----- SubmitJob / SubmitJobAck: the plugin ABI preflight tails -----

TEST(WireProtocol, SubmitJobRoundTripsPluginAbiAdverts) {
    SubmitJobMsg in;
    in.graph_json = R"({"ops":[]})";
    in.plugins.push_back(PluginBinary{.name = "job", .content_hash = "abcd1234", .bytes = {}});
    in.checkpoint.checkpoint_dir = "/tmp/ckpt";
    PluginAbiAdvert ad;
    ad.content_hash = "abcd1234";
    ad.abi_fingerprint = "fp-1";
    ad.abi_hash = "commit-1";
    ad.abi_version = 1;
    ad.target_triple = "darwin-arm64";
    ad.toolchain = "libc++";
    in.plugin_abi_adverts.push_back(ad);

    auto out = round_trip(MessageKind::SubmitJob, in, decode_submit_job);
    EXPECT_EQ(out.graph_json, in.graph_json);
    ASSERT_EQ(out.plugin_abi_adverts.size(), 1u);
    EXPECT_EQ(out.plugin_abi_adverts[0].content_hash, "abcd1234");
    EXPECT_EQ(out.plugin_abi_adverts[0].abi_fingerprint, "fp-1");
    EXPECT_EQ(out.plugin_abi_adverts[0].abi_hash, "commit-1");
    EXPECT_EQ(out.plugin_abi_adverts[0].abi_version, 1u);
    EXPECT_EQ(out.plugin_abi_adverts[0].target_triple, "darwin-arm64");
    EXPECT_EQ(out.plugin_abi_adverts[0].toolchain, "libc++");
    // The pre-existing fields still land where they always did.
    EXPECT_EQ(out.checkpoint.checkpoint_dir, "/tmp/ckpt");
}

TEST(WireProtocol, SubmitJobWithoutAdvertTailDecodesToNoAdverts) {
    // An old client's frame ends at the capture-config tail; the decoder
    // must leave the adverts empty and the preflight is simply skipped.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::SubmitJob));
    b.put_string(R"({"ops":[]})");
    b.put_u32_be(0);   // no plugins
    b.put_string("");  // checkpoint_dir
    b.put_u64_be(0);   // interval_ms
    b.put_string("");  // restore_from_dir
    b.put_u64_be(0);   // restore_from_checkpoint_id
    b.put_u32_be(0);   // max_restarts_on_worker_loss
    b.put_u8(0);       // alignment
    b.put_string("");  // state_backend_uri
    b.put_string("");  // capture_dir
    b.put_u64_be(0);   // capture_records
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::SubmitJob);
    auto out = decode_submit_job(r);
    EXPECT_TRUE(out.plugin_abi_adverts.empty());
}

TEST(WireProtocol, SubmitJobAckRoundTripsClusterAbiIdentity) {
    SubmitJobAckMsg in;
    in.job_id = 0;
    in.ok = false;
    in.message = "submit preflight refused plugin";
    in.cluster_abi_fingerprint = "fp-cluster";
    in.cluster_target_triple = "linux-x86_64";
    in.cluster_toolchain = "libstdc++-cxx11abi1";
    in.cluster_abi_manifest = "include/clink/runtime/dag.hpp=aaa\n";
    auto out = round_trip(MessageKind::SubmitJobAck, in, decode_submit_job_ack);
    EXPECT_EQ(out.message, in.message);
    EXPECT_EQ(out.cluster_abi_fingerprint, "fp-cluster");
    EXPECT_EQ(out.cluster_target_triple, "linux-x86_64");
    EXPECT_EQ(out.cluster_toolchain, "libstdc++-cxx11abi1");
    EXPECT_EQ(out.cluster_abi_manifest, "include/clink/runtime/dag.hpp=aaa\n");
}

TEST(WireProtocol, SubmitJobAckWithoutIdentityTailDecodesEmpty) {
    // A pre-preflight coordinator's ack ends at missing_plugin_hashes; the
    // identity fields must stay empty so the client shows the plain message.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::SubmitJobAck));
    b.put_u64_be(7);       // job_id
    b.put_u8(1);           // ok
    b.put_string("fine");  // message
    b.put_u32_be(0);       // missing_plugin_hashes: none
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::SubmitJobAck);
    auto out = decode_submit_job_ack(r);
    EXPECT_TRUE(out.ok);
    EXPECT_TRUE(out.cluster_abi_fingerprint.empty());
    EXPECT_TRUE(out.cluster_target_triple.empty());
    EXPECT_TRUE(out.cluster_toolchain.empty());
    EXPECT_TRUE(out.cluster_abi_manifest.empty());
}
