// Wire-format invariants for the JM/TM cluster protocol.
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
    RegisterMsg in{.tm_id = "tm-a", .data_host = "10.0.0.7", .slot_count = 4};
    auto out = round_trip(MessageKind::Register, in, decode_register);
    EXPECT_EQ(out.tm_id, in.tm_id);
    EXPECT_EQ(out.data_host, in.data_host);
    EXPECT_EQ(out.slot_count, in.slot_count);
}

TEST(WireProtocol, RegisterAckRoundTrips) {
    RegisterAckMsg ok_msg{.ok = true, .message = "welcome"};
    auto ok_out = round_trip(MessageKind::RegisterAck, ok_msg, decode_register_ack);
    EXPECT_EQ(ok_out.ok, true);
    EXPECT_EQ(ok_out.message, "welcome");

    RegisterAckMsg bad_msg{.ok = false, .message = "duplicate tm_id"};
    auto bad_out = round_trip(MessageKind::RegisterAck, bad_msg, decode_register_ack);
    EXPECT_EQ(bad_out.ok, false);
    EXPECT_EQ(bad_out.message, "duplicate tm_id");
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
                             .tm_id = "tm-a",
                             .role = "producer",
                             .subtask_idx = 0,
                             .had_error = false,
                             .error_message = ""};
    auto out_ok = round_trip(MessageKind::SubtaskFinished, in_ok, decode_subtask_finished);
    EXPECT_EQ(out_ok.job_id, 1u);
    EXPECT_EQ(out_ok.tm_id, "tm-a");
    EXPECT_FALSE(out_ok.had_error);
    EXPECT_EQ(out_ok.error_message, "");

    SubtaskFinishedMsg in_err{.job_id = 99,
                              .tm_id = "tm-b",
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
    HeartbeatMsg in{.tm_id = "tm-z"};
    auto out = round_trip(MessageKind::Heartbeat, in, decode_heartbeat);
    EXPECT_EQ(out.tm_id, "tm-z");
}

// ----- Backwards-compat: Register without slot_count must still parse -----

TEST(WireProtocol, RegisterWithoutSlotCountDefaultsToOne) {
    // Old peers built without slot_count - encode the body manually so it
    // ends after data_host. The decoder must accept and default to 1.
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(MessageKind::Register));
    b.put_string("legacy-tm");
    b.put_string("10.0.0.99");
    MessageReader r(body_of(b.finalize()));
    EXPECT_EQ(static_cast<MessageKind>(r.read_u8()), MessageKind::Register);
    auto out = decode_register(r);
    EXPECT_EQ(out.tm_id, "legacy-tm");
    EXPECT_EQ(out.data_host, "10.0.0.99");
    EXPECT_EQ(out.slot_count, 1u);
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
    EXPECT_EQ(static_cast<int>(MessageKind::RegisterAck), 100);
    EXPECT_EQ(static_cast<int>(MessageKind::Deploy), 101);
    EXPECT_EQ(static_cast<int>(MessageKind::StartJob), 102);
    EXPECT_EQ(static_cast<int>(MessageKind::CancelJob), 103);
    EXPECT_EQ(static_cast<int>(MessageKind::PeerUpdate), 104);
    EXPECT_EQ(static_cast<int>(MessageKind::SubmitJobAck), 105);
    EXPECT_EQ(static_cast<int>(MessageKind::JobCompleted), 106);
    EXPECT_EQ(static_cast<int>(MessageKind::RescaleJobAck), 111);
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

TEST(WireProtocol, SubmitJobCarriesAlignmentMode) {
    SubmitJobMsg in;
    in.graph_json = "{}";
    in.checkpoint.checkpoint_dir = "/var/clink/state";
    in.checkpoint.alignment = CheckpointAlignment::Unaligned;
    auto out = round_trip(MessageKind::SubmitJob, in, decode_submit_job);
    EXPECT_EQ(out.checkpoint.alignment, CheckpointAlignment::Unaligned);

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

    JobCompletedMsg bad{
        .job_id = 12, .ok = false, .errors = {"tm-a/producer[0]: send failed", "tm-b: lost"}};
    auto bad_out = round_trip(MessageKind::JobCompleted, bad, decode_job_completed);
    EXPECT_EQ(bad_out.job_id, 12u);
    EXPECT_FALSE(bad_out.ok);
    ASSERT_EQ(bad_out.errors.size(), 2u);
    EXPECT_EQ(bad_out.errors[0], "tm-a/producer[0]: send failed");
}

TEST(WireProtocol, SubtaskListeningRoundTrips) {
    SubtaskListeningMsg in;
    in.job_id = 5;
    in.tm_id = "tm-b";
    in.role = "consumer";
    in.subtask_idx = 0;
    in.host = "10.0.0.7";
    in.edge_ports.push_back(
        {.upstream_role = "producer", .upstream_subtask_idx = 0, .port = 39444});
    in.edge_ports.push_back(
        {.upstream_role = "producer", .upstream_subtask_idx = 1, .port = 39445});
    auto out = round_trip(MessageKind::SubtaskListening, in, decode_subtask_listening);
    EXPECT_EQ(out.job_id, 5u);
    EXPECT_EQ(out.tm_id, "tm-b");
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

// Phase 30c: AbortCheckpoint wire frame round-trips. Same payload
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

// Phase 29d: BeginRescale message frame. JM -> TM signal that starts
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
    // Defensive: an empty op_id is invalid at the JM level but the
    // wire codec shouldn't choke on it. The JM dispatch is the
    // layer that validates / rejects.
    BeginRescaleMsg in{.job_id = 1, .op_id = "", .target_parallelism = 2, .cutover_checkpoint = 0};
    auto out = round_trip(MessageKind::BeginRescale, in, decode_begin_rescale);
    EXPECT_EQ(out.op_id, "");
    EXPECT_EQ(out.target_parallelism, 2u);
}

// Phase 29d-4: per-operator rescale request + ack.
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
