// Write a seed corpus, derived from the real encoders.
//
// A fuzzer that starts from nothing spends its first minutes rediscovering
// the frame format. Seeded with valid encodings it starts at the edge of
// the reachable code and mutates from there, which is where the bugs are.
//
// The seeds are GENERATED rather than committed as hex blobs, for the same
// reason the guarantee analyser reads the capability registry instead of a
// literal list: a hand-written seed goes stale the first time a message
// gains a field, and a stale seed silently narrows what the fuzzer explores
// without anyone noticing. Regenerating is one build target.
//
// Reproducers are different - those ARE committed bytes, because their
// whole value is being the exact input that broke something.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/runtime/network/wire.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/schema_version.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;

std::filesystem::path g_root;

void write_seed(const std::string& target,
                const std::string& name,
                const std::vector<std::uint8_t>& bytes) {
    const auto dir = g_root / target;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const auto path = dir / ("seed-" + name);
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
}

void write_text_seed(const std::string& target, const std::string& name, std::string_view text) {
    write_seed(target, name, {text.begin(), text.end()});
}

// A cluster-frame seed is [kind byte][body], matching what
// fuzz_cluster_frame expects - so the kind is steerable by mutation rather
// than fixed per seed.
template <typename Msg>
void write_frame_seed(const std::string& name, MessageKind kind, const Msg& m) {
    const auto framed = encode_frame(kind, m);
    std::vector<std::uint8_t> out;
    out.push_back(static_cast<std::uint8_t>(kind));
    // Skip the 4-byte length prefix: the target is handed a body, not a
    // framed message, because read_frame has already consumed the length
    // by the time a decoder runs. Seeding with the prefix still attached
    // would spend the fuzzer's budget on bytes no decoder ever sees.
    for (std::size_t i = 4 + 1; i < framed.size(); ++i) {
        out.push_back(static_cast<std::uint8_t>(framed[i]));
    }
    write_seed("cluster_frame", name, out);
}

// Data-plane seeds (follow-up 10): an operator-to-operator frame body.
//
// Byte 0 is the element kind, matching what fuzz_data_frame expects, so the
// kind is steerable by mutation rather than fixed per seed.
//
// The ArrowBatch seed carries a REAL IPC stream produced by the same encoder the
// wire uses. Seeding only with garbage would leave the fuzzer to discover Arrow's
// stream header by chance, and it would spend its whole budget being rejected at
// the first bytes - the interesting inputs are the ones that get far enough into
// the decoder to matter.
void seed_data_frames() {
    const auto emit =
        [](const std::string& name, network::Kind kind, const std::vector<std::uint8_t>& payload) {
            std::vector<std::uint8_t> out;
            out.push_back(static_cast<std::uint8_t>(kind));
            out.insert(out.end(), payload.begin(), payload.end());
            write_seed("data_frame", name, out);
        };

    emit("watermark", network::Kind::Watermark, {0, 0, 0, 0, 0, 0, 0, 42});
    emit("idle_watermark", network::Kind::WatermarkIdle, {0, 0, 0, 0, 0, 0, 0, 7});
    emit("barrier", network::Kind::Barrier, {0, 0, 0, 0, 0, 0, 0, 3});
    emit("credit_update", network::Kind::CreditUpdate, {0, 0, 8, 0});
    // Short bodies: the receive loop must not read past the end when a peer
    // truncates a frame mid-field.
    emit("watermark_truncated", network::Kind::Watermark, {0, 0, 1});
    emit("barrier_empty", network::Kind::Barrier, {});
    emit("arrow_batch_empty", network::Kind::ArrowBatch, {});

#ifdef CLINK_HAS_ARROW
    // A valid single-column batch through the real encoder.
    arrow::Int64Builder b;
    (void)b.Append(1);
    (void)b.Append(2);
    (void)b.Append(3);
    std::shared_ptr<arrow::Array> arr;
    if (b.Finish(&arr).ok()) {
        auto schema = arrow::schema({arrow::field("v", arrow::int64())});
        auto batch = arrow::RecordBatch::Make(schema, arr->length(), {arr});
        const auto ipc = clink::arrow_batch_to_ipc(*batch);
        std::vector<std::uint8_t> payload;
        payload.reserve(ipc.size());
        for (const auto byte : ipc) {
            payload.push_back(static_cast<std::uint8_t>(byte));
        }
        emit("arrow_batch_valid", network::Kind::ArrowBatch, payload);
        // Truncated at the halfway point: an openable header with a body that
        // stops early is the shape a dropped connection produces.
        payload.resize(payload.size() / 2);
        emit("arrow_batch_truncated", network::Kind::ArrowBatch, payload);
    }
#endif
}

void seed_cluster_frames() {
    {
        RegisterMsg m{.worker_id = "worker-0", .data_host = "10.0.0.1", .slot_count = 4};
        m.http_port = 8081;
        write_frame_seed("register", MessageKind::Register, m);
    }
    {
        RegisterAckMsg m{.ok = true, .message = "welcome"};
        m.coordinator_epoch = 3;
        write_frame_seed("register_ack", MessageKind::RegisterAck, m);
    }
    {
        // The richest body in the protocol: nested task list, nested peer
        // list, and a plugin blob. Every length prefix a fuzzer would want
        // to corrupt is in here.
        DeployMsg m;
        m.job_id = 7;
        DeploymentTask t;
        t.role = "__clink_subtask";
        t.subtask_idx = 1;
        t.data_port = 41000;
        t.peers.push_back(
            PeerAddress{.role = "src", .subtask_idx = 0, .host = "10.0.0.2", .data_port = 41001});
        m.tasks.push_back(std::move(t));
        m.plugins.push_back(PluginBinary{
            .name = "p", .content_hash = "abc", .bytes = {std::byte{1}, std::byte{2}}});
        m.checkpoint_dir = "/var/clink/ckpt";
        m.expected_state_versions_packed = "1|i64_sum|2\n";
        m.coordinator_epoch = 3;
        write_frame_seed("deploy", MessageKind::Deploy, m);
    }
    {
        PeerUpdateMsg m;
        m.job_id = 7;
        PeerUpdateMsg::TaskPeers tp;
        tp.role = "sink";
        tp.subtask_idx = 0;
        tp.peers.push_back(
            PeerAddress{.role = "src", .subtask_idx = 0, .host = "h", .data_port = 5});
        m.tasks.push_back(std::move(tp));
        m.coordinator_epoch = 3;
        write_frame_seed("peer_update", MessageKind::PeerUpdate, m);
    }
    {
        SubmitJobMsg m;
        m.graph_json = R"({"ops":[{"id":"src","type":"int64_range_source"}]})";
        m.checkpoint.checkpoint_dir = "/var/clink/ckpt";
        m.checkpoint.interval_ms = 500;
        write_frame_seed("submit_job", MessageKind::SubmitJob, m);
    }
    {
        CommitCheckpointMsg m{.job_id = 7, .checkpoint_id = 12, .coordinator_epoch = 3};
        write_frame_seed("commit_checkpoint", MessageKind::CommitCheckpoint, m);
    }
    {
        SubtaskCheckpointedMsg m;
        m.job_id = 7;
        m.checkpoint_id = 12;
        m.role = "__clink_subtask";
        m.subtask_idx = 0;
        m.ok = false;
        m.error = "snapshot failed";
        write_frame_seed("subtask_checkpointed", MessageKind::SubtaskCheckpointed, m);
    }
    {
        SubtaskListeningMsg m;
        m.job_id = 7;
        m.worker_id = "worker-0";
        m.role = "__clink_subtask";
        m.subtask_idx = 0;
        m.host = "10.0.0.1";
        m.edge_ports.push_back(SubtaskListeningMsg::EdgePort{
            .upstream_role = "src", .upstream_subtask_idx = 0, .port = 41002});
        write_frame_seed("subtask_listening", MessageKind::SubtaskListening, m);
    }
    {
        ListJobsAckMsg m;
        JobInfo j;
        j.job_id = 7;
        j.total_subtasks = 2;
        j.completed_subtasks = 1;
        m.jobs.push_back(j);
        write_frame_seed("list_jobs_ack", MessageKind::ListJobsAck, m);
    }
    {
        BeginRescaleMsg m;
        m.job_id = 7;
        m.op_id = "agg";
        m.target_parallelism = 4;
        m.cutover_checkpoint = 12;
        m.coordinator_epoch = 3;
        write_frame_seed("begin_rescale", MessageKind::BeginRescale, m);
    }
}

void seed_checkpoint_meta() {
    // Round-tripped through the real serializer, so the seed cannot drift
    // from the format.
    state::CheckpointMeta meta;
    meta.checkpoint_id = 12;
    meta.payload_bytes = 4096;
    meta.payload_crc32c = 0xDEADBEEF;
    write_text_seed("checkpoint_meta", "valid", meta.serialise());
    // A truncated sidecar is the realistic corruption: a partial write.
    const auto text = meta.serialise();
    write_text_seed("checkpoint_meta", "truncated", text.substr(0, text.size() / 2));
}

void seed_state_version_map() {
    StateVersionMap m;
    m.set(OperatorId{1}, "i64_sum", 3);
    m.set(OperatorId{2}, "i64_count", 1);
    write_text_seed("state_version_map", "valid", m.pack());
    write_text_seed("state_version_map", "slotted", "1|i64_sum|3|left_buf\n2|i64_cnt|2\n");
    write_text_seed("state_version_map", "empty", "");
}

void seed_fault_spec() {
    // The documented grammar, plus the shapes the parser is meant to
    // refuse - a fuzzer mutating a REJECTED input explores the error paths,
    // which is where a parser is most likely to be wrong.
    write_text_seed("fault_spec", "single", "checkpoint.before_write=exit:70@1");
    write_text_seed(
        "fault_spec", "multi", "a.b=exit:3@2, c.d=block:50, e.f=throw, g.h=truncate:4@7");
    write_text_seed("fault_spec", "observe", "state.before_restore=observe");
    write_text_seed("fault_spec", "malformed_no_equals", "no-equals-sign");
    write_text_seed("fault_spec", "malformed_action", "a.b=nosuchaction");
    write_text_seed("fault_spec", "malformed_ordinal", "a.b=throw@notanumber");
}

void seed_sql() {
    // Valid statements across the surface, plus constructs clink refuses.
    // The rejected ones matter: they steer the fuzzer into the diagnostic
    // paths added by W13, which are newer and less travelled than the
    // parser itself.
    const char* const statements[] = {
        "SELECT 1;",
        "CREATE TABLE t (k BIGINT, v VARCHAR) WITH (connector='file', format='json', "
        "path='/tmp/x');",
        "CREATE TABLE t (k BIGINT PRIMARY KEY) WITH (connector='file', format='json', "
        "path='/tmp/x', mode='upsert');",
        "INSERT INTO o SELECT k, COUNT(*) FROM t GROUP BY k;",
        "INSERT INTO o SELECT k FROM t WHERE v LIKE 'a%' AND k BETWEEN 1 AND 9;",
        "INSERT INTO o SELECT window_start, SUM(v) FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), "
        "INTERVAL '10' SECOND)) GROUP BY window_start;",
        "INSERT INTO o SELECT a.k FROM t a JOIN u b ON a.k = b.k;",
        "INSERT INTO o SELECT CAST(k AS DECIMAL(20, 4)) FROM t;",
        "INSERT INTO o SELECT ARRAY[1, 2, 3][1], MAP['a', 1]['a'] FROM t;",
        "CREATE FUNCTION f(x BIGINT) RETURNS BIGINT LANGUAGE SQL AS 'x + 1';",
        "INSERT INTO o SELECT NOW() FROM t;",                           // refused: nondeterministic
        "INSERT INTO o SELECT no_such_function(k) FROM t;",             // refused: unknown function
        "CREATE TABLE t (k BIGINT NOT NULL) WITH (connector='file');",  // refused: constraint
        "SELECT * FROM t FOR UPDATE;",
        "WITH c AS (SELECT k FROM t) SELECT * FROM c;",
    };
    int n = 0;
    for (const auto* sql : statements) {
        write_text_seed("sql_parse", "stmt" + std::to_string(n++), sql);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: clink_fuzz_seeds <corpus-root>\n";
        return 2;
    }
    g_root = argv[1];
    std::error_code ec;
    std::filesystem::create_directories(g_root, ec);
    seed_cluster_frames();
    seed_data_frames();
    seed_checkpoint_meta();
    seed_state_version_map();
    seed_fault_spec();
    seed_sql();
    std::cout << "seed corpus written to " << g_root.string() << "\n";
    return 0;
}
