#pragma once

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// Encode helpers - append the body of a typed message into a builder. Each
// caller is responsible for putting the leading kind byte.

inline void encode_body(MessageBuilder& b, const RegisterMsg& m) {
    b.put_string(m.tm_id);
    b.put_string(m.data_host);
    b.put_u32_be(m.slot_count);
    // Trailing u16 - old JMs that don't read this far just see end-of-
    // frame, get http_port=0 on the decoded struct, treat the TM as
    // HTTP-disabled. Same wire-compat pattern as slot_count.
    b.put_u16_be(m.http_port);
}

inline void encode_body(MessageBuilder& b, const RegisterAckMsg& m) {
    b.put_u8(m.ok ? 1 : 0);
    b.put_string(m.message);
}

// Append a PluginBinary's wire encoding. The bytes blob can be large
// (multi-megabyte plugins); we use a u32 length prefix which caps us
// at 4 GB per blob - more than enough for v1.
inline void encode_plugin_binary(MessageBuilder& b, const PluginBinary& p) {
    b.put_string(p.name);
    b.put_string(p.content_hash);
    b.put_u32_be(static_cast<std::uint32_t>(p.bytes.size()));
    for (auto byte : p.bytes) {
        b.put_u8(static_cast<std::uint8_t>(byte));
    }
}

inline PluginBinary decode_plugin_binary(MessageReader& r) {
    PluginBinary p;
    p.name = r.read_string();
    p.content_hash = r.read_string();
    const std::uint32_t n = r.read_u32_be();
    p.bytes.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        p.bytes.push_back(static_cast<std::byte>(r.read_u8()));
    }
    return p;
}

inline void encode_body(MessageBuilder& b, const DeployMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u32_be(static_cast<std::uint32_t>(m.tasks.size()));
    for (const auto& t : m.tasks) {
        b.put_string(t.role);
        b.put_u32_be(t.subtask_idx);
        b.put_u16_be(t.data_port);
        b.put_u32_be(static_cast<std::uint32_t>(t.peers.size()));
        for (const auto& p : t.peers) {
            b.put_string(p.role);
            b.put_u32_be(p.subtask_idx);
            b.put_string(p.host);
            b.put_u16_be(p.data_port);
        }
        b.put_string(t.extra_config);
    }
    b.put_u32_be(static_cast<std::uint32_t>(m.plugins.size()));
    for (const auto& p : m.plugins) {
        encode_plugin_binary(b, p);
    }
    b.put_string(m.checkpoint_dir);
    b.put_string(m.restore_from_dir);
    b.put_u64_be(m.restore_from_checkpoint_id);
    // Per-task rescale directives - appended as a parallel array after
    // the legacy task / plugin / checkpoint fields so old TM decoders
    // (which stop reading at EOF here) silently see the defaults
    // (kRestoreFromSelf + {0, 0} = no override).
    for (const auto& t : m.tasks) {
        b.put_u32_be(t.restore_from_subtask_idx);
        b.put_u16_be(t.key_group_first);
        b.put_u16_be(t.key_group_last);
    }
    // Trailing parent-count array - wire-compat after the kg directives
    // so v0 peers that only read the prior block leave it at the
    // default 1 (single-parent scale-up).
    for (const auto& t : m.tasks) {
        b.put_u32_be(t.restore_from_parent_count);
    }
    // Trailing per-job alignment flag. Older TMs see EOF before this
    // byte and default to aligned, matching their historical behaviour.
    b.put_u8(m.unaligned_checkpoints ? 1 : 0);
    // Trailing packed expected state-version map. Older TMs see EOF
    // before this string and leave it empty (no schema migration).
    b.put_string(m.expected_state_versions_packed);
    // Trailing state-backend URI. Older TMs see EOF before this string and
    // leave it empty, so checkpoint_dir doubles as the backend URI.
    b.put_string(m.state_backend_uri);
}

inline void encode_body(MessageBuilder& b, const StartJobMsg& m) {
    b.put_u64_be(m.job_id);
}
inline void encode_body(MessageBuilder& b, const CancelJobMsg& m) {
    b.put_u64_be(m.job_id);
}
inline void encode_body(MessageBuilder& b, const CancelJobAckMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_string(m.message);
}

inline void encode_body(MessageBuilder& b, const RescaleJobMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u32_be(static_cast<std::uint32_t>(m.role_parallelism.size()));
    for (const auto& [role, p] : m.role_parallelism) {
        b.put_string(role);
        b.put_u32_be(p);
    }
}

inline void encode_body(MessageBuilder& b, const RescaleJobAckMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_string(m.message);
}

inline void encode_body(MessageBuilder& b, const RescaleOperatorMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_string(m.op_id);
    b.put_u32_be(m.new_parallelism);
}

inline void encode_body(MessageBuilder& b, const RescaleOperatorAckMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_u32_be(m.accepted_target);
    b.put_string(m.message);
}

inline void encode_body(MessageBuilder& b, const SavepointMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u64_be(static_cast<std::uint64_t>(m.timeout_ms));
}

inline void encode_body(MessageBuilder& b, const SavepointAckMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_u64_be(m.checkpoint_id);
    b.put_string(m.checkpoint_dir);
    b.put_string(m.message);
}

inline void encode_body(MessageBuilder& b, const SubtaskFinishedMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_string(m.tm_id);
    b.put_string(m.role);
    b.put_u32_be(m.subtask_idx);
    b.put_u8(m.had_error ? 1 : 0);
    b.put_string(m.error_message);
}

inline void encode_body(MessageBuilder& b, const HeartbeatMsg& m) {
    b.put_string(m.tm_id);
}

inline void encode_body(MessageBuilder& /*b*/, const HelloClientMsg&) {}

inline void encode_body(MessageBuilder& b, const SubmitJobMsg& m) {
    b.put_string(m.graph_json);
    b.put_u32_be(static_cast<std::uint32_t>(m.plugins.size()));
    for (const auto& p : m.plugins) {
        encode_plugin_binary(b, p);
    }
    b.put_string(m.checkpoint.checkpoint_dir);
    b.put_u64_be(static_cast<std::uint64_t>(m.checkpoint.interval_ms));
    b.put_string(m.checkpoint.restore_from_dir);
    b.put_u64_be(m.checkpoint.restore_from_checkpoint_id);
    b.put_u32_be(m.checkpoint.max_restarts_on_tm_loss);
    // Trailing wire-compat: alignment mode added later than the rest.
    // Older JMs ignore the trailing byte and see alignment=Aligned via
    // the default-init, which matches their historical behaviour.
    b.put_u8(static_cast<std::uint8_t>(m.checkpoint.alignment));
    // Trailing state-backend URI (decoupled from checkpoint_dir). Older
    // JMs see EOF before this string and leave it empty, so checkpoint_dir
    // doubles as the backend URI (legacy behaviour).
    b.put_string(m.checkpoint.state_backend_uri);
}

inline void encode_body(MessageBuilder& b, const SubmitJobAckMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_string(m.message);
}

inline void encode_body(MessageBuilder& b, const JobCompletedMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u8(m.ok ? 1 : 0);
    b.put_u32_be(static_cast<std::uint32_t>(m.errors.size()));
    for (const auto& e : m.errors) {
        b.put_string(e);
    }
}

inline void encode_body(MessageBuilder& /*b*/, const ListJobsMsg&) {}

inline void encode_body(MessageBuilder& b, const TriggerCheckpointMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u64_be(m.checkpoint_id);
}

inline void encode_body(MessageBuilder& b, const CommitCheckpointMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u64_be(m.checkpoint_id);
}

inline void encode_body(MessageBuilder& b, const AbortCheckpointMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u64_be(m.checkpoint_id);
}

inline void encode_body(MessageBuilder& b, const BeginRescaleMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_string(m.op_id);
    b.put_u32_be(m.target_parallelism);
    b.put_u64_be(m.cutover_checkpoint);
}

inline void encode_body(MessageBuilder& b, const SubtaskCheckpointedMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u64_be(m.checkpoint_id);
    b.put_string(m.role);
    b.put_u32_be(m.subtask_idx);
    b.put_u8(m.ok ? 1 : 0);
    b.put_string(m.error);
}

inline void encode_body(MessageBuilder& b, const ListJobsAckMsg& m) {
    b.put_u32_be(static_cast<std::uint32_t>(m.jobs.size()));
    for (const auto& j : m.jobs) {
        b.put_u64_be(j.job_id);
        b.put_u32_be(j.total_subtasks);
        b.put_u32_be(j.completed_subtasks);
        b.put_u8(j.completion_signalled ? 1 : 0);
    }
}

inline void encode_body(MessageBuilder& b, const SubtaskListeningMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_string(m.tm_id);
    b.put_string(m.role);
    b.put_u32_be(m.subtask_idx);
    b.put_string(m.host);
    b.put_u32_be(static_cast<std::uint32_t>(m.edge_ports.size()));
    for (const auto& ep : m.edge_ports) {
        b.put_string(ep.upstream_role);
        b.put_u32_be(ep.upstream_subtask_idx);
        b.put_u16_be(ep.port);
    }
}

inline void encode_body(MessageBuilder& b, const PeerUpdateMsg& m) {
    b.put_u64_be(m.job_id);
    b.put_u32_be(static_cast<std::uint32_t>(m.tasks.size()));
    for (const auto& t : m.tasks) {
        b.put_string(t.role);
        b.put_u32_be(t.subtask_idx);
        b.put_u32_be(static_cast<std::uint32_t>(t.peers.size()));
        for (const auto& p : t.peers) {
            b.put_string(p.role);
            b.put_u32_be(p.subtask_idx);
            b.put_string(p.host);
            b.put_u16_be(p.data_port);
        }
    }
}

// Wrap any typed message: produces the final framed byte buffer ready
// for socket send_all.
template <typename Msg>
inline std::vector<std::byte> encode_frame(MessageKind kind, const Msg& m) {
    MessageBuilder b;
    b.put_u8(static_cast<std::uint8_t>(kind));
    encode_body(b, m);
    return b.finalize();
}

// Decode helpers (kind byte already consumed by caller).

inline RegisterMsg decode_register(MessageReader& r) {
    RegisterMsg m;
    m.tm_id = r.read_string();
    m.data_host = r.read_string();
    m.slot_count = r.eof() ? std::uint32_t{1} : r.read_u32_be();
    m.http_port = r.eof() ? std::uint16_t{0} : r.read_u16_be();
    return m;
}

inline RegisterAckMsg decode_register_ack(MessageReader& r) {
    RegisterAckMsg m;
    m.ok = r.read_u8() != 0;
    m.message = r.read_string();
    return m;
}

inline DeployMsg decode_deploy(MessageReader& r) {
    DeployMsg m;
    m.job_id = r.read_u64_be();
    const std::uint32_t n = r.read_u32_be();
    m.tasks.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        DeploymentTask t;
        t.role = r.read_string();
        t.subtask_idx = r.read_u32_be();
        t.data_port = r.read_u16_be();
        const std::uint32_t pn = r.read_u32_be();
        t.peers.reserve(pn);
        for (std::uint32_t j = 0; j < pn; ++j) {
            PeerAddress p;
            p.role = r.read_string();
            p.subtask_idx = r.read_u32_be();
            p.host = r.read_string();
            p.data_port = r.read_u16_be();
            t.peers.push_back(std::move(p));
        }
        t.extra_config = r.read_string();
        m.tasks.push_back(std::move(t));
    }
    if (!r.eof()) {
        const std::uint32_t pcount = r.read_u32_be();
        m.plugins.reserve(pcount);
        for (std::uint32_t i = 0; i < pcount; ++i) {
            m.plugins.push_back(decode_plugin_binary(r));
        }
    }
    if (!r.eof()) {
        m.checkpoint_dir = r.read_string();
        m.restore_from_dir = r.read_string();
        m.restore_from_checkpoint_id = r.read_u64_be();
    }
    // Rescale directives: per-task triple, in the same task order as the
    // body above. Missing for legacy JM peers - leave defaults in place
    // (kRestoreFromSelf + {0, 0} means "restore from own subtask_idx,
    // no kg filter") so behaviour is unchanged.
    if (!r.eof()) {
        for (auto& t : m.tasks) {
            t.restore_from_subtask_idx = r.read_u32_be();
            t.key_group_first = r.read_u16_be();
            t.key_group_last = r.read_u16_be();
        }
    }
    // Parent-count array - present only for JMs that emit scale-down
    // rescale directives. Older JMs / non-rescale Deploys leave each
    // task at parent_count=1 (single parent, the historic shape).
    if (!r.eof()) {
        for (auto& t : m.tasks) {
            t.restore_from_parent_count = r.read_u32_be();
        }
    }
    if (!r.eof()) {
        m.unaligned_checkpoints = r.read_u8() != 0;
    }
    // Trailing packed expected state-version map (schema evolution).
    // Absent from older JM peers -> stays empty (no migration).
    if (!r.eof()) {
        m.expected_state_versions_packed = r.read_string();
    }
    // Trailing state-backend URI. Absent from older JM peers -> stays
    // empty (checkpoint_dir is the backend URI).
    if (!r.eof()) {
        m.state_backend_uri = r.read_string();
    }
    return m;
}

inline StartJobMsg decode_start_job(MessageReader& r) {
    StartJobMsg m;
    if (!r.eof()) {
        m.job_id = r.read_u64_be();
    }
    return m;
}
inline CancelJobMsg decode_cancel_job(MessageReader& r) {
    CancelJobMsg m;
    if (!r.eof()) {
        m.job_id = r.read_u64_be();
    }
    return m;
}
inline CancelJobAckMsg decode_cancel_job_ack(MessageReader& r) {
    CancelJobAckMsg m;
    if (!r.eof()) {
        m.job_id = r.read_u64_be();
        m.ok = r.read_u8() != 0;
        m.message = r.read_string();
    }
    return m;
}

inline RescaleJobMsg decode_rescale_job(MessageReader& r) {
    RescaleJobMsg m;
    m.job_id = r.read_u64_be();
    const std::uint32_t n = r.read_u32_be();
    m.role_parallelism.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        std::string role = r.read_string();
        std::uint32_t p = r.read_u32_be();
        m.role_parallelism.emplace_back(std::move(role), p);
    }
    return m;
}

inline RescaleJobAckMsg decode_rescale_job_ack(MessageReader& r) {
    RescaleJobAckMsg m;
    m.job_id = r.read_u64_be();
    m.ok = r.read_u8() != 0;
    m.message = r.read_string();
    return m;
}

inline RescaleOperatorMsg decode_rescale_operator(MessageReader& r) {
    RescaleOperatorMsg m;
    m.job_id = r.read_u64_be();
    m.op_id = r.read_string();
    m.new_parallelism = r.read_u32_be();
    return m;
}

inline RescaleOperatorAckMsg decode_rescale_operator_ack(MessageReader& r) {
    RescaleOperatorAckMsg m;
    m.job_id = r.read_u64_be();
    m.ok = r.read_u8() != 0;
    m.accepted_target = r.read_u32_be();
    m.message = r.read_string();
    return m;
}

inline SavepointMsg decode_savepoint(MessageReader& r) {
    SavepointMsg m;
    m.job_id = r.read_u64_be();
    m.timeout_ms = static_cast<std::int64_t>(r.read_u64_be());
    return m;
}

inline SavepointAckMsg decode_savepoint_ack(MessageReader& r) {
    SavepointAckMsg m;
    m.job_id = r.read_u64_be();
    m.ok = r.read_u8() != 0;
    m.checkpoint_id = r.read_u64_be();
    m.checkpoint_dir = r.read_string();
    m.message = r.read_string();
    return m;
}

inline SubtaskFinishedMsg decode_subtask_finished(MessageReader& r) {
    SubtaskFinishedMsg m;
    m.job_id = r.read_u64_be();
    m.tm_id = r.read_string();
    m.role = r.read_string();
    m.subtask_idx = r.read_u32_be();
    m.had_error = r.read_u8() != 0;
    m.error_message = r.read_string();
    return m;
}

inline HeartbeatMsg decode_heartbeat(MessageReader& r) {
    return HeartbeatMsg{r.read_string()};
}

inline HelloClientMsg decode_hello_client(MessageReader&) {
    return {};
}

inline SubmitJobMsg decode_submit_job(MessageReader& r) {
    SubmitJobMsg m;
    m.graph_json = r.read_string();
    if (!r.eof()) {
        const std::uint32_t pcount = r.read_u32_be();
        m.plugins.reserve(pcount);
        for (std::uint32_t i = 0; i < pcount; ++i) {
            m.plugins.push_back(decode_plugin_binary(r));
        }
    }
    if (!r.eof()) {
        m.checkpoint.checkpoint_dir = r.read_string();
        m.checkpoint.interval_ms = static_cast<std::int64_t>(r.read_u64_be());
        m.checkpoint.restore_from_dir = r.read_string();
        m.checkpoint.restore_from_checkpoint_id = r.read_u64_be();
    }
    if (!r.eof()) {
        // Trailing wire-compat field (added later than the other
        // checkpoint fields). Older clients leave it implicitly 0 = off.
        m.checkpoint.max_restarts_on_tm_loss = r.read_u32_be();
    }
    if (!r.eof()) {
        const auto raw = r.read_u8();
        m.checkpoint.alignment = (raw == static_cast<std::uint8_t>(CheckpointAlignment::Unaligned))
                                     ? CheckpointAlignment::Unaligned
                                     : CheckpointAlignment::Aligned;
    }
    if (!r.eof()) {
        // Trailing state-backend URI. Absent from older clients -> stays
        // empty (checkpoint_dir is the backend URI).
        m.checkpoint.state_backend_uri = r.read_string();
    }
    return m;
}

inline SubmitJobAckMsg decode_submit_job_ack(MessageReader& r) {
    SubmitJobAckMsg m;
    m.job_id = r.read_u64_be();
    m.ok = r.read_u8() != 0;
    m.message = r.read_string();
    return m;
}

inline TriggerCheckpointMsg decode_trigger_checkpoint(MessageReader& r) {
    TriggerCheckpointMsg m;
    m.job_id = r.read_u64_be();
    m.checkpoint_id = r.read_u64_be();
    return m;
}

inline CommitCheckpointMsg decode_commit_checkpoint(MessageReader& r) {
    CommitCheckpointMsg m;
    m.job_id = r.read_u64_be();
    m.checkpoint_id = r.read_u64_be();
    return m;
}

inline AbortCheckpointMsg decode_abort_checkpoint(MessageReader& r) {
    AbortCheckpointMsg m;
    m.job_id = r.read_u64_be();
    m.checkpoint_id = r.read_u64_be();
    return m;
}

inline BeginRescaleMsg decode_begin_rescale(MessageReader& r) {
    BeginRescaleMsg m;
    m.job_id = r.read_u64_be();
    m.op_id = r.read_string();
    m.target_parallelism = r.read_u32_be();
    m.cutover_checkpoint = r.read_u64_be();
    return m;
}

inline SubtaskCheckpointedMsg decode_subtask_checkpointed(MessageReader& r) {
    SubtaskCheckpointedMsg m;
    m.job_id = r.read_u64_be();
    m.checkpoint_id = r.read_u64_be();
    m.role = r.read_string();
    m.subtask_idx = r.read_u32_be();
    m.ok = r.read_u8() != 0;
    m.error = r.read_string();
    return m;
}

inline ListJobsAckMsg decode_list_jobs_ack(MessageReader& r) {
    ListJobsAckMsg m;
    const std::uint32_t n = r.read_u32_be();
    m.jobs.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        JobInfo j;
        j.job_id = r.read_u64_be();
        j.total_subtasks = r.read_u32_be();
        j.completed_subtasks = r.read_u32_be();
        j.completion_signalled = r.read_u8() != 0;
        m.jobs.push_back(j);
    }
    return m;
}

inline JobCompletedMsg decode_job_completed(MessageReader& r) {
    JobCompletedMsg m;
    m.job_id = r.read_u64_be();
    m.ok = r.read_u8() != 0;
    const std::uint32_t n = r.read_u32_be();
    m.errors.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        m.errors.push_back(r.read_string());
    }
    return m;
}

inline SubtaskListeningMsg decode_subtask_listening(MessageReader& r) {
    SubtaskListeningMsg m;
    m.job_id = r.read_u64_be();
    m.tm_id = r.read_string();
    m.role = r.read_string();
    m.subtask_idx = r.read_u32_be();
    m.host = r.read_string();
    const std::uint32_t n = r.read_u32_be();
    m.edge_ports.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        SubtaskListeningMsg::EdgePort ep;
        ep.upstream_role = r.read_string();
        ep.upstream_subtask_idx = r.read_u32_be();
        ep.port = r.read_u16_be();
        m.edge_ports.push_back(std::move(ep));
    }
    return m;
}

inline PeerUpdateMsg decode_peer_update(MessageReader& r) {
    PeerUpdateMsg m;
    m.job_id = r.read_u64_be();
    const std::uint32_t n = r.read_u32_be();
    m.tasks.reserve(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        PeerUpdateMsg::TaskPeers tp;
        tp.role = r.read_string();
        tp.subtask_idx = r.read_u32_be();
        const std::uint32_t pn = r.read_u32_be();
        tp.peers.reserve(pn);
        for (std::uint32_t j = 0; j < pn; ++j) {
            PeerAddress p;
            p.role = r.read_string();
            p.subtask_idx = r.read_u32_be();
            p.host = r.read_string();
            p.data_port = r.read_u16_be();
            tp.peers.push_back(std::move(p));
        }
        m.tasks.push_back(std::move(tp));
    }
    return m;
}

}  // namespace clink::cluster
