#pragma once

// The fuzz targets themselves, as plain functions.
//
// Each takes a byte string and must not crash, hang, read out of bounds,
// or allocate from a number it was handed. Throwing is fine and expected -
// a malformed input is an error, not a catastrophe.
//
// They live in a header rather than in the libFuzzer entry points because
// they have two callers, and the second one matters more:
//
//   * DISCOVERY - fuzz/fuzz_<name>.cpp, one libFuzzer binary each. Needs a
//     clang that ships libFuzzer, runs for as long as you give it, and is
//     therefore not something CI can gate on.
//
//   * REGRESSION - tests/test_fuzz_corpus.cpp, which replays every file in
//     fuzz/corpus/ through these same functions under plain gtest. Needs
//     no fuzzing engine, runs in milliseconds, and gates in CI on every
//     platform.
//
// That split is the point. Discovery finds a crash; the input goes into
// fuzz/corpus/<target>/ and is committed; from then on the crash is a
// permanent regression test that runs everywhere, including on builds that
// could not run a fuzzer at all.

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/schema_version.hpp"

#ifdef CLINK_FUZZ_WITH_SQL
#include "clink/sql/parser.hpp"
#endif

namespace clink::fuzzing {

// Every fuzz target, by name. The name is the corpus subdirectory, so the
// regression replay and the discovery binaries agree on where inputs live
// without either hard-coding a path.
struct Target {
    const char* name;
    void (*run)(const std::uint8_t* data, std::size_t size);
};

// --- cluster control-plane frames ---------------------------------------
//
// The highest-value target: these bytes arrive from an unauthenticated
// peer on the control port. W14 fixed three defects here (a 4-byte header
// requesting a 4 GB allocation, eleven unbounded reserves, and a decode
// throw terminating the process); this is what keeps them fixed against
// inputs nobody thought to write down.
inline void fuzz_cluster_frame(const std::uint8_t* data, std::size_t size) {
    if (size < 1) {
        return;
    }
    // Byte 0 selects the message kind, so one corpus entry can steer the
    // fuzzer at any decoder rather than only the one it happened to hit.
    // The remainder is the body.
    const auto kind = static_cast<cluster::MessageKind>(data[0]);
    std::vector<std::byte> body;
    body.reserve(size - 1);
    for (std::size_t i = 1; i < size; ++i) {
        body.push_back(static_cast<std::byte>(data[i]));
    }
    cluster::MessageReader r(std::move(body));
    try {
        switch (kind) {
            case cluster::MessageKind::Register:
                (void)cluster::decode_register(r);
                break;
            case cluster::MessageKind::RegisterAck:
                (void)cluster::decode_register_ack(r);
                break;
            case cluster::MessageKind::Deploy:
                (void)cluster::decode_deploy(r);
                break;
            case cluster::MessageKind::PeerUpdate:
                (void)cluster::decode_peer_update(r);
                break;
            case cluster::MessageKind::CancelJob:
                (void)cluster::decode_cancel_job(r);
                break;
            case cluster::MessageKind::TriggerCheckpoint:
                (void)cluster::decode_trigger_checkpoint(r);
                break;
            case cluster::MessageKind::CommitCheckpoint:
                (void)cluster::decode_commit_checkpoint(r);
                break;
            case cluster::MessageKind::AbortCheckpoint:
                (void)cluster::decode_abort_checkpoint(r);
                break;
            case cluster::MessageKind::BeginRescale:
                (void)cluster::decode_begin_rescale(r);
                break;
            case cluster::MessageKind::SubtaskFinished:
                (void)cluster::decode_subtask_finished(r);
                break;
            case cluster::MessageKind::SubtaskListening:
                (void)cluster::decode_subtask_listening(r);
                break;
            case cluster::MessageKind::SubtaskCheckpointed:
                (void)cluster::decode_subtask_checkpointed(r);
                break;
            case cluster::MessageKind::Heartbeat:
                (void)cluster::decode_heartbeat(r);
                break;
            case cluster::MessageKind::HelloClient:
                (void)cluster::decode_hello_client(r);
                break;
            case cluster::MessageKind::SubmitJob:
                (void)cluster::decode_submit_job(r);
                break;
            case cluster::MessageKind::SubmitJobAck:
                (void)cluster::decode_submit_job_ack(r);
                break;
            case cluster::MessageKind::ListJobsAck:
                (void)cluster::decode_list_jobs_ack(r);
                break;
            case cluster::MessageKind::FinalCheckpointAssigned:
                (void)cluster::decode_final_checkpoint_assigned(r);
                break;
            case cluster::MessageKind::RequestFinalCheckpoint:
                (void)cluster::decode_request_final_checkpoint(r);
                break;
            case cluster::MessageKind::Savepoint:
                (void)cluster::decode_savepoint(r);
                break;
            case cluster::MessageKind::SavepointAck:
                (void)cluster::decode_savepoint_ack(r);
                break;
            default:
                // An unknown kind byte is what the dispatch switches do
                // with it: nothing. Fuzzing it is still worthwhile,
                // because it proves the fuzzer reaching an unhandled kind
                // is not itself a crash.
                break;
        }
    } catch (const std::exception&) {
        // The contract: malformed input is an error, not a crash.
    }
}

// --- checkpoint integrity sidecar ---------------------------------------
//
// Read from disk, so the input is whatever survived a partial write, a
// truncation, or a corrupted filesystem - not only what clink wrote.
inline void fuzz_checkpoint_meta(const std::uint8_t* data, std::size_t size) {
    const std::string_view text(reinterpret_cast<const char*>(data), size);
    try {
        state::CheckpointMeta out;
        (void)state::CheckpointMeta::parse(text, out);
    } catch (const std::exception&) {
    }
}

// --- packed state-version map -------------------------------------------
//
// Line-oriented text read out of snapshot metadata. Its own header says
// "unparseable content is a corrupt snapshot, not an absent map", which is
// a claim worth testing against inputs nobody chose.
inline void fuzz_state_version_map(const std::uint8_t* data, std::size_t size) {
    const std::string_view packed(reinterpret_cast<const char*>(data), size);
    try {
        (void)StateVersionMap::unpack(packed);
    } catch (const std::exception&) {
    }
}

// --- fault-injection schedule -------------------------------------------
//
// Comes from CLINK_FAULT_INJECT, so it is operator input, and its own
// contract is that a malformed schedule is rejected LOUDLY rather than
// silently disarming the test that depends on it.
//
// Only exists when fault injection is compiled in - the Registry is
// compiled out otherwise, and there is no parser to fuzz. Guarded rather
// than always-declared so a fuzz build with injection off fails to list
// the target rather than failing to link it.
#ifdef CLINK_FAULT_INJECTION
inline void fuzz_fault_spec(const std::uint8_t* data, std::size_t size) {
    const std::string_view spec(reinterpret_cast<const char*>(data), size);
    try {
        (void)fault::Registry::instance().arm_from_spec(spec);
    } catch (const std::exception&) {
    }
    // Leave no armed rule behind: a fuzz iteration that armed something
    // would perturb every iteration after it, and a fault point firing
    // inside the fuzzer is not the thing under test.
    fault::Registry::instance().reset();
}
#endif  // CLINK_FAULT_INJECTION

#ifdef CLINK_FUZZ_WITH_SQL
// --- SQL text -----------------------------------------------------------
//
// User input by definition, and the largest attack surface by input space.
// Runs through libpg_query, which is C and vendored.
inline void fuzz_sql_parse(const std::uint8_t* data, std::size_t size) {
    // Cap the input. libpg_query's own recursion depth is bounded, but a
    // multi-megabyte parse is slow rather than interesting, and a fuzzer
    // left to grow inputs will find that before it finds a bug.
    if (size > 64 * 1024) {
        return;
    }
    const std::string sql(reinterpret_cast<const char*>(data), size);
    try {
        (void)sql::parse(sql);
    } catch (const std::exception&) {
    }
}
#endif

// Every target, in one place. The regression replay walks this, so a new
// target gets its corpus replayed without touching the test.
[[nodiscard]] inline const std::vector<Target>& all_targets() {
    static const std::vector<Target> targets = {
        {"cluster_frame", &fuzz_cluster_frame},
        {"checkpoint_meta", &fuzz_checkpoint_meta},
        {"state_version_map", &fuzz_state_version_map},
#ifdef CLINK_FAULT_INJECTION
        {"fault_spec", &fuzz_fault_spec},
#endif
#ifdef CLINK_FUZZ_WITH_SQL
        {"sql_parse", &fuzz_sql_parse},
#endif
    };
    return targets;
}

}  // namespace clink::fuzzing
