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
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/network/wire.hpp"
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
// The DATA plane: an operator-to-operator frame, decoded from untrusted bytes.
//
// Every other target here covers the CONTROL plane. The data path carries Arrow
// IPC between operators and its decoder is reached with whatever a peer sends -
// a corrupt batch, a truncated stream, a schema that does not match what this
// channel expects. It was the one untested decoder (follow-up 10).
//
// Byte 0 selects the element kind, mirroring the wire's own framing, so one
// corpus entry can steer at any branch rather than only the one it happened to
// hit. The remainder is the frame body.
//
// What this asserts is only "does not crash, does not read out of bounds, does
// not overflow" - the sanitizers make that meaningful. arrow_batch_from_ipc is
// expected to return nullptr on garbage; returning a batch for a corrupt stream
// would be a different and much worse defect, but it is not what this target can
// judge.
inline void fuzz_data_frame(const std::uint8_t* data, std::size_t size) {
    if (size < 1) {
        return;
    }
    const auto kind = static_cast<network::Kind>(data[0]);
    const std::uint8_t* payload = data + 1;
    const std::size_t payload_size = size - 1;

    switch (kind) {
        case network::Kind::ArrowBatch: {
#ifdef CLINK_HAS_ARROW
            // The decoder that runs on every columnar hop. Copies into an
            // Arrow-owned buffer and opens a stream reader over it.
            auto batch =
                arrow_batch_from_ipc(reinterpret_cast<const std::byte*>(payload), payload_size);
            if (batch) {
                // Touch the shape the receive loop touches: a decoded batch whose
                // schema or row count disagrees with the channel's expectation is
                // rejected there, and reading them here is what makes a malformed
                // but openable stream visible to the fuzzer.
                (void)batch->num_rows();
                (void)batch->num_columns();
                (void)batch->schema()->ToString();
                // Properties, not just absence of crashes. A decoder that
                // ACCEPTS a batch vouches for it, so:
                //
                // 1. The accepted batch must pass Arrow's own full
                //    validation - offsets in range, buffer sizes coherent.
                //    An accepted-but-invalid batch is silent corruption
                //    travelling the wire, exactly what crash-only fuzzing
                //    cannot see (downstream reads of it are the crash, far
                //    from the cause).
                if (const auto st = batch->ValidateFull(); !st.ok()) {
                    std::fprintf(stderr,
                                 "fuzz_data_frame: decoder accepted a batch that fails "
                                 "ValidateFull: %s\n",
                                 st.ToString().c_str());
                    std::abort();
                }
                // 2. Round-trip: re-encoding through the send path and
                //    decoding again must reproduce the batch exactly. A
                //    divergence means one side of the wire format lies.
                //
                // nans_equal: Arrow's DEFAULT equality follows IEEE-754, so
                // NaN != NaN and a float column containing one never
                // compares equal to ITSELF. Without this option the
                // property fires on a faithful round-trip - a 25-minute
                // campaign found exactly that (v: double carrying
                // [5e-324, nan, 3.2377e-319] came back byte-identical and
                // still "failed"). Bit-identical NaN is what the wire has
                // to preserve, and it does; asserting IEEE equality on it
                // asks the transport for something arithmetic, not
                // transport, and would have cost a future reader a
                // diagnosis on a scheduled run.
                const auto reencoded = arrow_batch_to_ipc(*batch);
                auto again = arrow_batch_from_ipc(reencoded.data(), reencoded.size());
                if (!again) {
                    std::fprintf(stderr, "fuzz_data_frame: re-encoded batch failed to decode\n");
                    std::abort();
                }
                const auto nan_aware = arrow::EqualOptions::Defaults().nans_equal(true);
                if (!batch->schema()->Equals(*again->schema()) ||
                    !batch->Equals(*again, /*check_metadata=*/false, nan_aware)) {
                    std::fprintf(stderr,
                                 "fuzz_data_frame: decode -> encode -> decode did not "
                                 "round-trip\n");
                    std::abort();
                }
            }
#else
            (void)payload;
            (void)payload_size;
#endif
            break;
        }
        case network::Kind::Watermark:
        case network::Kind::WatermarkIdle:
            // [i64 timestamp_be]. Short frames are the interesting case: the
            // receive loop must not read past the end when a peer truncates.
            if (payload_size >= 8) {
                (void)network::read_i64_be(reinterpret_cast<const std::byte*>(payload));
            }
            break;
        case network::Kind::Barrier:
            if (payload_size >= 8) {
                (void)network::read_u64_be(reinterpret_cast<const std::byte*>(payload));
            }
            break;
        case network::Kind::CreditUpdate:
            if (payload_size >= 4) {
                (void)network::read_u32_be(reinterpret_cast<const std::byte*>(payload));
            }
            break;
        default:
            break;
    }
}

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
        {"data_frame", &fuzz_data_frame},
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
