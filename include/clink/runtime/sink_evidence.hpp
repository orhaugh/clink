#pragma once

// Checkpoint-interval evidence from a sink, for destination reconciliation
// (docs/internals/incident-verification.md, "Destination reconciliation").
//
// When a job's flight recorder is armed (JobConfig::capture_dir), the sink
// runner records the sink's input per checkpoint interval exactly as an
// operator runner records an operator's, and, once the sink's on_barrier has
// returned for checkpoint N, writes
//
//   <capture_dir>/op-<sink id>/subtask-<idx>/epoch-<N>.sink.json
//
// before the barrier is acknowledged: what the interval's capture came to,
// the barrier before it in this run, the runner's attempt, and whatever the
// sink says about the interval through IntervalEvidenceSink. A checkpoint that
// completed was therefore acknowledged after its evidence was written, unless
// the write failed, which leaves the evidence absent rather than wrong.
//
// Like the rest of the flight recorder this is best-effort and opt-in: a
// failed write never fails the job. Evidence that is absent, truncated or
// disarmed makes a reconciliation unavailable; it never passes one.

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "clink/core/types.hpp"

namespace clink {

// A sink that can describe what it did with a checkpoint interval's rows. The
// runner finds it by dynamic_cast, so a sink that does not implement it is
// unaffected. Both calls are made on the sink's task thread.
class IntervalEvidenceSink {
public:
    virtual ~IntervalEvidenceSink() = default;

    // Called once, before open(), when the job records evidence. A sink may
    // read more about its destination at open because of it.
    virtual void arm_interval_evidence() = 0;

    // Called right after on_barrier(checkpoint_id) returned: a JSON object, as
    // text, describing the interval that ended at that barrier, or empty when
    // the sink has nothing to say. Must not throw.
    [[nodiscard]] virtual std::string interval_evidence(std::uint64_t checkpoint_id) noexcept = 0;
};

namespace capture {

inline constexpr const char* kSinkIntervalFormat = "clink-sink-interval";
inline constexpr std::int64_t kSinkIntervalVersion = 1;

// What the sink runner knows about one interval.
struct SinkIntervalHeader {
    OperatorId op;
    std::size_t subtask_idx{0};   // the capture directory's subtask index
    std::uint64_t checkpoint{0};  // the barrier the interval ended at
    // The barrier this runner handled before it in this run; 0 when this is the
    // first barrier the runner saw, so the interval began where the run did.
    std::uint64_t previous_barrier{0};
    std::string attempt;  // 16 hex digits, drawn when the runner started
    // What the interval's capture came to: "written" (the epoch file holds
    // it), "empty" (no records, so no file), or "disarmed" (a capture write
    // failed, now or earlier, so the file is absent or stale).
    std::string capture;
    std::uint64_t records_seen{0};
    std::uint64_t records_stored{0};
    bool truncated{false};
    // The last watermark the sink had received when the barrier arrived, in
    // this run; none when it had received none.
    std::optional<std::int64_t> watermark_ms;
};

// The evidence file's text: the header's fields, with `sink_json` (the sink's
// own object, or empty for none) under "sink".
[[nodiscard]] std::string sink_interval_json(const SinkIntervalHeader& header,
                                             std::string_view sink_json);

// Writes <dir>/op-<op>/subtask-<idx>/epoch-<checkpoint>.sink.json beside its
// name and renames it into place, so the file is whole or absent. Returns
// false when it could not be written. Never throws.
bool write_sink_interval(const std::filesystem::path& dir,
                         const SinkIntervalHeader& header,
                         std::string_view sink_json) noexcept;

// A fresh 64-bit attempt identity as 16 hex digits.
[[nodiscard]] std::string new_attempt_id();

// Whether a parameter's name suggests a credential (password, secret, token,
// sasl, auth, ...): the rule an incident bundle redacts graph parameters by,
// and a sink's op-spec sidecar its own.
[[nodiscard]] bool looks_like_credential(std::string_view param_name) noexcept;

// `params` with the value of every name that looks like a credential replaced
// by "<redacted>".
[[nodiscard]] std::map<std::string, std::string> redact_credentials(
    std::map<std::string, std::string> params);

}  // namespace capture
}  // namespace clink
