#pragma once
//
// The protocol trace: structured events at the exactly-once protocol's
// decision points, one per line, off by default. Enabled by
// CLINK_PROTOCOL_TRACE_DIR naming a directory; each process then appends to
// its own file there (<role>-<pid>-<start>.ndjson). When the variable is
// unset every site costs one relaxed atomic load, the way fault points do.
//
// The vocabulary is a contract with the specification: each event maps to
// one action of formal/ExactlyOnce.tla (or to a stutter the trace module
// recognises), the manifest formal/trace/events.txt lists every event the
// engine may emit, and scripts/check-protocol-trace-events.py fails the
// build when code, manifest and trace module disagree. A recorded trace is
// validated by `scripts/formal-check.sh --trace <file-or-dir>`, which
// model-checks it against the specification.
//
// Line shape:
//   {"seq":12,"ts":1725555555123456,"proc":"coordinator:4242","event":"Trigger",
//    "job":1,"ckpt":3,"epoch":1}
// seq is per process and monotonic; ts is microseconds since the Unix epoch
// on the process's clock; every other field is the event's own.

#include <cstdint>
#include <string>
#include <string_view>

namespace clink::protocol_trace {

// True when tracing is on. The first call resolves CLINK_PROTOCOL_TRACE_DIR;
// later calls are one relaxed atomic load.
[[nodiscard]] bool enabled() noexcept;

// Names the process in the trace ("coordinator", "worker"). The first call
// wins; a process hosting both (an in-process test cluster) reports the first.
void set_process_role(std::string_view role);

// The directory in use, empty when disabled.
[[nodiscard]] std::string directory();

// Re-resolve the environment (tests that turn tracing on after the process
// started). Closes the current file.
void reset_for_tests();

// One event under construction. Build only behind `if (enabled())`; emit()
// appends the line and flushes it, so a process killed right after still
// leaves the event on disk.
class Event {
public:
    explicit Event(std::string_view name);
    Event& u(std::string_view key, std::uint64_t value);
    Event& i(std::string_view key, std::int64_t value);
    Event& s(std::string_view key, std::string_view value);
    Event& b(std::string_view key, bool value);
    void emit();

private:
    std::string line_;
};

}  // namespace clink::protocol_trace
