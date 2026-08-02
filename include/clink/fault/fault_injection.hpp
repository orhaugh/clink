#pragma once

// Deterministic fault injection for clink's own runtime paths.
//
// The clink::test harnesses already mediate operator call sites and can
// inject failures there (include/clink/test/failure_injection.hpp). That
// covers "what does one operator do when process() throws". It cannot
// reach the paths a production incident actually travels: the durable
// checkpoint write between fsync and rename, the sink's prepare/commit
// boundary, the network frame writer, the coordinator's metadata write.
// Those live inside the runtime, several layers below any harness, and
// some of them must fail in a CHILD process for the test to mean anything.
//
// This is the framework for those. A fault point is a named site in
// runtime code:
//
//     CLINK_FAULT_POINT("checkpoint.before_metadata_write");
//
// A test arms it by name and ordinal (the 1-based count of times that
// name has been reached in this process):
//
//     clink::fault::Registry::instance().arm(
//         {.point = "checkpoint.before_metadata_write",
//          .ordinal = 2,                       // 0 = every occurrence
//          .action = clink::fault::Action::Throw});
//
// Determinism: activation is keyed on (name, ordinal), never on wall
// clock, thread id, or a random seed. Arming the same rule and driving
// the same input reproduces the same fault at the same point every run.
// Where a test wants a schedule rather than a single rule it arms several
// rules; they are evaluated in arm order and the first match wins.
//
// Cross-process: the registry seeds itself once from CLINK_FAULT_INJECT,
// so a test that spawns clink_node can arm a fault inside the child:
//
//     CLINK_FAULT_INJECT="checkpoint.after_publish=exit:1@3"
//
// Grammar (comma-separated rules, whitespace around a rule is ignored):
//
//     <point>=<action>[:<arg>][@<ordinal>]
//
//     action  throw | exit | abort | block | error | truncate | delay
//     arg     exit -> exit code (default 70)
//             block/delay -> milliseconds (block: 0 = until released)
//             truncate -> byte count the writer should stop at
//     @n      fire on the nth occurrence only (default: every occurrence)
//
// Build gating: CLINK_FAULT_INJECTION is defined by the build when fault
// injection is compiled in (test builds by default, or an explicit
// -DCLINK_ENABLE_FAULT_INJECTION=ON). When it is not defined every macro
// below expands to nothing, no counter is incremented, no registry symbol
// is referenced, and the runtime paths are byte-identical to a build that
// never heard of this header. Production binaries therefore carry no fault
// surface at all unless somebody deliberately asks for it - and a build
// that does ask for it says so in `clink --capabilities`.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#ifdef CLINK_FAULT_INJECTION
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace clink::fault {

// Thrown by Action::Throw. Distinct type so a test can assert that the
// failure it observed is the one it armed and not an unrelated error.
class InjectedFault : public std::runtime_error {
public:
    explicit InjectedFault(const std::string& point)
        : std::runtime_error("injected fault at '" + point + "'"), point_(point) {}

    [[nodiscard]] const std::string& point() const noexcept { return point_; }

private:
    std::string point_;
};

enum class Action : std::uint8_t {
    Throw,     // throw InjectedFault
    Exit,      // _exit(arg) - hard death, no unwinding, no atexit, no flush
    Abort,     // raise SIGABRT
    Block,     // park until released (arg ms, or indefinitely when arg == 0)
    Delay,     // sleep arg ms then continue - simulates a slow dependency
    Error,     // no side effect; the call site observes it and returns a failure
    Truncate,  // no side effect; the call site observes it and short-writes
    Observe,   // no side effect at all - arms hit counting for the point
};

[[nodiscard]] std::string_view to_string(Action a) noexcept;

// Parse an action name ("throw", "exit", ...). nullopt on an unknown name.
[[nodiscard]] std::optional<Action> action_from_string(std::string_view s) noexcept;

struct Rule {
    std::string point;
    // 0 = fire on every occurrence; n >= 1 = fire only on the nth.
    std::uint64_t ordinal{0};
    Action action{Action::Throw};
    // Exit code / block-or-delay milliseconds / truncate byte count.
    std::int64_t arg{0};
};

// What a call site learns when it reaches an armed point that does not
// unwind or kill the process. Sites that only ever throw can ignore this.
struct Outcome {
    bool fired{false};
    Action action{Action::Throw};
    std::int64_t arg{0};

    [[nodiscard]] bool is_error() const noexcept { return fired && action == Action::Error; }
    [[nodiscard]] bool is_truncate() const noexcept { return fired && action == Action::Truncate; }
    // Byte count a truncating writer should stop at, clamped to `size`.
    [[nodiscard]] std::size_t truncate_to(std::size_t size) const noexcept {
        if (!is_truncate() || arg < 0) {
            return size;
        }
        const auto want = static_cast<std::size_t>(arg);
        return want < size ? want : size;
    }
};

#ifdef CLINK_FAULT_INJECTION

// Process-wide fault registry.
//
// Scope caveat, deliberate and documented: clink_core is a static library
// and plugins are dlopen'd RTLD_LOCAL, so a plugin .so linking its own
// copy of clink_core gets its OWN Registry instance rather than sharing
// the host's (the same reason inline operator registrations must route
// through env_->registry()). Environment seeding is what makes this
// harmless: CLINK_FAULT_INJECT is read by every instance, so an env-armed
// rule is live in all of them. Programmatic arm() reaches only the module
// that called it. Tests that need a fault inside a plugin or a spawned
// process must use the environment form.
class Registry {
public:
    static Registry& instance();

    // Arm a rule. Rules are evaluated in arm order; the first whose point
    // and ordinal match wins.
    void arm(Rule rule);

    // Arm from the CLINK_FAULT_INJECT grammar. Returns the number of rules
    // parsed. Throws std::invalid_argument on a malformed spec - a typo in
    // a fault schedule must fail the test, not silently disarm it.
    std::size_t arm_from_spec(std::string_view spec);

    // Disarm everything and reset every occurrence counter. Call between
    // tests; the registry is process-wide, so a leaked rule would leak
    // into the next test.
    void reset();

    // Release every thread parked on Action::Block at `point` (empty =
    // all points). Returns how many were waiting.
    std::size_t release(std::string_view point = {});

    // Reached by CLINK_FAULT_POINT. Increments the occurrence counter for
    // `point`, evaluates the armed rules, and performs Throw/Exit/Abort/
    // Block/Delay inline. Returns the Outcome for the passive actions.
    Outcome reach(std::string_view point);

    // Occurrences of `point` so far. A test asserts on this to prove the
    // path it meant to exercise was actually travelled - a fault that
    // never fires because the code moved is a silently useless test.
    //
    // Counting only runs while the registry holds at least one rule (the
    // disarmed fast path never enters the registry at all). To count
    // without perturbing behaviour, arm Action::Observe on the point.
    [[nodiscard]] std::uint64_t hits(std::string_view point) const;

    // Every point name reached in this process, with its hit count. Used
    // by the fault-point inventory test to catch a point that was renamed
    // in code but not in the test that arms it.
    [[nodiscard]] std::vector<std::pair<std::string, std::uint64_t>> all_hits() const;

    [[nodiscard]] bool any_armed() const;

private:
    Registry();

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<Rule> rules_;
    std::unordered_map<std::string, std::uint64_t> hits_;
    std::unordered_map<std::string, std::uint64_t> blocked_;
    // Monotonic wake-up epochs. Never reset - a parked thread captures the
    // value it slept on and re-checks it after waking, so a counter that
    // went backwards would silently re-park it (it did, once; see
    // ResetReleasesAParkedThread).
    std::uint64_t global_release_epoch_{0};
    std::unordered_map<std::string, std::uint64_t> release_epoch_;
};

// Set whenever the registry holds at least one rule. Read inline on the
// disarmed path so an unarmed fault point costs one relaxed atomic load
// and no mutex, no map lookup, and no Registry::instance() guard check.
// Owned by fault_injection.cpp; every module that links clink_core sees
// its own copy, consistent with the Registry scoping note above.
extern std::atomic<bool> g_any_armed;

// Reach a fault point.
inline Outcome reach(std::string_view point) {
    if (!g_any_armed.load(std::memory_order_relaxed)) {
        return Outcome{};
    }
    return Registry::instance().reach(point);
}

// Scoped arm/reset guard for tests. Restores a clean registry on
// destruction so a throwing assertion cannot leak a rule into the next
// test in the binary.
class ScopedFault {
public:
    ScopedFault() = default;
    explicit ScopedFault(Rule rule) { Registry::instance().arm(std::move(rule)); }
    explicit ScopedFault(std::string_view spec) { Registry::instance().arm_from_spec(spec); }
    ScopedFault(const ScopedFault&) = delete;
    ScopedFault& operator=(const ScopedFault&) = delete;
    ~ScopedFault() { Registry::instance().reset(); }
};

#define CLINK_FAULT_POINT(name) ::clink::fault::reach(name)
#define CLINK_FAULT_ENABLED 1

#else  // !CLINK_FAULT_INJECTION

// Compiled out. `(void)0` in a value context still needs to yield an
// Outcome for the sites that read one, so use a default-constructed
// (fired == false) temporary; the optimiser removes it entirely.
inline Outcome reach(std::string_view) noexcept {
    return Outcome{};
}

#define CLINK_FAULT_POINT(name) (::clink::fault::Outcome{})
#define CLINK_FAULT_ENABLED 0

#endif  // CLINK_FAULT_INJECTION

// True when this binary was compiled with fault injection available.
// Reported by the capability manifest so an operator can tell at a glance
// whether a running node carries the fault surface.
[[nodiscard]] constexpr bool available() noexcept {
    return CLINK_FAULT_ENABLED != 0;
}

// ---------------------------------------------------------------------------
// Fault-point names.
//
// String literals, not an enum: the environment form has to name a point
// from outside the process, and a point compiled out of a build (say the
// SQL frontend is off) must still be nameable in a spec without a link
// error. Collected here so the set is greppable and reviewable in one
// place rather than scattered across the tree.
// ---------------------------------------------------------------------------
namespace points {

// Checkpoint write path (state backends + the durable file writer).
inline constexpr char kCheckpointBeforeWrite[] = "checkpoint.before_write";
inline constexpr char kCheckpointDuringWrite[] = "checkpoint.during_write";
inline constexpr char kCheckpointBeforeFsync[] = "checkpoint.before_fsync";
inline constexpr char kCheckpointBeforePublish[] = "checkpoint.before_publish";
inline constexpr char kCheckpointAfterPublish[] = "checkpoint.after_publish";

// Coordinator metadata + the global-completion marker.
inline constexpr char kCoordinatorBeforeMetadataWrite[] = "coordinator.before_metadata_write";
inline constexpr char kCoordinatorBeforeCompletedMarker[] = "coordinator.before_completed_marker";
inline constexpr char kCoordinatorAfterCompletedMarker[] = "coordinator.after_completed_marker";
inline constexpr char kCoordinatorBeforeCommitBroadcast[] = "coordinator.before_commit_broadcast";

// Sink two-phase commit.
inline constexpr char kSinkBeforePrepare[] = "sink.before_prepare";
inline constexpr char kSinkAfterPrepare[] = "sink.after_prepare";
inline constexpr char kSinkBeforeCommit[] = "sink.before_commit";
inline constexpr char kSinkDuringCommit[] = "sink.during_commit";
inline constexpr char kSinkAfterExternalCommit[] = "sink.after_external_commit";

// State backend + restore.
inline constexpr char kStateBeforeRestore[] = "state.before_restore";
inline constexpr char kStateDuringFlush[] = "state.during_flush";

// Network.
inline constexpr char kNetworkBeforeSend[] = "network.before_send";
inline constexpr char kNetworkDuringFrameWrite[] = "network.during_frame_write";

// Source offset snapshotting.
inline constexpr char kSourceBeforeOffsetSnapshot[] = "source.before_offset_snapshot";
inline constexpr char kSourceAfterOffsetSnapshot[] = "source.after_offset_snapshot";

}  // namespace points

}  // namespace clink::fault
