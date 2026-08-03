#include "clink/cluster/config_lint.hpp"

#include <algorithm>
#include <sstream>

namespace clink::cluster {

namespace {

// "memory" as a backend URI means state does not survive the process. Same
// test the delivery-guarantee analyser uses (guarantee_gate.cpp), on
// purpose: two gates disagreeing about what "durable" means would be worse
// than either being wrong.
[[nodiscard]] bool is_memory_backend(const CheckpointConfig& c) {
    return c.state_backend_uri.rfind("memory", 0) == 0;
}

// What the engine will actually do, as distinct from what was asked for.
// checkpoint_trigger_loop_ skips any job with an empty dir or a
// non-positive interval, so both are required for a checkpoint to ever
// fire periodically.
[[nodiscard]] bool periodic_checkpointing_will_run(const CheckpointConfig& c) {
    return !c.checkpoint_dir.empty() && c.interval_ms > 0;
}

}  // namespace

std::vector<ConfigProblem> lint_checkpoint_config(const CheckpointConfig& c) {
    std::vector<ConfigProblem> out;

    // --- settings that are accepted and then ignored ---------------------

    // checkpoint_trigger_loop_: `if (checkpoint_dir.empty() ||
    // interval_ms <= 0) continue;`. An interval with no directory is a
    // request for periodic checkpoints that produces none.
    if (c.interval_ms > 0 && c.checkpoint_dir.empty()) {
        out.push_back(
            {LintSeverity::Error,
             "checkpoint_interval_ms",
             "a checkpoint interval of " + std::to_string(c.interval_ms) +
                 "ms was set but checkpoint_dir is empty, so NO checkpoint will ever be taken. "
                 "The coordinator's trigger loop skips any job without a directory. Set "
                 "checkpoint_dir, or drop the interval to say plainly that this job has no "
                 "recovery."});
    }

    // CheckpointConfig's own comment: "Has no effect without
    // checkpoint_dir." kRestartAuto is the unset sentinel, so only an
    // explicit non-zero value is a request being ignored.
    if (c.max_restarts_on_worker_loss != kRestartAuto && c.max_restarts_on_worker_loss > 0 &&
        c.checkpoint_dir.empty()) {
        out.push_back(
            {LintSeverity::Error,
             "max_restarts_on_worker_loss",
             "max_restarts_on_worker_loss=" + std::to_string(c.max_restarts_on_worker_loss) +
                 " was set but checkpoint_dir is empty, so it has no effect: a restart "
                 "resumes from the latest completed checkpoint, and without a directory "
                 "there are none. The job will fail fast on the first worker loss."});
    }

    // The restore pair is all-or-nothing: the engine restores only "when
    // non-empty + non-zero". Half of it set is a resume that will not
    // happen - the job silently starts from scratch, which is the worst
    // possible outcome for someone trying to resume.
    if (c.restore_from_checkpoint_id != 0 && c.restore_from_dir.empty()) {
        out.push_back(
            {LintSeverity::Error,
             "restore_from_checkpoint_id",
             "restore_from_checkpoint_id=" + std::to_string(c.restore_from_checkpoint_id) +
                 " was set but restore_from_dir is empty, so nothing is restored and the "
                 "job starts from scratch. Both are required."});
    }
    if (!c.restore_from_dir.empty() && c.restore_from_checkpoint_id == 0) {
        out.push_back({LintSeverity::Error,
                       "restore_from_dir",
                       "restore_from_dir was set but restore_from_checkpoint_id is 0, so nothing "
                       "is restored and the job starts from scratch. Both are required."});
    }

    // capture_records bounds an epoch of the record capture; with no
    // capture_dir there is no capture to bound.
    if (c.capture_records > 0 && c.capture_dir.empty()) {
        out.push_back({LintSeverity::Error,
                       "capture_records",
                       "capture_records=" + std::to_string(c.capture_records) +
                           " was set but capture_dir is empty, so no records are captured and the "
                           "bound applies to nothing."});
    }

    // Unaligned barrier handling is a property of checkpointing. Asking for
    // it without checkpointing is not harmful, but it means the setting was
    // misunderstood.
    if (c.alignment == CheckpointAlignment::Unaligned && !periodic_checkpointing_will_run(c)) {
        out.push_back({LintSeverity::Warning,
                       "unaligned_checkpoints",
                       "unaligned checkpoints were requested but this job takes no periodic "
                       "checkpoints, so the setting has nothing to apply to."});
    }

    // --- combinations that contradict each other -------------------------

    // The durability illusion. COMPLETED-N markers get written, so the
    // control plane believes checkpoints are completing, while the state
    // they refer to lives in a process that is about to end. A restore
    // finds markers and no state.
    if (is_memory_backend(c) && !c.checkpoint_dir.empty()) {
        out.push_back(
            {LintSeverity::Error,
             "state_backend_uri",
             "state_backend_uri is a memory backend but checkpoint_dir is set. The coordinator "
             "will record completed checkpoints while the state they describe does not survive "
             "the process, so a restart or failover restores nothing. Use a durable backend "
             "(a filesystem path, rocksdb://, remote-read://, ...) or drop checkpoint_dir."});
    }

    // A durable directory with no interval takes no PERIODIC checkpoint. A
    // bounded job still gets its end-of-stream checkpoint, so this is
    // legitimate; an unbounded one gets nothing and replays from the
    // beginning after any failure. The linter cannot tell which this is
    // from the config alone, so it warns and says what to check.
    if (!c.checkpoint_dir.empty() && c.interval_ms <= 0) {
        out.push_back(
            {LintSeverity::Warning,
             "checkpoint_interval_ms",
             "checkpoint_dir is set but no interval, so no PERIODIC checkpoint is taken. A "
             "bounded source still gets its end-of-stream checkpoint; an unbounded job gets "
             "none at all and replays from the beginning after any failure."});
    }

    return out;
}

std::vector<ConfigProblem> lint_liveness_config(std::int64_t heartbeat_interval_ms,
                                                std::int64_t heartbeat_timeout_ms,
                                                std::int64_t watchdog_interval_ms) {
    std::vector<ConfigProblem> out;

    // Coordinator::Config's own comment: heartbeats "should have a shorter
    // interval - typically heartbeat_timeout / 3 - so a single missed
    // message doesn't trigger a false positive". At interval >= timeout a
    // healthy worker is declared lost on schedule.
    if (heartbeat_interval_ms > 0 && heartbeat_timeout_ms > 0 &&
        heartbeat_interval_ms >= heartbeat_timeout_ms) {
        out.push_back(
            {LintSeverity::Error,
             "heartbeat_timeout",
             "heartbeat_interval (" + std::to_string(heartbeat_interval_ms) +
                 "ms) is not shorter than heartbeat_timeout (" +
                 std::to_string(heartbeat_timeout_ms) +
                 "ms), so a HEALTHY worker will be declared lost: the timeout expires before its "
                 "next heartbeat is due. Make the interval roughly a third of the timeout."});
    } else if (heartbeat_interval_ms > 0 && heartbeat_timeout_ms > 0 &&
               heartbeat_interval_ms * 2 > heartbeat_timeout_ms) {
        // Under 2x, a single dropped heartbeat is enough to trip the
        // watchdog. Not fatal, but it turns one lost packet into a job
        // restart.
        out.push_back({LintSeverity::Warning,
                       "heartbeat_timeout",
                       "heartbeat_timeout (" + std::to_string(heartbeat_timeout_ms) +
                           "ms) is less than twice the heartbeat interval (" +
                           std::to_string(heartbeat_interval_ms) +
                           "ms), so ONE dropped heartbeat declares the worker lost and restarts "
                           "its subtasks."});
    }

    // The watchdog is what notices the timeout, so detection cannot be
    // faster than its own period. A watchdog slower than the timeout means
    // the configured timeout is not the one in effect.
    if (watchdog_interval_ms > 0 && heartbeat_timeout_ms > 0 &&
        watchdog_interval_ms > heartbeat_timeout_ms) {
        out.push_back({LintSeverity::Warning,
                       "watchdog_interval",
                       "watchdog_interval (" + std::to_string(watchdog_interval_ms) +
                           "ms) is longer than heartbeat_timeout (" +
                           std::to_string(heartbeat_timeout_ms) +
                           "ms), so worker loss is detected on the watchdog's schedule rather "
                           "than the timeout's. The effective timeout is the watchdog period."});
    }

    return out;
}

std::string render_problems(const std::vector<ConfigProblem>& problems) {
    // Errors first: the reader needs the thing that blocks them before the
    // thing that merely surprises them.
    std::vector<const ConfigProblem*> ordered;
    ordered.reserve(problems.size());
    for (const auto& p : problems) {
        if (p.is_error()) {
            ordered.push_back(&p);
        }
    }
    for (const auto& p : problems) {
        if (!p.is_error()) {
            ordered.push_back(&p);
        }
    }
    std::ostringstream os;
    for (const auto* p : ordered) {
        os << (p->is_error() ? "error" : "warning") << ": " << p->setting << ": " << p->message
           << "\n";
    }
    return os.str();
}

std::string check_config(const CheckpointConfig& c, std::vector<ConfigProblem>* out_problems) {
    auto problems = lint_checkpoint_config(c);
    const bool any_error = std::any_of(
        problems.begin(), problems.end(), [](const ConfigProblem& p) { return p.is_error(); });
    std::string rejection;
    if (any_error) {
        std::ostringstream os;
        os << "job configuration is not coherent:\n";
        for (const auto& p : problems) {
            if (p.is_error()) {
                os << "  - " << p.setting << ": " << p.message << "\n";
            }
        }
        rejection = os.str();
    }
    if (out_problems != nullptr) {
        *out_problems = std::move(problems);
    }
    return rejection;
}

// --- profiles ------------------------------------------------------------

const char* to_string(ConfigProfile p) noexcept {
    switch (p) {
        case ConfigProfile::Development:
            return "development";
        case ConfigProfile::Production:
            return "production";
    }
    return "?";
}

std::optional<ConfigProfile> profile_from_string(std::string_view s) {
    if (s == "development" || s == "dev") {
        return ConfigProfile::Development;
    }
    if (s == "production" || s == "prod") {
        return ConfigProfile::Production;
    }
    return std::nullopt;
}

void apply_profile(ConfigProfile p,
                   CheckpointConfig& c,
                   bool explicit_checkpoint_dir,
                   bool explicit_interval) {
    switch (p) {
        case ConfigProfile::Development:
            // Nothing to fill in. Development means "no recovery", and the
            // defaults already are that: no directory, no interval, and
            // kRestartAuto resolves to fail-fast without a directory. Named
            // so a submission can SAY it wants no recovery rather than
            // arriving at it by omission - which is the difference between
            // a choice and an accident.
            break;
        case ConfigProfile::Production:
            // Only what the submitter left alone. An explicit interval of 0
            // means "no periodic checkpoints" and must survive: quietly
            // rewriting a flag someone set is the same failure as ignoring
            // one.
            if (!explicit_interval && c.interval_ms <= 0) {
                // 30s. Long enough not to dominate throughput, short
                // enough that a failure replays seconds rather than
                // minutes. A default, not a recommendation - a job with a
                // known SLA should set its own.
                c.interval_ms = 30'000;
            }
            if (c.max_restarts_on_worker_loss == kRestartAuto) {
                // Leave the sentinel: with a directory it already resolves
                // to self-heal. Writing a number here would only make the
                // resolution harder to trace.
            }
            (void)explicit_checkpoint_dir;
            break;
    }
}

std::vector<ConfigProblem> lint_profile(ConfigProfile p, const CheckpointConfig& c) {
    std::vector<ConfigProblem> out;
    if (p != ConfigProfile::Production) {
        return out;
    }
    // The production profile's promise is recovery. A submission that names
    // it and cannot provide it is refused rather than downgraded, because
    // the name is the request.
    if (c.checkpoint_dir.empty()) {
        out.push_back({LintSeverity::Error,
                       "profile",
                       "profile=production requires a checkpoint_dir: without one no checkpoint "
                       "is taken, no restart can resume, and the profile's guarantees do not "
                       "exist. Set checkpoint_dir, or use profile=development to say plainly "
                       "that this job has no recovery."});
    }
    if (is_memory_backend(c)) {
        out.push_back({LintSeverity::Error,
                       "profile",
                       "profile=production requires a durable state backend, but "
                       "state_backend_uri is a memory backend, which does not survive the "
                       "process. Recovery would restore nothing."});
    }
    if (c.max_restarts_on_worker_loss != kRestartAuto && c.max_restarts_on_worker_loss == 0) {
        out.push_back({LintSeverity::Warning,
                       "max_restarts_on_worker_loss",
                       "profile=production with max_restarts_on_worker_loss=0 fails the job on "
                       "the first worker loss rather than recovering from its last checkpoint. "
                       "That is a deliberate choice if you want it, and unusual for production."});
    }
    return out;
}

}  // namespace clink::cluster
