#include "clink/fault/fault_injection.hpp"

#include <cstdlib>
#include <string>
#include <string_view>

namespace clink::fault {

std::string_view to_string(Action a) noexcept {
    switch (a) {
        case Action::Throw:
            return "throw";
        case Action::Exit:
            return "exit";
        case Action::Abort:
            return "abort";
        case Action::Block:
            return "block";
        case Action::Delay:
            return "delay";
        case Action::Error:
            return "error";
        case Action::Truncate:
            return "truncate";
        case Action::Observe:
            return "observe";
    }
    return "?";
}

std::optional<Action> action_from_string(std::string_view s) noexcept {
    if (s == "throw")
        return Action::Throw;
    if (s == "exit")
        return Action::Exit;
    if (s == "abort")
        return Action::Abort;
    if (s == "block")
        return Action::Block;
    if (s == "delay")
        return Action::Delay;
    if (s == "error")
        return Action::Error;
    if (s == "truncate")
        return Action::Truncate;
    if (s == "observe")
        return Action::Observe;
    return std::nullopt;
}

}  // namespace clink::fault

#ifdef CLINK_FAULT_INJECTION

#include <chrono>
#include <csignal>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace clink::fault {

std::atomic<bool> g_any_armed{false};

namespace {

std::string_view trim(std::string_view s) {
    const auto is_space = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; };
    while (!s.empty() && is_space(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && is_space(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

// Parse one `<point>=<action>[:<arg>][@<ordinal>]` clause. Throws
// std::invalid_argument on anything it does not understand: a typo in a
// fault schedule must fail loudly, because the alternative is a test that
// silently exercises nothing and passes.
Rule parse_rule(std::string_view clause) {
    const auto bad = [&](const std::string& why) {
        throw std::invalid_argument("CLINK_FAULT_INJECT: " + why + " in '" + std::string(clause) +
                                    "'");
    };

    const auto eq = clause.find('=');
    if (eq == std::string_view::npos) {
        bad("expected <point>=<action>");
    }
    Rule rule;
    rule.point = std::string(trim(clause.substr(0, eq)));
    if (rule.point.empty()) {
        bad("empty fault-point name");
    }

    auto rhs = trim(clause.substr(eq + 1));

    // Trailing @ordinal.
    if (const auto at = rhs.find('@'); at != std::string_view::npos) {
        const auto ord = trim(rhs.substr(at + 1));
        if (ord.empty()) {
            bad("empty ordinal after '@'");
        }
        try {
            rule.ordinal = std::stoull(std::string(ord));
        } catch (const std::exception&) {
            bad("ordinal is not a number");
        }
        rhs = trim(rhs.substr(0, at));
    }

    // Optional :arg.
    std::string_view action_name = rhs;
    if (const auto colon = rhs.find(':'); colon != std::string_view::npos) {
        action_name = trim(rhs.substr(0, colon));
        const auto arg = trim(rhs.substr(colon + 1));
        if (arg.empty()) {
            bad("empty argument after ':'");
        }
        try {
            rule.arg = std::stoll(std::string(arg));
        } catch (const std::exception&) {
            bad("argument is not a number");
        }
    }

    const auto action = action_from_string(action_name);
    if (!action.has_value()) {
        bad("unknown action '" + std::string(action_name) + "'");
    }
    rule.action = *action;

    // Exit without an explicit code uses 70 (EX_SOFTWARE): distinguishable
    // from a normal non-zero exit the code under test might produce itself.
    if (rule.action == Action::Exit && rule.arg == 0) {
        rule.arg = 70;
    }
    return rule;
}

}  // namespace

Registry& Registry::instance() {
    static Registry reg;
    return reg;
}

namespace {

// Force the registry to exist during static initialisation.
//
// Without this the environment form is dead code. reach() checks
// g_any_armed inline and returns immediately when it is false - which is
// the whole point of the fast path - so on a process where nothing armed
// anything programmatically, Registry::instance() is never called, the
// constructor never runs, and CLINK_FAULT_INJECT is never read. Every
// fault point stays inert while the operator believes a fault is armed.
//
// This is safe on ordering: g_any_armed is a constant-initialised
// std::atomic<bool> in the same TU, so it is zero-initialised before any
// dynamic initialiser here runs. The TU is always linked because the
// inline reach() references g_any_armed.
std::atomic<bool> g_env_seeding_ran{false};

const bool g_env_seeded = [] {
    (void)Registry::instance();
    g_env_seeding_ran.store(true, std::memory_order_relaxed);
    return true;
}();

}  // namespace

bool Registry::env_seeding_ran() noexcept {
    return g_env_seeding_ran.load(std::memory_order_relaxed);
}

Registry::Registry() {
    // Seed once from the environment. Read here rather than per-reach so a
    // setenv() after first use cannot change behaviour halfway through a
    // run - a fault schedule that mutates mid-test is not reproducible.
    const char* spec = std::getenv("CLINK_FAULT_INJECT");
    if (spec != nullptr && *spec != '\0') {
        arm_from_spec(spec);
    }
}

void Registry::arm(Rule rule) {
    {
        std::lock_guard lock(mu_);
        rules_.push_back(std::move(rule));
    }
    g_any_armed.store(true, std::memory_order_relaxed);
}

std::size_t Registry::arm_from_spec(std::string_view spec) {
    std::size_t n = 0;
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        const auto comma = spec.find(',', pos);
        const auto clause = trim(spec.substr(
            pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos));
        if (!clause.empty()) {
            arm(parse_rule(clause));
            ++n;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        pos = comma + 1;
    }
    return n;
}

void Registry::reset() {
    {
        std::lock_guard lock(mu_);
        rules_.clear();
        hits_.clear();
        // Wake anything parked so reset() cannot wedge a test that armed
        // Block and then failed an assertion before releasing.
        //
        // The release counters are deliberately NOT cleared here. A parked
        // thread captured its epoch before sleeping and re-checks it after
        // waking; zeroing the counter under it would make the predicate
        // read false again and park it forever. Monotonic counters that are
        // never reset make the wake-up edge unmissable. They are bounded by
        // the number of distinct fault-point names in the binary.
        ++global_release_epoch_;
    }
    cv_.notify_all();
    g_any_armed.store(false, std::memory_order_relaxed);
}

std::size_t Registry::release(std::string_view point) {
    std::size_t woken = 0;
    {
        std::lock_guard lock(mu_);
        if (point.empty()) {
            for (const auto& [name, count] : blocked_) {
                woken += count;
            }
            ++global_release_epoch_;
        } else {
            const std::string key(point);
            if (const auto it = blocked_.find(key); it != blocked_.end()) {
                woken = it->second;
            }
            ++release_epoch_[key];
        }
    }
    cv_.notify_all();
    return woken;
}

Outcome Registry::reach(std::string_view point) {
    Rule matched;
    bool found = false;
    // Release-epoch baselines for Action::Block, captured HERE rather than
    // in the Block case below.
    //
    // The mutex is dropped between matching a rule and parking on it, and a
    // release() or reset() landing in that window used to be lost entirely:
    // the waker bumped the epoch and notified with nobody yet waiting, then
    // the thread took the lock, read the ALREADY-BUMPED epoch as its own
    // baseline, found the predicate false, and slept forever. Capturing the
    // baseline under the same lock that matched the rule makes the window
    // empty - a wake that lands after this point necessarily moves the
    // epoch ABOVE the baseline, so the predicate is true before the thread
    // ever parks.
    //
    // Found on Linux by ResetReleasesAParkedThread timing out. It is a race
    // the macOS scheduler almost always won.
    std::uint64_t point_epoch_at_entry = 0;
    std::uint64_t global_epoch_at_entry = 0;
    {
        std::lock_guard lock(mu_);
        if (rules_.empty()) {
            return Outcome{};
        }
        const std::string key(point);
        const std::uint64_t hit = ++hits_[key];
        for (const auto& r : rules_) {
            if (r.point != key) {
                continue;
            }
            if (r.ordinal != 0 && r.ordinal != hit) {
                continue;
            }
            matched = r;
            found = true;
            break;
        }
        if (found && matched.action == Action::Block) {
            point_epoch_at_entry = release_epoch_[key];
            global_epoch_at_entry = global_release_epoch_;
        }
    }
    if (!found) {
        return Outcome{};
    }

    switch (matched.action) {
        case Action::Throw:
            throw InjectedFault(matched.point);
        case Action::Exit:
            // _exit, not exit: no atexit handlers, no stream flushing, no
            // destructors. This is the closest a process can get to being
            // SIGKILLed at exactly this line, which is the point.
            ::_exit(static_cast<int>(matched.arg));
        case Action::Abort:
            std::raise(SIGABRT);
            break;
        case Action::Delay:
            if (matched.arg > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(matched.arg));
            }
            break;
        case Action::Block: {
            std::unique_lock lock(mu_);
            const std::string key(matched.point);
            ++blocked_[key];
            const auto released = [&] {
                return global_release_epoch_ != global_epoch_at_entry ||
                       release_epoch_[key] != point_epoch_at_entry;
            };
            if (matched.arg > 0) {
                cv_.wait_for(lock, std::chrono::milliseconds(matched.arg), released);
            } else {
                cv_.wait(lock, released);
            }
            if (auto it = blocked_.find(key); it != blocked_.end() && it->second > 0) {
                --it->second;
            }
            break;
        }
        case Action::Error:
        case Action::Truncate:
        case Action::Observe:
            break;
    }
    if (matched.action == Action::Observe) {
        return Outcome{};
    }
    return Outcome{.fired = true, .action = matched.action, .arg = matched.arg};
}

std::uint64_t Registry::hits(std::string_view point) const {
    std::lock_guard lock(mu_);
    const auto it = hits_.find(std::string(point));
    return it == hits_.end() ? 0 : it->second;
}

std::vector<std::pair<std::string, std::uint64_t>> Registry::all_hits() const {
    std::lock_guard lock(mu_);
    return {hits_.begin(), hits_.end()};
}

bool Registry::any_armed() const {
    std::lock_guard lock(mu_);
    return !rules_.empty();
}

}  // namespace clink::fault

#endif  // CLINK_FAULT_INJECTION
