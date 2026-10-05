#pragma once

// Which connection gives way when the coordinator's admission set is full.
//
// The coordinator admits each accepted connection (its transport handshake,
// then its first frame) on a thread of its own, and holds at most
// Coordinator::Config::max_pending_connections of them at once. A new
// connection beyond that either evicts one being admitted or is refused. Which
// one is decided here, so that the choice can be tested without a network.
//
// Evicting the oldest regardless of who sent it let any one host that could
// reach the port evict every worker: a connect loop that sends nothing, run
// faster than the cap divided by a worker's admission time, made each worker
// the oldest entry in turn. And a worker whose TLS handshake had completed
// (under mTLS, an authenticated peer) gave way to a stranger that had sent
// nothing. Two rules replace it:
//
//   * The source holding the most admissions gives way first. A source is the
//     peer's address (an IPv6 peer's /64), so a flood from one host evicts its
//     own connections, never another host's, and a worker from a host of its
//     own is evicted only when every source holds at most one admission.
//   * Within that, the connection that has got least far gives way, the
//     oldest of those first. One that has completed a real handshake or begun
//     its first frame is never evicted for one that has done neither; the new
//     connection is refused instead.
//
// A connection whose whole first frame has arrived is waiting to be handled
// and is never evicted, but it still counts against the cap and towards its
// source's share: that is what bounds the threads waiting to register while
// registration is slow. Such a connection waits on the coordinator alone and
// leaves as fast as registrations are handled, so a source all of whose
// admissions have got that far is not refused at the cap: a burst of workers
// starting together on one host, whose first frames queue while registration
// works through them, used to have everything past the cap turned away,
// where the kernel's listen backlog had held them before. It is admitted up
// to the ceiling below instead, and the fair-share rule there bounds it.
//
// Both rules rest on the coordinator seeing each peer's own address. Behind a
// hop that rewrites the source address (an L4 load balancer or HA virtual IP
// that SNATs, a NAT gateway), or with several peers on one host, every peer
// behind it is one source, and among them only progress tells connections
// apart: a connection that has sent nothing can still evict a worker whose
// handshake is under way from the same address. Run the control port without
// SNAT in front of it, or require mTLS, under which a worker past its
// handshake is protected whatever its address.
//
// Eviction alone does not bound what admissions hold: an evicted admission is
// interrupted (its socket shut down), and its thread and descriptor stay until
// that thread notices. So decide_admission also counts the interrupted ones,
// each towards its own source. Past a ceiling of twice the cap, a newcomer is
// admitted only if its source holds less than its fair share of that ceiling
// (or nothing at all), so one source whose evicted admissions have not yet let
// go cannot turn away a worker from another host; and at three times the cap
// every newcomer is refused, whatever the threads are blocked in. Below the
// ceiling, a newcomer whose source holds no admission at all is admitted even
// when nothing can be evicted for it, so a set full of first frames from one
// source, waiting on a slow registration, does not turn away a worker from
// another host.
//
// A listener-form accept factory (Coordinator::set_accept_factory) hands over
// connections it has already accepted, so the coordinator never sees their
// addresses: they all share the empty source, and each arrives Handshaken.
// Among them only progress and age count, so a newcomer evicts the oldest that
// has got no further than its handshake. That is the weaker guarantee: without
// mTLS, a client that completes handshakes and then sends nothing can evict a
// worker whose handshake is done.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace clink::cluster {

// How far a connection has got in being admitted, in order.
enum class AdmissionProgress : std::uint8_t {
    // Accepted, and no further: any handshake is still under way, and nothing
    // of the first frame has arrived. Plain TCP, which has no handshake, stays
    // here until its first frame begins.
    Accepted,
    // A real transport handshake has completed (TLS; under mTLS the peer is
    // authenticated). Nothing of the first frame has arrived yet.
    Handshaken,
    // The first frame's length prefix has arrived.
    FrameStarted,
    // The whole first frame has arrived and is waiting to be handled. Never
    // evicted.
    Queued,
};

// One connection being admitted, as the policy sees it.
struct AdmissionCandidate {
    // Rises in accept order, so a lower id is an older connection.
    std::uint64_t id{0};
    // The peer's address key (see admission_source_key in the coordinator);
    // empty when it is not known, so all such connections share one source.
    std::string source;
    AdmissionProgress progress{AdmissionProgress::Accepted};
    // Interrupted (evicted, past its deadline, or stopping) and on its way
    // out: not evictable, and not part of its source's share when choosing
    // whom to evict, but its thread and descriptor still count towards the
    // ceiling, and towards its source's fair share of it (see
    // decide_admission).
    bool interrupted{false};
};

// The admission to evict so that a new connection from `newcomer_source`, as
// far along as `newcomer_progress` (Accepted for a connection the coordinator
// accepts itself, Handshaken for one a listener-form factory hands over), can
// be admitted into a full set, or nullopt when the new connection is to be
// refused instead. `pending` is every admission being tracked; interrupted
// ones are skipped, as they are already on their way out.
[[nodiscard]] inline std::optional<std::uint64_t> choose_admission_to_evict(
    const std::vector<AdmissionCandidate>& pending,
    const std::string& newcomer_source,
    AdmissionProgress newcomer_progress = AdmissionProgress::Accepted) {
    std::unordered_map<std::string, std::size_t> held;
    for (const auto& p : pending) {
        if (!p.interrupted) {
            ++held[p.source];
        }
    }
    ++held[newcomer_source];

    // The new connection is the first candidate, and newer than all.
    std::size_t best_held = held[newcomer_source];
    auto best_progress = newcomer_progress;
    std::optional<std::uint64_t> best;  // nullopt while the new connection is the choice
    for (const auto& p : pending) {
        if (p.interrupted || p.progress == AdmissionProgress::Queued) {
            continue;
        }
        const std::size_t h = held[p.source];
        bool better = false;
        if (h != best_held) {
            better = h > best_held;  // the larger share gives way
        } else if (p.progress != best_progress) {
            better = p.progress < best_progress;  // then the least progress
        } else {
            // Then the oldest; an existing connection gives way before the new
            // one, so a stream of stalling connections cannot keep a slot.
            better = !best.has_value() || p.id < *best;
        }
        if (better) {
            best_held = h;
            best_progress = p.progress;
            best = p.id;
        }
    }
    return best;
}

// What becomes of a new connection.
struct AdmissionDecision {
    enum class Kind : std::uint8_t {
        Admit,   // there is room for it
        Evict,   // admit it, having interrupted `victim` to make room
        Refuse,  // close it unadmitted
    };
    Kind kind{Kind::Refuse};
    std::uint64_t victim{0};  // Evict only
    // Refuse only: true when the ceiling was the reason.
    bool at_ceiling{false};
};

// The number of admissions tracked (interrupted ones included) past which a
// newcomer is admitted only from a source below its fair share: twice the
// cap, saturating.
[[nodiscard]] inline std::size_t admission_ceiling(std::size_t cap) {
    cap = cap == 0 ? 1 : cap;
    return cap > static_cast<std::size_t>(-1) / 2 ? static_cast<std::size_t>(-1) : cap * 2;
}

// The most admissions tracked at once, interrupted ones included, whoever
// sent them: the ceiling plus one cap more, for sources below their share,
// saturating. This is the bound on admission threads and their descriptors.
[[nodiscard]] inline std::size_t admission_hard_limit(std::size_t cap) {
    cap = cap == 0 ? 1 : cap;
    const std::size_t ceiling = admission_ceiling(cap);
    return ceiling > static_cast<std::size_t>(-1) - cap ? static_cast<std::size_t>(-1)
                                                        : ceiling + cap;
}

// Whether a new connection from `newcomer_source`, as far along as
// `newcomer_progress`, is admitted, admitted in place of one it evicts, or
// refused, given every admission being tracked (`pending`, interrupted ones
// included) and the cap (zero is treated as one).
//
//   * At the hard limit (admission_hard_limit: three times the cap, counting
//     interrupted admissions whose threads have not yet let go) it is
//     refused, from any source.
//   * At the ceiling (admission_ceiling: twice the cap) it is admitted only
//     if its source holds nothing, or less than its fair share of the
//     ceiling (the ceiling divided by the sources holding admissions, its own
//     included), counting that source's interrupted admissions too; otherwise
//     it is refused. Nothing is evicted either way, since an eviction frees
//     nothing until a thread exits. So one source whose evicted admissions
//     are slow to let go fills the ceiling with its own, and is refused
//     there, without shutting out a host that holds less.
//   * Below the cap (counting admissions not interrupted) it is admitted.
//   * At the cap, it evicts the admission choose_admission_to_evict names,
//     unless that is the only admission of a source other than the
//     newcomer's while some source holds more than one. Those are then all
//     first frames waiting to be handled (one still waiting on its peer would
//     have been named first), and evicting a host's only admission for them
//     would let one source's queue push workers from other hosts out in
//     turn; it is treated as naming none.
//   * When it names none (every admission is a first frame waiting to be
//     handled, or further along than the newcomer), a newcomer whose source
//     holds no admission, interrupted or not, is admitted beyond the cap,
//     still under the ceiling. Each such admission gives its source one, so
//     no one source can use this more than once at a time.
//   * So is a newcomer whose source's admissions, other than interrupted
//     ones, have all got their whole first frame in (Queued): they need
//     nothing more from their peers, so this is a burst waiting on
//     registration, not a peer stalling.
//   * Otherwise it is refused: its source has a connection still waiting on
//     its peer, and further along than the newcomer.
[[nodiscard]] inline AdmissionDecision decide_admission(
    const std::vector<AdmissionCandidate>& pending,
    const std::string& newcomer_source,
    std::size_t cap,
    AdmissionProgress newcomer_progress = AdmissionProgress::Accepted) {
    using Kind = AdmissionDecision::Kind;
    cap = cap == 0 ? 1 : cap;
    if (pending.size() >= admission_hard_limit(cap)) {
        return AdmissionDecision{.kind = Kind::Refuse, .at_ceiling = true};
    }
    std::size_t live = 0;
    std::size_t newcomer_holds = 0;  // interrupted ones included
    // The newcomer's source's admissions still waiting on their peer: not
    // interrupted, and their first frame not yet all in.
    std::size_t newcomer_waiting_on_peer = 0;
    std::unordered_map<std::string_view, std::size_t> sources;
    // Admissions other than interrupted ones, by source, and the most any
    // one source holds.
    std::unordered_map<std::string_view, std::size_t> live_by_source;
    std::size_t most_live = 0;
    for (const auto& p : pending) {
        live += p.interrupted ? 0 : 1;
        if (p.source == newcomer_source) {
            ++newcomer_holds;
            newcomer_waiting_on_peer +=
                !p.interrupted && p.progress != AdmissionProgress::Queued ? 1 : 0;
        }
        ++sources[p.source];
        if (!p.interrupted) {
            most_live = std::max(most_live, ++live_by_source[p.source]);
        }
    }
    if (pending.size() >= admission_ceiling(cap)) {
        const std::size_t holders = sources.size() + (newcomer_holds == 0 ? 1 : 0);
        const std::size_t fair_share = admission_ceiling(cap) / holders;
        if (newcomer_holds == 0 || newcomer_holds < fair_share) {
            return AdmissionDecision{.kind = Kind::Admit};
        }
        return AdmissionDecision{.kind = Kind::Refuse, .at_ceiling = true};
    }
    if (live < cap) {
        return AdmissionDecision{.kind = Kind::Admit};
    }
    if (const auto victim = choose_admission_to_evict(pending, newcomer_source, newcomer_progress);
        victim.has_value()) {
        const auto it = std::find_if(
            pending.begin(), pending.end(), [&](const auto& p) { return p.id == *victim; });
        const bool lone_admission_of_another_source =
            it != pending.end() && it->source != newcomer_source && live_by_source[it->source] == 1;
        if (!lone_admission_of_another_source || most_live <= 1) {
            return AdmissionDecision{.kind = Kind::Evict, .victim = *victim};
        }
    }
    if (newcomer_holds == 0 || newcomer_waiting_on_peer == 0) {
        return AdmissionDecision{.kind = Kind::Admit};
    }
    return AdmissionDecision{.kind = Kind::Refuse};
}

// Bounds the log lines that connections refused or dropped while being
// admitted can produce. A refusal or an eviction costs a client one connect,
// so whoever can reach the control port sets their rate; a line for each
// would rotate older history out within minutes and put synchronous log I/O
// on the accept and admission threads. The first `burst` in each `interval`
// are logged in full; the rest are counted, and summarised (how many, and the
// source that sent the most) once the interval is over. The per-event
// metrics, not the log, are the precise record. Thread-safe.
class AdmissionLogLimiter {
public:
    using Clock = std::chrono::steady_clock;

    AdmissionLogLimiter(std::size_t burst, Clock::duration interval)
        : burst_(burst), interval_(interval) {}

    struct Verdict {
        // Log this event's own line.
        bool log{false};
        // When not empty, the summary of an interval that has just ended, to
        // log before anything else.
        std::string summary;
    };

    // One connection refused or dropped, from `source` (the peer's address
    // key, empty when not known).
    [[nodiscard]] Verdict note(Clock::time_point now, std::string_view source) {
        std::lock_guard lock(mu_);
        Verdict v;
        v.summary = roll_locked_(now);
        if (logged_ < burst_) {
            ++logged_;
            v.log = true;
            return v;
        }
        ++suppressed_;
        if (const auto it = by_source_.find(std::string{source}); it != by_source_.end()) {
            ++it->second;
        } else if (by_source_.size() < kMaxSourcesTallied) {
            by_source_.emplace(std::string{source}, 1);
        }
        return v;
    }

    // The summary of an interval that is over and had lines left out, or an
    // empty string; the interval then starts again. For a caller with a clock
    // of its own (the accept thread), so a summary is not left waiting for
    // the next refusal, which may never come.
    [[nodiscard]] std::string flush(Clock::time_point now) {
        std::lock_guard lock(mu_);
        if (suppressed_ == 0 || now - window_start_ < interval_) {
            return {};
        }
        return roll_locked_(now);
    }

    // When flush next has a summary to give, or time_point::max().
    [[nodiscard]] Clock::time_point flush_due() {
        std::lock_guard lock(mu_);
        return suppressed_ == 0 ? Clock::time_point::max() : window_start_ + interval_;
    }

private:
    // Sources tallied per interval for the summary; beyond this many, a
    // further source's lines are counted in the total only.
    static constexpr std::size_t kMaxSourcesTallied = 32;

    std::string roll_locked_(Clock::time_point now) {
        if (started_ && now - window_start_ < interval_) {
            return {};
        }
        std::string summary;
        if (suppressed_ > 0) {
            const auto top = std::max_element(
                by_source_.begin(), by_source_.end(), [](const auto& a, const auto& b) {
                    return a.second < b.second;
                });
            summary = std::to_string(suppressed_) +
                      " more connections refused or dropped while being admitted within " +
                      std::to_string(
                          std::chrono::duration_cast<std::chrono::seconds>(interval_).count()) +
                      " s were not logged";
            if (top != by_source_.end()) {
                summary += " (most from " +
                           (top->first.empty() ? std::string{"an unknown address"} : top->first) +
                           ": " + std::to_string(top->second) + ")";
            }
        }
        started_ = true;
        window_start_ = now;
        logged_ = 0;
        suppressed_ = 0;
        by_source_.clear();
        return summary;
    }

    const std::size_t burst_;
    const Clock::duration interval_;
    std::mutex mu_;
    bool started_{false};
    Clock::time_point window_start_{};
    std::size_t logged_{0};
    std::size_t suppressed_{0};
    std::unordered_map<std::string, std::size_t> by_source_;
};

}  // namespace clink::cluster
