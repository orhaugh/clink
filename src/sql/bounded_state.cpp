#include "clink/sql/bounded_state.hpp"

#include <cctype>
#include <stdexcept>

#include "clink/connectors/capability.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

namespace clink::sql {

namespace {

struct KnownUnbounded {
    const char* kind;
    const char* what_it_retains;
    const char* remedy;
};

// The node kinds that retain per-key state for the life of the job.
//
// Deliberately a closed list rather than "anything stateful": windowed
// operators release their state when the window fires, TopN-per-key is
// bounded by N per key, and a lookup join keeps nothing. Flagging those
// would be noise, and a gate that cries wolf is a gate people disable.
constexpr KnownUnbounded kUnboundedKinds[] = {
    {"Aggregate",
     "one accumulator per group, kept for the life of the job",
     "add a window (TUMBLE / HOP / SESSION), set 'state_ttl', or write ALLOW UNBOUNDED STATE"},
    {"Distinct",
     "every distinct row seen so far",
     "add a window, set 'state_ttl', or write ALLOW UNBOUNDED STATE"},
    {"EquiJoin",
     "every row from both inputs, so either side can match a future row from the other",
     "use an interval join (a time-bounded ON condition), set 'state_ttl', or write ALLOW "
     "UNBOUNDED STATE"},
    {"SemiJoin",
     "every row from the probed side",
     "use an interval join, set 'state_ttl', or write ALLOW UNBOUNDED STATE"},
    {"SetOp",
     "every row seen on both sides, to evaluate the set semantics",
     "set 'state_ttl' or write ALLOW UNBOUNDED STATE"},
    {"RowNumber",
     "the running row count per partition",
     "bound the query with a window, set 'state_ttl', or write ALLOW UNBOUNDED STATE"},
};

void walk(const LogicalPlan& node, std::vector<UnboundedStateFinding>& out) {
    const auto kind = node.kind();
    for (const auto& k : kUnboundedKinds) {
        if (kind == k.kind) {
            out.push_back(UnboundedStateFinding{
                .node_kind = kind, .description = k.what_it_retains, .remedy = k.remedy});
            break;
        }
    }
    for (const auto* in : node.inputs()) {
        if (in != nullptr) {
            walk(*in, out);
        }
    }
}

}  // namespace

bool retains_unbounded_state(const std::string& node_kind) {
    for (const auto& k : kUnboundedKinds) {
        if (node_kind == k.kind) {
            return true;
        }
    }
    return false;
}

std::int64_t parse_retention_ms(const std::string& text) {
    if (text.empty()) {
        return 0;
    }
    std::size_t i = 0;
    while (i < text.size() && (std::isspace(static_cast<unsigned char>(text[i])) != 0)) {
        ++i;
    }
    const std::size_t digits_begin = i;
    while (i < text.size() && (std::isdigit(static_cast<unsigned char>(text[i])) != 0)) {
        ++i;
    }
    if (i == digits_begin) {
        throw std::invalid_argument("state_ttl: '" + text +
                                    "' does not start with a number; expected forms are "
                                    "'30s', '15m', '1h', '7d', or a bare millisecond count");
    }
    std::int64_t value = 0;
    try {
        value = std::stoll(text.substr(digits_begin, i - digits_begin));
    } catch (const std::exception&) {
        throw std::invalid_argument("state_ttl: '" + text + "' is out of range");
    }
    std::string unit;
    while (i < text.size()) {
        if (std::isspace(static_cast<unsigned char>(text[i])) == 0) {
            unit += static_cast<char>(std::tolower(static_cast<unsigned char>(text[i])));
        }
        ++i;
    }
    if (unit.empty() || unit == "ms") {
        return value;
    }
    if (unit == "s" || unit == "sec" || unit == "secs" || unit == "second" || unit == "seconds") {
        return value * 1000;
    }
    if (unit == "m" || unit == "min" || unit == "mins" || unit == "minute" || unit == "minutes") {
        return value * 60 * 1000;
    }
    if (unit == "h" || unit == "hr" || unit == "hrs" || unit == "hour" || unit == "hours") {
        return value * 60 * 60 * 1000;
    }
    if (unit == "d" || unit == "day" || unit == "days") {
        return value * 24 * 60 * 60 * 1000;
    }
    throw std::invalid_argument("state_ttl: unknown unit '" + unit + "' in '" + text +
                                "'; expected ms, s, m, h or d");
}

namespace {

// Collect every scan's TableDef, walking the whole plan.
void collect_scans(const LogicalPlan& node, std::vector<const TableDef*>& out) {
    if (const auto* scan = dynamic_cast<const LogicalScan*>(&node)) {
        out.push_back(&scan->table());
    }
    for (const auto* in : node.inputs()) {
        if (in != nullptr) {
            collect_scans(*in, out);
        }
    }
}

}  // namespace

BoundedStateReport check_plan_bounded_state(const LogicalPlan& plan, bool allow_unbounded) {
    std::vector<const TableDef*> scans;
    collect_scans(plan, scans);

    StateRetention retention;
    retention.allow_unbounded = allow_unbounded;

    // Boundedness is a TRI-state, and the distinction decides whether this
    // gate is useful or merely annoying.
    //
    //   known bounded    the input ends; retention is a non-issue.
    //   known unbounded  the input never ends; unbounded state is a real
    //                    incident waiting to happen. REJECT.
    //   unknown          the connector has no capability declaration.
    //
    // Only KNOWN-unbounded rejects. Treating unknown as unbounded was the
    // first cut and it was wrong: capability declarations cover a subset of
    // the connector catalogue, so unknown is the common case, and a gate
    // that fires on it rejects ordinary correct queries (it took out 19
    // planner tests reading plain files). A gate that cries wolf gets
    // switched off wholesale, which is worse than one with a known blind
    // spot.
    //
    // The blind spot shrinks as connectors are declared, which is the right
    // incentive: declaring a connector strictly increases what the gate can
    // catch, and never decreases it.
    bool any_known_unbounded = false;
    std::vector<std::string> unknown_connectors;
    for (const auto* t : scans) {
        if (t == nullptr) {
            continue;
        }
        // A table may set retention directly. The shortest non-zero TTL
        // across the plan wins: it is the one that actually bounds things.
        if (const auto it = t->properties.find("state_ttl"); it != t->properties.end()) {
            const auto ms = parse_retention_ms(it->second);
            if (ms > 0 && (retention.ttl_ms == 0 || ms < retention.ttl_ms)) {
                retention.ttl_ms = ms;
            }
        }
        const auto conn = t->properties.find("connector");
        if (conn == t->properties.end()) {
            unknown_connectors.emplace_back("(unnamed connector)");
            continue;
        }
        const auto* caps = connectors::CapabilityRegistry::instance().find(conn->second);
        if (caps == nullptr) {
            unknown_connectors.push_back(conn->second);
            continue;
        }
        if (caps->boundedness == connectors::Boundedness::Unbounded) {
            any_known_unbounded = true;
        }
    }

    auto report = check_bounded_state(plan, retention, /*sources_bounded=*/!any_known_unbounded);
    report.unknown_boundedness_connectors = std::move(unknown_connectors);
    return report;
}

BoundedStateReport check_bounded_state(const LogicalPlan& plan,
                                       const StateRetention& retention,
                                       bool sources_bounded) {
    BoundedStateReport report;
    report.all_inputs_bounded = sources_bounded;

    // A bounded input ends, so every accumulator is released at end of
    // stream. Nothing to gate.
    if (sources_bounded) {
        return report;
    }
    // A chosen retention or an explicit override satisfies the gate. The
    // override is recorded so the caller can shout about it.
    if (retention.ttl_ms > 0) {
        return report;
    }
    if (retention.allow_unbounded) {
        report.used_unsafe_override = true;
        return report;
    }

    walk(plan, report.findings);
    return report;
}

std::string BoundedStateReport::error_message() const {
    if (findings.empty()) {
        return {};
    }
    std::string msg = "this query retains state without bound over an unbounded input:\n";
    for (const auto& f : findings) {
        msg += "  - " + f.node_kind + " retains " + f.description + "\n";
        msg += "    fix: " + f.remedy + "\n";
    }
    msg +=
        "State that only grows will eventually exhaust the worker. Choose one of the fixes "
        "above, read from a bounded table, or state the risk explicitly with ALLOW UNBOUNDED "
        "STATE.";
    return msg;
}

}  // namespace clink::sql
