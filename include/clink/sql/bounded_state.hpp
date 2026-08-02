#pragma once

// Bounded-state validation for continuous queries.
//
// Several perfectly ordinary SQL constructs retain per-key state for the
// lifetime of the job when the input never ends. A windowless GROUP BY
// keeps one accumulator per group forever; SELECT DISTINCT keeps every
// distinct row it has ever seen; an unwindowed equi-join keeps both build
// sides indefinitely. Over a bounded input all three are fine. Over an
// unbounded one they are an out-of-memory incident with a delay fuse, and
// clink accepted them silently.
//
// This is the gate. A query that would retain state without bound must
// carry one of:
//
//   * a bounded input       - every source has an end
//   * a finite window       - state is scoped to a window and released
//   * an explicit state TTL - retention is chosen deliberately
//   * ALLOW UNBOUNDED STATE - the user has said, in the query, that they
//                             know and accept it
//
// The override exists because there are legitimate uses (a low-cardinality
// GROUP BY over a bounded key space, a short-lived job) and because a gate
// with no escape hatch gets disabled wholesale. It is deliberately verbose
// to write, is reported prominently, and increments a metric, so a cluster
// operator can find every job running on it.
//
// Syntax (clink extensions, in the existing dialect's style):
//
//   SELECT ... FROM t GROUP BY k              -- rejected when t is unbounded
//   CREATE TABLE t (...) WITH (connector='kafka', state_ttl='1h')
//     ... GROUP BY k                          -- accepted, retention chosen
//   SELECT ... FROM t GROUP BY k
//     ALLOW UNBOUNDED STATE                   -- accepted, explicitly unsafe
//
// The retention option is `state_ttl` on a table's WITH clause. A bare
// identifier, not a dotted one: PostgreSQL's WITH grammar takes
// identifiers, so `state.ttl` is a syntax error - and the dialect already
// spells its options this way (delivery_guarantee, primary_key,
// commit_group).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace clink::sql {

class LogicalPlan;

// One construct in the plan that retains state without a natural bound.
struct UnboundedStateFinding {
    std::string node_kind;    // "Aggregate", "Distinct", "EquiJoin", ...
    std::string description;  // what it retains, in the user's terms
    std::string remedy;       // the concrete alternatives, in SQL
};

// Retention policy applied to a query. Resolved from, in order of
// precedence: a query-level WITH option, the session/script SET, the
// table's DDL option.
struct StateRetention {
    // 0 = unset. Milliseconds.
    std::int64_t ttl_ms{0};
    bool allow_unbounded{false};

    [[nodiscard]] bool bounded() const noexcept { return ttl_ms > 0 || allow_unbounded; }
};

struct BoundedStateReport {
    std::vector<UnboundedStateFinding> findings;
    // True when the plan's inputs all terminate, which makes retention a
    // non-issue however much state each operator holds.
    bool all_inputs_bounded{false};
    // Set when the query relies on ALLOW UNBOUNDED STATE. The caller logs
    // this prominently and bumps a metric.
    bool used_unsafe_override{false};
    // Connectors in this plan with no capability declaration, so their
    // boundedness is unknown. These do NOT trip the gate - see
    // check_plan_bounded_state for why - but they are the gate's blind
    // spot, and naming them is how it gets smaller.
    std::vector<std::string> unknown_boundedness_connectors;

    [[nodiscard]] bool ok() const noexcept { return findings.empty(); }
    // The rejection message, empty when ok().
    [[nodiscard]] std::string error_message() const;
};

// Parse a retention duration: "30s", "15m", "1h", "7d", or a bare integer
// read as milliseconds. Returns 0 for an empty string; throws
// std::invalid_argument on anything it cannot parse, because a
// mistyped retention that silently became "no retention" would defeat the
// entire gate.
[[nodiscard]] std::int64_t parse_retention_ms(const std::string& text);

// Walk `plan` and report every unbounded-state construct that `retention`
// does not cover. `sources_bounded` is the planner's verdict on whether
// every scan in the plan reads a bounded table.
[[nodiscard]] BoundedStateReport check_bounded_state(const LogicalPlan& plan,
                                                     const StateRetention& retention,
                                                     bool sources_bounded);

// The planner-facing entry point: resolve boundedness and retention from
// the plan's own scans, then gate.
//
// Boundedness comes from the connector capability registry, which already
// declares it per connector and is derived from the implementation. An
// UNDECLARED connector is treated as unbounded - the safe assumption,
// since guessing "bounded" would silently disable the gate for exactly the
// connectors nobody has characterised yet.
//
// Retention is read from any participating table's `state_ttl` WITH
// option. `allow_unbounded` is the statement's ALLOW UNBOUNDED STATE flag.
[[nodiscard]] BoundedStateReport check_plan_bounded_state(const LogicalPlan& plan,
                                                          bool allow_unbounded);

// True when this node kind retains per-key state for the life of the job.
// Exposed so EXPLAIN can annotate the same nodes the gate looks at.
[[nodiscard]] bool retains_unbounded_state(const std::string& node_kind);

}  // namespace clink::sql
