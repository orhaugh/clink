#pragma once

// Validation for the table WITH-options clink itself interprets.
//
// A DDL WITH clause carries two kinds of option: ones clink reads and acts
// on (`delivery_guarantee`, `primary_key`, `mode`, `state_ttl`, ...), and
// ones it passes straight through to a connector (`bootstrap_servers`,
// `topic`, `path`, and whatever else that connector wants). The two need
// opposite treatment, which is why this is not a simple allowlist:
//
//   * The passthrough set is open-ended and connector-specific. Rejecting
//     an unrecognised option would break every connector that accepts
//     something clink has never heard of - which is most of them.
//
//   * The interpreted set is small, closed, and semantically loaded. A
//     typo there is silent and consequential: `delivery_gurantee=
//     'exactly_once'` reads as an unknown passthrough option, the sink
//     stays at-least-once, and nothing says a word. So does
//     `primary_keys='id'` on an upsert sink, which leaves the sink with
//     no key and voids the effectively-once guarantee its capability
//     record advertises.
//
// So: near-miss detection on the interpreted set, plus value validation
// for the ones with a closed value domain. An option that is nowhere near
// a known name is assumed to be connector passthrough and left alone.
//
// This deliberately cannot catch a typo that happens to look like a
// plausible connector option. It catches the ones that look like a
// misspelling of something clink acts on, which is where the silent
// semantic change lives.

#include <map>
#include <string>
#include <vector>

namespace clink::sql {

struct TableOptionProblem {
    std::string option;   // the offending key as written
    std::string message;  // full diagnostic, including the suggestion
};

// Every option key clink itself reads from a table's WITH clause. Adding
// a new interpreted option means adding it here, or a misspelling of it
// becomes silent again.
[[nodiscard]] const std::vector<std::string>& interpreted_table_options();

// Check one table's options. Returns a problem per offending key: a
// near-miss of an interpreted option, or an interpreted option with a
// value outside its closed domain. Empty means nothing to say.
[[nodiscard]] std::vector<TableOptionProblem> check_table_options(
    const std::string& table_name, const std::map<std::string, std::string>& properties);

}  // namespace clink::sql
