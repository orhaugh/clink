#include "clink/sql/table_option_check.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "clink/connectors/capability.hpp"
#include "clink/sql/bounded_state.hpp"

namespace clink::sql {

namespace {

// Levenshtein, capped: we only care whether the distance is small, so bail
// out as soon as it cannot be.
std::size_t edit_distance(std::string_view a, std::string_view b, std::size_t cap) {
    if (a.size() > b.size() + cap || b.size() > a.size() + cap) {
        return cap + 1;
    }
    std::vector<std::size_t> prev(b.size() + 1);
    std::vector<std::size_t> cur(b.size() + 1);
    for (std::size_t j = 0; j <= b.size(); ++j) {
        prev[j] = j;
    }
    for (std::size_t i = 1; i <= a.size(); ++i) {
        cur[0] = i;
        std::size_t row_min = cur[0];
        for (std::size_t j = 1; j <= b.size(); ++j) {
            const auto cost = a[i - 1] == b[j - 1] ? 0U : 1U;
            cur[j] = std::min({prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost});
            row_min = std::min(row_min, cur[j]);
        }
        if (row_min > cap) {
            return cap + 1;
        }
        std::swap(prev, cur);
    }
    return prev[b.size()];
}

// A value domain for an option that only accepts a fixed set. Silently
// accepting `mode='upsrt'` would leave an append-only sink where the user
// asked for upsert, with the same class of consequence as a misspelt key.
struct ClosedDomain {
    const char* option;
    std::vector<std::string> allowed;
};

const std::vector<ClosedDomain>& closed_domains() {
    static const std::vector<ClosedDomain> domains = {
        // append / upsert / cdc. The first cut listed only the first two
        // and rejected every CDC table in the suite - a reminder that a
        // closed-domain list is only safe when it was READ off the code
        // rather than assumed. These were: physical_plan.cpp's `== "cdc"`
        // sites and TableDef::mode()'s "append" default.
        {"mode", {"append", "upsert", "cdc"}},
        {"state_ttl_domain", {"event_time", "processing_time"}},
        {"changelog", {"true", "false"}},
        {"columnar_decode", {"true", "false"}},
    };
    return domains;
}

}  // namespace

const std::vector<std::string>& interpreted_table_options() {
    // Read off the sites in the SQL frontend that consult a table's
    // properties. A new interpreted option MUST be added here, or a
    // misspelling of it becomes silent again - which is the whole failure
    // this file exists to prevent.
    static const std::vector<std::string> opts = {
        "connector",
        "format",
        "mode",
        "primary_key",
        "delivery_guarantee",
        "commit_group",
        "changelog",
        "state_ttl",
        "state_ttl_domain",
        "columnar_decode",
        "event_time_column",
        "watermark_delay_ms",
        "late_records_to_dlq",
        "freshness",
        "bounded",
    };
    return opts;
}

std::vector<TableOptionProblem> check_table_options(
    const std::string& table_name, const std::map<std::string, std::string>& properties) {
    std::vector<TableOptionProblem> problems;
    const auto& known = interpreted_table_options();

    for (const auto& [key, value] : properties) {
        // Exact match: not a typo. Check its value domain instead.
        if (std::find(known.begin(), known.end(), key) != known.end()) {
            for (const auto& d : closed_domains()) {
                if (key != d.option) {
                    continue;
                }
                if (std::find(d.allowed.begin(), d.allowed.end(), value) != d.allowed.end()) {
                    continue;
                }
                std::string allowed;
                for (std::size_t i = 0; i < d.allowed.size(); ++i) {
                    allowed += (i > 0 ? ", '" : "'") + d.allowed[i] + "'";
                }
                problems.push_back(
                    {key,
                     "table '" + table_name + "': option '" + key + "' has value '" + value +
                         "', which is not one of " + allowed +
                         ". An unrecognised value is ignored, so the table would silently "
                         "behave as if the option had not been set."});
            }
            // delivery_guarantee has its own vocabulary, owned by the
            // connector capability contract rather than a literal list
            // here, so the two cannot drift apart.
            if (key == "delivery_guarantee" &&
                !connectors::delivery_from_string(value).has_value()) {
                problems.push_back(
                    {key,
                     "table '" + table_name + "': delivery_guarantee='" + value +
                         "' is not a recognised guarantee. Valid values are "
                         "'at_most_once', 'at_least_once', "
                         "'effectively_once_idempotent', "
                         "'exactly_once_atomic_publish', "
                         "'exactly_once_two_phase_commit' (or the legacy spelling "
                         "'exactly_once'). An unrecognised value would leave the sink at "
                         "its default guarantee with no warning."});
            }
            // state_ttl must parse. A mistyped retention that silently
            // became "no retention" would defeat the bounded-state gate,
            // which is exactly what it is there to prevent.
            if (key == "state_ttl") {
                try {
                    if (parse_retention_ms(value) <= 0) {
                        problems.push_back({key,
                                            "table '" + table_name + "': state_ttl='" + value +
                                                "' resolves to no retention."});
                    }
                } catch (const std::exception& e) {
                    problems.push_back({key, "table '" + table_name + "': " + e.what()});
                }
            }
            continue;
        }

        // Not an interpreted option. Is it a near-miss of one? Anything
        // further away is assumed to be connector passthrough and left
        // alone - the option space out there is open-ended and not ours.
        //
        // Distance 1 for short names, 2 for longer ones: one edit in a
        // six-character word is as likely to be a different option as a
        // typo, whereas two edits in `delivery_guarantee` is not.
        for (const auto& candidate : known) {
            const std::size_t cap = candidate.size() >= 12 ? 2 : 1;
            if (edit_distance(key, candidate, cap) <= cap) {
                problems.push_back(
                    {key,
                     "table '" + table_name + "': unknown option '" + key +
                         "' looks like a misspelling of '" + candidate +
                         "', which clink interprets. An unrecognised option is passed "
                         "through to the connector and otherwise ignored, so the setting "
                         "you intended would have no effect and no diagnostic. Correct "
                         "the spelling, or rename the option if it really is meant for "
                         "the connector."});
                break;
            }
        }
    }
    return problems;
}

}  // namespace clink::sql
