#pragma once

#include <string>
#include <string_view>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

// Changelog convention for clink::sql::Row.
//
// Operators that produce changelog streams (TOP-N-per-key,
// future retracting aggregates, CDC-style sources) tag each emitted
// Row with a synthetic "__row_kind" field whose value is one of
// kRowKindInsert / kRowKindDelete (extendable to update before/after
// when we wire UPDATE retraction).
//
// Rules:
//   - Records without a __row_kind value are implicit inserts; pure
//     append streams don't need to mark anything.
//   - Pass-through operators that copy the entire Row.values map
//     (filter_row_predicate, union_row, identity_row, row_compute_key)
//     preserve __row_kind automatically.
//   - project_row picks specific output columns, so it special-cases
//     __row_kind: if the input carries one, the output Row carries
//     the same value even when it isn't in the declared outputs list.
//   - Sinks pass __row_kind through to the encoded wire format
//     today; a future upsert-aware sink layer can consume it.

namespace clink::sql {

// Field name used on Row.values to carry the changelog kind. Starts
// with the same double-underscore prefix as the other privileged
// synthetic field (__key) so the value never collides with a
// user-declared SQL column.
inline constexpr std::string_view kRowKindField = "__row_kind";

// Canonical kind values. update_before / update_after let producers
// emit a single in-flight update as a pair instead
// of decomposing to delete + insert. Sinks that key by primary
// key (upsert sinks) treat update_after as overwrite and drop
// update_before; sinks that natively support UPDATE statements can
// coalesce the pair into one round trip.
inline constexpr std::string_view kRowKindInsert = "insert";
inline constexpr std::string_view kRowKindDelete = "delete";
inline constexpr std::string_view kRowKindUpdateBefore = "update_before";
inline constexpr std::string_view kRowKindUpdateAfter = "update_after";

// Classify a kind string. Useful for the sink + aggregate dispatch
// without having to compare against every constant.
inline bool is_insert_like(std::string_view kind) {
    return kind == kRowKindInsert || kind == kRowKindUpdateAfter;
}
inline bool is_delete_like(std::string_view kind) {
    return kind == kRowKindDelete || kind == kRowKindUpdateBefore;
}

// The marker's interned handle, resolved once. Every helper below runs per
// record, and each used to build a throwaway std::string from a string_view just
// to probe a map that accepts a string_view.
inline clink::config::InternedName row_kind_name() {
    static const clink::config::InternedName n{kRowKindField};
    return n;
}

inline bool has_row_kind(const Row& row) {
    auto it = row.values.find(kRowKindField);
    return it != row.values.end() && it->second.is_string();
}

inline std::string row_kind_of(const Row& row) {
    auto it = row.values.find(kRowKindField);
    if (it == row.values.end() || !it->second.is_string()) {
        return std::string{kRowKindInsert};  // unmarked records are inserts
    }
    return it->second.as_string();
}

inline void set_row_kind(Row& row, std::string_view kind) {
    row.values[row_kind_name()] = clink::config::JsonValue{std::string{kind}};
}

// Copy a __row_kind value from `src` onto `dst` if `src` carries one.
// Used by project_row to preserve the privileged field even when the
// user's projection doesn't include it in its outputs.
inline void copy_row_kind(const Row& src, Row& dst) {
    auto it = src.values.find(kRowKindField);
    if (it != src.values.end() && it->second.is_string()) {
        dst.values[row_kind_name()] = it->second;
    }
}

}  // namespace clink::sql
