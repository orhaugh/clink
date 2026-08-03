#pragma once

// "Did you mean ...?" for a name that was not recognised.
//
// A rejection a user cannot act on is barely better than silence, and the
// commonest reason a name is unrecognised is a typo. Two places need this
// - the table-option checker and the function-name check in the binder -
// so the edit distance lives here rather than being written twice.

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace clink::sql {

// Levenshtein distance, capped: callers only care whether the distance is
// small, so bail out as soon as it cannot be.
[[nodiscard]] inline std::size_t edit_distance(std::string_view a,
                                               std::string_view b,
                                               std::size_t cap) {
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

// The closest candidate to `name`, or empty when nothing is close enough
// to be worth suggesting.
//
// The cap scales with length: one edit in a four-character name is as
// likely to be a different name as a typo, whereas two edits in
// `regexp_replace` is not. Suggesting confidently and wrongly is worse
// than not suggesting.
template <typename Range>
[[nodiscard]] std::string nearest_name(std::string_view name, const Range& candidates) {
    const std::size_t cap = name.size() >= 12 ? 2 : (name.size() >= 5 ? 1 : 0);
    if (cap == 0) {
        return {};
    }
    std::string best;
    std::size_t best_distance = cap + 1;
    for (const auto& candidate : candidates) {
        const std::string_view c{candidate};
        if (const auto d = edit_distance(name, c, cap); d < best_distance) {
            best_distance = d;
            best = std::string{c};
        }
    }
    return best_distance <= cap ? best : std::string{};
}

}  // namespace clink::sql
