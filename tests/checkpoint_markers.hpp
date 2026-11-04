// Checkpoint markers, read the way the coordinator reads them.
//
// A coordinator records a checkpoint's completion as COMPLETED-<id>, and the
// confirmation of its commits as CONFIRMED-<id>, under
// <checkpoint dir>/_jobs/<job>. A marker is published by renaming a temp file,
// <marker>.tmp.<pid>.<n>, into place, so a directory can hold a name that starts
// like a marker and is not one yet. The coordinator counts only the prefix
// followed by digits and nothing else (latest_marker_id_on_disk). std::stoull,
// std::strtoull and std::from_chars all read "21.tmp.74715.0" as 21: a test that
// parsed names that way once waited for checkpoint 21, saw its temp file, killed
// the leader before the rename, and found the takeover restoring checkpoint 20.
// Every test reads marker names through marker_id, so each counts what the
// coordinator counts.
#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace clink::test_markers {

// The checkpoint id a file name carries when it is a whole marker: `prefix`,
// then digits to the end. Any other name, a marker's temp file among them, is
// no marker. With an empty prefix it reads a numeric tail, such as the id that
// ends a commit receipt's sub<subtask>-<checkpoint>.
inline std::optional<std::uint64_t> marker_id(std::string_view name, std::string_view prefix) {
    if (!name.starts_with(prefix)) {
        return std::nullopt;
    }
    const auto digits = name.substr(prefix.size());
    const auto* last = digits.data() + digits.size();
    std::uint64_t id = 0;
    const auto [end, err] = std::from_chars(digits.data(), last, id);
    if (err != std::errc{} || end != last) {
        return std::nullopt;
    }
    return id;
}

}  // namespace clink::test_markers
