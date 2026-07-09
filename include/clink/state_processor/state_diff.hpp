#pragma once

// State diff - compare the keyed state of two savepoints/checkpoints.
//
// The time-travel debugging primitive: given two snapshots of a job
// (checkpoint N and checkpoint M, or a savepoint before and after an
// incident), report exactly which keys appeared, vanished, or changed in
// every operator's every state slot, plus any state-schema version-stamp
// changes. Built on the state_processor Savepoint (in-memory materialise +
// scan), so it works on any InMemoryStateBackend-format `.snap` blob - the
// format FileBackedStateBackend writes for checkpoints and savepoints.
//
// Layered as a library so the CLI (`clink state-diff` / `clink state-cat`)
// is a thin printer over it and later time-travel tooling (record-level
// explain, deterministic replay) can reuse the same collected-entry model.
//
// Entry collection preserves the stored-key structure documented in
// keyed_state.hpp - [key_group byte][slot]['|'][user-key bytes] - and
// renders user keys and values readably: an 8-byte user key doubles as its
// little-endian int64 reading (the built-in int64 codec layout), printable
// bytes render as text, anything else as hex.

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state_processor/savepoint.hpp"

namespace clink::state_processor {

// One keyed-state entry in structured form: where it lives (operator, key
// group, slot) plus the raw user-key and value bytes.
struct StateEntry {
    std::uint8_t key_group{};
    std::string user_key;  // raw bytes after the '|' separator
    std::string value;     // raw codec bytes
};

// Every entry of one savepoint, grouped op -> slot -> (user_key -> entry).
// Ordered maps keep iteration (and therefore every report) deterministic.
using SlotEntries = std::map<std::string, StateEntry>;
using StateEntries = std::map<OperatorId, std::map<std::string, SlotEntries>>;

// Scan a savepoint's backing store into the structured model. Entries whose
// stored key does not parse as a KeyedState key (no '|' separator) are
// surfaced under the reserved slot name "<raw>" with the whole stored key as
// the user key, so nothing is silently dropped.
inline StateEntries collect_entries(const Savepoint& sp) {
    StateEntries out;
    auto& backend = const_cast<Savepoint&>(sp).backend();
    for (const auto op : sp.operators()) {
        backend.scan(op, [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            StateEntry e;
            std::string slot;
            const auto pipe = k.size() >= 2 ? k.find('|', 1) : std::string_view::npos;
            if (pipe == std::string_view::npos) {
                slot = "<raw>";
                e.user_key = std::string{k};
            } else {
                e.key_group = static_cast<std::uint8_t>(k[0]);
                slot = std::string{k.substr(1, pipe - 1)};
                e.user_key = std::string{k.substr(pipe + 1)};
            }
            e.value = std::string{v};
            out[op][slot][e.user_key] = std::move(e);
        });
    }
    return out;
}

// Merge entries collected from several savepoint files into one model - the
// multi-subtask case (a checkpoint directory holds one .snap per subtask;
// key groups are disjoint across subtasks so the union is collision-free;
// on a collision the later file wins).
inline void merge_entries(StateEntries& into, StateEntries&& from) {
    for (auto& [op, slots] : from) {
        for (auto& [slot, entries] : slots) {
            for (auto& [key, entry] : entries) {
                into[op][slot][key] = std::move(entry);
            }
        }
    }
}

// One changed/added/removed entry in the diff, with both sides' values
// (empty string for the missing side; IsEmptyValue disambiguates a genuine
// empty value from absence).
struct EntryDelta {
    enum class Kind { Added, Removed, Changed };
    Kind kind{};
    std::uint8_t key_group{};
    std::string user_key;
    std::string value_a;  // empty when Added
    std::string value_b;  // empty when Removed
};

// Per-(operator, slot) diff summary plus up to `max_samples` concrete
// deltas (counts always cover everything; samples are for display).
struct SlotDiff {
    OperatorId op{};
    std::string slot;
    std::size_t added{};
    std::size_t removed{};
    std::size_t changed{};
    std::size_t unchanged{};
    std::vector<EntryDelta> samples;
};

// A state-schema version-stamp change between the two sides.
struct VersionDelta {
    OperatorId op{};
    std::string state_type;
    std::uint32_t version_a{};  // 0 = absent on that side
    std::uint32_t version_b{};
};

struct StateDiffReport {
    std::vector<SlotDiff> slots;  // only slots with at least one delta
    std::vector<VersionDelta> versions;
    std::size_t total_entries_a{};
    std::size_t total_entries_b{};
    [[nodiscard]] bool identical() const { return slots.empty() && versions.empty(); }
};

// Diff two collected-entry models. `max_samples` bounds the per-slot delta
// list embedded in the report (0 = keep every delta).
inline StateDiffReport diff_entries(const StateEntries& a,
                                    const StateEntries& b,
                                    std::size_t max_samples = 20) {
    StateDiffReport report;
    auto count_all = [](const StateEntries& s) {
        std::size_t n = 0;
        for (const auto& [op, slots] : s) {
            for (const auto& [slot, entries] : slots) {
                n += entries.size();
            }
        }
        return n;
    };
    report.total_entries_a = count_all(a);
    report.total_entries_b = count_all(b);

    // Union of (op, slot) across both sides, in deterministic order.
    std::map<OperatorId, std::map<std::string, int>> keys;
    for (const auto& [op, slots] : a) {
        for (const auto& [slot, _] : slots) {
            keys[op][slot] = 1;
        }
    }
    for (const auto& [op, slots] : b) {
        for (const auto& [slot, _] : slots) {
            keys[op][slot] = 1;
        }
    }

    static const SlotEntries kEmpty;
    auto slot_of =
        [](const StateEntries& s, OperatorId op, const std::string& slot) -> const SlotEntries& {
        auto oit = s.find(op);
        if (oit == s.end()) {
            return kEmpty;
        }
        auto sit = oit->second.find(slot);
        return sit == oit->second.end() ? kEmpty : sit->second;
    };
    auto want_sample = [max_samples](const SlotDiff& d) {
        return max_samples == 0 || d.samples.size() < max_samples;
    };

    for (const auto& [op, slots] : keys) {
        for (const auto& [slot, _] : slots) {
            const auto& ea = slot_of(a, op, slot);
            const auto& eb = slot_of(b, op, slot);
            SlotDiff d;
            d.op = op;
            d.slot = slot;
            // Two-pointer walk over the ordered maps.
            auto ia = ea.begin();
            auto ib = eb.begin();
            while (ia != ea.end() || ib != eb.end()) {
                if (ib == eb.end() || (ia != ea.end() && ia->first < ib->first)) {
                    ++d.removed;
                    if (want_sample(d)) {
                        d.samples.push_back(EntryDelta{.kind = EntryDelta::Kind::Removed,
                                                       .key_group = ia->second.key_group,
                                                       .user_key = ia->first,
                                                       .value_a = ia->second.value,
                                                       .value_b = {}});
                    }
                    ++ia;
                } else if (ia == ea.end() || ib->first < ia->first) {
                    ++d.added;
                    if (want_sample(d)) {
                        d.samples.push_back(EntryDelta{.kind = EntryDelta::Kind::Added,
                                                       .key_group = ib->second.key_group,
                                                       .user_key = ib->first,
                                                       .value_a = {},
                                                       .value_b = ib->second.value});
                    }
                    ++ib;
                } else {
                    if (ia->second.value != ib->second.value) {
                        ++d.changed;
                        if (want_sample(d)) {
                            d.samples.push_back(EntryDelta{.kind = EntryDelta::Kind::Changed,
                                                           .key_group = ia->second.key_group,
                                                           .user_key = ia->first,
                                                           .value_a = ia->second.value,
                                                           .value_b = ib->second.value});
                        }
                    } else {
                        ++d.unchanged;
                    }
                    ++ia;
                    ++ib;
                }
            }
            if (d.added + d.removed + d.changed > 0) {
                report.slots.push_back(std::move(d));
            }
        }
    }
    return report;
}

// Diff the state-schema version stamps carried in each savepoint's Arrow
// schema metadata (see schema_version.hpp). Reported per (op, state_type)
// where the two sides disagree or one side lacks the stamp.
inline std::vector<VersionDelta> diff_versions(const Savepoint& a, const Savepoint& b) {
    auto va = const_cast<Savepoint&>(a).backend().restored_state_versions();
    auto vb = const_cast<Savepoint&>(b).backend().restored_state_versions();
    std::map<std::pair<std::uint64_t, std::string>, std::pair<std::uint32_t, std::uint32_t>> merged;
    for (const auto& e : va.entries()) {
        merged[{e.op_id.value(), e.state_type}].first = e.version;
    }
    for (const auto& e : vb.entries()) {
        merged[{e.op_id.value(), e.state_type}].second = e.version;
    }
    std::vector<VersionDelta> out;
    for (const auto& [key, versions] : merged) {
        if (versions.first != versions.second) {
            out.push_back(VersionDelta{.op = OperatorId{key.first},
                                       .state_type = key.second,
                                       .version_a = versions.first,
                                       .version_b = versions.second});
        }
    }
    return out;
}

// ---- display helpers (shared by the CLI and any other front end) ----------

// Render raw bytes readably: an 8-byte buffer additionally shows its
// little-endian int64 reading (the built-in int64 codec layout); printable
// bytes render as text; anything else as hex. Truncated to `max_len` bytes
// of source with a trailing ellipsis marker.
inline std::string render_bytes(const std::string& bytes, std::size_t max_len = 64) {
    const bool truncated = bytes.size() > max_len;
    const std::string_view view{bytes.data(), truncated ? max_len : bytes.size()};
    bool printable = !view.empty();
    for (const char c : view) {
        if (static_cast<unsigned char>(c) < 0x20 || static_cast<unsigned char>(c) > 0x7E) {
            printable = false;
            break;
        }
    }
    std::string out;
    if (printable) {
        out = "\"" + std::string{view} + "\"";
    } else {
        static const char* kHex = "0123456789abcdef";
        out = "0x";
        for (const char c : view) {
            const auto u = static_cast<unsigned char>(c);
            out += kHex[u >> 4];
            out += kHex[u & 0x0F];
        }
    }
    if (bytes.size() == 8) {
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[i])) << (i * 8);
        }
        out += " (int64 " + std::to_string(static_cast<std::int64_t>(u)) + ")";
    }
    if (truncated) {
        out += "... [" + std::to_string(bytes.size()) + " bytes]";
    }
    return out;
}

}  // namespace clink::state_processor
