#include "clink/state/state_migration_on_restore.hpp"

#include <charconv>
#include <span>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace clink {

std::vector<StateIncompatibility> check_restore_compatibility(const StateVersionMap& stored,
                                                              const StateVersionMap& expected,
                                                              const StateMigrationRegistry& reg) {
    std::vector<StateIncompatibility> out;
    for (const auto& e : expected.entries()) {
        const std::uint32_t from = stored.get(e.op_id, e.state_type).value_or(1);
        const std::uint32_t to = e.version;
        if (from == to) {
            continue;  // already at the expected version
        }
        if (!reg.has_path(e.state_type, from, to)) {
            out.push_back(StateIncompatibility{e.op_id, e.state_type, from, to});
        }
    }
    return out;
}

void migrate_restored_state(StateBackend& backend,
                            const StateVersionMap& expected,
                            const StateMigrationRegistry& reg) {
    const StateVersionMap stored = backend.restored_state_versions();
    for (const auto& e : expected.entries()) {
        const std::uint32_t from = stored.get(e.op_id, e.state_type).value_or(1);
        const std::uint32_t to = e.version;
        if (from == to) {
            continue;
        }
        if (!reg.has_path(e.state_type, from, to)) {
            throw std::runtime_error("migrate_restored_state: no migration path for op=" +
                                     std::to_string(e.op_id.value()) + " state_type='" +
                                     e.state_type + "' from v" + std::to_string(from) + " to v" +
                                     std::to_string(to));
        }
        // Collect first - the StateBackend scan contract forbids
        // mutating during the visit - then write the migrated values
        // back. migrate() is pure (registry-only), so calling it inside
        // the visitor is safe; if it throws (e.g. an Arrow schema
        // mismatch) nothing is written, keeping the op's state
        // all-or-nothing.
        std::vector<std::pair<std::string, std::vector<std::byte>>> migrated;
        backend.scan(e.op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
            // Slot-aware filter. Stored-key layout (KeyedState::encode_key)
            // is [kg_byte][slot_name]['|'][user_key_bytes]. When this entry
            // names a slot, migrate ONLY that slot's values so a sibling
            // slot under the same operator stays byte-identical. The '|'
            // sentinel check stops "buf" from matching "buf2". An empty
            // slot means "every value under the op" - the single-slot /
            // legacy behaviour and the back-compatible default.
            if (!e.slot.empty()) {
                if (key.size() < 1 + e.slot.size() + 1) {
                    return;
                }
                if (key.compare(1, e.slot.size(), e.slot) != 0 || key[1 + e.slot.size()] != '|') {
                    return;
                }
            }
            const std::span<const std::byte> in{reinterpret_cast<const std::byte*>(value.data()),
                                                value.size()};
            migrated.emplace_back(std::string{key}, reg.migrate(e.state_type, from, to, in));
        });
        for (const auto& [key, val] : migrated) {
            backend.put(e.op_id,
                        key,
                        std::string_view{reinterpret_cast<const char*>(val.data()), val.size()});
        }
    }
    // Re-stamp so the next snapshot records the migrated versions and a
    // re-restore from it needs no further migration. MERGE (not replace)
    // onto the stored map: a job that bumps only one slot declares only
    // that slot in `expected`, so a plain replace would DROP the stamps
    // of sibling slots/types the current generation left unchanged - and
    // a later restore would then read their version as the v1 default and
    // wrongly re-migrate already-current data. Merging keeps every stored
    // stamp and overwrites only the entries this run migrated.
    StateVersionMap restamped = stored;
    for (const auto& e : expected.entries()) {
        restamped.set(e.op_id, e.state_type, e.version, e.slot);
    }
    backend.set_state_versions(std::move(restamped));
}

std::string pack_incompatibilities(const std::vector<StateIncompatibility>& incompat) {
    std::ostringstream oss;
    bool first = true;
    for (const auto& e : incompat) {
        if (!first)
            oss << '\n';
        oss << e.op_id.value() << '|' << e.state_type << '|' << e.from_version << '|'
            << e.to_version;
        first = false;
    }
    return oss.str();
}

std::vector<StateIncompatibility> unpack_incompatibilities(std::string_view packed) {
    std::vector<StateIncompatibility> out;
    if (packed.empty())
        return out;

    std::size_t start = 0;
    while (start < packed.size()) {
        auto nl = packed.find('\n', start);
        std::string_view line =
            packed.substr(start, nl == std::string_view::npos ? packed.size() - start : nl - start);
        if (line.empty()) {
            // Skip blank lines defensively (e.g., a trailing newline).
            start = nl == std::string_view::npos ? packed.size() : nl + 1;
            continue;
        }
        auto p1 = line.find('|');
        auto p2 = p1 == std::string_view::npos ? std::string_view::npos : line.find('|', p1 + 1);
        auto p3 = p2 == std::string_view::npos ? std::string_view::npos : line.find('|', p2 + 1);
        if (p1 == std::string_view::npos || p2 == std::string_view::npos ||
            p3 == std::string_view::npos) {
            throw std::runtime_error("unpack_incompatibilities: expected 3 '|' separators in line");
        }
        std::string_view op_str = line.substr(0, p1);
        std::string_view type_str = line.substr(p1 + 1, p2 - p1 - 1);
        std::string_view from_str = line.substr(p2 + 1, p3 - p2 - 1);
        std::string_view to_str = line.substr(p3 + 1);

        std::uint64_t op_val = 0;
        auto [op_end, op_ec] =
            std::from_chars(op_str.data(), op_str.data() + op_str.size(), op_val);
        if (op_ec != std::errc{} || op_end != op_str.data() + op_str.size()) {
            throw std::runtime_error("unpack_incompatibilities: invalid op_id");
        }
        std::uint32_t from_val = 0;
        auto [from_end, from_ec] =
            std::from_chars(from_str.data(), from_str.data() + from_str.size(), from_val);
        if (from_ec != std::errc{} || from_end != from_str.data() + from_str.size()) {
            throw std::runtime_error("unpack_incompatibilities: invalid from_version");
        }
        std::uint32_t to_val = 0;
        auto [to_end, to_ec] =
            std::from_chars(to_str.data(), to_str.data() + to_str.size(), to_val);
        if (to_ec != std::errc{} || to_end != to_str.data() + to_str.size()) {
            throw std::runtime_error("unpack_incompatibilities: invalid to_version");
        }
        out.push_back(
            StateIncompatibility{OperatorId{op_val}, std::string{type_str}, from_val, to_val});

        if (nl == std::string_view::npos)
            break;
        start = nl + 1;
    }
    return out;
}

}  // namespace clink
