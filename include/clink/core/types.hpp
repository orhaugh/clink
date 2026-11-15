#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace clink {

// Strong-typed identifiers. We deliberately avoid implicit conversions to keep
// operator/partition/checkpoint identifiers from being silently mixed up.
namespace detail {

template <typename Tag>
class StrongId {
public:
    using value_type = std::uint64_t;

    constexpr StrongId() = default;
    constexpr explicit StrongId(value_type v) noexcept : value_(v) {}

    constexpr value_type value() const noexcept { return value_; }

    constexpr auto operator<=>(const StrongId&) const = default;

private:
    value_type value_{0};
};

template <typename Tag>
inline std::ostream& operator<<(std::ostream& os, const StrongId<Tag>& id) {
    return os << id.value();
}

}  // namespace detail

struct OperatorIdTag {};
struct PartitionIdTag {};
struct CheckpointIdTag {};

using OperatorId = detail::StrongId<OperatorIdTag>;
using PartitionId = detail::StrongId<PartitionIdTag>;
using CheckpointId = detail::StrongId<CheckpointIdTag>;

// The hash every operator id is derived from: FNV-1a 64 over a domain-tagged
// string ("uid/<uid>", "node/<graph node id>", "stage<idx>/<name>"), 0 mapped
// to 1 (0 is the default-constructed sentinel). Frozen: state is stored under
// these ids, so a change to this function orphans every checkpoint ever taken.
// Unlike std::hash it is the same in every build, on every platform and with
// either standard library, so a checkpoint restores wherever clink runs. Inline
// because job plugins compute ids in their own code (fluent expected-version
// declarations), and they link no clink library that could supply it.
inline constexpr std::uint64_t stable_operator_id_hash(std::string_view tagged) noexcept {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    for (const char c : tagged) {
        h ^= static_cast<std::uint8_t>(c);
        h *= 0x100000001b3ULL;
    }
    return h == 0 ? std::uint64_t{1} : h;
}

inline OperatorId stable_operator_id(std::string_view tagged) {
    return OperatorId{stable_operator_id_hash(tagged)};
}

// Which derivation a body of state's operator ids came from. Legacy ids are
// std::hash of the id string, as every build derived them until the stable
// ids (include/clink/core/legacy_operator_ids.hpp computes both libraries'
// values); Stable ids are stable_operator_id_hash. Snapshots record it, so a
// restore knows whether the state it reads needs its ids moved.
enum class OperatorIdScheme : std::uint8_t { Legacy = 1, Stable = 2 };

// The scheme this build derives operator ids in, and so the scheme of state a
// fresh backend accumulates.
inline constexpr OperatorIdScheme kCurrentOperatorIdScheme = OperatorIdScheme::Stable;

// State stored under operator id `from` that belongs to the operator now
// called `to`: the id an older build derived for an operator, and the id this
// build derives (include/clink/core/legacy_operator_ids.hpp).
struct OperatorIdAlias {
    OperatorId from{};
    OperatorId to{};
};
using OperatorIdAliases = std::vector<OperatorIdAlias>;

// What a snapshot records of each operator whose id it holds, so a reader can
// name an operator without the job graph that took it: its uid (empty when it
// has none) and its node id in the job graph.
struct OperatorIdentity {
    std::string uid;
    std::string node;
};
using OperatorIdentities = std::map<OperatorId, OperatorIdentity>;

// Derive the OperatorId for a user-assigned `.uid("...")`: the stable hash of
// "uid/<uid>". The runtime (Dag) uses it to key keyed state and to stamp
// state-version metadata; the SQL/API env uses the same transform to record a
// job's expected state versions, so both sides agree on the key. Builds up to
// v0.15.0 hashed the same string with std::hash, which libc++ and libstdc++
// compute differently; legacy_operator_ids.hpp computes those ids, and a
// restore moves state found under them to this one.
inline OperatorId operator_id_from_uid(std::string_view uid) {
    return stable_operator_id("uid/" + std::string{uid});
}

// Which of the previous run's subtasks a subtask succeeds after a restore, for
// state that must be acted on exactly once: a two-phase-commit sink's prepared
// transactions, which exactly one successor may commit or roll back. Every
// previous subtask has exactly one successor. Without a rescale a subtask
// succeeds itself. One that inherits several parents (a scale-down) succeeds
// all of them. Of the subtasks that share one parent (a scale-up) only the
// first succeeds it, and the others succeed none (`count` 0). Indices are the
// operator's own subtask indices. `generation` is the topology generation this
// run writes and `restore_generation` the one that wrote the restored
// checkpoint; they differ when the restore crosses a rescale.
struct RestoreSuccession {
    std::uint32_t first{0};
    std::uint32_t count{1};
    std::uint32_t generation{1};
    std::uint32_t restore_generation{1};

    [[nodiscard]] bool succeeds(std::uint32_t previous_subtask) const noexcept {
        return previous_subtask >= first && previous_subtask - first < count;
    }
};

}  // namespace clink

namespace std {

template <typename Tag>
struct hash<clink::detail::StrongId<Tag>> {
    std::size_t operator()(const clink::detail::StrongId<Tag>& id) const noexcept {
        return std::hash<typename clink::detail::StrongId<Tag>::value_type>{}(id.value());
    }
};

}  // namespace std
