#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>

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

// Derive the stable OperatorId for a user-assigned `.uid("...")`. The
// runtime (Dag) uses this to key keyed-state and to stamp state-version
// metadata; the SQL/API env uses the same transform to record a job's
// expected state versions, so both sides agree on the key. std::hash is
// only "stable across runs in the same process build", which is
// sufficient here: the plugin ABI gate pins the same toolchain + commit
// across the submit and deploy processes that must agree.
inline OperatorId operator_id_from_uid(std::string_view uid) {
    const std::string keyed = "uid/" + std::string{uid};
    const std::uint64_t h = std::hash<std::string>{}(keyed);
    return OperatorId{h == 0 ? std::uint64_t{1} : h};
}

}  // namespace clink

namespace std {

template <typename Tag>
struct hash<clink::detail::StrongId<Tag>> {
    std::size_t operator()(const clink::detail::StrongId<Tag>& id) const noexcept {
        return std::hash<typename clink::detail::StrongId<Tag>::value_type>{}(id.value());
    }
};

}  // namespace std
