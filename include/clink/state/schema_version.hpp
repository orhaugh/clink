#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/core/types.hpp"

// Forward declarations to avoid pulling Arrow headers into widely-included
// downstream code. The implementation file pulls Arrow proper.
namespace arrow {
class Schema;
}

namespace clink {

// Compile-time schema version trait. Users specialise per type to bump
// the version on a breaking shape change to the state value. The
// engine reads this trait at snapshot time to stamp each (operator,
// slot) with the version of its codec, and at restore time to decide
// whether a stored snapshot needs migrating before it can be re-fed
// into the live operator.
//
// Default = 1. Bumping is the user's signal that "the bytes I write
// today are not byte-compatible with the bytes I wrote yesterday."
//
// Example:
//   struct UserCounterV2 { int64_t count; int64_t last_seen_ms; };
//   template <> struct SchemaVersionTrait<UserCounterV2> {
//       static constexpr std::uint32_t value = 2;
//   };
template <typename T>
struct SchemaVersionTrait {
    static constexpr std::uint32_t value = 1;
};

template <typename T>
inline constexpr std::uint32_t schema_version_v = SchemaVersionTrait<T>::value;

// A single (operator, state_type) -> version binding. Recorded in the
// snapshot at write time so a future restore can compare against the
// live job's expected version and decide whether to migrate.
//
// state_type is a free-form tag the user chooses; convention is to
// match the string used when registering migrations with
// StateMigrationRegistry (typically the typed name of T plus an
// optional slot suffix). It must not contain '\n' or '|' since the
// engine packs entries into a single Arrow-IPC schema metadata value
// with those delimiters.
//
// slot is the keyed-state slot name this version applies to (the name
// passed to RuntimeContext::keyed_state). It is the backend-key prefix
// the migrator filters on so that, when an operator has MORE THAN ONE
// keyed-state slot, bumping one slot's version migrates only that slot's
// values and leaves the others byte-identical. Empty slot means
// "every value under the operator" - the single-slot / legacy behaviour,
// and the back-compatible default. Like state_type, it must not contain
// '\n' or '|'.
struct StateVersionEntry {
    OperatorId op_id{};
    std::string state_type;
    std::uint32_t version{1};
    std::string slot;
};

class StateVersionMap {
public:
    // Set or replace the version stamp for (op_id, state_type), with an
    // optional keyed-state `slot` name the migrator filters on (empty =
    // every value under the operator; see StateVersionEntry::slot).
    // Throws std::invalid_argument if state_type or slot contains a
    // forbidden delimiter ('\n' or '|') - those are reserved for the
    // on-disk packing format.
    void set(OperatorId op, std::string state_type, std::uint32_t version, std::string slot = {});

    // Look up the recorded version for (op_id, state_type). Returns
    // nullopt if the pair was not stamped at snapshot time - callers
    // should treat that as "unknown, assume version 1" by convention,
    // matching the SchemaVersionTrait default.
    [[nodiscard]] std::optional<std::uint32_t> get(OperatorId op,
                                                   const std::string& state_type) const;

    [[nodiscard]] std::vector<StateVersionEntry> entries() const;

    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] bool empty() const noexcept { return entries_.empty(); }
    void clear() noexcept { entries_.clear(); }

    // Pack to / unpack from the canonical line-oriented string format
    // we embed in Arrow IPC schema metadata. Each line is
    // "<op_id>|<state_type>|<version>", lines separated by '\n'. Empty
    // input produces an empty map. Unparseable input throws
    // std::runtime_error - callers should treat a parse failure as a
    // corrupt savepoint, not as an absent map.
    [[nodiscard]] std::string pack() const;
    static StateVersionMap unpack(std::string_view packed);

private:
    // Keyed on (op_id, state_type); duplicates would mean two slots of
    // the same type on one operator - which we forbid.
    struct Key {
        OperatorId op;
        std::string state_type;
        bool operator==(const Key& other) const noexcept {
            return op == other.op && state_type == other.state_type;
        }
    };
    struct KeyHash {
        std::size_t operator()(const Key& k) const noexcept {
            std::hash<std::uint64_t> uh;
            std::hash<std::string> sh;
            return uh(k.op.value()) ^ (sh(k.state_type) << 1);
        }
    };
    // The version stamp plus the keyed-state slot it applies to. slot is
    // an attribute of the (op, state_type) binding, not part of the key -
    // each slot already has a distinct state_type per the invariant above.
    struct Stamp {
        std::uint32_t version{1};
        std::string slot;
    };
    std::unordered_map<Key, Stamp, KeyHash> entries_;
};

// A migration function transforms the on-disk byte form of one state
// value from `from_version` to `to_version`. Migration functions are
// pure: they read input bytes and produce output bytes; they do not
// touch the live state map or any operator. The registry composes
// single-step migrations into multi-step chains as needed.
class StateMigrationRegistry {
public:
    using MigrationFn = std::function<std::vector<std::byte>(std::span<const std::byte> input)>;

    // Register a single-step migration. state_type is a user-chosen tag
    // identifying which typed state slot this migration applies to.
    // Convention: the typed name of T (e.g., "UserCounter") or the
    // qualified "{op_uid}.{slot}" form. The registry stores migrations
    // by (state_type, from_version) and walks them forward.
    void register_migration(std::string state_type,
                            std::uint32_t from_version,
                            std::uint32_t to_version,
                            MigrationFn fn);

    // Migrate input bytes from `from_version` to `to_version`. Plans a
    // chain of single-step migrations as needed (BFS over registered
    // edges); the output of step k feeds step k+1. Throws
    // std::runtime_error with a descriptive message if no path exists
    // between the two versions for state_type.
    //
    // from == to is a no-op (returns input copied).
    std::vector<std::byte> migrate(const std::string& state_type,
                                   std::uint32_t from_version,
                                   std::uint32_t to_version,
                                   std::span<const std::byte> input) const;

    // Returns true if a migration chain exists. Use this from the
    // pre-deploy compatibility checker (clink_check_savepoint) to
    // surface incompatibilities before the deploy starts.
    bool has_path(const std::string& state_type,
                  std::uint32_t from_version,
                  std::uint32_t to_version) const;

    // Process-global registry. Sole instance shared across operators
    // and CLI tools. Tests construct local instances to avoid
    // cross-test bleed.
    static StateMigrationRegistry& global();

    // Test/CLI: enumerate all registered migrations for a state_type.
    // Returned vector is a snapshot under the registry lock.
    struct Edge {
        std::uint32_t from_version;
        std::uint32_t to_version;
    };
    std::vector<Edge> edges_for(const std::string& state_type) const;

    // Phase 27d: Arrow-aware auto-migration. Users register the Arrow
    // schema of each (state_type, version) they care about. When a
    // migrate() call has no explicit migration registered but BOTH
    // versions have registered schemas AND the change is "additive"
    // (new fields are nullable; existing fields preserved or widened
    // to a non-narrowing integer), the registry synthesises the
    // migration function on the fly. 80% of real-world state schema
    // changes are additive; this removes the user-code burden for
    // those cases.
    //
    // Wire convention for an Arrow-encoded state value: the value
    // bytes are a complete Arrow IPC stream (schema + a single
    // RecordBatch). The auto-migration reads the stream, projects to
    // the new schema (filling new nullable columns with nulls,
    // casting widening integers), and writes the result back as an
    // Arrow IPC stream with the new schema.
    void register_arrow_schema(std::string state_type,
                               std::uint32_t version,
                               std::shared_ptr<arrow::Schema> schema);

    // Look up the registered Arrow schema for (state_type, version).
    // Returns nullptr if no schema was registered.
    [[nodiscard]] std::shared_ptr<arrow::Schema> arrow_schema_for(const std::string& state_type,
                                                                  std::uint32_t version) const;

    // True if there are registered Arrow schemas for both versions
    // and the change from -> to is additively-compatible (so the
    // registry can synthesise a migration without an explicit fn).
    [[nodiscard]] bool can_auto_arrow_migrate(const std::string& state_type,
                                              std::uint32_t from_version,
                                              std::uint32_t to_version) const;

private:
    struct Key {
        std::string state_type;
        std::uint32_t from_version;
        bool operator==(const Key& other) const noexcept {
            return from_version == other.from_version && state_type == other.state_type;
        }
    };
    struct KeyHash {
        std::size_t operator()(const Key& k) const noexcept {
            std::hash<std::string> sh;
            std::hash<std::uint32_t> uh;
            return sh(k.state_type) ^ (uh(k.from_version) << 1);
        }
    };

    // Forward-only adjacency: from a given (state_type, from_version)
    // there may be multiple registered targets. Chain planning explores
    // forward edges in BFS order so it always picks the shortest path.
    struct Edge_ {
        std::uint32_t to_version;
        MigrationFn fn;
    };

    // BFS over the registered edges. Returns the sequence of
    // to_version hops that walk `from` to `to`, or empty if no path
    // exists. Caller must hold `mu_`.
    std::vector<std::uint32_t> plan_path_unlocked(const std::string& state_type,
                                                  std::uint32_t from,
                                                  std::uint32_t to) const;

    // Look up an Arrow schema in the side map under the lock.
    std::shared_ptr<arrow::Schema> arrow_schema_unlocked(const std::string& state_type,
                                                         std::uint32_t version) const;

    // Side map for Arrow schemas, keyed by (state_type, version).
    // Reuses the existing Key + KeyHash since (state_type, version)
    // is the natural identity for schema registration too.
    struct ArrowKey {
        std::string state_type;
        std::uint32_t version;
        bool operator==(const ArrowKey& other) const noexcept {
            return version == other.version && state_type == other.state_type;
        }
    };
    struct ArrowKeyHash {
        std::size_t operator()(const ArrowKey& k) const noexcept {
            std::hash<std::string> sh;
            std::hash<std::uint32_t> uh;
            return sh(k.state_type) ^ (uh(k.version) << 1);
        }
    };

    mutable std::mutex mu_;
    std::unordered_map<Key, std::vector<Edge_>, KeyHash> edges_;
    std::unordered_map<ArrowKey, std::shared_ptr<arrow::Schema>, ArrowKeyHash> arrow_schemas_;
};

}  // namespace clink
