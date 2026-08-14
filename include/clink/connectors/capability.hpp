#pragma once

// Connector capability contract.
//
// Before this existed, a connector's delivery semantics were a free-text
// property (`delivery_guarantee='exactly_once'` in a DDL WITH clause,
// checked by a handful of string comparisons in the SQL binder) plus
// whatever the connector's doc page happened to claim. Two consequences:
//
//   * A job could ask for exactly-once from a sink that has no
//     transactional path and get no complaint, because nothing held a
//     machine-readable statement of what that sink can actually do.
//   * "Exactly once" got used as one word for two very different
//     properties - clink's internal state being exactly-once, and the
//     OUTPUT reaching an external system exactly once. Only the first is
//     true for most sinks.
//
// A ConnectorCapabilities record is that machine-readable statement. It is
// declared next to the factory registration, so a connector that is not
// compiled into this binary has no record, and `clink --capabilities`
// reports what this binary can genuinely do rather than what the project
// supports somewhere.
//
// Honesty rule for anyone adding a record: every field is a claim that
// something can be held to. `delivery` in particular must describe the
// mechanism the code implements, not the mechanism the external system is
// capable of. Kafka supports transactions; a Kafka sink registered without
// a transactional.id does not.

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace clink::connectors {

// The delivery guarantee a connector implementation provides. An explicit
// closed set, deliberately not a string: a typo in a string silently
// becomes a new guarantee level, and comparing guarantees for "is this at
// least as strong as that" needs an ordering the string does not have.
enum class DeliveryGuarantee : std::uint8_t {
    // Records can be lost on failure. A source with no offset state, a
    // sink that acknowledges before the write lands.
    AtMostOnce,
    // No loss, duplicates possible after a restart. The default for a
    // replayable source with a non-transactional sink.
    AtLeastOnce,
    // Duplicates are delivered but collapse in the external system,
    // because writes are keyed upserts. Correct only if the key is right,
    // which is why this level names the requirement.
    EffectivelyOnceIdempotent,
    // Output becomes visible in one atomic publication tied to checkpoint
    // completion - a rename, a manifest swap, a multipart complete.
    ExactlyOnceAtomicPublish,
    // Output is staged in an external transaction prepared at the barrier
    // and committed on global checkpoint completion.
    ExactlyOnceTwoPhaseCommit,
    // The connector keeps no position and participates in no checkpoint,
    // so a restart has no defined relationship to what was already
    // processed. Distinct from AtMostOnce: this is "the question does not
    // have an answer", not "the answer is loss is possible".
    NoDurableRestartGuarantee,
};

[[nodiscard]] std::string_view to_string(DeliveryGuarantee g) noexcept;
[[nodiscard]] std::optional<DeliveryGuarantee> delivery_from_string(std::string_view s) noexcept;

// Ordering for "how strong is this". Used to compute the weakest link in a
// pipeline. EffectivelyOnceIdempotent sits below the two exactly-once
// mechanisms because it is conditional on the user having chosen a correct
// idempotency key, which clink cannot verify.
[[nodiscard]] int strength(DeliveryGuarantee g) noexcept;

// How a source records where it got to.
enum class OffsetModel : std::uint8_t {
    None,         // no position is kept
    FileOffset,   // byte or row offset within a named file
    LogOffset,    // broker/partition offset (Kafka, Pulsar)
    Lsn,          // database log sequence number (Postgres CDC)
    Timestamp,    // wall-clock or event-time watermark position
    OpaqueToken,  // vendor cursor the connector round-trips unmodified
};

[[nodiscard]] std::string_view to_string(OffsetModel m) noexcept;

enum class Boundedness : std::uint8_t { Bounded, Unbounded, Either };

[[nodiscard]] std::string_view to_string(Boundedness b) noexcept;

// One connector's declared contract. Aggregate-initialised with
// designated initialisers at the registration site so a field added later
// defaults sanely rather than silently shifting an existing declaration's
// meaning.
struct ConnectorCapabilities {
    // Identity.
    std::string name;     // "kafka", "postgres", ...
    std::string version;  // connector revision, not clink's
    bool is_source{false};
    bool is_sink{false};

    // Build + runtime dependencies. Named so an operator can tell why a
    // connector is absent from a given binary.
    std::vector<std::string> build_dependencies;
    std::vector<std::string> runtime_dependencies;

    // Data.
    std::vector<std::string> formats;  // "json", "arrow", "parquet", ...
    Boundedness boundedness{Boundedness::Unbounded};

    // Recovery.
    bool replayable{false};  // can re-read from a recorded position
    OffsetModel offset_model{OffsetModel::None};
    bool checkpoint_integrated{false};  // participates in barrier/snapshot
    DeliveryGuarantee delivery{DeliveryGuarantee::NoDurableRestartGuarantee};
    bool transactional{false};  // has prepare/commit/abort
    // Whether a prepared-but-uncommitted transaction can be COMMITTED AGAIN
    // by a fresh process after a crash (CommittingSink's recover(): COMMIT
    // PREPARED, re-complete a multipart upload, re-rename a staged file).
    // False for sinks whose transaction dies with the producer session
    // (Kafka: librdkafka has no transaction resume). A false here puts the
    // job on the commit-confirmed restore protocol: restores select the
    // newest checkpoint whose commits provably EXECUTED (CONFIRMED-N), so
    // a die-before-commit loses nothing - at the price that a
    // die-after-commit-before-confirmation replays one interval as
    // duplicates. Only meaningful when `delivery` is an exactly-once
    // level.
    bool commit_recoverable{true};
    // Non-empty when the guarantee is conditional on the user supplying an
    // idempotency key; names the option that carries it.
    std::string idempotency_key_option;

    // Schema + partitioning.
    bool schema_evolution{false};
    bool partition_discovery{false};

    // Connectivity.
    std::vector<std::string> auth_methods;
    bool tls{false};
    bool backpressure{false};  // honours downstream backpressure
    bool retries{false};
    // Option names that carry timeouts. Empty means the connector exposes
    // none, which the config linter reports for a production profile.
    std::vector<std::string> timeout_options;
    // 0 = no documented ceiling.
    std::uint64_t max_message_bytes{0};

    // Surfaces.
    bool available_in_sql{false};
    bool available_in_cpp{true};

    // Anything a user would be annoyed to discover at 3am.
    std::vector<std::string> limitations;

    // Options the connector requires when `delivery` is one of the
    // exactly-once levels. Checked at validation time.
    std::vector<std::string> required_options_for_exactly_once;

    // Internal consistency of the record itself. A declaration that claims
    // two-phase commit but not `transactional`, or exactly-once from a
    // non-replayable source, is a bug in the DECLARATION and is caught by
    // a test rather than discovered in production.
    [[nodiscard]] std::vector<std::string> self_check() const;
};

// Process-wide registry of declared capabilities.
//
// Scoping note, same as the fault registry: clink_core is static and
// plugins are dlopen'd RTLD_LOCAL, so a plugin carrying its own clink_core
// copy has its own instance. In practice every in-tree connector declares
// itself from the same install() call that registers its factories into
// the host registry, so the host manifest is complete for in-tree
// connectors; an out-of-tree plugin's declarations are visible to that
// plugin's own validation.
class CapabilityRegistry {
public:
    static CapabilityRegistry& instance();

    // Last declaration for a given name wins, matching the last-write-wins
    // behaviour of factory registration.
    void declare(ConnectorCapabilities caps);

    [[nodiscard]] const ConnectorCapabilities* find(std::string_view name) const;

    // All declarations, ordered by name so output is stable and diffable.
    [[nodiscard]] std::vector<ConnectorCapabilities> all() const;

    [[nodiscard]] std::size_t size() const;

private:
    mutable std::mutex mu_;
    std::map<std::string, ConnectorCapabilities, std::less<>> by_name_;
};

// Convenience for a registration site.
inline void declare_connector(ConnectorCapabilities caps) {
    CapabilityRegistry::instance().declare(std::move(caps));
}

// ---------------------------------------------------------------------------
// Manifest rendering
// ---------------------------------------------------------------------------

// Non-connector facts about this binary that belong in the same manifest:
// which optional subsystems were compiled in, and whether the build
// carries surfaces (fault injection, unverified-checkpoint tolerance) that
// weaken its guarantees.
// Version of the JSON manifest's SHAPE, reported as the top-level
// `schema_version` key so a consumer can detect a layout it does not
// understand instead of misreading one. Bump it when a key is renamed,
// removed, or changes meaning or type; ADDING a key is compatible and does
// not bump it. Pinned by test_connector_capability.
inline constexpr std::uint32_t kCapabilityManifestSchemaVersion = 1;

struct BuildFacts {
    std::string clink_version;
    // Exact commit of the source tree this binary was built from, and
    // whether that tree was clean - the pair that maps a manifest back to
    // code. Same values the plugin ABI handshake reports.
    std::string git_sha;
    bool git_clean{false};
    bool sql{false};
    bool http{false};
    bool tls{false};
    bool wasm{false};
    bool onnx{false};
    bool fault_injection{false};
    bool allows_unverified_checkpoints{false};
    std::uint32_t checkpoint_meta_version{0};
};

// Whether the SQL frontend is present in this process. clink_core cannot
// answer that with a #ifdef - clink_sql sits above it - so the frontend
// announces itself from install().
void mark_sql_frontend_present();
[[nodiscard]] bool sql_frontend_present();

[[nodiscard]] BuildFacts current_build_facts();

// Human-readable manifest for `clink --capabilities`.
[[nodiscard]] std::string render_manifest_text(const BuildFacts& facts,
                                               const std::vector<ConnectorCapabilities>& caps);

// Machine-readable manifest for `clink --capabilities-json`. Hand-rolled
// JSON, consistent with the rest of the control plane's output.
[[nodiscard]] std::string render_manifest_json(const BuildFacts& facts,
                                               const std::vector<ConnectorCapabilities>& caps);

}  // namespace clink::connectors
