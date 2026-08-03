#pragma once

// End-to-end delivery-guarantee analysis.
//
// A pipeline's guarantee is a property of the WHOLE path - source
// replayability, checkpoint durability, operator determinism, sink commit
// protocol - and it is only ever as strong as its weakest link. clink
// previously had no single place that computed it, so "exactly once"
// tended to mean "checkpointing is enabled", which is a statement about
// internal state and says nothing about what an external system sees.
//
// This computes the honest answer and names the link that caps it. The
// levels are deliberately verbose: a label that cannot be misread as a
// blanket exactly-once promise is the entire point.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/capability.hpp"

namespace clink::connectors {

enum class EndToEndGuarantee : std::uint8_t {
    // Nothing survives a restart in a defined way.
    NoRecoveryGuarantee,
    // The source cannot replay, so a restart loses whatever was in flight.
    AtMostOnceSource,
    // No loss; duplicates reach the sink after a restart.
    StateExactlyOnceOutputAtLeastOnce,
    // Duplicates reach the sink but collapse there, PROVIDED the user's
    // idempotency key is correct. clink cannot verify that, so the label
    // carries the condition.
    EffectivelyOnceRequiresIdempotentKey,
    // Replayable source, durable exactly-once state, and every sink
    // commits atomically or transactionally on checkpoint completion.
    EndToEndExactlyOnce,
};

[[nodiscard]] std::string_view to_string(EndToEndGuarantee g) noexcept;

// Why a job might not be able to replay deterministically. Reported
// separately from the delivery level because a non-deterministic pipeline
// can still be exactly-once in the delivery sense (each record committed
// once) while producing DIFFERENT output on replay.
struct DeterminismFacts {
    bool reads_wall_clock{false};
    bool uses_random{false};
    bool calls_external_service{false};
    bool has_nondeterministic_udf{false};
    bool has_async_side_effects{false};
    std::vector<std::string> sources_of_nondeterminism;

    [[nodiscard]] bool deterministic() const noexcept {
        return !reads_wall_clock && !uses_random && !calls_external_service &&
               !has_nondeterministic_udf && !has_async_side_effects;
    }
};

// One participating connector instance in the job under analysis.
struct PipelineConnector {
    std::string op_type;         // the factory name in the plan
    std::string connector_name;  // key into the capability registry
    bool is_source{false};
    // Options supplied for this instance, used to check the
    // required-for-exactly-once list and the idempotency key.
    std::vector<std::string> supplied_options;
    // Present when the connector has no capability declaration; the
    // analysis then has to assume the worst rather than guess.
    bool declaration_missing{false};
    // The commit_group this sink was given, empty when it has none.
    //
    // Per-sink exactly-once does not compose into job-level atomicity.
    // Two transactional sinks that commit independently can be left in
    // disagreement by a failure between their commits - one published,
    // one not - and each sink is still, correctly, exactly-once on its
    // own. Sinks sharing a commit_group commit as a unit instead.
    std::string commit_group;
};

struct PipelineFacts {
    std::vector<PipelineConnector> connectors;
    bool checkpointing_enabled{false};
    // The state backend keeps state across a process restart. In-memory
    // backends do not, which caps the whole pipeline no matter what the
    // connectors can do.
    bool durable_state_backend{false};
    DeterminismFacts determinism;
    // What the user asked for, if anything.
    std::optional<DeliveryGuarantee> requested;
};

struct GuaranteeReport {
    EndToEndGuarantee level{EndToEndGuarantee::NoRecoveryGuarantee};
    // The connector or property that caps the level. Empty when nothing
    // constrains it below exactly-once.
    std::string limiting_factor;
    // Full reasoning, one line per constraint considered. Rendered by
    // EXPLAIN and the CLI so the answer is auditable rather than oracular.
    std::vector<std::string> reasons;
    // Things that do not change the level but that an operator should
    // know: an idempotency key that must be right, a sink weaker than the
    // rest, non-determinism that makes replay produce different output.
    std::vector<std::string> warnings;
    // Non-empty when the pipeline cannot provide what was requested. A
    // caller turns this into a submission rejection.
    std::vector<std::string> rejections;

    [[nodiscard]] bool acceptable() const noexcept { return rejections.empty(); }
    [[nodiscard]] std::string render_text() const;
    [[nodiscard]] std::string render_json() const;
};

// Compute the strongest guarantee the pipeline can actually provide, and
// reject a request the pipeline cannot honour.
[[nodiscard]] GuaranteeReport analyse_pipeline(const PipelineFacts& facts);

}  // namespace clink::connectors
