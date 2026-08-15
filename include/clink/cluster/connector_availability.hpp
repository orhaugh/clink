#pragma once

// Submission-time connector-availability gate.
//
// A connector that was not compiled into this binary registers no
// factories, so a job that names one used to compile (the SQL planner's
// vocabulary is static) and fail only when deployment tried to build the
// operator on a worker - after tasks were allocated, with an error that
// named a factory string rather than the connector or the fix.
//
// This gate runs in Coordinator::submit_job before any planning or
// allocation, in whichever process owns the submission:
//
//   * `clink run pipeline.sql` submits through an in-process coordinator,
//     so the gate checks the local binary;
//   * a distributed submission (POST /api/v1/jobs/spec or /jobs/sql) is
//     gated on the coordinator, so the TARGET CLUSTER is authoritative -
//     never the submitting CLI's binary, whose connector set may differ.
//
// Availability is read from the registries the job would actually deploy
// against (never a hardcoded list): an op type any registry knows passes.
// Only when a type is unknown AND names a connector in clink's compiled
// vocabulary does the gate reject, with the connectors this binary DOES
// have (from the capability registry) and the CMake flag that adds the
// missing one. Types that match no connector vocabulary are left for
// deploy to resolve, so plugin and inline operators are unaffected.

#include <optional>
#include <string>
#include <string_view>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"

namespace clink::cluster {

// The compiled-in knowledge that op types with this prefix belong to a
// named connector behind a named build flag. This is diagnostics-only
// vocabulary: a connector that is ABSENT has no capability record and no
// factories, so nothing in the running binary can name the flag that
// would add it. Availability itself is never decided from this table.
struct ConnectorVocabularyEntry {
    std::string_view prefix;      // op-type prefix, e.g. "kafka"
    std::string_view connector;   // user-facing connector name
    std::string_view build_flag;  // CMake option, empty when always built
};

// Longest-prefix match of an op type against the vocabulary, honouring
// the '_' boundary ("s3_parquet_string_source" matches s3_parquet, not
// s3; "kafka" itself matches "kafka"). Returns nullopt for op types that
// are not connector-shaped (window operators, plugin ops, ...).
[[nodiscard]] std::optional<ConnectorVocabularyEntry> connector_vocabulary_lookup(
    std::string_view op_type);

// Check every op in the graph. Returns an empty string to allow the
// submission, or the full rejection message. `ops` and `runners` must be
// the registries the job would deploy against (the bundle's when the job
// carries one, so plugin registrations are honoured).
[[nodiscard]] std::string check_connector_availability(const JobGraphSpec& graph,
                                                       const OperatorRegistry& ops,
                                                       const RunnerRegistry& runners);

// Whether a user-facing connector name is available in THIS binary, for
// vocabulary surfaces like GET /api/v1/connectors. In-tree connectors
// that every SQL-linked build carries report true directly; everything
// else is matched against the capability registry (a record is declared
// only when the impl is compiled in), tolerating the prefix families in
// both directions: "kafka" is available when "kafka_2pc" is declared,
// and "s3_parquet" is available when its providing impl declared "s3".
[[nodiscard]] bool connector_declared_available(std::string_view connector);

}  // namespace clink::cluster
