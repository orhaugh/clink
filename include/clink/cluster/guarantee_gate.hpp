#pragma once

// Submission-time delivery-guarantee gate.
//
// The analyser in clink/connectors/delivery_guarantee.hpp computes what a
// pipeline can actually provide. This is the bridge that feeds it a real
// JobGraphSpec and turns its verdict into a submission decision, so the
// analysis stops being advice and starts being enforcement.
//
// Two things it must get right, and one it must not do.
//
// Right: mapping an op TYPE to a connector NAME. Op types are factory
// names ("kafka_2pc_sink_string", "file_line_sink"), capability records
// are keyed on connector identity ("kafka_2pc", "file"). The mapping is
// longest-prefix over the declared names, so kafka_2pc_sink_string
// resolves to kafka_2pc rather than kafka - which matters enormously,
// because those two have different guarantees and picking the shorter one
// would systematically over-promise.
//
// Right: an op whose connector cannot be identified is reported as
// UNDECLARED, not skipped. The analyser then assumes at-least-once for it.
// Skipping would let an unrecognised sink silently not count towards the
// weakest link.
//
// Must not: reject a job merely because its guarantee is weak. Most jobs
// are at-least-once and that is fine. The gate rejects only when the
// submitter ASKED for something stronger than the pipeline can provide.
// Everything else is reported, logged, and allowed.

#include <string>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

namespace clink::cluster {

// Build the analyser's input from a plan. `checkpoint` supplies the
// durability facts the graph does not carry.
[[nodiscard]] connectors::PipelineFacts pipeline_facts_from_graph(
    const JobGraphSpec& graph, const CheckpointConfig& checkpoint);

// Analyse and decide. Returns an empty string to allow the submission, or
// the rejection message to throw.
//
// The report is returned through `out_report` (when non-null) so the
// caller can log the full reasoning even on the allow path - an operator
// should be able to see what guarantee their job actually has without
// having to make it fail first.
[[nodiscard]] std::string check_delivery_guarantee(const JobGraphSpec& graph,
                                                   const CheckpointConfig& checkpoint,
                                                   connectors::GuaranteeReport* out_report);

// Resolve one op type to a declared connector name. Empty when nothing
// matches. Exposed for testing, because the longest-prefix rule is the
// part most likely to be got wrong by a future connector name.
[[nodiscard]] std::string connector_name_for_op_type(const std::string& op_type);

// Whether this op type is a sink whose declared capability is an
// exactly-once level with commit_recoverable == false - a sink whose
// external commit dies with the process and cannot be re-executed at
// restore. A task hosting one puts its job on the commit-confirmed
// restore protocol: the coordinator gates CONFIRMED-N markers on that
// task's CommitConfirmed messages, and restores for the job select the
// newest CONFIRMED checkpoint instead of the newest COMPLETED one.
[[nodiscard]] bool op_type_needs_commit_confirmation(const std::string& op_type);

}  // namespace clink::cluster
