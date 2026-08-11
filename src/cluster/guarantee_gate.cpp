#include "clink/cluster/guarantee_gate.hpp"

#include <algorithm>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::cluster {

namespace {

// An op with no inputs is a source; anything whose output nothing consumes
// is a sink. Computed from the graph rather than guessed from the type
// name, because a name-based guess ("*_sink") misses every connector that
// does not follow the convention.
struct Roles {
    bool is_source{false};
    bool is_sink{false};
};

std::vector<Roles> classify(const JobGraphSpec& graph) {
    std::vector<Roles> roles(graph.ops.size());
    std::vector<bool> consumed(graph.ops.size(), false);
    for (std::size_t i = 0; i < graph.ops.size(); ++i) {
        roles[i].is_source = graph.ops[i].inputs.empty();
        for (const auto& in : graph.ops[i].inputs) {
            for (std::size_t j = 0; j < graph.ops.size(); ++j) {
                if (graph.ops[j].id == in) {
                    consumed[j] = true;
                }
            }
        }
    }
    for (std::size_t i = 0; i < graph.ops.size(); ++i) {
        roles[i].is_sink = !consumed[i] && !roles[i].is_source;
    }
    return roles;
}

}  // namespace

std::string connector_name_for_op_type(const std::string& op_type) {
    // Longest prefix wins. "kafka_2pc_sink_string" must resolve to
    // "kafka_2pc", never "kafka": the two carry different guarantees and
    // taking the shorter match would systematically over-promise, which is
    // the exact failure this whole mechanism exists to prevent.
    std::string best;
    for (const auto& caps : connectors::CapabilityRegistry::instance().all()) {
        if (op_type.rfind(caps.name, 0) == 0 && caps.name.size() > best.size()) {
            best = caps.name;
        }
    }
    return best;
}

bool op_type_needs_commit_confirmation(const std::string& op_type) {
    const auto name = connector_name_for_op_type(op_type);
    if (name.empty()) {
        return false;
    }
    const auto* rec = connectors::CapabilityRegistry::instance().find(name);
    if (rec == nullptr || !rec->is_sink) {
        return false;
    }
    const bool exactly_once =
        rec->delivery == connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit ||
        rec->delivery == connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish;
    return exactly_once && !rec->commit_recoverable;
}

connectors::PipelineFacts pipeline_facts_from_graph(const JobGraphSpec& graph,
                                                    const CheckpointConfig& checkpoint) {
    ensure_built_ins_registered();

    connectors::PipelineFacts facts;
    facts.checkpointing_enabled = !checkpoint.checkpoint_dir.empty() && checkpoint.interval_ms > 0;

    // "Durable" means the state survives the PROCESS, not just the job. An
    // empty URI resolves to memory unless a checkpoint dir was given (the
    // legacy bare-checkpoint-dir path), and memory:// never survives.
    const auto& uri = checkpoint.state_backend_uri;
    const bool explicitly_memory = uri.rfind("memory", 0) == 0;
    facts.durable_state_backend =
        !explicitly_memory && (!uri.empty() || !checkpoint.checkpoint_dir.empty());

    const auto roles = classify(graph);
    for (std::size_t i = 0; i < graph.ops.size(); ++i) {
        const auto& op = graph.ops[i];
        if (!roles[i].is_source && !roles[i].is_sink) {
            continue;  // a mid-chain operator has no delivery semantics
        }
        connectors::PipelineConnector pc;
        pc.op_type = op.type;
        pc.is_source = roles[i].is_source;
        pc.connector_name = connector_name_for_op_type(op.type);
        // An unmatched op is reported as undeclared rather than dropped:
        // the analyser then assumes the worst for it. Dropping would let an
        // unrecognised sink quietly not count towards the weakest link.
        pc.declaration_missing = pc.connector_name.empty();
        pc.supplied_options.reserve(op.params.size());
        for (const auto& [k, _] : op.params) {
            pc.supplied_options.push_back(k);
        }
        // Carried so the analyser can tell whether transactional sinks
        // commit as a unit or independently.
        if (const auto cg = op.params.find("commit_group"); cg != op.params.end()) {
            pc.commit_group = cg->second;
        }
        facts.connectors.push_back(std::move(pc));
    }

    // What the submitter asked for. Carried on any participating op's
    // params, which is where the SQL DDL WITH-clause lands it.
    for (const auto& op : graph.ops) {
        const auto it = op.params.find("delivery_guarantee");
        if (it == op.params.end()) {
            continue;
        }
        if (const auto want = connectors::delivery_from_string(it->second); want.has_value()) {
            // Strongest request across the graph wins: if any sink asked
            // for exactly-once, the pipeline as a whole is being asked for
            // it, and the weakest link still decides whether it can.
            if (!facts.requested.has_value() ||
                connectors::strength(*want) > connectors::strength(*facts.requested)) {
                facts.requested = want;
            }
        }
    }
    return facts;
}

std::string check_delivery_guarantee(const JobGraphSpec& graph,
                                     const CheckpointConfig& checkpoint,
                                     connectors::GuaranteeReport* out_report) {
    const auto facts = pipeline_facts_from_graph(graph, checkpoint);
    auto report = connectors::analyse_pipeline(facts);

    if (report.acceptable()) {
        // Logged on the ALLOW path too. An operator should be able to see
        // what guarantee a running job actually has without first having to
        // make it fail.
        log::info(
            "coordinator.guarantee",
            std::string("job delivery guarantee: ") +
                std::string(connectors::to_string(report.level)) +
                (report.limiting_factor.empty() ? std::string{}
                                                : " (limited by " + report.limiting_factor + ")"));
        for (const auto& w : report.warnings) {
            log::warn("coordinator.guarantee", w);
        }
    }

    std::string rejection;
    if (!report.acceptable()) {
        rejection = "submit_job: " + report.rejections.front();
        for (std::size_t i = 1; i < report.rejections.size(); ++i) {
            rejection += "; " + report.rejections[i];
        }
        rejection += "\n" + report.render_text();
    }
    if (out_report != nullptr) {
        *out_report = std::move(report);
    }
    return rejection;
}

}  // namespace clink::cluster
