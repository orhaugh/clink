#include "clink/cluster/guarantee_gate.hpp"

#include <algorithm>
#include <optional>

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

connectors::PipelineConnector connector_of(const OperatorSpec& op, bool is_source) {
    connectors::PipelineConnector pc;
    pc.op_type = op.type;
    pc.is_source = is_source;
    pc.connector_name = connector_name_for_op_type(op.type);
    // An unmatched op is reported as undeclared rather than dropped: the
    // analyser then assumes the worst for it. Dropping would let an
    // unrecognised sink quietly not count towards the weakest link.
    pc.declaration_missing = pc.connector_name.empty();
    pc.supplied_options.reserve(op.params.size());
    for (const auto& [k, _] : op.params) {
        pc.supplied_options.push_back(k);
    }
    // Carried so the analyser can tell whether transactional sinks commit
    // as a unit or independently.
    if (const auto cg = op.params.find("commit_group"); cg != op.params.end()) {
        pc.commit_group = cg->second;
    }
    return pc;
}

// The ops a sink reads through: the sink itself and every op that feeds it.
std::vector<bool> feeding(const JobGraphSpec& graph, std::size_t sink) {
    std::vector<bool> in(graph.ops.size(), false);
    std::vector<std::size_t> todo{sink};
    in[sink] = true;
    while (!todo.empty()) {
        const auto i = todo.back();
        todo.pop_back();
        for (const auto& id : graph.ops[i].inputs) {
            for (std::size_t j = 0; j < graph.ops.size(); ++j) {
                if (!in[j] && graph.ops[j].id == id) {
                    in[j] = true;
                    todo.push_back(j);
                }
            }
        }
    }
    return in;
}

struct HeldSink {
    std::size_t op{0};
    connectors::DeliveryGuarantee want{connectors::DeliveryGuarantee::AtLeastOnce};
    bool declared{false};
};

// What each sink is held to, when the sinks of one job declare different
// guarantees; empty when they do not, which is every job but those. A sink
// with a delivery_guarantee of its own is held to it. A sink without one is
// held to the strongest guarantee declared anywhere in the job, as the job
// as a whole is: leaving it undeclared beside an exactly-once sink is how a
// job asks for exactly-once everywhere, and it is refused if that sink
// cannot give it.
std::vector<HeldSink> held_sinks(const JobGraphSpec& graph,
                                 const std::vector<Roles>& roles,
                                 const std::optional<connectors::DeliveryGuarantee>& strongest) {
    if (!strongest.has_value()) {
        return {};
    }
    std::vector<HeldSink> held;
    bool differ = false;
    for (std::size_t i = 0; i < graph.ops.size(); ++i) {
        if (!roles[i].is_sink) {
            continue;
        }
        HeldSink h{i, *strongest, false};
        const auto it = graph.ops[i].params.find("delivery_guarantee");
        if (it != graph.ops[i].params.end()) {
            if (const auto own = connectors::delivery_from_string(it->second); own.has_value()) {
                h.want = *own;
                h.declared = true;
            }
        }
        differ = differ || h.want != *strongest;
        held.push_back(h);
    }
    if (!differ) {
        held.clear();
    }
    return held;
}

}  // namespace

std::string connector_name_for_op_type(const std::string& op_type) {
    // Op types whose names do not begin with the record that describes them.
    // The SQL row Parquet 2PC sink matched "parquet", the at-least-once
    // rolling sink, so a job asking for exactly-once through it was refused.
    if (op_type.rfind("parquet_row_2pc_sink", 0) == 0) {
        return "parquet_2pc";
    }
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
        if (!roles[i].is_source && !roles[i].is_sink) {
            continue;  // a mid-chain operator has no delivery semantics
        }
        facts.connectors.push_back(connector_of(graph.ops[i], roles[i].is_source));
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

    // ---- Replay determinism, classified from the spec itself.
    //
    // Delivery and determinism are separate answers: a job can commit each
    // record exactly once and still emit DIFFERENT values on a replay. The
    // facts here feed the analyser's warning, and - critically - coverage
    // decides what silence means. A SQL-compiled graph is fully classified
    // (determinism_coverage == "sql-planner": wall-clock and random
    // functions were rejected at bind time, and everything that reaches
    // outside the job is identifiable below), so no marks means
    // deterministic. A plugin-built graph's operators are native code the
    // engine cannot inspect, so no marks means UNKNOWN unless every
    // operator declares itself via kOpDeterminismParam.
    auto& det = facts.determinism;
    det.classification_complete = graph.determinism_coverage == "sql-planner";
    bool all_ops_declared = !graph.ops.empty();
    for (const auto& op : graph.ops) {
        const auto declared = op.params.find(std::string{kOpDeterminismParam});
        if (declared == op.params.end() || (declared->second != "deterministic" &&
                                            declared->second.rfind("nondeterministic", 0) != 0)) {
            all_ops_declared = false;
            if (!det.classification_complete) {
                det.unclassified_operators.push_back(op.id + " (" + op.type + ")");
            }
        } else if (declared->second.rfind("nondeterministic", 0) == 0) {
            det.has_nondeterministic_udf = true;
            const auto colon = declared->second.find(':');
            det.sources_of_nondeterminism.push_back(
                "operator '" + op.id + "' declares itself nondeterministic" +
                (colon == std::string::npos ? "" : ": " + declared->second.substr(colon + 1)));
        }
        if (op.type == "ml_predict_row") {
            // The planner threads the model's WITH options as model.*; a
            // remote provider means the answer depends on a service outside
            // the replayed stream.
            const auto prov = op.params.find("model.provider");
            if (prov != op.params.end() && prov->second == "http") {
                det.calls_external_service = true;
                const auto model = op.params.find("model_name");
                det.sources_of_nondeterminism.push_back("ML_PREDICT over http" +
                                                        (model == op.params.end()
                                                             ? std::string{}
                                                             : " (model '" + model->second + "')") +
                                                        " calls a remote inference service");
            }
        }
        if (op.type == "async_lookup_row" || op.type == "async_lookup_join_row") {
            det.calls_external_service = true;
            const auto fn = op.params.find("function_name");
            det.sources_of_nondeterminism.push_back(
                "async lookup" + (fn == op.params.end() ? std::string{} : " '" + fn->second + "'") +
                " reads an external system whose answers can change between runs");
        }
        if (op.type == "neo4j_lookup_join_row") {
            // Current-state enrichment: each lookup reads the graph as it is
            // when the query runs, so a replay can enrich a row differently.
            det.calls_external_service = true;
            det.sources_of_nondeterminism.push_back(
                "Neo4j lookup '" + op.id +
                "' reads the graph as it is when each lookup runs, so a replay can enrich a row "
                "differently");
        }
    }
    for (const auto& u : graph.udfs) {
        // LANGUAGE SQL bodies are composed of built-ins the binder already
        // vets (wall-clock and random are rejected), so they stay clean.
        // A WASM module is arbitrary guest code with no determinism
        // metadata: conservative default, assumed nondeterministic.
        if (u.language == "wasm" || u.language == "WASM") {
            det.has_nondeterministic_udf = true;
            det.sources_of_nondeterminism.push_back(
                "UDF '" + u.name +
                "' (LANGUAGE WASM) is native guest code with no determinism declaration");
        }
    }
    if (all_ops_declared) {
        det.classification_complete = true;
    }
    return facts;
}

std::string check_delivery_guarantee(const JobGraphSpec& graph,
                                     const CheckpointConfig& checkpoint,
                                     connectors::GuaranteeReport* out_report) {
    const auto facts = pipeline_facts_from_graph(graph, checkpoint);
    const auto roles = classify(graph);
    const auto held = held_sinks(graph, roles, facts.requested);
    connectors::GuaranteeReport report;
    std::vector<std::string> held_lines;
    if (held.empty()) {
        report = connectors::analyse_pipeline(facts);
    } else {
        // The sinks declare different guarantees, so each is checked for
        // the one it is held to, through its own sources, and the job is
        // reported as what its weakest sink gives, as any job is. An
        // at-least-once sink beside an exactly-once one neither caps the
        // exactly-once one nor is refused for being what it says it is;
        // what is refused is a sink that cannot give its own guarantee.
        auto job = facts;
        job.requested.reset();
        report = connectors::analyse_pipeline(job);
        for (const auto& h : held) {
            const auto& sink = graph.ops[h.op];
            const auto mask = feeding(graph, h.op);
            auto own = facts;
            own.connectors.clear();
            for (std::size_t j = 0; j < graph.ops.size(); ++j) {
                if (mask[j] && (roles[j].is_source || j == h.op)) {
                    own.connectors.push_back(connector_of(graph.ops[j], roles[j].is_source));
                }
            }
            own.requested = h.want;
            const auto verdict = connectors::analyse_pipeline(own);
            held_lines.push_back(
                "sink '" + sink.id + "' (" + sink.type + ") is held to " +
                std::string(connectors::to_string(h.want)) +
                (h.declared ? ", which it declares" : ", the strongest the job declares") +
                ", and through its own sources gives " +
                std::string(connectors::to_string(verdict.level)));
            report.reasons.push_back(held_lines.back());
            for (const auto& r : verdict.rejections) {
                report.rejections.push_back("sink '" + sink.id + "': " + r);
            }
        }
    }

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
        for (const auto& line : held_lines) {
            log::info("coordinator.guarantee", line);
        }
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
