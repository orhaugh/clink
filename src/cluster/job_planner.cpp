#include "clink/cluster/job_planner.hpp"

#include <cstdint>
#include <cstdlib>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/config/json.hpp"

namespace clink::cluster {

namespace {

std::string operator_kind_name(OperatorKind k) {
    switch (k) {
        case OperatorKind::Source:
            return "source";
        case OperatorKind::Operator:
            return "operator";
        case OperatorKind::Sink:
            return "sink";
        case OperatorKind::Join:
            return "join";
        case OperatorKind::CoOperator:
            return "co_operator";
    }
    return "operator";
}

OperatorKind operator_kind_from_name(const std::string& s) {
    if (s == "source") {
        return OperatorKind::Source;
    }
    if (s == "sink") {
        return OperatorKind::Sink;
    }
    if (s == "join") {
        return OperatorKind::Join;
    }
    if (s == "co_operator") {
        return OperatorKind::CoOperator;
    }
    return OperatorKind::Operator;
}

// Join-type classification. The planner asks the registry "is any
// join with this op_type name registered (built-in or plugin)?" The
// builtin int64_int64_match_join self-registers via
// ensure_built_ins_registered; plugins register via
// RunnerRegistry::register_join from their install() entry point so
// the planner can see them too. Falls back to true for the legacy
// hardcoded name when called from contexts that don't carry a
// registry reference (none in v1, but keeps the call sites that use
// the free function safe).
bool is_join_op_type(const std::string& type, const RunnerRegistry* rr = nullptr) {
    if (rr != nullptr) {
        return rr->has_join_for_type(type);
    }
    return type == "int64_int64_match_join";
}

std::string escape_json_string(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

}  // namespace

namespace {

// Serialise one ChainOp to its JSON object form. Factored out so the
// new optional fused_source/fused_sink slots can reuse the same shape
// when present, and so the per-op loop in to_json stays readable.
void append_chain_op_json(std::string& out, const ChainOp& op) {
    out += "{\"id\":";
    out += escape_json_string(op.id);
    out += ",\"type\":";
    out += escape_json_string(op.type);
    if (!op.uid.empty()) {
        out += ",\"uid\":";
        out += escape_json_string(op.uid);
    }
    if (!op.display_name.empty()) {
        out += ",\"display_name\":";
        out += escape_json_string(op.display_name);
    }
    out += ",\"kind\":";
    out += escape_json_string(operator_kind_name(op.kind));
    out += ",\"in_channel\":";
    out += escape_json_string(channel_type_name(op.in_channel));
    out += ",\"out_channel\":";
    out += escape_json_string(channel_type_name(op.out_channel));
    out += ",\"parallelism\":";
    out += std::to_string(op.parallelism);
    out += ",\"params\":{";
    bool first = true;
    for (const auto& [k, v] : op.params) {
        if (!first) {
            out += ',';
        }
        first = false;
        out += escape_json_string(k);
        out += ':';
        out += escape_json_string(v);
    }
    out += '}';
    if (!op.side_outputs.empty()) {
        out += ",\"side_outputs\":[";
        for (std::size_t k = 0; k < op.side_outputs.size(); ++k) {
            if (k > 0) {
                out += ',';
            }
            out += "{\"tag\":";
            out += escape_json_string(op.side_outputs[k].tag);
            out += ",\"channel_type\":";
            out += escape_json_string(channel_type_name(op.side_outputs[k].channel_type));
            out += '}';
        }
        out += ']';
    }
    out += '}';
}

}  // namespace

std::string OperatorChainSpec::to_json() const {
    std::string out = "{\"subtask_idx\":";
    out += std::to_string(subtask_idx);
    out += ",\"subtask_idx_in_op\":";
    out += std::to_string(subtask_idx_in_op);
    out += ",\"ops\":[";
    for (std::size_t i = 0; i < ops.size(); ++i) {
        if (i > 0) {
            out += ',';
        }
        append_chain_op_json(out, ops[i]);
    }
    out += "],\"input_edges\":[";
    for (std::size_t i = 0; i < input_edges.size(); ++i) {
        const auto& e = input_edges[i];
        if (i > 0) {
            out += ',';
        }
        out += "{\"peer_role\":";
        out += escape_json_string(e.peer_role);
        out += ",\"peer_subtask_idx\":";
        out += std::to_string(e.peer_subtask_idx);
        out += ",\"channel_type\":";
        out += escape_json_string(channel_type_name(e.channel_type));
        out += ",\"input_index\":";
        out += std::to_string(e.input_index);
        out += '}';
    }
    out += "],\"output_routing\":";
    out += escape_json_string(
        output_routing == OperatorChainSpec::OutputRouting::Split ? "split" : "broadcast");
    out += ",\"output_selector_fn\":";
    out += escape_json_string(output_selector_fn);
    out += ",\"output_groups\":[";
    for (std::size_t i = 0; i < output_groups.size(); ++i) {
        const auto& g = output_groups[i];
        if (i > 0) {
            out += ',';
        }
        out += "{\"mode\":";
        const char* mode_name = "forward";
        if (g.mode == RoutingMode::Rebalance) {
            mode_name = "rebalance";
        } else if (g.mode == RoutingMode::Hash) {
            mode_name = "hash";
        }
        out += escape_json_string(mode_name);
        if (!g.key_extractor_fn.empty()) {
            out += ",\"key_extractor_fn\":";
            out += escape_json_string(g.key_extractor_fn);
        }
        if (!g.side_output_tag.empty()) {
            out += ",\"side_output_tag\":";
            out += escape_json_string(g.side_output_tag);
        }
        out += ",\"edges\":[";
        for (std::size_t j = 0; j < g.edges.size(); ++j) {
            const auto& e = g.edges[j];
            if (j > 0) {
                out += ',';
            }
            out += "{\"peer_role\":";
            out += escape_json_string(e.peer_role);
            out += ",\"peer_subtask_idx\":";
            out += std::to_string(e.peer_subtask_idx);
            out += ",\"channel_type\":";
            out += escape_json_string(channel_type_name(e.channel_type));
            out += '}';
        }
        out += "]}";
    }
    out += ']';
    // Optional fused source / sink. Encoded as nested ChainOp JSON
    // objects so the wire shape mirrors the in-process struct - the
    // worker's decoder reads the same fields. Absent keys mean "no
    // fusion at this end", which is the default and the multi-task
    // path the worker has always used.
    if (fused_source.has_value()) {
        out += ",\"fused_source\":";
        append_chain_op_json(out, *fused_source);
    }
    if (fused_sink.has_value()) {
        out += ",\"fused_sink\":";
        append_chain_op_json(out, *fused_sink);
    }
    out += '}';
    return out;
}

namespace {

ChannelType decode_channel_type(const std::string& name, const std::string& ctx) {
    auto ct = channel_type_from_name(name);
    if (!ct.has_value()) {
        throw std::runtime_error("OperatorChainSpec: unknown channel_type '" + name + "' in " +
                                 ctx);
    }
    return *ct;
}

SubtaskEdge edge_from_json(const config::JsonValue& v, const std::string& ctx) {
    SubtaskEdge e;
    e.peer_role = v.string_or("peer_role", "");
    if (e.peer_role.empty()) {
        throw std::runtime_error("OperatorChainSpec: edge missing 'peer_role' in " + ctx);
    }
    e.peer_subtask_idx = static_cast<std::uint32_t>(v.int_or("peer_subtask_idx", 0));
    e.channel_type = decode_channel_type(v.string_or("channel_type", "int64"), ctx);
    e.input_index = static_cast<std::uint32_t>(v.int_or("input_index", 0));
    return e;
}

}  // namespace

OperatorChainSpec OperatorChainSpec::from_json(std::string_view json_text) {
    auto root = config::parse(json_text);
    if (!root.is_object()) {
        throw std::runtime_error("OperatorChainSpec::from_json: top-level must be an object");
    }
    OperatorChainSpec spec;
    spec.subtask_idx = static_cast<std::uint32_t>(root.int_or("subtask_idx", 0));
    spec.subtask_idx_in_op = static_cast<std::uint32_t>(root.int_or("subtask_idx_in_op", 0));
    if (!root.contains("ops") || !root.at("ops").is_array()) {
        throw std::runtime_error("OperatorChainSpec::from_json: missing 'ops' array");
    }
    auto chain_op_from_json = [](const config::JsonValue& opv) -> ChainOp {
        if (!opv.is_object()) {
            throw std::runtime_error("OperatorChainSpec::from_json: each op must be an object");
        }
        ChainOp op;
        op.id = opv.string_or("id", "");
        op.type = opv.string_or("type", "");
        if (op.type.empty()) {
            throw std::runtime_error("OperatorChainSpec::from_json: op missing 'type'");
        }
        op.uid = opv.string_or("uid", "");
        op.display_name = opv.string_or("display_name", "");
        op.kind = operator_kind_from_name(opv.string_or("kind", "operator"));
        op.in_channel = decode_channel_type(opv.string_or("in_channel", "int64"), op.id);
        op.out_channel = decode_channel_type(opv.string_or("out_channel", "int64"), op.id);
        op.parallelism = static_cast<std::uint32_t>(opv.int_or("parallelism", 1));
        if (opv.contains("params")) {
            const auto& pv = opv.at("params");
            if (!pv.is_object()) {
                throw std::runtime_error("OperatorChainSpec::from_json: 'params' must be object");
            }
            for (const auto& [k, val] : pv.as_object()) {
                if (val.is_string()) {
                    op.params.emplace(k, val.as_string());
                } else if (val.is_number()) {
                    op.params.emplace(k,
                                      std::to_string(static_cast<std::int64_t>(val.as_number())));
                } else if (val.is_bool()) {
                    op.params.emplace(k, val.as_bool() ? "true" : "false");
                }
            }
        }
        if (opv.contains("side_outputs")) {
            for (const auto& sv : opv.at("side_outputs").as_array()) {
                ChainSideOutput so;
                so.tag = sv.string_or("tag", "");
                so.channel_type = decode_channel_type(sv.string_or("channel_type", "int64"), op.id);
                op.side_outputs.push_back(std::move(so));
            }
        }
        return op;
    };
    for (const auto& opv : root.at("ops").as_array()) {
        spec.ops.push_back(chain_op_from_json(opv));
    }
    if (root.contains("input_edges")) {
        for (const auto& v : root.at("input_edges").as_array()) {
            spec.input_edges.push_back(edge_from_json(v, "input_edges"));
        }
    }
    {
        const auto routing = root.string_or("output_routing", "broadcast");
        spec.output_routing = (routing == "split") ? OperatorChainSpec::OutputRouting::Split
                                                   : OperatorChainSpec::OutputRouting::Broadcast;
        spec.output_selector_fn = root.string_or("output_selector_fn", "");
    }
    if (root.contains("output_groups")) {
        for (const auto& gv : root.at("output_groups").as_array()) {
            if (!gv.is_object()) {
                throw std::runtime_error(
                    "OperatorChainSpec::from_json: each output group must be an object");
            }
            SubtaskOutputGroup g;
            const auto mode_str = gv.string_or("mode", "forward");
            if (mode_str == "rebalance") {
                g.mode = RoutingMode::Rebalance;
            } else if (mode_str == "hash") {
                g.mode = RoutingMode::Hash;
            } else {
                g.mode = RoutingMode::Forward;
            }
            g.key_extractor_fn = gv.string_or("key_extractor_fn", "");
            g.side_output_tag = gv.string_or("side_output_tag", "");
            if (gv.contains("edges")) {
                for (const auto& ev : gv.at("edges").as_array()) {
                    g.edges.push_back(edge_from_json(ev, "output_groups.edges"));
                }
            }
            spec.output_groups.push_back(std::move(g));
        }
    }
    if (root.contains("fused_source")) {
        spec.fused_source = chain_op_from_json(root.at("fused_source"));
    }
    if (root.contains("fused_sink")) {
        spec.fused_sink = chain_op_from_json(root.at("fused_sink"));
    }
    return spec;
}

std::size_t total_subtask_count(const JobGraphSpec& graph) {
    std::size_t total = 0;
    for (const auto& op : graph.ops) {
        total += op.parallelism;
    }
    return total;
}

// Parse one entry of OperatorSpec.inputs.
//   "id"          -> (id, branch=0, explicit=false, tag="")
//   "id.N"        -> (id, branch=N, explicit=true,  tag="")
//   "id::tagname" -> (id, branch=0, explicit=false, tag="tagname")
// Split-style ".N" suffixes are for typed branch routing on the
// upstream's main output; "::tag" suffixes consume a named side output
// (different element type from the main).
struct InputRef {
    std::string id;
    std::uint32_t branch{0};
    bool explicit_branch{false};
    std::string side_tag;
};

namespace {
InputRef parse_input_ref(const std::string& s) {
    // "id::tag" form takes precedence over the dot form. The "::" is an
    // explicit, unambiguous separator that can't collide with an op id
    // (op ids are forbidden from containing ':' in the v1 grammar).
    if (const auto colons = s.find("::"); colons != std::string::npos) {
        return InputRef{s.substr(0, colons), 0, false, s.substr(colons + 2)};
    }
    const auto dot = s.rfind('.');
    if (dot == std::string::npos) {
        return InputRef{s, 0, false, {}};
    }
    // Tolerate dots in op-ids: only treat the suffix as a branch index
    // when it parses cleanly as a non-negative integer.
    const auto suffix = s.substr(dot + 1);
    if (suffix.empty()) {
        return InputRef{s, 0, false, {}};
    }
    for (char c : suffix) {
        if (c < '0' || c > '9') {
            return InputRef{s, 0, false, {}};
        }
    }
    try {
        return InputRef{s.substr(0, dot), static_cast<std::uint32_t>(std::stoul(suffix)), true, {}};
    } catch (...) {
        return InputRef{s, 0, false, {}};
    }
}
}  // namespace

JobPlan plan_job(const JobGraphSpec& graph, const OperatorRegistry& registry) {
    return plan_job(graph, registry, RunnerRegistry::default_instance());
}

JobPlan plan_job(const JobGraphSpec& graph,
                 const OperatorRegistry& registry,
                 const RunnerRegistry& runner_reg_param) {
    graph.validate();
    // Make sure built-in types live in the default RunnerRegistry before
    // we validate against it. Idempotent. The per-job runner_reg_param
    // typically parents at default-instance so this populates the
    // fallback layer of its lookups too.
    ensure_built_ins_registered();

    // Determine each op's role in the topology, accounting for the
    // "id.N" branch syntax. consumer_count counts how many downstreams
    // reference the op overall; per_branch_consumers tracks which
    // consumers go to which branch for split ops.
    std::unordered_map<std::string, std::size_t> consumer_count;
    // Main-output-only consumer count (side-output refs excluded). Used
    // to decide chain extension: an op with side outputs can still be
    // chained if its main output goes to exactly one downstream - the
    // side-output consumers don't block chaining because they're wired
    // separately on the chain's outbound groups (one group per side tag).
    std::unordered_map<std::string, std::size_t> main_consumer_count;
    std::unordered_map<std::string, std::map<std::uint32_t, std::vector<const OperatorSpec*>>>
        branch_consumers;
    std::unordered_set<std::string> split_ops;
    for (const auto& op : graph.ops) {
        for (const auto& raw : op.inputs) {
            const auto ref = parse_input_ref(raw);
            ++consumer_count[ref.id];
            if (ref.side_tag.empty()) {
                ++main_consumer_count[ref.id];
            }
            branch_consumers[ref.id][ref.branch].push_back(&op);
            if (ref.explicit_branch) {
                split_ops.insert(ref.id);
            }
        }
    }
    // Validation that the split op declares a selector_fn happens
    // below, once by_id is built.

    // Build an op-id -> OperatorSpec* index for quick lookups when
    // walking input/output edges.
    std::unordered_map<std::string, const OperatorSpec*> by_id;
    by_id.reserve(graph.ops.size());
    for (const auto& op : graph.ops) {
        by_id[op.id] = &op;
    }

    // Now we have by_id - finish split validation: each split op must
    // carry a selector_fn param naming an entry in SelectorRegistry.
    for (const auto& sid : split_ops) {
        auto it = by_id.find(sid);
        if (it == by_id.end()) {
            continue;
        }
        const auto sel = param_string(*it->second, "selector_fn", "");
        if (sel.empty()) {
            throw std::runtime_error("plan_job: split op '" + sid +
                                     "' has consumers using branch syntax but no "
                                     "'selector_fn' param");
        }
    }

    // Parallelism constraint: every op must have parallelism >= 1.
    // Adjacent ops with matching parallelism get Forward (1:1) edges;
    // adjacent ops with mismatched parallelism get Rebalance edges
    // (each upstream subtask round-robins records across all downstream
    // subtasks, broadcasts watermarks/barriers).
    for (const auto& op : graph.ops) {
        if (op.parallelism == 0) {
            throw std::runtime_error("plan_job: op '" + op.id + "' parallelism must be >= 1");
        }
    }

    // Resolve the channel type an input edge actually carries, accounting
    // for "<op_id>::<tag>" side-output references. For a main-output ref
    // the channel type is the upstream's `out_channel`; for a side-output
    // ref it's the matching `SideOutputDecl::channel_type` declared by
    // the upstream op (typed side outputs carry a different element type
    // from the main channel). Returns nullopt when the upstream id isn't
    // in the graph or when a side-tag isn't declared on the upstream.
    auto resolve_in_channel = [&](const std::string& input_ref) -> std::optional<ChannelType> {
        const auto ref = parse_input_ref(input_ref);
        auto it = by_id.find(ref.id);
        if (it == by_id.end()) {
            return std::nullopt;
        }
        if (ref.side_tag.empty()) {
            return it->second->out_channel;
        }
        for (const auto& sd : it->second->side_outputs) {
            if (sd.tag == ref.side_tag) {
                return sd.channel_type;
            }
        }
        return std::nullopt;
    };

    // Resolve a co-op's two input channel types from its upstreams,
    // honouring the side-output ref form. Returns (in1, in2) in the order
    // of op.inputs.
    auto resolve_co_op_in_channels =
        [&](const OperatorSpec& op) -> std::optional<std::pair<ChannelType, ChannelType>> {
        if (op.inputs.size() != 2) {
            return std::nullopt;
        }
        auto c0 = resolve_in_channel(op.inputs[0]);
        auto c1 = resolve_in_channel(op.inputs[1]);
        if (!c0.has_value() || !c1.has_value()) {
            return std::nullopt;
        }
        return std::make_pair(*c0, *c1);
    };
    auto is_co_op = [&](const OperatorSpec& op) {
        auto ins = resolve_co_op_in_channels(op);
        if (!ins.has_value()) {
            return false;
        }
        // RunnerRegistry::find_co_operator canonicalizes (in1, in2) at
        // both register and find sites, so order is irrelevant - the
        // planner doesn't need to know whether the user happened to
        // template <In1=X, In2=Y> or <In1=Y, In2=X>. The runner closure
        // partitions in_bridges by channel_type at dispatch time so the
        // CoOperator's process_element1 / process_element2 still see
        // the templated types they expect.
        return runner_reg_param.find_co_operator(
                   op.type, ins->first, ins->second, op.out_channel) != nullptr;
    };

    // Verify every op resolves in the registry to a factory matching its
    // declared role and channel types.
    for (const auto& op : graph.ops) {
        const bool is_source = op.inputs.empty();
        const bool is_sink = consumer_count[op.id] == 0;
        if (is_source && is_sink) {
            throw std::runtime_error("plan_job: op '" + op.id +
                                     "' has no inputs and no consumers; nothing to do");
        }
        if (is_join_op_type(op.type, &runner_reg_param)) {
            // Join ops are dispatched specially on the worker; their
            // factory isn't in the standard (type, in, out) registry.
            if (op.inputs.size() != 2) {
                throw std::runtime_error("plan_job: join op '" + op.id +
                                         "' must have exactly 2 inputs");
            }
            continue;
        }
        if (op.inputs.size() == 2 && is_co_op(op)) {
            // CoOperator dispatch goes through RunnerRegistry::find_co_operator;
            // bypass the single-input (type, in, out) factory check.
            continue;
        }
        // Look the op up in either registry. The legacy OperatorRegistry
        // (`registry` param) holds the built-ins; the per-job
        // RunnerRegistry (`runner_reg_param`) holds plugin/inline-lambda
        // registrations and falls through to the default singleton for
        // built-ins.
        const auto& runner_reg = runner_reg_param;
        if (is_source) {
            const bool old_ok = registry.find_source(op.type, op.out_channel) != nullptr;
            const bool new_ok = runner_reg.find_source(op.type, op.out_channel) != nullptr;
            if (!old_ok && !new_ok) {
                throw std::runtime_error("plan_job: no source factory registered for type '" +
                                         op.type +
                                         "' with out=" + channel_type_name(op.out_channel));
            }
        } else if (is_sink) {
            ChannelType in_ct = op.out_channel;
            for (const auto& in : op.inputs) {
                if (auto resolved = resolve_in_channel(in); resolved.has_value()) {
                    in_ct = *resolved;
                    break;
                }
            }
            const bool old_ok = registry.find_sink(op.type, in_ct) != nullptr;
            const bool new_ok = runner_reg.find_sink(op.type, in_ct) != nullptr;
            if (!old_ok && !new_ok) {
                throw std::runtime_error("plan_job: no sink factory registered for type '" +
                                         op.type + "' with in=" + channel_type_name(in_ct));
            }
        } else {
            ChannelType in_ct = op.out_channel;
            for (const auto& in : op.inputs) {
                if (auto resolved = resolve_in_channel(in); resolved.has_value()) {
                    in_ct = *resolved;
                    break;
                }
            }
            const bool old_ok = registry.find_operator(op.type, in_ct, op.out_channel) != nullptr;
            const bool new_ok = runner_reg.find_operator(op.type, in_ct, op.out_channel) != nullptr;
            if (!old_ok && !new_ok) {
                throw std::runtime_error("plan_job: no operator factory registered for type '" +
                                         op.type + "' with in=" + channel_type_name(in_ct) +
                                         " out=" + channel_type_name(op.out_channel));
            }
        }
    }

    // ----- Operator chaining --------------------------------------------
    //
    // Group adjacent Operator-kind ops into chains so their dispatch
    // runs in one thread via ChainedOperator (no BoundedChannel between
    // them). Eligibility:
    //   * Both ops are kind = Operator (sources/sinks stay unchained)
    //   * Upstream op has exactly one consumer (this op)
    //   * Downstream op has exactly one input (the upstream op)
    //   * Same parallelism
    //
    // Greedy left-to-right pairing. Chain length is unbounded - the
    // generic role on the worker folds N ops via repeated composition,
    // which keeps the template-instantiation matrix at 8 (one
    // ChainedOperator<A,B,C> per channel-type triple) regardless of N.
    constexpr std::size_t kMaxChainLength = 64;

    auto is_operator_kind = [&](const OperatorSpec& op) {
        const bool is_source = op.inputs.empty();
        const bool is_sink = consumer_count[op.id] == 0;
        return !is_source && !is_sink;
    };

    // chain_groups[op.id] -> list of ops in this group, in order.
    // chain_lead[op.id] -> id of the lead op of this chain (the first one).
    std::unordered_map<std::string, std::string> chain_lead;
    std::unordered_map<std::string, std::vector<const OperatorSpec*>> chain_groups;
    for (const auto& op : graph.ops) {
        chain_lead[op.id] = op.id;
        chain_groups[op.id] = {&op};
    }
    for (const auto& op : graph.ops) {
        if (!is_operator_kind(op) || op.parallelism == 0) {
            continue;
        }
        // Split ops can't be chained - their output goes through a
        // selector-routed Dag::add_split, not a direct pass-through.
        if (split_ops.contains(op.id)) {
            continue;
        }
        // Join ops can't be chained: they have 2 typed inputs that
        // need separate Dag stages, not a single chained Operator.
        if (is_join_op_type(op.type, &runner_reg_param)) {
            continue;
        }
        // CoOperators have 2 typed inputs too; same restriction.
        if (is_co_op(op)) {
            continue;
        }
        // Keyed ops CAN be chain leads: the Hash partitioner runs on
        // the upstream's outbound edge (upstream emits to this op's
        // subtask via Hash routing). Inside the chain the keyed op
        // processes the already-routed records normally and emits
        // through the chain's tail. The separate check on
        // `consumer->key_by` below still prevents a keyed op from
        // being absorbed INTO an upstream's chain (which would
        // short-circuit the partitioner).
        //
        // (Was previously gated; lifted when chain dispatch was
        // generalized to support user-registered channel types via
        // the DagBuilder path in worker.cpp.)
        // Side-output-emitting ops CAN be chained as of 2026-05-22.
        // The chain runtime already propagates side_output_channels
        // from the chain RC to each inner op's RC (see
        // ChainedOperator::open), and the chain spec carries each
        // inner op's side_outputs declarations. The remaining
        // bookkeeping - adding one output group per (inner op, side
        // tag) to the chain spec - happens further below in the
        // downstream-resolution path.
        const auto& lead_id = chain_lead.at(op.id);
        auto& this_group = chain_groups.at(lead_id);
        if (this_group.size() >= kMaxChainLength) {
            continue;
        }
        // Chain extension only requires the MAIN output to have
        // exactly one downstream consumer (= the next op in the
        // chain). Side outputs may fan out to any number of
        // additional consumers without blocking chaining.
        if (main_consumer_count[op.id] != 1) {
            continue;
        }
        // Find the unique consumer.
        const OperatorSpec* consumer = nullptr;
        for (const auto& dop : graph.ops) {
            for (const auto& raw : dop.inputs) {
                if (parse_input_ref(raw).id == op.id) {
                    consumer = &dop;
                    break;
                }
            }
            if (consumer != nullptr) {
                break;
            }
        }
        if (consumer == nullptr) {
            continue;
        }
        if (!is_operator_kind(*consumer)) {
            continue;
        }
        if (consumer->inputs.size() != 1 || parse_input_ref(consumer->inputs.front()).id != op.id) {
            continue;
        }
        if (consumer->parallelism != op.parallelism) {
            continue;
        }
        // Keyed consumers can't be absorbed into the upstream's chain
        // when parallelism > 1: folding them in short-circuits the
        // hash partitioner so records would flow direct from the chain
        // head's input edge to the keyed op without ever passing
        // through the routing layer that partitions records by key.
        //
        // At parallelism = 1 every key resolves to subtask 0 anyway,
        // so the partitioner is a no-op and we can safely chain
        // across the key_by boundary. This is the common case for
        // single-subtask jobs where avoiding the cross-thread channel
        // serde is a large win on per-record cost.
        if (!consumer->key_by.empty() && (op.parallelism > 1 || consumer->parallelism > 1)) {
            continue;
        }
        // Only chain ops that are findable in OperatorRegistry. The
        // chain dispatch path on the worker (legacy run_generic_subtask_
        // fallback for chain.ops.size() >= 2) looks each op up there;
        // RunnerRegistry-only ops (inline-lambda fluent API ops, plugin
        // ops registered via PluginRegistry) would fail with
        // "operator factory not found" at runtime. When in doubt about
        // dispatchability, skip chaining and let each op run in its
        // own subtask via the per-op RunnerRegistry path.
        auto resolve_in_ct = [&](const OperatorSpec& target) -> ChannelType {
            ChannelType in_ct = target.out_channel;
            for (const auto& in : target.inputs) {
                auto upit = by_id.find(parse_input_ref(in).id);
                if (upit != by_id.end()) {
                    in_ct = upit->second->out_channel;
                    break;
                }
            }
            return in_ct;
        };
        if (registry.find_operator(op.type, resolve_in_ct(op), op.out_channel) == nullptr) {
            continue;
        }
        if (registry.find_operator(
                consumer->type, resolve_in_ct(*consumer), consumer->out_channel) == nullptr) {
            continue;
        }
        // Merge consumer into op's chain group.
        this_group.push_back(consumer);
        chain_lead[consumer->id] = lead_id;
        chain_groups.erase(consumer->id);
    }

    // ----- Source/Sink fusion --------------------------------------------
    //
    // For each chain group whose ops are all at parallelism = 1, try to
    // absorb the upstream source and / or the downstream sink. A fused
    // source/sink runs in the chain task's thread (via Dag::add_source /
    // add_sink inline at chain dispatch time), eliminating one
    // inter-thread BoundedChannel hop per direction. The LocalDataPlane
    // bypass already removed codec serde on those hops at par=1; this
    // pass removes the thread synchronisation that remained.
    //
    // Eligibility:
    //   * Chain (all ops) at parallelism = 1.
    //   * Upstream source: par=1, exactly one main consumer (this
    //     chain), no side outputs that are consumed elsewhere, chain
    //     head reads from the source's main output (no side-tag ref).
    //   * Downstream sink: par=1, the chain's tail's only main consumer.
    //   * Source / sink can both be absorbed independently - one,
    //     either, both, or neither.
    //
    // Stored as side-maps keyed by chain lead id so the task-emission
    // loop below knows which OperatorSpec to lift into the chain's
    // fused_source / fused_sink slot.
    // Gate: fusion is opt-in via CLINK_PLAN_FUSE_PAR1=1. Default off
    // so the existing per-op subtask layout - which a lot of tests
    // and operational tooling assume - is preserved. Set to 1 in the
    // bench's run.sh (or any par=1 single-worker job) to take the win.
    const char* fuse_env = std::getenv("CLINK_PLAN_FUSE_PAR1");
    const bool fuse_enabled = (fuse_env != nullptr) && (std::string_view{fuse_env} == "1");
    std::unordered_map<std::string, const OperatorSpec*> chain_fused_source;
    std::unordered_map<std::string, const OperatorSpec*> chain_fused_sink;
    if (!fuse_enabled) {
        // skip the fusion pass entirely - chain_lead / chain_groups
        // stay exactly as the operator-chain loop left them.
    }
    for (const auto& [lead_id, group] : chain_groups) {
        if (!fuse_enabled) {
            break;
        }
        const OperatorSpec& head = *group.front();
        const OperatorSpec& tail = *group.back();
        if (head.parallelism != 1) {
            continue;
        }
        // Only fuse into chain groups that already host an operator -
        // a chain group that is itself a Source-only or Sink-only op
        // stays unfused so the existing single-op runner path
        // dispatches it correctly. (After fusion the chain's ops list
        // must be Operator-kind; folding a sink into a source-only
        // chain would violate that and trigger the "chained ops must
        // all be Operator kind" guard in worker.cpp.)
        if (!is_operator_kind(head) || !is_operator_kind(tail)) {
            continue;
        }
        // Upstream source absorption.
        if (!head.inputs.empty()) {
            const auto up_ref = parse_input_ref(head.inputs.front());
            if (up_ref.side_tag.empty()) {
                if (auto up_it = by_id.find(up_ref.id); up_it != by_id.end()) {
                    const OperatorSpec& upstream = *up_it->second;
                    const bool is_source = upstream.inputs.empty();
                    const bool par_one = upstream.parallelism == 1;
                    const bool unique_main_consumer =
                        main_consumer_count[upstream.id] == 1 && consumer_count[upstream.id] == 1;
                    const bool no_side_outputs = upstream.side_outputs.empty();
                    if (is_source && par_one && unique_main_consumer && no_side_outputs) {
                        chain_fused_source[lead_id] = &upstream;
                    }
                }
            }
        }
        // Downstream sink absorption: find the unique consumer of tail.
        if (main_consumer_count[tail.id] == 1 && consumer_count[tail.id] == 1) {
            const OperatorSpec* candidate = nullptr;
            for (const auto& dop : graph.ops) {
                for (const auto& raw : dop.inputs) {
                    const auto ref = parse_input_ref(raw);
                    if (ref.id == tail.id && ref.side_tag.empty()) {
                        candidate = &dop;
                        break;
                    }
                }
                if (candidate != nullptr) {
                    break;
                }
            }
            if (candidate != nullptr) {
                const bool is_sink = consumer_count[candidate->id] == 0;
                const bool par_one = candidate->parallelism == 1;
                if (is_sink && par_one) {
                    chain_fused_sink[lead_id] = candidate;
                }
            }
        }
    }
    // Remap chain_lead so the absorbed source/sink no longer leads its
    // own chain group - it'll be folded into the chain spec's
    // fused_source / fused_sink slot by the task-emission loop below.
    for (const auto& [lead_id, src] : chain_fused_source) {
        chain_lead[src->id] = lead_id;
        chain_groups.erase(src->id);
    }
    for (const auto& [lead_id, snk] : chain_fused_sink) {
        chain_lead[snk->id] = lead_id;
        chain_groups.erase(snk->id);
    }

    // Allocate subtask_idxs per chain group. Each chain group gets
    // parallelism subtask_idxs. op_subtasks[op.id][i] is the subtask
    // for op's i-th parallel unit (within its chain).
    std::unordered_map<std::string, std::vector<std::uint32_t>> op_subtasks;
    op_subtasks.reserve(graph.ops.size());
    std::uint32_t next_idx = 0;
    for (const auto& op : graph.ops) {
        if (chain_lead.at(op.id) == op.id) {
            // Lead of a chain group. Allocate one set of subtask_idxs
            // for the whole chain.
            const auto par = op.parallelism;
            std::vector<std::uint32_t> idxs;
            idxs.reserve(par);
            for (std::uint32_t i = 0; i < par; ++i) {
                idxs.push_back(next_idx++);
            }
            // Every op in the chain shares the same subtask_idxs - they
            // all run in the same subtask.
            for (const auto* m : chain_groups.at(op.id)) {
                op_subtasks[m->id] = idxs;
            }
        }
    }

    JobPlan plan;
    plan.tasks.reserve(next_idx);

    // Only emit tasks for chain leads. Chain followers are folded into
    // the lead's OperatorChainSpec.ops list and don't get their own
    // subtask.
    for (const auto& op : graph.ops) {
        if (chain_lead.at(op.id) != op.id) {
            continue;
        }
        const auto& group = chain_groups.at(op.id);
        const auto& head = *group.front();
        const auto& tail = *group.back();

        // Topology classification is by the chain's endpoints: a chain
        // is "source-led" only if head is a source, "sink-led" only if
        // tail is a sink. The chain itself is one logical subtask.
        const bool head_is_source = head.inputs.empty();
        const bool tail_is_sink = consumer_count[tail.id] == 0;

        // Resolve incoming channel type from one of head's upstreams.
        ChannelType chain_in_ct = head.out_channel;
        if (!head_is_source) {
            const auto in_ref = parse_input_ref(head.inputs.front());
            auto up_it = by_id.find(in_ref.id);
            if (up_it != by_id.end()) {
                if (!in_ref.side_tag.empty()) {
                    // Consumer reads from a named side output of the
                    // upstream - the channel type lives on the
                    // upstream's SideOutputDecl, not its main out_channel.
                    for (const auto& sd : up_it->second->side_outputs) {
                        if (sd.tag == in_ref.side_tag) {
                            chain_in_ct = sd.channel_type;
                            break;
                        }
                    }
                } else {
                    chain_in_ct = up_it->second->out_channel;
                }
            }
        }

        // Find downstream consumers of the tail. Each entry pairs a
        // consumer with a side-output tag - "" means consuming the
        // main output, non-empty names a side output. Split tails
        // group by branch index for ordering.
        const bool tail_is_split = split_ops.contains(tail.id);
        struct Downstream {
            const OperatorSpec* consumer;
            std::uint32_t branch;
            std::string side_tag;
        };
        std::vector<Downstream> downstreams;
        // Set of every inner op's id so we can match side-output
        // refs against any of them (not just the tail). The chain
        // runner shares one side_output_channels map across all
        // inner ops, so the chain's outbound side groups don't need
        // to discriminate which inner op emits which tag.
        std::unordered_set<std::string> chain_op_ids;
        const auto& chain_group_ops = group;  // alias to avoid shadowing below
        for (const auto* m : chain_group_ops) {
            chain_op_ids.insert(m->id);
        }
        if (tail_is_split) {
            for (const auto& [branch, consumers] : branch_consumers[tail.id]) {
                for (const auto* c : consumers) {
                    downstreams.push_back({c, branch, {}});
                }
            }
        } else {
            for (const auto& dop : graph.ops) {
                // Skip downstreams that are themselves part of THIS
                // chain - their "consumer" relationship is internal
                // and doesn't generate an outbound group.
                if (chain_op_ids.contains(dop.id)) {
                    continue;
                }
                // Side-output consumers: a ref `op_id::tag` where
                // op_id is any inner chain op.
                for (const auto& raw : dop.inputs) {
                    const auto ref = parse_input_ref(raw);
                    if (chain_op_ids.contains(ref.id) && !ref.side_tag.empty()) {
                        downstreams.push_back({&dop, 0U, ref.side_tag});
                    }
                }
                // Main consumer: a ref to the chain TAIL with empty
                // side_tag. Inner ops can't have external main
                // consumers (main_consumer_count == 1 means the only
                // main consumer is the next op IN the chain).
                for (const auto& raw : dop.inputs) {
                    const auto ref = parse_input_ref(raw);
                    if (ref.id == tail.id && ref.side_tag.empty()) {
                        downstreams.push_back({&dop, 0U, {}});
                        break;
                    }
                }
            }
        }

        const auto& chain_subs = op_subtasks.at(op.id);
        // Resolve fused source/sink (if any) for this chain group's lead.
        // `op.id` here is the chain lead id (matches the outer loop).
        const auto fused_source_it = chain_fused_source.find(op.id);
        const auto fused_sink_it = chain_fused_sink.find(op.id);
        const OperatorSpec* fused_source_spec =
            fused_source_it != chain_fused_source.end() ? fused_source_it->second : nullptr;
        const OperatorSpec* fused_sink_spec =
            fused_sink_it != chain_fused_sink.end() ? fused_sink_it->second : nullptr;

        for (std::uint32_t sub_i = 0; sub_i < head.parallelism; ++sub_i) {
            OperatorChainSpec chain;
            chain.subtask_idx = chain_subs[sub_i];
            chain.subtask_idx_in_op = sub_i;

            // Fold the absorbed source / sink into the chain spec. The
            // worker-side dispatch will build them via OperatorRegistry and
            // attach them directly to the dag (Dag::add_source for the
            // head, Dag::add_sink for the tail) - no in/out bridges
            // for the fused ends, no separate subtasks for them.
            auto make_chain_op = [](const OperatorSpec& s, OperatorKind kind) {
                std::vector<ChainSideOutput> side_outs;
                side_outs.reserve(s.side_outputs.size());
                for (const auto& so : s.side_outputs) {
                    side_outs.push_back(
                        ChainSideOutput{.tag = so.tag, .channel_type = so.channel_type});
                }
                return ChainOp{
                    .id = s.id,
                    .type = s.type,
                    .uid = s.uid,
                    .display_name = s.display_name,
                    .kind = kind,
                    .in_channel = s.out_channel,  // for Source: read-side type is out_channel
                    .out_channel = s.out_channel,
                    .parallelism = s.parallelism,
                    .params = s.params,
                    .side_outputs = std::move(side_outs),
                };
            };
            if (fused_source_spec != nullptr) {
                chain.fused_source = make_chain_op(*fused_source_spec, OperatorKind::Source);
            }
            if (fused_sink_spec != nullptr) {
                ChainOp sink_op = make_chain_op(*fused_sink_spec, OperatorKind::Sink);
                // For a sink the relevant channel is its in_channel
                // (records arrive here). Override the default we
                // copied from out_channel.
                sink_op.in_channel = chain_group_ops.back()->out_channel;
                sink_op.out_channel = sink_op.in_channel;
                chain.fused_sink = std::move(sink_op);
            }

            // Append every op in the chain in order. For 2-op chains,
            // the middle channel type is the upstream-op's out_channel.
            ChannelType prev_out = chain_in_ct;
            for (std::size_t i = 0; i < group.size(); ++i) {
                const auto& cop = *group[i];
                const bool cop_is_source = (i == 0) && head_is_source;
                const bool cop_is_sink = (i + 1 == group.size()) && tail_is_sink;
                const bool cop_is_join = is_join_op_type(cop.type, &runner_reg_param);
                const bool cop_is_co_op = is_co_op(cop);
                OperatorKind cop_kind = OperatorKind::Operator;
                if (cop_is_join) {
                    cop_kind = OperatorKind::Join;
                } else if (cop_is_co_op) {
                    cop_kind = OperatorKind::CoOperator;
                } else if (cop_is_source) {
                    cop_kind = OperatorKind::Source;
                } else if (cop_is_sink) {
                    cop_kind = OperatorKind::Sink;
                } else {
                    cop_kind = OperatorKind::Operator;
                }
                const auto cop_in = cop_is_source ? cop.out_channel : prev_out;
                std::vector<ChainSideOutput> chain_side_outs;
                chain_side_outs.reserve(cop.side_outputs.size());
                for (const auto& s : cop.side_outputs) {
                    chain_side_outs.push_back(
                        ChainSideOutput{.tag = s.tag, .channel_type = s.channel_type});
                }
                chain.ops.push_back(ChainOp{
                    .id = cop.id,
                    .type = cop.type,
                    .uid = cop.uid,
                    .display_name = cop.display_name,
                    .kind = cop_kind,
                    .in_channel = cop_in,
                    .out_channel = cop.out_channel,
                    .parallelism = cop.parallelism,
                    .params = cop.params,
                    .side_outputs = std::move(chain_side_outs),
                });
                prev_out = cop.out_channel;
            }

            // Input edges feed the head of the chain. For each upstream
            // op, decide forward (same-indexed peer, 1 edge) vs rebalance
            // (every upstream subtask, N edges) based on parallelism.
            // A keyed head also requires fan-in from every upstream
            // subtask: each one is hash-routing per record, so this
            // downstream subtask receives whatever slice landed on its
            // hash bucket - which means listening on N inbound bridges.
            const bool head_is_keyed = !head.key_by.empty();
            // When the chain absorbed its upstream source, the head no
            // longer reads from a NetworkBridgeSource - skip the
            // input_edges loop entirely so the worker-side dispatch goes
            // through the fused_source path.
            if (!head_is_source && fused_source_spec == nullptr) {
                std::uint32_t in_ord = 0;  // logical input position (In1=0, In2=1, ...)
                for (const auto& raw : head.inputs) {
                    const std::uint32_t logical_in_idx = in_ord++;
                    const auto ref = parse_input_ref(raw);
                    auto up_it = by_id.find(ref.id);
                    if (up_it == by_id.end()) {
                        continue;
                    }
                    const auto& up_subs = op_subtasks.at(ref.id);
                    const bool forward =
                        !head_is_keyed && (up_it->second->parallelism == head.parallelism);
                    // Resolve the channel type for this edge. Side-tag
                    // refs read from a named side output whose channel
                    // type lives on the upstream's SideOutputDecl;
                    // everything else reads the upstream's main
                    // out_channel.
                    ChannelType edge_ct = up_it->second->out_channel;
                    if (!ref.side_tag.empty()) {
                        bool found = false;
                        for (const auto& sd : up_it->second->side_outputs) {
                            if (sd.tag == ref.side_tag) {
                                edge_ct = sd.channel_type;
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            throw std::runtime_error("plan_job: op '" + head.id +
                                                     "' references side output '" + ref.side_tag +
                                                     "' of '" + ref.id +
                                                     "' but that op did not declare it");
                        }
                    }
                    if (forward) {
                        chain.input_edges.push_back(SubtaskEdge{
                            .peer_role = kGenericSubtaskRole,
                            .peer_subtask_idx = up_subs[sub_i],
                            .channel_type = edge_ct,
                            .input_index = logical_in_idx,
                        });
                    } else {
                        // Rebalance or Hash: this downstream subtask
                        // receives records from every upstream subtask.
                        for (auto up_sub : up_subs) {
                            chain.input_edges.push_back(SubtaskEdge{
                                .peer_role = kGenericSubtaskRole,
                                .peer_subtask_idx = up_sub,
                                .channel_type = edge_ct,
                                .input_index = logical_in_idx,
                            });
                        }
                    }
                }
            }

            // Output groups leave from the tail of the chain. One group
            // per downstream consumer. Mode depends on parallelism:
            //   * forward when tail.parallelism == downstream.parallelism
            //   * rebalance otherwise (records round-robin across all
            //     downstream subtasks; watermarks/barriers broadcast)
            // For split tails, groups are ordered by branch index and
            // the chain's output_routing is set to Split.
            if (tail_is_split) {
                chain.output_routing = OperatorChainSpec::OutputRouting::Split;
                chain.output_selector_fn = param_string(tail, "selector_fn", "");
            }
            for (const auto& ds : downstreams) {
                const auto* dop = ds.consumer;
                // Skip the main-output group for the absorbed sink -
                // the chain's tail emits directly into the inline sink
                // via the fused_sink path; no peer-bridge group needed.
                // Side-output groups for other consumers still go
                // through the normal output_groups path.
                if (fused_sink_spec != nullptr && dop->id == fused_sink_spec->id &&
                    ds.side_tag.empty()) {
                    continue;
                }
                const auto& d_subs = op_subtasks.at(dop->id);
                SubtaskOutputGroup group;
                group.side_output_tag = ds.side_tag;
                const bool forward = (dop->parallelism == tail.parallelism);
                // Side outputs carry a different channel type than the
                // main output. Resolve it across ALL inner ops in the
                // chain so mid-chain emitters work: any inner op may
                // declare the side output with this tag.
                ChannelType edge_channel = tail.out_channel;
                if (!ds.side_tag.empty()) {
                    bool found = false;
                    for (const auto* m : chain_group_ops) {
                        for (const auto& sd : m->side_outputs) {
                            if (sd.tag == ds.side_tag) {
                                edge_channel = sd.channel_type;
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            break;
                        }
                    }
                    if (!found) {
                        throw std::runtime_error("plan_job: consumer '" + dop->id +
                                                 "' references side output '" + ds.side_tag +
                                                 "' but no op in the chain declared it");
                    }
                }
                // If the downstream op is keyed, the edge MUST hash-
                // partition - same key has to land on the same subtask
                // for keyed state to be correct. Hash beats both
                // Forward (would skip the partitioning step entirely)
                // and Rebalance (round-robin would scatter same-key
                // records across subtasks). Watermarks/barriers
                // broadcast over all peers in either case.
                if (!dop->key_by.empty()) {
                    group.mode = RoutingMode::Hash;
                    group.key_extractor_fn = dop->key_by;
                    for (auto d_sub : d_subs) {
                        group.edges.push_back(SubtaskEdge{
                            .peer_role = kGenericSubtaskRole,
                            .peer_subtask_idx = d_sub,
                            .channel_type = edge_channel,
                        });
                    }
                    chain.output_groups.push_back(std::move(group));
                    continue;
                }
                if (forward) {
                    group.mode = RoutingMode::Forward;
                    group.edges.push_back(SubtaskEdge{
                        .peer_role = kGenericSubtaskRole,
                        .peer_subtask_idx = d_subs[sub_i],
                        .channel_type = edge_channel,
                    });
                } else {
                    group.mode = RoutingMode::Rebalance;
                    for (auto d_sub : d_subs) {
                        group.edges.push_back(SubtaskEdge{
                            .peer_role = kGenericSubtaskRole,
                            .peer_subtask_idx = d_sub,
                            .channel_type = edge_channel,
                        });
                    }
                }
                chain.output_groups.push_back(std::move(group));
            }

            PlannedTask t;
            t.worker_id = "";
            t.role = kGenericSubtaskRole;
            t.subtask_idx = chain.subtask_idx;
            t.data_port = 0;
            for (const auto& g : chain.output_groups) {
                for (const auto& e : g.edges) {
                    t.peer_refs.emplace_back(e.peer_role, e.peer_subtask_idx);
                }
            }
            t.extra_config = chain.to_json();
            plan.tasks.push_back(std::move(t));
        }
    }

    return plan;
}

}  // namespace clink::cluster
