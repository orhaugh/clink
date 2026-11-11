// The windowed transformation a job declares, read from the job graph and
// manifest an incident bundle carries (reconcile.hpp, "windowed results").
//
// The audit of windowed results supports one shape and refuses the rest by
// name, so that nothing it reports rests on a semantics it does not model:
//
//   kafka source (one, parallelism = its partitions)
//     -> the JSON bridge
//     -> assign_timestamps_row (directly: it sees every record the source
//        delivers; no idleness)
//     -> filter_row_predicate / project_row / row_compute_key, record-wise,
//        passing the time column through unchanged
//     -> tumbling_window_row (COUNT and SUM, not DISTINCT)
//     -> row_bind_columns
//     -> clickhouse_native_sink
//
// with aligned checkpoints, as the job manifest records them.

#include <algorithm>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "clink/incident/reconcile.hpp"

namespace clink::incident::reconcile {

namespace {

struct GraphOp {
    std::string id;
    std::string type;
    std::vector<std::string> inputs;
    std::map<std::string, std::string> params;
    std::int64_t parallelism{1};
};

bool is_window_type(const std::string& t) {
    return t == "tumbling_window_row" || t == "hopping_window_row" || t == "cumulate_window_row" ||
           t == "session_window_row";
}

// Operators that keep state across records, so a job holding one is not
// record-wise.
bool is_stateful_type(const std::string& t) {
    static const std::set<std::string> kStateful{"aggregate_row",
                                                 "async_lookup_join_row",
                                                 "async_lookup_row",
                                                 "cdc_keep_rows_row",
                                                 "distinct_row",
                                                 "last_n_agg_row",
                                                 "limit_row",
                                                 "match_recognize_row",
                                                 "ml_predict_row",
                                                 "over_aggregate_row",
                                                 "process_table_function_row",
                                                 "top_n_per_key_row",
                                                 "top_n_row",
                                                 "union_row"};
    return is_window_type(t) || kStateful.contains(t) || t.find("join") != std::string::npos;
}

bool is_source_type(const std::string& t) {
    return t.find("source") != std::string::npos;
}

std::optional<std::int64_t> whole(const std::string& s) {
    if (s.empty()) {
        return std::nullopt;
    }
    std::size_t at = 0;
    try {
        const auto v = std::stoll(s, &at);
        if (at != s.size()) {
            return std::nullopt;
        }
        return v;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::vector<std::string> csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t at = 0;
    while (at <= s.size() && !s.empty()) {
        const auto comma = s.find(',', at);
        const auto part = s.substr(at, comma == std::string::npos ? std::string::npos : comma - at);
        if (!part.empty()) {
            out.push_back(part);
        }
        if (comma == std::string::npos) {
            break;
        }
        at = comma + 1;
    }
    return out;
}

}  // namespace

WindowAnalysis analyse_window(const Bundle& b) {
    WindowAnalysis a;
    const auto unsupported = [&](std::string why) -> WindowAnalysis& {
        a.semantics.reset();
        if (a.unsupported.empty()) {
            a.unsupported = std::move(why);
        }
        return a;
    };
    if (b.find("job/manifest.json") == nullptr) {
        return a;  // no graph: nothing says the job is windowed
    }
    config::JsonValue manifest;
    std::vector<GraphOp> ops;
    try {
        std::ifstream in(b.root / "job/manifest.json", std::ios::binary);
        const std::string text((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        manifest = config::parse(text);
        const auto graph = config::parse(manifest.string_or("graph_json", ""));
        for (const auto& o : graph.at("ops").as_array()) {
            GraphOp op;
            op.id = o.string_or("id", "");
            op.type = o.string_or("type", "");
            op.parallelism = o.int_or("parallelism", 1);
            if (o.contains("inputs") && o.at("inputs").is_array()) {
                for (const auto& i : o.at("inputs").as_array()) {
                    op.inputs.push_back(i.is_string() ? i.as_string() : i.serialize());
                }
            }
            if (o.contains("params") && o.at("params").is_object()) {
                for (const auto& [k, v] : o.at("params").as_object()) {
                    op.params[k] = v.is_string() ? v.as_string() : v.serialize();
                }
            }
            ops.push_back(std::move(op));
        }
    } catch (const std::exception&) {
        return a;  // the graph is described elsewhere as unreadable
    }
    a.windowed = std::any_of(
        ops.begin(), ops.end(), [](const GraphOp& op) { return is_stateful_type(op.type); });
    if (!a.windowed) {
        return a;
    }
    WindowSemantics w;

    std::map<std::string, const GraphOp*> by_id;
    for (const auto& op : ops) {
        by_id[op.id] = &op;
    }
    std::vector<const GraphOp*> sinks;
    std::vector<const GraphOp*> sources;
    std::vector<const GraphOp*> windows;
    for (const auto& op : ops) {
        if (op.type == "clickhouse_native_sink") {
            sinks.push_back(&op);
        }
        if (is_source_type(op.type)) {
            sources.push_back(&op);
        }
        if (is_window_type(op.type)) {
            windows.push_back(&op);
        }
    }
    if (windows.empty()) {
        return unsupported(
            "the job keeps state across records without a window (a GROUP BY, join, ranking or "
            "the like); the audit supports record-wise jobs and one tumbling window");
    }
    if (windows.size() > 1 || windows.front()->type != "tumbling_window_row") {
        return unsupported("the job's window is " + windows.front()->type +
                           (windows.size() > 1 ? " among others" : "") +
                           "; the audit supports one tumbling window");
    }
    if (sinks.size() != 1 || sources.size() != 1) {
        return unsupported("the job has " + std::to_string(sources.size()) + " source(s) and " +
                           std::to_string(sinks.size()) +
                           " ClickHouse native sink(s); the audit supports one of each");
    }
    const GraphOp& source = *sources.front();
    if (source.type.find("kafka") == std::string::npos) {
        return unsupported("the job's source is " + source.type +
                           "; the audit supports a Kafka source");
    }

    // The chain, sink first, each operator with exactly one input.
    std::vector<const GraphOp*> chain;
    for (const GraphOp* at = sinks.front(); at != nullptr;) {
        chain.push_back(at);
        if (chain.size() > ops.size()) {
            return unsupported("the job graph has a cycle");
        }
        if (at->inputs.empty()) {
            break;
        }
        if (at->inputs.size() != 1 || !by_id.contains(at->inputs.front())) {
            return unsupported("operator " + at->id + " (" + at->type +
                               ") has several inputs; the audit supports one chain from the "
                               "source to the sink");
        }
        at = by_id.at(at->inputs.front());
    }
    if (chain.size() != ops.size()) {
        return unsupported(
            "the job graph holds operators outside the chain from its source to its sink");
    }
    std::reverse(chain.begin(), chain.end());  // source first
    if (chain.front() != &source) {
        return unsupported("the chain into the sink does not start at the job's source");
    }

    // source -> bridge -> assigner
    std::size_t at = 1;
    if (at < chain.size() && chain[at]->type.rfind("json_string_to_row", 0) == 0) {
        ++at;
    }
    if (at >= chain.size() || chain[at]->type != "assign_timestamps_row") {
        return unsupported(
            "the watermark is not assigned directly after the source: the audit needs the "
            "watermark to advance on every record the source delivers (an event_time_column on "
            "the source table)");
    }
    const GraphOp& assigner = *chain[at++];
    const auto column = assigner.params.contains("column") ? assigner.params.at("column") : "";
    const auto lag =
        whole(assigner.params.contains("out_of_order_ms") ? assigner.params.at("out_of_order_ms")
                                                          : std::string{"0"});
    const auto idle =
        whole(assigner.params.contains("idle_timeout_ms") ? assigner.params.at("idle_timeout_ms")
                                                          : std::string{"0"});
    if (column.empty() || !lag || *lag < 0) {
        return unsupported("the watermark's column or lag cannot be read from the job graph");
    }
    if (!idle || *idle != 0) {
        return unsupported(
            "the source declares idle_timeout_ms: a partition is then left out of the watermark "
            "after a stretch of wall-clock time, which the source's records do not show; the "
            "audit supports a watermark without idleness");
    }
    w.time_column = column;
    w.lag_ms = *lag;

    // assigner -> record-wise operators -> window
    for (; at < chain.size() && chain[at]->type != "tumbling_window_row"; ++at) {
        const GraphOp& op = *chain[at];
        if (op.type == "filter_row_predicate" || op.type == "row_compute_key" ||
            op.type == "identity_row") {
            continue;
        }
        if (op.type == "project_row") {
            // The time column must reach the window as it was when the
            // watermark was assigned.
            try {
                const auto outputs = config::parse(
                    op.params.contains("outputs") ? op.params.at("outputs") : std::string{"[]"});
                bool passed = false;
                for (const auto& o : outputs.as_array()) {
                    if (o.string_or("name", "") != column) {
                        continue;
                    }
                    const auto& e = o.at("expr");
                    passed = e.is_object() && e.as_object().size() == 1 &&
                             e.string_or("col", "") == column;
                }
                if (!passed) {
                    return unsupported("projection " + op.id + " does not pass the time column " +
                                       column +
                                       " through unchanged, so the window would assign records "
                                       "by another time than the watermark advanced on");
                }
            } catch (const std::exception&) {
                return unsupported("projection " + op.id + "'s outputs cannot be read");
            }
            continue;
        }
        return unsupported("operator " + op.id + " (" + op.type +
                           ") sits between the watermark and the window; the audit supports "
                           "record-wise filters and projections there");
    }
    if (at >= chain.size()) {
        return unsupported("the window is not on the chain into the sink");
    }
    const GraphOp& win = *chain[at++];
    w.window_op = win.id;
    const auto size = whole(win.params.contains("size_ms") ? win.params.at("size_ms") : "");
    if (!size || *size <= 0) {
        return unsupported("the window's size cannot be read from the job graph");
    }
    w.size_ms = *size;
    const std::string time_column =
        win.params.contains("time_column") ? win.params.at("time_column") : "";
    if (time_column != column) {
        return unsupported("the window assigns records by " + time_column +
                           " and the watermark advances on " + column);
    }
    const auto lateness =
        whole(win.params.contains("allowed_lateness_ms") ? win.params.at("allowed_lateness_ms")
                                                         : std::string{"0"});
    if (!lateness || *lateness < 0) {
        return unsupported("the window's allowed lateness cannot be read from the job graph");
    }
    w.allowed_lateness_ms = *lateness;
    w.group_keys = csv(win.params.contains("group_keys") ? win.params.at("group_keys") : "");
    auto group_outputs =
        csv(win.params.contains("group_key_outputs") ? win.params.at("group_key_outputs") : "");
    if (group_outputs.size() != w.group_keys.size()) {
        group_outputs = w.group_keys;
    }
    const std::string start_out = win.params.contains("window_start_output")
                                      ? win.params.at("window_start_output")
                                      : "window_start";
    const std::string end_out = win.params.contains("window_end_output")
                                    ? win.params.at("window_end_output")
                                    : "window_end";
    std::vector<WindowAggregate> aggs;
    try {
        const auto list = config::parse(
            win.params.contains("aggregates") ? win.params.at("aggregates") : std::string{"[]"});
        for (const auto& e : list.as_array()) {
            WindowAggregate g;
            g.function = e.string_or("fn", "");
            g.input = e.string_or("input_column", "");
            g.output = e.string_or("name", "");
            if (e.bool_or("distinct", false)) {
                return unsupported("aggregate " + g.output +
                                   " is DISTINCT; the audit supports COUNT and SUM");
            }
            if (g.function != "count" && g.function != "sum") {
                return unsupported("aggregate " + g.output + " is " + g.function +
                                   "; the audit supports COUNT and SUM");
            }
            if (g.function == "sum" && g.input.empty()) {
                return unsupported("aggregate " + g.output + " sums no column");
            }
            aggs.push_back(std::move(g));
        }
    } catch (const std::exception&) {
        return unsupported("the window's aggregates cannot be read from the job graph");
    }

    // window -> row_bind_columns -> sink, naming the window's outputs as the
    // sink's columns.
    std::map<std::string, std::string> to_sink;
    for (const auto& name : group_outputs) {
        to_sink[name] = name;
    }
    to_sink[start_out] = start_out;
    to_sink[end_out] = end_out;
    for (const auto& g : aggs) {
        to_sink[g.output] = g.output;
    }
    for (; at + 1 < chain.size(); ++at) {
        const GraphOp& op = *chain[at];
        if (op.type == "identity_row") {
            continue;
        }
        if (op.type != "row_bind_columns") {
            return unsupported("operator " + op.id + " (" + op.type +
                               ") sits between the window and the sink; the audit supports the "
                               "window's rows written as they are");
        }
        const auto select =
            csv(op.params.contains("select_columns") ? op.params.at("select_columns") : "");
        const auto sink =
            csv(op.params.contains("sink_columns") ? op.params.at("sink_columns") : "");
        if (select.size() != sink.size()) {
            return unsupported("the binding into the sink's columns cannot be read");
        }
        std::map<std::string, std::string> renamed;
        for (std::size_t i = 0; i < select.size(); ++i) {
            for (const auto& [out, current] : to_sink) {
                if (current == select[i]) {
                    renamed[out] = sink[i];
                }
            }
        }
        to_sink = std::move(renamed);
    }
    const auto sink_name = [&](const std::string& out) -> std::string {
        const auto it = to_sink.find(out);
        return it == to_sink.end() ? std::string{} : it->second;
    };
    for (const auto& name : group_outputs) {
        if (sink_name(name).empty()) {
            return unsupported("the group key " + name +
                               " is not written to the sink, so a window's row cannot be "
                               "identified there");
        }
        w.identity.push_back(sink_name(name));
    }
    w.start_column = sink_name(start_out);
    w.end_column = sink_name(end_out);
    if (w.start_column.empty() || w.end_column.empty()) {
        return unsupported(
            "the window's start and end are not both written to the sink, so a window's row "
            "cannot be identified there");
    }
    w.identity.push_back(w.start_column);
    w.identity.push_back(w.end_column);
    for (auto& g : aggs) {
        g.output = sink_name(g.output);
    }
    w.aggregates = std::move(aggs);
    w.source_parallelism =
        static_cast<std::uint64_t>(std::max<std::int64_t>(source.parallelism, 0));

    // Aligned checkpoints: a window operator then processes everything before
    // a barrier on every input, and nothing after it, before the barrier
    // passes.
    if (!manifest.contains("alignment")) {
        return unsupported(
            "the job manifest does not record how checkpoint barriers aligned (a Coordinator "
            "before it was recorded); the audit needs aligned checkpoints");
    }
    w.alignment = manifest.string_or("alignment", "");
    if (w.alignment != "aligned") {
        return unsupported("the job's checkpoints are " + w.alignment +
                           ": a barrier can then pass a window operator before records ahead "
                           "of it on another input, so the watermark at a barrier is not "
                           "bounded by the source's positions; the audit needs aligned ones");
    }
    a.semantics = std::move(w);
    return a;
}

}  // namespace clink::incident::reconcile
