#include "clink/sql/incident_replay.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/row_layout_admission.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/config/json.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/sql/replay.hpp"
#include "clink/sql/row.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::sql {

namespace {

namespace fs = std::filesystem;
using incident::Bundle;
using incident::Check;
using incident::Verdict;

// One captured epoch, as the reader checked it against the file the replay
// engine reads.
using Entry = incident::CapturedEpoch;

std::string entry_name(const Entry& e) {
    return "op " + std::to_string(e.op) + " subtask " + std::to_string(e.subtask) + " epoch " +
           std::to_string(e.epoch);
}

bool selected(const Entry& e, const IncidentReplayOptions& o) {
    return (!o.op || *o.op == e.op) && (!o.subtask || *o.subtask == e.subtask) &&
           (!o.epoch || *o.epoch == e.epoch);
}

std::string read_text(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

// What the capture's op.json records about the operator.
struct CapturedSpec {
    std::string op_type;
    std::string uid;
    std::map<std::string, std::string> params;
};

// The op.json the replay engine reads for the entry's operator and subtask.
CapturedSpec captured_spec(const Bundle& b, const Entry& e) {
    const auto rel =
        "capture/op-" + std::to_string(e.op) + "/subtask-" + std::to_string(e.subtask) + "/op.json";
    if (b.find(rel) == nullptr) {
        return {};
    }
    try {
        const auto v = config::parse(read_text(b.root / rel));
        CapturedSpec s{v.string_or("op_type", ""), v.string_or("uid", ""), {}};
        if (v.contains("params") && v.at("params").is_object()) {
            for (const auto& [k, p] : v.at("params").as_object()) {
                if (p.is_string()) {
                    s.params[std::string{k}] = p.as_string();
                }
            }
        }
        return s;
    } catch (const std::exception&) {
        return {};
    }
}

// Operators that call out of the process for each record: lookups and model
// inference. Verification contacts no service, so it does not replay them;
// `clink replay` does, against the services the job used.
bool calls_external_systems(const std::string& op_type) {
    return op_type.find("lookup") != std::string::npos ||
           op_type.find("predict") != std::string::npos;
}

std::string external_note(const std::string& op_type) {
    return "op type '" + op_type +
           "' calls an external system for each record (a lookup or model inference), and "
           "verification contacts no service";
}

// The restores the bundle records, read once for all the replays.
struct Restores {
    std::vector<cluster::RestoredRecord> known;
    std::string error;  // the RESTORED record does not read
};

Restores read_restores(const Bundle& b) {
    Restores r;
    try {
        r.known = incident::recorded_restores(b);
    } catch (const std::exception& e) {
        r.error = std::string{"the bundle's RESTORED record does not read: "} + e.what();
    }
    return r;
}

// Where a replay of `epoch` starts (incident::epoch_start), and that
// checkpoint's generation directory, resolved over the bundle's own markers as
// a restore would. Checkpoint 0 is fresh state, with no directory. The
// snapshot must be one the bundle lists: a marker's state root can name the
// live directory the bundle was collected from.
struct StartingState {
    Verdict verdict{Verdict::Passed};  // Passed: replayable
    std::uint64_t checkpoint{0};
    std::string dir;
    std::string why;
};

StartingState starting_state(const Bundle& b,
                             const Restores& restores,
                             std::uint64_t epoch,
                             std::uint64_t subtask) {
    StartingState s;
    s.verdict = Verdict::Unavailable;
    if (!restores.error.empty()) {
        s.why = restores.error;
        return s;
    }
    const auto start = incident::epoch_start(b, restores.known, epoch);
    if (!start.known) {
        s.verdict = start.unsupported ? Verdict::Unsupported : Verdict::Unavailable;
        s.why = start.reason;
        return s;
    }
    s.checkpoint = start.checkpoint;
    if (s.checkpoint == 0) {
        s.verdict = Verdict::Passed;
        return s;
    }
    if (b.find("checkpoint/_jobs/" + std::to_string(b.job()) + "/COMPLETED-" +
               std::to_string(s.checkpoint)) == nullptr) {
        s.why = "the bundle holds no COMPLETED marker for checkpoint " +
                std::to_string(s.checkpoint) + ", the epoch's starting state";
        return s;
    }
    try {
        const auto src = cluster::resolve_restore_source(
            cluster::completed_marker_dir_for(b.checkpoint_dir().string(), b.job()).string(),
            s.checkpoint);
        const fs::path dir =
            state_dir_for(src.state_root, src.generation, static_cast<std::uint32_t>(subtask));
        const auto snap =
            b.inside((dir / ("checkpoint-" + std::to_string(s.checkpoint) + ".snap")).string());
        if (!snap.has_value() || b.find(*snap) == nullptr) {
            s.why = "the epoch's starting snapshot (checkpoint " + std::to_string(s.checkpoint) +
                    ", subtask " + std::to_string(subtask) +
                    ") is not in the bundle; collect with --include-state";
            return s;
        }
        s.dir = dir.parent_path().string();
    } catch (const std::exception& e) {
        s.why = "checkpoint " + std::to_string(s.checkpoint) + " cannot be resolved: " + e.what();
        return s;
    }
    s.verdict = Verdict::Passed;
    return s;
}

ReplayRequest replay_request(const Bundle& b,
                             std::uint64_t op,
                             std::uint64_t epoch,
                             std::optional<std::uint64_t> subtask,
                             const StartingState& state) {
    ReplayRequest req;
    req.capture_dir = b.capture_dir().string();
    req.checkpoint_dir = state.dir;
    req.op_id = op;
    req.subtask = subtask;
    req.epoch = std::to_string(epoch);
    req.state_from = state.checkpoint;
    return req;
}

struct Replayed {
    Verdict verdict{Verdict::Unavailable};
    std::string note;
    std::vector<std::string> rows;  // the first run's emissions
    bool partial{false};            // the capture held only part of the epoch
};

// EpochReplay names what it could not do; sort that into the verdicts. An
// operator this build cannot rebuild is unsupported; evidence the bundle lacks
// or holds unreadable is unavailable; an operator that throws on its captured
// events is a failure.
Verdict classify_replay_error(const std::string& what) {
    for (const char* unsupported : {"no registered factory", "replay driver", "not registered"}) {
        if (what.find(unsupported) != std::string::npos) {
            return Verdict::Unsupported;
        }
    }
    for (const char* unavailable : {".snap under",
                                    "cannot open",
                                    "requires checkpoint_dir",
                                    "not a capture file",
                                    "malformed op.json"}) {
        if (what.find(unavailable) != std::string::npos) {
            return Verdict::Unavailable;
        }
    }
    return Verdict::Failed;
}

std::size_t first_difference(const std::vector<std::string>& a, const std::vector<std::string>& b) {
    std::size_t at = 0;
    while (at < a.size() && at < b.size() && a[at] == b[at]) {
        ++at;
    }
    return at;
}

Replayed replay_twice(const Bundle& b, const Restores& restores, const Entry& e) {
    Replayed r;
    const auto state = starting_state(b, restores, e.epoch, e.subtask);
    if (state.verdict != Verdict::Passed) {
        r.verdict = state.verdict;
        r.note = state.why;
        return r;
    }
    try {
        const auto replay = EpochReplay::load(replay_request(b, e.op, e.epoch, e.subtask, state));
        const auto& info = replay.info();
        // The operator the engine is about to build, whatever the manifest says.
        if (calls_external_systems(info.spec.op_type)) {
            r.verdict = Verdict::Unsupported;
            r.note = external_note(info.spec.op_type);
            return r;
        }
        // A capture whose header does not say it was truncated holds every
        // record the header counts, unless the file was cut short.
        if (!info.truncated && info.format_version >= 2 && info.data_count != info.records_seen) {
            r.verdict = Verdict::Unavailable;
            r.partial = true;
            r.note = "the capture holds " + std::to_string(info.data_count) + " of the " +
                     std::to_string(info.records_seen) +
                     " records its header counts: it was cut short when it was written or "
                     "copied, so it cannot stand for the epoch";
            return r;
        }
        r.partial = info.truncated;
        r.rows = replay.run();
        const auto again = replay.run();
        if (again != r.rows) {
            r.verdict = Verdict::Failed;
            r.note = "the two replays diverge at emission " +
                     std::to_string(first_difference(r.rows, again)) + " (" +
                     std::to_string(r.rows.size()) + " and " + std::to_string(again.size()) +
                     " emissions)";
            return r;
        }
        r.verdict = Verdict::Passed;
        r.note = std::to_string(r.rows.size()) + " emissions from " +
                 (state.checkpoint == 0 ? std::string{"fresh state"}
                                        : "checkpoint " + std::to_string(state.checkpoint)) +
                 ", identical twice";
        if (info.truncated) {
            r.note += "; the capture was truncated, so this covers its stored prefix only";
        }
    } catch (const std::exception& ex) {
        r.verdict = classify_replay_error(ex.what());
        r.note = ex.what();
    }
    return r;
}

std::optional<cluster::JobGraphSpec> bundle_graph(const Bundle& b) {
    if (b.find("job/manifest.json") == nullptr) {
        return std::nullopt;
    }
    try {
        const auto m = config::parse(read_text(b.root / "job/manifest.json"));
        const auto text = m.string_or("graph_json", "");
        if (text.empty()) {
            return std::nullopt;
        }
        return cluster::JobGraphSpec::from_json(text);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// The graph operator a capture is of. The worker copies each operator's type,
// parameters and uid from the graph into op.json, so a uid names it and,
// without one, its type and parameters do, less the Row layout the coordinator
// stamps on each deployment and never into the graph. Null when no operator,
// or more than one, matches.
const cluster::OperatorSpec* graph_op_of(const cluster::JobGraphSpec& graph,
                                         const CapturedSpec& spec) {
    auto params = spec.params;
    params.erase(std::string{cluster::kRowLayoutParam});
    const cluster::OperatorSpec* found = nullptr;
    for (const auto& op : graph.ops) {
        const bool match = spec.uid.empty()
                               ? op.uid.empty() && op.type == spec.op_type && op.params == params
                               : op.uid == spec.uid;
        if (match) {
            if (found != nullptr) {
                return nullptr;
            }
            found = &op;
        }
    }
    return found;
}

// The one operator that consumes `self`, when the topology makes its input
// exactly the operator's output: one consumer, with one input, and one subtask
// on both sides.
const cluster::OperatorSpec* sole_consumer(const cluster::JobGraphSpec& graph,
                                           const cluster::OperatorSpec& self,
                                           std::string& why) {
    std::vector<const cluster::OperatorSpec*> consumers;
    for (const auto& op : graph.ops) {
        if (std::find(op.inputs.begin(), op.inputs.end(), self.id) != op.inputs.end()) {
            consumers.push_back(&op);
        }
    }
    if (consumers.size() != 1 || consumers.front()->inputs.size() != 1) {
        why = "the operator does not feed exactly one single-input operator";
        return nullptr;
    }
    if (self.parallelism != 1 || consumers.front()->parallelism != 1) {
        why =
            "the operator or its consumer runs on more than one subtask, so one subtask's "
            "output is not one subtask's input";
        return nullptr;
    }
    return consumers.front();
}

// What production handed the operator's sole consumer in the same epoch, read
// from that consumer's own capture and rendered as the replay renders its
// emissions. Without rows, `verdict` says why: unsupported where the topology
// does not make the two one stream, unavailable where evidence is missing.
struct Recorded {
    std::optional<std::vector<std::string>> rows;
    Verdict verdict{Verdict::Unavailable};
    std::string why;
};

Recorded recorded_downstream(const Bundle& b,
                             const Entry& e,
                             const std::vector<Entry>& all,
                             const std::optional<cluster::JobGraphSpec>& graph) {
    Recorded r;
    if (e.truncated) {
        r.why = "the operator's capture was truncated, so it cannot stand for the whole epoch";
        return r;
    }
    if (!graph.has_value()) {
        r.why = "the bundle holds no job graph to find the operator's downstream operator";
        return r;
    }
    const auto* self = graph_op_of(*graph, captured_spec(b, e));
    if (self == nullptr) {
        r.why = "the capture's op.json does not name exactly one operator of the job graph";
        return r;
    }
    const auto* down = sole_consumer(*graph, *self, r.why);
    if (down == nullptr) {
        r.verdict = Verdict::Unsupported;
        return r;
    }
    std::vector<const Entry*> found;
    for (const auto& c : all) {
        if (c.epoch == e.epoch && graph_op_of(*graph, captured_spec(b, c)) == down) {
            found.push_back(&c);
        }
    }
    const bool terminal = std::none_of(graph->ops.begin(), graph->ops.end(), [&](const auto& op) {
        return std::find(op.inputs.begin(), op.inputs.end(), down->id) != op.inputs.end();
    });
    if (found.empty() && terminal) {
        r.verdict = Verdict::Unsupported;
        r.why = "its consumer ('" + down->id +
                "') is a sink, whose input the flight recorder does not capture";
        return r;
    }
    if (found.size() != 1) {
        r.why = "the downstream operator ('" + down->id + "') has " +
                (found.empty() ? std::string{"no capture"} : std::string{"several captures"}) +
                " of this epoch in the bundle";
        return r;
    }
    if (found.front()->truncated) {
        r.why = "the downstream operator's capture was truncated";
        return r;
    }
    const auto bytes = read_text(b.root / found.front()->path);
    const auto parsed =
        capture::read_capture_events(std::as_bytes(std::span{bytes}), row_json_codec());
    if (!parsed.has_value()) {
        r.why = "the downstream operator's capture does not read";
        return r;
    }
    std::vector<std::string> rows;
    for (const auto& ev : parsed->second) {
        if (const auto* rec = std::get_if<Record<Row>>(&ev)) {
            rows.push_back(format_replay_row(rec->value()));
        }
    }
    if (parsed->first.version >= 2 && rows.size() != parsed->first.records_seen) {
        r.why = "the downstream operator's capture holds " + std::to_string(rows.size()) +
                " of the " + std::to_string(parsed->first.records_seen) +
                " records its header counts: it was cut short";
        return r;
    }
    r.rows = std::move(rows);
    return r;
}

// A row as format_replay_row renders it, or an oracle's JSON object, in one
// form: the changelog prefix ("+I " and none are the same), then the object
// with its keys sorted.
std::string canonical_row(std::string_view line) {
    std::string prefix;
    for (const std::string_view p : {"-D ", "-U ", "+U ", "+I "}) {
        if (line.starts_with(p)) {
            prefix = p == "+I " ? "" : std::string{p};
            line.remove_prefix(p.size());
            break;
        }
    }
    const auto v = config::parse(line);
    if (!v.is_object()) {
        throw std::runtime_error("not a JSON object: " + std::string{line.substr(0, 80)});
    }
    std::map<std::string, std::string> sorted;
    for (const auto& [k, val] : v.as_object()) {
        sorted[std::string{k}] = val.serialize(0);
    }
    std::string out = prefix + "{";
    for (const auto& [k, val] : sorted) {
        out += (out.size() > prefix.size() + 1 ? "," : "") + config::JsonValue{k}.serialize(0) +
               ":" + val;
    }
    return out + "}";
}

using incident::SelectionTally;

// What a bound left unchecked, said the same way by every replay check.
std::string omission_note(const SelectionTally& t, std::size_t max_replays) {
    return std::to_string(t.omitted()) + " of the " + std::to_string(t.selected) +
           " selected epochs were not checked: --max-replays=" + std::to_string(max_replays) +
           " stopped the replay";
}

std::string raise_or_narrow(const SelectionTally& t) {
    return "raise --max-replays to at least " + std::to_string(t.selected) +
           ", or narrow the selection with --op, --subtask and --epoch";
}

void conclude_determinism(Check& c, const SelectionTally& t, std::size_t max_replays) {
    c.verdict = t.verdict();
    c.counts = t.counts();
    const auto of = " of the " + std::to_string(t.selected) + " selected epoch(s)";
    switch (c.verdict) {
        case Verdict::Failed:
            c.reason = std::to_string(t.failed) + of +
                       " did not replay identically twice, whatever the rest show (listed)";
            c.next_action =
                "replay the epoch with clink replay --verify; an operator that reads the "
                "wall clock or iterates unordered state replays differently";
            return;
        case Verdict::Passed:
            c.reason = "all " + std::to_string(t.selected) +
                       " selected epoch(s) replayed twice with identical emissions. This is "
                       "determinism, not agreement with production.";
            return;
        case Verdict::Unavailable: {
            std::vector<std::string> parts;
            if (t.omitted() > 0) {
                parts.push_back(omission_note(t, max_replays));
            }
            if (t.unavailable > 0) {
                parts.push_back(std::to_string(t.unavailable) +
                                " could not be replayed whole for want of evidence (listed)");
            }
            if (t.unsupported > 0) {
                parts.push_back(std::to_string(t.unsupported) +
                                " are of operators this build does not replay");
            }
            parts.push_back(std::to_string(t.passed) + " replayed identically");
            c.reason = "the selection is not fully checked: ";
            for (std::size_t i = 0; i < parts.size(); ++i) {
                c.reason += (i == 0 ? "" : "; ") + parts[i];
            }
            c.next_action = t.omitted() > 0
                                ? raise_or_narrow(t)
                                : "collect with --include-state, which carries each epoch's "
                                  "starting state, from captures taken without a record cap";
            return;
        }
        case Verdict::Unsupported:
            c.reason = std::to_string(t.unsupported) + of +
                       " are of operators this build does not replay (listed); " +
                       std::to_string(t.passed) +
                       " replayed identically, which does not establish the selection";
            c.next_action =
                "narrow the selection with --op to operators this build replays, or replay "
                "a plugin operator with clink replay --plugin=<so>";
            return;
    }
}

void conclude_recorded(Check& c,
                       const SelectionTally& t,
                       std::size_t reordered,
                       std::size_t max_replays) {
    c.verdict = t.verdict();
    c.counts = t.counts();
    const auto of = " of the " + std::to_string(t.selected) + " selected epoch(s)";
    switch (c.verdict) {
        case Verdict::Failed:
            c.reason = std::to_string(t.failed) + of +
                       " disagree with what production handed the downstream operator, "
                       "whatever the rest show (listed)";
            c.next_action =
                "the replay does not reproduce production for these epochs: compare with "
                "clink replay --out and clink replay-diff, and check the build is the "
                "job's";
            return;
        case Verdict::Passed:
            c.reason = "all " + std::to_string(t.selected) +
                       " selected epoch(s) equal, row for row, what production handed the "
                       "downstream operator";
            if (reordered > 0) {
                c.reason += "; in " + std::to_string(reordered) +
                            " of them rows of different keys came in another order, which the "
                            "engine does not fix, and each key's own rows in the same order";
            }
            return;
        case Verdict::Unavailable: {
            std::vector<std::string> parts;
            if (t.omitted() > 0) {
                parts.push_back(omission_note(t, max_replays));
            }
            if (t.unavailable > 0) {
                parts.push_back(std::to_string(t.unavailable) +
                                " could not be compared for want of evidence");
            }
            if (t.unsupported > 0) {
                parts.push_back(std::to_string(t.unsupported) +
                                " cannot be compared, by their topology or because this build "
                                "does not replay their operator");
            }
            parts.push_back(std::to_string(t.passed) + " equal what production did");
            c.reason = "the selection is not fully checked: ";
            for (std::size_t i = 0; i < parts.size(); ++i) {
                c.reason += (i == 0 ? "" : "; ") + parts[i];
            }
            c.next_action =
                t.omitted() > 0
                    ? raise_or_narrow(t)
                    : "collect the downstream operators' captures too (no --ops filter), "
                      "untruncated, with the job manifest (--ha-dir) and each epoch's starting "
                      "state";
            return;
        }
        case Verdict::Unsupported:
            c.reason = std::to_string(t.unsupported) + of +
                       " cannot be compared (listed): the operator feeds a sink, is not one "
                       "single-input operator at parallelism 1, or is not replayed by this "
                       "build; " +
                       std::to_string(t.passed) +
                       " equal what production did, which does not establish the selection";
            c.next_action =
                "narrow the selection with --op to operators this build replays whose sole "
                "consumer is captured";
            return;
    }
}

// The columns an operator declares it groups or partitions its rows by, as its
// SQL parameters name them in its output: the engine orders a key's own rows,
// and nothing across keys. Empty when the operator declares none.
std::vector<std::string> grouping_columns(const CapturedSpec& spec) {
    for (const char* name :
         {"group_key_outputs", "partition_columns", "partition_keys", "partition_by"}) {
        const auto it = spec.params.find(name);
        if (it == spec.params.end() || it->second.empty()) {
            continue;
        }
        std::vector<std::string> out;
        std::stringstream in(it->second);
        std::string col;
        while (std::getline(in, col, ',')) {
            if (!col.empty()) {
                out.push_back(col);
            }
        }
        return out;
    }
    return {};
}

// Each key's rows, in the order they came, keyed by the grouping columns'
// values. Nullopt when a row is not a JSON object.
std::optional<std::map<std::string, std::vector<std::string>>> rows_by_key(
    const std::vector<std::string>& rows, const std::vector<std::string>& columns) {
    std::map<std::string, std::vector<std::string>> out;
    for (const auto& row : rows) {
        std::string_view text = row;
        for (const std::string_view p : {"-D ", "-U ", "+U ", "+I "}) {
            if (text.starts_with(p)) {
                text.remove_prefix(p.size());
                break;
            }
        }
        try {
            const auto v = config::parse(text);
            if (!v.is_object()) {
                return std::nullopt;
            }
            std::string key;
            for (const auto& col : columns) {
                key += (v.contains(col) ? v.at(col).serialize(0) : std::string{"null"}) + '\x1f';
            }
            out[key].push_back(row);
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }
    return out;
}

// Compare one replayed epoch with what production handed the operator's
// consumer. The same rows in the same order pass. The same rows with only rows
// of different keys reordered pass too, and the epoch is counted in
// `reordered`: production's state was built by its own history, the replay's
// restored from a snapshot, and an operator that fires many keys at once may
// visit them in another order. Anything else fails.
void compare_with_recorded(Check& c,
                           SelectionTally& t,
                           std::size_t& reordered,
                           const Entry& e,
                           const CapturedSpec& spec,
                           const std::vector<std::string>& replayed,
                           const Recorded& found) {
    if (!found.rows.has_value()) {
        t.add(found.verdict);
        c.details.push_back(entry_name(e) + ": not compared - " + found.why);
        return;
    }
    const auto& recorded = found.rows;
    c.evidence.push_back(e.path);
    if (*recorded == replayed) {
        t.add(Verdict::Passed);
        c.details.push_back(entry_name(e) + ": the replay's " + std::to_string(replayed.size()) +
                            " emissions equal the downstream operator's captured input");
        return;
    }
    const auto at = first_difference(replayed, *recorded);
    auto line = entry_name(e) + ": differs at emission " + std::to_string(at) + " (replay " +
                std::to_string(replayed.size()) + ", production " +
                std::to_string(recorded->size()) + ")";
    if (at < replayed.size()) {
        line += "; replay: " + replayed[at];
    }
    if (at < recorded->size()) {
        line += "; production: " + (*recorded)[at];
    }
    auto a = replayed;
    auto b = *recorded;
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    if (a != b) {
        t.add(Verdict::Failed);
        c.details.push_back(line);
        return;
    }
    const auto columns = grouping_columns(spec);
    if (!columns.empty()) {
        const auto mine = rows_by_key(replayed, columns);
        const auto theirs = rows_by_key(*recorded, columns);
        if (mine.has_value() && theirs.has_value() && *mine == *theirs) {
            t.add(Verdict::Passed);
            ++reordered;
            std::string by;
            for (const auto& col : columns) {
                by += (by.empty() ? "" : ", ") + col;
            }
            c.details.push_back(
                entry_name(e) + ": the replay's " + std::to_string(replayed.size()) +
                " emissions equal the downstream operator's captured input; "
                "rows of different keys (by " +
                by + ") came in another order, first at emission " + std::to_string(at));
            return;
        }
        line += "; the same rows, but a key's own rows in a different order";
    } else {
        line +=
            "; the same rows in a different order, and the operator declares no grouping "
            "that would allow it";
    }
    t.add(Verdict::Failed);
    c.details.push_back(line);
}

// The selected epochs of one operator's subtask, replayed each from its own
// starting state, against an independent expectation of their emissions. An
// expectation need not know which epoch a row fell in, nor the order within
// one, so the comparison is of multisets. A row the replay emitted that the
// expectation does not hold fails it, however much of the selection was
// replayed. Otherwise it passes only when every epoch the expectation covers
// was replayed whole and the rows are the expected ones; a selection a bound
// or missing evidence left incomplete is a partial result.
Check expected_check(const Restores& restores,
                     const std::string& path,
                     const std::vector<Entry>& selection,
                     const std::vector<std::pair<Entry, Replayed>>& runs,
                     std::size_t max_replays) {
    Check c{.id = "replay.expected_output",
            .verdict = Verdict::Unavailable,
            .evidence = {path},
            .reason = {},
            .next_action = {},
            .details = {},
            .counts = {}};
    if (selection.empty()) {
        c.reason = "no captured epoch is selected to compare with it";
        c.next_action = "select an operator with --op, and its epochs with --epoch or none";
        return c;
    }
    // An epoch a recorded restore passes over belongs to a run whose work the
    // restore discarded: its events were processed again in the restored run's
    // first epoch, so counting both would count them twice.
    if (!restores.error.empty()) {
        c.reason = restores.error;
        return c;
    }
    for (const auto& e : selection) {
        if (e.op != selection.front().op || e.subtask != selection.front().subtask) {
            c.reason =
                "an expected output describes one operator's subtask, and the selection "
                "spans several";
            c.next_action = "select one with --op and --subtask";
            return c;
        }
    }
    const auto discarded = [&](const Entry& e) {
        return incident::epoch_discarded(restores.known, e.epoch);
    };
    SelectionTally t;
    for (const auto& e : selection) {
        if (!discarded(e)) {
            ++t.selected;
        }
    }
    std::vector<std::string> got;
    std::vector<std::string> want;
    try {
        for (const auto& [entry, run] : runs) {
            if (discarded(entry)) {
                c.details.push_back(entry_name(entry) +
                                    ": left out, as the run that processed it was discarded by "
                                    "a recorded restore");
                continue;
            }
            c.evidence.push_back(entry.path);
            if (run.verdict == Verdict::Passed && !run.partial) {
                t.add(Verdict::Passed);
                for (const auto& row : run.rows) {
                    got.push_back(canonical_row(row));
                }
                continue;
            }
            // Part of an epoch, or none of it, is not what an expectation of
            // the whole describes.
            t.add(run.verdict == Verdict::Unsupported ? Verdict::Unsupported
                                                      : Verdict::Unavailable);
            c.details.push_back(entry_name(entry) + ": not compared - " +
                                (run.verdict == Verdict::Passed
                                     ? std::string{"the capture was truncated, so it holds only "
                                                   "part of the epoch"}
                                     : "it did not replay: " + run.note));
        }
        std::ifstream in(path);
        if (!in) {
            throw std::runtime_error("cannot read " + path);
        }
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                want.push_back(canonical_row(line));
            }
        }
    } catch (const std::exception& ex) {
        c.verdict = Verdict::Failed;
        c.reason = std::string{"the expected output does not read: "} + ex.what();
        c.next_action = "give one JSON object per line, one per expected row";
        c.counts = t.counts();
        return c;
    }
    c.counts = t.counts();
    std::sort(want.begin(), want.end());
    std::sort(got.begin(), got.end());
    std::vector<std::string> only_got;
    std::vector<std::string> only_want;
    std::set_difference(
        got.begin(), got.end(), want.begin(), want.end(), std::back_inserter(only_got));
    std::set_difference(
        want.begin(), want.end(), got.begin(), got.end(), std::back_inserter(only_want));
    const auto list = [&](const std::vector<std::string>& rows, const char* label) {
        for (std::size_t i = 0; i < rows.size() && i < 10; ++i) {
            c.details.push_back(std::string{label} + rows[i]);
        }
    };
    const bool complete = t.selected > 0 && t.passed == t.selected;
    if (!only_got.empty()) {
        c.verdict = Verdict::Failed;
        c.reason = "the replay emitted " + std::to_string(only_got.size()) +
                   " row(s) the expectation does not hold, across the " + std::to_string(t.passed) +
                   " epoch(s) compared" +
                   (complete ? std::string{}
                             : "; that stands whatever the epochs not compared would show");
        c.next_action = "decide which is wrong: the job's result or the expectation";
        list(only_got, "only in the replay: ");
        if (complete) {
            list(only_want, "only expected: ");
        }
        return c;
    }
    if (complete) {
        if (only_want.empty()) {
            c.verdict = Verdict::Passed;
            c.reason = "the replay's " + std::to_string(got.size()) + " emissions across all " +
                       std::to_string(t.selected) +
                       " selected epoch(s) equal the expected rows, as a multiset; how "
                       "independent the expectation is rests with whoever produced it";
            return c;
        }
        c.verdict = Verdict::Failed;
        c.reason = "the replay of every selected epoch emitted " + std::to_string(got.size()) +
                   " row(s), and the expectation holds " + std::to_string(only_want.size()) +
                   " more it did not emit";
        c.next_action = "decide which is wrong: the job's result or the expectation";
        list(only_want, "only expected: ");
        return c;
    }
    // Every row replayed is an expected one, and the selection is incomplete:
    // the expected rows not emitted may belong to the epochs not compared.
    c.verdict = t.verdict();
    std::vector<std::string> parts;
    if (t.omitted() > 0) {
        parts.push_back(omission_note(t, max_replays));
    }
    if (t.unavailable + t.unsupported > 0) {
        parts.push_back(std::to_string(t.unavailable + t.unsupported) +
                        " were not compared (listed)");
    }
    c.reason = "the selection is not fully checked: ";
    for (std::size_t i = 0; i < parts.size(); ++i) {
        c.reason += (i == 0 ? "" : "; ") + parts[i];
    }
    c.reason += "; the " + std::to_string(got.size()) + " row(s) of the " +
                std::to_string(t.passed) + " epoch(s) compared are all expected, and " +
                std::to_string(only_want.size()) + " expected row(s) cannot be judged";
    c.next_action = t.omitted() > 0 ? raise_or_narrow(t)
                                    : "see replay.determinism for why the rest did not replay";
    return c;
}

Check replay_check(const char* id) {
    return Check{.id = id,
                 .verdict = Verdict::Unavailable,
                 .evidence = {},
                 .reason = {},
                 .next_action = {},
                 .details = {},
                 .counts = {}};
}

}  // namespace

std::vector<Check> incident_replay_checks(const Bundle& b, const IncidentReplayOptions& o) {
    auto det = replay_check("replay.determinism");
    auto rec = replay_check("replay.recorded_output");
    const auto& all = b.captures;
    const auto restores = read_restores(b);
    // The selection --op, --subtask and --epoch name is the scope every check
    // answers for. --max-replays bounds how much of it one run replays; what it
    // leaves out is counted as omitted, never dropped from the scope.
    std::vector<Entry> selection;
    std::copy_if(all.begin(), all.end(), std::back_inserter(selection), [&](const Entry& e) {
        return selected(e, o);
    });
    SelectionTally det_t;
    det_t.selected = selection.size();
    SelectionTally rec_t = det_t;
    if (selection.empty()) {
        det.reason = b.contains("captures")
                         ? "no captured epoch in the bundle matches the selection"
                         : "the bundle holds no captured epochs";
        det.next_action = b.contains("captures")
                              ? "check --op, --subtask and --epoch against the manifest's captures"
                              : "run the job with --capture-dir and collect with "
                                "--include-captures";
        det.counts = det_t.counts();
        rec.reason = det.reason;
        rec.next_action = det.next_action;
        rec.counts = rec_t.counts();
        std::vector<Check> out{det, rec};
        if (!o.expected_path.empty()) {
            out.push_back(expected_check(restores, o.expected_path, selection, {}, o.max_replays));
        }
        return out;
    }
    const auto replayed = std::min<std::size_t>(selection.size(), o.max_replays);
    if (replayed < selection.size()) {
        std::string names;
        for (std::size_t i = replayed; i < selection.size() && i < replayed + 5; ++i) {
            names += (names.empty() ? "" : ", ") + entry_name(selection[i]);
        }
        if (selection.size() > replayed + 5) {
            names += ", and " + std::to_string(selection.size() - replayed - 5) + " more";
        }
        const auto line =
            "not checked, beyond --max-replays=" + std::to_string(o.max_replays) + ": " + names;
        det.details.push_back(line);
        rec.details.push_back(line);
    }

    const auto graph = bundle_graph(b);
    std::size_t reordered = 0;
    std::vector<std::pair<Entry, Replayed>> runs;
    for (std::size_t i = 0; i < replayed; ++i) {
        const auto& e = selection[i];
        det.evidence.push_back(e.path);
        auto r = replay_twice(b, restores, e);
        // A truncated capture's stored prefix replaying identically does not
        // establish the epoch: the records past the cap were never replayed.
        const auto verdict =
            r.verdict == Verdict::Passed && r.partial ? Verdict::Unavailable : r.verdict;
        det_t.add(verdict);
        det.details.push_back(entry_name(e) + ": " + incident::to_string(verdict) + " - " + r.note);
        if (r.verdict == Verdict::Passed && !r.partial) {
            compare_with_recorded(rec,
                                  rec_t,
                                  reordered,
                                  e,
                                  captured_spec(b, e),
                                  r.rows,
                                  recorded_downstream(b, e, all, graph));
        } else {
            // What kept the replay from passing keeps the comparison from
            // being made: an operator this build cannot replay cannot be
            // compared either, and a replay without its evidence, or with part
            // of it, has nothing to compare.
            rec_t.add(r.verdict == Verdict::Unsupported ? Verdict::Unsupported
                                                        : Verdict::Unavailable);
            rec.details.push_back(entry_name(e) + ": not compared - " +
                                  (r.verdict == Verdict::Passed
                                       ? std::string{"the capture was truncated, so it holds "
                                                     "only part of the epoch"}
                                       : "the replay did not pass"));
        }
        runs.emplace_back(e, std::move(r));
    }
    conclude_determinism(det, det_t, o.max_replays);
    conclude_recorded(rec, rec_t, reordered, o.max_replays);
    std::vector<Check> out{det, rec};
    if (!o.expected_path.empty()) {
        out.push_back(expected_check(restores, o.expected_path, selection, runs, o.max_replays));
    }
    return out;
}

std::size_t incident_regress(const Bundle& b,
                             std::uint64_t op,
                             std::uint64_t epoch,
                             std::optional<std::uint64_t> subtask,
                             const std::string& out_dir) {
    // The subtask, from the bundle's captures when it is not named: its
    // starting snapshot is that subtask's.
    if (!subtask.has_value()) {
        std::set<std::uint64_t> subtasks;
        for (const auto& e : b.captures) {
            if (e.op == op && e.epoch == epoch) {
                subtasks.insert(e.subtask);
            }
        }
        if (subtasks.size() != 1) {
            throw std::runtime_error(
                subtasks.empty()
                    ? "the bundle holds no capture of op " + std::to_string(op) + " epoch " +
                          std::to_string(epoch)
                    : "op " + std::to_string(op) + " epoch " + std::to_string(epoch) +
                          " was captured on several subtasks; name one with --subtask");
        }
        subtask = *subtasks.begin();
    }
    const auto restores = read_restores(b);
    const auto state = starting_state(b, restores, epoch, *subtask);
    if (state.verdict != Verdict::Passed) {
        throw std::runtime_error(state.why);
    }
    const auto req = replay_request(b, op, epoch, subtask, state);
    {
        // The operator the engine would build, before anything is built.
        const auto replay = EpochReplay::load(req);
        if (calls_external_systems(replay.info().spec.op_type)) {
            throw std::runtime_error(external_note(replay.info().spec.op_type) +
                                     ", so its replay is not a self-contained regression test");
        }
        const auto& info = replay.info();
        if (info.truncated || (info.format_version >= 2 && info.data_count != info.records_seen)) {
            throw std::runtime_error(
                "the epoch's capture holds only part of the epoch, so a "
                "regression test of it would freeze a part");
        }
    }
    std::error_code ec;
    const bool existed = std::filesystem::exists(out_dir, ec);
    try {
        const auto n = emit_replay_regression(req, out_dir);
        if (const auto failure = run_replay_regression(out_dir); !failure.empty()) {
            throw std::runtime_error("the emitted regression bundle does not pass on this build: " +
                                     failure);
        }
        return n;
    } catch (...) {
        // Nothing that would not pass is left behind.
        if (!existed) {
            std::filesystem::remove_all(out_dir, ec);
        }
        throw;
    }
}

}  // namespace clink::sql
