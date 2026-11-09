#include "clink/sql/incident_replay.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
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

// One captured epoch the manifest lists.
struct Entry {
    std::uint64_t op{0};
    std::uint64_t subtask{0};
    std::uint64_t epoch{0};
    std::string path;  // bundle-relative epoch file
    bool truncated{false};
};

std::string entry_name(const Entry& e) {
    return "op " + std::to_string(e.op) + " subtask " + std::to_string(e.subtask) + " epoch " +
           std::to_string(e.epoch);
}

bool selected(const Entry& e, const IncidentReplayOptions& o) {
    return (!o.op || *o.op == e.op) && (!o.subtask || *o.subtask == e.subtask) &&
           (!o.epoch || *o.epoch == e.epoch);
}

// Every captured epoch the manifest lists whose file the bundle holds.
std::vector<Entry> captured_entries(const Bundle& b) {
    std::vector<Entry> out;
    if (!b.manifest.contains("captures") || !b.manifest.at("captures").is_array()) {
        return out;
    }
    for (const auto& c : b.manifest.at("captures").as_array()) {
        if (!c.is_object()) {
            continue;
        }
        Entry e;
        e.op = static_cast<std::uint64_t>(c.int_or("op", 0));
        e.subtask = static_cast<std::uint64_t>(c.int_or("subtask", 0));
        e.epoch = static_cast<std::uint64_t>(c.int_or("epoch", 0));
        e.path = c.string_or("path", "");
        e.truncated = c.bool_or("truncated", false);
        if (e.epoch > 0 && !e.path.empty() && b.find(e.path) != nullptr) {
            out.push_back(std::move(e));
        }
    }
    std::sort(out.begin(), out.end(), [](const Entry& x, const Entry& y) {
        return std::tie(x.op, x.subtask, x.epoch) < std::tie(y.op, y.subtask, y.epoch);
    });
    return out;
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

CapturedSpec captured_spec(const Bundle& b, const Entry& e) {
    const auto rel = (fs::path{e.path}.parent_path() / "op.json").generic_string();
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

// Where a replay of `epoch` starts: the checkpoint the epoch's events were
// processed from (epoch_starting_checkpoint, over the bundle's RESTORED
// record), and that checkpoint's generation directory, resolved over the
// bundle's own markers as a restore would. Checkpoint 0 is fresh state, with
// no directory.
struct StartingState {
    std::uint64_t checkpoint{0};
    std::string dir;
};

std::optional<StartingState> starting_state(const Bundle& b,
                                            std::uint64_t epoch,
                                            std::uint64_t subtask,
                                            std::string& why) {
    std::optional<cluster::RestoredRecord> restored;
    try {
        restored = cluster::read_restore_point(b.checkpoint_dir().string(), b.job());
    } catch (const std::exception& e) {
        why = std::string{"the bundle's RESTORED record does not read: "} + e.what();
        return std::nullopt;
    }
    StartingState s;
    s.checkpoint = incident::epoch_starting_checkpoint(
        epoch,
        restored.has_value() ? std::optional{restored->restore} : std::nullopt,
        restored.has_value() ? std::optional{restored->next} : std::nullopt);
    if (s.checkpoint == 0) {
        return s;
    }
    if (b.find("checkpoint/_jobs/" + std::to_string(b.job()) + "/COMPLETED-" +
               std::to_string(s.checkpoint)) == nullptr) {
        why = "the bundle holds no COMPLETED marker for checkpoint " +
              std::to_string(s.checkpoint) + ", the epoch's starting state";
        return std::nullopt;
    }
    try {
        const auto src = cluster::resolve_restore_source(
            cluster::completed_marker_dir_for(b.checkpoint_dir().string(), b.job()).string(),
            s.checkpoint);
        s.dir = fs::path{state_dir_for(
                             src.state_root, src.generation, static_cast<std::uint32_t>(subtask))}
                    .parent_path()
                    .string();
    } catch (const std::exception& e) {
        why = "checkpoint " + std::to_string(s.checkpoint) + " cannot be resolved: " + e.what();
        return std::nullopt;
    }
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
};

// EpochReplay names what it could not do; sort that into the verdicts. An
// operator this build cannot rebuild is unsupported, evidence the bundle lacks
// is unavailable, and anything else - an unreadable capture, an operator that
// throws on its captured events - is a failure.
Verdict classify_replay_error(const std::string& what) {
    for (const char* unsupported : {"no registered factory", "replay driver", "not registered"}) {
        if (what.find(unsupported) != std::string::npos) {
            return Verdict::Unsupported;
        }
    }
    for (const char* unavailable : {".snap under", "cannot open", "requires checkpoint_dir"}) {
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

Replayed replay_twice(const Bundle& b, const Entry& e) {
    Replayed r;
    if (const auto spec = captured_spec(b, e); calls_external_systems(spec.op_type)) {
        r.verdict = Verdict::Unsupported;
        r.note = "op type '" + spec.op_type +
                 "' calls an external system for each record (a lookup or model inference), "
                 "and verification contacts no service";
        return r;
    }
    const auto state = starting_state(b, e.epoch, e.subtask, r.note);
    if (!state.has_value()) {
        r.verdict = Verdict::Unavailable;
        return r;
    }
    try {
        const auto replay = EpochReplay::load(replay_request(b, e.op, e.epoch, e.subtask, *state));
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
                 (state->checkpoint == 0 ? std::string{"fresh state"}
                                         : "checkpoint " + std::to_string(state->checkpoint)) +
                 ", identical twice";
        if (replay.info().truncated) {
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

// The rows production handed the operator's sole consumer in the same epoch,
// read from that consumer's own capture and rendered as the replay renders
// its emissions. Sets `why` and returns nullopt when the bundle and the
// topology do not make that the same stream.
std::optional<std::vector<std::string>> recorded_downstream(
    const Bundle& b,
    const Entry& e,
    const std::vector<Entry>& all,
    const std::optional<cluster::JobGraphSpec>& graph,
    std::string& why) {
    if (e.truncated) {
        why = "the operator's capture was truncated, so it cannot stand for the whole epoch";
        return std::nullopt;
    }
    if (!graph.has_value()) {
        why = "the bundle holds no job graph to find the operator's downstream operator";
        return std::nullopt;
    }
    const auto* self = graph_op_of(*graph, captured_spec(b, e));
    if (self == nullptr) {
        why = "the capture's op.json does not name exactly one operator of the job graph";
        return std::nullopt;
    }
    const auto* down = sole_consumer(*graph, *self, why);
    if (down == nullptr) {
        return std::nullopt;
    }
    std::vector<const Entry*> found;
    for (const auto& c : all) {
        if (c.epoch == e.epoch && graph_op_of(*graph, captured_spec(b, c)) == down) {
            found.push_back(&c);
        }
    }
    if (found.size() != 1) {
        why = "the downstream operator ('" + down->id + "') has " +
              (found.empty() ? std::string{"no capture"} : std::string{"several captures"}) +
              " of this epoch in the bundle";
        return std::nullopt;
    }
    if (found.front()->truncated) {
        why = "the downstream operator's capture was truncated";
        return std::nullopt;
    }
    const auto bytes = read_text(b.root / found.front()->path);
    const auto parsed =
        capture::read_capture_events(std::as_bytes(std::span{bytes}), row_json_codec());
    if (!parsed.has_value()) {
        why = "the downstream operator's capture does not read";
        return std::nullopt;
    }
    std::vector<std::string> rows;
    for (const auto& ev : parsed->second) {
        if (const auto* rec = std::get_if<Record<Row>>(&ev)) {
            rows.push_back(format_replay_row(rec->value()));
        }
    }
    return rows;
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

struct Tally {
    std::size_t passed{0};
    std::size_t failed{0};
    std::size_t unavailable{0};
    std::size_t unsupported{0};

    void add(Verdict v) {
        switch (v) {
            case Verdict::Passed:
                ++passed;
                break;
            case Verdict::Failed:
                ++failed;
                break;
            case Verdict::Unavailable:
                ++unavailable;
                break;
            case Verdict::Unsupported:
                ++unsupported;
                break;
        }
    }
};

void conclude_determinism(Check& c, const Tally& t) {
    if (t.failed > 0) {
        c.verdict = Verdict::Failed;
        c.reason = std::to_string(t.failed) + " captured epoch(s) replay differently twice";
        c.next_action =
            "replay the epoch with clink replay --verify; an operator that reads the "
            "wall clock or iterates unordered state replays differently";
    } else if (t.unavailable > 0) {
        c.verdict = Verdict::Unavailable;
        c.reason = std::to_string(t.unavailable) +
                   " selected epoch(s) could not be replayed for want of evidence (" +
                   std::to_string(t.passed) + " replayed identically)";
        c.next_action = "collect with --include-state, which carries each epoch's starting state";
    } else if (t.passed == 0) {
        c.verdict = Verdict::Unsupported;
        c.reason = "none of the selected operators can be replayed by this build";
        c.next_action =
            "replay a plugin operator with clink replay --plugin=<so>; operators on "
            "other channels need their job plugin";
    } else {
        c.verdict = Verdict::Passed;
        c.reason =
            std::to_string(t.passed) + " captured epoch(s) replayed twice with identical emissions";
        if (t.unsupported > 0) {
            c.reason += "; " + std::to_string(t.unsupported) +
                        " are of operators this build does not replay (listed)";
        }
        c.reason += ". This is determinism, not agreement with production.";
    }
}

void conclude_recorded(Check& c, const Tally& t, std::size_t reordered) {
    if (t.failed > 0) {
        c.verdict = Verdict::Failed;
        c.reason = std::to_string(t.failed) +
                   " replayed epoch(s) disagree with what production handed the downstream "
                   "operator";
        c.next_action =
            "the replay does not reproduce production for these epochs: compare with "
            "clink replay --out and clink replay-diff, and check the build is the "
            "job's";
    } else if (t.passed == 0) {
        c.verdict = Verdict::Unavailable;
        c.reason =
            "no replayed epoch had its production output captured in a form that can be "
            "compared";
        c.next_action =
            "collect the downstream operator's capture too (no --ops filter), from a "
            "job whose compared operators run at parallelism 1, with untruncated "
            "epochs";
    } else {
        c.verdict = Verdict::Passed;
        c.reason = std::to_string(t.passed) +
                   " replayed epoch(s) equal, row for row, what production handed the "
                   "downstream operator";
        if (reordered > 0) {
            c.reason += "; in " + std::to_string(reordered) +
                        " of them rows of different keys came in another order, which the "
                        "engine does not fix, and each key's own rows in the same order";
        }
        if (t.unavailable > 0) {
            c.reason += "; " + std::to_string(t.unavailable) + " were not compared (listed)";
        }
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
                           Tally& t,
                           std::size_t& reordered,
                           const Entry& e,
                           const CapturedSpec& spec,
                           const std::vector<std::string>& replayed,
                           const std::optional<std::vector<std::string>>& recorded,
                           const std::string& why) {
    if (!recorded.has_value()) {
        t.add(Verdict::Unavailable);
        c.details.push_back(entry_name(e) + ": not compared - " + why);
        return;
    }
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
// one, so the comparison is of multisets.
Check expected_check(const Bundle& b,
                     const std::string& path,
                     const std::vector<std::pair<Entry, Replayed>>& runs) {
    Check c{.id = "replay.expected_output",
            .verdict = Verdict::Unavailable,
            .evidence = {path},
            .reason = {},
            .next_action = {},
            .details = {}};
    if (runs.empty()) {
        c.reason = "no captured epoch is selected to compare with it";
        c.next_action = "select an operator with --op, and its epochs with --epoch or none";
        return c;
    }
    // An epoch the RESTORED record passes over belongs to a run whose work the
    // restore discarded: its events were processed again in the restored run's
    // first epoch, so counting both would count them twice.
    std::optional<cluster::RestoredRecord> restored;
    try {
        restored = cluster::read_restore_point(b.checkpoint_dir().string(), b.job());
    } catch (const std::exception& e) {
        c.reason = std::string{"the bundle's RESTORED record does not read: "} + e.what();
        return c;
    }
    const auto discarded = [&](const Entry& e) {
        return restored.has_value() && restored->passes_over(e.epoch);
    };
    for (const auto& [entry, run] : runs) {
        if (entry.op != runs.front().first.op || entry.subtask != runs.front().first.subtask) {
            c.reason =
                "an expected output describes one operator's subtask, and the selection "
                "spans several";
            c.next_action = "select one with --op and --subtask";
            return c;
        }
        if (discarded(entry)) {
            c.details.push_back(entry_name(entry) +
                                ": left out, as the run that processed it was discarded by the "
                                "restore the RESTORED record describes");
            continue;
        }
        c.evidence.push_back(entry.path);
        if (run.verdict != Verdict::Passed) {
            c.reason = entry_name(entry) + " did not replay: " + run.note;
            c.next_action = "see replay.determinism";
            return c;
        }
    }
    std::vector<std::string> want;
    std::vector<std::string> got;
    try {
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
        for (const auto& [entry, run] : runs) {
            if (discarded(entry)) {
                continue;
            }
            for (const auto& row : run.rows) {
                got.push_back(canonical_row(row));
            }
        }
    } catch (const std::exception& ex) {
        c.verdict = Verdict::Failed;
        c.reason = std::string{"the expected output does not read: "} + ex.what();
        c.next_action = "give one JSON object per line, one per expected row";
        return c;
    }
    std::sort(want.begin(), want.end());
    std::sort(got.begin(), got.end());
    const auto span = std::to_string(runs.size()) + " epoch(s)";
    if (want == got) {
        c.verdict = Verdict::Passed;
        c.reason = "the replay's " + std::to_string(got.size()) + " emissions across " + span +
                   " equal the expected rows, as a multiset; how independent the expectation "
                   "is rests with whoever produced it";
        return c;
    }
    c.verdict = Verdict::Failed;
    c.reason = "the replay's emissions across " + span + " are not the expected rows (" +
               std::to_string(got.size()) + " against " + std::to_string(want.size()) + ")";
    c.next_action = "decide which is wrong: the job's result or the expectation";
    std::vector<std::string> only_got;
    std::vector<std::string> only_want;
    std::set_difference(
        got.begin(), got.end(), want.begin(), want.end(), std::back_inserter(only_got));
    std::set_difference(
        want.begin(), want.end(), got.begin(), got.end(), std::back_inserter(only_want));
    for (std::size_t i = 0; i < only_got.size() && i < 10; ++i) {
        c.details.push_back("only in the replay: " + only_got[i]);
    }
    for (std::size_t i = 0; i < only_want.size() && i < 10; ++i) {
        c.details.push_back("only expected: " + only_want[i]);
    }
    return c;
}

Check replay_check(const char* id) {
    return Check{.id = id,
                 .verdict = Verdict::Unavailable,
                 .evidence = {},
                 .reason = {},
                 .next_action = {},
                 .details = {}};
}

}  // namespace

std::vector<Check> incident_replay_checks(const Bundle& b, const IncidentReplayOptions& o) {
    auto det = replay_check("replay.determinism");
    auto rec = replay_check("replay.recorded_output");
    const auto all = captured_entries(b);
    std::vector<Entry> chosen;
    std::copy_if(all.begin(), all.end(), std::back_inserter(chosen), [&](const Entry& e) {
        return selected(e, o);
    });
    const auto selected_count = chosen.size();
    if (chosen.size() > o.max_replays) {
        det.details.push_back("only the first " + std::to_string(o.max_replays) + " of " +
                              std::to_string(chosen.size()) +
                              " selected epochs are replayed; narrow with --op and --epoch");
        chosen.resize(o.max_replays);
    }
    if (chosen.empty()) {
        det.reason = b.contains("captures")
                         ? "no captured epoch in the bundle matches the selection"
                         : "the bundle holds no captured epochs";
        det.next_action = b.contains("captures")
                              ? "check --op, --subtask and --epoch against the manifest's captures"
                              : "run the job with --capture-dir and collect with "
                                "--include-captures";
        rec.reason = det.reason;
        rec.next_action = det.next_action;
        std::vector<Check> out{det, rec};
        if (!o.expected_path.empty()) {
            out.push_back(expected_check(b, o.expected_path, {}));
        }
        return out;
    }

    const auto graph = bundle_graph(b);
    Tally det_tally;
    Tally rec_tally;
    std::size_t reordered = 0;
    std::vector<std::pair<Entry, Replayed>> runs;
    for (const auto& e : chosen) {
        det.evidence.push_back(e.path);
        auto r = replay_twice(b, e);
        det_tally.add(r.verdict);
        det.details.push_back(entry_name(e) + ": " + incident::to_string(r.verdict) + " - " +
                              r.note);
        if (r.verdict == Verdict::Passed) {
            std::string why;
            const auto recorded = recorded_downstream(b, e, all, graph, why);
            compare_with_recorded(
                rec, rec_tally, reordered, e, captured_spec(b, e), r.rows, recorded, why);
        } else {
            rec_tally.add(Verdict::Unavailable);
            rec.details.push_back(entry_name(e) + ": not compared - the replay did not pass");
        }
        runs.emplace_back(e, std::move(r));
    }
    conclude_determinism(det, det_tally);
    conclude_recorded(rec, rec_tally, reordered);
    std::vector<Check> out{det, rec};
    if (!o.expected_path.empty()) {
        if (selected_count > runs.size()) {
            // A union of some of the selected epochs is not what the
            // expectation describes.
            Check exp = replay_check("replay.expected_output");
            exp.evidence = {o.expected_path};
            exp.reason = "the selection holds " + std::to_string(selected_count) +
                         " epochs, more than the " + std::to_string(runs.size()) +
                         " one run replays, so their union cannot be compared";
            exp.next_action =
                "narrow the selection with --epoch, or compare part of it with an "
                "expectation of that part";
            out.push_back(exp);
        } else {
            out.push_back(expected_check(b, o.expected_path, runs));
        }
    }
    return out;
}

std::size_t incident_regress(const Bundle& b,
                             std::uint64_t op,
                             std::uint64_t epoch,
                             std::optional<std::uint64_t> subtask,
                             const std::string& out_dir) {
    for (const auto& e : captured_entries(b)) {
        if (e.op == op && e.epoch == epoch && (!subtask || *subtask == e.subtask)) {
            if (const auto spec = captured_spec(b, e); calls_external_systems(spec.op_type)) {
                throw std::runtime_error("op type '" + spec.op_type +
                                         "' calls an external system for each record, so its "
                                         "replay is not a self-contained regression test");
            }
        }
    }
    std::string why;
    const auto state = starting_state(b, epoch, subtask.value_or(0), why);
    if (!state.has_value()) {
        throw std::runtime_error(why);
    }
    const auto req = replay_request(b, op, epoch, subtask, *state);
    const auto n = emit_replay_regression(req, out_dir);
    if (const auto failure = run_replay_regression(out_dir); !failure.empty()) {
        throw std::runtime_error("the emitted regression bundle does not pass on this build: " +
                                 failure);
    }
    return n;
}

}  // namespace clink::sql
