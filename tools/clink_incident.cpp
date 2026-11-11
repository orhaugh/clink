// clink incident-collect, incident-verify, incident-regress and
// incident-reconcile: incident evidence bundles (design record 014,
// docs/internals/incident-verification.md).
//
//   clink incident-collect --checkpoint-dir=<dir> --job=<id> --out=<dir> [...]
//   clink incident-verify <bundle> [--profile=<p>] [--expected=<ndjson>] [--json]
//   clink incident-regress <bundle> --op=<id> --epoch=<n> --out=<dir>
//   clink incident-reconcile <bundle> --host=<h> --key=<col>[,<col>] [...]
//   clink incident-scope <bundle> --out=<file> [--checkpoint=<id>] [...]
//
// The library is clink::incident (include/clink/incident/bundle.hpp); the
// replay checks and incident-regress run on the SQL frontend's replay engine
// through tools/clink_incident_replay.cpp, or report themselves unsupported
// through its SQL-off stub.
//
// incident-verify exits 0 when every check the profile requires passed, 1 when
// a check failed, 3 when none failed but a required check is unavailable or
// unsupported (a partial result), and 2 on bad usage or a bundle that cannot be
// read. incident-collect and incident-regress exit 0 when done, 1 when they
// refuse, and 2 on bad usage. incident-reconcile exits as incident-verify
// does, and 2 when the request does not fit the bundle, the destination is
// not the one its evidence names, or a reference artifact is malformed or
// bound to another scope. It is the only one of them that opens a
// connection, to the destination the operator names, and only reads.
// incident-scope reads the bundle alone and writes the scope document a
// reference implementation computes from (clink/incident/reference.hpp).

#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/incident/reconcile.hpp"
#include "clink/incident/reference.hpp"

#include "clink_incident_reconcile.hpp"

// tools/clink_incident_replay.cpp, or clink_incident_replay_stub.cpp.
std::vector<clink::incident::Check> clink_incident_replay_checks(
    const clink::incident::Bundle& bundle,
    std::optional<std::uint64_t> op,
    std::optional<std::uint64_t> subtask,
    std::optional<std::uint64_t> epoch,
    const std::string& expected_path,
    std::size_t max_replays);
std::size_t clink_incident_regress(const clink::incident::Bundle& bundle,
                                   std::uint64_t op,
                                   std::uint64_t epoch,
                                   std::optional<std::uint64_t> subtask,
                                   const std::string& out_dir);

namespace {

namespace fs = std::filesystem;
using namespace clink::incident;

// --name=value flags, --name switches and positional arguments. Anything not
// in `valued` or `switches` is a usage error.
struct Args {
    std::map<std::string, std::vector<std::string>> values;
    std::set<std::string> switches;
    std::vector<std::string> positional;
    std::string error;

    [[nodiscard]] bool has(const std::string& k) const { return values.contains(k); }
    [[nodiscard]] std::string get(const std::string& k) const {
        const auto it = values.find(k);
        return it == values.end() ? std::string{} : it->second.back();
    }
};

Args parse_args(int argc,
                char** argv,
                const std::set<std::string>& valued,
                const std::set<std::string>& switches) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (!arg.starts_with("--")) {
            a.positional.push_back(arg);
            continue;
        }
        const auto eq = arg.find('=');
        const auto name = arg.substr(2, eq == std::string::npos ? std::string::npos : eq - 2);
        if (eq != std::string::npos && valued.contains(name)) {
            a.values[name].push_back(arg.substr(eq + 1));
        } else if (eq == std::string::npos && switches.contains(name)) {
            a.switches.insert(name);
        } else {
            a.error = "unknown or malformed argument '" + arg + "'";
        }
    }
    return a;
}

// A whole number up to 2^64 - 1: operator ids use all 64 bits.
std::optional<std::uint64_t> parse_u64(const std::string& s) {
    std::uint64_t out = 0;
    const auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    if (s.empty() || ec != std::errc{} || end != s.data() + s.size()) {
        return std::nullopt;
    }
    return out;
}

void collect_usage() {
    std::cerr << "Usage: clink incident-collect --checkpoint-dir=<dir> --job=<id> --out=<dir>\n"
              << "         [--ha-dir=<dir>] [--capture-dir=<dir>] [--trace-dir=<dir>]\n"
              << "         [--commit-confirmation=yes|no] [--checkpoint=<id>]\n"
              << "         [--include-state] [--include-captures] [--op=<id>[,<id>...]]\n"
              << "         [--epochs=<from>-<to>] [--max-bytes=N] [--max-files=N] [--timeout-s=N]\n"
              << "         [--json]\n"
              << "\n"
              << "Gathers one job's evidence into a new bundle directory and publishes it by\n"
              << "renaming it into place. The checkpoint is the one a coordinator takeover would\n"
              << "restore from, chosen by the engine's own rule over the markers on disk;\n"
              << "--checkpoint overrides it. The rule depends on whether the job's sinks need\n"
              << "commit confirmation, read from the job manifest under --ha-dir, or given with\n"
              << "--commit-confirmation.\n"
              << "\n"
              << "Snapshots and captures hold the job's records and keyed state: they are\n"
              << "collected only with --include-state and --include-captures. Graph parameters\n"
              << "whose names suggest a credential are redacted from the job manifest.\n"
              << "\n"
              << "Refuses, leaving nothing at --out, when the evidence changes while it is\n"
              << "copied, a required file is past a bound, or the time runs out. With --json a\n"
              << "refusal is also written to standard output, with its kind: changed (the job\n"
              << "moved on during the copy: collect again), missing (no completion marker for\n"
              << "the job or the checkpoint), bound, timeout or other.\n"
              << "\n"
              << "Exit status: 0 published, 1 refused, 2 bad usage.\n";
}

void verify_usage() {
    std::cerr
        << "Usage: clink incident-verify <bundle> [--profile=integrity|consistency|replay|full]\n"
        << "         [--expected=<ndjson>] [--op=<id>] [--subtask=<s>] [--epoch=<n>]\n"
        << "         [--max-replays=N] [--formal-dir=<dir>] [--formal-tools-dir=<dir>]\n"
        << "         [--json] [--report=<file>]\n"
        << "\n"
        << "Checks a bundle offline: no cluster, no service, no network. Each check reports\n"
        << "passed, failed, unavailable or unsupported, with the evidence it used, the\n"
        << "reason and the next action. A profile names the checks a run requires\n"
        << "(default consistency). --op, --subtask and --epoch select the captured\n"
        << "epochs the replay checks answer for (default all of them). --max-replays\n"
        << "(default 64) bounds how many of them one run replays; the rest count as not\n"
        << "checked, and a replay check passes only when every selected epoch was\n"
        << "checked and passed. --expected compares the selected epochs of one\n"
        << "operator subtask with an independent expectation, one JSON object per line.\n"
        << "\n"
        << "Exit status: 0 every required check passed, 1 a check failed, 3 a partial\n"
        << "result (nothing failed, but a required check is unavailable or unsupported),\n"
        << "2 bad usage or a bundle that cannot be read.\n";
}

void regress_usage() {
    std::cerr << "Usage: clink incident-regress <bundle> --op=<id> --epoch=<n> --out=<dir>\n"
              << "         [--subtask=<s>]\n"
              << "\n"
              << "Freezes one captured epoch of a verified bundle as a replay regression\n"
              << "bundle (the format clink replay --emit-test writes), runs it once on this\n"
              << "build, and refuses a bundle whose artifacts do not verify.\n"
              << "\n"
              << "Exit status: 0 written and passing, 1 refused, 2 bad usage.\n";
}

void reconcile_usage() {
    std::cerr << "Usage: clink incident-reconcile <bundle> --host=<host> [--key=<col>[,<col>...]]\n"
              << "         [--port=N] [--user=<user>] [--password-env=<VAR>] [--secure]\n"
              << "         [--tls-ca-file=<file>] [--tls-verify=true|false]\n"
              << "         [--checkpoint=<id>] [--sink-op=<id>] [--job=<id>]\n"
              << "         [--database=<db>] [--table=<table>]\n"
              << "         [--query-timeout-ms=N] [--max-rows-per-query=N] [--keys-per-query=N]\n"
              << "         [--settle-ms=N] [--list=N] [--expected-out=<ndjson>] [--json]\n"
              << "         [--report=<file>] [--from-run-start]\n"
              << "         [--reference=<artifact>] [--max-reference-bytes=N]\n"
              << "         [--coverage=selected-keys|complete-table] [--max-table-rows=N]\n"
              << "\n"
              << "Reads the ClickHouse destination a job's sink wrote, and says whether it holds,\n"
              << "for one completed checkpoint (--checkpoint, default the bundle's), exactly the\n"
              << "rows the sink received in that checkpoint's intervals: no row missing, changed,\n"
              << "unexpected or duplicated. The expected rows are the sink's own record of its\n"
              << "input, which the bundle carries when the job's Workers ran with --capture-dir\n"
              << "and it was collected with --include-captures; --key names the columns that\n"
              << "identify one row the job writes. A windowed job's key is its windows'\n"
              << "identity, the group key's columns and the window's start and end, which the\n"
              << "bundle's job graph names.\n"
              << "\n"
              << "The connection is the operator's: nothing it connects with or runs comes from\n"
              << "the bundle, and the password is read from the environment variable\n"
              << "--password-env names. Every statement is a bounded SELECT under readonly = 2.\n"
              << "The destination must be the one the evidence names, by server and table UUID,\n"
              << "or the run is refused. --expected-out writes the expected rows, as canonical\n"
              << "cells, for an independent check of the expectation.\n"
              << "\n"
              << "--reference audits against a reference artifact computed outside the job from\n"
              << "the scope `clink incident-scope` exports: the reference against the sink's\n"
              << "captured input (reference.capture: the transformation), the captured input\n"
              << "against the destination (reconcile.rows: the delivery), and the reference\n"
              << "against the destination over the keys of both (reference.destination). An\n"
              << "artifact that is malformed, unfinished or bound to another scope is refused;\n"
              << "one that says it is incomplete leaves the audit partial. Nothing the artifact\n"
              << "names is run, and its hashes show it is whole, not who wrote it.\n"
              << "\n"
              << "The destination is read for the selected keys, and rows of other keys are not\n"
              << "read. --coverage=complete-table also reads the table whole, at most\n"
              << "--max-table-rows rows, and requires it to hold exactly the expected rows: the\n"
              << "operator declares the table holds this one bounded run's writes and nothing\n"
              << "else, and the scope then spans the run from its start (as --from-run-start).\n"
              << "\n"
              << "Exit status: 0 the destination holds the scope exactly, 1 it does not (or the\n"
              << "bundle's integrity failed, or the job's output differs from the reference), 3\n"
              << "the comparison could not be made whole or stable (partial), 2 bad usage or a\n"
              << "refused request.\n";
}

void scope_usage() {
    std::cerr
        << "Usage: clink incident-scope <bundle> --out=<file> [--checkpoint=<id>]\n"
        << "         [--sink-op=<id>] [--job=<id>] [--from-run-start]\n"
        << "\n"
        << "Writes the scope document of one completed checkpoint (--checkpoint, default the\n"
        << "bundle's), from the bundle alone: the run, its job graph's fingerprint, the\n"
        << "bounding checkpoints, each Kafka source's topic, cluster and topic ids,\n"
        << "isolation level and offset ranges, and the sink's columns. A reference\n"
        << "implementation computes the scope's expected output from it, and\n"
        << "`clink incident-reconcile --reference` refuses an artifact computed for any\n"
        << "other scope. Reads no network.\n"
        << "\n"
        << "A windowed job's scope (version 2) spans the job's whole lineage, from a start\n"
        << "with no state through every restore the bundle records, and adds the window's\n"
        << "rules, each completed checkpoint's source positions and the watermark the sink\n"
        << "recorded at its barrier, by which a window is final.\n"
        << "\n"
        << "Exit status: 0 written, 3 for a scope whose evidence or source positions are\n"
        << "not whole (written once its columns are known; no reference can be complete\n"
        << "for it), or not written for a job whose state across records the audit does\n"
        << "not support, 2 bad usage or a request the bundle does not fit.\n";
}

void print_report(const Report& r, bool json, const std::string& report_path) {
    std::cout << (json ? r.render_json() : r.render_text()) << (json ? "\n" : "");
    if (!report_path.empty()) {
        std::ofstream out(report_path, std::ios::binary | std::ios::trunc);
        out << r.render_json() << "\n";
        if (!out) {
            std::cerr << "incident-verify: cannot write " << report_path << "\n";
        }
    }
}

}  // namespace

int clink_cmd_incident_collect(int argc, char** argv) {
    const auto a = parse_args(argc,
                              argv,
                              {"checkpoint-dir",
                               "job",
                               "out",
                               "ha-dir",
                               "capture-dir",
                               "trace-dir",
                               "commit-confirmation",
                               "checkpoint",
                               "op",
                               "epochs",
                               "max-bytes",
                               "max-files",
                               "timeout-s"},
                              {"include-state", "include-captures", "json", "help"});
    if (a.switches.contains("help")) {
        collect_usage();
        return 0;
    }
    CollectOptions o;
    o.checkpoint_dir = a.get("checkpoint-dir");
    o.out = a.get("out");
    const auto job = parse_u64(a.get("job"));
    std::string error = a.error;
    if (error.empty() && !a.positional.empty()) {
        error = "unexpected argument '" + a.positional.front() + "'";
    }
    if (error.empty() && (o.checkpoint_dir.empty() || o.out.empty() || !job.has_value())) {
        error = "--checkpoint-dir, --job and --out are required";
    }
    if (job.has_value()) {
        o.job = *job;
    }
    o.ha_dir = a.get("ha-dir");
    o.capture_dir = a.get("capture-dir");
    o.trace_dir = a.get("trace-dir");
    o.include_state = a.switches.contains("include-state");
    o.include_captures = a.switches.contains("include-captures");
    if (a.has("commit-confirmation")) {
        const auto v = a.get("commit-confirmation");
        if (v == "yes" || v == "no") {
            o.commit_confirmation = v == "yes";
        } else {
            error = "--commit-confirmation takes yes or no";
        }
    }
    const auto number = [&](const char* flag, auto& into) {
        if (!a.has(flag)) {
            return;
        }
        const auto v = parse_u64(a.get(flag));
        if (!v.has_value()) {
            error = std::string{"--"} + flag + " takes a whole number";
            return;
        }
        into = static_cast<std::remove_reference_t<decltype(into)>>(*v);
    };
    std::uint64_t checkpoint = 0;
    number("checkpoint", checkpoint);
    if (a.has("checkpoint")) {
        o.checkpoint = checkpoint;
    }
    number("max-bytes", o.max_bytes);
    number("max-files", o.max_files);
    std::uint64_t timeout = 0;
    number("timeout-s", timeout);
    if (a.has("timeout-s")) {
        o.timeout = std::chrono::seconds{static_cast<std::int64_t>(timeout)};
    }
    if (a.has("op")) {
        for (const auto& list : a.values.at("op")) {
            std::size_t start = 0;
            while (start <= list.size()) {
                const auto comma = list.find(',', start);
                const auto item = list.substr(
                    start, comma == std::string::npos ? std::string::npos : comma - start);
                const auto id = parse_u64(item);
                if (!id.has_value()) {
                    error = "--op takes operator ids, comma-separated";
                    break;
                }
                o.ops.push_back(*id);
                if (comma == std::string::npos) {
                    break;
                }
                start = comma + 1;
            }
        }
    }
    if (a.has("epochs")) {
        const auto v = a.get("epochs");
        const auto dash = v.find('-');
        const auto from = parse_u64(v.substr(0, dash));
        const auto to = dash == std::string::npos ? from : parse_u64(v.substr(dash + 1));
        if (!from.has_value() || !to.has_value() || *from == 0 || *to < *from) {
            error = "--epochs takes <from>-<to>, both at least 1";
        } else {
            o.epochs = std::make_pair(*from, *to);
        }
    }
    if ((!o.ops.empty() || o.epochs.has_value()) && !o.include_captures && error.empty()) {
        error = "--op and --epochs select captures, which need --include-captures";
    }
    if (!error.empty()) {
        std::cerr << "incident-collect: " << error << "\n\n";
        collect_usage();
        return 2;
    }
    try {
        const auto r = collect(o);
        if (a.switches.contains("json")) {
            clink::config::JsonArray notes;
            for (const auto& n : r.notes) {
                notes.emplace_back(n);
            }
            clink::config::JsonObject out;
            out["bundle"] = clink::config::JsonValue{r.bundle.string()};
            out["checkpoint"] = clink::config::JsonValue{static_cast<std::int64_t>(r.checkpoint)};
            out["files"] = clink::config::JsonValue{static_cast<std::int64_t>(r.files)};
            out["bytes"] = clink::config::JsonValue{static_cast<std::int64_t>(r.bytes)};
            out["seconds"] = clink::config::JsonValue{r.seconds};
            out["notes"] = clink::config::JsonValue{std::move(notes)};
            std::cout << clink::config::JsonValue{std::move(out)}.serialize(0) << "\n";
        } else {
            std::cout << "bundle:     " << r.bundle.string() << "\n"
                      << "checkpoint: " << r.checkpoint << "\n"
                      << "files:      " << r.files << " (" << r.bytes << " bytes)\n"
                      << "seconds:    " << r.seconds << "\n";
            for (const auto& n : r.notes) {
                std::cout << "note: " << n << "\n";
            }
        }
        return 0;
    } catch (const CollectError& e) {
        std::cerr << "incident-collect: refused: " << e.what() << "\n";
        if (a.switches.contains("json")) {
            // A caller that acts on the refusal reads its kind, not its text.
            clink::config::JsonObject out;
            out["refused"] = clink::config::JsonValue{true};
            out["kind"] = clink::config::JsonValue{std::string{collect_error_kind_name(e.kind)}};
            out["reason"] = clink::config::JsonValue{std::string{e.what()}};
            std::cout << clink::config::JsonValue{std::move(out)}.serialize(0) << "\n";
        }
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "incident-collect: " << e.what() << "\n";
        return 1;
    }
}

int clink_cmd_incident_verify(int argc, char** argv) {
    const auto a = parse_args(argc,
                              argv,
                              {"profile",
                               "expected",
                               "op",
                               "subtask",
                               "epoch",
                               "formal-dir",
                               "formal-tools-dir",
                               "report",
                               "max-replays"},
                              {"json", "help"});
    if (a.switches.contains("help")) {
        verify_usage();
        return 0;
    }
    std::string error = a.error;
    if (error.empty() && a.positional.size() != 1) {
        error = "name exactly one bundle directory";
    }
    VerifyOptions o;
    if (a.has("profile")) {
        o.profile = a.get("profile");
    }
    o.formal_dir = a.get("formal-dir");
    o.formal_tools_dir = a.get("formal-tools-dir");
    const auto expected = a.get("expected");
    std::vector<std::string> required;
    try {
        required = required_checks(o.profile, !expected.empty());
    } catch (const std::invalid_argument& e) {
        error = e.what();
    }
    std::optional<std::uint64_t> op;
    std::optional<std::uint64_t> subtask;
    std::optional<std::uint64_t> epoch;
    for (const auto& [flag, into] :
         {std::pair<const char*, std::optional<std::uint64_t>*>{"op", &op},
          {"subtask", &subtask},
          {"epoch", &epoch}}) {
        if (a.has(flag)) {
            *into = parse_u64(a.get(flag));
            if (!into->has_value()) {
                error = std::string{"--"} + flag + " takes a whole number";
            }
        }
    }
    std::size_t max_replays = 64;
    if (a.has("max-replays")) {
        const auto v = parse_u64(a.get("max-replays"));
        if (!v.has_value() || *v == 0 || *v > 100000) {
            error = "--max-replays takes a whole number from 1 to 100000";
        } else {
            max_replays = static_cast<std::size_t>(*v);
        }
    }
    if (!expected.empty() && o.profile != "replay" && o.profile != "full" && error.empty()) {
        error = "--expected needs --profile=replay or --profile=full";
    }
    if (!error.empty()) {
        std::cerr << "incident-verify: " << error << "\n\n";
        verify_usage();
        return 2;
    }
    const bool json = a.switches.contains("json");
    Report report{
        .bundle = a.positional.front(), .profile = o.profile, .required = required, .checks = {}};
    Bundle bundle;
    try {
        bundle = read_bundle(a.positional.front(), o.limits);
    } catch (const BundleRefused& e) {
        report.checks.push_back(refused_check(e.what()));
        print_report(report, json, a.get("report"));
        return 2;
    }
    try {
        report.checks = verify_core(bundle, o);
        if (o.profile == "replay" || o.profile == "full") {
            bool intact = false;
            for (const auto& c : report.checks) {
                intact = intact || (c.id == "bundle.artifacts" && c.verdict == Verdict::Passed);
            }
            if (intact) {
                for (auto& c : clink_incident_replay_checks(
                         bundle, op, subtask, epoch, expected, max_replays)) {
                    report.checks.push_back(std::move(c));
                }
            } else {
                for (const auto& id : required) {
                    if (id.starts_with("replay.")) {
                        report.checks.push_back(Check{
                            .id = id,
                            .verdict = Verdict::Unavailable,
                            .evidence = {},
                            .reason =
                                "the bundle's artifacts are not intact, so nothing is replayed",
                            .next_action = "see bundle.artifacts",
                            .details = {},
                            .counts = {}});
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        // The checks report what they find; this is anything they did not foresee.
        std::cerr << "incident-verify: " << e.what() << "\n";
        return 2;
    }
    print_report(report, json, a.get("report"));
    return report.exit_code();
}

int clink_cmd_incident_reconcile(int argc, char** argv) {
    const auto a = parse_args(argc,
                              argv,
                              {"host",
                               "port",
                               "user",
                               "password-env",
                               "tls-ca-file",
                               "tls-verify",
                               "key",
                               "checkpoint",
                               "sink-op",
                               "job",
                               "database",
                               "table",
                               "query-timeout-ms",
                               "max-rows-per-query",
                               "keys-per-query",
                               "settle-ms",
                               "list",
                               "expected-out",
                               "report",
                               "reference",
                               "max-reference-bytes",
                               "coverage",
                               "max-table-rows"},
                              {"json", "help", "secure", "from-run-start"});
    if (a.switches.contains("help")) {
        reconcile_usage();
        return 0;
    }
    std::string error = a.error;
    if (error.empty() && a.positional.size() != 1) {
        error = "name exactly one bundle directory";
    }
    if (error.empty() && a.get("host").empty()) {
        error = "--host is required: the destination is the operator's to name";
    }
    namespace rc = clink::incident::reconcile;
    rc::Options o;
    ClinkReconcileTarget target;
    target.host = a.get("host");
    if (a.has("user")) {
        target.user = a.get("user");
    }
    target.secure = a.switches.contains("secure");
    target.tls_ca_file = a.get("tls-ca-file");
    if (a.has("tls-verify")) {
        const auto v = a.get("tls-verify");
        if (v != "true" && v != "false") {
            error = "--tls-verify takes true or false";
        }
        target.tls_verify = v == "true";
    }
    if (!target.secure && (a.has("tls-ca-file") || a.has("tls-verify")) && error.empty()) {
        error = "--tls-ca-file and --tls-verify need --secure";
    }
    if (a.has("password-env")) {
        const char* v = std::getenv(a.get("password-env").c_str());
        if (v == nullptr) {
            error = "--password-env names " + a.get("password-env") + ", which is not set";
        } else {
            target.password = v;
        }
    }
    const auto number =
        [&](const char* flag, std::uint64_t lo, std::uint64_t hi) -> std::optional<std::uint64_t> {
        if (!a.has(flag)) {
            return std::nullopt;
        }
        const auto v = parse_u64(a.get(flag));
        if (!v || *v < lo || *v > hi) {
            error = std::string{"--"} + flag + " takes a whole number from " + std::to_string(lo) +
                    " to " + std::to_string(hi);
            return std::nullopt;
        }
        return v;
    };
    if (const auto v = number("port", 1, 65535)) {
        target.port = static_cast<std::uint16_t>(*v);
    }
    if (const auto v = number("checkpoint", 1, UINT64_MAX)) {
        o.checkpoint = *v;
    }
    if (const auto v = number("sink-op", 1, UINT64_MAX)) {
        o.sink_op = *v;
    }
    if (const auto v = number("job", 0, UINT64_MAX)) {
        o.job = *v;
    }
    if (const auto v = number("query-timeout-ms", 1, 3600000)) {
        o.bounds.query_timeout = std::chrono::milliseconds{*v};
    }
    if (const auto v = number("max-rows-per-query", 1, 100000000)) {
        o.bounds.max_rows_per_query = *v;
    }
    if (const auto v = number("keys-per-query", 1, 100000)) {
        o.bounds.keys_per_query = static_cast<std::size_t>(*v);
    }
    if (const auto v = number("settle-ms", 0, 600000)) {
        o.settle = std::chrono::milliseconds{*v};
    }
    if (const auto v = number("list", 0, 100000)) {
        o.list_limit = static_cast<std::size_t>(*v);
    }
    if (const auto v = number("max-reference-bytes", 1, std::uint64_t{1} << 40)) {
        o.max_reference_bytes = *v;
    }
    if (const auto v = number("max-table-rows", 1, 1000000000)) {
        o.max_table_rows = *v;
    }
    o.from_run_start = a.switches.contains("from-run-start");
    o.reference = a.get("reference");
    if (a.has("coverage")) {
        const auto c = a.get("coverage");
        if (c == "complete-table") {
            o.coverage = rc::Options::Coverage::CompleteTable;
        } else if (c != "selected-keys") {
            error = "--coverage takes selected-keys or complete-table";
        }
    }
    o.database = a.get("database");
    o.table = a.get("table");
    {
        std::string keys = a.get("key");
        std::size_t start = 0;
        while (start <= keys.size() && !keys.empty()) {
            const auto comma = keys.find(',', start);
            const auto part =
                keys.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
            if (part.empty()) {
                error = "--key takes column names separated by commas";
                break;
            }
            o.key.push_back(part);
            if (comma == std::string::npos) {
                break;
            }
            start = comma + 1;
        }
        // A windowed job's key is its windows' identity, which the bundle
        // names; any other job's is required, which the reconciliation says.
    }
    if (!error.empty()) {
        std::cerr << "incident-reconcile: " << error << "\n\n";
        reconcile_usage();
        return 2;
    }
    rc::Report report;
    try {
        report = clink_incident_reconcile(a.positional.front(), target, o);
    } catch (const std::exception& e) {
        std::cerr << "incident-reconcile: " << e.what() << "\n";
        return 2;
    }
    const bool json = a.switches.contains("json");
    std::cout << (json ? report.render_json() : report.render_text());
    if (a.has("report")) {
        std::ofstream out(a.get("report"), std::ios::binary | std::ios::trunc);
        out << report.render_json();
        if (!out) {
            std::cerr << "incident-reconcile: cannot write " << a.get("report") << "\n";
        }
    }
    if (a.has("expected-out") && !report.refused) {
        std::ofstream out(a.get("expected-out"), std::ios::binary | std::ios::trunc);
        out << rc::expected_ndjson(report.scope);
        if (!out) {
            std::cerr << "incident-reconcile: cannot write " << a.get("expected-out") << "\n";
        }
    }
    return report.exit_code();
}

int clink_cmd_incident_scope(int argc, char** argv) {
    const auto a =
        parse_args(argc, argv, {"out", "checkpoint", "sink-op", "job"}, {"help", "from-run-start"});
    if (a.switches.contains("help")) {
        scope_usage();
        return 0;
    }
    std::string error = a.error;
    if (error.empty() && a.positional.size() != 1) {
        error = "name exactly one bundle directory";
    }
    if (error.empty() && a.get("out").empty()) {
        error = "--out is required";
    }
    std::uint64_t checkpoint = 0;
    std::uint64_t sink_op = 0;
    std::optional<std::uint64_t> job;
    for (const auto& [flag, into] :
         {std::pair{"checkpoint", &checkpoint}, std::pair{"sink-op", &sink_op}}) {
        if (error.empty() && a.has(flag)) {
            const auto v = parse_u64(a.get(flag));
            if (!v || *v == 0) {
                error = std::string{"--"} + flag + " takes a whole number above 0";
            } else {
                *into = *v;
            }
        }
    }
    if (error.empty() && a.has("job")) {
        job = parse_u64(a.get("job"));
        if (!job) {
            error = "--job takes a whole number";
        }
    }
    if (!error.empty()) {
        std::cerr << "incident-scope: " << error << "\n\n";
        scope_usage();
        return 2;
    }
    namespace rc = clink::incident::reconcile;
    clink::incident::Bundle b;
    try {
        b = clink::incident::read_bundle(a.positional.front());
    } catch (const clink::incident::BundleRefused& e) {
        std::cerr << "incident-scope: the bundle cannot be read: " << e.what() << "\n";
        return 2;
    }
    if (job && *job != b.job()) {
        std::cerr << "incident-scope: wrong run: the bundle is evidence of job " << b.job()
                  << ", not job " << *job << "\n";
        return 2;
    }
    clink::incident::VerifyOptions vo;
    vo.profile = "integrity";
    for (const auto& c : clink::incident::verify_core(b, vo)) {
        if (c.verdict != clink::incident::Verdict::Passed) {
            std::cerr << "incident-scope: the bundle's integrity check " << c.id
                      << " did not pass (" << c.reason << "), so its evidence is not read\n";
            return 2;
        }
    }
    rc::Scope sc;
    try {
        sc = rc::load_scope(b, checkpoint, sink_op, a.switches.contains("from-run-start"), true);
    } catch (const rc::ScopeRefused& e) {
        std::cerr << "incident-scope: " << e.what() << "\n";
        return 2;
    }
    if (sc.columns.empty()) {
        // Evidence missing before the sink's columns were read: the scope is
        // not whole, which a bundle collected with the evidence can make it.
        std::cerr << "incident-scope: the scope names no columns"
                  << (sc.gap ? ": " + *sc.gap : std::string{}) << "\n";
        return sc.gap ? 3 : 2;
    }
    if (sc.window_unsupported) {
        std::cerr << "incident-scope: the job keeps state across records in a form the audit "
                     "does not support, so no reference can be computed for it: "
                  << *sc.window_unsupported << "\n";
        return 3;
    }
    const auto doc = rc::scope_document(b, sc);
    std::ofstream out(a.get("out"), std::ios::binary | std::ios::trunc);
    out << doc.serialize(2) << "\n";
    if (!out) {
        std::cerr << "incident-scope: cannot write " << a.get("out") << "\n";
        return 2;
    }
    std::cout << "scope " << doc.string_or("scope_sha256", "") << ": job " << sc.job
              << ", checkpoint " << sc.checkpoint << ", " << sc.expected.size()
              << " captured row(s), written to " << a.get("out") << "\n";
    if (sc.window) {
        std::cout << "a windowed scope: the job's lineage from a start with no state, "
                  << sc.lineage.size() << " completed checkpoint(s), " << sc.restores.size()
                  << " restore(s); windows are final at checkpoint " << sc.checkpoint
                  << " by the watermark recorded there ("
                  << (sc.lineage.empty() || !sc.lineage.back().watermark_ms
                          ? std::string{"none"}
                          : std::to_string(*sc.lineage.back().watermark_ms) + " ms")
                  << ")\n";
    }
    if (sc.gap) {
        std::cerr << "incident-scope: the scope is not whole: " << *sc.gap << "\n";
        return 3;
    }
    if (!doc.bool_or("sources_complete", false)) {
        std::cerr << "incident-scope: the scope does not record every source position and log "
                     "(collect with --include-state), so no reference can be complete for it\n";
        return 3;
    }
    return 0;
}

int clink_cmd_incident_regress(int argc, char** argv) {
    const auto a = parse_args(argc, argv, {"op", "epoch", "subtask", "out"}, {"help"});
    if (a.switches.contains("help")) {
        regress_usage();
        return 0;
    }
    std::string error = a.error;
    const auto op = parse_u64(a.get("op"));
    const auto epoch = parse_u64(a.get("epoch"));
    std::optional<std::uint64_t> subtask;
    if (a.has("subtask")) {
        subtask = parse_u64(a.get("subtask"));
        if (!subtask.has_value()) {
            error = "--subtask takes a whole number";
        }
    }
    const auto out = a.get("out");
    if (error.empty() && a.positional.size() != 1) {
        error = "name exactly one bundle directory";
    }
    if (error.empty() && (!op.has_value() || !epoch.has_value() || *epoch == 0 || out.empty())) {
        error = "--op, --epoch (at least 1) and --out are required";
    }
    if (!error.empty()) {
        std::cerr << "incident-regress: " << error << "\n\n";
        regress_usage();
        return 2;
    }
    std::error_code ec;
    if (fs::exists(out, ec)) {
        std::cerr << "incident-regress: refused: " << out << " already exists\n";
        return 1;
    }
    try {
        const auto bundle = read_bundle(a.positional.front());
        VerifyOptions integrity;
        integrity.profile = "integrity";
        for (const auto& c : verify_core(bundle, integrity)) {
            if (c.verdict != Verdict::Passed) {
                std::cerr << "incident-regress: refused: " << c.id << " is " << to_string(c.verdict)
                          << ": " << c.reason << "\n";
                return 1;
            }
        }
        const auto n = clink_incident_regress(bundle, *op, *epoch, subtask, out);
        std::cout << "wrote " << out << ": " << n << " golden emission(s), passing on this build\n"
                  << "add " << (fs::path{out} / "replay_regression_test.cpp").string()
                  << " to a test target linking clink::sql\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "incident-regress: refused: " << e.what() << "\n";
        return 1;
    }
}
