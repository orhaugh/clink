// clink incident-collect, incident-verify and incident-regress: incident
// evidence bundles (design record 014, docs/internals/incident-verification.md).
//
//   clink incident-collect --checkpoint-dir=<dir> --job=<id> --out=<dir> [...]
//   clink incident-verify <bundle> [--profile=<p>] [--expected=<ndjson>] [--json]
//   clink incident-regress <bundle> --op=<id> --epoch=<n> --out=<dir>
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
// refuse, and 2 on bad usage.

#include <charconv>
#include <chrono>
#include <cstdint>
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
              << "copied, a required file is past a bound, or the time runs out.\n"
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
        << "(default consistency). --expected compares one replayed epoch with an\n"
        << "independent expectation, one JSON object per line. At most --max-replays\n"
        << "epochs (default 64) replay per run.\n"
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
                            .details = {}});
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
