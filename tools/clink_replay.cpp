// clink replay - deterministic epoch replay over the flight recorder's
// captures: one operator, or the whole job.
//
//   clink replay --capture-dir=<dir> --checkpoint-dir=<dir> --epoch=N \
//                [--op=<operator id>] [--subtask=S] [--state-from=<ckpt id>] \
//                [--max-rows=N] [--flush] [--verify]
//
// With --op: rebuilds that operator from its op.json sidecar, restores
// its keyed state from checkpoint N-1, feeds it epoch-N.cap's captured
// event stream (records, watermarks, timer-fire clock positions) through
// the production paths, and prints every emission - offline, no cluster.
// Without --op: replays EVERY captured operator of the job for the epoch,
// each from its own captured input (which embeds the production
// interleaving, so the sweep is deterministic even around joins), and
// prints a per-operator summary.
//
// --verify replays each selected epoch TWICE from the same snapshot and
// event stream and byte-compares the emissions - the determinism gate
// (docs/internals/replay-determinism.md).
//
// The engine lives in clink::sql::EpochReplay (include/clink/sql/replay.hpp);
// this file is the CLI.
//
// Exit codes: 0 = ok, 1 = --verify found a divergence, 2 = error.

#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/plugin_loader.hpp"
#include "clink/sql/replay.hpp"

namespace {

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

void usage() {
    std::cerr
        << "Usage: clink replay --capture-dir=<dir> --checkpoint-dir=<dir> --epoch=<N|final>\n"
        << "                    [--op=<operator id>] [--subtask=S] [--state-from=<ckpt id>]\n"
        << "                    [--max-rows=N] [--flush] [--verify]\n"
        << "\n"
        << "With --op: rebuild one operator from its op.json capture sidecar,\n"
        << "restore its keyed state from checkpoint N-1, feed it epoch-N.cap's\n"
        << "captured event stream (records, watermarks, timer-fire clock\n"
        << "positions), and print every emission - offline, no cluster.\n"
        << "--epoch=1 starts from fresh state; --epoch=final requires\n"
        << "--state-from. v2 captures replay watermark-driven window fires and\n"
        << "processing-time timer fires at their production positions; v1\n"
        << "captures replay data records only.\n"
        << "\n"
        << "Without --op: replay EVERY captured operator of the job for the\n"
        << "epoch (each from its own captured input) and print a per-operator\n"
        << "summary; skipped operators (no epoch file, unsupported channel)\n"
        << "are listed with the reason.\n"
        << "\n"
        << "--verify replays each selected epoch TWICE and byte-compares the\n"
        << "emissions: the determinism gate\n"
        << "(docs/internals/replay-determinism.md).\n"
        << "\n"
        << "Cross-version A/B:\n"
        << "  --out=<file>      write ALL emissions to <file>, one per line\n"
        << "                    (single-op mode; ignores --max-rows)\n"
        << "  --plugin=<so>     dlopen a job plugin before replaying, so the\n"
        << "                    operator rebuilds from a CANDIDATE build - then\n"
        << "                    diff two --out dumps with `clink replay-diff`\n"
        << "\n"
        << "Exit codes: 0 = ok, 1 = --verify found a divergence, 2 = error.\n";
}

struct Options {
    std::string capture_dir;
    std::string checkpoint_dir;
    std::string epoch;
    std::optional<std::uint64_t> subtask;
    std::optional<std::uint64_t> state_from;
    bool flush{false};
    bool verify{false};
    std::size_t max_rows{100};
    std::string out_path;
};

clink::sql::ReplayRequest to_request(const Options& o, std::uint64_t op_id) {
    clink::sql::ReplayRequest req;
    req.capture_dir = o.capture_dir;
    req.checkpoint_dir = o.checkpoint_dir;
    req.op_id = op_id;
    req.subtask = o.subtask;
    req.epoch = o.epoch;
    req.state_from = o.state_from;
    req.flush = o.flush;
    return req;
}

void announce(const clink::sql::ReplayInfo& info) {
    std::cout << "replay: op " << info.op_id << " (" << info.spec.op_type << ") subtask "
              << info.subtask << "\n  input: " << info.capture_path << " (format v"
              << info.format_version << ": " << info.data_count << " records stored, "
              << info.records_seen << " seen, " << info.watermark_count << " watermarks, "
              << info.clock_count << " clock advances"
              << (info.truncated ? ", TRUNCATED - replaying the stored prefix" : "") << ")\n  "
              << info.state_desc << "\n";
    if (info.format_version < 2) {
        std::cerr << "note: v1 capture (records only) - watermark-driven fires do not occur "
                     "in this replay\n";
    }
}

// --verify on one loaded epoch: two runs, byte-compared. Returns 0/1.
int verify_one(const clink::sql::EpochReplay& replay, bool print_pass) {
    const auto a = replay.run();
    const auto b = replay.run();
    if (a == b) {
        if (print_pass) {
            std::cout << "deterministic: " << a.size() << " emissions identical across 2 runs\n";
        }
        return 0;
    }
    std::cout << "NON-DETERMINISTIC: run 1 emitted " << a.size() << ", run 2 emitted " << b.size()
              << "\n";
    const auto n = std::min(a.size(), b.size());
    for (std::size_t i = 0; i < n; ++i) {
        if (a[i] != b[i]) {
            std::cout << "first divergence at emission " << i << ":\n  run 1: " << a[i]
                      << "\n  run 2: " << b[i] << "\n";
            break;
        }
    }
    return 1;
}

int replay_single(const Options& o, std::uint64_t op_id) {
    const auto replay = clink::sql::EpochReplay::load(to_request(o, op_id));
    announce(replay.info());
    if (o.verify) {
        return verify_one(replay, /*print_pass=*/true);
    }
    if (!o.out_path.empty()) {
        // Emission dump for cross-version diffing (`clink replay-diff`):
        // every emission, one per line, no truncation.
        const auto rows = replay.run();
        std::ofstream out(o.out_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot open --out file " + o.out_path);
        }
        for (const auto& r : rows) {
            out << r << "\n";
        }
        std::cout << "wrote " << rows.size() << " emissions to " << o.out_path << "\n";
        return 0;
    }
    std::cout << "---\n";
    const auto rows = replay.run();
    std::size_t shown = 0;
    for (const auto& r : rows) {
        if (o.max_rows != 0 && shown >= o.max_rows) {
            break;
        }
        ++shown;
        std::cout << r << "\n";
    }
    const auto& info = replay.info();
    std::cout << "---\nreplayed " << info.data_count << " records, " << info.watermark_count
              << " watermarks, " << info.clock_count << " clock advances -> " << rows.size()
              << " emissions";
    if (shown < rows.size()) {
        std::cout << " (" << shown << " shown; raise --max-rows)";
    }
    std::cout << "\n";
    return 0;
}

int replay_whole_job(const Options& o) {
    const auto ops = clink::sql::list_captured_ops(o.capture_dir);
    if (ops.empty()) {
        std::cerr << "clink replay: no operator captures under " << o.capture_dir << "\n";
        return 2;
    }
    std::cout << "replay: " << ops.size() << " captured operator(s), epoch " << o.epoch
              << (o.verify ? ", verifying determinism (2 runs each)" : "") << "\n";
    std::size_t replayed = 0;
    std::size_t skipped = 0;
    int worst = 0;
    for (const auto& op : ops) {
        Options per = o;
        per.subtask = op.subtask;
        try {
            const auto replay = clink::sql::EpochReplay::load(to_request(per, op.op_id));
            const auto& info = replay.info();
            std::cout << "  op " << op.op_id << " (" << info.spec.op_type << ") subtask "
                      << op.subtask << ": " << info.data_count << " records, "
                      << info.watermark_count << " watermarks, " << info.clock_count
                      << " clock advances";
            if (o.verify) {
                const auto a = replay.run();
                const auto b = replay.run();
                if (a == b) {
                    std::cout << " -> " << a.size() << " emissions [deterministic]\n";
                } else {
                    std::cout << " -> NON-DETERMINISTIC (" << a.size() << " vs " << b.size()
                              << " emissions)\n";
                    worst = 1;
                }
            } else {
                const auto rows = replay.run();
                std::cout << " -> " << rows.size() << " emissions"
                          << (info.truncated ? " [truncated capture]" : "") << "\n";
            }
            ++replayed;
        } catch (const std::exception& e) {
            std::cout << "  op " << op.op_id << " subtask " << op.subtask
                      << ": skipped: " << e.what() << "\n";
            ++skipped;
        }
    }
    std::cout << "replayed " << replayed << " operator(s), skipped " << skipped << "\n";
    if (o.verify && worst == 0 && replayed > 0) {
        std::cout << "deterministic: every replayed operator byte-identical across 2 runs\n";
    }
    return worst;
}

}  // namespace

int clink_cmd_replay(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        usage();
        return 0;
    }
    Options o;
    o.capture_dir = get_arg(argc, argv, "capture-dir");
    o.checkpoint_dir = get_arg(argc, argv, "checkpoint-dir");
    o.epoch = get_arg(argc, argv, "epoch");
    o.flush = has_flag(argc, argv, "flush");
    o.verify = has_flag(argc, argv, "verify");
    if (const auto v = get_arg(argc, argv, "subtask"); !v.empty()) {
        o.subtask = std::stoull(v);
    }
    if (const auto v = get_arg(argc, argv, "state-from"); !v.empty()) {
        o.state_from = std::stoull(v);
    }
    if (const auto v = get_arg(argc, argv, "max-rows"); !v.empty()) {
        o.max_rows = static_cast<std::size_t>(std::stoull(v));
    }
    o.out_path = get_arg(argc, argv, "out");
    const auto op_str = get_arg(argc, argv, "op");
    const auto plugin_path = get_arg(argc, argv, "plugin");
    if (o.capture_dir.empty() || o.epoch.empty()) {
        usage();
        return 2;
    }
    try {
        if (!plugin_path.empty()) {
            // Candidate-build A/B: load the job plugin so the operator
            // factories resolve from THAT .so (ABI-gated like a cluster
            // deploy), then replay production bytes through it.
            auto loaded = clink::cluster::PluginLoader::default_instance().load(plugin_path);
            if (!loaded.ok) {
                throw std::runtime_error("cannot load --plugin " + plugin_path + ": " +
                                         loaded.error);
            }
            std::cout << "plugin: " << loaded.plugin.name << " " << loaded.plugin.version << " ("
                      << plugin_path << ")\n";
        }
        if (op_str.empty()) {
            if (!o.out_path.empty()) {
                throw std::runtime_error("--out needs --op (single-operator mode)");
            }
            return replay_whole_job(o);
        }
        return replay_single(o, std::stoull(op_str));
    } catch (const std::exception& e) {
        std::cerr << "clink replay: " << e.what() << "\n";
        return 2;
    }
}

// ---- clink replay-diff ------------------------------------------------------

namespace {

void diff_usage() {
    std::cerr << "Usage: clink replay-diff <a> <b> [--max-diffs=N]\n"
              << "\n"
              << "Compare two replay emission dumps (written with clink replay\n"
              << "--out=<file>, one emission per line) - the cross-version A/B:\n"
              << "replay the same epoch through two builds (e.g. --plugin with a\n"
              << "candidate .so, or two engine versions), dump both, diff. Reports\n"
              << "the first divergence and up to N differing lines (default 10).\n"
              << "\n"
              << "Exit codes: 0 = identical, 1 = different, 2 = error.\n";
}

std::vector<std::string> read_lines(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path);
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    return lines;
}

}  // namespace

int clink_cmd_replay_diff(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        diff_usage();
        return 0;
    }
    std::vector<std::string> paths;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (!a.starts_with("--")) {
            paths.push_back(a);
        }
    }
    std::size_t max_diffs = 10;
    if (const auto v = get_arg(argc, argv, "max-diffs"); !v.empty()) {
        max_diffs = static_cast<std::size_t>(std::stoull(v));
    }
    if (paths.size() != 2) {
        diff_usage();
        return 2;
    }
    try {
        const auto a = read_lines(paths[0]);
        const auto b = read_lines(paths[1]);
        if (a == b) {
            std::cout << "identical: " << a.size() << " emissions\n";
            return 0;
        }
        std::cout << "different: " << paths[0] << " has " << a.size() << " emissions, " << paths[1]
                  << " has " << b.size() << "\n";
        const auto n = std::min(a.size(), b.size());
        std::size_t shown = 0;
        std::size_t differing = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (a[i] == b[i]) {
                continue;
            }
            ++differing;
            if (max_diffs == 0 || shown < max_diffs) {
                ++shown;
                std::cout << "emission " << i << ":\n  a: " << a[i] << "\n  b: " << b[i] << "\n";
            }
        }
        if (a.size() != b.size()) {
            const auto& longer = a.size() > b.size() ? a : b;
            const char side = a.size() > b.size() ? 'a' : 'b';
            const auto extra = longer.size() - n;
            std::cout << extra << " extra emission(s) in " << side << ", first: " << longer[n]
                      << "\n";
        }
        std::cout << differing << " differing emission(s) in the common prefix";
        if (shown < differing) {
            std::cout << " (" << shown << " shown; raise --max-diffs)";
        }
        std::cout << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "clink replay-diff: " << e.what() << "\n";
        return 2;
    }
}
