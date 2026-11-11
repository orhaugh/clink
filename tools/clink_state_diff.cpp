// clink state-diff / clink state-cat - time-travel inspection of keyed state.
//
// state-diff compares two snapshots of a job's keyed state and reports, per
// (operator, slot), exactly which keys appeared, vanished or changed - plus
// any state-schema version-stamp changes. state-cat dumps one snapshot's
// contents. Both read the InMemoryStateBackend-format `.snap` blobs that
// FileBackedStateBackend writes for checkpoints and savepoints (RocksDB's
// native SST checkpoints are a different format and are rejected clearly).
//
// Inputs:
//   * two explicit files:       state-diff --a=<f.snap> --b=<f.snap>
//   * one checkpoint dir + ids: state-diff --dir=<root> --from=N --to=M
//     The dir form merges every per-subtask file (<root>/<subtask>/
//     checkpoint-<id>.snap, plus <root>/checkpoint-<id>.snap for the
//     single-subtask flat layout); key groups are disjoint across subtasks
//     so the merge is a clean union.
//
// The checkpoint-consistent forms read and write checkpoint datasets
// (clink/state_processor/checkpoint_dataset.hpp): state-export
// --checkpoint-dir exports one, state-diff --a/--b compares two directories
// holding one, and state-cat --dataset lists one. They render bytes as hex
// with their lengths and guess no type.
//
// Exit codes (diff): 0 = identical, 1 = differences found, 2 = error;
//   for datasets, 3 = not established (a partial side, a slot not
//   compared, a limit reached).
// Exit codes (cat):  0 = ok, 2 = error; 3 = a partial dataset, refused.
// Exit codes (export, dataset form): 0 = complete, 3 = published partial,
//   1 = refused (nothing published), 2 = usage.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/state_processor/checkpoint_dataset.hpp"
#include "clink/state_processor/parquet_export.hpp"
#include "clink/state_processor/savepoint.hpp"
#include "clink/state_processor/state_diff.hpp"

#include "state_tool_io.hpp"

#ifdef CLINK_LINKED_ROCKSDB
#include "clink/state/rocksdb_state_backend.hpp"
#endif
#ifdef CLINK_LINKED_ICEBERG
#include "clink/iceberg/state_export.hpp"
#endif

namespace {

namespace fs = std::filesystem;
using clink::state_processor::collect_entries;
using clink::state_processor::diff_entries;
using clink::state_processor::diff_versions;
using clink::state_processor::EntryDelta;
using clink::state_processor::merge_entries;
using clink::state_processor::render_bytes;
using clink::state_processor::Savepoint;
using clink::state_processor::StateDiffReport;
using clink::state_processor::StateEntries;

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

// Strict decimal: digits only, at most 20, and within 64 bits.
bool parse_uint(const std::string& s, std::uint64_t& out) {
    if (s.empty() || s.size() > 20 || s.find_first_not_of("0123456789") != std::string::npos) {
        return false;
    }
    try {
        out = std::stoull(s);
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

// Every argument is one of `allowed`, as --name=value or --name.
bool only_flags(int argc, char** argv, std::initializer_list<std::string_view> allowed) {
    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        if (!a.starts_with("--")) {
            std::cerr << "unexpected argument '" << a << "'\n";
            return false;
        }
        a.remove_prefix(2);
        const auto name = a.substr(0, a.find('='));
        if (std::find(allowed.begin(), allowed.end(), name) == allowed.end()) {
            std::cerr << "unknown option --" << name << "\n";
            return false;
        }
    }
    return true;
}

// A directory (not a link) holding a dataset.json: a checkpoint dataset.
bool is_dataset_dir(const std::string& p) {
    std::error_code ec;
    return !p.empty() && fs::is_directory(fs::symlink_status(p, ec)) &&
           fs::exists(fs::symlink_status(fs::path{p} / clink::state_processor::kDatasetFile, ec));
}

std::string json_text(const std::string& s) {
    return clink::config::JsonValue{s}.serialize(0);
}

using clink_tools::canonical_bytes_for;
using clink_tools::checkpoint_files;
using clink_tools::is_rocksdb_checkpoint_dir;

// Load one side of the comparison: either an explicit .snap file, or a
// checkpoint dir + id merged across subtask files. Also returns the loaded
// Savepoints so version stamps stay inspectable (versions ride the backend,
// not the entry model). Throws with a clear message on any failure.
struct LoadedSide {
    std::string label;
    StateEntries entries;
    std::vector<Savepoint> savepoints;
};

LoadedSide load_file(const std::string& path,
                     const std::shared_ptr<clink::ExternalMaterializationStore>& store = nullptr) {
    LoadedSide side;
    side.label = path;
    // canonical_bytes_for handles every input shape: a canonical .snap
    // verbatim, a changelog snapshot replayed to canonical form (external
    // materialisation handles resolved via `store`), or a RocksDB
    // checkpoint dir rendered through the Arrow export.
    clink::Snapshot snap;
    snap.bytes = canonical_bytes_for(path, store);
    side.savepoints.push_back(Savepoint::load_from_snapshot(std::move(snap)));
    side.entries = collect_entries(side.savepoints.back());
    return side;
}

LoadedSide load_dir(const std::string& dir,
                    std::uint64_t id,
                    const std::shared_ptr<clink::ExternalMaterializationStore>& store = nullptr) {
    LoadedSide side;
    side.label = dir + " @ checkpoint " + std::to_string(id);
    const auto files = checkpoint_files(dir, id);
    if (files.empty()) {
        throw std::runtime_error("no checkpoint-" + std::to_string(id) + ".snap under " + dir +
                                 " (or its subtask subdirectories)");
    }
    for (const auto& f : files) {
        clink::Snapshot snap;
        snap.bytes = canonical_bytes_for(f.string(), store);
        side.savepoints.push_back(Savepoint::load_from_snapshot(std::move(snap)));
        merge_entries(side.entries, collect_entries(side.savepoints.back()));
    }
    return side;
}

const char* kind_symbol(EntryDelta::Kind k) {
    switch (k) {
        case EntryDelta::Kind::Added:
            return "+";
        case EntryDelta::Kind::Removed:
            return "-";
        case EntryDelta::Kind::Changed:
            return "~";
    }
    return "?";
}

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

void print_diff_text(const LoadedSide& a, const LoadedSide& b, const StateDiffReport& report) {
    std::cout << "state-diff\n  A: " << a.label << " (" << report.total_entries_a
              << " entries)\n  B: " << b.label << " (" << report.total_entries_b << " entries)\n";
    if (report.identical()) {
        std::cout << "identical: no keyed-state or schema-version differences\n";
        return;
    }
    for (const auto& d : report.slots) {
        std::cout << "\nop " << d.op.value() << " slot \"" << d.slot << "\": +" << d.added << " -"
                  << d.removed << " ~" << d.changed << " (" << d.unchanged << " unchanged)\n";
        for (const auto& s : d.samples) {
            std::cout << "  " << kind_symbol(s.kind) << " kg=" << static_cast<int>(s.key_group)
                      << " key " << render_bytes(s.user_key, 32);
            switch (s.kind) {
                case EntryDelta::Kind::Added:
                    std::cout << "  -> " << render_bytes(s.value_b);
                    break;
                case EntryDelta::Kind::Removed:
                    std::cout << "  was " << render_bytes(s.value_a);
                    break;
                case EntryDelta::Kind::Changed:
                    std::cout << "  " << render_bytes(s.value_a) << " -> "
                              << render_bytes(s.value_b);
                    break;
            }
            std::cout << "\n";
        }
        const auto shown = d.samples.size();
        const auto total = d.added + d.removed + d.changed;
        if (shown < total) {
            std::cout << "  ... " << (total - shown) << " more (raise --max-rows)\n";
        }
    }
    for (const auto& v : report.versions) {
        std::cout << "version: op " << v.op.value() << " type \"" << v.state_type << "\": v"
                  << v.version_a << " -> v" << v.version_b << " (0 = absent)\n";
    }
}

void print_diff_json(const LoadedSide& a, const LoadedSide& b, const StateDiffReport& report) {
    std::cout << "{\"a\":\"" << json_escape(a.label) << "\",\"b\":\"" << json_escape(b.label)
              << "\",\"entries_a\":" << report.total_entries_a
              << ",\"entries_b\":" << report.total_entries_b
              << ",\"identical\":" << (report.identical() ? "true" : "false") << ",\"slots\":[";
    bool first_slot = true;
    for (const auto& d : report.slots) {
        if (!first_slot) {
            std::cout << ",";
        }
        first_slot = false;
        std::cout << "{\"op\":" << d.op.value() << ",\"slot\":\"" << json_escape(d.slot)
                  << "\",\"added\":" << d.added << ",\"removed\":" << d.removed
                  << ",\"changed\":" << d.changed << ",\"unchanged\":" << d.unchanged
                  << ",\"samples\":[";
        bool first_sample = true;
        for (const auto& s : d.samples) {
            if (!first_sample) {
                std::cout << ",";
            }
            first_sample = false;
            std::cout << "{\"kind\":\"" << kind_symbol(s.kind)
                      << "\",\"key_group\":" << static_cast<int>(s.key_group) << ",\"key\":\""
                      << json_escape(render_bytes(s.user_key, 32)) << "\",\"value_a\":\""
                      << json_escape(render_bytes(s.value_a)) << "\",\"value_b\":\""
                      << json_escape(render_bytes(s.value_b)) << "\"}";
        }
        std::cout << "]}";
    }
    std::cout << "],\"versions\":[";
    bool first_v = true;
    for (const auto& v : report.versions) {
        if (!first_v) {
            std::cout << ",";
        }
        first_v = false;
        std::cout << "{\"op\":" << v.op.value() << ",\"state_type\":\"" << json_escape(v.state_type)
                  << "\",\"version_a\":" << v.version_a << ",\"version_b\":" << v.version_b << "}";
    }
    std::cout << "]}\n";
}

void diff_usage() {
    std::cerr << "Usage: clink state-diff --a=<dataset> --b=<dataset> [--raw-bytes] "
                 "[--max-samples=N]\n"
              << "           [--max-entries=N] [--timeout-s=N] [--allow-partial] [--json]\n"
              << "       clink state-diff --a=<f.snap> --b=<f.snap> [--json] [--max-rows=N]\n"
              << "       clink state-diff --dir=<checkpoint-root> --from=N --to=M [--json] "
                 "[--max-rows=N]\n"
              << "\n"
              << "Two checkpoint datasets (clink state-export --checkpoint-dir): a streaming\n"
              << "comparison of every slot, reporting entries added, removed, changed and\n"
              << "unchanged, keyed entries relocated to another subtask (not a change), the\n"
              << "lineage between the two, and every slot it could not compare and why. A\n"
              << "slot whose schema stamps differ is not compared unless --raw-bytes, which\n"
              << "compares its stored bytes and labels it raw. Operator state is compared\n"
              << "only between the same subtasks. Bytes are shown as hex with their length.\n"
              << "Exit codes: 0 no differences, 1 differences, 3 not established (a partial\n"
              << "side, a slot not compared, a limit reached), 2 error.\n"
              << "\n"
              << "  --max-samples=N  differing entries shown per slot (default 20)\n"
              << "  --max-entries=N  entries read across both sides (default 100000000)\n"
              << "  --timeout-s=N    time limit (default 600)\n"
              << "  --allow-partial  compare a partial dataset; the result is still not\n"
              << "                   established\n"
              << "\n"
              << "The snapshot forms below read whatever snapshot files exist: they do not\n"
              << "check a checkpoint's completion marker or participants, so they are not\n"
              << "checkpoint-consistent, and they render 8-byte values as int64 by guess.\n"
              << "\n"
              << "Compares the keyed state of two snapshots (checkpoints or savepoints)\n"
              << "and reports added / removed / changed keys per operator state slot,\n"
              << "plus state-schema version-stamp changes. The --dir form merges every\n"
              << "per-subtask snapshot file of each checkpoint id.\n"
              << "\n"
              << "  --max-rows=N   samples shown per slot (default 20, 0 = all)\n"
              << "  --json         machine-readable report on stdout\n"
              << "\n"
              << "Exit codes: 0 = identical, 1 = differences found, 2 = error.\n";
}

void cat_usage() {
    std::cerr << "Usage: clink state-cat --dataset=<dataset> [--json] [--max-rows=N] "
                 "[--allow-partial]\n"
              << "       clink state-cat --dir=<checkpoint-root> --id=N [--json] [--max-rows=N]\n"
              << "\n"
              << "Dumps a snapshot's keyed state: operators, slots, and entries\n"
              << "(keys and values rendered as int64 / text / hex).\n"
              << "--dataset lists a checkpoint dataset (clink state-export --checkpoint-dir)\n"
              << "after verifying it: every slot, with its scope, schema stamps and entries,\n"
              << "each as hex with its length and the participant holding it. A partial\n"
              << "dataset is refused (exit 3) unless --allow-partial.\n"
              << "\n"
              << "  --max-rows=N   entries shown per slot (default 50, 0 = all)\n"
              << "\n"
              << "Exit codes: 0 = ok, 2 = error.\n";
}

}  // namespace

// ---- checkpoint datasets ----------------------------------------------------

namespace {

namespace sp = clink::state_processor;

int diff_datasets_cmd(int argc, char** argv) {
    if (!only_flags(argc,
                    argv,
                    {"a",
                     "b",
                     "raw-bytes",
                     "max-samples",
                     "max-entries",
                     "timeout-s",
                     "allow-partial",
                     "json"})) {
        diff_usage();
        return 2;
    }
    sp::StateDiffOptions o;
    o.raw_bytes = has_flag(argc, argv, "raw-bytes");
    o.allow_partial = has_flag(argc, argv, "allow-partial");
    std::uint64_t v = 0;
    if (const auto s = get_arg(argc, argv, "max-samples"); !s.empty()) {
        if (!parse_uint(s, v)) {
            diff_usage();
            return 2;
        }
        o.max_samples = static_cast<std::size_t>(v);
    }
    if (const auto s = get_arg(argc, argv, "max-entries"); !s.empty()) {
        if (!parse_uint(s, v)) {
            diff_usage();
            return 2;
        }
        o.max_entries = v;
    }
    if (const auto s = get_arg(argc, argv, "timeout-s"); !s.empty()) {
        if (!parse_uint(s, v) || v == 0) {
            diff_usage();
            return 2;
        }
        o.timeout = std::chrono::seconds{static_cast<std::int64_t>(v)};
    }
    try {
        const auto a = sp::open_dataset(get_arg(argc, argv, "a"));
        const auto b = sp::open_dataset(get_arg(argc, argv, "b"));
        const auto r = sp::diff_datasets(a, b, o);
        std::cout << (has_flag(argc, argv, "json") ? r.render_json() : r.render_text());
        return r.exit_code();
    } catch (const std::exception& e) {
        std::cerr << "clink state-diff: " << e.what() << "\n";
        return 2;
    }
}

int cat_dataset_cmd(int argc, char** argv) {
    if (!only_flags(argc, argv, {"dataset", "json", "max-rows", "allow-partial"})) {
        cat_usage();
        return 2;
    }
    std::uint64_t max_rows = 50;
    if (const auto s = get_arg(argc, argv, "max-rows"); !s.empty() && !parse_uint(s, max_rows)) {
        cat_usage();
        return 2;
    }
    const bool json = has_flag(argc, argv, "json");
    try {
        const auto d = sp::open_dataset(get_arg(argc, argv, "dataset"));
        if (!d.complete && !has_flag(argc, argv, "allow-partial")) {
            std::cerr << "clink state-cat: " << d.root.string()
                      << " is a PARTIAL dataset (see incomplete and unsupported in its "
                         "dataset.json); --allow-partial lists it anyway\n";
            return 3;
        }
        struct Shown {
            std::uint64_t rows{0};
            std::vector<sp::DatasetEntry> entries;
        };
        std::map<sp::SlotKey, Shown> shown;
        sp::for_each_entry(d, [&](const sp::DatasetEntry& e) {
            auto& s = shown[sp::SlotKey{e.op, e.slot, e.scope}];
            ++s.rows;
            if (max_rows == 0 || s.entries.size() < max_rows) {
                s.entries.push_back(e);
            }
            return true;
        });
        const auto schema_text = [&](std::uint64_t op, const std::string& slot) {
            const auto it = d.schemas.find({op, slot});
            if (it == d.schemas.end()) {
                return std::string{"no stamps"};
            }
            std::string t = it->second.fingerprint ? "fingerprint " + *it->second.fingerprint
                                                   : std::string{"no fingerprint"};
            for (const auto& [type, ver] : it->second.versions) {
                t += ", " + type + " v" + std::to_string(ver);
            }
            return t;
        };
        if (json) {
            std::cout << "{\"dataset\":" << json_text(d.root.string()) << ",\"job\":" << d.job
                      << ",\"checkpoint\":" << d.checkpoint << ",\"generation\":" << d.generation
                      << ",\"complete\":" << (d.complete ? "true" : "false")
                      << ",\"entries\":" << d.entries << ",\"slots\":[";
            bool first = true;
            for (const auto& [k, s] : shown) {
                const auto& [op, slot, scope] = k;
                std::cout << (first ? "" : ",") << "{\"op_id\":\"" << op
                          << "\",\"slot\":" << json_text(slot) << ",\"scope\":\""
                          << sp::scope_name(scope) << "\",\"rows\":" << s.rows
                          << ",\"schema\":" << json_text(schema_text(op, slot)) << ",\"entries\":[";
                first = false;
                bool fe = true;
                for (const auto& e : s.entries) {
                    std::cout << (fe ? "" : ",") << "{\"key_group\":"
                              << (e.key_group >= 0 ? std::to_string(e.key_group) : "null")
                              << ",\"participant\":" << e.participant << ",\"user_key_hex\":\""
                              << sp::hex_bytes(e.user_key)
                              << "\",\"user_key_bytes\":" << e.user_key.size()
                              << ",\"value_hex\":\"" << sp::hex_bytes(e.value)
                              << "\",\"value_bytes\":" << e.value.size() << "}";
                    fe = false;
                }
                std::cout << "],\"truncated\":" << (s.rows > s.entries.size() ? "true" : "false")
                          << "}";
            }
            std::cout << "]}\n";
            return 0;
        }
        std::cout << "state-cat: " << d.root.string() << " (job " << d.job << ", checkpoint "
                  << d.checkpoint << ", generation " << d.generation << ", "
                  << (d.complete ? "complete" : "PARTIAL") << ", " << d.entries << " entries)\n";
        for (const auto& [k, s] : shown) {
            const auto& [op, slot, scope] = k;
            std::cout << "\nop " << op << " slot \"" << slot << "\" (" << sp::scope_name(scope)
                      << ", " << s.rows << " entries, " << schema_text(op, slot) << ")\n";
            for (const auto& e : s.entries) {
                std::cout << "  ";
                if (e.key_group >= 0) {
                    std::cout << "kg=" << e.key_group << " ";
                }
                std::cout << "p=" << e.participant << " key " << sp::hex_bytes(e.user_key) << " ("
                          << e.user_key.size() << " bytes) = " << sp::hex_bytes(e.value) << " ("
                          << e.value.size() << " bytes)\n";
            }
            if (s.rows > s.entries.size()) {
                std::cout << "  ... " << (s.rows - s.entries.size())
                          << " more (raise --max-rows)\n";
            }
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink state-cat: " << e.what() << "\n";
        return 2;
    }
}

void export_dataset_usage() {
    std::cerr << "usage: clink state-export --checkpoint-dir=<dir> --job=<id>\n"
              << "           (--checkpoint=<N> | --restore-point) --out=<dataset>\n"
              << "           [--ha-dir=<dir>] [--commit-confirmation=yes|no] [--allow-partial]\n"
              << "           [--max-bytes=N] [--max-files=N] [--max-entries=N] [--timeout-s=N] "
                 "[--json]\n"
              << "\n"
              << "Export one completed checkpoint of a whole job as a verified dataset: the\n"
              << "checkpoint's markers and every participant's snapshot and sidecar, collected\n"
              << "under the engine's own rules and refused if anything changes meanwhile,\n"
              << "plus entries.parquet (every stored entry, exact bytes) and dataset.json (what\n"
              << "it holds, its provenance, and whether it is complete). Published by one\n"
              << "rename; never written over. Read-only; local directories only.\n"
              << "\n"
              << "  --restore-point     the checkpoint a restore of the job would choose now,\n"
              << "                      which needs --ha-dir or --commit-confirmation\n"
              << "  --allow-partial     publish a dataset that is not complete, marked partial\n"
              << "  --max-bytes/--max-files bound what is collected (default 1 GiB, 10000)\n"
              << "  --max-entries       bound the entries decoded (default 100000000)\n"
              << "\n"
              << "Exit codes: 0 complete, 3 published partial, 1 refused (nothing published),\n"
              << "2 usage.\n";
}

int export_dataset_cmd(int argc, char** argv) {
    if (!only_flags(argc,
                    argv,
                    {"checkpoint-dir",
                     "job",
                     "checkpoint",
                     "restore-point",
                     "out",
                     "ha-dir",
                     "commit-confirmation",
                     "allow-partial",
                     "max-bytes",
                     "max-files",
                     "max-entries",
                     "timeout-s",
                     "json"})) {
        export_dataset_usage();
        return 2;
    }
    sp::DatasetOptions o;
    o.checkpoint_dir = get_arg(argc, argv, "checkpoint-dir");
    o.out = get_arg(argc, argv, "out");
    o.ha_dir = get_arg(argc, argv, "ha-dir");
    o.allow_partial = has_flag(argc, argv, "allow-partial");
    const bool json = has_flag(argc, argv, "json");
    const auto ckpt = get_arg(argc, argv, "checkpoint");
    const bool restore_point = has_flag(argc, argv, "restore-point");
    bool ok = parse_uint(get_arg(argc, argv, "job"), o.job) && o.job != 0 && !o.out.empty();
    ok = ok && (ckpt.empty() != !restore_point);
    std::uint64_t v = 0;
    if (!ckpt.empty()) {
        ok = ok && parse_uint(ckpt, v) && v != 0;
        o.checkpoint = v;
    }
    if (const auto c = get_arg(argc, argv, "commit-confirmation"); !c.empty()) {
        ok = ok && (c == "yes" || c == "no");
        o.commit_confirmation = c == "yes";
    }
    const auto bound = [&](const char* name, std::uint64_t& into) {
        if (const auto s = get_arg(argc, argv, name); !s.empty()) {
            ok = ok && parse_uint(s, into) && into != 0;
        }
    };
    bound("max-bytes", o.max_bytes);
    bound("max-files", o.max_files);
    bound("max-entries", o.max_entries);
    std::uint64_t timeout = 600;
    bound("timeout-s", timeout);
    o.timeout = std::chrono::seconds{static_cast<std::int64_t>(timeout)};
    if (!ok) {
        export_dataset_usage();
        return 2;
    }
    sp::DatasetResult r;
    try {
        r = sp::export_checkpoint_dataset(o);
    } catch (const sp::DatasetRefused& e) {
        if (json) {
            std::cout << "{\"refused\":true,\"kind\":" << json_text(e.kind)
                      << ",\"reason\":" << json_text(e.what()) << "}\n";
        } else {
            std::cerr << "state-export: refused (" << e.kind << "): " << e.what() << "\n";
        }
        return 1;
    }
    if (json) {
        clink::config::JsonObject j;
        j["refused"] = clink::config::JsonValue{false};
        j["dataset"] = clink::config::JsonValue{r.dataset.string()};
        j["checkpoint"] = clink::config::JsonValue{static_cast<std::int64_t>(r.checkpoint)};
        j["complete"] = clink::config::JsonValue{r.complete};
        clink::config::JsonArray inc;
        for (const auto& s : r.incomplete) {
            inc.push_back(clink::config::JsonValue{s});
        }
        j["incomplete"] = clink::config::JsonValue{std::move(inc)};
        clink::config::JsonArray uns;
        for (const auto& s : r.unsupported) {
            uns.push_back(clink::config::JsonValue{s});
        }
        j["unsupported"] = clink::config::JsonValue{std::move(uns)};
        j["participants"] = clink::config::JsonValue{static_cast<std::int64_t>(r.participants)};
        j["entries"] = clink::config::JsonValue{static_cast<std::int64_t>(r.entries)};
        j["bytes"] = clink::config::JsonValue{static_cast<std::int64_t>(r.bytes)};
        j["seconds"] = clink::config::JsonValue{r.seconds};
        std::cout << clink::config::JsonValue{std::move(j)}.serialize(0) << "\n";
    } else {
        std::cout << "state-export: checkpoint " << r.checkpoint << " of job " << o.job << " -> "
                  << r.dataset.string() << " (" << (r.complete ? "complete" : "PARTIAL") << ", "
                  << r.participants << " participants, " << r.entries << " entries, " << r.bytes
                  << " bytes, " << r.seconds << " s)\n";
        for (const auto& s : r.unsupported) {
            std::cout << "  unsupported: " << s << "\n";
        }
        for (const auto& s : r.incomplete) {
            std::cout << "  incomplete: " << s << "\n";
        }
    }
    return r.complete ? 0 : 3;
}

}  // namespace

int clink_cmd_state_diff(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        diff_usage();
        return 0;
    }
    const auto file_a = get_arg(argc, argv, "a");
    const auto file_b = get_arg(argc, argv, "b");
    if (is_dataset_dir(file_a) || is_dataset_dir(file_b)) {
        if (!is_dataset_dir(file_a) || !is_dataset_dir(file_b)) {
            std::cerr << "clink state-diff: a dataset is compared only with another dataset\n";
            return 2;
        }
        return diff_datasets_cmd(argc, argv);
    }
    const auto dir = get_arg(argc, argv, "dir");
    const auto from = get_arg(argc, argv, "from");
    const auto to = get_arg(argc, argv, "to");
    const bool json = has_flag(argc, argv, "json");
    std::size_t max_rows = 20;
    if (const auto v = get_arg(argc, argv, "max-rows"); !v.empty()) {
        max_rows = static_cast<std::size_t>(std::stoull(v));
    }

    try {
        LoadedSide a;
        LoadedSide b;
        if (!file_a.empty() && !file_b.empty()) {
            const auto mat_store = clink_tools::materialisation_store_for(
                get_arg(argc, argv, "materialisation-store"));
            a = load_file(file_a, mat_store);
            b = load_file(file_b, mat_store);
        } else if (!dir.empty() && !from.empty() && !to.empty()) {
            const auto mat_store = clink_tools::materialisation_store_for(
                get_arg(argc, argv, "materialisation-store"));
            a = load_dir(dir, std::stoull(from), mat_store);
            b = load_dir(dir, std::stoull(to), mat_store);
        } else {
            diff_usage();
            return 2;
        }

        auto report = diff_entries(a.entries, b.entries, max_rows);
        // Version stamps: for the dir form, stamps ride each subtask file;
        // merge the deltas (same stamp on every subtask, so first-file
        // comparison suffices for the common case; compare pairwise by index
        // to stay honest when subtask counts match).
        const auto pairs = std::min(a.savepoints.size(), b.savepoints.size());
        for (std::size_t i = 0; i < pairs; ++i) {
            for (auto& v : diff_versions(a.savepoints[i], b.savepoints[i])) {
                const bool seen =
                    std::any_of(report.versions.begin(), report.versions.end(), [&](const auto& e) {
                        return e.op.value() == v.op.value() && e.state_type == v.state_type;
                    });
                if (!seen) {
                    report.versions.push_back(std::move(v));
                }
            }
        }

        if (json) {
            print_diff_json(a, b, report);
        } else {
            print_diff_text(a, b, report);
        }
        return report.identical() ? 0 : 1;
    } catch (const std::exception& e) {
        std::cerr << "clink state-diff: " << e.what() << "\n";
        return 2;
    }
}

int clink_cmd_state_cat(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        cat_usage();
        return 0;
    }
    if (!get_arg(argc, argv, "dataset").empty()) {
        return cat_dataset_cmd(argc, argv);
    }
    const auto file = get_arg(argc, argv, "file");
    const auto dir = get_arg(argc, argv, "dir");
    const auto id = get_arg(argc, argv, "id");
    const auto mat_store =
        clink_tools::materialisation_store_for(get_arg(argc, argv, "materialisation-store"));
    const bool json = has_flag(argc, argv, "json");
    std::size_t max_rows = 50;
    if (const auto v = get_arg(argc, argv, "max-rows"); !v.empty()) {
        max_rows = static_cast<std::size_t>(std::stoull(v));
    }

    try {
        LoadedSide side;
        if (!file.empty()) {
            side = load_file(file, mat_store);
        } else if (!dir.empty() && !id.empty()) {
            side = load_dir(dir, std::stoull(id), mat_store);
        } else {
            cat_usage();
            return 2;
        }

        if (json) {
            std::cout << "{\"source\":\"" << json_escape(side.label) << "\",\"operators\":[";
            bool first_op = true;
            for (const auto& [op, slots] : side.entries) {
                if (!first_op) {
                    std::cout << ",";
                }
                first_op = false;
                std::cout << "{\"op\":" << op.value() << ",\"slots\":[";
                bool first_slot = true;
                for (const auto& [slot, entries] : slots) {
                    if (!first_slot) {
                        std::cout << ",";
                    }
                    first_slot = false;
                    std::cout << "{\"slot\":\"" << json_escape(slot)
                              << "\",\"count\":" << entries.size() << ",\"entries\":[";
                    bool first_e = true;
                    std::size_t shown = 0;
                    for (const auto& [key, entry] : entries) {
                        if (max_rows != 0 && shown++ >= max_rows) {
                            break;
                        }
                        if (!first_e) {
                            std::cout << ",";
                        }
                        first_e = false;
                        std::cout << "{\"key_group\":" << static_cast<int>(entry.key_group)
                                  << ",\"key\":\"" << json_escape(render_bytes(key, 32))
                                  << "\",\"value\":\"" << json_escape(render_bytes(entry.value))
                                  << "\"}";
                    }
                    std::cout << "]}";
                }
                std::cout << "]}";
            }
            std::cout << "]}\n";
            return 0;
        }

        std::cout << "state-cat: " << side.label << "\n";
        if (side.entries.empty()) {
            std::cout << "(no keyed state)\n";
            return 0;
        }
        for (const auto& [op, slots] : side.entries) {
            for (const auto& [slot, entries] : slots) {
                std::cout << "\nop " << op.value() << " slot \"" << slot << "\" (" << entries.size()
                          << " entries)\n";
                std::size_t shown = 0;
                for (const auto& [key, entry] : entries) {
                    if (max_rows != 0 && shown >= max_rows) {
                        std::cout << "  ... " << (entries.size() - shown)
                                  << " more (raise --max-rows)\n";
                        break;
                    }
                    ++shown;
                    std::cout << "  kg=" << static_cast<int>(entry.key_group) << " key "
                              << render_bytes(key, 32) << " = " << render_bytes(entry.value)
                              << "\n";
                }
            }
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink state-cat: " << e.what() << "\n";
        return 2;
    }
}

// ---- clink capture-cat ------------------------------------------------------

namespace {

void capture_cat_usage() {
    std::cerr << "Usage: clink capture-cat --file=<epoch-N.cap> [--max-rows=N]\n"
              << "       clink capture-cat --dir=<capture-root>\n"
              << "\n"
              << "Inspects record-capture flight-recorder files (written when a job\n"
              << "runs with a capture dir; see clink run --capture-dir). --file dumps\n"
              << "one epoch's records (event time + value bytes rendered as\n"
              << "int64/text/hex; the framing is codec-agnostic). --dir lists every\n"
              << "captured epoch under a capture root with counts.\n"
              << "\n"
              << "Exit codes: 0 = ok, 2 = error.\n";
}

std::vector<std::byte> read_bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path.string());
    }
    std::vector<std::byte> bytes;
    std::istreambuf_iterator<char> it{in}, end;
    for (; it != end; ++it) {
        bytes.push_back(static_cast<std::byte>(*it));
    }
    return bytes;
}

// Codec-agnostic walk of the capture framing. v1 payload: [u32 count] then
// per record [u8 has_t][i64 t?][u32 len][len bytes]. v2 payload: [u32 count]
// then per EVENT a tag byte - 0 = record (v1 record shape), 1 = watermark
// [i64 ts][u8 idle], 2 = clock [i64 now]. Returns rendered rows.
struct WalkedRecord {
    enum class Kind { Data, Watermark, Clock };
    Kind kind{Kind::Data};
    bool has_event_time{false};
    std::int64_t event_time_ms{0};  // record event time / watermark ts / clock now
    bool idle{false};               // watermark only
    std::string value;              // raw bytes (data records only)
};

std::vector<WalkedRecord> walk_capture_payload(std::span<const std::byte> in,
                                               std::uint32_t version) {
    std::vector<WalkedRecord> out;
    std::size_t pos = 0;
    auto read_u32 = [&]() -> std::uint32_t {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
        }
        pos += 4;
        return v;
    };
    auto read_i64 = [&]() -> std::int64_t {
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
        }
        pos += 8;
        return static_cast<std::int64_t>(u);
    };
    if (in.size() < 4) {
        return out;
    }
    const auto count = read_u32();
    for (std::uint32_t r = 0; r < count && pos < in.size(); ++r) {
        WalkedRecord rec;
        if (version >= 2) {
            const auto tag = static_cast<std::uint8_t>(in[pos++]);
            if (tag == 1) {  // watermark
                if (pos + 9 > in.size()) {
                    break;
                }
                rec.kind = WalkedRecord::Kind::Watermark;
                rec.event_time_ms = read_i64();
                rec.idle = static_cast<std::uint8_t>(in[pos++]) != 0;
                out.push_back(std::move(rec));
                continue;
            }
            if (tag == 2) {  // clock
                if (pos + 8 > in.size()) {
                    break;
                }
                rec.kind = WalkedRecord::Kind::Clock;
                rec.event_time_ms = read_i64();
                out.push_back(std::move(rec));
                continue;
            }
            if (tag != 0) {
                break;  // unknown tag: stop rather than misparse
            }
            if (pos >= in.size()) {
                break;
            }
        }
        rec.has_event_time = static_cast<std::uint8_t>(in[pos++]) != 0;
        if (rec.has_event_time) {
            if (pos + 8 > in.size()) {
                break;
            }
            rec.event_time_ms = read_i64();
        }
        if (pos + 4 > in.size()) {
            break;
        }
        const auto len = read_u32();
        if (pos + len > in.size()) {
            break;
        }
        rec.value.assign(reinterpret_cast<const char*>(in.data() + pos), len);
        pos += len;
        out.push_back(std::move(rec));
    }
    return out;
}

}  // namespace

int clink_cmd_capture_cat(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        capture_cat_usage();
        return 0;
    }
    const auto file = get_arg(argc, argv, "file");
    const auto dir = get_arg(argc, argv, "dir");
    std::size_t max_rows = 50;
    if (const auto v = get_arg(argc, argv, "max-rows"); !v.empty()) {
        max_rows = static_cast<std::size_t>(std::stoull(v));
    }
    try {
        if (!file.empty()) {
            const auto bytes = read_bytes(file);
            auto hdr = clink::capture::decode_capture_header(
                std::span<const std::byte>{bytes.data(), bytes.size()});
            if (!hdr.has_value()) {
                std::cerr << "clink capture-cat: not a capture file (bad magic): " << file << "\n";
                return 2;
            }
            const auto& [h, payload_off] = *hdr;
            auto records = walk_capture_payload(
                std::span<const std::byte>{bytes.data() + payload_off, bytes.size() - payload_off},
                h.version);
            std::size_t data_n = 0;
            std::size_t wm_n = 0;
            std::size_t clock_n = 0;
            for (const auto& r : records) {
                switch (r.kind) {
                    case WalkedRecord::Kind::Data:
                        ++data_n;
                        break;
                    case WalkedRecord::Kind::Watermark:
                        ++wm_n;
                        break;
                    case WalkedRecord::Kind::Clock:
                        ++clock_n;
                        break;
                }
            }
            std::cout << "capture-cat: " << file << " (format v" << h.version << ")\n"
                      << "records stored: " << data_n << ", seen in epoch: " << h.records_seen
                      << ", watermarks: " << wm_n << ", clock advances: " << clock_n
                      << (h.truncated ? " (TRUNCATED at the cap)" : "") << "\n";
            std::size_t shown = 0;
            for (const auto& r : records) {
                if (max_rows != 0 && shown >= max_rows) {
                    std::cout << "  ... " << (records.size() - shown)
                              << " more (raise --max-rows)\n";
                    break;
                }
                ++shown;
                if (r.kind == WalkedRecord::Kind::Watermark) {
                    std::cout << "  [watermark t=" << r.event_time_ms << (r.idle ? " idle" : "")
                              << "]\n";
                    continue;
                }
                if (r.kind == WalkedRecord::Kind::Clock) {
                    std::cout << "  [clock now=" << r.event_time_ms << "]\n";
                    continue;
                }
                std::cout << "  ";
                if (r.has_event_time) {
                    std::cout << "t=" << r.event_time_ms << " ";
                }
                std::cout << render_bytes(r.value, 96) << "\n";
            }
            return 0;
        }
        if (!dir.empty()) {
            std::cout << "capture-cat: " << dir << "\n";
            bool any = false;
            std::vector<fs::path> files;
            for (const auto& entry : fs::recursive_directory_iterator(dir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".cap") {
                    files.push_back(entry.path());
                }
            }
            std::sort(files.begin(), files.end());
            for (const auto& f : files) {
                any = true;
                const auto bytes = read_bytes(f);
                auto hdr = clink::capture::decode_capture_header(
                    std::span<const std::byte>{bytes.data(), bytes.size()});
                if (!hdr.has_value()) {
                    std::cout << "  " << f.string() << "  (not a capture file)\n";
                    continue;
                }
                std::cout << "  " << fs::relative(f, dir).string()
                          << "  seen=" << hdr->first.records_seen
                          << (hdr->first.truncated ? " truncated" : "") << "\n";
            }
            if (!any) {
                std::cout << "(no .cap files)\n";
            }
            return 0;
        }
        capture_cat_usage();
        return 2;
    } catch (const std::exception& e) {
        std::cerr << "clink capture-cat: " << e.what() << "\n";
        return 2;
    }
}

namespace {

void export_usage() {
    std::cout << "usage: clink state-export --from=<path> [--out=<file>] [--format=...]\n"
              << "\n"
              << "Write a checkpoint/savepoint's keyed state as an open dataset.\n"
              << "\n"
              << "  --from=<path>     a .snap/.arrows snapshot file, or a RocksDB checkpoint\n"
              << "                    directory (rendered via the Arrow export; requires a\n"
              << "                    RocksDB-linked build)\n"
              << "  --dir=<root> --id=N\n"
              << "                    instead of --from: merge every subtask's\n"
              << "                    <root>/<subtask>/checkpoint-N.snap (and the flat\n"
              << "                    <root>/checkpoint-N.snap) into ONE export. It reads the\n"
              << "                    files it finds, without the checkpoint's marker or\n"
              << "                    participants, and operator state of several subtasks\n"
              << "                    collapses: not checkpoint-consistent; use\n"
              << "                    --checkpoint-dir (below) for that\n"
              << "  --job=<id> [--coordinator=host:port]\n"
              << "                    instead of --from: fetch a RUNNING job's whole keyed\n"
              << "                    state from the coordinator's live-export route (per-subtask\n"
              << "                    atomic view, not a checkpoint-consistent cut;\n"
              << "                    default coordinator 127.0.0.1:8081)\n"
              << "  --out=<file>      output file (arrow / parquet formats)\n"
              << "  --format=arrow    the canonical Arrow IPC stream (op_id, key_bytes,\n"
              << "                    value_bytes) - exact fidelity, restorable, readable by\n"
              << "                    every clink state tool and any Arrow consumer\n"
              << "  --format=parquet  the DECODED entry table (op_id, key_group, slot,\n"
              << "                    user_key, value_bytes) as one Parquet file - the\n"
              << "                    analytics projection, directly queryable in DuckDB /\n"
              << "                    Spark / pyarrow\n"
              << "  --format=iceberg  commit the decoded entry table as ONE snapshot of an\n"
              << "                    Apache Iceberg table (created when missing; repeated\n"
              << "                    exports accumulate as snapshots). Takes --warehouse and\n"
              << "                    --table (plus optional --namespace, --catalog-uri)\n"
              << "                    instead of --out. Requires an Iceberg-linked build.\n"
              << "\n"
              << "Default format: parquet when --out ends in .parquet, else arrow.\n";
}

}  // namespace

int clink_cmd_state_export(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        export_usage();
        export_dataset_usage();
        return 0;
    }
    if (!get_arg(argc, argv, "checkpoint-dir").empty()) {
        return export_dataset_cmd(argc, argv);
    }
    const auto from = get_arg(argc, argv, "from");
    const auto dir = get_arg(argc, argv, "dir");
    const auto id_str = get_arg(argc, argv, "id");
    const auto job = get_arg(argc, argv, "job");
    const auto coordinator = get_arg(argc, argv, "coordinator");
    const auto mat_store_path = get_arg(argc, argv, "materialisation-store");
    const auto out = get_arg(argc, argv, "out");
    std::string format = get_arg(argc, argv, "format");
    if (format.empty()) {
        format = out.ends_with(".parquet") ? "parquet" : "arrow";
    }
    if (format != "arrow" && format != "parquet" && format != "iceberg") {
        std::cerr << "state-export: unknown --format '" << format << "' (arrow|parquet|iceberg)\n";
        return 2;
    }
    // Input: exactly one of --from, --dir+--id, or --job. File formats
    // need --out; the iceberg format targets a catalogued table instead.
    const bool dir_form = !dir.empty() || !id_str.empty();
    const bool dir_form_complete = !dir.empty() && !id_str.empty();
    const int input_forms = (!from.empty() ? 1 : 0) + (dir_form ? 1 : 0) + (!job.empty() ? 1 : 0);
    if (input_forms != 1 ||                      // exactly one input form
        (dir_form && !dir_form_complete) ||      // --dir without --id or vice versa
        (format != "iceberg" && out.empty())) {  // file formats need --out
        export_usage();
        return 2;
    }
    try {
        auto resolved = clink_tools::resolve_state_input(
            from,
            dir,
            id_str,
            job,
            coordinator,
            clink_tools::materialisation_store_for(mat_store_path));
        auto& bytes = resolved.bytes;
        const auto& source = resolved.label;

        // Validate + summarise by loading what we are about to write: a
        // malformed input fails HERE, not in the consumer's reader.
        clink::Snapshot snap;
        snap.bytes = bytes;
        auto sp = Savepoint::load_from_snapshot(std::move(snap));
        const auto entries = collect_entries(sp);
        std::size_t slots = 0, rows = 0;
        for (const auto& [op, slot_map] : entries) {
            slots += slot_map.size();
            for (const auto& [slot, kv] : slot_map) {
                rows += kv.size();
            }
        }

        if (format == "iceberg") {
#ifdef CLINK_LINKED_ICEBERG
            clink::iceberg::IcebergStateExportOptions io;
            io.warehouse = get_arg(argc, argv, "warehouse");
            io.table = get_arg(argc, argv, "table");
            io.catalog_uri = get_arg(argc, argv, "catalog-uri");
            if (const auto ns = get_arg(argc, argv, "namespace"); !ns.empty()) {
                io.namespace_levels.clear();
                std::size_t start = 0;
                while (start <= ns.size()) {
                    const auto dot = ns.find('.', start);
                    if (dot == std::string::npos) {
                        io.namespace_levels.push_back(ns.substr(start));
                        break;
                    }
                    io.namespace_levels.push_back(ns.substr(start, dot - start));
                    start = dot + 1;
                }
            }
            if (io.warehouse.empty() || io.table.empty()) {
                std::cerr << "state-export: --format=iceberg needs --warehouse and --table\n";
                return 2;
            }
            const auto res = clink::iceberg::export_state_iceberg(entries, std::move(io));
            std::cout << "state-export: " << source << " -> iceberg table at " << res.table_location
                      << " (1 snapshot, " << res.rows << " rows, " << entries.size()
                      << " operators, " << slots << " slots)\n";
            return 0;
#else
            std::cerr << "state-export: this clink build has no Iceberg support; rebuild "
                         "with the Iceberg impl linked\n";
            return 2;
#endif
        }
        std::uintmax_t written = 0;
        if (format == "parquet") {
            // The analytics projection: the decoded entry table, one row
            // per (op, slot, user_key), straight into a Parquet file.
            clink::state_processor::write_state_parquet(
                entries, sp.backend().restored_state_versions(), out);
            written = fs::file_size(out);
        } else {
            std::ofstream sink(out, std::ios::binary | std::ios::trunc);
            if (!sink) {
                throw std::runtime_error("cannot open " + out + " for writing");
            }
            sink.write(reinterpret_cast<const char*>(bytes.data()),
                       static_cast<std::streamsize>(bytes.size()));
            sink.flush();
            if (!sink) {
                throw std::runtime_error("write to " + out + " failed");
            }
            written = bytes.size();
        }
        std::cout << "state-export: " << source << " -> " << out << " (" << format << ", "
                  << written << " bytes, " << entries.size() << " operators, " << slots
                  << " slots, " << rows << " keyed entries)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "state-export: " << e.what() << "\n";
        return 2;
    }
}
