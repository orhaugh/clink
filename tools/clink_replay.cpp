// clink replay - deterministic single-operator replay (time travel inc 3).
//
// Rebuilds ONE operator offline from the op.json sidecar the flight
// recorder wrote, restores its keyed state from checkpoint N-1, feeds it
// epoch-N.cap's captured records through the same process() path the
// runner drives, and prints everything it emits - the record-level
// "explain why this output happened" loop, no cluster required.
//
//   clink replay --capture-dir=<dir> --checkpoint-dir=<dir> \
//                --op=<operator id> --epoch=N [--subtask=S] [--max-rows=N] [--flush]
//
// State source: <checkpoint-dir>/<S>/checkpoint-<N-1>.snap (the flat
// <checkpoint-dir>/checkpoint-<N-1>.snap layout is also probed). --epoch=1
// replays from fresh state. --epoch=final replays final.cap and requires
// --state-from=<checkpoint id> naming the state to start from.
//
// v1 scope and honesty:
//   * Row-channel operators (the SQL frontend's ops) - the factories are
//     registered in-process via clink::sql::install.
//   * Data records only: the flight recorder does not capture watermarks
//     or barriers, so purely watermark-driven emissions (window fires) do
//     not occur during replay; per-record operators (GROUP BY, filter,
//     project, DISTINCT, TOP-N) replay exactly. --flush additionally
//     invokes the operator's end-of-stream flush after the records.
//   * Truncated epochs replay the STORED prefix; the header's true count
//     is printed so a sampled epoch is never mistaken for a complete one.
//
// Exit codes: 0 = ok, 2 = error.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/state_processor/savepoint.hpp"

namespace {

namespace fs = std::filesystem;
using clink::sql::Row;

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
        << "Usage: clink replay --capture-dir=<dir> --checkpoint-dir=<dir>\n"
        << "                    --op=<operator id> --epoch=<N|final>\n"
        << "                    [--subtask=S] [--state-from=<ckpt id>] [--max-rows=N] [--flush]\n"
        << "\n"
        << "Rebuilds one operator from its op.json capture sidecar, restores its\n"
        << "keyed state from checkpoint N-1, feeds it epoch-N.cap's records, and\n"
        << "prints every emission - offline, no cluster. --epoch=1 starts from\n"
        << "fresh state; --epoch=final requires --state-from. Data records only:\n"
        << "watermark-driven (window) fires do not occur; per-record operators\n"
        << "(GROUP BY, filter, project, DISTINCT, TOP-N) replay exactly.\n"
        << "\n"
        << "Exit codes: 0 = ok, 2 = error.\n";
}

// Minimal reader for the op.json sidecar (written by write_op_spec).
clink::capture::OpSpecSidecar read_op_spec(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path.string() +
                                 " (was the job run with --capture-dir?)");
    }
    std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    auto js = clink::config::parse(text);
    if (!js.is_object()) {
        throw std::runtime_error("malformed op.json at " + path.string());
    }
    clink::capture::OpSpecSidecar spec;
    auto str = [&](const char* key) {
        return js.as_object().count(key) != 0U && js.at(key).is_string() ? js.at(key).as_string()
                                                                         : std::string{};
    };
    spec.op_type = str("op_type");
    spec.in_channel = str("in_channel");
    spec.out_channel = str("out_channel");
    spec.uid = str("uid");
    if (js.as_object().count("params") != 0U && js.at("params").is_object()) {
        for (const auto& [k, v] : js.at("params").as_object()) {
            if (v.is_string()) {
                spec.params[k] = v.as_string();
            }
        }
    }
    return spec;
}

std::vector<std::byte> read_bytes_file(const fs::path& path) {
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

// Find the state snapshot for checkpoint `id`: per-subtask layout first
// (<dir>/<subtask>/checkpoint-<id>.snap), then flat.
fs::path find_snapshot(const fs::path& ckpt_dir, std::uint64_t subtask, std::uint64_t id) {
    const std::string name = "checkpoint-" + std::to_string(id) + ".snap";
    const auto per_subtask = ckpt_dir / std::to_string(subtask) / name;
    if (fs::exists(per_subtask)) {
        return per_subtask;
    }
    const auto flat = ckpt_dir / name;
    if (fs::exists(flat)) {
        return flat;
    }
    throw std::runtime_error("no " + name + " under " + ckpt_dir.string() + " (subtask " +
                             std::to_string(subtask) + " or flat layout)");
}

void ensure_row_factories_installed() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
    });
}

// Print one emitted Row like the print sink does: changelog kinds
// prefixed, the marker stripped.
void print_row(const Row& row) {
    std::string prefix;
    auto vals = row.values;
    if (auto it = vals.find(std::string{clink::sql::kRowKindField}); it != vals.end()) {
        const std::string kind = it->second.is_string() ? it->second.as_string() : std::string{};
        if (kind == clink::sql::kRowKindDelete) {
            prefix = "-D ";
        } else if (kind == clink::sql::kRowKindUpdateBefore) {
            prefix = "-U ";
        } else if (kind == clink::sql::kRowKindUpdateAfter) {
            prefix = "+U ";
        }
        vals.erase(it);
    }
    std::cout << prefix
              << clink::config::serialize_output(
                     clink::config::JsonValue{clink::config::JsonObject{std::move(vals)}})
              << "\n";
}

}  // namespace

int clink_cmd_replay(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        usage();
        return 0;
    }
    const auto capture_dir = get_arg(argc, argv, "capture-dir");
    const auto ckpt_dir = get_arg(argc, argv, "checkpoint-dir");
    const auto op_str = get_arg(argc, argv, "op");
    const auto epoch_str = get_arg(argc, argv, "epoch");
    const auto subtask_str = get_arg(argc, argv, "subtask", "");
    const auto state_from_str = get_arg(argc, argv, "state-from");
    const bool do_flush = has_flag(argc, argv, "flush");
    std::size_t max_rows = 100;
    if (const auto v = get_arg(argc, argv, "max-rows"); !v.empty()) {
        max_rows = static_cast<std::size_t>(std::stoull(v));
    }
    if (capture_dir.empty() || op_str.empty() || epoch_str.empty()) {
        usage();
        return 2;
    }

    try {
        const std::uint64_t op_id = std::stoull(op_str);

        // Resolve the subtask dir: explicit --subtask, or the single
        // subtask-<n> directory under op-<id> when unambiguous.
        const auto op_dir = fs::path(capture_dir) / ("op-" + op_str);
        std::uint64_t subtask = 0;
        if (!subtask_str.empty()) {
            subtask = std::stoull(subtask_str);
        } else {
            std::vector<std::string> found;
            if (fs::is_directory(op_dir)) {
                for (const auto& e : fs::directory_iterator(op_dir)) {
                    const auto name = e.path().filename().string();
                    if (e.is_directory() && name.starts_with("subtask-")) {
                        found.push_back(name.substr(8));
                    }
                }
            }
            if (found.size() == 1) {
                subtask = std::stoull(found[0]);
            } else if (found.empty()) {
                throw std::runtime_error("no subtask captures under " + op_dir.string());
            } else {
                throw std::runtime_error("multiple subtasks captured under " + op_dir.string() +
                                         "; pass --subtask=<n>");
            }
        }
        const auto subdir = op_dir / ("subtask-" + std::to_string(subtask));

        // The op spec + the epoch's records.
        const auto spec = read_op_spec(subdir / "op.json");
        const bool is_final = epoch_str == "final";
        const auto cap_path = subdir / (is_final ? "final.cap" : ("epoch-" + epoch_str + ".cap"));
        const auto cap_bytes = read_bytes_file(cap_path);
        auto hdr = clink::capture::decode_capture_header(
            std::span<const std::byte>{cap_bytes.data(), cap_bytes.size()});
        if (!hdr.has_value()) {
            throw std::runtime_error("not a capture file: " + cap_path.string());
        }
        // v1: Row-channel operators (the SQL frontend's set).
        if (spec.in_channel != "row" || spec.out_channel != "row") {
            throw std::runtime_error("replay v1 supports row->row operators; this op is " +
                                     spec.in_channel + "->" + spec.out_channel);
        }
        ensure_row_factories_installed();
        auto records = clink::capture::deserialize_records(
            std::span<const std::byte>{cap_bytes.data() + hdr->second,
                                       cap_bytes.size() - hdr->second},
            clink::sql::row_json_codec());

        // State: checkpoint N-1 (or --state-from), fresh for epoch 1.
        auto backend = std::make_shared<clink::InMemoryStateBackend>();
        std::string state_desc = "fresh state";
        std::uint64_t state_id = 0;
        if (!state_from_str.empty()) {
            state_id = std::stoull(state_from_str);
        } else if (!is_final) {
            const auto epoch = std::stoull(epoch_str);
            state_id = epoch > 0 ? epoch - 1 : 0;
        } else {
            throw std::runtime_error("--epoch=final requires --state-from=<checkpoint id>");
        }
        if (state_id > 0) {
            if (ckpt_dir.empty()) {
                throw std::runtime_error("state from checkpoint " + std::to_string(state_id) +
                                         " requires --checkpoint-dir");
            }
            const auto snap_path = find_snapshot(ckpt_dir, subtask, state_id);
            const auto snap_bytes = read_bytes_file(snap_path);
            clink::Snapshot snap{.checkpoint_id = clink::CheckpointId{state_id},
                                 .bytes = snap_bytes};
            backend->restore(snap);
            state_desc = "state from " + snap_path.string();
        }

        // Rebuild the operator from its sidecar spec via the registry.
        const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_operator(
            spec.op_type, spec.in_channel, spec.out_channel);
        if (factory == nullptr) {
            throw std::runtime_error("no registered factory for op type '" + spec.op_type + "' (" +
                                     spec.in_channel + "->" + spec.out_channel + ")");
        }
        clink::cluster::OperatorBuildContext bctx;
        bctx.params = spec.params;
        auto op = std::static_pointer_cast<clink::Operator<Row, Row>>(factory->build(bctx));
        if (!spec.uid.empty()) {
            op->set_uid(spec.uid);
        }

        std::cout << "replay: op " << op_str << " (" << spec.op_type << ") subtask " << subtask
                  << "\n  input: " << cap_path.string() << " (" << records.size()
                  << " records stored, " << hdr->first.records_seen << " seen"
                  << (hdr->first.truncated ? ", TRUNCATED - replaying the stored prefix" : "")
                  << ")\n  " << state_desc << "\n---\n";

        // Drive the operator exactly as the runner does for data: attach,
        // restore timers, open, per-record process, optional flush.
        clink::MetricsRegistry metrics;
        clink::RuntimeContext ctx{clink::OperatorId{op_id}, spec.op_type, backend.get(), &metrics};
        op->attach_runtime(&ctx);
        op->restore_timers(*backend, clink::OperatorId{op_id});
        op->open();

        clink::BoundedChannel<clink::StreamElement<Row>> out_ch(4096);
        clink::Emitter<Row> emitter(&out_ch);
        std::size_t emitted = 0;
        std::size_t shown = 0;
        auto drain = [&] {
            while (auto el = out_ch.try_pop()) {
                if (el->is_data()) {
                    for (const auto& rec : el->as_data()) {
                        ++emitted;
                        if (max_rows == 0 || shown < max_rows) {
                            ++shown;
                            print_row(rec.value());
                        }
                    }
                }
            }
        };
        for (auto& rec : records) {
            clink::Batch<Row> b;
            b.push(std::move(rec));
            op->process(clink::StreamElement<Row>::data(std::move(b)), emitter);
            drain();
        }
        if (do_flush) {
            op->flush(emitter);
            drain();
        }
        op->close();
        std::cout << "---\nreplayed " << records.size() << " records -> " << emitted
                  << " emissions";
        if (shown < emitted) {
            std::cout << " (" << shown << " shown; raise --max-rows)";
        }
        std::cout << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink replay: " << e.what() << "\n";
        return 2;
    }
}
