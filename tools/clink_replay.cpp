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
// Fidelity:
//   * Format v2 captures (current) hold the FULL ordered event stream -
//     data records, watermarks, and the clock positions at which due
//     processing-time timers fired. Replay feeds all three through the
//     production paths (watermarks via process(), clock positions via the
//     manual TimerService clock + fire_due_timers), so watermark-driven
//     window fires and timer fires reproduce at their true positions.
//   * Format v1 captures (records only) replay data-only, as before:
//     per-record operators replay exactly; window fires do not occur.
//   * Row-channel operators (the SQL frontend's ops) - the factories are
//     registered in-process via clink::sql::install.
//   * Truncated epochs replay the STORED prefix; the header's true count
//     is printed so a sampled epoch is never mistaken for a complete one.
//   * --flush additionally invokes the operator's end-of-stream flush
//     after the events.
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
#include <variant>
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
        << "                    [--verify]\n"
        << "\n"
        << "Rebuilds one operator from its op.json capture sidecar, restores its\n"
        << "keyed state from checkpoint N-1, feeds it epoch-N.cap's captured\n"
        << "event stream (records, watermarks, timer-fire clock positions), and\n"
        << "prints every emission - offline, no cluster. --epoch=1 starts from\n"
        << "fresh state; --epoch=final requires --state-from. v2 captures replay\n"
        << "watermark-driven window fires and processing-time timer fires at\n"
        << "their production positions; v1 captures replay data records only.\n"
        << "\n"
        << "--verify replays the epoch TWICE from the same snapshot and event\n"
        << "stream and byte-compares the emissions: the determinism check\n"
        << "(docs/internals/replay-determinism.md). Prints the first divergence\n"
        << "when they differ.\n"
        << "\n"
        << "Exit codes: 0 = ok, 1 = --verify found a divergence, 2 = error.\n";
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

// Render one emitted Row like the print sink does: changelog kinds
// prefixed, the marker stripped.
std::string format_row(const Row& row) {
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
    return prefix + clink::config::serialize_output(
                        clink::config::JsonValue{clink::config::JsonObject{std::move(vals)}});
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
    const bool do_verify = has_flag(argc, argv, "verify");
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
        // Row-channel operators (the SQL frontend's set).
        if (spec.in_channel != "row" || spec.out_channel != "row") {
            throw std::runtime_error("replay supports row->row operators; this op is " +
                                     spec.in_channel + "->" + spec.out_channel);
        }
        ensure_row_factories_installed();
        auto parsed = clink::capture::read_capture_events(
            std::span<const std::byte>{cap_bytes.data(), cap_bytes.size()},
            clink::sql::row_json_codec());
        if (!parsed.has_value()) {
            throw std::runtime_error("not a capture file: " + cap_path.string());
        }
        auto& events = parsed->second;
        std::size_t data_count = 0;
        std::size_t wm_count = 0;
        std::size_t clock_count = 0;
        for (const auto& e : events) {
            if (std::holds_alternative<clink::Record<Row>>(e)) {
                ++data_count;
            } else if (std::holds_alternative<clink::capture::WatermarkEvent>(e)) {
                ++wm_count;
            } else {
                ++clock_count;
            }
        }

        // State: checkpoint N-1 (or --state-from), fresh for epoch 1. The
        // snapshot BYTES are held so every run restores a fresh backend -
        // what makes run-twice verification meaningful.
        std::optional<std::vector<std::byte>> snap_bytes;
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
            snap_bytes = read_bytes_file(snap_path);
            state_desc = "state from " + snap_path.string();
        }

        // The operator factory from the sidecar spec via the registry.
        const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_operator(
            spec.op_type, spec.in_channel, spec.out_channel);
        if (factory == nullptr) {
            throw std::runtime_error("no registered factory for op type '" + spec.op_type + "' (" +
                                     spec.in_channel + "->" + spec.out_channel + ")");
        }

        std::cout << "replay: op " << op_str << " (" << spec.op_type << ") subtask " << subtask
                  << "\n  input: " << cap_path.string() << " (format v" << hdr->first.version
                  << ": " << data_count << " records stored, " << hdr->first.records_seen
                  << " seen, " << wm_count << " watermarks, " << clock_count << " clock advances"
                  << (hdr->first.truncated ? ", TRUNCATED - replaying the stored prefix" : "")
                  << ")\n  " << state_desc << "\n";
        if (hdr->first.version < 2) {
            std::cerr << "note: v1 capture (records only) - watermark-driven fires do not "
                         "occur in this replay\n";
        }

        // One complete replay run: fresh backend (snapshot restored), fresh
        // operator, manual clock, then the event stream through the
        // production paths. Returns the rendered emissions in order.
        auto run_once = [&]() -> std::vector<std::string> {
            auto backend = std::make_shared<clink::InMemoryStateBackend>();
            if (snap_bytes.has_value()) {
                clink::Snapshot snap{.checkpoint_id = clink::CheckpointId{state_id},
                                     .bytes = *snap_bytes};
                backend->restore(snap);
            }
            clink::cluster::OperatorBuildContext bctx;
            bctx.params = spec.params;
            auto op = std::static_pointer_cast<clink::Operator<Row, Row>>(factory->build(bctx));
            if (!spec.uid.empty()) {
                op->set_uid(spec.uid);
            }
            clink::MetricsRegistry metrics;
            clink::RuntimeContext ctx{
                clink::OperatorId{op_id}, spec.op_type, backend.get(), &metrics};
            auto replay_now = std::make_shared<std::int64_t>(0);
            ctx.timer_service()->set_now_fn([replay_now] { return *replay_now; });
            op->attach_runtime(&ctx);
            op->restore_timers(*backend, clink::OperatorId{op_id});
            op->open();

            std::vector<std::string> rows;
            clink::Emitter<Row> emitter([&rows](clink::StreamElement<Row> el) {
                if (el.is_data()) {
                    for (const auto& rec : el.as_data()) {
                        rows.push_back(format_row(rec.value()));
                    }
                }
                return true;
            });
            for (const auto& e : events) {
                if (const auto* rec = std::get_if<clink::Record<Row>>(&e)) {
                    clink::Batch<Row> b;
                    b.push(*rec);
                    op->process(clink::StreamElement<Row>::data(std::move(b)), emitter);
                } else if (const auto* wm = std::get_if<clink::capture::WatermarkEvent>(&e)) {
                    // The runner delivers watermarks through process(); the
                    // operator's own dispatch runs its production watermark
                    // path.
                    const auto mark =
                        wm->idle ? clink::Watermark{clink::EventTime{wm->ts_ms}, /*idle=*/true}
                                 : clink::Watermark{clink::EventTime{wm->ts_ms}};
                    op->process(clink::StreamElement<Row>::watermark(mark), emitter);
                } else {
                    // A captured timer-fire position: move the manual clock
                    // there and fire due processing-time timers - the runner's
                    // between-pops poll reproduced at the captured position.
                    *replay_now = std::get<clink::capture::ClockEvent>(e).now_ms;
                    op->fire_due_timers(emitter, *replay_now);
                }
            }
            if (do_flush) {
                op->flush(emitter);
            }
            op->close();
            return rows;
        };

        if (do_verify) {
            // Determinism check: two complete runs from the same snapshot
            // and event stream must be byte-identical.
            const auto a = run_once();
            const auto b = run_once();
            if (a == b) {
                std::cout << "deterministic: " << a.size()
                          << " emissions identical across 2 runs\n";
                return 0;
            }
            std::cout << "NON-DETERMINISTIC: run 1 emitted " << a.size() << ", run 2 emitted "
                      << b.size() << "\n";
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

        std::cout << "---\n";
        const auto rows = run_once();
        std::size_t shown = 0;
        for (const auto& r : rows) {
            if (max_rows != 0 && shown >= max_rows) {
                break;
            }
            ++shown;
            std::cout << r << "\n";
        }
        std::cout << "---\nreplayed " << data_count << " records, " << wm_count << " watermarks, "
                  << clock_count << " clock advances -> " << rows.size() << " emissions";
        if (shown < rows.size()) {
            std::cout << " (" << shown << " shown; raise --max-rows)";
        }
        std::cout << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink replay: " << e.what() << "\n";
        return 2;
    }
}
