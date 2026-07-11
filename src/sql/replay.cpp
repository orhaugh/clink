// The deterministic epoch-replay engine (see include/clink/sql/replay.hpp).

#include "clink/sql/replay.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink::sql {

namespace {

namespace fs = std::filesystem;

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

// Minimal reader for the op.json sidecar (written by capture::write_op_spec).
capture::OpSpecSidecar read_op_spec(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path.string() +
                                 " (was the job run with --capture-dir?)");
    }
    std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    auto js = config::parse(text);
    if (!js.is_object()) {
        throw std::runtime_error("malformed op.json at " + path.string());
    }
    capture::OpSpecSidecar spec;
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
        cluster::ensure_built_ins_registered();
        plugin::PluginRegistry reg;
        install(reg);
    });
}

}  // namespace

std::string format_replay_row(const Row& row) {
    std::string prefix;
    auto vals = row.values;
    if (auto it = vals.find(std::string{kRowKindField}); it != vals.end()) {
        const std::string kind = it->second.is_string() ? it->second.as_string() : std::string{};
        if (kind == kRowKindDelete) {
            prefix = "-D ";
        } else if (kind == kRowKindUpdateBefore) {
            prefix = "-U ";
        } else if (kind == kRowKindUpdateAfter) {
            prefix = "+U ";
        }
        vals.erase(it);
    }
    return prefix +
           config::serialize_output(config::JsonValue{config::JsonObject{std::move(vals)}});
}

std::vector<CapturedOp> list_captured_ops(const std::string& capture_dir) {
    std::vector<CapturedOp> out;
    if (!fs::is_directory(capture_dir)) {
        return out;
    }
    for (const auto& op_entry : fs::directory_iterator(capture_dir)) {
        const auto op_name = op_entry.path().filename().string();
        if (!op_entry.is_directory() || !op_name.starts_with("op-")) {
            continue;
        }
        std::uint64_t op_id = 0;
        try {
            op_id = std::stoull(op_name.substr(3));
        } catch (const std::exception&) {
            continue;
        }
        for (const auto& sub_entry : fs::directory_iterator(op_entry.path())) {
            const auto sub_name = sub_entry.path().filename().string();
            if (!sub_entry.is_directory() || !sub_name.starts_with("subtask-")) {
                continue;
            }
            try {
                out.push_back(CapturedOp{op_id, std::stoull(sub_name.substr(8))});
            } catch (const std::exception&) {
            }
        }
    }
    std::sort(out.begin(), out.end(), [](const CapturedOp& a, const CapturedOp& b) {
        return a.op_id != b.op_id ? a.op_id < b.op_id : a.subtask < b.subtask;
    });
    return out;
}

EpochReplay EpochReplay::load(const ReplayRequest& request) {
    if (request.capture_dir.empty() || request.epoch.empty()) {
        throw std::runtime_error("replay needs capture_dir and epoch");
    }
    EpochReplay r;
    r.info_.op_id = request.op_id;
    r.flush_ = request.flush;

    // Resolve the subtask dir: explicit, or the single subtask-<n>
    // directory under op-<id> when unambiguous.
    const auto op_dir = fs::path(request.capture_dir) / ("op-" + std::to_string(request.op_id));
    if (request.subtask.has_value()) {
        r.info_.subtask = *request.subtask;
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
            r.info_.subtask = std::stoull(found[0]);
        } else if (found.empty()) {
            throw std::runtime_error("no subtask captures under " + op_dir.string());
        } else {
            throw std::runtime_error("multiple subtasks captured under " + op_dir.string() +
                                     "; name one explicitly");
        }
    }
    const auto subdir = op_dir / ("subtask-" + std::to_string(r.info_.subtask));

    // The op spec + the epoch's event stream.
    r.info_.spec = read_op_spec(subdir / "op.json");
    const bool is_final = request.epoch == "final";
    const auto cap_path = subdir / (is_final ? "final.cap" : ("epoch-" + request.epoch + ".cap"));
    r.info_.capture_path = cap_path.string();
    const auto cap_bytes = read_bytes_file(cap_path);
    auto parsed = capture::read_capture_events(
        std::span<const std::byte>{cap_bytes.data(), cap_bytes.size()}, row_json_codec());
    if (!parsed.has_value()) {
        throw std::runtime_error("not a capture file: " + cap_path.string());
    }
    r.info_.format_version = parsed->first.version;
    r.info_.records_seen = parsed->first.records_seen;
    r.info_.truncated = parsed->first.truncated;
    r.events_ = std::move(parsed->second);
    for (const auto& e : r.events_) {
        if (std::holds_alternative<Record<Row>>(e)) {
            ++r.info_.data_count;
        } else if (std::holds_alternative<capture::WatermarkEvent>(e)) {
            ++r.info_.watermark_count;
        } else {
            ++r.info_.clock_count;
        }
    }

    // Row-channel operators (the SQL frontend's set).
    if (r.info_.spec.in_channel != "row" || r.info_.spec.out_channel != "row") {
        throw std::runtime_error("replay supports row->row operators; this op is " +
                                 r.info_.spec.in_channel + "->" + r.info_.spec.out_channel);
    }
    ensure_row_factories_installed();

    // State: checkpoint N-1 (or state_from), fresh for epoch 1. The
    // snapshot BYTES are held so every run() restores a fresh backend.
    r.info_.state_desc = "fresh state";
    if (request.state_from.has_value()) {
        r.state_id_ = *request.state_from;
    } else if (!is_final) {
        const auto epoch = std::stoull(request.epoch);
        r.state_id_ = epoch > 0 ? epoch - 1 : 0;
    } else {
        throw std::runtime_error("epoch 'final' requires an explicit state_from checkpoint id");
    }
    if (r.state_id_ > 0) {
        if (request.checkpoint_dir.empty()) {
            throw std::runtime_error("state from checkpoint " + std::to_string(r.state_id_) +
                                     " requires checkpoint_dir");
        }
        const auto snap_path = find_snapshot(request.checkpoint_dir, r.info_.subtask, r.state_id_);
        r.snap_bytes_ = read_bytes_file(snap_path);
        r.info_.state_desc = "state from " + snap_path.string();
        r.info_.state_snapshot_path = snap_path.string();
    }

    // The operator factory from the sidecar spec via the registry.
    r.factory_ = cluster::OperatorRegistry::default_instance().find_operator(
        r.info_.spec.op_type, r.info_.spec.in_channel, r.info_.spec.out_channel);
    if (r.factory_ == nullptr) {
        throw std::runtime_error("no registered factory for op type '" + r.info_.spec.op_type +
                                 "' (" + r.info_.spec.in_channel + "->" + r.info_.spec.out_channel +
                                 ")");
    }
    return r;
}

std::vector<std::string> EpochReplay::run() const {
    // Fresh backend (snapshot restored), fresh operator, manual clock,
    // then the event stream through the production paths.
    auto backend = std::make_shared<InMemoryStateBackend>();
    if (snap_bytes_.has_value()) {
        Snapshot snap{.checkpoint_id = CheckpointId{state_id_}, .bytes = *snap_bytes_};
        backend->restore(snap);
    }
    cluster::OperatorBuildContext bctx;
    bctx.params = info_.spec.params;
    auto op = std::static_pointer_cast<Operator<Row, Row>>(factory_->build(bctx));
    if (!info_.spec.uid.empty()) {
        op->set_uid(info_.spec.uid);
    }
    MetricsRegistry metrics;
    RuntimeContext ctx{OperatorId{info_.op_id}, info_.spec.op_type, backend.get(), &metrics};
    auto replay_now = std::make_shared<std::int64_t>(0);
    ctx.timer_service()->set_now_fn([replay_now] { return *replay_now; });
    op->attach_runtime(&ctx);
    op->restore_timers(*backend, OperatorId{info_.op_id});
    op->open();

    std::vector<std::string> rows;
    Emitter<Row> emitter([&rows](StreamElement<Row> el) {
        if (el.is_data()) {
            for (const auto& rec : el.as_data()) {
                rows.push_back(format_replay_row(rec.value()));
            }
        }
        return true;
    });
    for (const auto& e : events_) {
        if (const auto* rec = std::get_if<Record<Row>>(&e)) {
            Batch<Row> b;
            b.push(*rec);
            op->process(StreamElement<Row>::data(std::move(b)), emitter);
        } else if (const auto* wm = std::get_if<capture::WatermarkEvent>(&e)) {
            // The runner delivers watermarks through process(); the
            // operator's own dispatch runs its production watermark path.
            const auto mark = wm->idle ? Watermark{EventTime{wm->ts_ms}, /*idle=*/true}
                                       : Watermark{EventTime{wm->ts_ms}};
            op->process(StreamElement<Row>::watermark(mark), emitter);
        } else {
            // A captured timer-fire position: move the manual clock there
            // and fire due processing-time timers - the runner's
            // between-pops poll reproduced at the captured position.
            *replay_now = std::get<capture::ClockEvent>(e).now_ms;
            op->fire_due_timers(emitter, *replay_now);
        }
    }
    if (flush_) {
        op->flush(emitter);
    }
    op->close();
    return rows;
}

// ---- Regression bundles -----------------------------------------------

namespace {

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
            case '\n':
                out += "\\n";
                break;
            default:
                out += c;
        }
    }
    return out;
}

void write_text_file(const fs::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("cannot write " + path.string());
    }
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
}

}  // namespace

std::size_t emit_replay_regression(const ReplayRequest& request, const std::string& out_dir) {
    // Load + run NOW: the golden is what the current build produces, and
    // load() validates everything before any file lands.
    const auto replay = EpochReplay::load(request);
    const auto& info = replay.info();
    const auto golden = replay.run();

    const fs::path bundle{out_dir};
    fs::create_directories(bundle);

    // The op's capture: sidecar + the one epoch file, in the layout
    // EpochReplay::load expects.
    const auto cap_subdir = bundle / "capture" / ("op-" + std::to_string(info.op_id)) /
                            ("subtask-" + std::to_string(info.subtask));
    fs::create_directories(cap_subdir);
    const auto src_subdir = fs::path(request.capture_dir) / ("op-" + std::to_string(info.op_id)) /
                            ("subtask-" + std::to_string(info.subtask));
    fs::copy_file(
        src_subdir / "op.json", cap_subdir / "op.json", fs::copy_options::overwrite_existing);
    const auto epoch_file = fs::path(info.capture_path).filename();
    fs::copy_file(info.capture_path, cap_subdir / epoch_file, fs::copy_options::overwrite_existing);

    // The starting state (when not fresh), under the per-subtask layout.
    const bool has_state = !info.state_snapshot_path.empty();
    if (has_state) {
        const auto state_dir = bundle / "state" / std::to_string(info.subtask);
        fs::create_directories(state_dir);
        fs::copy_file(info.state_snapshot_path,
                      state_dir / fs::path(info.state_snapshot_path).filename(),
                      fs::copy_options::overwrite_existing);
    }

    // The golden emissions.
    {
        std::string text;
        for (const auto& row : golden) {
            text += row;
            text += "\n";
        }
        write_text_file(bundle / "golden.ndjson", text);
    }

    // The manifest run_replay_regression reads.
    {
        std::string manifest = "{\"op_id\":\"" + std::to_string(info.op_id) + "\",\"subtask\":\"" +
                               std::to_string(info.subtask) + "\",\"epoch\":\"" +
                               json_escape(request.epoch) + "\"";
        if (request.state_from.has_value()) {
            manifest += ",\"state_from\":\"" + std::to_string(*request.state_from) + "\"";
        }
        manifest += std::string{",\"flush\":\""} + (request.flush ? "1" : "0") + "\"";
        manifest += ",\"op_type\":\"" + json_escape(info.spec.op_type) + "\"}";
        write_text_file(bundle / "bundle.json", manifest);
    }

    // The generated test: one call into the library, so it can never
    // drift from the replay implementation.
    {
        std::string test_src =
            "// Generated by `clink replay --emit-test` from a captured production epoch:\n"
            "// op " +
            std::to_string(info.op_id) + " (" + info.spec.op_type + "), epoch " + request.epoch +
            ".\n"
            "// Replays the bundled capture from the bundled state and asserts the\n"
            "// emissions equal golden.ndjson, byte for byte. Add this file to a test\n"
            "// target that links clink::sql.\n"
            "\n"
            "#include <filesystem>\n"
            "\n"
            "#include <gtest/gtest.h>\n"
            "\n"
            "#include \"clink/sql/replay.hpp\"\n"
            "\n"
            "TEST(ReplayRegression, " +
            info.spec.op_type + "_epoch_" + (request.epoch == "final" ? "final" : request.epoch) +
            ") {\n"
            "    const auto bundle = std::filesystem::path(__FILE__).parent_path().string();\n"
            "    const auto error = clink::sql::run_replay_regression(bundle);\n"
            "    EXPECT_TRUE(error.empty()) << error;\n"
            "}\n";
        write_text_file(bundle / "replay_regression_test.cpp", test_src);
    }
    return golden.size();
}

std::string run_replay_regression(const std::string& bundle_dir) {
    try {
        const fs::path bundle{bundle_dir};
        const auto manifest_text = [&] {
            std::ifstream in(bundle / "bundle.json", std::ios::binary);
            if (!in) {
                throw std::runtime_error("cannot open " + (bundle / "bundle.json").string());
            }
            return std::string{std::istreambuf_iterator<char>{in},
                               std::istreambuf_iterator<char>{}};
        }();
        auto js = config::parse(manifest_text);
        if (!js.is_object()) {
            throw std::runtime_error("malformed bundle.json");
        }
        auto field = [&](const char* key) {
            return js.as_object().count(key) != 0U && js.at(key).is_string()
                       ? js.at(key).as_string()
                       : std::string{};
        };
        ReplayRequest req;
        req.capture_dir = (bundle / "capture").string();
        req.checkpoint_dir = (bundle / "state").string();
        req.op_id = std::stoull(field("op_id"));
        req.subtask = std::stoull(field("subtask"));
        req.epoch = field("epoch");
        if (const auto sf = field("state_from"); !sf.empty()) {
            req.state_from = std::stoull(sf);
        }
        req.flush = field("flush") == "1";

        const auto rows = EpochReplay::load(req).run();

        std::vector<std::string> golden;
        {
            std::ifstream in(bundle / "golden.ndjson", std::ios::binary);
            if (!in) {
                throw std::runtime_error("cannot open " + (bundle / "golden.ndjson").string());
            }
            std::string line;
            while (std::getline(in, line)) {
                golden.push_back(line);
            }
        }
        if (rows == golden) {
            return {};
        }
        std::string err = "replay regression: emitted " + std::to_string(rows.size()) +
                          " rows, golden has " + std::to_string(golden.size());
        const auto n = std::min(rows.size(), golden.size());
        for (std::size_t i = 0; i < n; ++i) {
            if (rows[i] != golden[i]) {
                err += "; first divergence at emission " + std::to_string(i) + ": replayed '" +
                       rows[i] + "' vs golden '" + golden[i] + "'";
                break;
            }
        }
        return err;
    } catch (const std::exception& e) {
        return std::string{"replay regression: "} + e.what();
    }
}

}  // namespace clink::sql
