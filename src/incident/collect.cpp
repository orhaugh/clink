// `clink incident-collect`: gather one incident's evidence into a bundle.
//
// The collector selects the checkpoint with the engine's own restore rule
// (restore_point_on_disk), copies what that checkpoint and the chosen
// captures need into a temporary directory beside the destination, hashing
// every file as it copies, and publishes the bundle by one rename. It
// refuses to publish what it cannot vouch for: a file that changed or
// vanished while it was copied, a marker set that changed during the
// collection, required evidence past a bound, or a restore rule it would
// have to guess. Evidence that is simply not there is recorded as absent,
// with the reason, for the verifier to report.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unistd.h>

#include "clink/cluster/coordination_store.hpp"
#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/core/sha256.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::incident {

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;

struct Copy {
    fs::path source;
    std::string rel;  // bundle-relative
    std::string role;
};

class Collector {
public:
    explicit Collector(const CollectOptions& o) : o_(o), started_(Clock::now()) {}

    CollectResult run();

private:
    // Copy one file into the bundle, hashing as it goes, and refuse when the
    // source changed or vanished while it was read.
    void copy(const fs::path& source,
              const std::string& rel,
              const std::string& role,
              bool required);
    void write_generated(const std::string& rel, const std::string& role, const std::string& body);
    void check_deadline() const;
    void absent(const std::string& role, const std::string& path, const std::string& reason);
    std::set<std::string> list_markers() const;
    void collect_job_manifest();
    void collect_checkpoint();
    void collect_captures();
    // With --include-state: each participant's snapshot of a completed
    // checkpoint that bounds a collected sink interval, so the source
    // positions at both ends of the interval travel with it.
    void collect_interval_state(std::uint64_t checkpoint);
    void collect_traces();
    std::string manifest_text() const;

    const CollectOptions& o_;
    Clock::time_point started_;
    fs::path tmp_;
    std::uint64_t files_{0};
    std::uint64_t bytes_{0};
    config::JsonArray artifacts_;
    config::JsonArray absent_;
    config::JsonArray excluded_;
    config::JsonArray capture_list_;
    config::JsonArray interval_list_;  // sink interval evidence
    std::vector<std::string> notes_;

    // Selection.
    std::optional<bool> confirm_;
    std::string confirm_source_;
    cluster::RestorePointOnDisk point_;
    // The job's RESTORED record, whatever its restore rule: it also says which
    // checkpoint the restored run's first capture epoch starts from.
    std::optional<cluster::RestoredRecord> restored_;
    std::uint64_t checkpoint_{0};
    bool overridden_{false};
    cluster::RestoreSource source_;
    std::string state_rel_;      // bundle-relative state root ("checkpoint/_state/<job>")
    std::string original_root_;  // the state root on this machine, which state_rel_ mirrors
    config::JsonArray participant_state_;  // each participant's backend and path
    bool state_local_{false};

    // Job manifest facts.
    bool have_job_manifest_{false};
    std::optional<config::JsonValue> delivery_;
    bool rescaled_{false};
    std::uint64_t prior_graph_through_{0};

    bool traces_{false};
    bool have_captures_{false};
    std::set<std::uint64_t> interval_states_;  // checkpoints collect_interval_state took
};

// A state root as a canonical local path, or nullopt for one in a store a
// URI names (s3://, rocksdb://, ...), which is never inside the checkpoint
// directory whatever the working directory.
std::optional<std::string> local_path(const std::string& root) {
    std::string_view p = root;
    if (p.starts_with("file://")) {
        p.remove_prefix(7);
    } else if (p.find("://") != std::string_view::npos) {
        return std::nullopt;
    }
    std::error_code ec;
    const auto canon = fs::weakly_canonical(fs::path{std::string{p}}, ec);
    if (ec) {
        return std::nullopt;
    }
    return canon.string();
}

// A state root on this machine's filesystem, as written: an absolute path, or
// one behind the file, RocksDB or changelog-over-files schemes, whose
// checkpoints a bundle can copy. Anything else (an object store, a remote tier,
// a relative path, which would depend on the working directory) is not.
std::optional<std::string> written_state_root(const std::string& root) {
    std::string_view p = root;
    for (const std::string_view scheme : {"file://", "rocksdb://", "changelog+file://"}) {
        if (p.starts_with(scheme)) {
            p.remove_prefix(scheme.size());
            if (const auto q = p.find('?'); q != std::string_view::npos) {
                p = p.substr(0, q);
            }
            break;
        }
    }
    if (p.find("://") != std::string_view::npos || p.empty() || !fs::path{p}.is_absolute()) {
        return std::nullopt;
    }
    std::string out{p};
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    return out;
}

std::string utc_now() {
    const auto t = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

void Collector::check_deadline() const {
    if (Clock::now() - started_ > o_.timeout) {
        throw CollectError("collection ran past --timeout-s (" +
                               std::to_string(o_.timeout.count()) +
                               " s); nothing was published. Narrow it with --op and --epochs, or "
                               "raise the bound.",
                           CollectError::Kind::Timeout);
    }
}

void Collector::absent(const std::string& role,
                       const std::string& path,
                       const std::string& reason) {
    config::JsonObject a;
    a["role"] = config::JsonValue{role};
    a["path"] = config::JsonValue{path};
    a["reason"] = config::JsonValue{reason};
    absent_.push_back(config::JsonValue{std::move(a)});
}

void Collector::copy(const fs::path& source,
                     const std::string& rel,
                     const std::string& role,
                     bool required) {
    check_deadline();
    std::error_code ec;
    const auto st = fs::symlink_status(source, ec);
    if (ec || !fs::is_regular_file(st)) {
        throw CollectError(source.string() +
                           " is not a regular file; a bundle copies only "
                           "regular files");
    }
    const auto size_before = fs::file_size(source, ec);
    const auto mtime_before = fs::last_write_time(source, ec);
    if (ec) {
        throw CollectError(source.string() + " vanished before it could be copied: " +
                               ec.message() + ". Nothing was published; collect again.",
                           CollectError::Kind::Changed);
    }
    if (files_ + 1 > o_.max_files || bytes_ + size_before > o_.max_bytes) {
        if (required) {
            throw CollectError("the evidence the selected checkpoint needs passes --max-files (" +
                                   std::to_string(o_.max_files) + ") or --max-bytes (" +
                                   std::to_string(o_.max_bytes) +
                                   "); nothing was published. Raise "
                                   "the bound.",
                               CollectError::Kind::Bound);
        }
        excluded_.push_back(config::JsonValue{rel + ": past --max-files or --max-bytes"});
        return;
    }
    const auto dest = tmp_ / rel;
    fs::create_directories(dest.parent_path());
    std::ifstream in(source, std::ios::binary);
    std::ofstream out(dest, std::ios::binary | std::ios::trunc);
    if (!in || !out) {
        throw CollectError("cannot copy " + source.string() + " into the bundle");
    }
    Sha256 hash;
    std::vector<char> buf(1 << 20);
    std::uint64_t copied = 0;
    while (in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        const auto got = in.gcount();
        if (got > 0) {
            hash.update(buf.data(), static_cast<std::size_t>(got));
            out.write(buf.data(), got);
            copied += static_cast<std::uint64_t>(got);
        }
    }
    out.close();
    if (!out) {
        throw CollectError("cannot write " + rel + " into the bundle");
    }
    if (o_.after_copy) {
        o_.after_copy(source);
    }
    // The copy is only evidence if the source was still those bytes when it
    // was read: a file rewritten, truncated or removed meanwhile means the
    // bundle would mix two states of the job.
    const auto still = fs::symlink_status(source, ec);
    if (ec || !fs::exists(still)) {
        throw CollectError(
            source.string() +
                " disappeared while the bundle was collected; nothing was published. "
                "Collect again, or stop the job first.",
            CollectError::Kind::Changed);
    }
    const auto size_after = fs::file_size(source, ec);
    const auto mtime_after = fs::last_write_time(source, ec);
    if (ec || size_after != size_before || mtime_after != mtime_before || copied != size_before) {
        throw CollectError(source.string() +
                               " changed while the bundle was collected; nothing was published. "
                               "Collect again, or stop the job first.",
                           CollectError::Kind::Changed);
    }
    ++files_;
    bytes_ += copied;
    config::JsonObject a;
    a["path"] = config::JsonValue{rel};
    a["role"] = config::JsonValue{role};
    a["bytes"] = config::JsonValue{static_cast<std::int64_t>(copied)};
    a["sha256"] = config::JsonValue{Sha256::to_hex(hash.finalize())};
    artifacts_.push_back(config::JsonValue{std::move(a)});
}

void Collector::write_generated(const std::string& rel,
                                const std::string& role,
                                const std::string& body) {
    const auto dest = tmp_ / rel;
    fs::create_directories(dest.parent_path());
    {
        std::ofstream out(dest, std::ios::binary | std::ios::trunc);
        out << body;
        if (!out) {
            throw CollectError("cannot write " + rel + " into the bundle");
        }
    }
    Sha256 hash;
    hash.update(body.data(), body.size());
    ++files_;
    bytes_ += body.size();
    config::JsonObject a;
    a["path"] = config::JsonValue{rel};
    a["role"] = config::JsonValue{role};
    a["bytes"] = config::JsonValue{static_cast<std::int64_t>(body.size())};
    a["sha256"] = config::JsonValue{Sha256::to_hex(hash.finalize())};
    artifacts_.push_back(config::JsonValue{std::move(a)});
}

std::set<std::string> Collector::list_markers() const {
    std::set<std::string> out;
    const auto keys = cluster::make_coordination_store(o_.checkpoint_dir)
                          ->list("_jobs/" + std::to_string(o_.job));
    for (const auto& k : keys) {
        out.insert(fs::path(k).filename().string());
    }
    return out;
}

void Collector::collect_job_manifest() {
    if (o_.ha_dir.empty()) {
        absent("job_manifest",
               "jobs/" + std::to_string(o_.job) + "/manifest.json",
               "no --ha-dir was given");
        return;
    }
    std::optional<std::string> body;
    try {
        body = cluster::make_coordination_store(o_.ha_dir)->get("jobs/" + std::to_string(o_.job) +
                                                                "/manifest.json");
    } catch (const std::exception& e) {
        absent("job_manifest",
               "jobs/" + std::to_string(o_.job) + "/manifest.json",
               std::string{"the HA directory could not be read: "} + e.what());
        return;
    }
    if (!body.has_value()) {
        absent("job_manifest",
               "jobs/" + std::to_string(o_.job) + "/manifest.json",
               "the HA directory holds no manifest for this job");
        return;
    }
    config::JsonValue m;
    try {
        m = config::parse(*body);
    } catch (const std::exception& e) {
        absent("job_manifest",
               "jobs/" + std::to_string(o_.job) + "/manifest.json",
               std::string{"the job manifest does not parse: "} + e.what());
        return;
    }
    if (m.is_object() && m.contains("requires_commit_confirmation") &&
        m.at("requires_commit_confirmation").is_bool() && !o_.commit_confirmation.has_value()) {
        confirm_ = m.at("requires_commit_confirmation").as_bool();
        confirm_source_ = "the job manifest";
    }
    if (m.is_object() && m.contains("prior_graph_json")) {
        rescaled_ = true;
        prior_graph_through_ = static_cast<std::uint64_t>(m.int_or("prior_graph_through", 0));
    }
    // The delivery report, from the graph as the gate analyses it at submission.
    std::string graph_text;
    if (m.is_object() && m.contains("graph_json") && m.at("graph_json").is_string()) {
        graph_text = m.at("graph_json").as_string();
    }
    if (!graph_text.empty()) {
        try {
            const auto graph = cluster::JobGraphSpec::from_json(graph_text);
            cluster::CheckpointConfig cfg;
            cfg.checkpoint_dir = m.string_or("checkpoint_dir", "");
            cfg.state_backend_uri = m.string_or("state_backend_uri", "");
            cfg.interval_ms = m.int_or("interval_ms", 0);
            connectors::GuaranteeReport report;
            (void)cluster::check_delivery_guarantee(graph, cfg, &report);
            delivery_ = config::parse(report.render_json());
        } catch (const std::exception& e) {
            notes_.push_back(std::string{"the delivery report could not be computed from the "
                                         "job's graph: "} +
                             e.what());
        }
        // Credentials out of the graph's parameters before it leaves the machine.
        try {
            auto graph = config::parse(graph_text);
            std::size_t redacted = 0;
            if (graph.is_object() && graph.contains("ops") && graph.at("ops").is_array()) {
                for (auto& op : graph.as_object()["ops"].as_array()) {
                    if (!op.is_object() || !op.contains("params") || !op.at("params").is_object()) {
                        continue;
                    }
                    for (auto& [k, v] : op.as_object()["params"].as_object()) {
                        if (looks_like_credential(std::string{k})) {
                            v = config::JsonValue{std::string{"<redacted>"}};
                            ++redacted;
                        }
                    }
                }
            }
            m.as_object()["graph_json"] = config::JsonValue{graph.serialize(0)};
            if (redacted > 0) {
                notes_.push_back(std::to_string(redacted) +
                                 " graph parameter(s) that looked like credentials were "
                                 "redacted from job/manifest.json");
            }
        } catch (const std::exception&) {
            m.as_object()["graph_json"] =
                config::JsonValue{std::string{"<redacted: the graph did not parse>"}};
            notes_.push_back(
                "the job's graph did not parse and was left out of "
                "job/manifest.json");
        }
    }
    if (m.is_object() && m.contains("prior_graph_json")) {
        m.as_object()["prior_graph_json"] =
            config::JsonValue{std::string{"<omitted: may carry credentials>"}};
    }
    write_generated("job/manifest.json", "job_manifest", m.serialize(2) + "\n");
    have_job_manifest_ = true;
}

void Collector::collect_checkpoint() {
    const auto marker_dir = cluster::completed_marker_dir_for(o_.checkpoint_dir, o_.job);
    const auto markers = list_markers();
    // Every marker of the job: the restore rule is recomputed over them. A
    // marker is its prefix and a checkpoint id and nothing else, as the
    // engine's scans read them; a writer's temporary file is not one.
    const auto is_marker = [](const std::string& name) {
        for (const std::string_view prefix : {"COMPLETED-", "CONFIRMED-"}) {
            if (name.size() > prefix.size() && name.starts_with(prefix) &&
                name.find_first_not_of("0123456789", prefix.size()) == std::string::npos) {
                return true;
            }
        }
        return name == "RESTORED";
    };
    for (const auto& name : markers) {
        if (!is_marker(name)) {
            continue;
        }
        const auto role = name == "RESTORED"                 ? "restored_record"
                          : name.rfind("COMPLETED-", 0) == 0 ? "completed_marker"
                                                             : "confirmed_marker";
        copy(marker_dir / name,
             "checkpoint/_jobs/" + std::to_string(o_.job) + "/" + name,
             role,
             /*required=*/true);
    }
    // The participants' state, under the root and generation the marker names.
    try {
        source_ = cluster::resolve_restore_source(marker_dir.string(), checkpoint_);
    } catch (const std::exception& e) {
        throw CollectError("checkpoint " + std::to_string(checkpoint_) +
                           "'s restore source cannot be resolved: " + e.what());
    }
    std::error_code ec;
    const auto ckpt_canon = fs::weakly_canonical(o_.checkpoint_dir, ec).string() + "/";
    const auto root = written_state_root(source_.state_root);
    state_local_ = !source_.legacy && root.has_value();
    if (!state_local_) {
        absent("snapshot",
               source_.state_root,
               source_.legacy
                   ? "the checkpoint predates job-namespaced state, which this release does not "
                     "collect"
                   : "the state is not on this machine's filesystem (an object store or a remote "
                     "tier), which this release does not collect");
        return;
    }
    // Files are read where the job wrote them; the canonical form only decides
    // whether that is inside the checkpoint directory. State there keeps its
    // place; a root outside it (a RocksDB or changelog path of its own) is
    // mirrored under _external.
    original_root_ = *root;
    const auto root_canon = fs::weakly_canonical(fs::path{*root}, ec).string();
    state_rel_ = !ec && root_canon.rfind(ckpt_canon, 0) == 0
                     ? "checkpoint/" + root_canon.substr(ckpt_canon.size())
                     : "checkpoint/_external/" + std::to_string(o_.job);
    if (!source_.participants.has_value()) {
        absent("snapshot",
               state_rel_,
               "the marker records no participant set, so which snapshots belong to it is "
               "unknown");
        return;
    }
    const auto id = std::to_string(checkpoint_);
    const auto name = "checkpoint-" + id + ".snap";
    for (const auto sub : *source_.participants) {
        const fs::path dir = state_dir_for(original_root_, source_.generation, sub);
        const auto rel_dir = state_dir_for(state_rel_, source_.generation, sub);
        config::JsonObject ps;
        ps["index"] = config::JsonValue{static_cast<std::int64_t>(sub)};
        const auto record = [&](const std::string& backend, const std::string& path) {
            ps["backend"] = config::JsonValue{backend};
            ps["path"] = config::JsonValue{path};
            participant_state_.push_back(config::JsonValue{ps});
        };
        if (fs::exists(dir / name, ec) || fs::exists(dir / (name + ".meta"), ec)) {
            // The file backend: the canonical snapshot and its sidecar.
            const auto rel = rel_dir + "/" + name;
            record("file", rel);
            if (fs::exists(dir / (name + ".meta"), ec)) {
                copy(dir / (name + ".meta"), rel + ".meta", "snapshot_meta", true);
            } else {
                absent("snapshot_meta", rel + ".meta", "not present in the job's state directory");
            }
            if (!o_.include_state) {
                continue;
            }
            if (fs::exists(dir / name, ec)) {
                copy(dir / name, rel, "snapshot", true);
            } else {
                absent("snapshot", rel, "not present in the participant's state directory");
            }
            continue;
        }
        const auto changelog = "changelog-" + id + ".snap";
        if (fs::exists(dir / changelog, ec)) {
            // A changelog backend: its snapshot and the materialisation files
            // its handles name, which must be under the same root.
            const auto rel = rel_dir + "/" + changelog;
            record("changelog", rel);
            if (!o_.include_state) {
                continue;
            }
            copy(dir / changelog, rel, "changelog_snapshot", true);
            std::vector<std::byte> bytes;
            {
                std::ifstream in(tmp_ / rel, std::ios::binary);
                std::vector<char> raw((std::istreambuf_iterator<char>(in)),
                                      std::istreambuf_iterator<char>());
                bytes.resize(raw.size());
                if (!raw.empty()) {
                    std::memcpy(bytes.data(), raw.data(), raw.size());
                }
            }
            std::vector<std::string> handles;
            try {
                handles = changelog_materialization_handles(bytes);
            } catch (const std::exception& e) {
                absent("materialization",
                       rel,
                       std::string{"the changelog snapshot does not decode: "} + e.what());
                continue;
            }
            for (const auto& h : handles) {
                const auto local = written_state_root(h);
                if (!local.has_value() || local->rfind(original_root_ + "/", 0) != 0) {
                    absent("materialization",
                           h,
                           "a materialisation outside the job's state root, which this release "
                           "does not collect");
                    continue;
                }
                const auto inner = local->substr(original_root_.size() + 1);
                if (!safe_relative_path(inner)) {
                    absent("materialization", h, "not a plain path under the job's state root");
                    continue;
                }
                copy(*local, state_rel_ + "/" + inner, "materialization", true);
            }
            continue;
        }
        const auto rocks = fs::path{dir.string() + ".cp-" + id};
        if (fs::is_directory(fs::symlink_status(rocks, ec))) {
            // A RocksDB checkpoint directory: every file in it.
            const auto rel = rel_dir + ".cp-" + id;
            record("rocksdb", rel);
            if (!o_.include_state) {
                continue;
            }
            std::vector<fs::path> files;
            for (const auto& e : fs::recursive_directory_iterator(rocks, ec)) {
                if (!e.is_directory(ec)) {
                    files.push_back(e.path());
                }
            }
            std::sort(files.begin(), files.end());
            for (const auto& f : files) {
                copy(f,
                     rel + "/" + f.lexically_relative(rocks).generic_string(),
                     "rocksdb_checkpoint",
                     true);
            }
            continue;
        }
        record("file", rel_dir + "/" + name);
        absent("snapshot_meta",
               rel_dir + "/" + name + ".meta",
               "not present in the job's state directory");
        if (o_.include_state) {
            absent("snapshot",
                   rel_dir + "/" + name,
                   "no checkpoint of the participant in its state directory: neither a "
                   "snapshot, a changelog snapshot nor a RocksDB checkpoint");
        }
    }
}

void Collector::collect_interval_state(std::uint64_t checkpoint) {
    if (checkpoint == 0 || !interval_states_.insert(checkpoint).second) {
        return;
    }
    const auto marker_dir = cluster::completed_marker_dir_for(o_.checkpoint_dir, o_.job);
    std::error_code ec;
    if (!fs::exists(marker_dir / ("COMPLETED-" + std::to_string(checkpoint)), ec)) {
        return;  // a barrier whose checkpoint did not complete has no state to read
    }
    cluster::RestoreSource src;
    try {
        src = cluster::resolve_restore_source(marker_dir.string(), checkpoint);
    } catch (const std::exception& e) {
        absent("interval_state",
               "checkpoint " + std::to_string(checkpoint),
               std::string{"cannot be resolved: "} + e.what());
        return;
    }
    const auto local = local_path(src.state_root);
    const auto cc = fs::weakly_canonical(o_.checkpoint_dir, ec).string() + "/";
    if (src.legacy || !local.has_value() || local->rfind(cc, 0) != 0 || !src.participants) {
        absent("interval_state",
               "checkpoint " + std::to_string(checkpoint),
               "its state is outside the checkpoint directory or its participants are unknown");
        return;
    }
    const auto rel_root = "checkpoint/" + local->substr(cc.size());
    const auto name = "checkpoint-" + std::to_string(checkpoint) + ".snap";
    for (const auto sub : *src.participants) {
        const fs::path snap = fs::path{state_dir_for(src.state_root, src.generation, sub)} / name;
        const auto rel = state_dir_for(rel_root, src.generation, sub) + "/" + name;
        bool have = false;
        for (const auto& a : artifacts_) {
            have = have || a.at("path").as_string() == rel;
        }
        if (have) {
            continue;
        }
        if (!fs::exists(snap, ec)) {
            absent("interval_state", rel, "not present in the job's state directory");
            continue;
        }
        copy(snap, rel, "interval_state", false);
        if (fs::exists(snap.string() + ".meta", ec)) {
            bool have_meta = false;
            for (const auto& a : artifacts_) {
                have_meta = have_meta || a.at("path").as_string() == rel + ".meta";
            }
            if (!have_meta) {
                copy(snap.string() + ".meta", rel + ".meta", "interval_state_meta", false);
            }
        }
    }
}

void Collector::collect_captures() {
    std::error_code ec;
    if (!fs::is_directory(o_.capture_dir, ec)) {
        absent("capture_epoch", "capture/", "the capture directory does not exist");
        return;
    }
    // The epochs around the restore point by default: the one that ended at
    // it, the one after it, and, when the job's newest restore was from it,
    // that run's first epoch, which numbers from the RESTORED record's next.
    std::set<std::uint64_t> chosen_epochs;
    if (!o_.epochs.has_value()) {
        chosen_epochs = {std::max<std::uint64_t>(checkpoint_, 1), checkpoint_ + 1};
        if (restored_.has_value() && restored_->restore == checkpoint_) {
            chosen_epochs.insert(restored_->next);
        }
    }
    const auto wanted = [&](std::uint64_t epoch) {
        return o_.epochs ? (epoch >= o_.epochs->first && epoch <= o_.epochs->second)
                         : chosen_epochs.contains(epoch);
    };
    std::string selection_text;
    if (o_.epochs) {
        selection_text = std::to_string(o_.epochs->first) + "-" + std::to_string(o_.epochs->second);
    } else {
        for (const auto e : chosen_epochs) {
            selection_text += (selection_text.empty() ? "" : ", ") + std::to_string(e);
        }
    }
    std::map<std::string, std::uint64_t> outside;  // op/subtask -> files outside the selection
    std::vector<fs::path> op_dirs;
    for (const auto& e : fs::directory_iterator(o_.capture_dir, ec)) {
        if (e.is_directory() && e.path().filename().string().rfind("op-", 0) == 0) {
            op_dirs.push_back(e.path());
        }
    }
    std::sort(op_dirs.begin(), op_dirs.end());
    for (const auto& op_dir : op_dirs) {
        const auto op_name = op_dir.filename().string();
        std::uint64_t op_id = 0;
        try {
            op_id = std::stoull(op_name.substr(3));
        } catch (...) {
            continue;
        }
        if (!o_.ops.empty() && std::find(o_.ops.begin(), o_.ops.end(), op_id) == o_.ops.end()) {
            excluded_.push_back(config::JsonValue{"capture/" + op_name + ": not among --op"});
            continue;
        }
        std::vector<fs::path> subs;
        for (const auto& e : fs::directory_iterator(op_dir, ec)) {
            if (e.is_directory() && e.path().filename().string().rfind("subtask-", 0) == 0) {
                subs.push_back(e.path());
            }
        }
        std::sort(subs.begin(), subs.end());
        for (const auto& sub_dir : subs) {
            const auto sub_name = sub_dir.filename().string();
            std::uint64_t sub = 0;
            try {
                sub = std::stoull(sub_name.substr(8));
            } catch (...) {
                continue;
            }
            const auto base = "capture/" + op_name + "/" + sub_name;
            if (fs::exists(sub_dir / "op.json", ec)) {
                copy(sub_dir / "op.json", base + "/op.json", "capture_spec", false);
            }
            std::vector<std::pair<std::uint64_t, fs::path>> epochs;
            // A sink's interval evidence, epoch-<N>.sink.json, beside its
            // epochs; an interval with no records has evidence and no epoch file.
            std::map<std::uint64_t, fs::path> intervals;
            for (const auto& e : fs::directory_iterator(sub_dir, ec)) {
                const auto n = e.path().filename().string();
                static constexpr std::string_view kIntervalSuffix = ".sink.json";
                if (n.rfind("epoch-", 0) == 0 && n.size() > 6 + kIntervalSuffix.size() &&
                    n.substr(n.size() - kIntervalSuffix.size()) == kIntervalSuffix) {
                    const auto digits = n.substr(6, n.size() - 6 - kIntervalSuffix.size());
                    if (!digits.empty() &&
                        digits.find_first_not_of("0123456789") == std::string::npos) {
                        try {
                            intervals.emplace(std::stoull(digits), e.path());
                        } catch (...) {
                        }
                    }
                    continue;
                }
                if (n.rfind("epoch-", 0) != 0 || n.size() < 11 ||
                    n.substr(n.size() - 4) != ".cap") {
                    continue;
                }
                try {
                    epochs.emplace_back(std::stoull(n.substr(6, n.size() - 10)), e.path());
                } catch (...) {
                }
            }
            std::sort(epochs.begin(), epochs.end());
            for (const auto& [epoch, path] : intervals) {
                if (!wanted(epoch)) {
                    ++outside[op_name + "/" + sub_name];
                    continue;
                }
                const auto rel = base + "/" + path.filename().string();
                const auto before = artifacts_.size();
                copy(path, rel, "sink_interval", false);
                if (artifacts_.size() == before) {
                    continue;  // excluded by a bound
                }
                config::JsonObject iv;
                iv["op"] = config::JsonValue{std::to_string(op_id)};
                iv["subtask"] = config::JsonValue{static_cast<std::int64_t>(sub)};
                iv["epoch"] = config::JsonValue{static_cast<std::int64_t>(epoch)};
                iv["path"] = config::JsonValue{rel};
                interval_list_.push_back(config::JsonValue{std::move(iv)});
                if (o_.include_state) {
                    collect_interval_state(epoch);
                    // A run's first interval begins at the checkpoint the run
                    // restored from, which its evidence names as barrier 0.
                    if (restored_.has_value()) {
                        collect_interval_state(restored_->restore);
                    }
                    try {
                        std::ifstream in(path, std::ios::binary);
                        std::ostringstream text;
                        text << in.rdbuf();
                        const auto doc = config::parse(text.str());
                        if (doc.contains("previous_barrier") &&
                            doc.at("previous_barrier").is_integral_number() &&
                            doc.at("previous_barrier").as_int() > 0) {
                            collect_interval_state(
                                static_cast<std::uint64_t>(doc.at("previous_barrier").as_int()));
                        }
                    } catch (const std::exception&) {
                        // An interval that does not parse is listed as it is;
                        // reconciliation says why it cannot use it.
                    }
                }
            }
            for (const auto& [epoch, path] : epochs) {
                if (!wanted(epoch)) {
                    ++outside[op_name + "/" + sub_name];
                    continue;
                }
                const auto rel = base + "/" + path.filename().string();
                const auto before = artifacts_.size();
                copy(path, rel, "capture_epoch", false);
                if (artifacts_.size() == before) {
                    continue;  // excluded by a bound
                }
                have_captures_ = true;
                config::JsonObject cap;
                // Text: an operator id is 64 bits, and JSON integers stop at 2^63.
                cap["op"] = config::JsonValue{std::to_string(op_id)};
                cap["subtask"] = config::JsonValue{static_cast<std::int64_t>(sub)};
                cap["epoch"] = config::JsonValue{static_cast<std::int64_t>(epoch)};
                cap["path"] = config::JsonValue{rel};
                // The header says whether the epoch was truncated at capture.
                std::ifstream in(path, std::ios::binary);
                std::array<char, 32> head{};
                in.read(head.data(), head.size());
                const auto got = static_cast<std::size_t>(in.gcount());
                if (const auto h = capture::decode_capture_header(std::span<const std::byte>{
                        reinterpret_cast<const std::byte*>(head.data()), got});
                    h.has_value()) {
                    cap["format"] = config::JsonValue{static_cast<std::int64_t>(h->first.version)};
                    cap["truncated"] = config::JsonValue{h->first.truncated};
                    cap["records_seen"] =
                        config::JsonValue{static_cast<std::int64_t>(h->first.records_seen)};
                }
                capture_list_.push_back(config::JsonValue{std::move(cap)});
                // The replay's starting state: checkpoint epoch-1, for this subtask.
                const auto prev = epoch_starting_checkpoint(
                    epoch,
                    restored_.has_value() ? std::optional{restored_->restore} : std::nullopt,
                    restored_.has_value() ? std::optional{restored_->next} : std::nullopt);
                if (o_.include_state && prev > 0) {
                    try {
                        const auto src = cluster::resolve_restore_source(
                            cluster::completed_marker_dir_for(o_.checkpoint_dir, o_.job).string(),
                            prev);
                        const auto local = local_path(src.state_root);
                        const auto rc = local.value_or("");
                        const auto cc = fs::weakly_canonical(o_.checkpoint_dir, ec).string() + "/";
                        if (src.legacy || !local.has_value() || rc.rfind(cc, 0) != 0) {
                            absent("replay_state",
                                   base + " epoch " + std::to_string(epoch),
                                   "its starting state is outside the checkpoint directory");
                            continue;
                        }
                        const auto name = "checkpoint-" + std::to_string(prev) + ".snap";
                        const fs::path snap =
                            fs::path{state_dir_for(
                                src.state_root, src.generation, static_cast<std::uint32_t>(sub))} /
                            name;
                        const auto snap_rel = state_dir_for("checkpoint/" + rc.substr(cc.size()),
                                                            src.generation,
                                                            static_cast<std::uint32_t>(sub)) +
                                              "/" + name;
                        bool have = false;
                        for (const auto& a : artifacts_) {
                            have = have || a.at("path").as_string() == snap_rel;
                        }
                        if (have) {
                            continue;
                        }
                        if (fs::exists(snap, ec)) {
                            copy(snap, snap_rel, "replay_state", false);
                            if (fs::exists(snap.string() + ".meta", ec)) {
                                copy(snap.string() + ".meta",
                                     snap_rel + ".meta",
                                     "replay_state_meta",
                                     false);
                            }
                        } else {
                            absent("replay_state",
                                   snap_rel,
                                   "the starting state for epoch " + std::to_string(epoch) +
                                       " is not present");
                        }
                    } catch (const std::exception& e) {
                        absent("replay_state",
                               base + " epoch " + std::to_string(epoch),
                               std::string{"checkpoint "} + std::to_string(epoch - 1) +
                                   " cannot be resolved: " + e.what());
                    }
                }
            }
        }
    }
    for (const auto& [where, n] : outside) {
        excluded_.push_back(config::JsonValue{"capture/" + where + ": " + std::to_string(n) +
                                              " epoch file(s) outside epochs " + selection_text});
    }
}

void Collector::collect_traces() {
    std::error_code ec;
    if (!fs::is_directory(o_.trace_dir, ec)) {
        absent("trace", "traces/", "the trace directory does not exist");
        return;
    }
    std::vector<fs::path> files;
    for (const auto& e : fs::directory_iterator(o_.trace_dir, ec)) {
        if (e.is_regular_file() && e.path().extension() == ".ndjson") {
            files.push_back(e.path());
        }
    }
    std::sort(files.begin(), files.end());
    for (const auto& f : files) {
        const auto before = artifacts_.size();
        copy(f, "traces/" + f.filename().string(), "trace", false);
        traces_ = traces_ || artifacts_.size() != before;
    }
    if (files.empty()) {
        absent("trace", "traces/", "the trace directory holds no .ndjson files");
    }
}

std::string Collector::manifest_text() const {
    config::JsonObject m;
    m["format"] = config::JsonValue{std::string{kFormat}};
    m["version"] = config::JsonValue{kVersion};
    const auto facts = connectors::current_build_facts();
    config::JsonObject col;
    col["collected_at"] = config::JsonValue{utc_now()};
    col["clink_version"] = config::JsonValue{facts.clink_version};
    col["git_sha"] = config::JsonValue{facts.git_sha};
    col["git_clean"] = config::JsonValue{facts.git_clean};
    m["collector"] = config::JsonValue{std::move(col)};

    config::JsonObject job;
    job["id"] = config::JsonValue{static_cast<std::int64_t>(o_.job)};
    job["commit_confirmation"] =
        confirm_.has_value() ? config::JsonValue{*confirm_} : config::JsonValue{};
    job["commit_confirmation_source"] = config::JsonValue{confirm_source_};
    job["rescaled"] = config::JsonValue{rescaled_};
    if (rescaled_) {
        job["prior_graph_through"] =
            config::JsonValue{static_cast<std::int64_t>(prior_graph_through_)};
    }
    m["job"] = config::JsonValue{std::move(job)};

    config::JsonObject sel;
    sel["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(checkpoint_)};
    sel["engine_choice"] = config::JsonValue{static_cast<std::int64_t>(point_.chosen)};
    sel["overridden"] = config::JsonValue{overridden_};
    sel["completed"] = config::JsonValue{static_cast<std::int64_t>(point_.completed)};
    sel["confirmed"] = config::JsonValue{static_cast<std::int64_t>(point_.confirmed)};
    if (point_.restored.has_value()) {
        config::JsonObject r;
        r["restore"] = config::JsonValue{static_cast<std::int64_t>(point_.restored->restore)};
        r["next"] = config::JsonValue{static_cast<std::int64_t>(point_.restored->next)};
        sel["restored"] = config::JsonValue{std::move(r)};
    }
    sel["generation"] = config::JsonValue{static_cast<std::int64_t>(source_.generation)};
    config::JsonArray parts;
    if (source_.participants.has_value()) {
        for (const auto p : *source_.participants) {
            parts.push_back(config::JsonValue{static_cast<std::int64_t>(p)});
        }
    }
    sel["participants"] = config::JsonValue{std::move(parts)};
    sel["state_root"] = config::JsonValue{state_rel_};
    sel["state_root_original"] = config::JsonValue{original_root_};
    sel["participant_state"] = config::JsonValue{participant_state_};
    m["selection"] = config::JsonValue{std::move(sel)};

    m["delivery"] = delivery_.has_value() ? *delivery_ : config::JsonValue{};

    config::JsonObject contents;
    contents["state"] = config::JsonValue{o_.include_state && state_local_};
    contents["captures"] = config::JsonValue{have_captures_};
    contents["sink_intervals"] = config::JsonValue{!interval_list_.empty()};
    contents["traces"] = config::JsonValue{traces_};
    contents["job_manifest"] = config::JsonValue{have_job_manifest_};
    m["contents"] = config::JsonValue{std::move(contents)};

    config::JsonObject limits;
    limits["max_bytes"] = config::JsonValue{static_cast<std::int64_t>(o_.max_bytes)};
    limits["max_files"] = config::JsonValue{static_cast<std::int64_t>(o_.max_files)};
    limits["timeout_s"] = config::JsonValue{static_cast<std::int64_t>(o_.timeout.count())};
    m["limits"] = config::JsonValue{std::move(limits)};

    m["captures"] = config::JsonValue{capture_list_};
    m["sink_intervals"] = config::JsonValue{interval_list_};
    m["artifacts"] = config::JsonValue{artifacts_};
    m["absent"] = config::JsonValue{absent_};
    m["excluded"] = config::JsonValue{excluded_};
    config::JsonArray notes;
    for (const auto& n : notes_) {
        notes.push_back(config::JsonValue{n});
    }
    m["notes"] = config::JsonValue{std::move(notes)};
    return config::JsonValue{std::move(m)}.serialize(2) + "\n";
}

CollectResult Collector::run() {
    if (o_.checkpoint_dir.empty() || o_.job == 0 || o_.out.empty()) {
        throw CollectError("a checkpoint directory, a job and an output directory are required");
    }
    if (o_.checkpoint_dir.find("://") != std::string::npos) {
        throw CollectError("only a local checkpoint directory is collected in this release");
    }
    std::error_code ec;
    fs::path out = fs::absolute(o_.out, ec).lexically_normal();
    if (!out.has_filename()) {
        out = out.parent_path();  // a trailing separator
    }
    if (fs::exists(fs::symlink_status(out, ec))) {
        throw CollectError(out.string() + " already exists; a bundle is never written over");
    }
    if (!fs::is_directory(cluster::completed_marker_dir_for(o_.checkpoint_dir, o_.job), ec)) {
        throw CollectError("no markers for job " + std::to_string(o_.job) + " under " +
                               o_.checkpoint_dir + "/_jobs/",
                           CollectError::Kind::Missing);
    }
    std::random_device rd;
    tmp_ = out.parent_path() /
           ("." + out.filename().string() + ".incomplete-" +
            std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(rd() % 1000000));
    fs::create_directories(tmp_);
    try {
        const auto markers_before = list_markers();
        restored_ = cluster::read_restore_point(o_.checkpoint_dir, o_.job);
        if (o_.commit_confirmation.has_value()) {
            confirm_ = o_.commit_confirmation;
            confirm_source_ = "--commit-confirmation";
        }
        collect_job_manifest();
        if (!confirm_.has_value() && !o_.checkpoint.has_value()) {
            throw CollectError(
                "which restore rule applies depends on whether the job's sinks "
                "need commit confirmation, which neither --ha-dir nor "
                "--commit-confirmation says; nothing was published");
        }
        if (confirm_.has_value()) {
            point_ = cluster::restore_point_on_disk(o_.checkpoint_dir, o_.job, *confirm_);
        }
        checkpoint_ = o_.checkpoint.value_or(point_.chosen);
        overridden_ = o_.checkpoint.has_value();
        if (checkpoint_ == 0) {
            throw CollectError(
                "the job has no checkpoint a restore could choose; nothing was "
                "published",
                CollectError::Kind::Missing);
        }
        if (!markers_before.count("COMPLETED-" + std::to_string(checkpoint_))) {
            throw CollectError("checkpoint " + std::to_string(checkpoint_) +
                                   " has no COMPLETED marker; nothing was published",
                               CollectError::Kind::Missing);
        }
        collect_checkpoint();
        if (o_.include_captures && !o_.capture_dir.empty()) {
            collect_captures();
        } else if (!o_.capture_dir.empty()) {
            notes_.push_back(
                "captures were not collected: --include-captures was not given "
                "(they hold the job's records)");
        }
        if (!o_.trace_dir.empty()) {
            collect_traces();
        }
        // A marker set that changed while the evidence was copied belongs to
        // a job that moved on: the bundle would describe no single moment.
        const auto markers_after = list_markers();
        if (o_.allow_new_markers) {
            std::size_t added = 0;
            for (const auto& name : markers_before) {
                if (!markers_after.contains(name)) {
                    throw CollectError("the job's marker " + name +
                                           " was removed while the checkpoint was collected; "
                                           "nothing was published. Collect again.",
                                       CollectError::Kind::Changed);
                }
            }
            for (const auto& name : markers_after) {
                added += markers_before.contains(name) ? 0 : 1;
            }
            if (added > 0) {
                notes_.push_back(std::to_string(added) +
                                 " marker(s) appeared while the checkpoint was collected (later "
                                 "checkpoints completing, or a restore recorded); they are not "
                                 "part of it");
            }
        } else if (markers_after != markers_before) {
            throw CollectError(
                "the job's markers changed while the bundle was collected (a "
                "checkpoint completed or a restart was recorded); nothing was "
                "published. Collect again, or stop the job first.",
                CollectError::Kind::Changed);
        }
        {
            std::ofstream mf(tmp_ / "manifest.json", std::ios::binary | std::ios::trunc);
            mf << manifest_text();
            if (!mf) {
                throw CollectError("cannot write the manifest");
            }
        }
        fs::rename(tmp_, out, ec);
        if (ec) {
            throw CollectError("cannot publish the bundle at " + out.string() + ": " +
                               ec.message());
        }
    } catch (...) {
        std::error_code rm;
        fs::remove_all(tmp_, rm);
        throw;
    }
    CollectResult r;
    r.bundle = out;
    r.checkpoint = checkpoint_;
    r.files = files_;
    r.bytes = bytes_;
    r.seconds = std::chrono::duration<double>(Clock::now() - started_).count();
    r.notes = notes_;
    for (const auto& e : excluded_) {
        r.notes.push_back("excluded: " + e.as_string());
    }
    return r;
}

}  // namespace

const char* collect_error_kind_name(CollectError::Kind kind) noexcept {
    switch (kind) {
        case CollectError::Kind::Changed:
            return "changed";
        case CollectError::Kind::Missing:
            return "missing";
        case CollectError::Kind::Bound:
            return "bound";
        case CollectError::Kind::Timeout:
            return "timeout";
        case CollectError::Kind::Other:
            break;
    }
    return "other";
}

// A relative directory is the caller's working directory's. The collector
// resolves the state roots a marker records against the checkpoint directory
// and reads only absolute ones as local, so a relative directory made every
// participant's state look as if it lived elsewhere.
static std::string absolute_local(const std::string& dir) {
    if (dir.empty() || dir.find("://") != std::string::npos) {
        return dir;
    }
    std::error_code ec;
    fs::path p = fs::absolute(dir, ec).lexically_normal();
    if (ec) {
        return dir;
    }
    if (!p.has_filename() && p.has_parent_path()) {
        p = p.parent_path();
    }
    return p.string();
}

CollectResult collect(const CollectOptions& options) {
    try {
        CollectOptions resolved = options;
        resolved.checkpoint_dir = absolute_local(options.checkpoint_dir);
        resolved.ha_dir = absolute_local(options.ha_dir);
        Collector c(resolved);
        return c.run();
    } catch (const CollectError&) {
        throw;
    } catch (const std::exception& e) {
        throw CollectError(std::string{"collection failed: "} + e.what() +
                           "; nothing was published");
    }
}

}  // namespace clink::incident
