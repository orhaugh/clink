#include "clink/cluster/coordinator.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <set>
#include <stdexcept>
#include <system_error>
#include <unordered_set>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/cluster/admission_policy.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/config_lint.hpp"
#include "clink/cluster/connector_availability.hpp"
#include "clink/cluster/coordination_store.hpp"
#include "clink/cluster/fenced_metadata.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"
#include "clink/cluster/protocol_trace.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/cluster/restore_compat_gate.hpp"
#include "clink/cluster/row_layout_admission.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/metrics/otlp_export.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/runtime/event_bus.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink::cluster {

namespace {

// A name for one coordinator's claims on a job's checkpoint-id record, unique
// to this deploy of the job in this process (record_triggered_id): a retry that
// finds its own claim on record, the first attempt's answer lost, knows it for
// its own. The epoch is there for whoever reads the record; the random part is
// what makes the name this coordinator's alone.
[[nodiscard]] std::string new_id_claimant(std::uint64_t epoch) {
    std::random_device entropy;
    std::string name = "epoch" + std::to_string(epoch) + "-";
    constexpr char kHex[] = "0123456789abcdef";
    for (int word = 0; word < 4; ++word) {
        auto bits = static_cast<std::uint32_t>(entropy());
        for (int nibble = 0; nibble < 8; ++nibble) {
            name.push_back(kHex[bits & 0xFU]);
            bits >>= 4U;
        }
    }
    return name;
}

// The highest generation directory (v<N>) present under a state root, or 1 if there
// is none.
//
// Needed only for a restore from an EXTERNAL directory - a savepoint, or an explicit
// --restore-from-checkpoint-id. For its own checkpoints the coordinator knows the
// generation exactly and never guesses. A savepoint carries no metadata naming its
// generation, so the highest one present is the best available answer, and it is the
// right one for the ordinary case of a savepoint taken from a job's final state.
[[nodiscard]] std::uint32_t highest_generation_in(const std::string& root) {
    std::uint32_t best = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(root, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_directory(ec)) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.size() < 2 || name[0] != 'v') {
            continue;
        }
        const auto digits = name.substr(1);
        if (!std::all_of(digits.begin(), digits.end(), [](unsigned char c) {
                return std::isdigit(c) != 0;
            })) {
            continue;
        }
        try {
            best = std::max(best, static_cast<std::uint32_t>(std::stoul(digits)));
        } catch (const std::exception&) {
            continue;
        }
    }
    return best == 0 ? 1u : best;
}

}  // namespace

// Out-of-line so the unique_ptr<JobBundle> field can hold a
// forward-declared type in the header; the .cpp pulls in
// job_bundle.hpp so JobBundle is complete here.
Coordinator::JobState::JobState() = default;
Coordinator::JobState::~JobState() = default;
Coordinator::JobState::JobState(JobState&&) noexcept = default;
Coordinator::JobState& Coordinator::JobState::operator=(JobState&&) noexcept = default;

namespace {

// JSON-quote a string for embedding inside event payloads. Used for
// EventBus payloads where the only user-controlled inputs are worker_ids
// and role names; full JSON shape lives in the writer in clink_node
// (this is just a per-callsite "stringify-safe" helper).
std::string js_quote(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
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
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

}  // namespace

namespace {

// Default plain-TCP accept factory: wrap the socket accept_loop_ has
// accepted in a PlainTcpConnection. TLS callers replace this via
// set_accept_factory. Plain TCP has no handshake, so there is nothing for
// the deadline or the wake to bound.
std::unique_ptr<network::Connection> default_accept_factory(
    const Coordinator::AcceptRequest& request) {
    return network::make_plain_connection(request.fd);
}

// The eviction policy's source for an accepted connection (see
// admission_policy.hpp): the peer's address, an IPv4-mapped peer as IPv4, an
// IPv6 peer by its /64, since one host is commonly given a whole /64. Empty
// when the address cannot be read.
std::string admission_source_key(int fd) {
    sockaddr_storage ss{};
    socklen_t len = sizeof(ss);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&ss), &len) != 0) {
        return {};
    }
    std::array<char, INET6_ADDRSTRLEN> buf{};
    if (ss.ss_family == AF_INET) {
        sockaddr_in in{};
        std::memcpy(&in, &ss, sizeof(in));
        return ::inet_ntop(AF_INET, &in.sin_addr, buf.data(), buf.size()) != nullptr
                   ? std::string{buf.data()}
                   : std::string{};
    }
    if (ss.ss_family == AF_INET6) {
        sockaddr_in6 in6{};
        std::memcpy(&in6, &ss, sizeof(in6));
        if (IN6_IS_ADDR_V4MAPPED(&in6.sin6_addr)) {
            in_addr v4{};
            std::memcpy(&v4, &in6.sin6_addr.s6_addr[12], sizeof(v4));
            return ::inet_ntop(AF_INET, &v4, buf.data(), buf.size()) != nullptr
                       ? std::string{buf.data()}
                       : std::string{};
        }
        std::memset(&in6.sin6_addr.s6_addr[8], 0, 8);
        return ::inet_ntop(AF_INET6, &in6.sin6_addr, buf.data(), buf.size()) != nullptr
                   ? std::string{buf.data()} + "/64"
                   : std::string{};
    }
    return {};
}

// An accepted socket, closed on the way out unless released: the accept
// thread owns each one until the connection's admission thread has taken it,
// so nothing that throws in between can leak it.
class OwnedFd {
public:
    explicit OwnedFd(int fd) noexcept : fd_(fd) {}
    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    ~OwnedFd() { network::NetworkSocket::close(fd_); }
    void release() noexcept { fd_ = -1; }

private:
    int fd_;
};

// Five lines in full per ten seconds, then a summary: enough to see what a
// flood is and where it comes from, at a rate no connect loop can raise.
std::unique_ptr<AdmissionLogLimiter> make_admission_log_limiter() {
    return std::make_unique<AdmissionLogLimiter>(5, std::chrono::seconds{10});
}

}  // namespace

Coordinator::Coordinator() {
    accept_factory_ = default_accept_factory;
    admission_log_ = make_admission_log_limiter();
}

Coordinator::Coordinator(Config cfg) : cfg_(cfg) {
    if (cfg_.advertise_host.empty()) {
        cfg_.advertise_host = cfg_.bind_host;
    }
    accept_factory_ = default_accept_factory;
    admission_log_ = make_admission_log_limiter();
}

namespace {

// The Row sidecar layout a whole-job deployment may use, `by_worker` being
// its tasks by the worker they are placed on: layout 1 when no task names a
// layout-bearing operator, otherwise from the protocol versions those workers
// registered at. A worker that is no longer registered cannot host anything
// and is skipped.
template <typename Registry, typename TasksByWorker>
RowLayoutAdmission admission_on(const Registry& registered, const TasksByWorker& by_worker) {
    std::vector<HostingWorker> hosts;
    std::size_t layout_bearing_ops = 0;
    for (const auto& [id, tasks] : by_worker) {
        layout_bearing_ops += count_layout_bearing_ops(tasks);
        if (const auto it = registered.find(id); it != registered.end()) {
            hosts.push_back(HostingWorker{id, it->second->protocol_version});
        }
    }
    return admit_row_layout(hosts, layout_bearing_ops);
}

}  // namespace

void Coordinator::set_accept_factory(AcceptFactory f) {
    listener_accept_factory_ = std::move(f);
}

void Coordinator::set_accept_factory(BoundedAcceptFactory f) {
    accept_factory_ = std::move(f);
    accept_factory_handshakes_ = true;
    listener_accept_factory_ = nullptr;
}

void Coordinator::set_autoscaler_sample_fn(AutoscalerSampleFn fn) {
    autoscaler_sample_fn_ = std::move(fn);
}

std::optional<std::uint64_t> Coordinator::autoscaler_ticks(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || !it->second->autoscaler) {
        return std::nullopt;
    }
    return it->second->autoscaler->ticks();
}

void Coordinator::stop_autoscalers_() {
    // Move each per-job autoscaler out from under the lock, then
    // destroy them outside the lock. Autoscaler::~ joins its thread;
    // the thread's tick may be blocked acquiring mu_, so we must NOT
    // hold mu_ while it spins down.
    std::vector<std::unique_ptr<Autoscaler>> to_stop;
    {
        std::lock_guard lock(mu_);
        to_stop.reserve(jobs_.size());
        for (auto& [_, job] : jobs_) {
            if (job->autoscaler) {
                to_stop.push_back(std::move(job->autoscaler));
            }
        }
    }
    to_stop.clear();  // destructors join the polling threads.
}

void Coordinator::set_ha_dir(std::string dir) {
    ha_dir_ = std::move(dir);
    if (!ha_dir_.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path{ha_dir_} / "jobs", ec);
        std::filesystem::create_directories(std::filesystem::path{ha_dir_} / "history", ec);
        reload_history_from_disk_();
    }
}

// HA metadata now goes through the coordination store's fenced_put with
// metadata_stored_epoch as the extractor - the same fenced_metadata_cas_write
// mechanism (check and durable rename inside one per-target critical
// section), behind the seam an object-store implementation can also satisfy.

// Deliberately the same hand-rolled scan the manifest readers use rather
// than a JSON parser: the field is written by this file and read by this
// file, and pulling in a parser for one integer would be the tail wagging
// the dog.
std::uint64_t metadata_epoch_in_body(const std::string& body) {
    const auto key = std::string("\"coordinator_epoch\":");
    const auto pos = body.find(key);
    if (pos == std::string::npos) {
        return 0;
    }
    try {
        return std::stoull(body.substr(pos + key.size()));
    } catch (const std::exception&) {
        return 0;
    }
}

std::uint64_t metadata_stored_epoch(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        return 0;
    }
    const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return metadata_epoch_in_body(body);
}

// Serialize a terminal-state record to <ha_dir>/history/<job_id>.json.
// One file per job mirrors HistoryServer archive layout and
// avoids needing a DB. Atomic-rename so a partially-written file is
// never observed by reload.
void persist_history_record_(const std::string& ha_dir,
                             const CompletedJobRecord& rec,
                             std::uint64_t writer_epoch) {
    if (ha_dir.empty())
        return;
    auto q = [](const std::string& s) { return js_quote(s); };
    std::string body;
    body += "{\"job_id\":" + std::to_string(rec.job_id);
    body += ",\"status\":" + q(rec.status);
    body += ",\"errors\":[";
    for (std::size_t i = 0; i < rec.errors.size(); ++i) {
        if (i > 0)
            body += ",";
        body += q(rec.errors[i]);
    }
    body += "]";
    body += ",\"restart_attempts\":" + std::to_string(rec.restart_attempts);
    body +=
        ",\"latest_completed_checkpoint_id\":" + std::to_string(rec.latest_completed_checkpoint_id);
    body += ",\"duration_ms\":" + std::to_string(rec.duration_ms.count());
    body += ",\"completed_at_unix_seconds\":" + std::to_string(rec.completed_at_unix_seconds);
    // Stamped and fenced for the same reason the manifest is: a superseded
    // coordinator recording "cancelled" over the current leader's "ok" is a
    // false operational record, even though it changes no running work.
    body += ",\"coordinator_epoch\":" + std::to_string(writer_epoch);
    body += "}";
    (void)make_coordination_store(ha_dir)->fenced_put(
        "history/" + std::to_string(rec.job_id) + ".json",
        body,
        writer_epoch,
        metadata_epoch_in_body,
        {});
}

void apply_default_state_backend(CheckpointConfig& checkpoint, const std::string& default_uri) {
    // Only the backend CHOICE (state_backend_uri) gates the default; an empty
    // default_uri is a no-op. checkpoint_dir is intentionally NOT consulted -
    // it doubles as the HA/coordination dir, so an HA-enabled job that set only
    // checkpoint_dir should still inherit a configured durable deferring tier.
    if (checkpoint.state_backend_uri.empty() && !default_uri.empty()) {
        checkpoint.state_backend_uri = default_uri;
    }
}

void pin_recovered_state_backend(CheckpointConfig& checkpoint) {
    // Resolve an empty (unspecified) backend to its legacy equivalent so the
    // recovered job keeps exactly the backend it ran with - and submit_job's
    // apply_default_state_backend, which runs next, is a no-op (URI non-empty)
    // even if a cluster default was configured after this job was submitted.
    // The resolution mirrors plugin_impl.hpp: bare checkpoint_dir -> file,
    // empty -> memory, so the rebuilt backend is byte-identical to the original.
    if (checkpoint.state_backend_uri.empty()) {
        checkpoint.state_backend_uri =
            checkpoint.checkpoint_dir.empty() ? "memory://" : checkpoint.checkpoint_dir;
    }
}

void persist_job_manifest_(const std::string& ha_dir,
                           JobId job_id,
                           const std::string& graph_json,
                           const std::vector<PluginBinary>& plugins,
                           const CheckpointConfig& checkpoint,
                           std::uint64_t writer_epoch,
                           bool requires_commit_confirmation) {
    if (ha_dir.empty())
        return;
    const auto store = make_coordination_store(ha_dir);
    const auto job_prefix = "jobs/" + std::to_string(job_id);
    // Plugin bytes: idempotent by content-hash. Same hash = same
    // bytes, so re-writes are no-ops; cheaper to skip than to re-
    // verify content.
    for (const auto& p : plugins) {
        const auto key = job_prefix + "/plugin-" + p.content_hash + ".so";
        if (store->exists(key))
            continue;
        store->put(key,
                   std::string_view(reinterpret_cast<const char*>(p.bytes.data()), p.bytes.size()));
    }
    // Manifest. Hand-crafted JSON; the readers (recover_persisted_jobs
    // below + the test fixture) parse only what they wrote.
    auto q = [](const std::string& s) { return js_quote(s); };
    std::string manifest;
    manifest += "{\"graph_json\":" + q(graph_json);
    manifest += ",\"checkpoint_dir\":" + q(checkpoint.checkpoint_dir);
    manifest += ",\"state_backend_uri\":" + q(checkpoint.state_backend_uri);
    manifest += ",\"interval_ms\":" + std::to_string(checkpoint.interval_ms);
    manifest += ",\"restore_from_dir\":" + q(checkpoint.restore_from_dir);
    manifest += ",\"requires_commit_confirmation\":";
    manifest += requires_commit_confirmation ? "true" : "false";
    manifest +=
        ",\"restore_from_checkpoint_id\":" + std::to_string(checkpoint.restore_from_checkpoint_id);
    manifest += ",\"max_restarts_on_worker_loss\":" +
                std::to_string(checkpoint.max_restarts_on_worker_loss);
    manifest += ",\"plugins\":[";
    for (std::size_t i = 0; i < plugins.size(); ++i) {
        if (i > 0)
            manifest += ",";
        manifest += "{\"name\":" + q(plugins[i].name) +
                    ",\"content_hash\":" + q(plugins[i].content_hash) + "}";
    }
    manifest += "]";
    // Stamped so a later writer can tell whether it has been superseded.
    manifest += ",\"coordinator_epoch\":" + std::to_string(writer_epoch);
    manifest += "}";
    // Refusal is logged inside the CAS (same line the direct fenced write
    // produced); the write is fire-and-forget for the same reason it was.
    (void)store->fenced_put(
        job_prefix + "/manifest.json", manifest, writer_epoch, metadata_epoch_in_body, {});
}

namespace {

// Where a job's COMPLETED-N markers live.
//
// The `_jobs/` component is load-bearing, not tidiness. The markers were
// briefly written to <checkpoint_dir>/<job_id>/, which shares a namespace
// with the per-subtask state directories: a job with parallelism 3 owns
// <checkpoint_dir>/0, /1 and /2, so job_id 1 wrote its markers straight into
// subtask 1's state directory. A prefixed component cannot collide, because
// a subtask directory is always a bare integer.
// (completed_marker_dir_for now lives in in_doubt_resolution.hpp/.cpp - the
// layout is shared with the in-doubt resolution walk.)

// Hand-roll the "find latest COMPLETED-N marker under
// <ckpt_dir>/_jobs/<job_id>/" lookup. coordinator's existing
// latest_completed_checkpoint_id is updated in-memory but NOT in the file
// system before a crash.
std::uint64_t latest_marker_id_on_disk(const std::string& checkpoint_dir,
                                       JobId job_id,
                                       const std::string& prefix) {
    if (checkpoint_dir.empty())
        return 0;
    std::uint64_t latest = 0;
    for (const auto& key :
         make_coordination_store(checkpoint_dir)->list("_jobs/" + std::to_string(job_id))) {
        const auto name = std::filesystem::path(key).filename().string();
        if (name.rfind(prefix, 0) != 0)
            continue;
        try {
            const auto id = std::stoull(name.substr(prefix.size()));
            if (id > latest)
                latest = id;
        } catch (...) {
        }
    }
    return latest;
}

std::uint64_t latest_completed_id_on_disk(const std::string& checkpoint_dir, JobId job_id) {
    return latest_marker_id_on_disk(checkpoint_dir, job_id, "COMPLETED-");
}

// Commit-confirmed restore protocol: the newest checkpoint whose external
// commits provably executed (CONFIRMED-N written by handle_commit_confirmed_).
std::uint64_t latest_confirmed_id_on_disk(const std::string& checkpoint_dir, JobId job_id) {
    return latest_marker_id_on_disk(checkpoint_dir, job_id, "CONFIRMED-");
}

// Lift one substring value out of a hand-rolled manifest JSON. Same
// shape as ha_coordinator.cpp's extractor (the format we wrote in
// persist_job_manifest_ is closed-world).
std::string read_string_field(const std::string& body, const std::string& key) {
    const auto needle = "\"" + key + "\":\"";
    auto pos = body.find(needle);
    if (pos == std::string::npos)
        return {};
    pos += needle.size();
    std::string out;
    while (pos < body.size() && body[pos] != '"') {
        if (body[pos] == '\\' && pos + 1 < body.size()) {
            const char nx = body[pos + 1];
            if (nx == 'n')
                out += '\n';
            else if (nx == 'r')
                out += '\r';
            else if (nx == 't')
                out += '\t';
            else if (nx == 'u' && pos + 5 < body.size()) {
                // 4-hex-digit escape. Treat as latin1 for our limited
                // content (we never embed non-ASCII in manifests).
                try {
                    out += static_cast<char>(std::stoi(body.substr(pos + 2, 4), nullptr, 16));
                } catch (...) {
                }
                pos += 6;
                continue;
            } else
                out += nx;
            pos += 2;
            continue;
        }
        out += body[pos++];
    }
    return out;
}

std::uint64_t read_uint_field(const std::string& body, const std::string& key) {
    const auto needle = "\"" + key + "\":";
    auto pos = body.find(needle);
    if (pos == std::string::npos)
        return 0;
    pos += needle.size();
    if (pos >= body.size() || body[pos] == '"')
        return 0;
    try {
        return std::stoull(body.substr(pos));
    } catch (...) {
        return 0;
    }
}

// Read a JSON array of quoted strings. Used for history record errors
// where every element is a plain message string written by us. Stops
// at the first unmatched bracket level - adequate since we never
// nest arrays in the records we emit.
std::vector<std::string> read_string_array_field(const std::string& body, const std::string& key) {
    const auto needle = "\"" + key + "\":[";
    auto pos = body.find(needle);
    if (pos == std::string::npos)
        return {};
    pos += needle.size();
    std::vector<std::string> out;
    while (pos < body.size() && body[pos] != ']') {
        if (body[pos] == '"') {
            std::string elem;
            ++pos;
            while (pos < body.size() && body[pos] != '"') {
                if (body[pos] == '\\' && pos + 1 < body.size()) {
                    const char nx = body[pos + 1];
                    if (nx == 'n')
                        elem += '\n';
                    else if (nx == 'r')
                        elem += '\r';
                    else if (nx == 't')
                        elem += '\t';
                    else
                        elem += nx;
                    pos += 2;
                    continue;
                }
                elem += body[pos++];
            }
            out.push_back(std::move(elem));
            if (pos < body.size())
                ++pos;  // skip closing quote
        } else {
            ++pos;
        }
    }
    return out;
}

}  // namespace

void Coordinator::recover_persisted_jobs() {
    {
        std::lock_guard begun_lock(recovery_begun_mu_);
        recovery_begun_.clear();  // a new leadership takes each job over once
    }
    if (ha_dir_.empty())
        return;
    // A persisted job is identified by its manifest key: jobs/<id>/manifest.json.
    // Key-shaped rather than directory-shaped so the coordination store's
    // object-store implementations (which have no directories) recover the
    // same set.
    std::vector<JobId> ids;
    for (const auto& key : make_coordination_store(ha_dir_)->list("jobs")) {
        const std::filesystem::path p(key);
        if (p.filename() != "manifest.json")
            continue;
        try {
            ids.push_back(static_cast<JobId>(std::stoull(p.parent_path().filename().string())));
        } catch (...) {
        }
    }
    std::sort(ids.begin(), ids.end());
    if (ids.empty()) {
        return;
    }
    // Let worker registrations settle before redeploying. The supervisors of
    // a running cluster reconnect within moments of a new leader binding, but
    // not simultaneously; recovering at the first registration schedules
    // every task onto that one worker and the cluster never rebalances back.
    // Stop waiting once the registered set has been stable for
    // recovery_worker_settle (and at least one worker exists), and
    // unconditionally at recovery_worker_settle_deadline. A cluster with no
    // workers at the deadline still proceeds: the submit parks the job for
    // capacity and the retry loop finishes the recovery.
    if (cfg_.recovery_worker_settle > std::chrono::milliseconds::zero()) {
        const auto deadline =
            std::chrono::steady_clock::now() + cfg_.recovery_worker_settle_deadline;
        auto count_workers = [this] {
            std::lock_guard lock(mu_);
            std::size_t live = 0;
            for (const auto& [_, w] : registered_) {
                if (w && !w->lost) {
                    ++live;
                }
            }
            return live;
        };
        std::size_t stable_count = count_workers();
        auto stable_since = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() < deadline) {
            const auto now_count = count_workers();
            if (now_count != stable_count) {
                stable_count = now_count;
                stable_since = std::chrono::steady_clock::now();
            } else if (stable_count > 0 && std::chrono::steady_clock::now() - stable_since >=
                                               cfg_.recovery_worker_settle) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
        log::info("coordinator.ha",
                  "recovering " + std::to_string(ids.size()) + " persisted job(s) with " +
                      std::to_string(stable_count) + " worker(s) registered");
    }
    for (auto job_id : ids) {
        recover_one_persisted_job_(job_id);
    }
}

void Coordinator::recover_one_persisted_job_(JobId job_id) {
    // Skip if already in jobs_ (idempotent on repeated takeover, and the
    // guard that makes the capacity RETRY safe to run any number of
    // times: a recovery that succeeded between park and retry is a no-op
    // here, never a duplicate submission).
    {
        std::lock_guard lock(mu_);
        if (jobs_.count(job_id) != 0)
            return;
    }
    {
        const auto store = make_coordination_store(ha_dir_);
        const auto job_prefix = "jobs/" + std::to_string(job_id);
        // A tombstoned job reached a terminal state under a previous
        // leader; its manifest (if the deletion was interrupted) is
        // history, not work (item 69).
        if (store->exists(job_prefix + "/TERMINAL")) {
            return;
        }
        const auto manifest = store->get(job_prefix + "/manifest.json");
        if (!manifest.has_value())
            return;
        const std::string& body = *manifest;
        const auto graph_json = read_string_field(body, "graph_json");
        if (graph_json.empty())
            return;
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = read_string_field(body, "checkpoint_dir");
        ckpt.state_backend_uri = read_string_field(body, "state_backend_uri");
        ckpt.interval_ms = static_cast<std::int64_t>(read_uint_field(body, "interval_ms"));
        ckpt.restore_from_dir = read_string_field(body, "restore_from_dir");
        ckpt.restore_from_checkpoint_id = read_uint_field(body, "restore_from_checkpoint_id");
        ckpt.max_restarts_on_worker_loss =
            static_cast<std::uint32_t>(read_uint_field(body, "max_restarts_on_worker_loss"));
        // Pin the recovered job to the backend it ran with BEFORE submit_job
        // re-applies the cluster default: a job persisted with an empty URI
        // (legacy resolution via checkpoint_dir) must not be silently rebound
        // to a default configured after it was first submitted.
        pin_recovered_state_backend(ckpt);
        // For recovery, restore from this job's checkpoint dir at the latest
        // COMPLETED-N marker the previous leader managed to write. A job that
        // has completed no checkpoint of its own yet keeps the restore point it
        // was submitted with (a savepoint, a predecessor's checkpoint), or
        // starts fresh when it had none: the same rule a restart applies. It
        // used to take its own id 0 as the restore point, which the deploy
        // lint refuses, so the job was dropped at the takeover.
        //
        // The markers are read once, here, and everything the takeover reports
        // and does follows that read: the restore point, CoordRecovers'
        // completed point, the RestartProceeds gate and the walk's upper
        // bound. A second read of the COMPLETED markers after plugin loading
        // saw a superseded coordinator's put that landed in between, and the
        // takeover reported a completed point it then restored below. The
        // CONFIRMED marker is read first: a confirmation is written only once
        // its checkpoint's COMPLETED marker has landed, so the completed point
        // read after it is never below the confirmed one.
        //
        // A recovery parked for capacity, retried when a worker registers,
        // reads nothing: it deploys what the takeover decided (its record in
        // recovery_begun_). A superseded coordinator's put landing during the
        // park moved a fresh read past anything the takeover's lines named.
        const bool needs_confirmation =
            body.find("\"requires_commit_confirmation\":true") != std::string::npos;
        std::optional<TakeoverDecision> decided;
        {
            std::lock_guard begun_lock(recovery_begun_mu_);
            if (const auto it = recovery_begun_.find(job_id);
                it != recovery_begun_.end() && it->second.decided) {
                decided = it->second;
            }
        }
        std::uint64_t own_restore_point = 0;
        std::uint64_t completed_on_disk = 0;
        if (decided.has_value()) {
            own_restore_point = decided->own_restore_point;
            completed_on_disk = decided->completed;
            ckpt.restore_from_dir = decided->restore_from_dir;
            ckpt.restore_from_checkpoint_id = decided->restore_from_checkpoint_id;
        } else if (!ckpt.checkpoint_dir.empty()) {
            // Commit-confirmed restore protocol: a manifest flagged as
            // carrying a non-recoverable-commit sink restores from the
            // newest CONFIRMED checkpoint - a completed-but-unconfirmed one
            // may hold a broker transaction that died with the previous
            // leader's workers, and restoring past it loses that interval.
            const auto confirmed_on_disk =
                needs_confirmation ? latest_confirmed_id_on_disk(ckpt.checkpoint_dir, job_id) : 0;
            completed_on_disk = latest_completed_id_on_disk(ckpt.checkpoint_dir, job_id);
            const auto latest = needs_confirmation ? confirmed_on_disk : completed_on_disk;
            own_restore_point = latest;
            if (latest > 0) {
                ckpt.restore_from_dir = ckpt.checkpoint_dir;
                ckpt.restore_from_checkpoint_id = latest;
            } else if (ckpt.restore_from_dir.empty() || ckpt.restore_from_checkpoint_id == 0) {
                ckpt.restore_from_dir.clear();
                ckpt.restore_from_checkpoint_id = 0;
            }
        }
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorTakeoverAfterMarkerRead);
        // Plugins: scan plugin-*.so keys in this job's prefix, load each
        // into a fresh JobBundle.
        std::vector<PluginBinary> plugins;
        for (const auto& key : store->list(job_prefix)) {
            const auto name = std::filesystem::path(key).filename().string();
            if (name.rfind("plugin-", 0) != 0 || name.size() < 11)
                continue;
            const auto bytes = store->get(key);
            if (!bytes.has_value()) {
                continue;  // pruned between list and read; recovery stays conservative
            }
            PluginBinary blob;
            blob.bytes.assign(reinterpret_cast<const std::byte*>(bytes->data()),
                              reinterpret_cast<const std::byte*>(bytes->data()) + bytes->size());
            blob.content_hash = fnv1a_64_hex(blob.bytes);
            blob.name = name;
            plugins.push_back(std::move(blob));
        }
        auto bundle = std::make_unique<JobBundle>();
        auto bundle_preg = bundle->as_plugin_registry();
        bool plugins_ok = true;
        std::vector<std::string> plugin_so_paths;
        plugin_so_paths.reserve(plugins.size());
        for (const auto& p : plugins) {
            const auto path = write_plugin_to_cache(p);
            auto load_result = PluginLoader::default_instance().load_into(path, bundle_preg);
            if (!load_result.ok) {
                // Refusing is correct - mismatched bytes must never run, and
                // parking would wedge forever (persisted bytes cannot become
                // compatible). The failure mode to defend against is SILENCE:
                // after an engine upgrade that rotated the ABI surface, every
                // persisted job whose plugin predates it lands here, and the
                // operator's remedy (resubmit a rebuilt plugin) starts with
                // seeing it happen. Hence error level + a counter, with the
                // gate's own message naming what differs.
                log::error("coordinator.ha",
                           "job NOT recovered: plugin '" + p.name +
                               "' failed this build's load gate and the job needs a "
                               "resubmit with a rebuilt plugin: " +
                               load_result.error);
                clink::metrics::orch::ha_recovery_skipped_plugin_inc();
                plugins_ok = false;
                break;
            }
            bundle->retain_plugin(std::move(load_result.plugin));
            plugin_so_paths.push_back(path);
        }
        if (!plugins_ok)
            return;
        // In-doubt commit resolution, AFTER plugin load (the resolvers
        // register at connector install, which the plugin loads carry) and
        // BEFORE the restore point is used: a completed-but-unconfirmed
        // checkpoint whose orphaned transactions a resolver can finalise
        // gets CONFIRMED here, and the restore point advances past the
        // interval instead of replaying it. Any failure leaves the
        // commit-confirmed contract in force.
        {
            // The takeover is recorded, and walks, once per job per leadership:
            // a recovery parked for capacity comes back through here when a
            // worker registers, and that is the same takeover continuing, not
            // another. Its walk has run, and the retry deploys the restore
            // point the walk left (the decision recorded below); whatever the
            // walk could not confirm it would only probe again, a second round
            // of EndTxn calls on handles it refused, under Walk events the
            // trace has no step for after the takeover's walk finished.
            bool first_recovery = false;
            if (!decided.has_value()) {
                std::lock_guard begun_lock(recovery_begun_mu_);
                first_recovery = recovery_begun_.try_emplace(job_id).second;
            }
            if (first_recovery && protocol_trace::enabled()) {
                // `confirmed` is the confirmed restore point the takeover
                // adopts from the job's own directory: for the commit-confirmed
                // family, the newest CONFIRMED marker as the restore point
                // read it (own_restore_point), not a second read, which a
                // superseded coordinator's put landing in between would put
                // ahead of the restore. It is not the restore point itself
                // when the job has confirmed nothing of its own yet and keeps
                // the savepoint it was submitted with, whose id names another
                // directory: that is 0 here.
                protocol_trace::Event("CoordRecovers")
                    .u("job", job_id)
                    .u("epoch", epoch())
                    .u("completed", completed_on_disk)
                    .u("confirmed",
                       needs_confirmation
                           ? own_restore_point
                           : latest_confirmed_id_on_disk(ckpt.checkpoint_dir, job_id))
                    .emit();
            }
            if (needs_confirmation && !ckpt.checkpoint_dir.empty() && first_recovery) {
                if (protocol_trace::enabled() &&
                    completed_on_disk > ckpt.restore_from_checkpoint_id) {
                    protocol_trace::Event("RestartProceeds")
                        .u("job", job_id)
                        .b("resolving", true)
                        .u("completed", completed_on_disk)
                        .u("confirmed", ckpt.restore_from_checkpoint_id)
                        .emit();
                }
                // The walk runs to the completed point the takeover read and
                // reported (CoordRecovers, RestartProceeds), not to a fresh
                // read: a stale COMPLETED put (its run redeployed past, or its
                // coordinator superseded) can land in between, and the walk
                // then probed checkpoints above the range its own lines named,
                // or walked with no RestartProceeds at all.
                CLINK_FAULT_POINT(clink::fault::points::kCoordinatorTakeoverBeforeWalk);
                const auto resolved = resolve_in_doubt_commits(ckpt.checkpoint_dir,
                                                               job_id,
                                                               ckpt.restore_from_checkpoint_id,
                                                               completed_on_disk);
                if (resolved > ckpt.restore_from_checkpoint_id) {
                    log::info("coordinator.ha",
                              "job_id=" + std::to_string(job_id) +
                                  " restore point advanced by in-doubt resolution: checkpoint " +
                                  std::to_string(ckpt.restore_from_checkpoint_id) + " -> " +
                                  std::to_string(resolved));
                    // The walk confirms this job's own checkpoints, in its own
                    // directory, so that is where the deploy now restores from,
                    // whatever it would have restored from before: a submitted
                    // savepoint's directory holds another run's checkpoints, and
                    // a job with nothing of its own confirmed had no directory to
                    // restore from at all, which the deploy's lint refuses. The
                    // takeover's Redeploy reports the same point.
                    ckpt.restore_from_dir = ckpt.checkpoint_dir;
                    ckpt.restore_from_checkpoint_id = resolved;
                    own_restore_point = resolved;
                }
            }
            if (first_recovery) {
                std::lock_guard begun_lock(recovery_begun_mu_);
                recovery_begun_[job_id] = TakeoverDecision{
                    .decided = true,
                    .completed = completed_on_disk,
                    .own_restore_point = own_restore_point,
                    .restore_from_dir = ckpt.restore_from_dir,
                    .restore_from_checkpoint_id = ckpt.restore_from_checkpoint_id,
                };
            }
        }
        // Schema-evolution D: skip recovering a job whose persisted
        // savepoint can't migrate to the (possibly newer) binary's
        // expected versions. Best-effort, same contract as the submit gate.
        if (auto reject = check_restore_compatibility_via_plugins(
                plugin_so_paths, ckpt.restore_from_dir, ckpt.restore_from_checkpoint_id);
            !reject.empty()) {
            log::warn("coordinator.ha",
                      "recovery skipped for job_id=" + std::to_string(job_id) + ": " + reject);
            return;
        }
        // Start of the clink.recovery lifecycle span (recorded below with
        // the outcome; parked is a deliberate wait, not a failure).
        const std::uint64_t recovery_span_start = clink::metrics::SpanBuffer::global().enabled()
                                                      ? clink::metrics::otlp_now_unix_nano()
                                                      : 0;
        std::string recovery_outcome = "recovered";
        bool recovery_ok = true;
        try {
            const auto graph = JobGraphSpec::from_json(graph_json);
            // Use submit_job (creates a fresh JobState). Keep the
            // job_id alignment: advance next_job_id_ past this one so
            // the submit reuses it.
            {
                std::lock_guard lock(mu_);
                if (next_job_id_ <= job_id)
                    next_job_id_ = job_id;
            }
            // Numbers above every id the job has on record, even with nothing
            // of its own to restore (submit_job's id floor), and resumes beside
            // the completed point the takeover reported.
            ckpt.taken_over = true;
            ckpt.resumed_completed = completed_on_disk;
            const auto deployed = submit_job(graph,
                                             OperatorRegistry::default_instance(),
                                             std::move(plugins),
                                             ckpt,
                                             std::move(bundle),
                                             /*notify_client_conn=*/nullptr);
            // Recorded after the submit, so a recovery parked for capacity (the
            // submit throws InsufficientSlotsError below) has not redeployed: a
            // Redeploy for a parked attempt read as a second redeploy from the
            // running state when the retry then deployed for real.
            if (protocol_trace::enabled()) {
                // The id the deploy numbered from, as the engine chose it: a
                // floor recomputed here could read a directory that has moved
                // on since.
                std::uint64_t next = 0;
                {
                    std::lock_guard lock(mu_);
                    if (const auto it = jobs_.find(deployed); it != jobs_.end()) {
                        next = it->second->first_checkpoint_id;
                    }
                }
                // The job's own restore point, as in-doubt resolution left it:
                // the one the deploy restores from, unless that is a submitted
                // savepoint, whose id names another run's checkpoint, which
                // the trace does not.
                protocol_trace::Event("Redeploy")
                    .u("job", job_id)
                    .u("restore", own_restore_point)
                    .u("next", next)
                    .emit();
            }
            clink::metrics::orch::ha_recovered_jobs_inc();
            log::info("coordinator.ha",
                      "recovered job_id=" + std::to_string(job_id) +
                          " restore_from_ckpt=" + std::to_string(ckpt.restore_from_checkpoint_id));
        } catch (const InsufficientSlotsError& e) {
            recovery_outcome = "parked";
            // No worker had registered yet - the takeover raced the
            // supervisor restarting them. The job is intact on disk, so
            // PARK it and retry from the manifest when capacity appears;
            // dropping it here silently lost a running job until the next
            // failover. The failed submit consumed this attempt's plugins
            // and bundle, which is why the retry re-runs the whole
            // manifest recovery rather than reusing them.
            {
                std::lock_guard lock(mu_);
                if (std::find(pending_recovery_ids_.begin(), pending_recovery_ids_.end(), job_id) ==
                    pending_recovery_ids_.end()) {
                    pending_recovery_ids_.push_back(job_id);
                }
                // Not once stop() has begun, for the reason the in-doubt
                // resolution thread is not (stage_in_doubt_resolution_locked_).
                if (!recovery_retry_thread_.joinable() && !stop_.load(std::memory_order_acquire)) {
                    recovery_retry_thread_ = std::thread([this] { recovery_retry_loop_(); });
                }
            }
            log::warn("coordinator.ha",
                      "recovery of job_id=" + std::to_string(job_id) + " parked for capacity (" +
                          e.what() + "); it retries when a worker registers");
        } catch (const std::exception& e) {
            recovery_outcome = "failed";
            recovery_ok = false;
            // The job is not running and no later takeover will run it either:
            // its manifest still describes the same refusal.
            clink::metrics::orch::ha_recovery_failed_inc();
            log::error("coordinator.ha",
                       "recovery failed for job_id=" + std::to_string(job_id) +
                           "; the job is NOT running and needs a resubmit: " + e.what());
        }
        if (recovery_span_start != 0 && clink::metrics::SpanBuffer::global().enabled()) {
            clink::metrics::OtlpSpan span;
            span.name = "clink.recovery";
            span.start_unix_nano = recovery_span_start;
            span.end_unix_nano = clink::metrics::otlp_now_unix_nano();
            span.ok = recovery_ok;
            span.attributes = {
                {"clink.job_id", std::to_string(job_id)},
                {"clink.outcome", recovery_outcome},
                {"clink.restore_from_checkpoint", std::to_string(ckpt.restore_from_checkpoint_id)}};
            clink::metrics::SpanBuffer::global().record(std::move(span));
        }
    }
}

void Coordinator::recovery_retry_loop_() {
    std::unique_lock lock(mu_);
    while (!stop_.load(std::memory_order_acquire)) {
        // Paced by notify (worker registration, stop) with a periodic
        // re-check; no predicate, because "parked but zero slots" must
        // WAIT here rather than spin through an always-true predicate.
        cv_.wait_for(lock, std::chrono::seconds{2});
        if (stop_.load(std::memory_order_acquire)) {
            break;
        }
        if (pending_recovery_ids_.empty()) {
            continue;
        }
        std::size_t free = 0;
        for (const auto& [_, w] : registered_) {
            // Guarded: a session can be charged past its capacity (a retry
            // moved onto a successor that was already full), and the unsigned
            // difference would then read as some four billion free slots.
            if (!w->lost && w->slots_in_use < w->slot_capacity) {
                free += (w->slot_capacity - w->slots_in_use);
            }
        }
        if (free == 0) {
            continue;  // a retry is a guaranteed refusal; wait for a register
        }
        auto ids = std::move(pending_recovery_ids_);
        pending_recovery_ids_.clear();
        lock.unlock();
        for (const auto id : ids) {
            // Re-parks itself on a fresh capacity refusal; the submit's own
            // bounded slot wait paces repeated attempts.
            recover_one_persisted_job_(id);
        }
        lock.lock();
    }
}

bool Coordinator::restart_drain_covered_(const JobState& job) {
    for (const auto& expected : job.restart_drain_expected) {
        if (job.restart_drained_keys.count(expected) == 0) {
            return false;
        }
    }
    return true;
}

std::uint64_t Coordinator::completed_point_(const JobState& job) {
    return std::max(job.latest_completed_checkpoint_id, job.checkpoint.resumed_completed);
}

bool Coordinator::stage_in_doubt_resolution_locked_(JobState& job) {
    if (job.resolving_in_doubt) {
        return true;  // in flight; the resolution thread fires the restart
    }
    if (job.in_doubt_walked) {
        return false;  // this restart's walk has answered; it waits only to redeploy
    }
    // Only tracked jobs (a non-recoverable-commit sink in the plan), only
    // when a completed-but-unconfirmed gap actually exists, and only with a
    // checkpoint dir to read the staged handles from. Everything else
    // restarts immediately, exactly as before.
    //
    // A job that resumed (a takeover, a tracked run's resume) has completed
    // nothing in memory until its first checkpoint lands, and the completed
    // point it resumed beside still counts: a gap its takeover's walk left
    // open is still open, and the specification's restart walks it again.
    // Without it the restart redeployed straight from the confirmed point.
    const auto completed = completed_point_(job);
    if (job.confirm_task_keys.empty() || job.checkpoint.checkpoint_dir.empty() || completed == 0 ||
        job.latest_confirmed_checkpoint_id >= completed) {
        return false;
    }
    job.resolving_in_doubt = true;
    job.in_doubt_walk_from = job.latest_confirmed_checkpoint_id;
    if (protocol_trace::enabled()) {
        protocol_trace::Event("RestartProceeds")
            .u("job", job.id)
            .b("resolving", true)
            .u("completed", completed)
            .u("confirmed", job.latest_confirmed_checkpoint_id)
            .emit();
    }
    job.in_doubt_cancel = std::make_shared<std::atomic<bool>>(false);
    job.in_doubt_cancel_requested = false;
    // A fresh hold is a fresh episode for the capacity clock: the deadline
    // measures CONSECUTIVE capacity starvation, and time spent resolving
    // in-doubt commits is work, not starvation. Left ticking across holds,
    // it expired mid-resolution and failed a job whose workers were back
    // and whose restart machinery was mid-flight ("capacity never
    // returned" with capacity long since returned).
    job.restart_capacity_deadline = {};
    // The drain is complete; the deadline now guards the RESOLUTION. It is
    // a SOFT deadline: the walk's own wire budget is NOT "far below 90s" -
    // five retry rounds over several handles at three 5s-timeout connect
    // attempts each legitimately runs past two minutes when the broker is
    // slow or out (the rig-night composite measured 117s against a healthy
    // but stalling broker). Hitting the deadline therefore CANCELS the walk
    // (it stops mutating and returns; the restart proceeds on the bounded
    // contract) rather than failing the job; only the hard grace after the
    // cancel treats a walk that never returned as hung.
    job.restart_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{90};
    pending_in_doubt_resolutions_.push_back(job.id);
    // Never once stop() has begun: it takes the handle under mu_ to join it,
    // and a handler still in flight on a worker's dispatch thread can stage a
    // resolution after that. A thread started then would outlive the join and
    // the Coordinator. stop_ is set before stop() takes mu_, so a stage after
    // that hold sees it.
    if (!in_doubt_resolution_thread_.joinable() && !stop_.load(std::memory_order_acquire)) {
        in_doubt_resolution_thread_ = std::thread([this] { in_doubt_resolution_loop_(); });
    }
    log::info(
        "coordinator.restart",
        "job_id=" + std::to_string(job.id) +
            " restart held for in-doubt resolution (latest_completed=" + std::to_string(completed) +
            " latest_confirmed=" + std::to_string(job.latest_confirmed_checkpoint_id) +
            "); nothing deploys until the broker answers");
    cv_.notify_all();
    return true;
}

void Coordinator::in_doubt_resolution_loop_() {
    std::unique_lock lock(mu_);
    while (!stop_.load(std::memory_order_acquire)) {
        if (pending_in_doubt_resolutions_.empty()) {
            cv_.wait_for(lock, std::chrono::seconds{2});
            continue;
        }
        auto ids = std::move(pending_in_doubt_resolutions_);
        pending_in_doubt_resolutions_.clear();
        for (const auto id : ids) {
            lock.unlock();
            CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeInDoubtWalk);
            lock.lock();
            std::string dir;
            std::uint64_t confirmed = 0;
            std::uint64_t completed = 0;
            std::shared_ptr<std::atomic<bool>> cancel;
            {
                auto it = jobs_.find(id);
                if (it == jobs_.end() || !it->second->resolving_in_doubt) {
                    continue;  // failed, cancelled, or gone while queued
                }
                dir = it->second->checkpoint.checkpoint_dir;
                // Where the stage said the walk starts, not where memory is
                // now (JobState::in_doubt_walk_from).
                confirmed = it->second->in_doubt_walk_from;
                completed = completed_point_(*it->second);
                cancel = it->second->in_doubt_cancel;
            }
            lock.unlock();
            // The broker round-trips, off mu_. No task of this job is
            // deployed while it runs, so nothing can fence the orphan
            // between this answer and the restore point that consumes it.
            const auto resolved = resolve_in_doubt_commits(
                dir, id, confirmed, completed, std::chrono::seconds{2}, cancel.get());
            lock.lock();
            auto it = jobs_.find(id);
            if (it == jobs_.end()) {
                continue;
            }
            auto& job = *it->second;
            if (!job.resolving_in_doubt) {
                continue;  // the watchdog backstop failed the job meanwhile
            }
            job.resolving_in_doubt = false;
            job.in_doubt_walked = true;
            // The resolution deadline is spent (soft-cancelled or not); the
            // deferred restart below may enter a capacity wait, which runs
            // on its own deadline.
            job.restart_deadline = {};
            if (resolved > job.latest_confirmed_checkpoint_id) {
                log::info("coordinator.restart",
                          "job_id=" + std::to_string(id) +
                              " restore point advanced by in-doubt resolution: checkpoint " +
                              std::to_string(job.latest_confirmed_checkpoint_id) + " -> " +
                              std::to_string(resolved));
                job.latest_confirmed_checkpoint_id = resolved;
            }
            std::vector<PendingDeploy> deploys;
            if (job.awaiting_restart && !job.completion_signalled && !job.cancel_requested &&
                restart_drain_covered_(job)) {
                deploys = restart_job_locked_(job);
            }
            lock.unlock();
            for (auto& d : deploys) {
                if (d.conn) {
                    send_frame(*d.conn, d.frame);
                }
            }
            lock.lock();
        }
    }
}

void Coordinator::reload_history_from_disk_() {
    if (ha_dir_.empty())
        return;
    const auto store = make_coordination_store(ha_dir_);
    std::vector<CompletedJobRecord> records;
    for (const auto& key : store->list("history")) {
        if (std::filesystem::path(key).extension() != ".json")
            continue;
        const auto body_opt = store->get(key);
        if (!body_opt.has_value())
            continue;
        const std::string& body = *body_opt;
        CompletedJobRecord rec;
        rec.job_id = static_cast<JobId>(read_uint_field(body, "job_id"));
        if (rec.job_id == 0)
            continue;
        rec.status = read_string_field(body, "status");
        rec.errors = read_string_array_field(body, "errors");
        rec.restart_attempts =
            static_cast<std::uint32_t>(read_uint_field(body, "restart_attempts"));
        rec.latest_completed_checkpoint_id =
            read_uint_field(body, "latest_completed_checkpoint_id");
        rec.duration_ms =
            std::chrono::milliseconds{static_cast<long long>(read_uint_field(body, "duration_ms"))};
        rec.completed_at_unix_seconds =
            static_cast<std::int64_t>(read_uint_field(body, "completed_at_unix_seconds"));
        records.push_back(std::move(rec));
    }
    // Sort oldest-first so the bounded ring keeps the most recent
    // entries (matches the eviction policy used by completion).
    std::sort(records.begin(),
              records.end(),
              [](const CompletedJobRecord& a, const CompletedJobRecord& b) {
                  if (a.completed_at_unix_seconds != b.completed_at_unix_seconds)
                      return a.completed_at_unix_seconds < b.completed_at_unix_seconds;
                  return a.job_id < b.job_id;
              });
    std::lock_guard lock(mu_);
    for (auto& r : records) {
        history_.push_back(std::move(r));
        while (history_.size() > kCoordinatorHistoryCap) {
            history_.pop_front();
        }
    }
}

Coordinator::~Coordinator() {
    stop();
}

std::uint16_t Coordinator::start(std::uint16_t port) {
    protocol_trace::set_process_role("coordinator");
    listener_fd_ = network::NetworkSocket::listen_on(port, cfg_.bind_host);
    if (listener_fd_ < 0) {
        throw std::runtime_error("Coordinator::start: listen failed");
    }
    bound_port_ = port;
    if (cfg_.advertise_host.empty()) {
        cfg_.advertise_host = cfg_.bind_host;
    }
    accept_wake_ = std::make_unique<network::AcceptWake>(listener_fd_);
    accept_thread_ = std::thread([this] { accept_loop_(); });
    watchdog_thread_ = std::thread([this] { watchdog_loop_(); });
    checkpoint_thread_ = std::thread([this] { checkpoint_trigger_loop_(); });
    return bound_port_;
}

void Coordinator::accept_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAcceptBeforeWait);
        // Close any admission past its deadline, and wake in time for the
        // next one: this thread is the admission deadlines' clock.
        const auto next_deadline = police_admissions_();
        if (std::chrono::steady_clock::now() < accept_retry_at_) {
            // accept() failed for want of a descriptor or of memory, and left
            // the connection pending: the listener stays readable, so waiting
            // on it would return at once and accept() would fail again, a
            // spin on this core until a descriptor frees. Wait on the stop
            // wake alone; the connection keeps its place in the backlog.
            CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAcceptBackoff);
            const auto until = std::min(next_deadline, accept_retry_at_);
            if (network::NetworkSocket::wait_for_wake(*accept_wake_, until)) {
                return;
            }
            if (std::chrono::steady_clock::now() < until) {
                // The wait could not be had (poll() failed, or the wake is
                // unusable) and returned early: end the backoff, so the wait
                // on the listener below reports it rather than this spinning.
                accept_retry_at_ = {};
            }
            continue;
        }
        // Wait here, where stop() can wake us, and never in a blocking
        // accept(): stop() joins this thread before it closes the listener
        // (see stop()). The wait leaves the listener non-blocking, so the
        // accept that follows returns at once even if the connection it saw
        // has gone, and the loop comes back here.
        if (!network::NetworkSocket::wait_for_connection(
                listener_fd_, *accept_wake_, next_deadline)) {
            const int err = errno;
            if (stop_.load(std::memory_order_acquire) || err == ECANCELED) {
                return;
            }
            if (err == ETIMEDOUT) {
                continue;  // an admission deadline is due
            }
            // poll() itself failing (ENOMEM, say) is not something a retry
            // here can outlast, and spinning on it helps nobody.
            log::error("coordinator.accept",
                       std::string{"no longer accepting connections: waiting for one failed: "} +
                           std::strerror(err));
            return;
        }
        if (stop_.load(std::memory_order_acquire)) {
            return;
        }
        if (listener_accept_factory_) {
            // A factory in its listener form does the accept, and any
            // handshake, here on this thread (see set_accept_factory); nullptr
            // when the connection went before it was accepted. On a handshake
            // failure it throws; catch so one bad client can't kill the loop.
            std::unique_ptr<network::Connection> conn;
            try {
                conn = listener_accept_factory_(listener_fd_);
            } catch (const std::exception& e) {
                if (stop_.load(std::memory_order_acquire)) {
                    return;  // stop() woke us between the wait and the accept
                }
                // The factory throws for an accept() that failed as well as
                // for a handshake that did, and errno after the throw is not
                // to be relied on. Out of descriptors is the failure that
                // would spin (the connection stays in the backlog, so the
                // listener stays readable), so look for a free one: none
                // means back off as for the accept below.
                if (const int probe = ::fcntl(listener_fd_, F_DUPFD_CLOEXEC, 0); probe >= 0) {
                    network::NetworkSocket::close(probe);
                } else if (const int err = errno; err == EMFILE || err == ENFILE) {
                    note_accept_exhausted_(err);
                    continue;
                }
                // One count per connection, and a line unless a flood is
                // being summarised: the factory has closed it.
                metrics::orch::coordinator_handshake_failed();
                log_admission_drop_({}, std::string{"connection rejected: "} + e.what());
                continue;
            }
            if (!conn) {
                continue;  // transient: the peer disappeared before the accept
            }
            note_accept_resumed_();
            (void)begin_admission_guarded_(-1, std::move(conn));
            continue;
        }
        // Accept here and nothing more: everything a connection does before
        // it is admitted (its handshake, its first frame) runs on a thread of
        // its own, so a client that stalls any of it holds up no other.
        int fd = -1;
        if (const auto injected = CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAcceptOne);
            injected.fired && injected.action == clink::fault::Action::Error) {
            errno = injected.arg != 0 ? static_cast<int>(injected.arg) : EMFILE;
        } else {
            fd = network::NetworkSocket::accept_one(listener_fd_);
        }
        if (fd < 0) {
            const int err = errno;
            if (err == EMFILE || err == ENFILE || err == ENOBUFS || err == ENOMEM) {
                note_accept_exhausted_(err);
            }
            // Otherwise the connection went before the accept (EAGAIN), or a
            // failure the next accept can get past.
            continue;
        }
        if (const int err = begin_admission_guarded_(fd, nullptr); err != 0) {
            // accept() had a descriptor for the connection but none was left
            // for its interrupt duplicate, so it is closed rather than left
            // in the backlog: back off as for accept() itself, and keep the
            // episode the accepts before it began.
            note_accept_exhausted_(err);
            continue;
        }
        note_accept_resumed_();
    }
}

void Coordinator::note_accept_resumed_() noexcept {
    if (accept_exhausted_failures_ == 0) {
        return;
    }
    try {
        log::info("coordinator.accept",
                  "accepting connections again after " +
                      std::to_string(accept_exhausted_failures_) +
                      " accept() failures for want of descriptors or memory");
    } catch (...) {
        // Logging failed: accepting has resumed all the same.
    }
    accept_exhausted_failures_ = 0;
    accept_backoff_ = std::chrono::milliseconds{0};
    accept_retry_at_ = {};
}

void Coordinator::note_accept_exhausted_(int err) noexcept {
    // From 10 ms, doubling, to a second: a descriptor freed by an admission
    // ending is picked up quickly, and a process that stays out of them costs
    // this thread one accept() a second.
    constexpr std::chrono::milliseconds kFirst{10};
    constexpr std::chrono::milliseconds kMost{1000};
    accept_backoff_ = accept_backoff_.count() == 0 ? kFirst : std::min(accept_backoff_ * 2, kMost);
    accept_retry_at_ = network::NetworkSocket::deadline_after(accept_backoff_);
    if (accept_exhausted_failures_++ == 0) {
        // Once per episode; the line when accepting resumes gives the count.
        try {
            log::error("coordinator.accept",
                       std::string{"cannot accept connections: "} + std::strerror(err) +
                           "; they wait in the listen backlog, and accept() is retried with a "
                           "backoff of up to a second until it succeeds. Each connection being "
                           "admitted holds two descriptors, so raise the descriptor limit "
                           "(ulimit -n) or lower max_pending_connections, "
                           "max_worker_connections or max_client_connections");
        } catch (...) {
            // Logging failed (out of memory, say): the backoff still stands.
        }
    }
}

int Coordinator::begin_admission_guarded_(int fd, std::unique_ptr<network::Connection> conn) {
    // An allocation failing while one connection is tracked must not end the
    // accept thread, and with it every later registration: begin_admission_
    // has closed what it had not handed on, so drop this one and carry on.
    try {
        return begin_admission_(fd, std::move(conn));
    } catch (const std::exception& e) {
        log_admission_drop_(
            {}, std::string{"connection rejected: cannot track its admission: "} + e.what());
    }
    return 0;
}

void Coordinator::log_admission_drop_(const std::string& source, std::string message) noexcept {
    try {
        auto verdict = admission_log_->note(std::chrono::steady_clock::now(), source);
        if (!verdict.summary.empty()) {
            log::warn("coordinator.accept", std::move(verdict.summary));
        }
        if (verdict.log) {
            log::warn("coordinator.accept", std::move(message));
        }
    } catch (...) {
        // Logging failed (out of memory, say): the drop still stands.
    }
}

void Coordinator::interrupt_admission_locked_(PendingAdmission& pending,
                                              std::string why,
                                              bool queued_too) {
    // Once Queued, its first frame is in and handle_first_frame_ decides what
    // becomes of it: the deadline and eviction leave it alone. stop() does
    // not, since a reply handle_first_frame_ sends to a peer that never reads
    // would otherwise keep stop() waiting to join its thread.
    if (!pending.interrupted.empty() ||
        (pending.progress == AdmissionProgress::Queued && !queued_too)) {
        return;
    }
    pending.interrupted = std::move(why);
    // Fails whatever the admission thread is waiting in. Neither frees
    // anything, so both are safe while that thread is inside its read.
    if (pending.interrupt_fd >= 0) {
        network::NetworkSocket::shutdown_read(pending.interrupt_fd);
        network::NetworkSocket::shutdown_write(pending.interrupt_fd);
        // Its one use is spent (an admission is interrupted once), so it
        // need not stay open for as long as the thread takes to notice.
        network::NetworkSocket::close(pending.interrupt_fd);
        pending.interrupt_fd = -1;
    } else if (pending.conn != nullptr) {
        pending.conn->shutdown_read();
    }
}

std::chrono::steady_clock::time_point Coordinator::police_admissions_() {
    // Join what has finished, so the list is bounded by the connections in
    // flight rather than by every connection ever accepted.
    std::erase_if(admission_threads_, [](AdmissionThread& t) {
        if (!t.finished->load(std::memory_order_acquire)) {
            return false;
        }
        t.thread.join();
        return true;
    });
    const auto now = std::chrono::steady_clock::now();
    // A flood that has stopped still gets its summary line (see
    // AdmissionLogLimiter): this thread is the clock that gives it.
    if (auto summary = admission_log_->flush(now); !summary.empty()) {
        log::warn("coordinator.accept", std::move(summary));
    }
    auto earliest = admission_log_->flush_due();
    std::lock_guard lock(admission_mu_);
    for (auto& [_, pending] : pending_admissions_) {
        if (!pending.interrupted.empty() || pending.progress == AdmissionProgress::Queued) {
            continue;  // on its way out, or its first frame is in
        }
        if (now >= pending.deadline) {
            interrupt_admission_locked_(pending, "not admitted within its admission deadline");
        } else {
            earliest = std::min(earliest, pending.deadline);
        }
    }
    return earliest;
}

int Coordinator::begin_admission_(int fd, std::unique_ptr<network::Connection> conn) {
    // This function's until the admission thread starts: every throw before
    // that (joining finished threads, building the source key, the
    // bookkeeping) closes it on the way out. conn, for a listener-form
    // connection, is owned the same way.
    OwnedFd owned{fd};
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAdmissionBeforeTrack);
    (void)police_admissions_();
    // From the accept: the handshake's own bound, then heartbeat_timeout for
    // the first frame, which is the coordinator's own definition of a live
    // peer (see handle_first_frame_). Saturating, so a timeout meant as "no
    // limit" is no deadline rather than an overflow into the past.
    auto deadline = std::chrono::steady_clock::time_point::max();
    if (cfg_.heartbeat_timeout.count() > 0) {
        const auto handshake = std::max(cfg_.handshake_timeout, std::chrono::milliseconds{0});
        const auto total = handshake >= std::chrono::milliseconds::max() - cfg_.heartbeat_timeout
                               ? std::chrono::milliseconds::max()
                               : handshake + cfg_.heartbeat_timeout;
        deadline = network::NetworkSocket::deadline_after(total);
    }
    PendingAdmission pending{
        .conn = conn.get(),
        .deadline = deadline,
        .source = fd >= 0 ? admission_source_key(fd) : std::string{},
        // A listener-form factory has done its handshake already.
        .progress = fd >= 0 ? AdmissionProgress::Accepted : AdmissionProgress::Handshaken};
    if (fd >= 0) {
        // A duplicate, so the socket can be shut down from another thread
        // however the factory has dealt with its own descriptor.
        pending.interrupt_fd = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (pending.interrupt_fd < 0) {
            const int err = errno;
            log_admission_drop_(pending.source,
                                std::string{"connection rejected: cannot track its admission: "} +
                                    std::strerror(err));
            // owned closes fd
            return err == EMFILE || err == ENFILE ? err : 0;
        }
    }
    // Owns the connection, if any, until its thread has taken it: if the
    // thread cannot start, the pending entry (which a listener-form
    // connection is reachable from) goes before the connection does.
    // Allocated before the entry is published, so nothing between publishing
    // it and starting the thread can fail but the start itself.
    std::shared_ptr<std::unique_ptr<network::Connection>> handoff;
    std::shared_ptr<std::atomic<bool>> finished;
    std::uint64_t id = 0;
    std::string refused;
    // For the admission thread's log lines; pending.source moves into the
    // entry.
    std::string source;
    try {
        handoff = std::make_shared<std::unique_ptr<network::Connection>>(std::move(conn));
        finished = std::make_shared<std::atomic<bool>>(false);
        source = pending.source;
        std::lock_guard lock(admission_mu_);
        // stop() interrupts every admission under this lock after it sets
        // stop_, so one that starts after that would be missed: refuse it.
        if (stop_.load(std::memory_order_acquire)) {
            network::NetworkSocket::close(pending.interrupt_fd);
            return 0;  // owned closes fd; conn, if any, closes on the way out
        }
        // Every admission still tracked, interrupted ones included: an
        // interrupted admission is on its way out, but its thread and its
        // socket stay until that thread notices, and one whose first frame is
        // in holds its thread and its frame until handle_first_frame_ has
        // taken it. decide_admission bounds them all (admission_policy.hpp).
        const std::size_t cap = std::max<std::size_t>(cfg_.max_pending_connections, 1);
        std::vector<AdmissionCandidate> tracked;
        tracked.reserve(pending_admissions_.size());
        for (const auto& [pid, p] : pending_admissions_) {
            tracked.push_back(AdmissionCandidate{.id = pid,
                                                 .source = p.source,
                                                 .progress = p.progress,
                                                 .interrupted = !p.interrupted.empty()});
        }
        const auto decision = decide_admission(tracked, pending.source, cap, pending.progress);
        const std::string limit =
            " (max_pending_connections=" + std::to_string(cfg_.max_pending_connections) + ")";
        switch (decision.kind) {
            case AdmissionDecision::Kind::Admit:
                break;
            case AdmissionDecision::Kind::Evict:
                // One eviction per new connection: a full set stays full, and
                // each newcomer either takes a slot or is turned away.
                interrupt_admission_locked_(pending_admissions_.at(decision.victim),
                                            "evicted to admit a newer connection" + limit);
                break;
            case AdmissionDecision::Kind::Refuse:
                if (!decision.at_ceiling) {
                    refused =
                        "no admission can make way for it: none is from a busier address, every "
                        "one from an address as busy as its own is further along, and its own "
                        "address has a connection still waiting on its peer" +
                        limit;
                } else if (tracked.size() >= admission_hard_limit(cap)) {
                    refused =
                        "three times max_pending_connections are still being admitted or "
                        "letting go after being interrupted" +
                        limit;
                } else {
                    refused =
                        "twice max_pending_connections are still being admitted or letting go "
                        "after being interrupted, and this address already holds its share of "
                        "them" +
                        limit;
                }
                break;
        }
        if (refused.empty()) {
            id = next_admission_id_++;
            pending_admissions_.emplace(id, std::move(pending));
        }
    } catch (...) {
        // Nothing published (emplace is all or nothing): the descriptors are
        // still this function's to close (owned closes fd). The accept loop
        // logs it.
        network::NetworkSocket::close(pending.interrupt_fd);
        throw;
    }
    if (!refused.empty()) {
        network::NetworkSocket::close(pending.interrupt_fd);
        metrics::orch::coordinator_handshake_failed();
        log_admission_drop_(source, "connection refused: " + refused);
        return 0;  // owned closes fd; conn, if any, closes on the way out
    }
    std::thread thread;
    try {
        // Room first: a push_back that failed after the thread started would
        // destroy a joinable std::thread.
        admission_threads_.reserve(admission_threads_.size() + 1);
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAdmissionBeforeThreadStart);
        thread = std::thread([this, id, fd, finished, handoff, source]() {
            admit_connection_(id, fd, std::move(*handoff), source);
            // Last act: police_admissions_ reads this to join the thread
            // without blocking on one still at work.
            finished->store(true, std::memory_order_release);
        });
        owned.release();  // the admission thread's now
    } catch (const std::exception& e) {
        // Out of threads, typically. The entry first, then the connection
        // (owned closes fd on the way out).
        (void)end_admission_(id);
        handoff->reset();
        log_admission_drop_(
            source, std::string{"connection rejected: cannot start its admission: "} + e.what());
        return 0;
    }
    admission_threads_.push_back(
        AdmissionThread{.thread = std::move(thread), .finished = finished});
    return 0;
}

std::string Coordinator::end_admission_(std::uint64_t id) {
    std::lock_guard lock(admission_mu_);
    const auto it = pending_admissions_.find(id);
    if (it == pending_admissions_.end()) {
        return {};
    }
    std::string why = std::move(it->second.interrupted);
    network::NetworkSocket::close(it->second.interrupt_fd);
    pending_admissions_.erase(it);
    return why;
}

std::string Coordinator::advance_admission_(std::uint64_t id, AdmissionProgress progress) {
    std::lock_guard lock(admission_mu_);
    const auto it = pending_admissions_.find(id);
    if (it == pending_admissions_.end()) {
        return "no longer tracked";
    }
    auto& pending = it->second;
    if (!pending.interrupted.empty()) {
        return pending.interrupted;
    }
    pending.progress = progress;
    if (progress == AdmissionProgress::Queued) {
        // Out of reach of the deadline and eviction from here, though not of
        // stop() (see interrupt_admission_locked_), which shuts the socket
        // down through interrupt_fd: that stays until end_admission_. A
        // listener-form connection may be handed by handle_first_frame_ to a
        // session that outlives this entry, so it is no longer reachable
        // from here; its replies are bounded by send_admission_reply_.
        pending.conn = nullptr;
    }
    return {};
}

bool Coordinator::send_admission_reply_(network::Connection& conn,
                                        const std::vector<std::byte>& frame) {
    // Each reply here is small (no peer-supplied string in it is longer than
    // kMaxRegisterStringBytes), and a peer waiting for it reads all it is
    // sent, so it goes into the socket's send buffer in one send() at once.
    // A peer that does not read can fill that buffer all the same: over TLS,
    // OpenSSL writes from inside the reads of the first frame (an alert for
    // each renegotiation it refuses, a reply to a TLS 1.3 KeyUpdate), so a
    // client can fill it before it sends its Register. The caller holds the
    // first-frame lock, and mu_ for the RegisterAck, so the bound is short
    // and fixed rather than heartbeat_timeout: it is how long such a peer
    // holds them, each time.
    const auto bound = cfg_.heartbeat_timeout.count() > 0
                           ? std::min(cfg_.heartbeat_timeout, kAdmissionReplySendBound)
                           : kAdmissionReplySendBound;
    const bool bounded = conn.set_send_timeout(bound);
    const bool sent = send_frame(conn, frame);
    if (bounded) {
        (void)conn.set_send_timeout(std::chrono::milliseconds{0});
    }
    return sent;
}

void Coordinator::admit_connection_(std::uint64_t id,
                                    int fd,
                                    std::unique_ptr<network::Connection> conn,
                                    const std::string& source) {
    // Nothing may leave this thread function: an exception escaping it is
    // std::terminate, which would take the whole control plane down for one
    // connection, before any authentication. The accept loop used to read
    // first frames inside a catch for this reason; a std::bad_alloc while a
    // frame grows, or a throw from a custom Connection, now lands here.
    try {
        admit_connection_unguarded_(id, fd, conn, source);
    } catch (const std::exception& e) {
        const auto why = end_admission_(id);
        metrics::orch::malformed_frame();
        try {
            log_admission_drop_(source,
                                std::string{"dropping a connection whose admission failed: "} +
                                    (why.empty() ? e.what() : why));
        } catch (...) {
            // Building the line failed too (out of memory, say): the drop
            // still stands.
        }
    } catch (...) {
        (void)end_admission_(id);
        metrics::orch::malformed_frame();
        log_admission_drop_(source,
                            "dropping a connection whose admission failed: unknown exception");
    }
    // Every path out: the entry goes before the connection (a listener-form
    // connection is reachable from it until then). Idempotent.
    (void)end_admission_(id);
    conn.reset();
}

void Coordinator::admit_connection_unguarded_(std::uint64_t id,
                                              int fd,
                                              std::unique_ptr<network::Connection>& conn,
                                              const std::string& source) {
    if (conn == nullptr) {
        // The handshake, bounded three ways: by handshake_timeout inside the
        // factory, by the admission deadline and eviction (both shut the
        // socket down under it), and by stop(), which does too.
        try {
            conn = accept_factory_(AcceptRequest{
                .fd = fd, .wake = accept_wake_.get(), .handshake_timeout = cfg_.handshake_timeout});
        } catch (const std::exception& e) {
            const auto why = end_admission_(id);
            if (stop_.load(std::memory_order_acquire)) {
                return;  // stop() ended it
            }
            // One count per connection, and a line unless a flood is being
            // summarised: the factory has closed it.
            metrics::orch::coordinator_handshake_failed();
            log_admission_drop_(
                source, std::string{"connection rejected: "} + (why.empty() ? e.what() : why));
            return;
        }
        if (conn == nullptr) {
            return;
        }
        if (accept_factory_handshakes_) {
            (void)advance_admission_(id, AdmissionProgress::Handshaken);
        }
    }
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAdmissionBeforeFirstFrame);
    // Bound each read as well as the whole admission: a peer that sends
    // nothing is dropped at heartbeat_timeout without waiting for the
    // admission deadline. The deadline is what stops a peer that sends a
    // byte at a time, each inside this bound.
    //
    // Under kMaxFirstFrameBytes, not the 256 MiB a plugin-carrying frame may
    // need later: nothing in a first frame is large, and nothing has been
    // authenticated yet.
    (void)conn->set_recv_timeout(cfg_.heartbeat_timeout);
    std::optional<std::vector<std::byte>> frame;
    if (const auto len = read_frame_length(*conn); len.has_value()) {
        if (*len <= kMaxFirstFrameBytes) {
            (void)advance_admission_(id, AdmissionProgress::FrameStarted);
        }
        frame = read_frame_body(*conn, *len, kMaxFirstFrameBytes);
    }
    // Cleared before the connection is handed to a thread of its own, where a
    // blocking read is correct: a client holds an idle connection open between
    // commands, and a worker between heartbeats.
    (void)conn->set_recv_timeout(std::chrono::milliseconds{0});
    if (!frame.has_value()) {
        const auto why = end_admission_(id);
        if (!why.empty() && !stop_.load(std::memory_order_acquire)) {
            log_admission_drop_(source, "dropping a connection before its first frame: " + why);
        }
        return;  // conn destructor closes
    }
    // Out of reach of the deadline, eviction and stop() from here, but still
    // counted against max_pending_connections until handle_first_frame_ has
    // taken it, so the threads waiting below are bounded too.
    if (const auto why = advance_admission_(id, AdmissionProgress::Queued); !why.empty()) {
        (void)end_admission_(id);
        if (!stop_.load(std::memory_order_acquire)) {
            log_admission_drop_(source, "dropping a connection before its first frame: " + why);
        }
        return;  // interrupted, so not to be trusted even if the frame did arrive
    }
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAdmissionQueued);
    // One at a time. Not in accept order: handle_register_ refuses a
    // registration older than the one on record for its worker instead.
    std::lock_guard lock(first_frame_mu_);
    // stop() is tearing down what an admission would join.
    if (stop_.load(std::memory_order_acquire)) {
        return;
    }
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAdmissionBeforeHandle);
    // A decoder throwing must not take the coordinator with it.
    //
    // MessageReader throws BY DESIGN on a truncated or malformed
    // payload - there is a test for it - and nothing caught it. The
    // throw propagated out of the accept thread function, which is
    // std::terminate: one malformed frame from anything that could
    // reach the control port killed the whole control plane, before
    // any authentication. Dropping the connection is the correct
    // response; the peer's framing cannot be trusted after this.
    try {
        (void)handle_first_frame_(std::move(conn), std::move(*frame), id);
    } catch (const std::exception& e) {
        metrics::orch::malformed_frame();
        log_admission_drop_(
            source,
            std::string{"dropping a connection whose first frame did not decode: "} + e.what());
    }
}

bool Coordinator::handle_first_frame_(std::unique_ptr<network::Connection> conn,
                                      std::vector<std::byte> frame,
                                      std::uint64_t admission) {
    // The first frame was read on the connection's admission thread, under
    // the admission deadline (admit_connection_). That deadline is
    // handshake_timeout + heartbeat_timeout, reusing heartbeat_timeout rather
    // than giving the first frame its own knob: the coordinator already
    // declares that a peer silent for that long is dead
    // (mark_worker_lost_locked_ uses it on ESTABLISHED peers), so a peer that
    // cannot get its first frame out inside that window is, by the
    // coordinator's own definition of liveness, not live.
    MessageReader r(std::move(frame));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    if (kind == MessageKind::Register) {
        handle_register_(std::move(conn), r, admission);
        return true;
    }
    if (kind == MessageKind::HelloClient) {
        // Same negotiation as a worker's. A CLI built against one cluster
        // version and pointed at another used to find out by submitting.
        const auto hello = decode_hello_client(r);
        if (const auto compat = check_protocol_compatibility(
                hello.protocol_version, hello.min_compatible_protocol_version, "client");
            !compat.compatible) {
            log::error("coordinator.client", "refusing client: " + compat.reason);
            metrics::orch::protocol_mismatch();
            const auto frame =
                encode_frame(MessageKind::SubmitJobAck,
                             SubmitJobAckMsg{.job_id = 0, .ok = false, .message = compat.reason});
            (void)send_admission_reply_(*conn, frame);
            return false;  // conn destructor closes
        }
        // Reap before admitting. A client that connected and went away
        // has an exited thread still holding a joinable handle; joining
        // it here is what keeps the list bounded by CONCURRENT clients
        // rather than by total clients ever seen.
        const auto live = reap_finished_clients_();
        if (live >= cfg_.max_client_connections) {
            log::warn("coordinator.client",
                      "refusing a client: " + std::to_string(live) +
                          " connections already open (max_client_connections=" +
                          std::to_string(cfg_.max_client_connections) +
                          "). Refusing is deliberate - accepting would spawn a thread the "
                          "coordinator cannot account for.");
            metrics::orch::client_connection_refused();
            const auto frame = encode_frame(
                MessageKind::SubmitJobAck,
                SubmitJobAckMsg{.job_id = 0,
                                .ok = false,
                                .message = "coordinator is at its client-connection limit (" +
                                           std::to_string(cfg_.max_client_connections) +
                                           "); retry, or raise max_client_connections"});
            (void)send_admission_reply_(*conn, frame);
            return false;  // conn destructor closes
        }

        // Client connection: spawn a per-client thread that reads
        // SubmitJob frames and writes acks/completions. shared_ptr
        // ownership lets stop() safely call shutdown_read() even if
        // the handler thread has already exited and dropped its share.
        std::shared_ptr<network::Connection> shared_conn(conn.release());
        auto finished = std::make_shared<std::atomic<bool>>(false);
        std::lock_guard lock(client_mu_);
        client_sessions_.push_back(
            ClientSession{.conn = shared_conn,
                          .thread = std::thread([this, shared_conn, finished] {
                              handle_client_loop_(shared_conn);
                              // Last act: the accept loop reads this to
                              // decide the session is joinable without
                              // blocking on one that is still serving.
                              finished->store(true, std::memory_order_release);
                          }),
                          .finished = finished});
        return true;
    }
    // Protocol violation - drop the connection.
    return false;
}

void Coordinator::handle_register_(std::unique_ptr<network::Connection> conn,
                                   MessageReader& r,
                                   std::uint64_t admission) {
    auto reg = decode_register(r);

    // Protocol negotiation, before anything else is done with this worker.
    //
    // Refusing at the handshake is the whole point: an incompatible peer
    // admitted here does not fail here, it fails later on some control
    // frame it could not decode, at which point the symptom (a job that
    // will not deploy, a checkpoint that never commits) is a long way from
    // the cause. The refusal carries both versions so the operator knows
    // which end to upgrade without reading two logs.
    if (const auto compat = check_protocol_compatibility(reg.protocol_version,
                                                         reg.min_compatible_protocol_version,
                                                         "worker '" + reg.worker_id + "'");
        !compat.compatible) {
        log::error("coordinator.register", "refusing registration: " + compat.reason);
        metrics::orch::protocol_mismatch();
        RegisterAckMsg nack{.ok = false, .message = compat.reason};
        const auto frame = fenced_frame_(MessageKind::RegisterAck, nack);
        (void)send_admission_reply_(*conn, frame);
        return;  // conn destructor closes
    }

    // Reap before admitting, then cap. Same order and the same reasoning as the
    // client path: joining readers that have already exited is what keeps this
    // bounded by CONCURRENT workers rather than by every worker id ever seen -
    // and without the reap, a cap would be a lifetime quota on distinct ids,
    // which is the "refuses things that work" failure a linter is warned about.
    //
    // A re-registration under the SAME id is not new capacity: it replaces the
    // existing record further down, so it must not be refused for being at the
    // limit. Checking membership first is what keeps a restarting worker able to
    // come back to a full cluster.
    {
        const auto live = reap_finished_workers_();
        bool already_known = false;
        {
            std::lock_guard lock(mu_);
            already_known = registered_.count(reg.worker_id) != 0;
        }
        if (!already_known && live >= cfg_.max_worker_connections) {
            const std::string reason =
                "coordinator is at its worker-connection limit (" +
                std::to_string(cfg_.max_worker_connections) + " already connected); refusing '" +
                reg.worker_id +
                "'. Refusing is deliberate - admitting would spawn two threads (a reader and a "
                "dispatcher) the coordinator cannot account for. Raise max_worker_connections "
                "if the cluster genuinely is this large.";
            log::warn("coordinator.register", reason);
            metrics::orch::worker_connection_refused();
            RegisterAckMsg nack{.ok = false, .message = reason, .retryable = true};
            const auto frame = fenced_frame_(MessageKind::RegisterAck, nack);
            (void)send_admission_reply_(*conn, frame);
            return;  // conn destructor closes
        }
    }

    auto worker = std::make_shared<WorkerConnection>();
    worker->worker_id = reg.worker_id;
    worker->data_host = reg.data_host;
    worker->conn = std::move(conn);
    worker->last_seen.store(std::chrono::steady_clock::now(), std::memory_order_release);
    worker->slot_capacity = reg.slot_count == 0 ? std::uint32_t{1} : reg.slot_count;
    worker->protocol_version = reg.protocol_version;
    worker->http_port = reg.http_port;
    worker->admission = admission;

    // Install the registration and send RegisterAck under ONE mu_ hold, in
    // that order. The ack is the worker's schedulability linearisation point:
    // connect_to_coordinator returns once the ack arrives and the caller may
    // deploy immediately, so the map swap must happen-before the ack is on
    // the wire. Acking first left a window where such a deploy still resolved
    // this worker_id to the retired session's dead connection - the Deploy
    // frame vanished into the closed socket (the first send after a peer
    // close succeeds into the buffer) and the job never started. Sending the
    // ack inside the lock also keeps it the FIRST frame on the wire: a
    // concurrent deploy cannot slip a Deploy frame ahead of it (the worker's
    // connect handshake requires RegisterAck as the first message).
    //
    // A re-registration under an existing id (a fresh in-process session or a
    // restarted process with a stable name) replaces the old WorkerConnection. Its reader
    // thread must be joined before the object can be destroyed - destroying a
    // joinable std::thread is std::terminate - and the join must happen off
    // this lock AND off the reader thread (the reader joins the connection's
    // dispatch thread, which takes mu_ for every frame it handles, and the
    // reader holds a shared_ptr to its own WorkerConnection, so letting the
    // map drop the last reference hands destruction to the exiting reader
    // itself: self-join, terminate). The join, and the retirement after it,
    // run on the new session's dispatch thread (retire_replaced_session_),
    // not here: the old session's dispatcher first handles every frame its
    // reader read, which can take as long as the store writes those frames
    // make, and waiting here would hold the accept thread, and leave the new
    // session's heartbeats unread, for all of it.
    std::shared_ptr<WorkerConnection> replaced;
    bool replaced_was_lost = false;
    // Why this registration is refused, decided under mu_ and sent after it
    // is released: a refusal is a reply to a peer that may never read, and
    // mu_ is what every dispatcher, the watchdog and stop() need.
    std::string refusal;
    {
        std::lock_guard lock(mu_);
        if (auto it = registered_.find(reg.worker_id); it != registered_.end()) {
            if (it->second->admission > admission) {
                // A registration from an OLDER connection than the session on
                // record. First frames are handled one at a time but not in
                // accept order, so a worker whose first attempt timed out
                // waiting for its RegisterAck, and which reconnected and
                // registered again, can have that abandoned attempt handled
                // after the live one. Replacing would tear down the live
                // session for one whose worker has already hung up on it.
                refusal = "worker '" + reg.worker_id +
                          "' registered from a connection accepted before the one its current "
                          "session registered on; dropping the older registration";
            } else if (it->second->retiring_predecessor) {
                // The session on record is still retiring the one IT
                // replaced: its dispatch thread waits for that session's
                // frames, and a session replacing it would wait for both.
                // Each such registration while the store holds the oldest
                // one's frames used to add two threads and a full dispatch
                // backlog, outside max_worker_connections, with no bound on
                // how many. Refused instead, retryably: the worker's
                // supervisor backs off and registers again, and is admitted
                // once the retirement ends. Only a worker that registers twice
                // while one retirement is out is refused, and the retirement
                // ends when the old session's frames have been handled.
                refusal = "worker '" + reg.worker_id +
                          "' re-registered while the coordinator is still retiring an earlier "
                          "session of it (handling the frames that session sent); retry once that "
                          "is done";
            } else {
                replaced = it->second;
                replaced_was_lost = replaced->lost;
            }
        }
        if (refusal.empty()) {
            worker->session = ++last_worker_session_;
            worker->retiring_predecessor = replaced != nullptr;
            registered_[reg.worker_id] = worker;
            // Binds the worker to this leader's epoch. Small and the first
            // frame on a fresh connection, so it goes in one send() at once;
            // send_admission_reply_ bounds it all the same, since mu_ is held.
            RegisterAckMsg ack_msg{.ok = true, .message = ""};
            const auto ack = fenced_frame_(MessageKind::RegisterAck, ack_msg);
            if (!send_admission_reply_(*worker->conn, ack)) {
                // Handshake failed (the client vanished mid-register, or never
                // read). Restore the previous session: a failed
                // re-registration must not retire a live worker.
                if (replaced) {
                    registered_[reg.worker_id] = replaced;
                } else {
                    registered_.erase(reg.worker_id);
                }
                return;
            }
        }
    }
    if (!refusal.empty()) {
        log::warn("coordinator.register", refusal);
        metrics::orch::worker_connection_refused();
        RegisterAckMsg nack{.ok = false, .message = refusal, .retryable = true};
        (void)send_admission_reply_(*worker->conn, fenced_frame_(MessageKind::RegisterAck, nack));
        return;  // conn destructor closes
    }
    // From here the replaced session is this thread's to retire until
    // start_reader_for_ takes it. Anything that throws before then (an
    // allocation in the logging below) retires it here instead, on the
    // registering thread: dropped unjoined, its reader would end holding the
    // last reference to its own WorkerConnection and destroy its own joinable
    // std::thread, which is std::terminate. The new session never got its
    // threads, so it is shut down and reported finished; with nothing stamping
    // it, the watchdog declares it lost.
    struct RetireUnlessHandedOver {
        Coordinator& self;
        WorkerConnection& successor;
        std::shared_ptr<WorkerConnection>& replaced;
        const bool& replaced_was_lost;
        bool handed_over{false};
        ~RetireUnlessHandedOver() {
            if (handed_over) {
                return;
            }
            if (successor.conn) {
                successor.conn->shutdown_read();
            }
            successor.reader_finished->store(true, std::memory_order_release);
            if (replaced) {
                self.retire_replaced_session_(successor, replaced, replaced_was_lost);
            }
        }
    } retire_guard{*this, *worker, replaced, replaced_was_lost};
    if (replaced && replaced->conn) {
        // Wake a reader parked in read_frame. It stops reading, and the
        // frames it has already read are handled before it exits.
        replaced->conn->shutdown_read();
    }
    // A registration IS forward progress for any restart waiting on
    // capacity: reset every waiting job's capacity clock so the deadline
    // measures restart_capacity_timeout with NO worker arriving - genuine
    // starvation - rather than that long of a kill/return cadence that keeps
    // almost catching up.
    // (The rig-night composite expired the clock across episodes whose
    // workers returned within seconds, each time reading a live recovery
    // as a dead cluster.) Under its own mu_ hold: the registration lock
    // above was deliberately released, and jobs_ is concurrently written by
    // deploys.
    //
    // Here, at the registration, and not after a replaced session's
    // retirement: the new session's slots are free from its RegisterAck, and
    // a placement made on it while the old session's frames drain is stamped
    // with the new session, which the retirement does not fold. Waking the
    // waiters only after the retirement would not have closed that window
    // anyway: a submit or a watchdog-kicked restart places without being
    // woken.
    {
        std::lock_guard lock(mu_);
        for (auto& [_, jptr] : jobs_) {
            if (jptr->awaiting_restart) {
                jptr->restart_capacity_deadline = {};
            }
        }
    }
    cv_.notify_all();

    // A replacement's gauges move when its predecessor is retired.
    if (!replaced) {
        metrics::coordinator::worker_registered(worker->slot_capacity);
    }
    log::info("coordinator.register",
              "worker=" + worker->worker_id + " host=" + worker->data_host +
                  " slots=" + std::to_string(worker->slot_capacity) +
                  " http_port=" + std::to_string(worker->http_port));
    events::publish("coordinator.worker_registered",
                    "{\"worker_id\":" + js_quote(worker->worker_id) +
                        ",\"data_host\":" + js_quote(worker->data_host) +
                        ",\"slots\":" + std::to_string(worker->slot_capacity) +
                        ",\"http_port\":" + std::to_string(worker->http_port) + "}");

    retire_guard.handed_over = true;
    start_reader_for_(worker, std::move(replaced), replaced_was_lost);
}

void Coordinator::retire_replaced_session_(WorkerConnection& successor,
                                           const std::shared_ptr<WorkerConnection>& replaced,
                                           bool replaced_was_lost) noexcept {
    try {
        retire_replaced_session_unguarded_(successor, replaced, replaced_was_lost);
    } catch (const std::exception& e) {
        try {
            log::error("coordinator.register",
                       "worker=" + successor.worker_id +
                           ": retiring the previous session failed: " + e.what());
        } catch (...) {
            // Only the log line was lost.
        }
    } catch (...) {
        // Not a std::exception; the join below is what matters.
    }
    // The join is the retirement's first act; if it was never reached, or
    // threw, it is attempted again here. join() throws only when it cannot
    // join at all, and then nothing more can be done.
    try {
        if (replaced->reader.joinable()) {
            replaced->reader.join();
        }
    } catch (...) {
        // Not joinable from here; nothing more can be done.
    }
    try {
        std::lock_guard lock(mu_);
        successor.retiring_predecessor = false;
    } catch (...) {
        // mu_ could not be locked; the flag only gates a retryable refusal.
    }
}

void Coordinator::retire_replaced_session_unguarded_(
    const WorkerConnection& successor,
    const std::shared_ptr<WorkerConnection>& replaced,
    bool replaced_was_lost) {
    // The old session's reader has been woken (handle_register_ shut its read
    // side) and exits once its dispatcher has handled every frame it read.
    // Those frames were sent before the re-registration and are handled as
    // they always were: a SubtaskFinished among them takes its subtask out of
    // the fold below rather than leaving it to be counted as lost.
    if (replaced->reader.joinable()) {
        replaced->reader.join();
    }
    if (stop_.load(std::memory_order_acquire)) {
        return;  // stop() abandons the old session's backlog; nothing to fold
    }
    log::info("coordinator.register",
              "worker=" + successor.worker_id + " re-registered; previous session retired");
    // Retiring the session is not enough: whatever the OLD session had
    // in flight for a job can never report now, and any restart drain
    // waiting on those subtasks would wait until its deadline and then
    // fail the job.
    //
    // This is how a replacement session deadlocked recovery. A session ends,
    // the coordinator starts a restart drain, and the worker registers a
    // fresh session under the SAME id (which is
    // correct - the id must be stable for the coordinator to recognise
    // it), and the drain is now waiting on a subtask whose owning session
    // can no longer report.
    // The re-registered worker is alive and heartbeating, so the watchdog
    // never declares it lost and never folds it in.
    //
    // The same treatment a lost worker's subtasks get: drop them from the
    // expected-drain set and queue them for redeploy. No restart attempt
    // is consumed - this is still the same restart, now correctly aware
    // that one of the survivors it was waiting for has been replaced.
    //
    // Only what the old session held. The new session has been schedulable
    // since its RegisterAck, so a job may already have been placed on it
    // while the old session's frames drained; the worker id names both
    // sessions, and the session stamp tells them apart.
    retire_previous_session_subtasks_(successor.worker_id, successor.session);
    // Replacing a still-live session keeps the registered-worker and slot
    // capacity gauges unchanged. A previously-lost session was already
    // subtracted by worker_lost(), so its replacement must add them back.
    // Counting every re-registration inflated both gauges on each transient
    // control-plane reconnect.
    if (replaced_was_lost) {
        metrics::coordinator::worker_registered(successor.slot_capacity);
    } else {
        // Read and marked in one hold: a slot moved off the replaced session
        // after this (a deploy re-targeting its successor) must not leave the
        // gauge a second time.
        std::uint32_t in_use = 0;
        {
            std::lock_guard lock(mu_);
            in_use = replaced->slots_in_use;
            replaced->slots_gauge_retired = true;
        }
        metrics::coordinator::worker_session_replaced(
            replaced->slot_capacity, in_use, successor.slot_capacity);
    }
}

std::uint64_t Coordinator::latest_completed_checkpoint(JobId job_id) const {
    std::lock_guard lock(mu_);
    const auto it = jobs_.find(job_id);
    return it == jobs_.end() ? 0 : it->second->latest_completed_checkpoint_id;
}

std::uint64_t Coordinator::latest_confirmed_checkpoint(JobId job_id) const {
    std::lock_guard lock(mu_);
    const auto it = jobs_.find(job_id);
    return it == jobs_.end() ? 0 : it->second->latest_confirmed_checkpoint_id;
}

std::size_t Coordinator::reap_finished_clients_() {
    std::vector<std::thread> to_join;
    std::size_t live = 0;
    {
        std::lock_guard lock(client_mu_);
        std::vector<ClientSession> keep;
        keep.reserve(client_sessions_.size());
        for (auto& session : client_sessions_) {
            if (session.finished->load(std::memory_order_acquire)) {
                to_join.push_back(std::move(session.thread));
            } else {
                keep.push_back(std::move(session));
            }
        }
        client_sessions_ = std::move(keep);
        live = client_sessions_.size();
    }
    // Joined OUTSIDE client_mu_. Each of these has already run to
    // completion, so the join returns at once - but holding a lock across
    // a join is how a future change to the client loop turns into a
    // deadlock, and the cost of not doing it is nothing.
    for (auto& t : to_join) {
        if (t.joinable()) {
            t.join();
        }
    }
    return live;
}

std::size_t Coordinator::reap_finished_workers_() {
    std::vector<std::thread> to_join;
    std::size_t live = 0;
    {
        std::lock_guard lock(mu_);
        for (auto& [_, worker] : registered_) {
            if (!worker) {
                continue;
            }
            if (worker->reader_finished->load(std::memory_order_acquire)) {
                if (worker->reader.joinable()) {
                    to_join.push_back(std::move(worker->reader));
                }
                // Drop the socket too. shutdown_read() only half-closes, so
                // without this the fd survives every lost worker for the life of
                // the process. This releases the map's reference only: a sender
                // that staged a frame under mu_ holds its own shared_ptr and
                // the Connection lives until that send returns. The null check
                // at every send site was never enough on its own - a raw
                // pointer copied before this reset dangled after it.
                worker->conn.reset();
            }
            if (worker->conn) {
                ++live;
            }
        }
    }
    // Joined OUTSIDE mu_, and never from the reader thread itself. Both matter:
    // this file has already produced a self-join terminate and a use-after-free
    // in the equivalent client path.
    for (auto& t : to_join) {
        if (t.joinable()) {
            t.join();
        }
    }
    return live;
}

void Coordinator::handle_client_loop_(std::shared_ptr<network::Connection> conn) {
    auto* conn_raw = conn.get();
    // Forget this connection on EVERY exit path, not just the one.
    //
    // JobState::notify_client_conn is a raw pointer into the Connection this
    // loop owns, and signal_job_completion_locked_ dereferences it to push
    // JobCompleted. When the loop returns, the shared_ptr dies and the
    // Connection with it - so any job still holding the pointer is holding
    // freed memory, and the next completion segfaults the coordinator.
    //
    // The read-failure path below cleared it. The other two - an unknown
    // frame kind, and a frame that failed to decode - returned without
    // clearing, which is a use-after-free waiting for a job to finish. It
    // presented as "connection closed by the coordinator" followed by the
    // coordinator dying of SIGSEGV, and only on Linux, because whether a
    // freed Connection faults depends on what the allocator did with it
    // (F39).
    //
    // A guard rather than three copies of the loop body's cleanup: the next
    // exit path added to this function cannot forget.
    struct ForgetClientConn {
        Coordinator* self;
        network::Connection* raw;
        ~ForgetClientConn() {
            std::lock_guard lock(self->mu_);
            for (auto& [_, job] : self->jobs_) {
                if (job->notify_client_conn == raw) {
                    job->notify_client_conn = nullptr;
                }
            }
        }
    } forget_guard{this, conn_raw};

    while (!stop_.load(std::memory_order_acquire)) {
        auto frame = read_frame(*conn);
        if (!frame.has_value()) {
            // Client closed. The job, if still in flight, continues - we
            // just lose the ability to push JobCompleted back. The guard
            // above drops the pointer.
            return;  // conn destructor closes
        }
        MessageReader r(std::move(*frame));
        // Same boundary as the accept loop. A client sending a malformed
        // frame must lose its own connection, not the coordinator.
        try {
            if (!dispatch_client_frame_(*conn, r)) {
                return;  // unknown kind; conn destructor closes
            }
        } catch (const std::exception& e) {
            log::warn("coordinator.client",
                      std::string{"dropping a client whose frame did not decode: "} + e.what());
            metrics::orch::malformed_frame();
            return;  // conn destructor closes
        }
    }
}

// Dispatch one decoded client frame. Returns false to close the
// connection (unknown kind), true to keep reading.
//
// Extracted from handle_client_loop_ so the loop can put a try around
// exactly one frame: a client sending something malformed loses its own
// connection, and the coordinator carries on.
bool Coordinator::dispatch_client_frame_(network::Connection& conn, MessageReader& r) {
    const auto kind = static_cast<MessageKind>(r.read_u8());
    // Every handled client->coordinator kind returns immediately. Do not let a
    // handler fall through to the next `if`: MessageKind values must be
    // distinct, but a missing return here would still double-dispatch
    // a frame to two handlers and read past its payload.
    if (kind == MessageKind::SubmitJob) {
        handle_submit_(conn, r);
        return true;
    }
    if (kind == MessageKind::ListJobs) {
        handle_list_jobs_(conn);
        return true;
    }
    if (kind == MessageKind::CancelJob) {
        handle_cancel_job_(conn, r);
        return true;
    }
    if (kind == MessageKind::RescaleOperator) {
        handle_rescale_operator_(conn, r);
        return true;
    }
    if (kind == MessageKind::RescaleJob) {
        handle_rescale_job_(conn, r);
        return true;
    }
    if (kind == MessageKind::Savepoint) {
        handle_savepoint_(conn, r);
        return true;
    }
    if (kind == MessageKind::StopJob) {
        handle_stop_job_(conn, r);
        return true;
    }
    // Unknown frame from a client - drop the connection, and SAY so.
    //
    // This used to close silently, which made it indistinguishable from the
    // client hanging up. A submitter waiting for JobCompleted then reported
    // "connection closed by the coordinator" with nothing on the coordinator
    // side to explain it, and the first hour of diagnosing F39 went into
    // looking for a crash that may not have been there.
    log::warn("coordinator.client",
              "dropping a client that sent an unhandled frame kind " +
                  std::to_string(static_cast<int>(kind)) +
                  ". A job this client was waiting on continues; it just loses the "
                  "JobCompleted push.");
    metrics::orch::malformed_frame();
    return false;
}

ClusterSnapshot Coordinator::snapshot_cluster() const {
    ClusterSnapshot s;
    s.bind_host = cfg_.bind_host;
    s.advertise_host = cfg_.advertise_host;
    s.control_port = bound_port_;
    std::lock_guard lock(mu_);
    for (const auto& [worker_id, worker] : registered_) {
        WorkerSummary ts;
        ts.worker_id = worker->worker_id;
        ts.data_host = worker->data_host;
        ts.slot_capacity = worker->slot_capacity;
        ts.slots_in_use = worker->slots_in_use;
        ts.lost = worker->lost;
        ts.http_port = worker->http_port;
        ts.protocol_version = worker->protocol_version;
        if (!worker->lost) {
            s.total_slot_capacity += worker->slot_capacity;
            s.slots_in_use += worker->slots_in_use;
        }
        s.workers.push_back(std::move(ts));
    }
    s.jobs_total = jobs_.size();
    for (const auto& [_, job] : jobs_) {
        if (job->completion_signalled) {
            ++s.jobs_completed;
        } else {
            ++s.jobs_running;
        }
    }
    return s;
}

std::vector<WorkerSummary> Coordinator::snapshot_workers() const {
    std::vector<WorkerSummary> out;
    std::lock_guard lock(mu_);
    out.reserve(registered_.size());
    for (const auto& [_, worker] : registered_) {
        WorkerSummary ts;
        ts.worker_id = worker->worker_id;
        ts.data_host = worker->data_host;
        ts.slot_capacity = worker->slot_capacity;
        ts.slots_in_use = worker->slots_in_use;
        ts.lost = worker->lost;
        ts.http_port = worker->http_port;
        out.push_back(std::move(ts));
    }
    return out;
}

// The job's terminal status, on exactly the precedence
// signal_job_completion_locked_ uses for its log line and ListJobs uses
// on the binary control plane - one function, so the HTTP API and the
// control plane cannot disagree about how a job ended.
std::string job_status_string(bool completion_signalled, bool cancel_requested, bool has_errors) {
    if (!completion_signalled) {
        return std::string{to_string(JobTerminalStatus::Running)};
    }
    if (cancel_requested) {
        return std::string{to_string(JobTerminalStatus::Cancelled)};
    }
    if (has_errors) {
        return std::string{to_string(JobTerminalStatus::Failed)};
    }
    return std::string{to_string(JobTerminalStatus::CompletedOk)};
}

std::vector<JobSummary> Coordinator::snapshot_jobs() const {
    std::vector<JobSummary> out;
    std::lock_guard lock(mu_);
    out.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) {
        JobSummary s;
        s.id = id;
        s.expected_completion = job->expected_completion;
        s.completed_count = job->completed_count;
        s.completion_signalled = job->completion_signalled;
        s.cancel_requested = job->cancel_requested;
        s.error_count = job->errors.size();
        s.status = job_status_string(
            job->completion_signalled, job->cancel_requested, !job->errors.empty());
        out.push_back(std::move(s));
    }
    return out;
}

std::optional<JobDetail> Coordinator::snapshot_job(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return std::nullopt;
    }
    const auto& job = *it->second;
    JobDetail d;
    d.id = job_id;
    d.expected_completion = job.expected_completion;
    d.completed_count = job.completed_count;
    d.completion_signalled = job.completion_signalled;
    d.cancel_requested = job.cancel_requested;
    d.errors = job.errors;
    d.subtask_errors = job.subtask_errors;
    for (const auto& [worker_id, tasks] : job.tasks_by_worker) {
        for (const auto& t : tasks) {
            JobTaskRecord r;
            r.role = t.role;
            r.subtask_idx = t.subtask_idx;
            r.worker_id = worker_id;
            d.tasks.push_back(std::move(r));
        }
    }
    d.latest_completed_checkpoint_id = job.latest_completed_checkpoint_id;
    for (const auto& [ckpt_id, _] : job.pending_checkpoint_acks) {
        d.pending_checkpoint_ids.push_back(ckpt_id);
    }
    // The configuration this job is RUNNING with, straight off the retained
    // CheckpointConfig rather than re-derived, so what the endpoint reports cannot
    // drift from what the coordinator is acting on.
    d.checkpoint_dir = job.checkpoint.checkpoint_dir;
    d.checkpoint_interval_ms = job.checkpoint.interval_ms;
    d.state_backend_uri = job.checkpoint.state_backend_uri;
    d.restore_from_dir = job.checkpoint.restore_from_dir;
    d.restore_from_checkpoint_id = job.checkpoint.restore_from_checkpoint_id;
    d.max_restarts_on_worker_loss = job.checkpoint.max_restarts_on_worker_loss;
    d.unaligned_checkpoints = job.checkpoint.alignment == CheckpointAlignment::Unaligned;
    d.adaptive_barrier_mode = job.checkpoint.alignment == CheckpointAlignment::Adaptive;
    d.status =
        job_status_string(job.completion_signalled, job.cancel_requested, !job.errors.empty());
    return d;
}

std::optional<JobGraphDetail> Coordinator::snapshot_job_graph(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return std::nullopt;
    }
    const auto& job = *it->second;
    JobGraphDetail d;
    d.id = job_id;
    d.topology_version = job.topology_version;
    if (job.graph_json.empty()) {
        d.available = false;  // job exists but no retained graph
        return d;
    }
    JobGraphSpec spec;
    try {
        spec = JobGraphSpec::from_json(job.graph_json);
    } catch (...) {
        d.available = false;
        return d;
    }

    // Resolve an input ref to (upstream op id, side-output tag). Forms:
    //   "id"        - main output
    //   "id.N"      - split branch N (N all-digits)
    //   "id::tag"   - named side output
    // Mirrors the planner's parse_input_ref so split / side-output topologies
    // resolve to real node ids (not "id.0") and carry the right channel type.
    const auto parse_ref = [](const std::string& ref) -> std::pair<std::string, std::string> {
        if (const auto p = ref.find("::"); p != std::string::npos) {
            return {ref.substr(0, p), ref.substr(p + 2)};
        }
        if (const auto p = ref.rfind('.'); p != std::string::npos && p + 1 < ref.size()) {
            const auto suffix = ref.substr(p + 1);
            const bool all_digits = std::all_of(
                suffix.begin(), suffix.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
            if (all_digits) {
                return {ref.substr(0, p), std::string{}};
            }
        }
        return {ref, std::string{}};
    };

    // Index ops by id, and collect every id used as an input so terminal ops
    // (no downstream) can be classified as sinks.
    std::unordered_map<std::string, const OperatorSpec*> by_id;
    std::unordered_set<std::string> has_downstream;
    by_id.reserve(spec.ops.size());
    for (const auto& op : spec.ops) {
        by_id.emplace(op.id, &op);
    }
    for (const auto& op : spec.ops) {
        for (const auto& in : op.inputs) {
            has_downstream.insert(parse_ref(in).first);
        }
    }

    // Subtask placement: parse each deployed subtask's OperatorChainSpec and
    // group (subtask_idx, worker_id) by the operator id(s) the chain hosts.
    std::unordered_map<std::string, std::vector<GraphSubtaskPlacement>> placement;
    for (const auto& [worker_id, tasks] : job.tasks_by_worker) {
        for (const auto& t : tasks) {
            if (t.extra_config.empty()) {
                continue;
            }
            try {
                const auto chain = OperatorChainSpec::from_json(t.extra_config);
                const std::string tkey = t.role + ":" + std::to_string(t.subtask_idx);
                std::int64_t started_ms = 0;
                std::int64_t finished_ms = 0;
                if (auto tit = job.subtask_timing.find(tkey); tit != job.subtask_timing.end()) {
                    started_ms = tit->second.started_ms;
                    finished_ms = tit->second.finished_ms;
                }
                for (const auto& cop : chain.ops) {
                    placement[cop.id].push_back(
                        {t.subtask_idx, worker_id, started_ms, finished_ms});
                }
            } catch (...) {
                // Non-generic / unparseable task config: skip placement for it.
            }
        }
    }

    // Actual (physical) parallelism per op from placement, which stays correct
    // across a rescale even though the retained spec holds the original value.
    // Falls back to the spec parallelism for ops with no observed placement.
    const auto actual_par = [&](const OperatorSpec& op) -> std::uint32_t {
        const auto it = placement.find(op.id);
        return (it != placement.end() && !it->second.empty())
                   ? static_cast<std::uint32_t>(it->second.size())
                   : op.parallelism;
    };

    // Channel type carried on an edge from `ref` into a consumer: the upstream's
    // declared side-output type when the ref names one, else its main output.
    const auto edge_channel = [&](const std::string& from_id,
                                  const std::string& tag,
                                  const OperatorSpec* from_op) -> std::string {
        if (from_op == nullptr) {
            return {};
        }
        if (!tag.empty()) {
            for (const auto& so : from_op->side_outputs) {
                if (so.tag == tag) {
                    return so.channel_type;
                }
            }
        }
        return from_op->out_channel;
    };

    d.nodes.reserve(spec.ops.size());
    for (const auto& op : spec.ops) {
        GraphNode n;
        n.id = op.id;
        n.op_type = op.type;
        n.display_name = op.display_name;
        n.uid = op.uid;
        n.parallelism = actual_par(op);
        n.out_channel = op.out_channel;
        n.keyed = !op.key_by.empty();
        n.kind = op.inputs.empty()
                     ? "source"
                     : (has_downstream.find(op.id) == has_downstream.end() ? "sink" : "operator");
        if (auto pit = placement.find(op.id); pit != placement.end()) {
            n.subtasks = std::move(pit->second);
        }
        d.nodes.push_back(std::move(n));

        for (const auto& in : op.inputs) {
            const auto [from, tag] = parse_ref(in);
            const auto* from_op = [&]() -> const OperatorSpec* {
                const auto fit = by_id.find(from);
                return fit == by_id.end() ? nullptr : fit->second;
            }();
            GraphEdge e;
            e.from = from;
            e.to = op.id;
            e.channel = edge_channel(from, tag, from_op);
            const auto up_par = (from_op != nullptr) ? actual_par(*from_op) : op.parallelism;
            e.routing = n.keyed ? "hash" : (up_par != n.parallelism ? "rebalance" : "forward");
            d.edges.push_back(std::move(e));
        }
    }
    d.available = true;
    return d;
}

std::optional<lineage::LineageGraph> Coordinator::snapshot_job_lineage(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return std::nullopt;
    }
    const auto& job = *it->second;
    if (job.graph_json.empty()) {
        return lineage::LineageGraph{};  // known job, but no retained graph
    }
    try {
        const auto spec = JobGraphSpec::from_json(job.graph_json);
        return lineage::extract_lineage(spec);
    } catch (...) {
        return lineage::LineageGraph{};
    }
}

std::optional<std::pair<std::string, std::uint16_t>> Coordinator::worker_http_target(
    const std::string& worker_id) const {
    std::lock_guard lock(mu_);
    auto it = registered_.find(worker_id);
    if (it == registered_.end()) {
        return std::nullopt;
    }
    const auto& worker = *it->second;
    if (worker.lost || worker.http_port == 0) {
        return std::nullopt;
    }
    return std::make_pair(worker.data_host, worker.http_port);
}

std::vector<std::pair<std::string, std::uint16_t>> Coordinator::workers_hosting_job(
    JobId job_id) const {
    std::lock_guard lock(mu_);
    auto job_it = jobs_.find(job_id);
    if (job_it == jobs_.end()) {
        return {};
    }
    std::vector<std::pair<std::string, std::uint16_t>> out;
    std::unordered_set<std::string> seen;
    for (const auto& [worker_id, _tasks] : job_it->second->tasks_by_worker) {
        if (!seen.insert(worker_id).second) {
            continue;
        }
        auto reg_it = registered_.find(worker_id);
        if (reg_it == registered_.end()) {
            continue;
        }
        const auto& worker = *reg_it->second;
        if (worker.lost || worker.http_port == 0) {
            continue;
        }
        out.emplace_back(worker.data_host, worker.http_port);
    }
    return out;
}

std::uint64_t Coordinator::topology_version(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return 0;
    }
    return it->second->topology_version;
}

std::optional<Coordinator::RouteTarget> Coordinator::route_key_for_job(
    JobId job_id, const std::string& role, std::span<const std::byte> key_bytes) const {
    const auto kg = key_group_for_key(key_bytes);
    std::lock_guard lock(mu_);
    auto job_it = jobs_.find(job_id);
    if (job_it == jobs_.end()) {
        return std::nullopt;
    }
    for (const auto& [worker_id, tasks] : job_it->second->tasks_by_worker) {
        for (const auto& task : tasks) {
            if (task.role != role) {
                continue;
            }
            // {0, 0} sentinel == full range (non-rescaled deploys),
            // matching the restore-side filter expansion.
            std::uint16_t first = task.key_group_first;
            std::uint16_t last = task.key_group_last;
            if (first == 0 && last == 0) {
                last = kNumKeyGroups;
            }
            if (kg < first || kg >= last) {
                continue;
            }
            auto reg_it = registered_.find(worker_id);
            if (reg_it == registered_.end()) {
                continue;
            }
            const auto& worker = *reg_it->second;
            if (worker.lost || worker.http_port == 0) {
                continue;
            }
            return RouteTarget{worker.data_host, worker.http_port, task.subtask_idx};
        }
    }
    return std::nullopt;
}

std::vector<Coordinator::RouteTarget> Coordinator::subtask_targets_for_role(
    JobId job_id, const std::string& role) const {
    std::vector<RouteTarget> out;
    std::lock_guard lock(mu_);
    auto job_it = jobs_.find(job_id);
    if (job_it == jobs_.end()) {
        return out;
    }
    for (const auto& [worker_id, tasks] : job_it->second->tasks_by_worker) {
        auto reg_it = registered_.find(worker_id);
        if (reg_it == registered_.end()) {
            continue;
        }
        const auto& worker = *reg_it->second;
        if (worker.lost || worker.http_port == 0) {
            continue;
        }
        for (const auto& task : tasks) {
            if (task.role == role) {
                out.push_back(RouteTarget{worker.data_host, worker.http_port, task.subtask_idx});
            }
        }
    }
    std::sort(out.begin(), out.end(), [](const RouteTarget& a, const RouteTarget& b) {
        return a.subtask_idx < b.subtask_idx;
    });
    return out;
}

CancelJobAckMsg Coordinator::cancel_job(JobId job_id) {
    CancelJobAckMsg ack;
    ack.job_id = job_id;
    // Collect worker connections inside the lock, fan out outside. A blocked
    // send on one peer must not stall every other client / worker holding mu_
    // (heartbeats, SubtaskFinished, ...).
    std::vector<std::shared_ptr<network::Connection>> worker_conns;
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            ack.ok = false;
            ack.message = "no such job";
        } else if (it->second->completion_signalled) {
            ack.ok = false;
            ack.message = "job already completed";
        } else if (it->second->cancel_requested) {
            ack.ok = false;
            ack.message = "cancel already in progress";
        } else {
            it->second->cancel_requested = true;
            // The cancel completes by counting SubtaskFinished arrivals, and
            // a peer that never reports would otherwise leave the job
            // "cancelled but RUNNING" forever - QUAL-06 run B watched
            // cancel_requested sit ignored for 40 minutes that way (item
            // 73). Same convergence bound as the fatal-error broadcast.
            it->second->terminal_cancel_deadline =
                std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
            for (const auto& [worker_id, _] : it->second->tasks_by_worker) {
                auto worker_it = registered_.find(worker_id);
                if (worker_it != registered_.end() && !worker_it->second->lost &&
                    worker_it->second->conn) {
                    worker_conns.push_back(worker_it->second->conn);
                }
            }
            ack.ok = true;
            ack.message =
                "cancel broadcast to " + std::to_string(worker_conns.size()) + " worker(s)";
        }
    }
    if (ack.ok) {
        // The job is cancelling but no worker knows yet: a source reaching end
        // of input in this window is declined its final checkpoint, and must
        // not commit its tail locally on the strength of that answer.
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeCancelBroadcast);
        CancelJobMsg cj;
        cj.job_id = job_id;
        const auto frame = fenced_frame_(MessageKind::CancelJob, cj);
        for (const auto& c : worker_conns) {
            send_frame(*c, frame);
        }
    }
    return ack;
}

void Coordinator::handle_cancel_job_(network::Connection& conn, MessageReader& r) {
    const auto req = decode_cancel_job(r);
    const auto ack = cancel_job(req.job_id);
    send_frame(conn, encode_frame(MessageKind::CancelJobAck, ack));
}

RescaleJobAckMsg Coordinator::rescale_job(
    JobId job_id, const std::unordered_map<std::string, std::uint32_t>& role_p) {
    RescaleJobAckMsg ack;
    ack.job_id = job_id;

    std::vector<std::shared_ptr<network::Connection>> worker_conns;
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            ack.ok = false;
            ack.message = "no such job";
            return ack;
        }
        auto& job = *it->second;
        if (job.completion_signalled) {
            ack.ok = false;
            ack.message = "job already completed";
            return ack;
        }
        if (job.cancel_requested) {
            ack.ok = false;
            ack.message = "cancel in progress";
            return ack;
        }
        if (job.awaiting_restart) {
            ack.ok = false;
            ack.message = "restart already in progress";
            return ack;
        }
        if (job.checkpoint.checkpoint_dir.empty()) {
            ack.ok = false;
            ack.message = "rescale requires a checkpoint dir";
            return ack;
        }
        if (job.latest_completed_checkpoint_id == 0) {
            ack.ok = false;
            ack.message = "rescale requires at least one completed checkpoint";
            return ack;
        }

        // Compute current per-role parallelism from task_records.
        std::unordered_map<std::string, std::uint32_t> current_p;
        for (const auto& [_, rec] : job.task_records) {
            ++current_p[rec.second.role];
        }

        // A whole-role rescale sets ONE parallelism for a role and rebuilds the
        // job's task set from it. That is coherent only while the role's tasks
        // are subtasks of the same operator chain. The planner puts every
        // subtask of every operator under the one shared role
        // kGenericSubtaskRole, so on a multi-operator job "the role's
        // parallelism" is the job's total subtask count, and setting it to N
        // deploys N tasks cloned from one chain - the other operators' chains
        // simply cease to exist, and the surviving clone's edges point at peers
        // that are no longer deployed.
        //
        // Measured, not theorised: a 3-operator job rescaled to parallelism 1
        // redeployed as a single task, failed every restart attempt with
        // "missing resolved peer for edge", exhausted its restart budget
        // and finished FAILED, having produced 3 of 240 records. The CLI had
        // reported ok=1 "rescale initiated".
        //
        // Refuse that combination. A single-operator job is unaffected, which
        // is the case the path was built for and the only one it handles.
        // The operator count comes from the RescaleCoordinator, which deploy
        // populates from the job graph.
        if (job.rescale_coordinator) {
            const auto ops = job.rescale_coordinator->all();
            if (ops.size() > 1 && role_p.find(kGenericSubtaskRole) != role_p.end()) {
                ack.ok = false;
                ack.message =
                    "rescale: role '" + std::string{kGenericSubtaskRole} + "' covers all " +
                    std::to_string(ops.size()) +
                    " operators in this job, so one parallelism for it cannot express the "
                    "job graph: the rescale would redeploy the job as clones of a single "
                    "operator chain and the remaining operators' subtasks would be dropped, "
                    "leaving edges pointing at peers that no longer exist. Whole-job rescale "
                    "is supported for single-operator jobs only. To change a multi-operator "
                    "job's parallelism, take a savepoint and resubmit at the new parallelism.";
                log::warn("coordinator.rescale",
                          "refused job_id=" + std::to_string(job_id) + ": " + ack.message);
                return ack;
            }
        }

        if (auto requested = role_p.find(kGenericSubtaskRole);
            requested != role_p.end() && requested->second > 1 && !job.graph_json.empty()) {
            try {
                const auto graph = JobGraphSpec::from_json(job.graph_json);
                const auto global =
                    std::find_if(graph.ops.begin(), graph.ops.end(), [](const auto& op) {
                        return is_forced_singleton(op);
                    });
                if (global != graph.ops.end()) {
                    ack.ok = false;
                    ack.message = "rescale: operator '" + global->id +
                                  "' requires a global SQL view and must remain at parallelism 1";
                    return ack;
                }
            } catch (const std::exception& e) {
                ack.ok = false;
                ack.message =
                    std::string{"rescale: the job's retained graph does not parse: "} + e.what();
                return ack;
            }
        }

        // Validate the rescale request. v1 supports integer scale-up
        // (new_p = k * old_p) and integer scale-down (old_p = k_down *
        // new_p). Non-integer factors would leave key groups straddling
        // parents, which is implementable but not in v1.
        std::int64_t slot_delta = 0;
        for (const auto& [role, new_p] : role_p) {
            auto cur = current_p.find(role);
            if (cur == current_p.end()) {
                ack.ok = false;
                ack.message = "rescale: unknown role '" + role + "'";
                return ack;
            }
            if (new_p == 0) {
                ack.ok = false;
                ack.message = "rescale: parallelism must be positive";
                return ack;
            }
            const std::uint32_t old_p = cur->second;
            const bool is_scale_up = new_p >= old_p && (new_p % old_p == 0);
            const bool is_scale_down = new_p < old_p && (old_p % new_p == 0);
            if (!is_scale_up && !is_scale_down) {
                ack.ok = false;
                ack.message =
                    "rescale: parallelism must be an integer multiple or divisor (role '" + role +
                    "': " + std::to_string(old_p) + " -> " + std::to_string(new_p) + ")";
                return ack;
            }
            slot_delta += static_cast<std::int64_t>(new_p) - static_cast<std::int64_t>(old_p);
        }
        // Sum free slots across alive workers. Pre-rescale tasks still
        // hold their slots; restart_job_locked_ runs AFTER the drain,
        // so by the time it claims slots they've been freed. The
        // check here only matters when the rescale net-grows slot
        // usage. Scale-down frees slots and so always fits.
        if (slot_delta > 0) {
            std::size_t total_free = 0;
            for (const auto& [_, worker] : registered_) {
                if (!worker->lost && worker->slot_capacity > worker->slots_in_use) {
                    total_free += (worker->slot_capacity - worker->slots_in_use);
                }
            }
            if (total_free < static_cast<std::size_t>(slot_delta)) {
                ack.ok = false;
                ack.message = "rescale: need " + std::to_string(slot_delta) +
                              " additional slot(s); cluster has " + std::to_string(total_free) +
                              " free";
                return ack;
            }
        }

        // Stage the rescale: store overrides + pre-rescale baseline,
        // mark awaiting_restart, populate restart_drain_expected with
        // every currently-pending subtask. The existing
        // handle_subtask_finished_ -> restart_job_locked_ machinery
        // fires when the drain completes; restart_job_locked_ honours
        // rescale_overrides and emits the per-task kg directives.
        for (const auto& [role, new_p] : role_p) {
            job.rescale_overrides[role] = new_p;
        }
        job.pre_rescale_parallelism = std::move(current_p);
        // Start of the clink.rescale lifecycle span (recorded when
        // restart_job_locked_ emits the rescaled deploys).
        if (clink::metrics::SpanBuffer::global().enabled()) {
            job.rescale_span_start_unix_nano = clink::metrics::otlp_now_unix_nano();
        }
        job.awaiting_restart = true;
        // Bound the rescale drain the same way the worker-loss path does: a
        // survivor that hangs while still heartbeating would otherwise wedge
        // the job in awaiting_restart forever (the watchdog deadline scan is
        // gated on a non-epoch restart_deadline). Set it whenever the job
        // enters awaiting_restart, per the field's contract.
        job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
        populate_restart_drain_locked_(job);
        // Empty drain (no in-flight subtasks): fire restart immediately.
        // Typical case is mid-stream, so we just collect worker conns for
        // the cancel broadcast and let the existing drain do its work.
        for (const auto& [worker_id, _] : job.tasks_by_worker) {
            auto worker_it = registered_.find(worker_id);
            if (worker_it != registered_.end() && !worker_it->second->lost &&
                worker_it->second->conn) {
                worker_conns.push_back(worker_it->second->conn);
            }
        }

        log::info("coordinator.rescale",
                  "job_id=" + std::to_string(job_id) + " roles=" + std::to_string(role_p.size()) +
                      " slot_delta=" + std::to_string(slot_delta));
    }

    // Broadcast CancelJob outside the lock to drain the existing task
    // set. handle_subtask_finished_ checks awaiting_restart and routes
    // the SubtaskFinished arrivals into the drain counter; once every
    // expected key has reported it calls restart_job_locked_, which
    // now picks up rescale_overrides and emits the rescaled deploys.
    CancelJobMsg cj;
    cj.job_id = job_id;
    const auto frame = fenced_frame_(MessageKind::CancelJob, cj);
    for (const auto& c : worker_conns) {
        send_frame(*c, frame);
    }

    ack.ok = true;
    ack.message = "rescale initiated; draining " + std::to_string(worker_conns.size()) +
                  " worker connection(s)";
    return ack;
}

void Coordinator::handle_rescale_job_(network::Connection& conn, MessageReader& r) {
    auto req = decode_rescale_job(r);
    std::unordered_map<std::string, std::uint32_t> role_p;
    role_p.reserve(req.role_parallelism.size());
    for (const auto& [role, p] : req.role_parallelism) {
        role_p[role] = p;
    }
    const auto ack = rescale_job(req.job_id, role_p);
    send_frame(conn, encode_frame(MessageKind::RescaleJobAck, ack));
}

void Coordinator::handle_rescale_operator_(network::Connection& conn, MessageReader& r) {
    auto req = decode_rescale_operator(r);
    const auto result = request_operator_rescale(req.job_id, req.op_id, req.new_parallelism);
    RescaleOperatorAckMsg ack;
    ack.job_id = req.job_id;
    ack.ok = result.ok;
    ack.accepted_target = result.accepted_target;
    ack.message = result.reason;
    send_frame(conn, encode_frame(MessageKind::RescaleOperatorAck, ack));
}

RescaleCoordinator::RequestResult Coordinator::request_operator_rescale(
    JobId job_id, const std::string& op_id, std::uint32_t new_parallelism) {
    // Change ONE operator's parallelism on a running job.
    //
    // The mechanism is a replan, not an in-place cutover: validate, drain the
    // job, re-derive the whole task set from the retained job graph at the new
    // parallelism, and redeploy from the last completed checkpoint with each
    // subtask told which parent's state it inherits. Everything that makes a
    // task set correct - chain specs, edge fan-out, key-group ranges - comes
    // from the planner, which is the same code submit uses.
    //
    // The alternative, resizing the deployed task set by cloning a
    // DeploymentTask, is what the role-based path does, and it cannot work for
    // a multi-operator job: a task's operator identity lives inside its packed
    // OperatorChainSpec, which cloning does not rewrite (F41).
    //
    // The cost is that this is a stop: the job stops, then starts again from
    // the checkpoint. Exactly-once is preserved by the same mechanism failover
    // uses - a replayable source plus a transactional sink - and NOT by the
    // rescale being seamless.
    auto refuse = [&](const std::string& reason) {
        log::warn("coordinator.rescale",
                  "refused job_id=" + std::to_string(job_id) + " op_id=" + op_id +
                      " target=" + std::to_string(new_parallelism) + ": " + reason);
        clink::metrics::orch::rescale_request_rejected();
        return RescaleCoordinator::RequestResult{.ok = false, .reason = reason};
    };

    std::vector<std::shared_ptr<network::Connection>> worker_conns;
    std::vector<PendingDeploy> hot_frames;
    bool hot_engaged = false;
    std::uint32_t old_parallelism = 0;
    // The cutover checkpoint a hot cutover reserved.
    std::uint64_t hot_cutover_id = 0;
    // A hot cutover reserves the job's next checkpoint id, and only once it is
    // on record: the arm that names it leaves straight after the lock hold
    // below. The job's claimer has normally put it there already; when it has
    // not, it is claimed here, before the lock, and a claim that fails sends
    // the request to the replan, which needs no id of its own.
    std::string hot_record_error;
    {
        std::shared_ptr<JobState::TriggeredRecord> record;
        std::uint64_t want = 0;
        {
            std::lock_guard lock(mu_);
            if (auto it = jobs_.find(job_id); it != jobs_.end() && cfg_.hot_rescale_enabled) {
                const auto& job = *it->second;
                if (!job.checkpoint.checkpoint_dir.empty() &&
                    job.triggered_record->owned.load(std::memory_order_acquire) <
                        job.next_checkpoint_id) {
                    record = job.triggered_record;
                    want = job.next_checkpoint_id;
                }
            }
        }
        if (record) {
            try {
                if (!claim_checkpoint_id_(*record, want)) {
                    hot_record_error =
                        "another coordinator of this job holds ids up to " +
                        std::to_string(record->held_elsewhere.load(std::memory_order_acquire));
                }
            } catch (const std::exception& e) {
                hot_record_error = e.what();
            }
        }
    }
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            return RescaleCoordinator::RequestResult{.ok = false, .reason = "unknown job_id"};
        }
        auto& job = *it->second;
        if (!job.rescale_coordinator) {
            return refuse("job has no rescale coordinator (rescale not enabled)");
        }
        if (job.completion_signalled) {
            return refuse("job has already completed");
        }
        if (job.cancel_requested) {
            return refuse("a cancel is in progress for this job");
        }
        if (job.awaiting_restart) {
            return refuse(
                "this job is already draining for a restart or rescale; wait for it to finish");
        }
        if (job.graph_json.empty()) {
            return refuse(
                "this job has no retained graph, so it cannot be replanned at a new "
                "parallelism");
        }
        // The operator has to exist in the graph, and the graph is what gets
        // replanned - so check there rather than against the deployed set.
        JobGraphSpec graph;
        try {
            graph = JobGraphSpec::from_json(job.graph_json);
        } catch (const std::exception& e) {
            return refuse(std::string{"the job's retained graph does not parse: "} + e.what());
        }
        const auto op_it = std::find_if(graph.ops.begin(),
                                        graph.ops.end(),
                                        [&](const OperatorSpec& o) { return o.id == op_id; });
        if (op_it == graph.ops.end()) {
            std::string known;
            for (const auto& o : graph.ops) {
                known += (known.empty() ? "" : ", ") + o.id;
            }
            return refuse("this job has no operator '" + op_id + "'. It has: " + known);
        }

        // How many subtasks the operator is ACTUALLY running, which is what its
        // snapshots correspond to. The graph's value is the fallback for a job
        // whose tasks did not come from the chain planner.
        for (const auto& [_, ident] : job.task_op_identity) {
            if (ident.op_id == op_id) {
                ++old_parallelism;
            }
        }
        if (old_parallelism == 0) {
            old_parallelism = op_it->parallelism;
        }
        if (new_parallelism == 0) {
            return refuse("parallelism must be at least 1");
        }
        if (new_parallelism == old_parallelism) {
            return refuse("operator '" + op_id + "' already runs at parallelism " +
                          std::to_string(new_parallelism) + "; nothing to do");
        }
        if (is_forced_singleton(*op_it)) {
            return refuse("operator '" + op_id +
                          "' requires a global SQL view and must remain at parallelism 1");
        }
        // Integer factor only, and refused HERE rather than discovered after
        // the job has already drained. The same helper the redeploy uses.
        if (const auto mapping = rescale_parent_mapping(old_parallelism, new_parallelism, 0);
            !mapping.ok) {
            return refuse(mapping.error + " (operator '" + op_id + "' currently runs " +
                          std::to_string(old_parallelism) + ")");
        }
        // Declared bounds. An operator with no bounds is not scalable by
        // policy: whoever built the job did not say what range is safe, and
        // guessing is worse than refusing. `.rescalable(min, max)` on the
        // fluent API or min/max_parallelism on the spec declares it.
        if (op_it->min_parallelism == 0 && op_it->max_parallelism == 0) {
            return refuse("operator '" + op_id +
                          "' declares no rescale bounds, so it is not scalable. Set them with "
                          ".rescalable(min, max) on the stream, or min_parallelism / "
                          "max_parallelism on the operator spec");
        }
        if (new_parallelism < op_it->min_parallelism) {
            return refuse("requested parallelism " + std::to_string(new_parallelism) +
                          " is below operator '" + op_id + "' min_parallelism " +
                          std::to_string(op_it->min_parallelism));
        }
        if (new_parallelism > op_it->max_parallelism) {
            return refuse("requested parallelism " + std::to_string(new_parallelism) +
                          " is above operator '" + op_id + "' max_parallelism " +
                          std::to_string(op_it->max_parallelism));
        }
        // Feasibility, checked after the request itself is known to make sense.
        // Order matters for the message the caller gets: telling someone who
        // mistyped an operator name to "retry once a checkpoint has landed"
        // sends them to wait for something that will not help.
        //
        // A replan redeploys from a checkpoint, so there has to be one. Without
        // periodic checkpointing there never will be, and the operator's state
        // would silently start empty at the new parallelism.
        if (job.checkpoint.checkpoint_dir.empty() || job.checkpoint.interval_ms <= 0) {
            return refuse(
                "operator rescale requires periodic checkpointing (set checkpoint_dir and "
                "interval_ms > 0); the new subtasks restore their state from a completed "
                "checkpoint, and without one they would start empty");
        }
        if (job.latest_completed_checkpoint_id == 0) {
            return refuse(
                "no checkpoint has completed yet for this job; the rescale would have no "
                "state to restore from. Retry once one has landed");
        }
        // Capacity for the growth. Scale-down always fits. The deployed tasks
        // still hold their slots now and free them during the drain, so only
        // the net increase has to be available.
        if (new_parallelism > old_parallelism) {
            const std::uint32_t growth = new_parallelism - old_parallelism;
            std::size_t total_free = 0;
            for (const auto& [_, worker] : registered_) {
                if (!worker->lost && worker->conn && worker->slot_capacity > worker->slots_in_use) {
                    total_free += (worker->slot_capacity - worker->slots_in_use);
                }
            }
            if (total_free < growth) {
                return refuse("rescaling '" + op_id + "' from " + std::to_string(old_parallelism) +
                              " to " + std::to_string(new_parallelism) + " needs " +
                              std::to_string(growth) + " more slot(s); the cluster has " +
                              std::to_string(total_free) + " free");
            }
        }

        // Record the request on the state machine too, so
        // operator_rescale_status and the autoscaler's cooldown see a rescale
        // in flight. It validates bounds a second time against what deploy
        // registered; a disagreement between that and the graph is a bug worth
        // surfacing rather than papering over.
        auto sm = job.rescale_coordinator->request_rescale(op_id, new_parallelism);
        if (!sm.ok) {
            return refuse(sm.reason);
        }

        // Hot first (design record 008): an eligible operator cuts over in
        // place at a checkpoint barrier and nothing else stops. Any
        // ineligibility - and any later failure - lands on the replan
        // below, which is the proven path.
        std::string hot_reason;
        if (try_begin_hot_cutover_locked_(job,
                                          op_id,
                                          new_parallelism,
                                          old_parallelism,
                                          graph,
                                          hot_record_error,
                                          hot_frames,
                                          hot_reason)) {
            hot_engaged = true;
            hot_cutover_id = job.hot_cutover->cutover_checkpoint;
        } else {
            log::info("coordinator.rescale",
                      "hot cutover not taken for job_id=" + std::to_string(job_id) +
                          " op_id=" + op_id + " (" + hot_reason + "); using the replan path");
            // Stage the replan and start the drain. handle_subtask_finished_
            // routes the drain acks and fires restart_job_locked_ when the
            // last one lands.
            job.pending_op_parallelism[op_id] = new_parallelism;
            job.pre_rescale_op_parallelism[op_id] = old_parallelism;
            // Marks the run as outside the exactly-once specification's scope:
            // the model fixes the sink set and its hosts, and a rescale changes
            // both. The trace validator skips a run that carries this.
            if (protocol_trace::enabled()) {
                protocol_trace::Event("Rescale")
                    .u("job", job.id)
                    .s("op", op_id)
                    .u("from", old_parallelism)
                    .u("to", new_parallelism)
                    .s("mode", "replan")
                    .emit();
            }
            // Start of the clink.rescale lifecycle span (mode=replan),
            // recorded when restart_job_locked_ emits the replanned deploys.
            if (clink::metrics::SpanBuffer::global().enabled()) {
                job.rescale_span_start_unix_nano = clink::metrics::otlp_now_unix_nano();
            }
            job.awaiting_restart = true;
            job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
            populate_restart_drain_locked_(job);
            for (const auto& [worker_id, _] : job.tasks_by_worker) {
                auto worker_it = registered_.find(worker_id);
                if (worker_it != registered_.end() && !worker_it->second->lost &&
                    worker_it->second->conn) {
                    worker_conns.push_back(worker_it->second->conn);
                }
            }
            log::info("coordinator.rescale",
                      "accepted job_id=" + std::to_string(job_id) + " op_id=" + op_id + " " +
                          std::to_string(old_parallelism) + "->" + std::to_string(new_parallelism) +
                          "; draining " + std::to_string(worker_conns.size()) +
                          " worker connection(s) before replanning from checkpoint " +
                          std::to_string(job.latest_completed_checkpoint_id));
        }
    }

    if (hot_engaged) {
        // The cutover was armed under the lock above and its arm frames have
        // not gone out. A worker loss or the watchdog's phase deadline can
        // abort it in between, and the abort's CancelJob and replan are then
        // already on their way: arm frames for a cutover the coordinator has
        // dropped would arm subtasks to stop at a barrier nobody will send.
        // So they go only if this is still the cutover the request armed and
        // it is still waiting for its arms.
        CLINK_FAULT_POINT(clink::fault::points::kHotCutoverBeforeArm);
        std::lock_guard lock(mu_);
        const auto it = jobs_.find(job_id);
        const bool still_arming =
            it != jobs_.end() && it->second->hot_cutover.has_value() &&
            it->second->hot_cutover->cutover_checkpoint == hot_cutover_id &&
            it->second->hot_cutover->phase == JobState::HotCutover::Phase::Arming;
        if (!still_arming) {
            log::info("coordinator.rescale",
                      "hot cutover at checkpoint " + std::to_string(hot_cutover_id) +
                          " for job_id=" + std::to_string(job_id) +
                          " ended before its arm frames were sent; they are dropped");
            return RescaleCoordinator::RequestResult{.ok = true,
                                                     .accepted_target = new_parallelism};
        }
        // Sent in the same lock hold as the check, so on every connection the
        // arm frames precede any abort's frames: an abort runs under the lock
        // and sends after it. Sent after the lock, as they were, an abort
        // landing between the check and the send could get its CancelJob out
        // first. Arming subtasks that are being cancelled is harmless (a
        // union's UnionRebindSlot hold yields to the runner's stop predicate,
        // await_armed_cutover_commit returns on the cancel token, and
        // GroupCutoverGate's waits take the stop probe), but an arm held up
        // past the abort's drain and redeploy, on a stalled sender or behind a
        // slow send to a lost worker, reaches the replan's new subtasks, whose
        // arm callbacks Worker::handle_begin_rescale_ finds under the same job
        // and operator, for a checkpoint that is never triggered: their unions
        // hold open at end of input, a sink waits out its bound for a commit
        // that never comes, and the feeding groups' gates refuse the next
        // cutover's arm. Arm frames are a few dozen bytes, and
        // send_peer_updates_locked_ sends under the lock too.
        CLINK_FAULT_POINT(clink::fault::points::kHotCutoverArmSend);
        for (auto& f : hot_frames) {
            if (f.conn != nullptr) {
                send_frame(*f.conn, f.frame);
            }
        }
        // Arm frames only; the job keeps running.
        return RescaleCoordinator::RequestResult{.ok = true, .accepted_target = new_parallelism};
    }

    // Drain outside the lock, as rescale_job does.
    CancelJobMsg cj;
    cj.job_id = job_id;
    const auto frame = fenced_frame_(MessageKind::CancelJob, cj);
    for (const auto& c : worker_conns) {
        send_frame(*c, frame);
    }
    return RescaleCoordinator::RequestResult{.ok = true, .accepted_target = new_parallelism};
}

namespace {
// Strip the branch (".N") and side-output ("::tag") suffixes an
// OperatorSpec.inputs entry may carry, leaving the producer op id. The
// planner has its own richer parse; the choreography only needs the id.
std::string producer_id_of_input_ref(const std::string& raw) {
    if (const auto colons = raw.find("::"); colons != std::string::npos) {
        return raw.substr(0, colons);
    }
    const auto dot = raw.rfind('.');
    if (dot == std::string::npos || dot + 1 >= raw.size()) {
        return raw;
    }
    for (std::size_t i = dot + 1; i < raw.size(); ++i) {
        if (raw[i] < '0' || raw[i] > '9') {
            return raw;
        }
    }
    return raw.substr(0, dot);
}
}  // namespace

void Coordinator::populate_restart_drain_locked_(JobState& job) {
    // Only a subtask on a LIVE worker can drain: the CancelJob broadcast
    // that starts the drain skips workers that are unregistered or lost,
    // so nothing ever asks the others, and waiting for them burns the
    // whole deadline before the watchdog fails the job.
    //
    // One exception: a lost session that is still retiring its predecessor
    // did not hold the predecessor's subtasks (mark_worker_lost_locked_ kept
    // them pending). The predecessor's queued frames drain them, or its
    // retirement, which runs once those frames are handled, folds them out
    // of this set. Redeployed now instead, a SubtaskFinished still queued for
    // one of them is handled against the new run.
    std::unordered_set<std::string> in_flight;
    for (const auto& [worker_id, pending] : job.pending_per_worker) {
        auto it = registered_.find(worker_id);
        if (it == registered_.end()) {
            continue;
        }
        const auto& worker = *it->second;
        const bool retiring_lost = worker.lost && worker.retiring_predecessor;
        if (worker.lost && !retiring_lost) {
            continue;
        }
        for (const auto& [role, sub] : pending) {
            const std::string key = role + ":" + std::to_string(sub);
            if (retiring_lost) {
                const auto stamp = job.pending_session.find(key);
                if (stamp == job.pending_session.end() || stamp->second >= worker.session) {
                    continue;
                }
            }
            in_flight.insert(key);
        }
    }
    job.restart_drain_expected = in_flight;
    // Everything not draining has to be redeployed, or a subtask stranded
    // on a dead worker is in neither set and simply never comes back:
    // restart_job_locked_ builds its task set from the union of the two.
    for (const auto& [worker_id, dts] : job.tasks_by_worker) {
        for (const auto& dt : dts) {
            const std::string k = dt.role + ":" + std::to_string(dt.subtask_idx);
            if (in_flight.count(k) != 0) {
                continue;
            }
            const bool already = std::any_of(
                job.restart_pending.begin(), job.restart_pending.end(), [&](const auto& p) {
                    return p.first == dt.role && p.second == dt.subtask_idx;
                });
            if (!already) {
                job.restart_pending.emplace_back(dt.role, dt.subtask_idx);
            }
        }
    }
}

bool Coordinator::try_begin_hot_cutover_locked_(JobState& job,
                                                const std::string& op_id,
                                                std::uint32_t new_parallelism,
                                                std::uint32_t old_parallelism,
                                                const JobGraphSpec& graph,
                                                const std::string& id_record_error,
                                                std::vector<PendingDeploy>& out_frames,
                                                std::string& reason) {
    if (!cfg_.hot_rescale_enabled) {
        reason = "hot rescale disabled by configuration";
        return false;
    }
    if (job.hot_cutover.has_value()) {
        reason = "another hot cutover is in flight for this job";
        return false;
    }
    const OperatorSpec* target = nullptr;
    for (const auto& op : graph.ops) {
        if (op.id == op_id) {
            target = &op;
        }
    }
    if (target == nullptr) {
        reason = "operator not in the retained graph";
        return false;
    }
    // Every edge INTO the op must be fan-shaped both before and after the
    // cutover (the deployed groups were only built swappable if they were
    // fan at deploy), and every edge OUT of it likewise - a forward 1:1
    // premise does not survive a parallelism change, and the replan path
    // owns that case.
    std::vector<std::string> feeder_ops;
    for (const auto& raw : target->inputs) {
        const auto fid = producer_id_of_input_ref(raw);
        for (const auto& op : graph.ops) {
            if (op.id != fid) {
                continue;
            }
            const bool fan_now = !target->key_by.empty() || op.parallelism != old_parallelism;
            const bool fan_after = !target->key_by.empty() || op.parallelism != new_parallelism;
            if (!fan_now || !fan_after) {
                reason = "edge " + fid + " -> " + op_id +
                         " is forward-shaped; only fan edges "
                         "(keyed or parallelism-mismatched) can cut over in place";
                return false;
            }
            feeder_ops.push_back(fid);
        }
    }
    std::vector<std::string> fed_ops;
    for (const auto& op : graph.ops) {
        for (const auto& raw : op.inputs) {
            if (producer_id_of_input_ref(raw) != op_id) {
                continue;
            }
            const bool fan_now = !op.key_by.empty() || op.parallelism != old_parallelism;
            const bool fan_after = !op.key_by.empty() || op.parallelism != new_parallelism;
            if (!fan_now || !fan_after) {
                reason = "edge " + op_id + " -> " + op.id +
                         " is forward-shaped; only fan edges "
                         "(keyed or parallelism-mismatched) can cut over in place";
                return false;
            }
            fed_ops.push_back(op.id);
        }
    }

    // The op's deployed block, captured while the identity records still
    // describe it (the teardown erases them).
    const auto blocks = derive_op_index_blocks(job.task_op_identity);
    const auto blk = blocks.find(op_id);
    if (blk == blocks.end() || !blk->second.consistent ||
        blk->second.parallelism != old_parallelism) {
        reason = "the deployed identity for '" + op_id + "' cannot be trusted";
        return false;
    }

    // The full post-cutover plan, validated before anything is armed.
    // Plugin jobs plan with their bundle's registries, exactly as the
    // replan path does.
    auto plan = job.bundle != nullptr ? plan_hot_cutover(graph,
                                                         op_id,
                                                         new_parallelism,
                                                         job.task_op_identity,
                                                         job.bundle->operator_registry(),
                                                         &job.bundle->runner_registry())
                                      : plan_hot_cutover(graph,
                                                         op_id,
                                                         new_parallelism,
                                                         job.task_op_identity,
                                                         OperatorRegistry::default_instance());
    if (!plan.ok) {
        reason = plan.error;
        return false;
    }

    // A cutover never changes the job's Row sidecar layout: every feeder of
    // the new subtasks keeps the one it was deployed with. So while that is
    // the second layout, the new subtasks may go only to workers that read
    // it, as the deploy places them. The pool is foreseen here as the deploy
    // will see it: today's free slots plus those the operator's old subtasks
    // give back at the teardown. The request is refused only when the
    // workers that read the layout cannot seat the plan and an older worker
    // has the room it needs; a plain lack of room is the deploy's to report
    // (it aborts to the replan). The deploy repeats the restriction against
    // the workers registered by then.
    if (job.row_layout == kRowLayoutV2) {
        std::vector<CutoverCandidate> candidates;
        for (const auto& [worker_id, worker] : registered_) {
            if (worker->lost || !worker->conn) {
                continue;
            }
            std::uint32_t free = worker->slot_capacity > worker->slots_in_use
                                     ? worker->slot_capacity - worker->slots_in_use
                                     : 0;
            for (const auto& [key, ident] : job.task_op_identity) {
                if (ident.op_id != op_id) {
                    continue;
                }
                const auto rec = job.task_records.find(key);
                if (rec == job.task_records.end() || rec->second.first != worker_id) {
                    continue;
                }
                const auto stamp = job.pending_session.find(key);
                if (stamp == job.pending_session.end() || stamp->second == worker->session) {
                    ++free;
                }
            }
            candidates.push_back(CutoverCandidate{worker_id, worker->protocol_version, free});
        }
        if (const auto blocker = layout_two_cutover_blocker(plan.tasks, candidates);
            blocker.has_value()) {
            reason = "the job runs row layout 2 and the new subtasks of '" + op_id +
                     "' need the free slots of worker '" + blocker->worker_id +
                     "', which registered at protocol v" +
                     std::to_string(blocker->protocol_version) + "; layout 2 needs v" +
                     std::to_string(kRowLayoutV2ProtocolVersion);
            return false;
        }
    }

    // The cutover checkpoint is the job's next id, and it must be on record
    // before the arm that names it leaves (claim_checkpoint_id_). The caller
    // claimed it before taking the lock; a periodic trigger can still have
    // taken that id in between, or the claim failed, and then the replan,
    // which needs no id of its own, takes over.
    if (job.triggered_record->owned.load(std::memory_order_acquire) < job.next_checkpoint_id) {
        reason = "could not record cutover checkpoint id " +
                 std::to_string(job.next_checkpoint_id) +
                 (id_record_error.empty() ? std::string{" (not on record yet)"}
                                          : " (" + id_record_error + ")");
        return false;
    }

    JobState::HotCutover hot;
    hot.op_id = op_id;
    hot.old_parallelism = old_parallelism;
    hot.target_parallelism = new_parallelism;
    hot.old_block_base = blk->second.base;
    hot.planned_tasks = std::move(plan.tasks);
    hot.appended_base = hot.planned_tasks.front().subtask_idx;
    // Reserve the cutover checkpoint id NOW: the arm names it, the runners
    // stop exactly at it, and the trigger sweep is already gated off this
    // job (hot_cutover engages under the same lock hold), so no other id
    // can be issued in between.
    hot.cutover_checkpoint = job.next_checkpoint_id++;

    // Task classes, from the identity records.
    std::unordered_set<std::string> feeder_set(feeder_ops.begin(), feeder_ops.end());
    std::unordered_set<std::string> fed_set(fed_ops.begin(), fed_ops.end());
    std::uint32_t op_tasks = 0;
    for (const auto& [key, ident] : job.task_op_identity) {
        if (ident.op_id == op_id) {
            ++op_tasks;
        } else if (feeder_set.count(ident.op_id) != 0) {
            hot.feeder_task_keys.push_back(key);
        } else if (fed_set.count(ident.op_id) != 0) {
            hot.fed_task_keys.push_back(key);
        }
    }
    hot.expected_op_tasks = op_tasks;
    hot.expected_groups = static_cast<std::uint32_t>(hot.feeder_task_keys.size());
    hot.expected_rebind_tasks = static_cast<std::uint32_t>(hot.fed_task_keys.size());

    // Arm every worker hosting any of the three classes. A worker with
    // nothing registered still acks (with zeros), which is exactly how a
    // legacy-built task surfaces as a shortfall.
    std::unordered_set<std::string> arm_workers;
    auto worker_of = [&](const std::string& key) -> std::string {
        auto it = job.task_records.find(key);
        return it == job.task_records.end() ? std::string{} : it->second.first;
    };
    for (const auto& [key, ident] : job.task_op_identity) {
        if (ident.op_id == op_id || feeder_set.count(ident.op_id) != 0 ||
            fed_set.count(ident.op_id) != 0) {
            if (auto w = worker_of(key); !w.empty()) {
                arm_workers.insert(w);
            }
        }
    }
    if (arm_workers.empty()) {
        reason = "no live workers host the operator or its neighbours";
        return false;
    }
    hot.arm_workers_pending = arm_workers;
    hot.phase = JobState::HotCutover::Phase::Arming;
    hot.phase_deadline = std::chrono::steady_clock::now() + cfg_.hot_cutover_phase_timeout;
    if (clink::metrics::SpanBuffer::global().enabled()) {
        hot.span_start_unix_nano = clink::metrics::otlp_now_unix_nano();
    }

    BeginRescaleMsg arm;
    arm.job_id = job.id;
    arm.op_id = op_id;
    arm.target_parallelism = new_parallelism;
    arm.cutover_checkpoint = hot.cutover_checkpoint;
    const auto frame = fenced_frame_(MessageKind::BeginRescale, arm);
    for (const auto& worker_id : arm_workers) {
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back({it->second->conn, frame});
        }
    }

    job.hot_cutover = std::move(hot);
    // Outside the specification's scope (see the replan path's marker).
    if (protocol_trace::enabled()) {
        protocol_trace::Event("Rescale")
            .u("job", job.id)
            .s("op", op_id)
            .u("from", old_parallelism)
            .u("to", new_parallelism)
            .s("mode", "hot")
            .emit();
    }
    log::info("coordinator.rescale",
              "hot cutover armed job_id=" + std::to_string(job.id) + " op_id=" + op_id + " " +
                  std::to_string(old_parallelism) + "->" + std::to_string(new_parallelism) +
                  " cutover_checkpoint=" + std::to_string(job.hot_cutover->cutover_checkpoint) +
                  " arm_workers=" + std::to_string(arm_workers.size()) +
                  " feeders=" + std::to_string(job.hot_cutover->expected_groups) +
                  " fed=" + std::to_string(job.hot_cutover->expected_rebind_tasks));
    return true;
}

void Coordinator::set_hot_cut_ack_pin_locked_(std::uint64_t checkpoint_id,
                                              const std::vector<std::string>& fed_task_keys) {
    auto keys = std::make_shared<std::unordered_set<std::string>>(fed_task_keys.begin(),
                                                                  fed_task_keys.end());
    {
        std::lock_guard pin_lock(hot_cut_ack_pin_mu_);
        hot_cut_ack_pin_keys_ = std::move(keys);
    }
    hot_cut_ack_pin_ckpt_.store(checkpoint_id, std::memory_order_relaxed);
}

void Coordinator::clear_hot_cut_ack_pin_locked_() {
    hot_cut_ack_pin_ckpt_.store(0, std::memory_order_relaxed);
    std::lock_guard pin_lock(hot_cut_ack_pin_mu_);
    hot_cut_ack_pin_keys_.reset();
}

void Coordinator::hot_cutover_trigger_c_locked_(JobState& job,
                                                std::vector<PendingDeploy>& out_frames) {
    // Every arm ack has landed; the cutover checkpoint has not been
    // triggered yet. A Delay here holds the pre-C window open.
    CLINK_FAULT_POINT(clink::fault::points::kHotCutoverBeforeTrigger);
    auto& hot = *job.hot_cutover;
    const auto ckpt_id = hot.cutover_checkpoint;
    // C was on record before it was reserved (try_begin_hot_cutover_locked_
    // reserves only an id this coordinator owns), so its barrier follows the
    // record. Checked here all the same, at the one barrier that leaves from
    // another call than the one that reserved it: an arm ack names no
    // checkpoint, so a late ack from an earlier cutover of the same operator
    // could arrive first.
    if (job.triggered_record->owned.load(std::memory_order_acquire) < ckpt_id) {
        abort_hot_cutover_locked_(
            job,
            "cutover checkpoint id " + std::to_string(ckpt_id) + " is not on record yet",
            out_frames);
        return;
    }
    std::unordered_set<std::string> pending;
    for (const auto& [key, _] : job.task_records) {
        pending.insert(key);
    }
    job.pending_checkpoint_acks[ckpt_id] = std::move(pending);
    {
        auto& rec = job.checkpoint_participants[ckpt_id];
        rec.generation = job.state_generation;
        rec.subtasks.clear();
        for (const auto& [key, _unused] : job.task_records) {
            const auto colon = key.rfind(':');
            if (colon == std::string::npos) {
                continue;
            }
            try {
                rec.subtasks.insert(static_cast<std::uint32_t>(std::stoul(key.substr(colon + 1))));
            } catch (const std::exception&) {
                continue;
            }
        }
    }
    job.pending_checkpoint_start_times[ckpt_id] = std::chrono::steady_clock::now();
    clink::metrics::ckpt::triggered();
    // The cut checkpoint is triggered here, not by the trigger loop. A run
    // that rescales is outside the specification's scope (the trace's Rescale
    // event), but its trace still carries every trigger.
    if (protocol_trace::enabled()) {
        protocol_trace::Event("Trigger")
            .u("job", job.id)
            .u("ckpt", ckpt_id)
            .u("epoch", epoch())
            .b("cutover", true)
            .emit();
    }
    TriggerCheckpointMsg tc;
    tc.job_id = job.id;
    tc.checkpoint_id = ckpt_id;
    tc.generation = job.state_generation;
    const auto frame = fenced_frame_(MessageKind::TriggerCheckpoint, tc);
    for (const auto& [worker_id, _] : job.tasks_by_worker) {
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back({it->second->conn, frame});
        }
    }
    hot.phase = JobState::HotCutover::Phase::AwaitingCut;
    hot.phase_deadline = std::chrono::steady_clock::now() + cfg_.hot_cutover_phase_timeout;
    set_hot_cut_ack_pin_locked_(ckpt_id, hot.fed_task_keys);
    log::info("coordinator.rescale",
              "hot cutover checkpoint triggered job_id=" + std::to_string(job.id) +
                  " op_id=" + hot.op_id + " ckpt_id=" + std::to_string(ckpt_id));
}

void Coordinator::hot_cutover_begin_rebind_locked_(JobState& job,
                                                   std::vector<PendingDeploy>& out_frames) {
    // Every old subtask has drained at the cutover checkpoint; the rebind
    // has not gone out. Between here and the deploy the operator has no
    // running subtasks - the window a hold-open or a worker kill aims at.
    CLINK_FAULT_POINT(clink::fault::points::kHotCutoverCuttingOver);
    auto& hot = *job.hot_cutover;
    clear_hot_cut_ack_pin_locked_();
    // Tear down the drained old subtasks' bookkeeping. Their
    // SubtaskFinished arrivals were counted as drained acks, not as
    // completions; remove records, identity and slots so the job's
    // accounting describes the survivors plus (soon) the new tasks.
    std::vector<std::string> teardown;
    for (const auto& [key, ident] : job.task_op_identity) {
        if (ident.op_id == hot.op_id) {
            teardown.push_back(key);
        }
    }
    for (const auto& key : teardown) {
        auto rec_it = job.task_records.find(key);
        if (rec_it == job.task_records.end()) {
            continue;
        }
        const auto worker_id = rec_it->second.first;
        release_task_slot_locked_(job, worker_id, key);
        if (auto tbt = job.tasks_by_worker.find(worker_id); tbt != job.tasks_by_worker.end()) {
            std::erase_if(tbt->second, [&](const DeploymentTask& t) {
                return t.role == rec_it->second.second.role &&
                       t.subtask_idx == rec_it->second.second.subtask_idx;
            });
            if (tbt->second.empty()) {
                job.tasks_by_worker.erase(tbt);
            }
        }
        if (auto ppw = job.pending_per_worker.find(worker_id);
            ppw != job.pending_per_worker.end()) {
            std::erase_if(ppw->second, [&](const auto& p) {
                return p.first == rec_it->second.second.role &&
                       p.second == rec_it->second.second.subtask_idx;
            });
        }
        job.task_records.erase(rec_it);
        job.task_op_identity.erase(key);
        if (job.expected_completion > 0) {
            --job.expected_completion;
        }
    }
    // The teardown above released every old subtask's slot, the early ones
    // included; an abort from here on must not release them again.
    hot.early_drains.clear();

    // Ask the fed tasks to bind listeners for the new upstream indices.
    hot.rebind_ports_pending.clear();
    std::unordered_set<std::string> fed_workers;
    for (const auto& key : hot.fed_task_keys) {
        auto rec_it = job.task_records.find(key);
        if (rec_it == job.task_records.end()) {
            continue;
        }
        fed_workers.insert(rec_it->second.first);
        for (std::uint32_t i = 0; i < hot.target_parallelism; ++i) {
            hot.rebind_ports_pending.insert({key, hot.appended_base + i});
        }
    }
    hot.phase = JobState::HotCutover::Phase::Rebinding;
    hot.phase_deadline = std::chrono::steady_clock::now() + cfg_.hot_cutover_phase_timeout;
    if (hot.rebind_ports_pending.empty()) {
        // No fed tasks (the op ends in a sink chain): deploy directly.
        hot_cutover_deploy_locked_(job, out_frames);
        return;
    }
    CutoverRebindMsg rb;
    rb.job_id = job.id;
    rb.op_id = hot.op_id;
    rb.upstream_role = kGenericSubtaskRole;
    for (std::uint32_t i = 0; i < hot.target_parallelism; ++i) {
        rb.new_subtask_indices.push_back(hot.appended_base + i);
    }
    const auto frame = fenced_frame_(MessageKind::CutoverRebind, rb);
    for (const auto& worker_id : fed_workers) {
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back({it->second->conn, frame});
        }
    }
    log::info("coordinator.rescale",
              "hot cutover rebind dispatched job_id=" + std::to_string(job.id) +
                  " op_id=" + hot.op_id + " fed_workers=" + std::to_string(fed_workers.size()) +
                  " awaited_ports=" + std::to_string(hot.rebind_ports_pending.size()));
}

void Coordinator::hot_cutover_deploy_locked_(JobState& job,
                                             std::vector<PendingDeploy>& out_frames) {
    auto& hot = *job.hot_cutover;
    // Place the validated plan onto free slots. While the job runs the Row
    // sidecar's second layout, only on workers that read it: eligibility
    // checked the placement it foresaw, and a worker that registered since
    // must not take a new subtask the feeders will send typed columns to.
    std::vector<PlacementWorker> workers;
    for (const auto& [worker_id, worker] : registered_) {
        if (worker->lost || !worker->conn) {
            continue;
        }
        if (job.row_layout == kRowLayoutV2 &&
            effective_protocol_version(worker->protocol_version) < kRowLayoutV2ProtocolVersion) {
            continue;
        }
        const std::uint32_t free = worker->slot_capacity > worker->slots_in_use
                                       ? worker->slot_capacity - worker->slots_in_use
                                       : 0;
        if (free > 0) {
            workers.push_back(PlacementWorker{worker_id, free});
        }
    }
    auto tasks = hot.planned_tasks;
    if (!assign_task_placement(tasks, workers)) {
        abort_hot_cutover_locked_(job,
                                  job.row_layout == kRowLayoutV2
                                      ? "no free slots for the post-cutover subtasks on workers "
                                        "that read the job's row layout 2"
                                      : "no free slots for the post-cutover subtasks",
                                  out_frames);
        return;
    }

    std::unordered_map<std::string, std::vector<DeploymentTask>> by_worker;
    for (const auto& t : tasks) {
        DeploymentTask d;
        d.role = t.role;
        d.subtask_idx = t.subtask_idx;
        d.data_port = 0;
        d.extra_config = t.extra_config;
        d.key_group_first = t.key_group_first;
        d.key_group_last = t.key_group_last;
        d.restore_from_subtask_idx = t.restore_from_subtask_idx;
        d.restore_from_parent_count = t.restore_from_parent_count;
        d.succeeds_first = t.succeeds_first;
        d.succeeds_count = t.succeeds_count;
        for (const auto& [pr_role, pr_sub] : t.peer_refs) {
            PeerAddress p;
            p.role = pr_role;
            p.subtask_idx = pr_sub;
            const std::string peer_key = pr_role + ":" + std::to_string(pr_sub);
            if (auto rec = job.task_records.find(peer_key); rec != job.task_records.end()) {
                if (auto w = registered_.find(rec->second.first); w != registered_.end()) {
                    p.host = w->second->data_host;
                }
            }
            d.peers.push_back(std::move(p));
        }
        const std::string key = d.role + ":" + std::to_string(d.subtask_idx);
        job.task_records[key] = {t.worker_id, d};
        job.task_op_identity[key] = JobState::TaskOpIdentity{
            .op_id = hot.op_id, .subtask_idx_in_op = t.subtask_idx - hot.appended_base};
        job.pending_per_worker[t.worker_id].emplace_back(d.role, d.subtask_idx);
        job.tasks_by_worker[t.worker_id].push_back(d);
        job.pending_session[key] = 0;
        if (auto w = registered_.find(t.worker_id); w != registered_.end()) {
            job.pending_session[key] = w->second->session;
            charge_slots_locked_(*w->second, 1);
        }
        ++job.expected_completion;
        ++job.expected_listenings;
        by_worker[t.worker_id].push_back(std::move(d));
    }
    ++job.topology_version;
    clink::metrics::orch::rescale_cutover_deploy();

    std::size_t stamped_ops = 0;
    for (auto& [worker_id, wtasks] : by_worker) {
        auto it = registered_.find(worker_id);
        if (it == registered_.end() || it->second->lost || !it->second->conn) {
            continue;
        }
        DeployMsg dm;
        dm.job_id = job.id;
        dm.tasks = std::move(wtasks);
        dm.plugins = plugins_for_worker_locked_(job, *it->second);
        dm.checkpoint_dir = job.checkpoint.checkpoint_dir;
        dm.state_backend_uri = job.checkpoint.state_backend_uri;
        dm.capture_dir = job.checkpoint.capture_dir;
        dm.capture_records = job.checkpoint.capture_records;
        // The new subtasks restore from the CUTOVER checkpoint - the last
        // thing the old subtasks processed - in the SAME state generation
        // (their appended directories are fresh; nothing is overwritten).
        // The dormant path forgot the generation pair and deployed new
        // tasks at generation 1 regardless of the job's.
        dm.restore_from_dir = job.checkpoint.checkpoint_dir;
        dm.restore_from_checkpoint_id = hot.cutover_checkpoint;
        dm.generation = job.state_generation;
        dm.restore_generation = job.state_generation;
        dm.unaligned_checkpoints = job.checkpoint.alignment == CheckpointAlignment::Unaligned;
        dm.adaptive_barrier_mode = job.checkpoint.alignment == CheckpointAlignment::Adaptive;
        dm.expected_state_versions_packed = job.expected_state_versions_packed;
        dm.udfs_packed = job.udfs_packed;
        // The new subtasks join running feeders and consumers, which keep the
        // layout they were deployed with: the job's current one, never a
        // recomputed one.
        stamped_ops += stamp_row_layout(dm.tasks, job.row_layout);
        out_frames.push_back({it->second->conn, fenced_frame_(MessageKind::Deploy, dm)});
    }
    hot.phase = JobState::HotCutover::Phase::Deploying;
    hot.phase_deadline = std::chrono::steady_clock::now() + cfg_.hot_cutover_phase_timeout;
    log::info("coordinator.rescale",
              "hot cutover deploy job_id=" + std::to_string(job.id) + " op_id=" + hot.op_id +
                  " new_parallelism=" + std::to_string(hot.target_parallelism) +
                  " cutover_checkpoint=" + std::to_string(hot.cutover_checkpoint) + " row_layout=" +
                  std::to_string(job.row_layout) + " stamped_ops=" + std::to_string(stamped_ops));
}

void Coordinator::hot_cutover_complete_locked_(JobState& job,
                                               std::vector<PendingDeploy>& out_frames) {
    // Every new subtask is ready; the peer updates that point the new tasks
    // at their downstreams and release the held feeder splits have not been
    // sent. The upstream edge is still held here.
    CLINK_FAULT_POINT(clink::fault::points::kHotCutoverBeforeComplete);
    auto& hot = *job.hot_cutover;

    // Targeted PeerUpdate for the new tasks: peer_updates_sent fired long
    // ago for the original deploy, and the one-shot path never re-fires -
    // the dormant cutover left its new tasks waiting for a PeerUpdate that
    // never came.
    std::unordered_map<std::string, PeerUpdateMsg> per_worker;
    for (std::uint32_t i = 0; i < hot.target_parallelism; ++i) {
        const std::string key =
            std::string{kGenericSubtaskRole} + ":" + std::to_string(hot.appended_base + i);
        auto rec = job.task_records.find(key);
        if (rec == job.task_records.end()) {
            continue;
        }
        const auto& task = rec->second.second;
        if (task.peers.empty()) {
            continue;
        }
        PeerUpdateMsg::TaskPeers tp;
        tp.role = task.role;
        tp.subtask_idx = task.subtask_idx;
        for (const auto& peer : task.peers) {
            JobState::EdgeKey ek{
                .downstream_role = peer.role,
                .downstream_subtask_idx = peer.subtask_idx,
                .upstream_role = task.role,
                .upstream_subtask_idx = task.subtask_idx,
            };
            PeerAddress resolved = peer;
            if (auto pit = job.ports.find(ek); pit != job.ports.end()) {
                resolved.host = pit->second.first;
                resolved.data_port = pit->second.second;
            }
            tp.peers.push_back(std::move(resolved));
        }
        per_worker[rec->second.first].tasks.push_back(std::move(tp));
    }
    for (auto& [worker_id, msg] : per_worker) {
        msg.job_id = job.id;
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back({it->second->conn, fenced_frame_(MessageKind::PeerUpdate, msg)});
        }
    }

    // CutoverPeerUpdate to the feeders: per feeding task, the ports the new
    // subtasks bound for THAT task's edges, in index-within-operator order.
    // This releases the held splits.
    std::unordered_map<std::string, CutoverPeerUpdateMsg> per_feeder_worker;
    bool ports_complete = true;
    for (const auto& key : hot.feeder_task_keys) {
        auto rec = job.task_records.find(key);
        if (rec == job.task_records.end()) {
            continue;
        }
        const auto& feeder = rec->second.second;
        CutoverPeerUpdateMsg::TaskPeers tp;
        tp.task_role = feeder.role;
        tp.task_subtask_idx = feeder.subtask_idx;
        for (std::uint32_t i = 0; i < hot.target_parallelism; ++i) {
            JobState::EdgeKey ek{
                .downstream_role = std::string{kGenericSubtaskRole},
                .downstream_subtask_idx = hot.appended_base + i,
                .upstream_role = feeder.role,
                .upstream_subtask_idx = feeder.subtask_idx,
            };
            auto pit = job.ports.find(ek);
            if (pit == job.ports.end()) {
                ports_complete = false;
                break;
            }
            PeerAddress p;
            p.role = std::string{kGenericSubtaskRole};
            p.subtask_idx = hot.appended_base + i;
            p.host = pit->second.first;
            p.data_port = pit->second.second;
            tp.peers.push_back(std::move(p));
        }
        if (!ports_complete) {
            break;
        }
        auto& msg = per_feeder_worker[rec->second.first];
        msg.tasks.push_back(std::move(tp));
    }
    if (!ports_complete) {
        abort_hot_cutover_locked_(
            job, "a new subtask's inbound port for a feeder edge never arrived", out_frames);
        return;
    }
    for (auto& [worker_id, msg] : per_feeder_worker) {
        msg.job_id = job.id;
        msg.op_id = hot.op_id;
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back(
                {it->second->conn, fenced_frame_(MessageKind::CutoverPeerUpdate, msg)});
        }
    }

    // Durable bookkeeping. The retained graph is what every future replan
    // and HA recovery plans from, so the new parallelism must live there.
    try {
        auto graph = JobGraphSpec::from_json(job.graph_json);
        for (auto& op : graph.ops) {
            if (op.id == hot.op_id) {
                op.parallelism = hot.target_parallelism;
            }
        }
        job.graph_json = graph.to_json();
    } catch (const std::exception& e) {
        log::warn("coordinator.rescale",
                  "hot cutover completed but the retained graph could not be rewritten: " +
                      std::string{e.what()} + "; a future replan would deploy the OLD parallelism");
    }
    // A whole-job failure inside [Complete, first post-cutover checkpoint]
    // restores from the cutover checkpoint, whose directories for this op
    // are the OLD block's: retain the translation window exactly as the
    // replan path does.
    job.stale_layout_blocks[hot.op_id] =
        JobState::StaleBlock{.base = hot.old_block_base, .parallelism = hot.old_parallelism};
    job.stale_layout_through = hot.cutover_checkpoint;

    log::info("coordinator.rescale",
              "hot cutover complete job_id=" + std::to_string(job.id) + " op_id=" + hot.op_id +
                  " " + std::to_string(hot.old_parallelism) + "->" +
                  std::to_string(hot.target_parallelism) + " cutover_checkpoint=" +
                  std::to_string(hot.cutover_checkpoint) + "; the checkpoint clock resumes");
    // Lifecycle span for OTLP export: arm to completion of the in-place
    // cutover (mode=hot_cutover; the drain-based flavours record theirs in
    // restart_job_locked_).
    if (hot.span_start_unix_nano != 0 && clink::metrics::SpanBuffer::global().enabled()) {
        clink::metrics::OtlpSpan span;
        span.name = "clink.rescale";
        span.start_unix_nano = hot.span_start_unix_nano;
        span.end_unix_nano = clink::metrics::otlp_now_unix_nano();
        span.attributes = {
            {"clink.job_id", std::to_string(job.id)},
            {"clink.mode", "hot_cutover"},
            {"clink.op_id", hot.op_id},
            {"clink.parallelism",
             std::to_string(hot.old_parallelism) + "->" + std::to_string(hot.target_parallelism)}};
        clink::metrics::SpanBuffer::global().record(std::move(span));
    }
    job.hot_cutover.reset();
    cv_.notify_all();
}

void Coordinator::abort_hot_cutover_locked_(JobState& job,
                                            const std::string& reason,
                                            std::vector<PendingDeploy>& out_frames) {
    if (!job.hot_cutover.has_value()) {
        return;
    }
    const auto op_id = job.hot_cutover->op_id;
    const auto old_p = job.hot_cutover->old_parallelism;
    const auto target = job.hot_cutover->target_parallelism;
    const auto ended_early = job.hot_cutover->early_drains.size();
    log::warn("coordinator.rescale",
              "hot cutover ABORTED job_id=" + std::to_string(job.id) + " op_id=" + op_id + " (" +
                  reason + "); falling back to the replan path at parallelism " +
                  std::to_string(target) +
                  (ended_early == 0
                       ? std::string{}
                       : "; " + std::to_string(ended_early) +
                             " old subtask(s) had already ended at C, released to the replan"));
    if (job.rescale_coordinator) {
        job.rescale_coordinator->abort(op_id, reason);
        // The replan path drives Preparing -> mark_replan_complete, so the
        // machine must accept a fresh request.
        (void)job.rescale_coordinator->request_rescale(op_id, target);
    }
    // Old subtasks that ended at C before this abort were held as early drains:
    // their exits are real, they already left pending_per_worker, and no drain
    // ack is coming from them again. The rebind teardown that would have freed
    // their slots will not run now, so free them here; the replan re-places the
    // operator, and the drain it stages below covers only what still runs.
    for (const auto& early : job.hot_cutover->early_drains) {
        release_task_slot_locked_(job, early.worker_id, early.key);
    }
    clear_hot_cut_ack_pin_locked_();
    job.hot_cutover.reset();

    // The proven stop-the-world staging, exactly as request_operator_rescale
    // does for the replan mode.
    job.pending_op_parallelism[op_id] = target;
    job.pre_rescale_op_parallelism[op_id] = old_p;
    job.awaiting_restart = true;
    job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
    populate_restart_drain_locked_(job);
    CancelJobMsg cj;
    cj.job_id = job.id;
    const auto frame = fenced_frame_(MessageKind::CancelJob, cj);
    for (const auto& [worker_id, _] : job.tasks_by_worker) {
        auto it = registered_.find(worker_id);
        if (it != registered_.end() && !it->second->lost && it->second->conn) {
            out_frames.push_back({it->second->conn, frame});
        }
    }
}

void Coordinator::handle_begin_rescale_ack_(MessageReader& r, const WorkerConnection& sender) {
    auto msg = decode_begin_rescale_ack(r);
    std::vector<PendingDeploy> frames;
    {
        std::lock_guard lock(mu_);
        if (sender.lost) {
            return;  // the loss fold has already accounted for this session
        }
        auto it = jobs_.find(msg.job_id);
        if (it == jobs_.end()) {
            return;
        }
        auto& job = *it->second;
        if (!job.hot_cutover.has_value() || job.hot_cutover->op_id != msg.op_id ||
            job.hot_cutover->phase != JobState::HotCutover::Phase::Arming) {
            return;  // stale or duplicate ack
        }
        auto& hot = *job.hot_cutover;
        if (hot.arm_workers_pending.erase(msg.worker_id) == 0) {
            return;  // not a worker this cutover armed, or a duplicate
        }
        hot.acked_callbacks += msg.armed_callbacks;
        hot.acked_groups += msg.armed_groups;
        hot.acked_rebind_tasks += msg.rebind_tasks;
        if (!hot.arm_workers_pending.empty()) {
            return;
        }
        // Every armed worker replied: the counts must cover what the
        // deployed identity and the graph say exists. A shortfall means a
        // task was built without the cutover machinery (a legacy path, or
        // a join side): replan, before C is triggered.
        if (hot.acked_groups != hot.expected_groups ||
            hot.acked_rebind_tasks != hot.expected_rebind_tasks ||
            hot.acked_callbacks < hot.expected_op_tasks) {
            abort_hot_cutover_locked_(job,
                                      "arm shortfall: groups " + std::to_string(hot.acked_groups) +
                                          "/" + std::to_string(hot.expected_groups) + ", rebinds " +
                                          std::to_string(hot.acked_rebind_tasks) + "/" +
                                          std::to_string(hot.expected_rebind_tasks) +
                                          ", callbacks " + std::to_string(hot.acked_callbacks) +
                                          " for " + std::to_string(hot.expected_op_tasks) +
                                          " task(s)",
                                      frames);
        } else {
            hot_cutover_trigger_c_locked_(job, frames);
        }
    }
    for (auto& f : frames) {
        if (f.conn != nullptr) {
            send_frame(*f.conn, f.frame);
        }
    }
}

std::optional<OperatorRescaleStatus> Coordinator::operator_rescale_status(
    JobId job_id, const std::string& op_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return std::nullopt;
    }
    auto& job = *it->second;
    if (!job.rescale_coordinator) {
        return std::nullopt;
    }
    return job.rescale_coordinator->status(op_id);
}

SavepointAckMsg Coordinator::take_savepoint(JobId job_id, std::chrono::milliseconds timeout) {
    SavepointAckMsg ack;
    ack.job_id = job_id;
    if (timeout == std::chrono::milliseconds{0}) {
        timeout = std::chrono::milliseconds{30'000};
    }

    // Stage the savepoint trigger under mu_: validate, assign a fresh
    // checkpoint id, register pending acks for every subtask of the
    // job, collect the worker connection list. Send the TriggerCheckpoint
    // frames outside the lock to avoid stalling readers.
    //
    // The id must be on record before it is allocated (claim_checkpoint_id_).
    // The job's claimer has normally put the next one there already; when it
    // has not, the savepoint claims it outside the lock and stages again. A
    // claim that cannot be written fails the savepoint with nothing sent, and
    // a few rounds cover a periodic trigger taking the claimed id first or a
    // renumber above another coordinator's ids.
    constexpr int kClaimRounds = 3;
    std::uint64_t ckpt_id = 0;
    std::uint64_t gen_for_trigger = 0;
    std::vector<std::shared_ptr<network::Connection>> worker_conns;
    std::string unclaimed_why;
    for (int round = 0;; ++round) {
        std::shared_ptr<JobState::TriggeredRecord> record;
        std::uint64_t want = 0;
        {
            std::lock_guard lock(mu_);
            auto it = jobs_.find(job_id);
            if (it == jobs_.end()) {
                ack.ok = false;
                ack.message = "no such job";
                return ack;
            }
            auto& job = *it->second;
            if (job.completion_signalled) {
                ack.ok = false;
                ack.message = "job already completed";
                return ack;
            }
            if (job.cancel_requested) {
                ack.ok = false;
                ack.message = "cancel in progress";
                return ack;
            }
            if (job.checkpoint.checkpoint_dir.empty()) {
                ack.ok = false;
                ack.message = "savepoint requires a checkpoint dir";
                return ack;
            }
            (void)renumber_above_held_ids_locked_(job);
            if (job.triggered_record->owned.load(std::memory_order_acquire) <
                job.next_checkpoint_id) {
                record = job.triggered_record;
                want = job.next_checkpoint_id;
            } else {
                ckpt_id = job.next_checkpoint_id++;
                // A savepoint is a checkpoint the trigger loop did not start:
                // the trace marks its trigger as the loop's are, or a run that
                // takes one has acks and a completion for an id nothing
                // triggered.
                if (protocol_trace::enabled()) {
                    protocol_trace::Event("Trigger")
                        .u("job", job.id)
                        .u("ckpt", ckpt_id)
                        .u("epoch", epoch())
                        .b("savepoint", true)
                        .emit();
                }
                std::unordered_set<std::string> pending;
                for (const auto& [key, _] : job.task_records) {
                    pending.insert(key);
                }
                job.pending_checkpoint_acks[ckpt_id] = std::move(pending);
                {
                    // What this checkpoint consists of, for the COMPLETED marker.
                    // Captured at TRIGGER because the ack set above is drained as
                    // acks arrive.
                    auto& rec = job.checkpoint_participants[ckpt_id];
                    rec.generation = job.state_generation;
                    auto& participants = rec.subtasks;
                    participants.clear();
                    for (const auto& [key, _unused] : job.task_records) {
                        const auto colon = key.rfind(':');
                        if (colon == std::string::npos) {
                            continue;
                        }
                        try {
                            participants.insert(
                                static_cast<std::uint32_t>(std::stoul(key.substr(colon + 1))));
                        } catch (const std::exception&) {
                            continue;
                        }
                    }
                }
                job.pending_checkpoint_start_times[ckpt_id] = std::chrono::steady_clock::now();
                clink::metrics::ckpt::triggered();
                for (const auto& [worker_id, _] : job.tasks_by_worker) {
                    auto worker_it = registered_.find(worker_id);
                    if (worker_it != registered_.end() && !worker_it->second->lost &&
                        worker_it->second->conn) {
                        worker_conns.push_back(worker_it->second->conn);
                    }
                }
                ack.checkpoint_dir = job.checkpoint.checkpoint_dir;
                gen_for_trigger = job.state_generation;
            }
        }
        if (ckpt_id != 0) {
            break;
        }
        if (round == kClaimRounds) {
            ack.ok = false;
            ack.message = "could not allocate checkpoint id " + std::to_string(want) +
                          " on record for the savepoint" +
                          (unclaimed_why.empty()
                               ? std::string{": periodic checkpoints kept taking the next id"}
                               : " (" + unclaimed_why + ")") +
                          "; nothing was triggered";
            return ack;
        }
        try {
            if (!claim_checkpoint_id_(*record, want)) {
                unclaimed_why =
                    "another coordinator of this job holds checkpoint ids up to " +
                    std::to_string(record->held_elsewhere.load(std::memory_order_acquire));
            }
        } catch (const std::exception& e) {
            log::error("coordinator.savepoint",
                       "job_id=" + std::to_string(job_id) + " could not record checkpoint id " +
                           std::to_string(want) + " (" + e.what() + "); savepoint not taken");
            ack.ok = false;
            ack.message = "could not record checkpoint id " + std::to_string(want) + " (" +
                          e.what() + "); nothing was triggered";
            return ack;
        }
    }
    // The job's claimer goes on to the id after this one.
    wake_trigger_loop_();

    TriggerCheckpointMsg tc;
    tc.job_id = job_id;
    tc.checkpoint_id = ckpt_id;
    tc.generation = gen_for_trigger;
    const auto frame = fenced_frame_(MessageKind::TriggerCheckpoint, tc);
    for (const auto& c : worker_conns) {
        send_frame(*c, frame);
    }
    log::info("coordinator.savepoint",
              "job_id=" + std::to_string(job_id) + " ckpt_id=" + std::to_string(ckpt_id) +
                  " worker_count=" + std::to_string(worker_conns.size()));

    // Wait for handle_subtask_checkpointed_ to drain pending_checkpoint_
    // acks[ckpt_id] and advance latest_completed_checkpoint_id past
    // ckpt_id. cv_.notify_all() fires from the COMPLETED-N write path.
    std::unique_lock lock(mu_);
    const bool done = cv_.wait_for(lock, timeout, [this, job_id, ckpt_id] {
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            return true;  // job disappeared; surface a fail-path ack
        }
        return it->second->latest_completed_checkpoint_id >= ckpt_id;
    });
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        ack.ok = false;
        ack.message = "job disappeared during savepoint";
        return ack;
    }
    if (!done) {
        ack.ok = false;
        ack.message = "savepoint timed out after " + std::to_string(timeout.count()) + "ms";
        return ack;
    }
    // The handle the operator is about to be given must not decay under
    // them. Pinned here, once the checkpoint has COMPLETED, so a savepoint
    // that failed pins nothing (item 74).
    it->second->pinned_checkpoint_ids.insert(ckpt_id);
    ack.ok = true;
    ack.checkpoint_id = ckpt_id;
    ack.message = "savepoint complete";
    return ack;
}

StopJobAckMsg Coordinator::stop_job(JobId job_id, std::chrono::milliseconds timeout) {
    StopJobAckMsg ack;
    ack.job_id = job_id;
    if (timeout == std::chrono::milliseconds{0}) {
        timeout = std::chrono::milliseconds{60'000};
    }

    std::vector<std::shared_ptr<network::Connection>> worker_conns;
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            ack.message = "no such job";
            return ack;
        }
        auto& job = *it->second;
        if (job.completion_signalled) {
            ack.message = "job has already finished";
            return ack;
        }
        if (job.cancel_requested) {
            ack.message = "a cancel is already in progress; that path does not drain";
            return ack;
        }
        if (job.awaiting_restart) {
            ack.message = "the job is draining for a restart or rescale; retry once it is running";
            return ack;
        }
        // Deliberately NOT cancel_requested. That flag decides the reported
        // outcome, and a job that stopped cleanly at a savepoint must report
        // success, not "cancelled by client".
        job.stop_requested = true;
        for (const auto& [worker_id, _] : job.tasks_by_worker) {
            auto worker_it = registered_.find(worker_id);
            if (worker_it != registered_.end() && !worker_it->second->lost &&
                worker_it->second->conn) {
                worker_conns.push_back(worker_it->second->conn);
            }
        }
        log::info("coordinator.stop",
                  "job_id=" + std::to_string(job_id) + " graceful stop requested; telling " +
                      std::to_string(worker_conns.size()) +
                      " worker(s) to stop producing and take a final checkpoint");
    }

    StopSubtasksMsg msg;
    msg.job_id = job_id;
    const auto frame = fenced_frame_(MessageKind::StopSubtasks, msg);
    for (const auto& c : worker_conns) {
        send_frame(*c, frame);
    }

    // Wait for the job to finish on its own. The runners take the final
    // checkpoint and block until the sinks commit it before reporting finished,
    // so completion here already means the tail is durable.
    const bool finished = await_job_completion(job_id, timeout);

    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        ack.message = "the job disappeared while stopping";
        return ack;
    }
    auto& job = *it->second;
    // The final checkpoint the runners took on the way out is the savepoint to
    // resume from. Fall back to the latest completed one when the job had no
    // sources that take a final checkpoint (nothing was in flight to commit).
    ack.savepoint_checkpoint_id =
        job.final_checkpoint_id.value_or(job.latest_completed_checkpoint_id);
    if (!finished) {
        ack.message = "the job did not finish within " + std::to_string(timeout.count()) +
                      "ms of the stop request; it may still be draining. Latest completed "
                      "checkpoint is " +
                      std::to_string(job.latest_completed_checkpoint_id);
        return ack;
    }
    if (!job.errors.empty()) {
        ack.message = "the job stopped with errors: " + job.errors.front();
        return ack;
    }
    ack.ok = true;
    ack.message = "stopped at checkpoint " + std::to_string(ack.savepoint_checkpoint_id);
    log::info("coordinator.stop",
              "job_id=" + std::to_string(job_id) + " stopped cleanly at checkpoint " +
                  std::to_string(ack.savepoint_checkpoint_id));
    return ack;
}

void Coordinator::handle_stop_job_(network::Connection& conn, MessageReader& r) {
    auto req = decode_stop_job(r);
    const auto ack = stop_job(req.job_id, std::chrono::milliseconds{req.timeout_ms});
    send_frame(conn, encode_frame(MessageKind::StopJobAck, ack));
}

void Coordinator::handle_savepoint_(network::Connection& conn, MessageReader& r) {
    auto req = decode_savepoint(r);
    const auto ack = take_savepoint(req.job_id, std::chrono::milliseconds{req.timeout_ms});
    send_frame(conn, encode_frame(MessageKind::SavepointAck, ack));
}

void Coordinator::handle_list_jobs_(network::Connection& conn) {
    ListJobsAckMsg ack;
    {
        std::lock_guard lock(mu_);
        ack.jobs.reserve(jobs_.size());
        for (const auto& [id, job] : jobs_) {
            JobInfo info;
            info.job_id = id;
            info.total_subtasks = static_cast<std::uint32_t>(job->expected_completion);
            info.completed_subtasks = static_cast<std::uint32_t>(job->completed_count);
            info.completion_signalled = job->completion_signalled;
            // Same precedence signal_job_completion_locked_ uses for its log
            // line, so a listing and the log cannot disagree about how a job
            // ended.
            if (!job->completion_signalled) {
                info.terminal_status = JobTerminalStatus::Running;
            } else if (job->cancel_requested) {
                info.terminal_status = JobTerminalStatus::Cancelled;
            } else if (!job->errors.empty()) {
                info.terminal_status = JobTerminalStatus::Failed;
            } else {
                info.terminal_status = JobTerminalStatus::CompletedOk;
            }
            ack.jobs.push_back(info);
        }
    }
    send_frame(conn, encode_frame(MessageKind::ListJobsAck, ack));
}

void Coordinator::handle_submit_(network::Connection& conn, MessageReader& r) {
    auto sj = decode_submit_job(r);
    SubmitJobAckMsg ack;
    // The cluster's ABI identity rides every ack (old clients EOF before it):
    // cheap constants, and a rejected client can say what it was refused by.
    ack.cluster_abi_fingerprint = cluster_abi_fingerprint();
    ack.cluster_target_triple = cluster_target_triple();
    ack.cluster_toolchain = cluster_toolchain();
    JobId assigned = 0;
    // ABI preflight: refuse an incompatible plugin on the references-only
    // exchange, BEFORE resolving references or requesting bytes. Adverts are
    // best-effort claims by the client, so passing here proves nothing - the
    // load-time gate below remains the authority; failing here only saves
    // shipping bytes that gate would refuse. The manifest tail lets the
    // client name the differing headers against its local copy.
    for (const auto& ad : sj.plugin_abi_adverts) {
        AbiCheckInput in;
        in.strict = strict_plugin_abi_enabled();
        in.plugin_has_fingerprint = !ad.abi_fingerprint.empty();
        in.plugin_fingerprint = ad.abi_fingerprint;
        in.cluster_fingerprint = cluster_abi_fingerprint();
        in.plugin_hash = ad.abi_hash;
        in.cluster_hash = cluster_abi_hash();
        in.plugin_triple = ad.target_triple;
        in.cluster_triple = cluster_target_triple();
        in.plugin_has_toolchain = !ad.toolchain.empty();
        in.plugin_toolchain = ad.toolchain;
        in.cluster_toolchain = cluster_toolchain();
        if (auto check = check_plugin_abi(in); !check.error.empty()) {
            ack.job_id = 0;
            ack.ok = false;
            ack.message = "submit preflight refused plugin (content hash " + ad.content_hash +
                          "): " + check.error;
            ack.cluster_abi_manifest = cluster_abi_manifest();
            metrics::coordinator::submit_preflight_refusal();
            send_frame(conn, encode_frame(MessageKind::SubmitJobAck, ack));
            return;
        }
    }
    try {
        const auto graph = JobGraphSpec::from_json(sj.graph_json);
        // Allocate a per-job bundle whose registries are parented at
        // the process-wide defaults. Plugin .so registrations land in
        // this bundle (NOT the singletons) so two concurrent jobs that
        // mint overlapping _inline_<kind>_<n> names don't trample each
        // other.
        auto bundle = std::make_unique<JobBundle>();
        auto bundle_preg = bundle->as_plugin_registry();
        // Content-addressed submit (item 30): a hash-only PluginBinary is a
        // REFERENCE to bytes this coordinator's cache may already hold. Any
        // reference the cache cannot resolve is answered with the missing
        // hashes so the submitter can retry once with bytes - the job is
        // NOT admitted on a partial plugin set. Resolved references are
        // materialised back into the message, because everything downstream
        // (the HA manifest, every worker Deploy) needs the full bytes on
        // the JobState.
        {
            std::vector<std::string> missing;
            for (auto& plug : sj.plugins) {
                if (!plug.is_reference()) {
                    continue;
                }
                const auto cached = find_plugin_in_cache(plug.content_hash);
                if (cached.empty()) {
                    missing.push_back(plug.content_hash);
                    continue;
                }
                auto loaded = make_plugin_binary_from_file(cached, plug.name);
                plug.bytes = std::move(loaded.bytes);
                metrics::coordinator::submit_plugin_cache_hit();
            }
            if (!missing.empty()) {
                ack.job_id = 0;
                ack.ok = false;
                ack.message = "plugin bytes required for " + std::to_string(missing.size()) +
                              " referenced module(s)";
                ack.missing_plugin_hashes = std::move(missing);
                send_frame(conn, encode_frame(MessageKind::SubmitJobAck, ack));
                return;
            }
        }
        std::vector<std::string> plugin_so_paths;
        plugin_so_paths.reserve(sj.plugins.size());
        for (const auto& plug : sj.plugins) {
            const auto path = write_plugin_to_cache(plug);
            auto load_result = PluginLoader::default_instance().load_into(path, bundle_preg);
            if (!load_result.ok) {
                // Remove what we just wrote. This runs BEFORE any slot or
                // admission check, so a peer sending submissions that fail to load
                // was writing one file per attempt - up to the 256 MiB frame limit
                // each - into a directory nothing prunes for the life of the
                // process. Unbounded disk from an unauthenticated peer, and the
                // only cost of an attempt was the bytes.
                //
                // Only on failure: a module that loaded is legitimately cached, and
                // the next submission of the same bytes reuses it.
                std::error_code rm_ec;
                std::filesystem::remove(path, rm_ec);
                throw std::runtime_error("plugin '" + plug.name +
                                         "' failed to load on coordinator: " + load_result.error);
            }
            bundle->retain_plugin(std::move(load_result.plugin));
            plugin_so_paths.push_back(path);
        }
        // Schema-evolution D: fail fast if the restore savepoint cannot be
        // migrated to the job's expected versions. Best-effort - only a
        // definite incompatibility verdict throws (see the gate's contract).
        if (auto reject =
                check_restore_compatibility_via_plugins(plugin_so_paths,
                                                        sj.checkpoint.restore_from_dir,
                                                        sj.checkpoint.restore_from_checkpoint_id);
            !reject.empty()) {
            throw std::runtime_error(reject);
        }
        assigned = submit_job(graph,
                              OperatorRegistry::default_instance(),
                              std::move(sj.plugins),
                              sj.checkpoint,
                              std::move(bundle),
                              &conn);
        ack.job_id = assigned;
        ack.ok = true;
    } catch (const std::exception& e) {
        ack.job_id = 0;
        ack.ok = false;
        ack.message = e.what();
    }
    send_frame(conn, encode_frame(MessageKind::SubmitJobAck, ack));
    if (assigned != 0) {
        // The ack is on the wire; release any completion push that fired
        // inside the admit-to-ack window (JobState::submit_ack_sent). A tiny
        // job can finish before this line runs, and its JobCompleted must
        // never overtake the ack the client reads first.
        std::lock_guard lock(mu_);
        if (const auto it = jobs_.find(assigned); it != jobs_.end()) {
            it->second->submit_ack_sent = true;
            if (it->second->completion_push_deferred) {
                it->second->completion_push_deferred = false;
                if (it->second->notify_client_conn != nullptr) {
                    push_job_completed_locked_(*it->second);
                }
            }
        }
    }
}

JobId Coordinator::allocate_job_id_() {
    return next_job_id_++;
}

JobId Coordinator::submit_job(const JobGraphSpec& graph,
                              const OperatorRegistry& registry,
                              network::Connection* notify_client_conn) {
    return submit_job(graph, registry, std::vector<PluginBinary>{}, notify_client_conn);
}

JobId Coordinator::submit_job(const JobGraphSpec& graph,
                              const OperatorRegistry& registry,
                              std::vector<PluginBinary> plugins,
                              network::Connection* notify_client_conn) {
    return submit_job(graph, registry, std::move(plugins), CheckpointConfig{}, notify_client_conn);
}

JobId Coordinator::submit_job(const JobGraphSpec& graph,
                              const OperatorRegistry& registry,
                              std::vector<PluginBinary> plugins,
                              CheckpointConfig checkpoint,
                              network::Connection* notify_client_conn) {
    return submit_job(graph,
                      registry,
                      std::move(plugins),
                      std::move(checkpoint),
                      /*bundle=*/nullptr,
                      notify_client_conn);
}

JobId Coordinator::submit_job(const JobGraphSpec& graph,
                              const OperatorRegistry& registry,
                              std::vector<PluginBinary> plugins,
                              CheckpointConfig checkpoint,
                              std::unique_ptr<JobBundle> bundle,
                              network::Connection* notify_client_conn) {
    // Start of the clink.submit lifecycle span, recorded at the success
    // return. Zero when no exporter has enabled the buffer.
    const std::uint64_t submit_span_start =
        clink::metrics::SpanBuffer::global().enabled() ? clink::metrics::otlp_now_unix_nano() : 0;
    // Cluster-level default: when the submitter chose no state backend, apply
    // the configured default here - before the HA manifest snapshot (line
    // ~1408) and the deploy below - so the resolved URI is what gets persisted
    // and what every subtask (and any HA-recovered restart, which reads the
    // resolved URI back from the manifest) builds. A per-job --state-backend
    // sets a non-empty URI and wins; an empty default preserves the legacy
    // resolution (empty -> memory, bare checkpoint_dir -> file).
    apply_default_state_backend(checkpoint, cfg_.default_state_backend_uri);

    // Delivery-guarantee gate. Runs AFTER the default state backend has
    // been resolved, because durability is one of its inputs and an
    // unresolved empty URI would be analysed as the wrong thing.
    //
    // This rejects only when the submitter ASKED for a guarantee the
    // pipeline cannot provide. A job that asks for nothing gets its
    // computed guarantee logged and proceeds - most jobs are at-least-once
    // and that is a legitimate choice, not an error.
    // Configuration coherence, before the guarantee analysis. Deliberately
    // first: "your checkpoint interval will never fire" is more useful than
    // "your pipeline is at-least-once", and the second is a CONSEQUENCE of
    // the first when the cause is a directory nobody set.
    {
        std::vector<ConfigProblem> problems;
        const auto reject = check_config(checkpoint, &problems);
        for (const auto& p : problems) {
            if (!p.is_error()) {
                log::warn("coordinator.config", p.setting + ": " + p.message);
            }
        }
        if (!reject.empty()) {
            throw std::runtime_error(reject);
        }
    }
    // Connector-availability gate: a job naming a connector this binary
    // was not built with is refused HERE, before planning or allocation,
    // with the connector, the available set and the rebuild flag - not on
    // a worker mid-deploy with a factory string. Runs against the
    // registries the job would deploy with (the bundle's when present, so
    // plugin registrations count). For a distributed submission this
    // process IS the target cluster's coordinator, which is what makes
    // the cluster - not the submitting CLI - authoritative.
    {
        const OperatorRegistry& effective_ops =
            bundle != nullptr ? bundle->operator_registry() : registry;
        const RunnerRegistry& effective_runners =
            bundle != nullptr ? bundle->runner_registry() : RunnerRegistry::default_instance();
        if (auto reject = check_connector_availability(graph, effective_ops, effective_runners);
            !reject.empty()) {
            throw std::runtime_error(reject);
        }
    }
    if (auto reject = check_delivery_guarantee(graph, checkpoint, /*out_report=*/nullptr);
        !reject.empty()) {
        throw std::runtime_error(reject);
    }
    // Graph-level lint. Errors reject the submission; warnings are logged, since
    // they are settings that will be ignored rather than settings that break the
    // job.
    //
    // Deliberately WITHOUT a slot count: the capacity check a few lines below
    // already refuses a plan larger than the cluster, and duplicating a gate that
    // exists would mean two messages for one fact. The slot check in
    // lint_job_graph is for callers with no cluster to plan against.
    {
        const auto problems = lint_job_graph(graph, checkpoint, /*available_slots=*/std::nullopt);
        std::string errors;
        for (const auto& p : problems) {
            if (p.is_error()) {
                errors += (errors.empty() ? "" : "; ") + p.setting + ": " + p.message;
            } else {
                log::warn("coordinator.lint", p.setting + ": " + p.message);
            }
        }
        if (!errors.empty()) {
            throw std::runtime_error("submit_job: " + errors);
        }
    }
    // Use the bundle's OperatorRegistry (parent-fallback to default
    // singleton for built-ins) when one is provided so the planner's
    // chain-eligibility check can find inline-lambda ops registered by
    // the plugin's build_fn. Without this, plan_job sees only the
    // default singleton (which has no inline ops) and the chain is
    // refused.
    auto plan = bundle != nullptr
                    ? plan_job(graph, bundle->operator_registry(), bundle->runner_registry())
                    : plan_job(graph, registry);

    // Restore layout gate. Every task this plan deploys restores from its own
    // job-global index (no directive; that is the rescale paths' translation), so
    // a checkpoint taken by a different layout would hand the operators each
    // other's state and the job would carry on. The savepoint submit, an explicit
    // checkpoint id and HA recovery all arrive here, and HA recovery replans the
    // SUBMITTED graph, which a rescale since then has not rewritten. Refused
    // before any slot is waited for or any task deployed; see
    // check_restore_layout for what counts as a verdict.
    std::vector<std::string> single_instance;
    for (const auto& op : graph.ops) {
        if (is_forced_singleton(op)) {
            single_instance.push_back(op.id);
        }
    }
    if (!checkpoint.restore_from_dir.empty() && checkpoint.restore_from_checkpoint_id != 0) {
        std::set<std::uint32_t> planned;
        for (const auto& t : plan.tasks) {
            planned.insert(t.subtask_idx);
        }
        if (auto reject = check_restore_layout(checkpoint.restore_from_dir,
                                               checkpoint.restore_from_checkpoint_id,
                                               planned,
                                               single_instance);
            !reject.empty()) {
            throw std::runtime_error(reject);
        }
    }

    // Wait for spare slots if configured. This is a coarse-grained
    // policy: we just check `free_slots() >= required` periodically. A
    // production scheduler would integrate with the slot accountant.
    const auto required = plan.tasks.size();
    if (cfg_.submit_wait_for_slots.count() > 0) {
        std::unique_lock lock(mu_);
        const auto deadline = std::chrono::steady_clock::now() + cfg_.submit_wait_for_slots;
        cv_.wait_until(lock, deadline, [&] {
            // stop_ in the predicate so a coordinator shutting down never
            // holds a submitter (or the HA recovery retry thread) hostage
            // for the full slot wait.
            if (stop_.load(std::memory_order_acquire)) {
                return true;
            }
            std::size_t free = 0;
            for (const auto& [_, worker] : registered_) {
                if (!worker->lost && worker->slots_in_use < worker->slot_capacity) {
                    free += (worker->slot_capacity - worker->slots_in_use);
                }
            }
            return free >= required;
        });
    }
    {
        std::lock_guard lock(mu_);
        std::size_t free = 0;
        for (const auto& [_, worker] : registered_) {
            if (!worker->lost && worker->slots_in_use < worker->slot_capacity) {
                free += (worker->slot_capacity - worker->slots_in_use);
            }
        }
        if (free < required) {
            throw InsufficientSlotsError("submit_job: insufficient free slots (need " +
                                         std::to_string(required) + ", have " +
                                         std::to_string(free) + ")");
        }
    }

    // Snapshot inputs for HA manifest write - deploy_internal_ moves
    // them into the JobState.
    const auto plugins_copy = plugins;
    const auto checkpoint_copy = checkpoint;
    const auto job_id = deploy_internal_(
        plan,
        notify_client_conn,
        std::move(plugins),
        std::move(checkpoint),
        std::move(bundle),
        graph.expected_state_versions.pack(),
        graph.udfs.empty() ? std::string{} : pack_udf_specs(graph.udfs),
        checkpoint_copy.checkpoint_dir.empty() ? std::string{} : job_graph_fingerprint(graph),
        single_instance);
    // Derive commit-group memberships from sink-op params
    // and stash them on JobState so handle_subtask_checkpointed_ can
    // gate CommitCheckpoint broadcasts on the group's collective ack.
    //
    // In the same walk, populate the RescaleCoordinator
    // with each operator's current parallelism + min/max
    // bounds. Operators with 0/0 bounds register too (the
    // coordinator's request_rescale will reject them as
    // not-scalable; cleaner than skipping at register time because
    // status() then surfaces them as Idle for dashboards).
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(job_id);
        if (it != jobs_.end()) {
            auto& job = *it->second;
            if (!job.rescale_coordinator) {
                job.rescale_coordinator = std::make_unique<RescaleCoordinator>();
            }
            for (const auto& op : graph.ops) {
                job.rescale_coordinator->register_operator(
                    op.id, op.parallelism, op.min_parallelism, op.max_parallelism);

                auto cg_it = op.params.find("commit_group");
                if (cg_it == op.params.end() || cg_it->second.empty()) {
                    continue;
                }
                const auto& group = cg_it->second;
                for (std::uint32_t sub = 0; sub < op.parallelism; ++sub) {
                    const std::string key = op.id + ":" + std::to_string(sub);
                    job.commit_groups[group].insert(key);
                    job.subtask_commit_group[key] = group;
                }
            }

            // Spin up the per-job autoscaler if the cluster
            // config opts in AND at least one op carries [min, max]
            // bounds. The autoscaler captures `this` + `job_id` so its
            // callbacks route into Coordinator::request_operator_rescale
            // / operator_rescale_status under the coordinator's lock discipline.
            // Idempotent w.r.t. recovery: re-deploy of the same job_id
            // would overwrite an existing autoscaler unique_ptr.
            if (cfg_.autoscaler.has_value()) {
                bool any_scalable = false;
                for (const auto& op : graph.ops) {
                    if (op.min_parallelism > 0 || op.max_parallelism > 0) {
                        any_scalable = true;
                        break;
                    }
                }
                if (any_scalable) {
                    auto sample_fn_copy = autoscaler_sample_fn_;
                    auto sample = [sample_fn_copy, job_id](const std::string& op_id) -> double {
                        return sample_fn_copy ? sample_fn_copy(job_id, op_id) : 0.5;
                    };
                    auto request = [this, job_id](const std::string& op_id, std::uint32_t new_p) {
                        return request_operator_rescale(job_id, op_id, new_p);
                    };
                    auto status =
                        [this,
                         job_id](const std::string& op_id) -> std::optional<OperatorRescaleStatus> {
                        std::lock_guard inner(mu_);
                        auto jit = jobs_.find(job_id);
                        if (jit == jobs_.end()) {
                            return std::nullopt;
                        }
                        auto& js = *jit->second;
                        if (js.completion_signalled || js.cancel_requested ||
                            !js.rescale_coordinator) {
                            return std::nullopt;
                        }
                        return js.rescale_coordinator->status(op_id);
                    };
                    job.autoscaler = std::make_unique<Autoscaler>(
                        *cfg_.autoscaler, std::move(sample), std::move(request), std::move(status));
                    for (const auto& op : graph.ops) {
                        if (op.min_parallelism > 0 || op.max_parallelism > 0) {
                            job.autoscaler->register_operator(op.id);
                        }
                    }
                    job.autoscaler->start();
                }
            }
        }
    }
    // Retain the logical graph for EVERY job (not just HA) so
    // GET /api/v1/jobs/:id/graph can serve the topology. HA additionally
    // persists it to the on-disk manifest for takeover recovery.
    {
        const auto graph_json = graph.to_json();
        {
            std::lock_guard lock(mu_);
            auto it = jobs_.find(job_id);
            if (it != jobs_.end()) {
                it->second->graph_json = graph_json;
                it->second->name = graph.name;
            }
        }
        if (!ha_dir_.empty()) {
            bool confirm_flag = false;
            {
                std::lock_guard lock(mu_);
                if (auto it = jobs_.find(job_id); it != jobs_.end()) {
                    confirm_flag = !it->second->confirm_task_keys.empty();
                }
            }
            persist_job_manifest_(
                ha_dir_, job_id, graph_json, plugins_copy, checkpoint_copy, epoch(), confirm_flag);
        }
    }

    // Emit the job's data lineage on the event bus so any registered
    // lineage exporter (and the /api/v1/events stream) can ship it. The
    // payload wraps the lineage graph with the job id and name; a
    // LineageDispatcher reconstructs them. Best-effort: never fail a
    // submit on lineage.
    try {
        const auto lg = lineage::extract_lineage(graph);
        if (!lg.empty()) {
            events::publish("coordinator.job_lineage",
                            "{\"job_id\":" + std::to_string(job_id) + ",\"job_name\":" +
                                js_quote(graph.name) + ",\"lineage\":" + lg.to_json() + "}");
        }
    } catch (...) {
    }

    // Lifecycle span for OTLP export: gate-to-deployed for this submit.
    // Success only - a rejected submit throws before reaching here and is
    // visible through its own error, not a span.
    if (submit_span_start != 0 && clink::metrics::SpanBuffer::global().enabled()) {
        clink::metrics::OtlpSpan span;
        span.name = "clink.submit";
        span.start_unix_nano = submit_span_start;
        span.end_unix_nano = clink::metrics::otlp_now_unix_nano();
        span.attributes = {{"clink.job_id", std::to_string(job_id)},
                           {"clink.tasks", std::to_string(plan.tasks.size())}};
        if (checkpoint_copy.restore_from_checkpoint_id != 0) {
            span.attributes.emplace_back(
                "clink.restore_from_checkpoint",
                std::to_string(checkpoint_copy.restore_from_checkpoint_id));
        }
        clink::metrics::SpanBuffer::global().record(std::move(span));
    }

    return job_id;
}

void Coordinator::deploy(const JobPlan& plan) {
    legacy_active_job_id_ =
        deploy_internal_(plan, nullptr, std::vector<PluginBinary>{}, CheckpointConfig{}, nullptr);
}

JobId Coordinator::deploy_internal_(const JobPlan& plan,
                                    network::Connection* notify_client_conn,
                                    std::vector<PluginBinary> plugins,
                                    CheckpointConfig checkpoint,
                                    std::unique_ptr<JobBundle> bundle,
                                    std::string expected_state_versions_packed,
                                    std::string udfs_packed,
                                    std::string graph_fingerprint,
                                    std::vector<std::string> single_instance_ops) {
    // Resolve per-task placement. The grouping contract, and why it exists, is documented on
    // assign_task_placement in coordinator.hpp; it lives there so it can be tested without a
    // cluster. The plan's data_port values are taken as-is: 0 means "the worker will bind
    // ephemerally and report via SubtaskListening", a non-zero port means "the caller pre-bound
    // this and the address is already known". The legacy in-process API uses the latter.
    JobPlan resolved_plan = plan;
    // The session each worker's Deploy goes to. The slots, the session stamps
    // on the job's pending subtasks, and the connection the Deploy is sent on
    // all name the same session. Picked in the hold that places the tasks and
    // checked again in the hold that publishes the job (below): the store
    // reads and writes between the two can outlast a re-registration.
    std::unordered_map<std::string, std::shared_ptr<WorkerConnection>> targets;
    std::unordered_map<std::string, std::uint32_t> charged;
    // Gives back every slot charged here if the job is never published: the
    // deploy fails, and nothing would release them.
    struct ReleaseUnlessPublished {
        Coordinator& self;
        std::unordered_map<std::string, std::shared_ptr<WorkerConnection>>& targets;
        std::unordered_map<std::string, std::uint32_t>& charged;
        bool published{false};
        ~ReleaseUnlessPublished() {
            if (published) {
                return;
            }
            std::lock_guard lock(self.mu_);
            for (const auto& [worker_id, n] : charged) {
                if (const auto t = targets.find(worker_id); t != targets.end()) {
                    self.release_slots_locked_(*t->second, n);
                }
            }
        }
    } release_unless_published{*this, targets, charged};
    {
        std::lock_guard lock(mu_);
        std::vector<PlacementWorker> pool;
        for (auto& [_, worker] : registered_) {
            if (!worker->lost) {
                pool.push_back(
                    PlacementWorker{.worker_id = worker->worker_id,
                                    .free_slots = worker->slots_in_use < worker->slot_capacity
                                                      ? worker->slot_capacity - worker->slots_in_use
                                                      : 0U});
            }
        }
        if (!assign_task_placement(resolved_plan.tasks, pool)) {
            throw std::runtime_error(
                "Coordinator::deploy: no worker with free slots for every "
                "task in the plan");
        }
        // Charge the slots actually taken back to the live worker records.
        for (const auto& t : resolved_plan.tasks) {
            auto it = registered_.find(t.worker_id);
            if (it != registered_.end()) {
                targets.emplace(t.worker_id, it->second);
                charge_slots_locked_(*it->second, 1);
                ++charged[t.worker_id];
            }
        }
    }
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorDeployAfterPlacement);

    // Build a (role, subtask) → (worker_id, port) lookup so we can resolve
    // peer references into concrete host:port addresses. Hosts come
    // from the registered worker's data_host.
    struct TaskKey {
        std::string role;
        std::uint32_t subtask_idx;
        bool operator==(const TaskKey&) const = default;
    };
    struct TaskKeyHash {
        std::size_t operator()(const TaskKey& k) const noexcept {
            return std::hash<std::string>{}(k.role) ^
                   (std::hash<std::uint32_t>{}(k.subtask_idx) << 1);
        }
    };
    std::unordered_map<TaskKey, std::pair<std::string, std::uint16_t>, TaskKeyHash> index;
    for (const auto& t : resolved_plan.tasks) {
        index[TaskKey{t.role, t.subtask_idx}] = {t.worker_id, t.data_port};
    }

    const JobId job_id = allocate_job_id_();
    auto job = std::make_shared<JobState>();
    job->id = job_id;

    // Resume from this job's own checkpoints (CheckpointConfig::track_runs): the
    // embedded engine's rerun of a script after a crash. A fresh process has a
    // fresh coordinator, so this is a takeover exactly as HA recovery sees one,
    // and the restore point is chosen the same way. Without it a rerun started
    // from nothing, re-read its sources from the beginning and re-published
    // everything its exactly-once sinks had already committed.
    //
    // Three records under _jobs/<id>/ decide it, because job ids restart at 1 in
    // every process and the markers alone say only that SOME job with this id
    // ran here:
    //   - graph-fingerprint: which job graph the current run is. A resume whose
    //     graph differs is refused, never guessed past.
    //   - run-base: the id the current run numbers above. Checkpoints at or
    //     below it are an earlier run's; a run that dies before completing a
    //     checkpoint of its own has nothing to resume, and must not be handed
    //     the earlier run's state (which, after --fresh with a changed script,
    //     would be another graph's).
    //   - FINISHED: the run reached the end of its input cleanly
    //     (mark_job_finished_). Rerunning a script means running such a job
    //     again - a bounded load over new input, a full-refresh materialized
    //     view - so it starts over; resuming would restore its sources at their
    //     end and publish nothing.
    const bool track_runs = checkpoint.track_runs && !checkpoint.checkpoint_dir.empty() &&
                            checkpoint.restore_from_checkpoint_id == 0 &&
                            checkpoint.restore_from_dir.empty();
    const std::string fingerprint_key = "_jobs/" + std::to_string(job_id) + "/graph-fingerprint";
    bool resumed_from_existing = false;
    if (track_runs && !checkpoint.start_fresh) {
        const auto store = make_coordination_store(checkpoint.checkpoint_dir);
        const auto completed = latest_completed_id_on_disk(checkpoint.checkpoint_dir, job_id);
        std::uint64_t run_base = 0;
        if (const auto recorded = store->get(run_base_key(job_id)); recorded.has_value()) {
            try {
                run_base = std::stoull(*recorded);
            } catch (const std::exception&) {
                throw std::runtime_error("refusing to resume job " + std::to_string(job_id) +
                                         " from " + checkpoint.checkpoint_dir +
                                         ": its run-base record is unreadable ('" + *recorded +
                                         "'). Use a new checkpoint directory, or start this job "
                                         "fresh (clink run --fresh).");
            }
        }
        const auto finished =
            completed > run_base ? store->get(finished_marker_key(job_id)) : std::nullopt;
        if (finished.has_value()) {
            log::info("coordinator.restart",
                      "job_id=" + std::to_string(job_id) + " finished cleanly in its last run in " +
                          checkpoint.checkpoint_dir + " (checkpoint " + *finished +
                          "); starting it from empty state");
        } else if (completed > run_base) {
            const auto recorded = store->get(fingerprint_key);
            if (!recorded.has_value() || *recorded != graph_fingerprint) {
                throw std::runtime_error(
                    "refusing to resume job " + std::to_string(job_id) + " from " +
                    checkpoint.checkpoint_dir + ": its checkpoints (up to " +
                    std::to_string(completed) + ") were written by " +
                    (recorded.has_value() ? "a different job graph (fingerprint " + *recorded +
                                                ", this job is " + graph_fingerprint + ")"
                                          : "a job that recorded no graph fingerprint") +
                    ". Restoring them would hand this job another job's state. Use a new "
                    "checkpoint directory, or start this job fresh (clink run --fresh).");
            }
            bool needs_confirmation = false;
            for (const auto& t : resolved_plan.tasks) {
                needs_confirmation = needs_confirmation || t.needs_commit_confirmation;
            }
            std::uint64_t restore =
                needs_confirmation ? latest_confirmed_id_on_disk(checkpoint.checkpoint_dir, job_id)
                                   : completed;
            // An earlier run's confirmed checkpoint is not this run's to restore.
            if (restore <= run_base) {
                restore = 0;
            }
            if (protocol_trace::enabled()) {
                // The confirmed restore point this run adopts, as a takeover
                // reports it: the newest CONFIRMED marker, read once above,
                // and 0 when it belongs to an earlier run (at or below the run
                // base), which this run neither resumes from nor seeds its
                // confirmed restore point with. A job outside the
                // commit-confirmed family confirms nothing.
                protocol_trace::Event("CoordRecovers")
                    .u("job", job_id)
                    .u("epoch", epoch())
                    .u("completed", completed)
                    .u("confirmed", needs_confirmation ? restore : 0)
                    .emit();
            }
            if (needs_confirmation && completed > std::max(restore, run_base)) {
                if (protocol_trace::enabled()) {
                    protocol_trace::Event("RestartProceeds")
                        .u("job", job_id)
                        .b("resolving", true)
                        .u("completed", completed)
                        .u("confirmed", restore)
                        .emit();
                }
                const auto resolved = resolve_in_doubt_commits(
                    checkpoint.checkpoint_dir, job_id, std::max(restore, run_base), completed);
                if (resolved > run_base) {
                    restore = std::max(restore, resolved);
                }
            }
            if (restore == 0) {
                // The run completed checkpoints but none of them committed its
                // sinks' output: nothing of it was published, so it starts over.
                log::info("coordinator.restart",
                          "job_id=" + std::to_string(job_id) +
                              " has no committed checkpoint of its last run in " +
                              checkpoint.checkpoint_dir + " (newest completed " +
                              std::to_string(completed) + "); starting it from empty state");
            } else {
                checkpoint.restore_from_dir = checkpoint.checkpoint_dir;
                checkpoint.restore_from_checkpoint_id = restore;
                // Beside the completed point its takeover line reported, as an
                // HA takeover does: a gap the walk above left open is walked
                // again by a restart before this run completes a checkpoint.
                // Only for a resume: a run that starts from empty state above
                // its base has no gap of its own, and a walk from its zero
                // confirmed point would reach the earlier runs' checkpoints.
                checkpoint.resumed_completed = completed;
                // The layout gate above ran before there was a restore point.
                std::set<std::uint32_t> planned;
                for (const auto& t : resolved_plan.tasks) {
                    planned.insert(t.subtask_idx);
                }
                if (auto reject = check_restore_layout(
                        checkpoint.restore_from_dir, restore, planned, single_instance_ops);
                    !reject.empty()) {
                    throw std::runtime_error(reject);
                }
                resumed_from_existing = true;
                log::info("coordinator.restart",
                          "job_id=" + std::to_string(job_id) + " resuming from checkpoint " +
                              std::to_string(restore) + " in " + checkpoint.checkpoint_dir +
                              " (newest completed " + std::to_string(completed) + ")");
            }
        }
    }
    // Continue checkpoint numbering ABOVE the checkpoint being restored
    // from, rather than restarting at 1.
    //
    // Checkpoint ids are not decoration. Several things are named by them:
    // the COMPLETED-<id> marker the recovery point is read from, and the
    // 2PC sink's committed output file (committed/sub<N>-<id>.dat). A
    // recovered or resumed job that starts again at 1 therefore writes over
    // artefacts of the run it is continuing.
    //
    // The consequence is silent DATA LOSS, and it is what
    // ExactlyOnceSurvivesACoordinatorFailover found: a job whose first two
    // records had been committed as committed/sub0-1.dat came back, took its
    // own checkpoint 1, and replaced that file - so records 0 and 1 vanished
    // from output that had already been published. 38 of 40 records
    // survived, no duplicates, and nothing failed.
    //
    // Not specific to HA. Any resume does it, including an explicit
    // --restore-from-checkpoint-id=N, which is the documented way to rewind
    // a job.
    //
    // Above the restore point is NOT enough: number above every checkpoint
    // id recorded on disk. The restore point can sit BELOW the newest
    // COMPLETED marker - the exact shape of a coordinator that died between
    // the marker write and the commit broadcast, where in-doubt resolution
    // reports the newest completed checkpoint as not externally committed
    // and the job restores from the older confirmed one. Numbering from
    // restore_from+1 then reuses the dead incarnation's id: QUAL-01 run C's
    // recovery restored from 245 while COMPLETED-246 sat on disk, and its
    // first checkpoint overwrote 246's marker and snapshot files. The
    // marker then vouches for participant files a DIFFERENT incarnation
    // wrote, and a further crash inside that window would recover from a
    // checkpoint whose marker and snapshots disagree. Ids are cheap;
    // never reuse one that has a durable record.
    // A run of a job that tracks its runs gets the same floor when it starts from
    // empty state: it has nothing to restore, but a checkpoint numbered 1 would
    // still overwrite the COMPLETED-1 marker and snapshot files of the run
    // before it. So does a job taken over before it completed a checkpoint of
    // its own: the dead leader may still have triggered some.
    //
    // The checkpoint-id record is read for every job with a checkpoint
    // directory, floored or not: what it holds now is where this run's own
    // claims begin (TriggeredRecord::owned). A floored job numbers above it,
    // and a record it cannot read fails the deploy, since numbering without it
    // could reuse an id whose barrier a dead leader delivered. A fresh job
    // numbers from 1 as it always has, through ids that no coordinator can
    // claim again, and one whose record cannot be read starts claiming from 1.
    const bool id_floored =
        checkpoint.restore_from_checkpoint_id > 0 || track_runs || checkpoint.taken_over;
    std::uint64_t ids_on_record = 0;
    if (!checkpoint.checkpoint_dir.empty()) {
        if (id_floored) {
            ids_on_record = latest_triggered_id_on_disk(checkpoint.checkpoint_dir, job_id);
        } else {
            try {
                ids_on_record = latest_triggered_id_on_disk(checkpoint.checkpoint_dir, job_id);
            } catch (const std::exception& e) {
                log::warn("coordinator.submit",
                          "job_id=" + std::to_string(job_id) +
                              " could not read its checkpoint-id record in " +
                              checkpoint.checkpoint_dir + " (" + e.what() +
                              "); its checkpoints claim ids from 1, numbering above whatever "
                              "the record turns out to hold");
            }
        }
    }
    job->triggered_record->checkpoint_dir = checkpoint.checkpoint_dir;
    job->triggered_record->job_id = job_id;
    job->triggered_record->claimant = new_id_claimant(epoch());
    job->triggered_record->owned.store(ids_on_record, std::memory_order_release);
    if (id_floored) {
        std::uint64_t id_floor = checkpoint.restore_from_checkpoint_id;
        if (!checkpoint.checkpoint_dir.empty()) {
            id_floor =
                std::max(id_floor, latest_completed_id_on_disk(checkpoint.checkpoint_dir, job_id));
            id_floor =
                std::max(id_floor, latest_confirmed_id_on_disk(checkpoint.checkpoint_dir, job_id));
            // Markers are not the only durable records: a seconds-lived
            // incarnation (a restart storm's middle attempts) dies holding
            // SNAPSHOT FILES for checkpoints that never completed - no
            // marker names them, and numbering above markers alone reuses
            // their ids. The successor's same-id files then interleave with
            // the dead incarnation's, and a later restore can assemble one
            // checkpoint from two vintages (qual01-20260819g re-published
            // ten windows that way). Number above every snapshot file too.
            id_floor = std::max(id_floor, latest_snapshot_id_on_disk(checkpoint.checkpoint_dir));
            // Nor are the files every capture: one whose barrier reached a
            // worker but whose snapshot has not landed leaves none, and a
            // worker that outlives the dead leader can still write it after
            // this scan, under the very path this run's capture of the same id
            // writes. Trace validation caught a takeover reusing such an id.
            // Every id a coordinator sends is on record before its frame leaves
            // (claim_checkpoint_id_), so number above the record as well.
            id_floor = std::max(id_floor, ids_on_record);
        }
        job->next_checkpoint_id = id_floor + 1;
        if (id_floor > checkpoint.restore_from_checkpoint_id) {
            log::info("coordinator.restart",
                      "job_id=" + std::to_string(job_id) +
                          (checkpoint.restore_from_checkpoint_id > 0
                               ? " resumes from checkpoint " +
                                     std::to_string(checkpoint.restore_from_checkpoint_id)
                               : std::string{" restores nothing of its own"}) +
                          " but numbers new checkpoints from " +
                          std::to_string(job->next_checkpoint_id) + ": ids up to " +
                          std::to_string(id_floor) +
                          " already have durable records in this directory");
        }
    }
    job->first_checkpoint_id = job->next_checkpoint_id;
    if (track_runs) {
        // Recorded before the deploy, in this order, so that whatever happens to
        // this run from here - a failed deploy, a kill before its first
        // checkpoint - the next run resumes only what this one completes: the
        // base first (an earlier run's checkpoints are out of reach), then the
        // graph, then the end of the previous run's clean finish.
        const auto store = make_coordination_store(checkpoint.checkpoint_dir);
        if (!resumed_from_existing) {
            store->put(run_base_key(job_id), std::to_string(job->next_checkpoint_id - 1));
        }
        store->put(fingerprint_key, graph_fingerprint);
        store->remove(finished_marker_key(job_id));
    }
    job->notify_client_conn = notify_client_conn;
    // A wire submission's completion push must wait behind the SubmitJobAck
    // handle_submit_ has not sent yet (see JobState::submit_ack_sent).
    job->submit_ack_sent = (notify_client_conn == nullptr);
    job->expected_completion = resolved_plan.tasks.size();
    job->submit_time = std::chrono::steady_clock::now();
    job->topology_version = 1;  // initial deploy is version 1
    job->bundle = std::move(bundle);
    job->expected_state_versions_packed = std::move(expected_state_versions_packed);
    job->udfs_packed = std::move(udfs_packed);
    // Only generic-role subtasks send SubtaskListening. Custom-role
    // (test-harness) tasks pre-bind their ports and never report.
    job->expected_listenings = 0;
    for (const auto& t : resolved_plan.tasks) {
        if (t.role == kGenericSubtaskRole) {
            ++job->expected_listenings;
        }
    }

    // Group tasks by worker_id and build DeploymentTask entries with peers
    // resolved against the lookup. Generic-role peer ports are 0 here
    // and get filled in via PeerUpdate after SubtaskListening arrives.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_worker;
    // Commit-confirmed restore protocol: record the tracked-task set from
    // THIS plan onto the job being built (it is not in jobs_ yet - this is
    // the submit path constructing it). Empty = the job has no
    // non-recoverable-commit sink and the protocol stays entirely dormant.
    job->confirm_task_keys.clear();
    for (const auto& t : resolved_plan.tasks) {
        if (t.needs_commit_confirmation) {
            job->confirm_task_keys.insert(t.role + ":" + std::to_string(t.subtask_idx));
        }
    }
    // Seed latest_confirmed from the CONFIRMED-N markers on disk - but ONLY
    // for a job that is actually resuming.
    //
    // The seed exists because a recovered or restarted coordinator starts
    // with in-memory latest_confirmed at zero while the markers of the run it
    // is resuming sit on disk; without it, a later worker-loss restart would
    // restore from scratch past genuinely confirmed checkpoints.
    //
    // Applying it to a FRESH submission is a different thing entirely, and it
    // is a correctness hole. The markers live under
    // <checkpoint_dir>/_jobs/<job_id>/, and job ids restart at 1 with every
    // coordinator, so a new job submitted into a checkpoint directory a
    // previous job used inherits that job's confirmed id. QUAL-01 hit exactly
    // this: a new job with zero completed checkpoints came up believing
    // checkpoint 224 was confirmed, and its first worker-loss restart chose
    // 224 as the restore point - another run's checkpoint. It replayed that
    // run's offsets and re-emitted windows it had already committed, which
    // the oracle counted as 181,071 duplicate results. Every value was
    // correct; each one simply arrived twice.
    //
    // A job that is not resuming has, by definition, confirmed nothing.
    const bool resuming =
        checkpoint.restore_from_checkpoint_id != 0 || !checkpoint.restore_from_dir.empty();
    if (resuming && !job->confirm_task_keys.empty() && !checkpoint.checkpoint_dir.empty()) {
        auto seed = latest_confirmed_id_on_disk(checkpoint.checkpoint_dir, job_id);
        // And never past the point being resumed from. A marker beyond the
        // restore point belongs to progress this job is deliberately not
        // taking - a savepoint restore to an earlier checkpoint is the plain
        // case - and treating it as confirmed would put the restore point
        // ahead of the state actually being restored.
        if (checkpoint.restore_from_checkpoint_id != 0 &&
            seed > checkpoint.restore_from_checkpoint_id) {
            log::warn("coordinator.restart",
                      "job_id=" + std::to_string(job_id) + " ignoring CONFIRMED-" +
                          std::to_string(seed) + " on disk: it is ahead of the checkpoint " +
                          std::to_string(checkpoint.restore_from_checkpoint_id) +
                          " this job is resuming from");
            seed = checkpoint.restore_from_checkpoint_id;
        }
        job->latest_confirmed_checkpoint_id = std::max(job->latest_confirmed_checkpoint_id, seed);
    }
    for (const auto& t : resolved_plan.tasks) {
        DeploymentTask d;
        d.role = t.role;
        d.subtask_idx = t.subtask_idx;
        d.data_port = t.data_port;
        d.extra_config = t.extra_config;
        for (const auto& [pr_role, pr_sub] : t.peer_refs) {
            auto it = index.find(TaskKey{pr_role, pr_sub});
            if (it == index.end()) {
                throw std::runtime_error("Coordinator::deploy: unresolved peer ref " + pr_role +
                                         "/" + std::to_string(pr_sub));
            }
            const auto& [peer_worker_id, peer_port] = it->second;
            std::string peer_host;
            {
                std::lock_guard lock(mu_);
                auto worker_it = registered_.find(peer_worker_id);
                if (worker_it == registered_.end()) {
                    throw std::runtime_error("Coordinator::deploy: peer worker not registered: " +
                                             peer_worker_id);
                }
                peer_host = worker_it->second->data_host;
            }
            d.peers.push_back(PeerAddress{
                .role = pr_role,
                .subtask_idx = pr_sub,
                .host = std::move(peer_host),
                .data_port = peer_port,
            });
        }
        by_worker[t.worker_id].push_back(std::move(d));
    }

    // Fill per-task key-group ranges based on the role's initial
    // parallelism. Same formula rescale_job uses (contiguous
    // ranges). Lets Queryable State kg-aware routing work on the
    // first deploy without waiting for a rescale; for non-keyed
    // operators the range field is just unread.
    {
        // Carry the planner's key-group slice onto the deploy directive.
        //
        // This used to compute the slice here, from the task's GLOBAL index
        // and a count of every task sharing its role. Both inputs are wrong
        // for anything deployed through the generic subtask role, which is
        // everything: all operators share that one role name, so a job of
        // three parallelism-1 operators had the key space split three ways
        // between DIFFERENT operators. A keyed operator that should own all
        // 128 groups was told it owned 43, wrote state for keys outside that
        // slice anyway, and lost it at the next restore without a word (F38).
        //
        // The planner sets it from the task's index within its operator and
        // that operator's parallelism, which are the two numbers this loop
        // could not recover.
        std::unordered_map<std::string, std::pair<std::uint32_t, std::uint32_t>> planned_kg;
        for (const auto& t : resolved_plan.tasks) {
            planned_kg[t.role + ":" + std::to_string(t.subtask_idx)] = {t.key_group_first,
                                                                        t.key_group_last};
        }
        // Fall back to the per-role split for any task whose range the
        // planner did not set. Not every PlannedTask comes from the chain
        // planner - queryable-state routing and the in-process test API build
        // their own - and for a single-operator job the two agree, because
        // the role count IS that operator's parallelism. It is only a job
        // with several operators sharing the generic role where the old
        // formula splits the key space between them.
        std::unordered_map<std::string, std::uint32_t> role_p;
        for (const auto& t : resolved_plan.tasks) {
            ++role_p[t.role];
        }
        for (auto& [_worker_id, tasks] : by_worker) {
            for (auto& d : tasks) {
                const auto key = d.role + ":" + std::to_string(d.subtask_idx);
                if (const auto it = planned_kg.find(key);
                    it != planned_kg.end() && !(it->second.first == 0 && it->second.second == 0)) {
                    d.key_group_first = it->second.first;
                    d.key_group_last = it->second.second;
                    continue;
                }
                const auto rp = role_p.find(d.role);
                if (rp == role_p.end() || rp->second == 0) {
                    continue;
                }
                const auto range = key_group_range_for_subtask(d.subtask_idx, rp->second);
                d.key_group_first = range.first;
                d.key_group_last = range.second;
            }
        }
    }

    // Which operator each task hosts. Only the plan knows: the deploy
    // directive carries a role shared by every task and a job-global index.
    // A later rescale needs it to translate (operator, index within operator)
    // back to the global index whose snapshot directory it must read.
    std::unordered_map<std::string, JobState::TaskOpIdentity> plan_op_identity;
    for (const auto& t : resolved_plan.tasks) {
        if (t.op_id.empty()) {
            continue;  // not from the chain planner; identity unknown
        }
        plan_op_identity[t.role + ":" + std::to_string(t.subtask_idx)] =
            JobState::TaskOpIdentity{.op_id = t.op_id, .subtask_idx_in_op = t.subtask_idx_in_op};
    }

    std::uint32_t row_layout = kRowLayoutV1;
    std::string row_layout_note;
    {
        std::lock_guard lock(mu_);
        // A worker may have re-registered since its tasks were placed, and the
        // session they were placed on may already be retired: the retirement
        // folds only the jobs it finds, and this one is not published yet.
        // Stamped with that session and deployed on its half-closed socket, the
        // job would never start and nothing would ever fold it. Its tasks go
        // to the worker's current session instead, which takes over their
        // slots if it still has room for them. It has been placeable since its
        // RegisterAck, so another submit may have filled it meanwhile, and
        // moving the tasks anyway put more subtasks on the worker than it has
        // slots. A worker with no live session left, or one whose new session
        // is full, fails the deploy retryably, as a lack of slots does; the
        // slots charged here go back with it.
        for (auto& [worker_id, target] : targets) {
            const auto current = registered_.find(worker_id);
            if (current == registered_.end() || current->second->lost) {
                throw InsufficientSlotsError("Coordinator::deploy: worker '" + worker_id +
                                             "' was lost after the job's tasks were placed on it");
            }
            if (current->second == target) {
                continue;
            }
            const auto n = charged[worker_id];
            const auto& successor = *current->second;
            const std::uint32_t room = successor.slots_in_use < successor.slot_capacity
                                           ? successor.slot_capacity - successor.slots_in_use
                                           : 0;
            if (room < n) {
                throw InsufficientSlotsError(
                    "Coordinator::deploy: worker '" + worker_id +
                    "' re-registered after the job's tasks were placed on it, and its new "
                    "session has " +
                    std::to_string(room) + " free slot(s) for the job's " + std::to_string(n));
            }
            release_slots_locked_(*target, n);
            charge_slots_locked_(*current->second, n);
            target = current->second;
            log::info("coordinator.submit",
                      "job_id=" + std::to_string(job_id) + " worker=" + worker_id +
                          " re-registered while the job was being deployed; its " +
                          std::to_string(n) + " task(s) go to the new session");
        }
        // The Row sidecar layout this deployment may use, from its operators
        // and the sessions it is about to be sent on. Submit, HA recovery and
        // the embedded engine all deploy through here, so each decides it
        // afresh.
        {
            const auto admission = admission_on(registered_, by_worker);
            job->row_layout = admission.layout;
            row_layout_note = admission.describe();
        }
        row_layout = job->row_layout;
        for (const auto& [worker_id, tasks] : by_worker) {
            const auto deploy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count();
            const auto target = targets.find(worker_id);
            const std::uint64_t session = target == targets.end() ? 0 : target->second->session;
            for (const auto& t : tasks) {
                const std::string key = t.role + ":" + std::to_string(t.subtask_idx);
                job->task_records[key] = {worker_id, t};
                if (auto oit = plan_op_identity.find(key); oit != plan_op_identity.end()) {
                    job->task_op_identity[key] = oit->second;
                }
                job->subtask_timing[key].started_ms = deploy_ms;
                job->subtask_timing[key].finished_ms = 0;  // (re)deploy clears any prior finish
                job->pending_per_worker[worker_id].emplace_back(t.role, t.subtask_idx);
                job->pending_session[key] = session;
            }
            job->tasks_by_worker[worker_id] = tasks;
        }
        // If no generic-role tasks, there's no port-discovery handshake;
        // mark peer_updates_sent=true to short-circuit the bookkeeping.
        if (job->expected_listenings == 0) {
            job->peer_updates_sent = true;
        }
        job->plugins = plugins;  // copy; each Deploy gets its own copy below
        job->checkpoint = checkpoint;
        jobs_[job_id] = job;
        release_unless_published.published = true;
    }

    metrics::coordinator::job_submitted();
    log::info("coordinator.submit",
              "job_id=" + std::to_string(job_id) +
                  " tasks=" + std::to_string(resolved_plan.tasks.size()));
    events::publish("coordinator.job_submitted",
                    "{\"job_id\":" + std::to_string(job_id) +
                        ",\"tasks\":" + std::to_string(resolved_plan.tasks.size()) + "}");

    // Send Deploy to each affected worker.
    std::size_t stamped_ops = 0;
    for (auto& [worker_id, tasks] : by_worker) {
        // The socket is copied in the hold: reap_finished_workers_ resets
        // WorkerConnection::conn under mu_, so reading it after the hold
        // races that reset and can dereference a null pointer.
        std::shared_ptr<network::Connection> conn;
        std::vector<PluginBinary> worker_plugins;
        {
            std::lock_guard lock(mu_);
            auto it = targets.find(worker_id);
            if (it == targets.end()) {
                throw std::runtime_error("Coordinator::deploy: worker not registered: " +
                                         worker_id);
            }
            conn = it->second->conn;
            worker_plugins = plugins_for_worker_locked_(*job, *it->second);
        }
        DeployMsg deploy_msg;
        deploy_msg.job_id = job_id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = std::move(worker_plugins);
        deploy_msg.checkpoint_dir = checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = checkpoint.state_backend_uri;
        deploy_msg.capture_dir = checkpoint.capture_dir;
        deploy_msg.capture_records = checkpoint.capture_records;
        deploy_msg.restore_from_dir = checkpoint.restore_from_dir;
        deploy_msg.restore_from_checkpoint_id = checkpoint.restore_from_checkpoint_id;
        // Generation 1 is an initial deploy. A restore here reads an EXTERNAL
        // directory (a savepoint, or an explicit --restore-from-checkpoint-id), so
        // its generation is whatever that directory holds rather than anything this
        // job knows - discovered by listing it. See docs/design/state-generations.md.
        deploy_msg.generation = job->state_generation;
        deploy_msg.restore_generation = checkpoint.restore_from_dir.empty()
                                            ? job->state_generation
                                            : highest_generation_in(checkpoint.restore_from_dir);
        deploy_msg.unaligned_checkpoints = checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.adaptive_barrier_mode = checkpoint.alignment == CheckpointAlignment::Adaptive;
        deploy_msg.expected_state_versions_packed = job->expected_state_versions_packed;
        deploy_msg.udfs_packed = job->udfs_packed;
        stamped_ops += stamp_row_layout(deploy_msg.tasks, row_layout);
        const auto frame = fenced_frame_(MessageKind::Deploy, deploy_msg);
        if (!conn || !send_frame(*conn, frame)) {
            throw std::runtime_error("Coordinator::deploy: send failed for " + worker_id);
        }
    }
    log::info("coordinator.submit",
              "job_id=" + std::to_string(job_id) + " " + row_layout_note +
                  " stamped_ops=" + std::to_string(stamped_ops));
    // Recorded after the deploy frames went out, as HA recovery records its own.
    if (resumed_from_existing && protocol_trace::enabled()) {
        protocol_trace::Event("Redeploy")
            .u("job", job_id)
            .u("restore", checkpoint.restore_from_checkpoint_id)
            .u("next", job->first_checkpoint_id)
            .emit();
    }
    return job_id;
}

// The frames one worker connection's reader has read and not answered itself,
// in the order it read them, waiting for that connection's dispatch thread.
// Shared by the two threads; mu is a leaf lock, never held across a dispatch
// or a join.
struct Coordinator::WorkerDispatchQueue {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::vector<std::byte>> frames;
    std::size_t bytes{0};
    // Set by the reader when it exits; the dispatcher handles what is queued
    // (frames read before a peer EOF are dispatched, as they always were) and
    // returns.
    bool closed{false};
    // Set when the connection has been given up: a frame did not decode or
    // the backlog overflowed. The backlog is cleared and the reader stops
    // reading, so last_seen stops moving and the watchdog declares the
    // worker lost.
    std::atomic<bool> failed{false};
    // Joined by the reader on its way out, so a finished reader still means
    // nothing of this connection is running.
    std::thread dispatcher;
};

void Coordinator::start_reader_for_(std::shared_ptr<WorkerConnection> worker,
                                    std::shared_ptr<WorkerConnection> predecessor,
                                    bool predecessor_was_lost) {
    // Shuts a session down whose threads could not all be started: the reader
    // is reported finished so the reaper releases the connection, and with
    // nothing stamping last_seen the watchdog declares the worker lost.
    const auto abandon = [&worker](const char* which, const char* why) noexcept {
        if (worker->conn) {
            worker->conn->shutdown_read();
        }
        worker->reader_finished->store(true, std::memory_order_release);
        try {
            log::error("coordinator.worker",
                       std::string{"could not start the "} + which + " thread for worker '" +
                           worker->worker_id + "': " + why + "; dropping its connection");
        } catch (...) {
            // Only the log line was lost.
        }
    };
    // Both threads start here, on the registering thread, so a failure to
    // create one is caught rather than escaping a thread function, which is
    // std::terminate. Allocating the queue, and std::thread's constructor,
    // throw std::bad_alloc or std::system_error; either way the session is
    // shut down, and the predecessor, which no dispatch thread now owns, is
    // retired here, on the registering thread.
    std::shared_ptr<WorkerDispatchQueue> queue;
    try {
        queue = std::make_shared<WorkerDispatchQueue>();
        queue->dispatcher =
            std::thread([this, worker, queue, predecessor, predecessor_was_lost]() mutable {
                if (predecessor) {
                    retire_replaced_session_(*worker, predecessor, predecessor_was_lost);
                    predecessor.reset();  // its socket, not held for this session's life
                }
                try {
                    worker_dispatch_loop_(worker, *queue);
                } catch (...) {
                    // Nothing in the loop is meant to throw past it. If
                    // something does, the connection is given up rather than
                    // the process: the reader stops at its next frame.
                    queue->failed.store(true, std::memory_order_release);
                }
            });
    } catch (const std::exception& e) {
        abandon("dispatch", e.what());
        if (predecessor) {
            retire_replaced_session_(*worker, predecessor, predecessor_was_lost);
        }
        return;
    }
    try {
        worker->reader = std::thread([this, worker, queue] {
            // Set on EVERY exit path, so a finished reader is distinguishable from a
            // running one and can be joined. Without it the thread and its socket
            // were held until stop(). Captured by value so it outlives the
            // WorkerConnection if that is ever replaced under the same id.
            auto finished = worker->reader_finished;
            struct MarkFinished {
                std::shared_ptr<std::atomic<bool>> flag;
                ~MarkFinished() { flag->store(true, std::memory_order_release); }
            } mark{finished};
            // Declared after `mark`, so it runs first: the dispatcher has
            // finished before the reader reports itself finished.
            struct JoinDispatcher {
                WorkerDispatchQueue& q;
                ~JoinDispatcher() {
                    {
                        std::lock_guard lock(q.mu);
                        q.closed = true;
                    }
                    q.cv.notify_all();
                    if (q.dispatcher.joinable()) {
                        q.dispatcher.join();
                    }
                }
            } join_dispatcher{*queue};
            // Never throws: it runs on the way out of a catch.
            const auto give_up = [&](const std::string& why) noexcept {
                {
                    std::lock_guard lock(queue->mu);
                    queue->failed.store(true, std::memory_order_release);
                    queue->frames.clear();
                    queue->bytes = 0;
                }
                try {
                    log::warn("coordinator.worker",
                              "dropping worker '" + worker->worker_id + "': " + why);
                    metrics::orch::malformed_frame();
                } catch (...) {
                    // Only the log line or the count was lost.
                }
            };
            try {
                while (!stop_.load(std::memory_order_acquire)) {
                    if (!worker->conn)
                        return;
                    auto frame = read_frame(*worker->conn);
                    if (!frame.has_value()) {
                        return;  // peer closed
                    }
                    // The dispatcher gave up on this connection. Stop reading,
                    // and so stop stamping: the worker is declared lost as it
                    // was when the reader itself met the bad frame.
                    if (queue->failed.load(std::memory_order_acquire)) {
                        return;
                    }
                    if (worker->lost.load(std::memory_order_acquire)) {
                        continue;
                    }
                    // Every frame read is proof of life, whatever its dispatch
                    // is waiting on.
                    worker->last_seen.store(std::chrono::steady_clock::now(),
                                            std::memory_order_release);
                    if (!frame->empty() &&
                        static_cast<MessageKind>((*frame)[0]) == MessageKind::Heartbeat) {
                        // Answered here, never queued: a heartbeat carries no
                        // protocol state, and its answer is what keeps the
                        // worker's own lease alive while a handler waits on the
                        // store.
                        try {
                            MessageReader r(std::move(*frame));
                            (void)r.read_u8();
                            answer_heartbeat_(*worker, r);
                        } catch (const std::exception& e) {
                            give_up(std::string{"frame did not decode: "} + e.what());
                            return;
                        }
                        continue;
                    }
                    bool overflow = false;
                    {
                        std::lock_guard lock(queue->mu);
                        const auto size = frame->size();
                        if (!queue->frames.empty() &&
                            (queue->frames.size() >= cfg_.max_worker_dispatch_backlog_frames ||
                             queue->bytes + size > cfg_.max_worker_dispatch_backlog_bytes)) {
                            overflow = true;
                        } else {
                            queue->frames.push_back(std::move(*frame));
                            queue->bytes += size;
                        }
                    }
                    if (overflow) {
                        give_up("its dispatch backlog exceeded " +
                                std::to_string(cfg_.max_worker_dispatch_backlog_frames) +
                                " frames or " +
                                std::to_string(cfg_.max_worker_dispatch_backlog_bytes) +
                                " bytes (max_worker_dispatch_backlog_frames / _bytes)");
                        return;
                    }
                    queue->cv.notify_one();
                }
            } catch (const std::exception& e) {
                // An allocation that failed reading or queueing a frame. The
                // connection is given up, as for a frame that does not decode,
                // rather than the exception leaving the thread.
                give_up(std::string{"its reader failed: "} + e.what());
            } catch (...) {
                give_up("its reader failed");
            }
        });
    } catch (const std::exception& e) {
        {
            std::lock_guard lock(queue->mu);
            queue->closed = true;
        }
        queue->cv.notify_all();
        // The dispatcher retires the predecessor first, so this also waits for
        // that; then it finds the queue closed and empty and returns.
        queue->dispatcher.join();
        abandon("reader", e.what());
    }
}

// The only consumer of a connection's queue: one frame at a time, in the order
// the reader read them, so every frame but a heartbeat is handled in the same
// order, and by the same handler, as when the reader ran the handlers itself.
//
// Liveness now means the worker's socket delivers frames, not that its frames
// are being handled. A dispatcher stalled for ever (a store write that never
// returns) no longer gets its worker declared lost; and one stalled past
// restart_drain_timeout during a restart keeps that restart's drain open, since
// the subtask exits queued behind it are not handled, until the watchdog fails
// the job as it fails any drain a survivor that still heartbeats does not
// finish.
void Coordinator::worker_dispatch_loop_(const std::shared_ptr<WorkerConnection>& worker,
                                        WorkerDispatchQueue& queue) {
    while (true) {
        std::vector<std::byte> frame;
        {
            std::unique_lock lock(queue.mu);
            queue.cv.wait(lock, [&] {
                return queue.closed || !queue.frames.empty() ||
                       queue.failed.load(std::memory_order_relaxed);
            });
            if (queue.failed.load(std::memory_order_relaxed) || queue.frames.empty()) {
                return;  // given up, or closed with nothing left to handle
            }
            frame = std::move(queue.frames.front());
            queue.frames.pop_front();
            queue.bytes -= frame.size();
        }
        // stop() abandons a backlog: a frame already being handled finishes,
        // nothing queued behind it starts.
        if (stop_.load(std::memory_order_acquire)) {
            return;
        }
        // A lost session's frames are dropped, as the reader drops them. This
        // check is only a fast path: the loss can be declared between it and
        // the handler's hold of mu_, so every handler checks again in that
        // hold, where the loss fold cannot interleave. A superseded session's
        // are not dropped: the reader read them before the
        // re-registration woke it, so they happened first, and the retirement
        // waits for them (retire_replaced_session_). Dropping them turned a
        // subtask that had reported a clean finish into a lost one.
        if (worker->lost.load(std::memory_order_acquire)) {
            continue;
        }
        MessageReader r(std::move(frame));
        // A registered worker sending a malformed frame is either a
        // version skew or a bug in the worker. Either way it must cost
        // that worker its connection, not the coordinator its life.
        try {
            dispatch_worker_frame_(worker, r);
        } catch (const std::exception& e) {
            {
                std::lock_guard lock(queue.mu);
                queue.failed.store(true, std::memory_order_release);
                queue.frames.clear();
                queue.bytes = 0;
            }
            log::warn(
                "coordinator.worker",
                "dropping worker '" + worker->worker_id + "': frame did not decode: " + e.what());
            metrics::orch::malformed_frame();
            return;
        }
    }
}

void Coordinator::answer_heartbeat_(WorkerConnection& worker, MessageReader& r) {
    const auto heartbeat = decode_heartbeat(r);
    if (heartbeat.worker_id != worker.worker_id) {
        throw std::runtime_error("Heartbeat names worker '" + heartbeat.worker_id +
                                 "', but this session belongs to '" + worker.worker_id + "'");
    }
    // HeartbeatAck is a v2 capability. Sending an unknown frame to a
    // v1 worker would turn a compatible rolling upgrade into a forced
    // disconnect, so the registration version gates it explicitly.
    if (worker.protocol_version >= 2 && worker.conn) {
        HeartbeatAckMsg ack{.worker_id = worker.worker_id, .sequence = heartbeat.sequence};
        const auto frame = fenced_frame_(MessageKind::HeartbeatAck, ack);
        if (!send_frame(*worker.conn, frame)) {
            worker.conn->close();
        }
    }
}

// Dispatch one decoded worker frame. Extracted from the dispatch loop so
// the loop can bound a throw to a single frame.
void Coordinator::dispatch_worker_frame_(const std::shared_ptr<WorkerConnection>& worker,
                                         MessageReader& r) {
    const auto kind = static_cast<MessageKind>(r.read_u8());
    switch (kind) {
        case MessageKind::SubtaskFinished:
            handle_subtask_finished_(r, *worker);
            break;
        case MessageKind::BeginRescaleAck:
            handle_begin_rescale_ack_(r, *worker);
            break;
        case MessageKind::SubtaskListening:
            handle_subtask_listening_(r, *worker);
            break;
        case MessageKind::Heartbeat:
            // The reader answers heartbeats itself and never queues one, so
            // this is reached only if that changes.
            answer_heartbeat_(*worker, r);
            break;
        case MessageKind::SubtaskCheckpointed:
            handle_subtask_checkpointed_(r, *worker);
            break;
        case MessageKind::CommitConfirmed:
            handle_commit_confirmed_(r, *worker);
            break;
        case MessageKind::RequestFinalCheckpoint:
            if (worker->conn) {
                handle_request_final_checkpoint_(r, *worker, worker->conn);
            }
            break;
        default:
            // See the worker's dispatch: an unhandled kind is a version
            // boundary, not a no-op, and it must be visible.
            clink::metrics::orch::unknown_control_frame();
            clink::log::warn("coordinator.protocol",
                             "dropping an unrecognised control frame (kind " +
                                 std::to_string(static_cast<int>(kind)) +
                                 "): this build cannot handle it, so whatever the peer "
                                 "expected of it will not happen");
            break;
    }
}

void Coordinator::handle_subtask_listening_(MessageReader& r, const WorkerConnection& sender) {
    auto msg = decode_subtask_listening(r);
    // Hot-cutover frames staged under the lock (rebind-complete deploys,
    // completion peer updates), sent after it.
    std::vector<PendingDeploy> hot_frames;
    {
        std::lock_guard lock(mu_);
        if (sender.lost) {
            return;  // the loss fold has already accounted for this session
        }
        auto it = jobs_.find(msg.job_id);
        if (it == jobs_.end()) {
            return;  // stale message
        }
        auto& job = *it->second;
        // The worker advertises its own data_host via Register; we trust
        // that for the peer-resolution. Fall back to msg.host for
        // hostless deployments.
        std::string host = msg.host;
        auto worker_it = registered_.find(msg.worker_id);
        if (worker_it != registered_.end() && !worker_it->second->data_host.empty()) {
            host = worker_it->second->data_host;
        }
        // Empty edge_ports means the subtask has no inbound listener
        // (pure source). We still record a "no bridge" sentinel so
        // duplicate listenings can be detected. Either way, this
        // subtask's "I'm ready" tick advances received_listenings once.
        for (const auto& ep : msg.edge_ports) {
            JobState::EdgeKey key{
                .downstream_role = msg.role,
                .downstream_subtask_idx = msg.subtask_idx,
                .upstream_role = ep.upstream_role,
                .upstream_subtask_idx = ep.upstream_subtask_idx,
            };
            if (job.ports.find(key) != job.ports.end()) {
                continue;  // duplicate, ignore.
            }
            job.ports[key] = {host, ep.port};
        }
        ++job.received_listenings;
        if (job.received_listenings == job.expected_listenings && !job.peer_updates_sent) {
            send_peer_updates_locked_(job);
            job.peer_updates_sent = true;
        }

        // Hot cutover, rebind phase: the fed tasks report the listeners
        // they bound for the NEW upstream indices via mid-run listenings.
        // The ports landed in job.ports above; once every awaited
        // (fed task, new index) pair has reported, the new subtasks can
        // deploy - their PeerUpdate resolution needs these ports.
        if (job.hot_cutover.has_value() &&
            job.hot_cutover->phase == JobState::HotCutover::Phase::Rebinding) {
            auto& hot = *job.hot_cutover;
            const std::string task_key = msg.role + ":" + std::to_string(msg.subtask_idx);
            for (const auto& ep : msg.edge_ports) {
                if (ep.upstream_subtask_idx >= hot.appended_base) {
                    hot.rebind_ports_pending.erase({task_key, ep.upstream_subtask_idx});
                }
            }
            if (hot.rebind_ports_pending.empty()) {
                hot_cutover_deploy_locked_(job, hot_frames);
            }
        }

        // If the listening subtask's operator is in
        // CuttingOver, treat the SubtaskListening as the readiness
        // signal that closes the rescale. Mark the new subtask
        // ready; when every new subtask has reported the coordinator
        // transitions CuttingOver -> Complete. The ack names the shared
        // generic role; the state machine is keyed by operator, so
        // translate through the deploy-time identity record (F40).
        if (job.rescale_coordinator) {
            const auto ack = op_scoped_ack(job.task_op_identity, msg.role, msg.subtask_idx);
            if (auto st = job.rescale_coordinator->status(ack.op_id);
                st.has_value() && st->state == RescaleState::CuttingOver) {
                job.rescale_coordinator->mark_new_ready(ack.op_id, ack.subtask_idx_in_op);
                if (auto post = job.rescale_coordinator->status(ack.op_id);
                    post.has_value() && post->state == RescaleState::Complete) {
                    log::info("coordinator.rescale",
                              "complete job_id=" + std::to_string(job.id) + " op_id=" + ack.op_id +
                                  " new_parallelism=" + std::to_string(post->target_parallelism));
                    if (job.hot_cutover.has_value() && job.hot_cutover->op_id == ack.op_id) {
                        hot_cutover_complete_locked_(job, hot_frames);
                    }
                }
            }
        }
    }
    for (auto& f : hot_frames) {
        if (f.conn != nullptr) {
            send_frame(*f.conn, f.frame);
        }
    }
}

void Coordinator::send_peer_updates_locked_(JobState& job) {
    // Group per worker. Each worker's PeerUpdate carries the resolved peers
    // for every task on that worker that has a non-empty peers[] list.
    //
    // The peer-resolution key is the 4-tuple
    // (downstream_role, downstream_sub, upstream_role, upstream_sub).
    // Each task's `peers` entries identify the downstream this task
    // wants to connect to; combined with the task's own (role, sub)
    // those form the lookup key.
    std::unordered_map<std::string, PeerUpdateMsg> per_worker;
    for (const auto& [worker_id, tasks] : job.tasks_by_worker) {
        for (const auto& task : tasks) {
            if (task.peers.empty()) {
                continue;
            }
            PeerUpdateMsg::TaskPeers tp;
            tp.role = task.role;
            tp.subtask_idx = task.subtask_idx;
            for (const auto& peer : task.peers) {
                JobState::EdgeKey key{
                    .downstream_role = peer.role,
                    .downstream_subtask_idx = peer.subtask_idx,
                    .upstream_role = task.role,
                    .upstream_subtask_idx = task.subtask_idx,
                };
                auto it = job.ports.find(key);
                PeerAddress resolved = peer;
                if (it != job.ports.end()) {
                    resolved.host = it->second.first;
                    resolved.data_port = it->second.second;
                }
                tp.peers.push_back(std::move(resolved));
            }
            per_worker[worker_id].tasks.push_back(std::move(tp));
        }
    }
    for (auto& [worker_id, msg] : per_worker) {
        msg.job_id = job.id;
        auto worker_it = registered_.find(worker_id);
        if (worker_it == registered_.end() || worker_it->second->lost) {
            continue;
        }
        const auto frame = fenced_frame_(MessageKind::PeerUpdate, msg);
        if (worker_it->second->conn)
            send_frame(*worker_it->second->conn, frame);
    }

    // Subtasks with empty peers[] still need a "go" signal. Send an
    // empty PeerUpdate to each worker that hosts at least one such task and
    // hasn't already received a real update.
    std::unordered_set<std::string> workers_with_real_update;
    for (const auto& [worker_id, _] : per_worker) {
        workers_with_real_update.insert(worker_id);
    }
    for (const auto& [worker_id, _] : job.tasks_by_worker) {
        if (workers_with_real_update.count(worker_id) > 0) {
            continue;
        }
        auto worker_it = registered_.find(worker_id);
        if (worker_it == registered_.end() || worker_it->second->lost) {
            continue;
        }
        PeerUpdateMsg empty;
        empty.job_id = job.id;
        const auto frame = fenced_frame_(MessageKind::PeerUpdate, empty);
        if (worker_it->second->conn)
            send_frame(*worker_it->second->conn, frame);
    }
}

void Coordinator::watchdog_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(cfg_.watchdog_interval);
        if (stop_.load(std::memory_order_acquire)) {
            return;
        }
        // Hot-cutover phase deadlines: a cutover stuck in any phase past
        // its bound aborts to the replan path. Checked here rather than in
        // any handler because the failure mode is precisely that no more
        // messages arrive.
        {
            std::vector<PendingDeploy> abort_frames;
            {
                std::lock_guard lock(mu_);
                const auto now = std::chrono::steady_clock::now();
                for (auto& [jid, job_ptr] : jobs_) {
                    auto& job = *job_ptr;
                    if (job.hot_cutover.has_value() && now > job.hot_cutover->phase_deadline) {
                        abort_hot_cutover_locked_(
                            job,
                            "phase timed out (phase " +
                                std::to_string(static_cast<int>(job.hot_cutover->phase)) + ")",
                            abort_frames);
                    }
                }
            }
            for (auto& f : abort_frames) {
                if (f.conn != nullptr) {
                    send_frame(*f.conn, f.frame);
                }
            }
        }
        const auto now = std::chrono::steady_clock::now();
        bool any_lost = false;
        std::vector<std::pair<std::shared_ptr<network::Connection>, JobId>> survivor_cancels;
        std::vector<PendingDeploy> deferred_restart_deploys;
        {
            std::lock_guard lock(mu_);
            // Self-pause detection. A sweep that is itself late by more than
            // the staleness bound was suspended (SIGSTOP, VM migration, a
            // long GC-like stall) or starved - and during that gap NO
            // worker's heartbeat could be read, so every last_seen is stale
            // by the coordinator's own pause, not the workers'. Judging
            // staleness on this sweep declares every worker lost at once;
            // mark_worker_lost_locked_ then shutdown_read()s their healthy
            // connections, they exit on the severed control plane, and the
            // restart finds no capacity anywhere: a paused-then-resumed
            // coordinator destroys its own healthy cluster. Found by the
            // hung-but-alive test the moment it first ran.
            //
            // The remedy is one full timeout of grace: refresh every live
            // worker's last_seen to now, so a worker that genuinely died
            // DURING the pause is still declared lost - one heartbeat_timeout
            // later than it would have been, which is the honest price of
            // the coordinator having been absent for the evidence window.
            if (now - last_watchdog_sweep_ > cfg_.watchdog_interval + cfg_.heartbeat_timeout) {
                log::warn("coordinator.watchdog",
                          "watchdog resumed after a suspension; deferring staleness "
                          "judgement one interval");
                for (auto& [_, worker] : registered_) {
                    if (!worker->lost) {
                        worker->last_seen.store(now, std::memory_order_release);
                    }
                }
            }
            last_watchdog_sweep_ = now;
            for (auto& [_, worker] : registered_) {
                if (worker->lost) {
                    continue;
                }
                // A reader stamps without mu_, so a stamp may be later than
                // `now`; the difference is then negative and never stale.
                if (now - worker->last_seen.load(std::memory_order_acquire) >
                    cfg_.heartbeat_timeout) {
                    mark_worker_lost_locked_(*worker);
                    any_lost = true;
                }
            }
            if (any_lost) {
                // Scan jobs touched by lost workers and broadcast CancelJob
                // to each of their surviving workers. The survivor's role
                // handler is expected to poll was_cancelled() and exit,
                // which makes its worker emit a normal SubtaskFinished that
                // increments completed_count via the regular path.
                for (auto& [job_id, job] : jobs_) {
                    bool job_touched = false;
                    for (const auto& [worker_id, _] : job->tasks_by_worker) {
                        auto it = registered_.find(worker_id);
                        if (it != registered_.end() && it->second->lost) {
                            job_touched = true;
                            break;
                        }
                    }
                    if (!job_touched) {
                        continue;
                    }
                    for (const auto& [worker_id, _] : job->tasks_by_worker) {
                        auto it = registered_.find(worker_id);
                        if (it != registered_.end() && !it->second->lost && it->second->conn) {
                            survivor_cancels.emplace_back(it->second->conn, job_id);
                        }
                    }
                    // If the job is awaiting_restart and no surviving
                    // subtasks still owe a drain (drained COVERS expected;
                    // they may have finished before the watchdog tick),
                    // kick off the redeploy here - unless an in-doubt
                    // resolution must answer first, in which case the
                    // resolution thread fires the restart.
                    if (job->awaiting_restart && restart_drain_covered_(*job) &&
                        !stage_in_doubt_resolution_locked_(*job)) {
                        auto deploys = restart_job_locked_(*job);
                        for (auto& d : deploys)
                            deferred_restart_deploys.push_back(std::move(d));
                    }
                    // Only force-signal completion if the synthesised
                    // errors from mark_worker_lost_locked_ already brought
                    // completed_count to expected_completion - i.e. every
                    // task was on lost workers. With a survivor still in
                    // flight, wait for its SubtaskFinished to arrive
                    // naturally.
                    if (!job->awaiting_restart &&
                        job->completed_count >= job->expected_completion) {
                        signal_job_completion_locked_(*job);
                    }
                }
            }
            // Bounded restart drain (runs every tick, independent of
            // any_lost): fail any job whose awaiting_restart drain has
            // outrun its deadline. This catches the case a lost-worker fold
            // can't - a survivor that is hung but still heartbeating, so it
            // neither acks the cancel nor dies. Failing is the safe
            // escalation; force-restarting could double-run a slow-but-alive
            // survivor's subtask against shared state.
            // Empty-drain kick, unconditional (F12).
            //
            // A job awaiting_restart with an EMPTY expected-drain set has nothing to
            // wait for and must be restarted now. There is a kick for this inside the
            // lost-worker block above, but that block is gated on a worker having been
            // newly declared lost in THIS tick - so it only ever rescues a job whose
            // restart was entered from the watchdog itself.
            //
            // A restart entered from the REGISTER path (a worker coming back and its
            // previous session being retired) sets awaiting_restart with an empty drain
            // set and no kick behind it. Nothing then called restart_job_locked_, the
            // job sat waiting for a drain of zero subtasks, and the deadline below
            // failed it 30s later with "survivors did not drain" - when there were no
            // survivors to drain and the real fault was that the restart was never
            // fired.
            //
            // Placed with the deadline sweep rather than in the lost-worker block
            // because both need to run every tick regardless of what happened in it:
            // the condition is a property of the job's state, not of this tick's
            // events.
            for (auto& [_, job] : jobs_) {
                // Coverage, not emptiness: a fold can shrink the expected
                // set to keys that already drained (the survivor's ack
                // arrived BEFORE the dead worker's fold), and no further
                // SubtaskFinished will ever arrive to re-evaluate it. This
                // was watch item 63's wedge: a ready restart that nothing
                // fired, failed 30s later as a phantom "survivors did not
                // drain".
                if (job->awaiting_restart && !job->completion_signalled && !job->cancel_requested &&
                    restart_drain_covered_(*job) && !stage_in_doubt_resolution_locked_(*job)) {
                    auto deploys = restart_job_locked_(*job);
                    for (auto& d : deploys) {
                        deferred_restart_deploys.push_back(std::move(d));
                    }
                }
            }
            // A cancel that wins over a restart. The kick above re-fires a
            // waiting restart only for a job no one has cancelled, and the
            // terminal-cancel bound below skips a job awaiting a restart. A
            // cancel that lands in the drain, in in-doubt resolution or in a
            // capacity wait therefore left the job cancelled and running for
            // good, with nothing running to report a SubtaskFinished. Once
            // the drain is covered and no resolution is under way, nothing
            // more can happen to it: end it as the cancel asked.
            for (auto& [_, job] : jobs_) {
                if (job->awaiting_restart && job->cancel_requested && !job->completion_signalled &&
                    !job->resolving_in_doubt && restart_drain_covered_(*job)) {
                    log::info("coordinator.restart",
                              "job_id=" + std::to_string(job->id) +
                                  " was cancelled while its restart waited; ending it");
                    job->awaiting_restart = false;
                    job->restart_deadline = {};
                    job->restart_capacity_deadline = {};
                    job->restart_pending.clear();
                    job->restart_drained_keys.clear();
                    job->restart_drain_expected.clear();
                    job->completed_count = job->expected_completion;
                    signal_job_completion_locked_(*job);
                }
            }
            // A transport failure that waited for its cause and never got
            // one. In a healthy job nothing else is coming: the refused send
            // IS the cause, and acting on it here is what keeps the bridge's
            // detector meaningful rather than advisory (item 83). Placed with
            // the deadline sweep because, like the restart backstop below, the
            // condition is a property of the job's state rather than of any
            // one tick's events.
            for (auto& [_, job] : jobs_) {
                if (!job->awaiting_restart && !job->completion_signalled &&
                    !job->cancel_requested &&
                    job->transport_error_deadline != std::chrono::steady_clock::time_point{} &&
                    now > job->transport_error_deadline) {
                    const std::string cause = job->transport_pending_cause;
                    job->transport_error_deadline = {};
                    job->transport_pending_cause.clear();
                    log::warn(
                        "coordinator.restart",
                        "job_id=" + std::to_string(job->id) + " a transport failure waited " +
                            std::to_string(cfg_.transport_symptom_grace.count()) +
                            "ms and no cause arrived, so it IS the cause; restarting: " + cause);
                    auto deploys = initiate_job_restart_locked_(
                        *job, "transport failure", cause, survivor_cancels);
                    for (auto& d : deploys) {
                        deferred_restart_deploys.push_back(std::move(d));
                    }
                }
            }
            for (auto& [_, job] : jobs_) {
                if (job->awaiting_restart && !job->completion_signalled &&
                    job->restart_deadline != std::chrono::steady_clock::time_point{} &&
                    now > job->restart_deadline) {
                    // Backstop for a wedged in-doubt resolution (its wire
                    // timeouts bound it far below the 90s the stage set, so
                    // reaching here means the resolver hung): fail the job
                    // naming the resolution, not a phantom drain. Clearing
                    // the flag makes a late-returning resolver skip its
                    // deferred restart - the job is already failed.
                    if (job->resolving_in_doubt && !job->in_doubt_cancel_requested) {
                        // Stage one: the walk is SLOW, not proven hung. Ask
                        // it to stop mutating and give it a hard grace to
                        // return; the resolution loop then fires the
                        // deferred restart at the un-advanced restore point
                        // - the bounded contract, not a dead job.
                        job->in_doubt_cancel_requested = true;
                        if (job->in_doubt_cancel) {
                            job->in_doubt_cancel->store(true, std::memory_order_release);
                        }
                        job->restart_deadline =
                            std::chrono::steady_clock::now() + std::chrono::seconds{60};
                        log::warn("coordinator.watchdog",
                                  "job_id=" + std::to_string(job->id) +
                                      " in-doubt resolution outran its deadline; cancelling "
                                      "the walk - the restart proceeds on the bounded "
                                      "contract once it returns");
                        continue;
                    }
                    if (job->resolving_in_doubt) {
                        // Stage two: the walk ignored the cancel through the
                        // hard grace - genuinely hung. Failing is the safe
                        // escalation, exactly as before.
                        job->resolving_in_doubt = false;
                        log::warn("coordinator.watchdog",
                                  "job_id=" + std::to_string(job->id) +
                                      " in-doubt resolution timed out; failing job");
                        job->errors.push_back(
                            "in-doubt resolution hung past its cancel grace; the restart "
                            "was held to keep resolution and restore-point selection one "
                            "decision");
                        job->awaiting_restart = false;
                        job->restart_deadline = {};
                        job->restart_pending.clear();
                        job->restart_drained_keys.clear();
                        job->restart_drain_expected.clear();
                        job->completed_count = job->expected_completion;
                        signal_job_completion_locked_(*job);
                        continue;
                    }
                    // Name the keys still owed and the workers that own them:
                    // a drain timeout with an anonymous survivor is
                    // undiagnosable from a transcript, and this line is what
                    // splits "which subtask" from "which worker" in a
                    // post-mortem (watch item 63).
                    std::string undrained;
                    for (const auto& key : job->restart_drain_expected) {
                        std::string owner = "?";
                        for (const auto& [worker_id, pending] : job->pending_per_worker) {
                            for (const auto& [role, sub] : pending) {
                                if (role + ":" + std::to_string(sub) == key) {
                                    owner = worker_id;
                                }
                            }
                        }
                        if (!undrained.empty()) {
                            undrained += ", ";
                        }
                        undrained += key + " on " + owner;
                    }
                    log::warn("coordinator.watchdog",
                              "job_id=" + std::to_string(job->id) +
                                  " restart drain timed out; failing job. undrained: " + undrained);
                    job->errors.push_back("restart drain timed out after " +
                                          std::to_string(cfg_.restart_drain_timeout.count()) +
                                          "ms (survivors did not drain: " + undrained + ")");
                    job->awaiting_restart = false;
                    job->restart_deadline = {};
                    job->restart_pending.clear();
                    job->restart_drained_keys.clear();
                    job->restart_drain_expected.clear();
                    job->completed_count = job->expected_completion;
                    signal_job_completion_locked_(*job);
                }
            }
            // Bounded terminal-cancel convergence (followups items 75a and
            // 73). Both the fatal-error broadcast and a client cancel
            // terminate the job by cancelling every peer and waiting for
            // completed_count to reach expected_completion - a COUNT, which
            // used to have no deadline. A peer whose cancel never lands (its
            // worker lost at broadcast time, or a task that finished
            // constructing after its worker flipped the cancel tokens and
            // ran on as an orphan) parks the count short forever: QUAL-06
            // run C reported RUNNING for 75 silent minutes at 291/292 with
            // its verdict already recorded, and run B watched a client
            // cancel sit "ignored" for 40 minutes. The outcome is decided
            // the moment the broadcast fires, so waiting past the deadline
            // buys nothing - force the completion, naming what never
            // reported; the FAILED/CANCELLED precedence is
            // signal_job_completion's as ever. awaiting_restart excluded: a
            // restart entered after an error broadcast owns the job and
            // resets this state itself.
            for (auto& [_, job] : jobs_) {
                if ((job->error_cancel_broadcast || job->cancel_requested) &&
                    !job->completion_signalled && !job->awaiting_restart &&
                    job->terminal_cancel_deadline != std::chrono::steady_clock::time_point{} &&
                    now > job->terminal_cancel_deadline) {
                    std::string unreported;
                    for (const auto& [worker_id, pending] : job->pending_per_worker) {
                        for (const auto& [role, sub] : pending) {
                            if (!unreported.empty()) {
                                unreported += ", ";
                            }
                            unreported += role + ":" + std::to_string(sub) + " on " + worker_id;
                        }
                    }
                    log::warn(
                        "coordinator.watchdog",
                        "job_id=" + std::to_string(job->id) +
                            " terminal-cancel convergence timed out at " +
                            std::to_string(job->completed_count) + "/" +
                            std::to_string(job->expected_completion) +
                            "; failing the job on its recorded verdict. unreported: " +
                            (unreported.empty() ? "(none pending - counts diverged)" : unreported));
                    job->errors.push_back(
                        "terminal-cancel convergence timed out after " +
                        std::to_string(cfg_.restart_drain_timeout.count()) +
                        "ms; peers that never reported: " +
                        (unreported.empty() ? "(none pending - counts diverged)" : unreported));
                    job->terminal_cancel_deadline = {};
                    job->completed_count = job->expected_completion;
                    signal_job_completion_locked_(*job);
                }
            }
        }
        for (const auto& [conn, jid] : survivor_cancels) {
            CancelJobMsg cj;
            cj.job_id = jid;
            send_frame(*conn, fenced_frame_(MessageKind::CancelJob, cj));
        }
        for (auto& d : deferred_restart_deploys) {
            if (d.conn)
                send_frame(*d.conn, d.frame);
        }
        if (any_lost) {
            cv_.notify_all();
        }
    }
}

// The per-job half of losing a worker's in-flight subtasks, shared by the two ways
// that can happen: the watchdog declaring a worker lost, and a worker RE-REGISTERING
// under the same id (its previous SESSION is gone, so whatever that session had in
// flight can never report). Both must reach the same outcome - fold into an
// in-progress restart, or start one - and they did not: the re-registration path
// only handled the fold, so a worker that died and came back before the coordinator
// noticed left its subtasks with nobody to redeploy them and the job hung forever.
// See F64 / follow-up 46.
//
// `cause` names the reason in the synthesised error and the log, because "worker
// lost (heartbeat timeout)" is simply untrue of a worker that is alive and has just
// come back.
void Coordinator::fold_dead_subtasks_into_restart_locked_(JobState& job,
                                                          const std::string& worker_id,
                                                          const char* log_channel,
                                                          const std::string& cause,
                                                          std::uint64_t live_from_session,
                                                          std::uint64_t dead_from_session) {
    // A worker lost mid-hot-cutover ends the cutover: some phase's messages
    // will never arrive. The abort stages the replan at the requested
    // parallelism (setting awaiting_restart and the drain expectations),
    // and the fold below then removes THIS worker's unreachable subtasks
    // from those expectations, exactly as it does for any in-progress
    // restart. The abort's own CancelJob frames are dropped on purpose:
    // the worker-loss path broadcasts survivor cancels itself.
    if (job.hot_cutover.has_value()) {
        std::vector<PendingDeploy> dropped;
        abort_hot_cutover_locked_(
            job, "worker '" + worker_id + "' lost mid-cutover (" + cause + ")", dropped);
    }
    auto it = job.pending_per_worker.find(worker_id);
    if (it == job.pending_per_worker.end()) {
        return;
    }
    // A fold takes only what the dying session held, by the session stamps.
    // A retirement keeps subtasks placed on its successor (stamped with
    // live_from_session or later): they are alive. A lost session that is
    // still retiring its predecessor keeps the predecessor's subtasks
    // (stamped below dead_from_session): that session's frames are still to
    // be handled, and a SubtaskFinished among them reports a subtask folded
    // here, maybe redeployed, against the next run. They stay pending until
    // those frames drain them or the retirement, which runs once they have
    // been handled, folds them. Either way the kept ones leave the dead list
    // here and go back into pending at every exit below, and a restart
    // started here drains them like any survivor.
    const auto spared_stamp = [&](std::uint64_t stamp) {
        return (live_from_session != 0 && stamp >= live_from_session) ||
               (dead_from_session != 0 && stamp < dead_from_session);
    };
    std::vector<std::pair<std::string, std::uint32_t>> live;
    if (live_from_session != 0 || dead_from_session != 0) {
        std::erase_if(it->second, [&](const std::pair<std::string, std::uint32_t>& p) {
            const auto stamp = job.pending_session.find(p.first + ":" + std::to_string(p.second));
            if (stamp == job.pending_session.end() || !spared_stamp(stamp->second)) {
                return false;
            }
            live.push_back(p);
            return true;
        });
    }
    if (protocol_trace::enabled()) {
        // The trace names the worker's subtasks the fold keeps, which live
        // on, so the validator does not kill a sink the engine goes on to
        // drain as a survivor.
        std::vector<std::uint64_t> spared;
        if (live_from_session != 0 || dead_from_session != 0) {
            for (const auto& [key, stamp] : job.pending_session) {
                const auto rec = job.task_records.find(key);
                if (spared_stamp(stamp) && rec != job.task_records.end() &&
                    rec->second.first == worker_id) {
                    spared.push_back(rec->second.second.subtask_idx);
                }
            }
            std::sort(spared.begin(), spared.end());
        }
        auto dies = protocol_trace::Event("WorkerDies");
        dies.u("job", job.id).s("worker", worker_id);
        if (!spared.empty()) {
            dies.us("spared", spared);
        }
        dies.emit();
    }
    // Deliberately NOT also returning when the list is EMPTY. A worker can be lost
    // with nothing in flight for this job - every subtask of its already reported
    // finished - and the job must still roll back to the last checkpoint, because
    // the redeploy set comes from tasks_by_worker rather than from this list. An
    // earlier cut of this refactor added that check and broke recovery in three
    // worker-kill tests: the job never restarted and never checkpointed again.
    // Second worker lost while this job is already draining for a restart.
    // Its subtasks were survivors at the first loss, so they sit in
    // restart_drain_expected - but they can never drain now (their worker is
    // dead). Fold them into the in-progress restart: drop them from the
    // expected-drain set and queue them for redeploy (restart_pending is
    // unioned with restart_drain_expected when restart_job_locked_ builds
    // the task set). The empty-drain kick in watchdog_loop_, or the
    // remaining survivors' drains, then fires the restart onto the workers
    // still alive. Without this the drain never completes and the job
    // wedges in awaiting_restart forever. We do NOT consume a restart
    // attempt here - this is still the same restart, now covering both
    // losses.
    if (job.awaiting_restart && !job.completion_signalled && !job.cancel_requested) {
        for (const auto& [role, sub] : it->second) {
            const std::string k = role + ":" + std::to_string(sub);
            job.restart_drain_expected.erase(k);
            job.restart_drained_keys.erase(k);
            job.restart_pending.emplace_back(role, sub);
        }
        // The kept subtasks drain like any survivor, as they do when this
        // fold starts the restart. The restart in progress may not have
        // counted them: a hot cutover aborted at the top of this fold staged
        // its drain with this worker already lost, and put them up for
        // redeploy instead.
        for (const auto& [role, sub] : live) {
            job.restart_drain_expected.insert(role + ":" + std::to_string(sub));
            std::erase_if(job.restart_pending, [&](const std::pair<std::string, std::uint32_t>& p) {
                return p.first == role && p.second == sub;
            });
        }
        log::warn(log_channel,
                  "job_id=" + std::to_string(job.id) +
                      " second worker lost during restart drain; folded " +
                      std::to_string(it->second.size()) +
                      " subtask(s) into the pending restart, drain_expected=" +
                      std::to_string(job.restart_drain_expected.size()));
        it->second = std::move(live);
        return;
    }
    const bool can_restart = !job.awaiting_restart && !job.completion_signalled &&
                             !job.cancel_requested && !job.checkpoint.checkpoint_dir.empty() &&
                             job.restart_attempts < effective_max_restarts(job.checkpoint);
    if (can_restart) {
        // Defer error synthesis. Capture the lost-worker subtasks for
        // re-deployment, plus the surviving-worker subtasks we need
        // to drain. drain_expected pulls from pending_per_worker
        // (still-in-flight) rather than tasks_by_worker (all subtasks
        // including completed ones) so a sink that already reported
        // SubtaskFinished before the watchdog declared the worker lost
        // doesn't get treated as a still-pending drainee. The
        // empty-drain case (everyone else finished already)
        // triggers restart right here.
        job.awaiting_restart = true;
        // Bound the drain: if survivors don't all report within
        // restart_drain_timeout, the watchdog fails the job rather than
        // wedge (e.g. a survivor that hangs without acking the cancel).
        job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
        // The worker's loss IS the cause the held transport failures were
        // waiting for; absorb them (item 83).
        job.transport_error_deadline = {};
        job.transport_pending_cause.clear();
        // Redeploy set on worker loss = the FULL topology rolled back to
        // the last checkpoint: drain every surviving in-flight subtask
        // (restart_drain_expected) and redeploy every subtask that is not
        // draining (restart_pending = tasks_by_worker minus in-flight,
        // which covers the lost worker's tasks AND cleanly-finished
        // survivors). This mirrors the subtask-error / EOS-timeout path in
        // handle_subtask_finished_.
        //
        // A narrower set - relocating only the lost worker's tasks and
        // leaving survivors running - is safe ONLY at EOS: a still-running
        // upstream survivor keeps a live downstream bridge to a peer on the
        // lost worker, and when that peer is relocated the bridge send fails
        // ("network_bridge_sink: peer gone"). That failure is itself a
        // subtask error, which escalated to a fresh whole-job restart and,
        // over a few hundred milliseconds, burned the entire restart budget
        // - a mid-stream worker kill cascaded to job failure. Rolling the
        // whole job back atomically leaves no survivor holding a stale
        // bridge, so the redeploy converges in one attempt.
        std::unordered_set<std::string> in_flight;
        for (const auto& [role, sub] : live) {
            in_flight.insert(role + ":" + std::to_string(sub));
        }
        for (const auto& [other_worker_id, pending] : job.pending_per_worker) {
            if (other_worker_id == worker_id) {
                continue;
            }
            auto other_it = registered_.find(other_worker_id);
            if (other_it == registered_.end() || other_it->second->lost) {
                continue;
            }
            for (const auto& [role, sub] : pending) {
                in_flight.insert(role + ":" + std::to_string(sub));
            }
        }
        job.restart_drain_expected = in_flight;
        for (const auto& [other_worker_id, dts] : job.tasks_by_worker) {
            for (const auto& dt : dts) {
                const std::string k = dt.role + ":" + std::to_string(dt.subtask_idx);
                if (in_flight.count(k) == 0) {
                    job.restart_pending.emplace_back(dt.role, dt.subtask_idx);
                }
            }
        }
        if (protocol_trace::enabled()) {
            // A survivor that already finished (its input closed with the dead
            // worker before the loss was detected) owes no drain: the protocol
            // counts it drained here, so the trace says so before the restart.
            for (const auto& [other_worker_id, dts] : job.tasks_by_worker) {
                if (other_worker_id == worker_id) {
                    continue;
                }
                auto other_it = registered_.find(other_worker_id);
                if (other_it == registered_.end() || other_it->second->lost) {
                    continue;
                }
                for (const auto& dt : dts) {
                    if (in_flight.count(dt.role + ":" + std::to_string(dt.subtask_idx)) == 0) {
                        protocol_trace::Event("SubtaskDrained")
                            .u("job", job.id)
                            .u("sub", dt.subtask_idx)
                            .emit();
                    }
                }
            }
        }
        log::warn(log_channel,
                  "job_id=" + std::to_string(job.id) + " awaiting_restart (attempt " +
                      std::to_string(job.restart_attempts + 1) + "/" +
                      std::to_string(effective_max_restarts(job.checkpoint)) +
                      ") drain_expected=" + std::to_string(job.restart_drain_expected.size()));
        // Bookkeeping: completed_count and errors may already include
        // entries for surviving-worker subtasks that finished before this
        // tick. Clear them - they belong to the previous attempt and
        // restart_job_locked_ will reset transient state anyway.
        job.completed_count = 0;
        job.errors.clear();
    } else {
        // Fail-fast: synthesise an error per pending task on the
        // lost worker and free its slot.
        for (const auto& [role, sub] : it->second) {
            job.errors.push_back(worker_id + "/" + role + "[" + std::to_string(sub) +
                                 "]: worker lost (heartbeat timeout)");
            ++job.completed_count;
        }
    }
    it->second = std::move(live);
}

void Coordinator::charge_slots_locked_(WorkerConnection& worker, std::uint32_t n) {
    worker.slots_in_use += n;
    if (!worker.slots_gauge_retired) {
        metrics::coordinator::slots_in_use_delta(static_cast<std::int64_t>(n));
    }
}

void Coordinator::release_slots_locked_(WorkerConnection& worker, std::uint32_t n) {
    const auto released = std::min(n, worker.slots_in_use);
    worker.slots_in_use -= released;
    if (!worker.slots_gauge_retired) {
        metrics::coordinator::slots_in_use_delta(-static_cast<std::int64_t>(released));
    }
}

void Coordinator::release_task_slot_locked_(const JobState& job,
                                            const std::string& worker_id,
                                            const std::string& key) {
    const auto w = registered_.find(worker_id);
    if (w == registered_.end()) {
        return;
    }
    if (const auto stamp = job.pending_session.find(key);
        stamp != job.pending_session.end() && stamp->second != w->second->session) {
        return;  // charged to a superseded session; its retirement accounts for it
    }
    release_slots_locked_(*w->second, 1);
}

void Coordinator::mark_worker_lost_locked_(WorkerConnection& worker) {
    worker.lost = true;
    lost_worker_ids_.push_back(worker.worker_id);
    // A session still retiring its predecessor did not hold the predecessor's
    // subtasks; the predecessor's frames and then its retirement account for
    // them.
    const std::uint64_t dead_from = worker.retiring_predecessor ? worker.session : 0;
    for (auto& [_, job] : jobs_) {
        fold_dead_subtasks_into_restart_locked_(*job,
                                                worker.worker_id,
                                                "coordinator.watchdog",
                                                "worker lost (heartbeat timeout)",
                                                /*live_from_session=*/0,
                                                dead_from);
    }
    if (worker.conn) {
        worker.conn->shutdown_read();
    }
    worker.slots_gauge_retired = true;
    metrics::coordinator::worker_lost(worker.slot_capacity, worker.slots_in_use);
    log::warn("coordinator.watchdog", "worker lost: " + worker.worker_id);
    events::publish("coordinator.worker_lost",
                    "{\"worker_id\":" + js_quote(worker.worker_id) + "}");
}

// A worker re-registered under an id that already had a live session. The previous
// SESSION has been fenced and drained, so anything it had in flight can never report.
//
// This now takes exactly the same path as a watchdog-declared loss. It used to
// handle only the case where a restart drain was ALREADY in progress and skip the
// job otherwise - so a worker that died and was restarted BEFORE the coordinator
// noticed left its subtasks with nobody to redeploy them: the watchdog never
// declared the worker lost (it is alive and heartbeating under the same id), no
// restart was ever triggered, and the job hung indefinitely with subtasks missing.
// Not exotic - restarting a crashed process quickly is what an orchestrator does by
// default. F64 / follow-up 46.
void Coordinator::retire_previous_session_subtasks_(const std::string& worker_id,
                                                    std::uint64_t successor_session) {
    bool touched = false;
    std::vector<std::pair<std::shared_ptr<network::Connection>, JobId>> survivor_cancels;
    {
        std::lock_guard lock(mu_);
        for (auto& [job_id, job] : jobs_) {
            // Only a job that still has a subtask of an earlier session in
            // flight here. One placed only on the successor (a submit, or a
            // restart that redeployed while the old session's frames drained)
            // lost nothing: folding it failed it, or restarted it on top of
            // the copy the successor is running.
            const auto pending = job->pending_per_worker.find(worker_id);
            if (pending == job->pending_per_worker.end() ||
                std::none_of(pending->second.begin(),
                             pending->second.end(),
                             [&](const std::pair<std::string, std::uint32_t>& p) {
                                 const auto stamp = job->pending_session.find(
                                     p.first + ":" + std::to_string(p.second));
                                 return stamp == job->pending_session.end() ||
                                        stamp->second < successor_session;
                             })) {
                continue;
            }
            fold_dead_subtasks_into_restart_locked_(
                *job,
                worker_id,
                "coordinator.register",
                "worker re-registered; the process holding this subtask is gone",
                successor_session);
            touched = true;

            // Starting a whole-job restart is only half of the worker-loss
            // choreography. Every still-running peer must also receive
            // CancelJob so that it reports SubtaskFinished and covers the
            // restart drain. The watchdog path already broadcasts this, but
            // a fast same-id replacement bypasses watchdog loss detection:
            // the replacement is healthy and heartbeating by the time the
            // next sweep runs. Without this register-path broadcast, an
            // unbounded survivor runs forever and the restart expires at its
            // drain deadline.
            //
            // The worker's own current session is a survivor too when the
            // job still has subtasks placed on it.
            const auto still_pending = job->pending_per_worker.find(worker_id);
            const bool successor_hosts_job =
                still_pending != job->pending_per_worker.end() && !still_pending->second.empty();
            for (const auto& [other_worker_id, _] : job->tasks_by_worker) {
                if (other_worker_id == worker_id && !successor_hosts_job) {
                    continue;
                }
                auto it = registered_.find(other_worker_id);
                if (it != registered_.end() && !it->second->lost && it->second->conn) {
                    survivor_cancels.emplace_back(it->second->conn, job_id);
                }
            }
        }
    }
    for (const auto& [conn, job_id] : survivor_cancels) {
        CancelJobMsg cancel;
        cancel.job_id = job_id;
        (void)send_frame(*conn, fenced_frame_(MessageKind::CancelJob, cancel));
    }
    // A fold may have emptied the drain, which is the condition that fires the
    // redeploy, and a fresh restart needs the watchdog to act on it. Either way,
    // nudge rather than wait a whole interval for a state change already made.
    if (touched) {
        cv_.notify_all();
    }
}

JobPlan Coordinator::replan_at_new_parallelism_(const JobState& job) const {
    if (job.graph_json.empty()) {
        throw std::runtime_error(
            "rescale: this job has no retained graph, so it cannot be replanned");
    }
    auto graph = JobGraphSpec::from_json(job.graph_json);
    for (const auto& [op_id, new_p] : job.pending_op_parallelism) {
        auto it = std::find_if(graph.ops.begin(), graph.ops.end(), [&](const OperatorSpec& o) {
            return o.id == op_id;
        });
        if (it == graph.ops.end()) {
            throw std::runtime_error("rescale: the job graph has no operator '" + op_id + "'");
        }
        it->parallelism = new_p;
        // Widen the declared bounds to admit the new value. The request was
        // validated against the bounds as SUBMITTED, and those bounds are what
        // makes the operator scalable at all; but JobGraphSpec::validate()
        // requires min <= parallelism <= max, and a scale to the boundary
        // leaves that intact while a bound of 0/0 on an operator being carried
        // along unchanged does not. Only the requested operator is touched.
        if (it->min_parallelism != 0 || it->max_parallelism != 0) {
            it->min_parallelism = std::min(it->min_parallelism, new_p);
            it->max_parallelism = std::max(it->max_parallelism, new_p);
        }
    }
    // Plan with the job's OWN registries when it has a bundle: a plugin job's
    // operator types live in the bundle's view, not the default instance, and
    // planning against the default instance would reject every one of them.
    if (job.bundle != nullptr) {
        return plan_job(graph, job.bundle->operator_registry(), job.bundle->runner_registry());
    }
    return plan_job(graph, OperatorRegistry::default_instance());
}

std::vector<Coordinator::PendingDeploy> Coordinator::restart_job_locked_(JobState& job) {
    // A fatal cause recorded during the drain means this restart cannot
    // succeed - the restore point itself is damaged - so redeploying would
    // only burn the budget re-hitting the same refusal (and each attempt's
    // errors.clear() would wipe the verdict). Fail the job NOW, carrying
    // the diagnosis. Checked here, at the single choke point every restart
    // path funnels through, rather than at each caller.
    if (!job.fatal_cause.empty()) {
        log::warn("coordinator.restart",
                  "job_id=" + std::to_string(job.id) +
                      " cancelling the pending restart: a draining subtask reported a FATAL "
                      "error, which a restart cannot fix: " +
                      job.fatal_cause);
        job.errors.push_back(job.fatal_cause);
        job.awaiting_restart = false;
        job.restart_deadline = {};
        job.restart_pending.clear();
        job.restart_drained_keys.clear();
        job.restart_drain_expected.clear();
        job.completed_count = job.expected_completion;
        signal_job_completion_locked_(job);
        return {};
    }
    // 1. Snapshot the task set we need to redeploy. Build a topology
    //    template (extra_config + original peer_refs) from task_records
    //    BEFORE clearing it. Without this, multi-stage jobs would lose
    //    cross-subtask wiring after restart.
    struct Template {
        DeploymentTask base;  // role/subtask_idx/extra_config/peer_refs preserved
    };
    std::unordered_map<std::string, Template> templates;  // key "role:idx"
    for (const auto& [key, worker_dt] : job.task_records) {
        Template t;
        t.base = worker_dt.second;
        // Drop the host/port from each peer - the new placement picks
        // them up dynamically via PeerUpdate. Keep role+subtask_idx so
        // the planner-equivalent step knows the graph topology.
        for (auto& p : t.base.peers) {
            p.host.clear();
            p.data_port = 0;
        }
        t.base.data_port = 0;
        templates[key] = std::move(t);
    }

    // Where each operator's subtasks lived BEFORE this redeploy. Captured
    // now, while task_op_identity still describes the deployed set, because
    // a replan below rebuilds it. The derivation (base = global index minus
    // index within operator, per-op contiguity only - which is what keeps
    // append-only hot-rescale layouts restorable) lives in
    // derive_op_index_blocks so it can be tested against exactly those
    // layouts without a cluster.
    auto old_op_blocks = derive_op_index_blocks(job.task_op_identity);

    // Replan rescale: the requested per-operator parallelism is applied to the
    // retained job graph and the whole task set is re-derived by the planner.
    // See JobState::pending_op_parallelism for why this replans rather than
    // resizing the deployed set.
    const bool is_replan_rescale = !job.pending_op_parallelism.empty();
    std::unordered_map<std::string, JobState::TaskOpIdentity> new_op_identity;
    std::unordered_map<std::string, std::uint32_t> new_op_parallelism;
    if (is_replan_rescale) {
        JobPlan new_plan;
        try {
            new_plan = replan_at_new_parallelism_(job);
        } catch (const std::exception& e) {
            // The job has already drained by the time this runs, so there is
            // nothing to fall back to: report the failure rather than leave it
            // half-deployed or silently stopped.
            job.errors.push_back(std::string{"rescale: replan failed: "} + e.what());
            job.awaiting_restart = false;
            job.restart_deadline = {};
            job.restart_pending.clear();
            job.restart_drained_keys.clear();
            job.restart_drain_expected.clear();
            job.pending_op_parallelism.clear();
            job.pre_rescale_op_parallelism.clear();
            log::warn("coordinator.rescale",
                      "job_id=" + std::to_string(job.id) + " replan failed: " + e.what());
            job.completed_count = job.expected_completion;
            signal_job_completion_locked_(job);
            return {};
        }
        templates.clear();
        for (const auto& t : new_plan.tasks) {
            const std::string key = t.role + ":" + std::to_string(t.subtask_idx);
            Template tmpl;
            tmpl.base.role = t.role;
            tmpl.base.subtask_idx = t.subtask_idx;
            tmpl.base.data_port = 0;
            tmpl.base.extra_config = t.extra_config;
            tmpl.base.key_group_first = t.key_group_first;
            tmpl.base.key_group_last = t.key_group_last;
            // Peer host/port are filled in by the placement step below and
            // then by PeerUpdate once each subtask reports its bound port.
            for (const auto& [pr_role, pr_sub] : t.peer_refs) {
                tmpl.base.peers.push_back(PeerAddress{.role = pr_role, .subtask_idx = pr_sub});
            }
            templates[key] = std::move(tmpl);
            if (!t.op_id.empty()) {
                new_op_identity[key] = JobState::TaskOpIdentity{
                    .op_id = t.op_id, .subtask_idx_in_op = t.subtask_idx_in_op};
                ++new_op_parallelism[t.op_id];
            }
        }
    }

    // Rescale path: when rescale_overrides is non-empty the caller
    // asked for a per-role parallelism change. Synthesize fresh
    // template entries for any new subtask indices (cloning from the
    // role's first existing template), and rewrite every task's peers
    // list so peers in rescaled roles fan out to the new subtask set.
    const bool is_rescale = !job.rescale_overrides.empty();
    if (is_rescale) {
        // 1a. Resize each rescaled role's template set to exactly the
        //     new parallelism. Scale-up clones from subtask_idx=0 (all
        //     siblings share extra_config / peer roles); scale-down
        //     drops the templates whose subtask_idx >= new_p so they
        //     don't get redeployed.
        for (const auto& [role, new_p] : job.rescale_overrides) {
            const std::string clone_key = role + ":0";
            auto base_it = templates.find(clone_key);
            if (base_it == templates.end()) {
                continue;
            }
            for (std::uint32_t i = 0; i < new_p; ++i) {
                const std::string key = role + ":" + std::to_string(i);
                if (templates.count(key) != 0) {
                    continue;
                }
                Template t = base_it->second;
                t.base.subtask_idx = i;
                templates[key] = std::move(t);
            }
            // Prune any pre-rescale templates whose subtask_idx is now
            // beyond the new parallelism. Without this, scale-down
            // would still attempt to deploy the retired indices.
            std::erase_if(templates, [&role, new_p](const auto& kv) {
                const auto colon = kv.first.find(':');
                if (colon == std::string::npos) {
                    return false;
                }
                if (kv.first.substr(0, colon) != role) {
                    return false;
                }
                const auto idx = static_cast<std::uint32_t>(std::stoul(kv.first.substr(colon + 1)));
                return idx >= new_p;
            });
        }
        // 1b. Rewrite peers in EVERY template so a peer reference to a
        //     rescaled role expands from one entry to N. The peer's
        //     subtask_idx as templated was the OLD parent index; we
        //     replace it with new_p entries spanning new_idx in [0, new_p).
        for (auto& [key, t] : templates) {
            std::vector<PeerAddress> expanded;
            expanded.reserve(t.base.peers.size());
            for (const auto& p : t.base.peers) {
                auto ov = job.rescale_overrides.find(p.role);
                if (ov == job.rescale_overrides.end()) {
                    expanded.push_back(p);
                    continue;
                }
                for (std::uint32_t i = 0; i < ov->second; ++i) {
                    PeerAddress np = p;
                    np.subtask_idx = i;
                    np.host.clear();
                    np.data_port = 0;
                    expanded.push_back(np);
                }
            }
            t.base.peers = std::move(expanded);
        }
    }

    std::vector<std::pair<std::string, std::uint32_t>> tasks_to_redeploy;
    if (is_rescale || is_replan_rescale) {
        // Drain expected covers everyone currently in flight; redeploy
        // the FULL new task set (including roles not being rescaled at
        // their current parallelism, plus the expanded set for rescaled
        // roles). Build by listing every templated key - that's the
        // post-rescale shape we want.
        for (const auto& [key, _] : templates) {
            const auto colon = key.find(':');
            const auto role = key.substr(0, colon);
            const auto sub = static_cast<std::uint32_t>(std::stoul(key.substr(colon + 1)));
            tasks_to_redeploy.emplace_back(role, sub);
        }
    } else {
        for (const auto& [role, sub] : job.restart_pending) {
            tasks_to_redeploy.emplace_back(role, sub);
        }
        for (const auto& key : job.restart_drain_expected) {
            const auto colon = key.find(':');
            if (colon == std::string::npos)
                continue;
            const auto role = key.substr(0, colon);
            const auto sub = static_cast<std::uint32_t>(std::stoul(key.substr(colon + 1)));
            tasks_to_redeploy.emplace_back(role, sub);
        }
    }

    // 2. Pick survivor workers (alive, with at least one slot free).
    std::vector<std::shared_ptr<WorkerConnection>> survivors;
    std::size_t total_free = 0;
    for (const auto& [worker_id, worker] : registered_) {
        if (worker->lost || !worker->conn)
            continue;
        if (worker->slots_in_use < worker->slot_capacity) {
            survivors.push_back(worker);
            total_free += (worker->slot_capacity - worker->slots_in_use);
        }
    }
    if (survivors.empty() || total_free < tasks_to_redeploy.size()) {
        // No room to restart RIGHT NOW. That is a transient condition, not
        // a verdict: the worker whose loss triggered this restart is
        // typically seconds from re-registering (a container restart, a
        // supervisor respawn), and the register path re-fires a pending
        // restart the moment capacity returns. The first cut synthesised
        // per-subtask errors and signalled completion here - permanent job
        // failure - which the broker-outage composite caught live: an
        // in-doubt walk finishing before the killed worker returned killed
        // the whole job. Rig topologies masked it only by slot headroom.
        // Wait under a deadline; fail with the original diagnosis only if
        // capacity genuinely never comes back.
        const auto now_tp = std::chrono::steady_clock::now();
        if (job.restart_capacity_deadline == std::chrono::steady_clock::time_point{}) {
            job.restart_capacity_deadline = now_tp + cfg_.restart_capacity_timeout;
            log::warn("coordinator.restart",
                      "job_id=" + std::to_string(job.id) + " restart waiting for capacity: " +
                          std::to_string(tasks_to_redeploy.size()) + " task(s) need slots, " +
                          std::to_string(total_free) +
                          " free; a worker re-registration re-fires this restart");
        }
        if (now_tp < job.restart_capacity_deadline) {
            // The drain/resolution deadline has served its purpose (the
            // drain is provably covered - this function only runs after
            // coverage); left set, the watchdog would fail the wait as a
            // phantom "survivors did not drain".
            job.restart_deadline = {};
            return {};  // awaiting_restart stays set; the next kick retries
        }
        log::warn("coordinator.restart",
                  "job_id=" + std::to_string(job.id) +
                      " capacity never returned within the restart's capacity deadline; "
                      "failing the job");
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        for (const auto& [role, sub] : tasks_to_redeploy) {
            job.errors.push_back("restart: no slot available for " + role + "[" +
                                 std::to_string(sub) + "]");
            SubtaskErrorRecord se;
            se.role = role;
            se.subtask_idx = sub;
            se.attempt = job.restart_attempts;
            se.ts_ms = now_ms;
            se.message = "restart aborted: no free slot to redeploy this subtask after worker loss";
            job.subtask_errors.push_back(std::move(se));
            ++job.completed_count;
        }
        job.awaiting_restart = false;
        job.restart_deadline = {};
        job.restart_capacity_deadline = {};
        job.restart_pending.clear();
        job.restart_drained_keys.clear();
        job.restart_drain_expected.clear();
        if (job.completed_count >= job.expected_completion) {
            signal_job_completion_locked_(job);
        }
        return {};
    }
    job.restart_capacity_deadline = {};

    // 3. Build new worker_id assignments round-robin across survivors.
    //    Reuse the existing DeploymentTask shape from task_records so
    //    extra_config / data_port=0 are preserved verbatim. peer
    //    addresses get re-resolved against the new placement.
    struct TaskKey {
        std::string role;
        std::uint32_t subtask_idx{};
        bool operator==(const TaskKey& o) const {
            return role == o.role && subtask_idx == o.subtask_idx;
        }
    };
    struct TaskKeyHash {
        std::size_t operator()(const TaskKey& k) const noexcept {
            return std::hash<std::string>{}(k.role) ^
                   (std::hash<std::uint32_t>{}(k.subtask_idx) << 1);
        }
    };
    std::unordered_map<TaskKey, std::pair<std::string, std::uint16_t>, TaskKeyHash> placement;
    std::size_t rr = 0;
    std::unordered_map<std::string, std::size_t> per_worker_remaining;
    for (const auto& worker : survivors) {
        per_worker_remaining[worker->worker_id] = worker->slot_capacity - worker->slots_in_use;
    }
    for (const auto& [role, sub] : tasks_to_redeploy) {
        // Skip duplicate task entries (restart_pending + restart_drain
        // might overlap if a lost subtask was already in tasks_by_worker
        // before - defensive).
        TaskKey k{role, sub};
        if (placement.count(k) != 0)
            continue;
        for (std::size_t i = 0; i < survivors.size(); ++i) {
            auto& s = survivors[(rr + i) % survivors.size()];
            if (per_worker_remaining[s->worker_id] > 0) {
                placement[k] = {s->worker_id, 0};
                --per_worker_remaining[s->worker_id];
                rr = (rr + i + 1) % survivors.size();
                break;
            }
        }
    }

    // 4. Reset transient JobState fields. expected_completion stays
    //    for plain restart (same task count); rescale resets it to the
    //    new total. attempt_counts retained - task-level retry budget
    //    is independent of worker-level restart budget.
    // A deliberate rescale (an operator, API or autoscaler request) is not a
    // failure and spends no restart budget; every other restart does.
    if (!is_rescale && !is_replan_rescale) {
        ++job.restart_attempts;
        ++job.restarts_total;
        job.last_restart_at = std::chrono::steady_clock::now();
    }
    ++job.redeploys;
    // The new run tracks only its own checkpoints' confirmations, as the
    // specification's Redeploy forgets every broadcast checkpoint. A
    // CommitConfirmed for the old run's checkpoint whose first hold comes
    // after this finds nothing to drain; left here, it drained the old set,
    // read the new redeploys, and advanced the new run's confirmed restore
    // point and traced a WriteConfirmed after the Redeploy. One whose first
    // hold came before this is caught by the redeploys check after its
    // marker write (handle_commit_confirmed_).
    job.pending_confirms.clear();
    job.awaiting_restart = false;
    job.in_doubt_walked = false;
    job.restart_deadline = {};
    job.restart_pending.clear();
    job.restart_drained_keys.clear();
    job.restart_drain_expected.clear();
    job.completed_count = 0;
    job.errors.clear();
    // A restart supersedes any outstanding error-cancel convergence: the
    // count it was waiting on restarts from zero with the new deployment,
    // and a stale deadline would force-fail the fresh attempt.
    job.error_cancel_broadcast = false;
    job.terminal_cancel_deadline = {};
    job.peer_updates_sent = false;
    job.received_listenings = 0;
    job.ports.clear();
    job.task_records.clear();
    job.tasks_by_worker.clear();
    job.pending_per_worker.clear();
    job.pending_session.clear();
    job.pending_checkpoint_acks.clear();
    job.rewind_floor_checkpoint_id = 0;
    // Clear the bounded-source final-checkpoint coordination so a replayed EOS
    // after this restart re-requests a FRESH final id seeded from the new task
    // set (the old pending set referenced pre-restart subtask keys). A request
    // still held for its id came from a source this restart has drained, and
    // goes unanswered.
    job.final_checkpoint_id.reset();
    job.sources_requested_final.clear();
    job.held_final_requests.clear();
    if (is_rescale || is_replan_rescale) {
        job.expected_completion = tasks_to_redeploy.size();
    }
    if (is_replan_rescale) {
        // The deployed set is about to be a different shape; the old identities
        // describe indices that no longer mean the same thing.
        job.task_op_identity = new_op_identity;
    }

    // 5. Build new DeploymentTasks with refreshed peer addresses.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_worker;
    job.expected_listenings = 0;
    for (const auto& [k, worker_port] : placement) {
        const std::string key = k.role + ":" + std::to_string(k.subtask_idx);
        DeploymentTask d;
        if (auto it = templates.find(key); it != templates.end()) {
            d = it->second.base;
        } else {
            d.role = k.role;
            d.subtask_idx = k.subtask_idx;
        }
        d.data_port = 0;                // generic subtasks pick at bind
        d.subtask_idx = k.subtask_idx;  // ensure cloned templates reflect new idx
        // Re-resolve each peer's host using the new placement. Port
        // stays 0; PeerUpdate fills it in once SubtaskListening
        // reports the bound port.
        for (auto& p : d.peers) {
            const std::string peer_key = p.role + ":" + std::to_string(p.subtask_idx);
            if (auto pit = placement.find(TaskKey{p.role, p.subtask_idx}); pit != placement.end()) {
                auto worker_it = registered_.find(pit->second.first);
                if (worker_it != registered_.end()) {
                    p.host = worker_it->second->data_host;
                }
            }
            p.data_port = 0;
        }
        // Rescale: tag this new subtask with the parent old subtask
        // whose state file it should restore from, and the key-group
        // range it's responsible for. Roles not being rescaled fall
        // through with kRestoreFromSelf + {0, 0} so the worker sees no
        // rescale directive and follows the historic same-idx path.
        //
        // Scale-up   (new_p > old_p): k = new_p/old_p new subtasks
        //   per parent; parent_idx = new_idx / k; parent_count = 1.
        // Scale-down (new_p < old_p): k_down = old_p/new_p parents
        //   per new subtask; parent_idx = new_idx * k_down;
        //   parent_count = k_down (contiguous range merged at the
        //   state backend factory).
        if (is_replan_rescale) {
            // EVERY task gets an explicit restore directive, not only those of
            // the operator being rescaled. The planner allocates global indices
            // as one contiguous block per operator in graph order, so changing
            // one operator's parallelism moves every later operator's block:
            // an untouched operator left on the default "restore from my own
            // index" would read a directory that now belongs to a different
            // operator. That is silent state corruption, the same shape as F38.
            //
            // So: find which operator this task belongs to, map its index
            // within that operator onto the parent index, and translate that
            // back through the operator's OLD block base.
            auto ident = new_op_identity.find(key);
            if (ident != new_op_identity.end() && !ident->second.op_id.empty()) {
                const auto& op_id = ident->second.op_id;
                const auto idx_in_op = ident->second.subtask_idx_in_op;
                auto old_block = old_op_blocks.find(op_id);
                auto new_p_it = new_op_parallelism.find(op_id);
                if (old_block != old_op_blocks.end() && old_block->second.consistent &&
                    old_block->second.parallelism != 0 && new_p_it != new_op_parallelism.end()) {
                    const auto mapping = rescale_parent_mapping(
                        old_block->second.parallelism, new_p_it->second, idx_in_op);
                    if (mapping.ok) {
                        d.restore_from_subtask_idx = old_block->second.base + mapping.parent_idx;
                        d.restore_from_parent_count = mapping.parent_count;
                        // In the operator's own indices, not the block's.
                        d.succeeds_first = mapping.succeeds_first;
                        d.succeeds_count = mapping.succeeds_count;
                    } else {
                        // Refused earlier by the request validation; reaching
                        // here means the two disagree. Restore nothing rather
                        // than restore the wrong slice.
                        log::warn("coordinator.rescale",
                                  "job_id=" + std::to_string(job.id) + " op_id=" + op_id +
                                      " subtask " + std::to_string(idx_in_op) +
                                      ": no parent mapping (" + mapping.error +
                                      "); this subtask starts with EMPTY state");
                    }
                }
                // No old block means the operator did not exist before this
                // deploy, so there is nothing of its to restore. The key-group
                // range from the planner still applies.
            }
        } else if (restore_needs_pre_rescale_layout(!job.stale_layout_blocks.empty(),
                                                    job.latest_completed_checkpoint_id,
                                                    job.stale_layout_through)) {
            // A plain restart whose restore point is still a PRE-rescale
            // checkpoint. Not a replan, so without this every task would take the
            // default "restore from my own index" - into a directory that belongs
            // to whichever operator held that index under the old layout. The
            // translation is the same one the replan does, against the layout
            // retained at that replan.
            auto ident = new_op_identity.find(key);
            if (ident == new_op_identity.end() || ident->second.op_id.empty()) {
                ident = job.task_op_identity.find(key);
            }
            if (ident != job.task_op_identity.end() && !ident->second.op_id.empty()) {
                const auto& op_id = ident->second.op_id;
                const auto idx_in_op = ident->second.subtask_idx_in_op;
                const auto stale = job.stale_layout_blocks.find(op_id);
                std::uint32_t new_p = 0;
                if (const auto it = new_op_parallelism.find(op_id);
                    it != new_op_parallelism.end()) {
                    new_p = it->second;
                } else {
                    // Not replanning, so the current deployed parallelism for this
                    // operator is what the tasks describe.
                    for (const auto& [other_key, other] : job.task_op_identity) {
                        if (other.op_id == op_id) {
                            new_p = std::max(new_p, other.subtask_idx_in_op + 1);
                        }
                    }
                }
                if (stale != job.stale_layout_blocks.end() && stale->second.parallelism != 0 &&
                    new_p != 0) {
                    const auto mapping =
                        rescale_parent_mapping(stale->second.parallelism, new_p, idx_in_op);
                    if (mapping.ok) {
                        d.restore_from_subtask_idx = stale->second.base + mapping.parent_idx;
                        d.restore_from_parent_count = mapping.parent_count;
                        d.succeeds_first = mapping.succeeds_first;
                        d.succeeds_count = mapping.succeeds_count;
                    } else {
                        log::warn("coordinator.restart",
                                  "job_id=" + std::to_string(job.id) + " op_id=" + op_id +
                                      " subtask " + std::to_string(idx_in_op) +
                                      ": restoring from a pre-rescale checkpoint but no parent "
                                      "mapping (" +
                                      mapping.error + "); this subtask starts with EMPTY state");
                    }
                }
            }
        } else if (is_rescale) {
            auto ov = job.rescale_overrides.find(k.role);
            auto old = job.pre_rescale_parallelism.find(k.role);
            if (ov != job.rescale_overrides.end() && old != job.pre_rescale_parallelism.end() &&
                old->second != 0) {
                const std::uint32_t new_p = ov->second;
                const std::uint32_t old_p = old->second;
                if (new_p >= old_p) {
                    const std::uint32_t k_factor = new_p / old_p;
                    if (k_factor != 0) {
                        d.restore_from_subtask_idx = k.subtask_idx / k_factor;
                        d.restore_from_parent_count = 1;
                    }
                } else {
                    const std::uint32_t k_down = old_p / new_p;
                    d.restore_from_subtask_idx = k.subtask_idx * k_down;
                    d.restore_from_parent_count = k_down;
                }
                // A role-level rescale renumbers a single operator, so the role
                // index is the operator's own.
                if (const auto succession = rescale_parent_mapping(old_p, new_p, k.subtask_idx);
                    succession.ok) {
                    d.succeeds_first = succession.succeeds_first;
                    d.succeeds_count = succession.succeeds_count;
                }
                const auto range = key_group_range_for_subtask(k.subtask_idx, new_p);
                d.key_group_first = range.first;
                d.key_group_last = range.second;
            }
        }
        by_worker[worker_port.first].push_back(std::move(d));
        ++job.expected_listenings;
    }

    // 6. Stash the new shape into job state for the rest of the coordinator
    //    to address: SubtaskListening etc. Stamped with the session the
    //    Deploy below goes to, resolved in this same hold.
    for (auto& [worker_id, tasks] : by_worker) {
        const auto worker_it = registered_.find(worker_id);
        const std::uint64_t session =
            worker_it == registered_.end() ? 0 : worker_it->second->session;
        for (const auto& t : tasks) {
            const std::string key = t.role + ":" + std::to_string(t.subtask_idx);
            job.task_records[key] = {worker_id, t};
            job.pending_per_worker[worker_id].emplace_back(t.role, t.subtask_idx);
            job.pending_session[key] = session;
        }
        job.tasks_by_worker[worker_id] = tasks;
    }

    // A rescale shifts key-group ownership; bump topology_version so
    // Queryable State route caches can invalidate. Restart-from-loss
    // also bumps - the placement might land on different workers.
    ++job.topology_version;

    // The STATE generation bumps only for a rescale, because only a rescale moves
    // the index -> operator mapping that state directories are addressed by. A plain
    // restart redeploys the same shape, so its state is still where it was - using
    // topology_version here instead sent a restarted job looking under a generation
    // nothing had written, and it replayed from offset zero.
    if (is_rescale || is_replan_rescale) {
        job.state_generation_after_checkpoint = job.latest_completed_checkpoint_id;
        ++job.state_generation;
        // The new generation exists but has written nothing yet. Until its first
        // checkpoint completes the job's restore point still names the PREVIOUS
        // generation - the window F65 corrupted. A kill armed here reproduces it
        // deliberately instead of waiting for a sweep to land in it.
        CLINK_FAULT_POINT(clink::fault::points::kRescaleBeforeFirstCheckpoint);
    }

    log::info(is_rescale ? "coordinator.rescale" : "coordinator.restart",
              "job_id=" + std::to_string(job.id) +
                  " attempt=" + std::to_string(job.restart_attempts) +
                  " survivors=" + std::to_string(survivors.size()) +
                  " tasks=" + std::to_string(tasks_to_redeploy.size()));
    // Lifecycle span for OTLP export: rescale request to rescaled-deploys-
    // emitted, for both drain-based flavours (the hot cutover records its
    // own at completion). Recorded before the overrides are cleared so the
    // role count is still known.
    if ((is_rescale || is_replan_rescale) && job.rescale_span_start_unix_nano != 0 &&
        clink::metrics::SpanBuffer::global().enabled()) {
        clink::metrics::OtlpSpan span;
        span.name = "clink.rescale";
        span.start_unix_nano = job.rescale_span_start_unix_nano;
        span.end_unix_nano = clink::metrics::otlp_now_unix_nano();
        span.attributes = {{"clink.job_id", std::to_string(job.id)},
                           {"clink.mode", is_rescale ? "drain" : "replan"},
                           {"clink.roles",
                            std::to_string(is_rescale ? job.rescale_overrides.size()
                                                      : job.pending_op_parallelism.size())},
                           {"clink.tasks", std::to_string(tasks_to_redeploy.size())}};
        clink::metrics::SpanBuffer::global().record(std::move(span));
    }
    job.rescale_span_start_unix_nano = 0;
    if (is_rescale) {
        job.rescale_overrides.clear();
        job.pre_rescale_parallelism.clear();
    }
    if (is_replan_rescale) {
        // The rescale has landed: the new task set is built and about to
        // deploy. Close the state machine so operator_rescale_status stops
        // reporting Preparing for something that has finished.
        if (job.rescale_coordinator) {
            for (const auto& [op_id, new_p] : job.pending_op_parallelism) {
                job.rescale_coordinator->mark_replan_complete(op_id, new_p);
            }
        }
        std::string changed;
        for (const auto& [op_id, new_p] : job.pending_op_parallelism) {
            const auto old_it = job.pre_rescale_op_parallelism.find(op_id);
            changed +=
                (changed.empty() ? "" : ",") + op_id + "=" +
                (old_it != job.pre_rescale_op_parallelism.end() ? std::to_string(old_it->second)
                                                                : std::string{"?"}) +
                "->" + std::to_string(new_p);
        }
        // The replan has produced the new task set but nothing is deployed yet: the
        // old topology is gone and the new one has not arrived. That is the window
        // F63 lives in.
        CLINK_FAULT_POINT(clink::fault::points::kRescaleAfterReplan);
        log::info(
            "coordinator.rescale",
            "replanned job_id=" + std::to_string(job.id) + " " + changed +
                " tasks=" + std::to_string(tasks_to_redeploy.size()) +
                " restore_from_checkpoint=" + std::to_string(job.latest_completed_checkpoint_id) +
                " topology_version=" + std::to_string(job.topology_version));
        // Retain the layout that produced the checkpoint we are restoring from.
        // The restore point is still a PRE-rescale checkpoint until one completes
        // under the new topology, and a restart before that is not a replan - it
        // would fall back to "restore from my own index" and read a directory that
        // belongs to a different operator. See JobState::stale_layout_blocks.
        job.stale_layout_blocks.clear();
        for (const auto& [op_id, block] : old_op_blocks) {
            if (block.consistent && block.base_set && block.parallelism != 0) {
                job.stale_layout_blocks[op_id] =
                    JobState::StaleBlock{.base = block.base, .parallelism = block.parallelism};
            }
        }
        job.stale_layout_through = job.latest_completed_checkpoint_id;
        job.pending_op_parallelism.clear();
        job.pre_rescale_op_parallelism.clear();
    }

    // 7. Build Deploy frames + claim slots; return for caller to send.
    //
    // Restart point: the job's OWN newest usable checkpoint - confirmed
    // for tracked jobs (commit-confirmed restore protocol: a
    // completed-but-unconfirmed checkpoint may hold a broker transaction
    // that died with the worker), completed otherwise.
    //
    // When the job has NO usable checkpoint of its own, fall back to the
    // restore point it was SUBMITTED with, exactly as the initial deploy
    // applied it. The restart used to rebuild the restore purely from
    // the job's own progress, so a restart that fired before the first
    // checkpoint completed silently DROPPED a submitted savepoint /
    // restore-from and redeployed with empty state. Observed live: a
    // restore whose integrity check correctly REFUSED a truncated
    // checkpoint raced a peer's restartable cancel, the restart
    // re-deployed without the restore, and the refusal was laundered
    // into a clean empty run.
    //
    // The restore point is a property of the restart, not of a deploy
    // frame, so it is decided once here and every frame below carries the
    // same one.
    const auto own_restore_id = job.confirm_task_keys.empty() ? job.latest_completed_checkpoint_id
                                                              : job.latest_confirmed_checkpoint_id;
    const bool resubmit_original_restore =
        own_restore_id == 0 && !job.checkpoint.restore_from_dir.empty();
    const std::string restore_from_dir =
        resubmit_original_restore ? job.checkpoint.restore_from_dir : job.checkpoint.checkpoint_dir;
    const std::uint64_t restore_from_checkpoint_id =
        resubmit_original_restore ? job.checkpoint.restore_from_checkpoint_id : own_restore_id;
    log::info("coordinator.restart",
              "job_id=" + std::to_string(job.id) + " restore point: checkpoint " +
                  std::to_string(restore_from_checkpoint_id) +
                  (resubmit_original_restore ? " (the SUBMITTED restore point; no own "
                                               "checkpoint is usable yet)"
                                             : "") +
                  " (latest_completed=" + std::to_string(job.latest_completed_checkpoint_id) +
                  " latest_confirmed=" + std::to_string(job.latest_confirmed_checkpoint_id) +
                  " tracked=" + std::to_string(job.confirm_task_keys.size()) + ")");
    // A restart redeploys the whole task set, a plain restart and both
    // rescale flavours alike, so the Row sidecar layout is decided again from
    // the workers this placement uses: a worker that joined or left since the
    // last deploy changes the answer, in either direction.
    std::string row_layout_note;
    {
        const auto admission = admission_on(registered_, by_worker);
        job.row_layout = admission.layout;
        row_layout_note = admission.describe();
    }
    std::size_t stamped_ops = 0;
    std::vector<PendingDeploy> out;
    for (auto& [worker_id, tasks] : by_worker) {
        auto worker_it = registered_.find(worker_id);
        if (worker_it == registered_.end() || worker_it->second->lost || !worker_it->second->conn)
            continue;
        charge_slots_locked_(*worker_it->second, static_cast<std::uint32_t>(tasks.size()));
        DeployMsg deploy_msg;
        deploy_msg.job_id = job.id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = plugins_for_worker_locked_(job, *worker_it->second);
        deploy_msg.checkpoint_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = job.checkpoint.state_backend_uri;
        deploy_msg.capture_dir = job.checkpoint.capture_dir;
        deploy_msg.capture_records = job.checkpoint.capture_records;
        deploy_msg.restore_from_dir = restore_from_dir;
        deploy_msg.restore_from_checkpoint_id = restore_from_checkpoint_id;
        // Which generation this deploy WRITES, and which one produced the restore
        // point. They differ exactly when the restore crosses a rescale, and the
        // boundary is the same one that decides whether the parent INDEX needs
        // translating - so the two answers are taken from one predicate and cannot
        // disagree. A restore that reads generation N-1's directories while
        // translating indices as though it were generation N is precisely the
        // mismatch F63 and F65 came from. The submitted-restore fallback reads
        // an EXTERNAL directory, so its generation is whatever that directory
        // holds - the same discovery the initial deploy performs.
        deploy_msg.generation = job.state_generation;
        deploy_msg.restore_generation =
            resubmit_original_restore
                ? highest_generation_in(job.checkpoint.restore_from_dir)
                : ((job.state_generation > 1 && job.latest_completed_checkpoint_id != 0 &&
                    job.latest_completed_checkpoint_id <= job.state_generation_after_checkpoint)
                       ? job.state_generation - 1
                       : job.state_generation);
        deploy_msg.unaligned_checkpoints =
            job.checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.adaptive_barrier_mode =
            job.checkpoint.alignment == CheckpointAlignment::Adaptive;
        deploy_msg.expected_state_versions_packed = job.expected_state_versions_packed;
        deploy_msg.udfs_packed = job.udfs_packed;
        stamped_ops += stamp_row_layout(deploy_msg.tasks, job.row_layout);
        out.push_back({worker_it->second->conn, fenced_frame_(MessageKind::Deploy, deploy_msg)});
    }
    log::info(is_rescale || is_replan_rescale ? "coordinator.rescale" : "coordinator.restart",
              "job_id=" + std::to_string(job.id) + " " + row_layout_note +
                  " stamped_ops=" + std::to_string(stamped_ops));
    // One restart, one Redeploy event, whatever the number of deploy frames.
    // The specification takes the step once (deploying -> running), so an
    // event per frame read as a second redeploy from the running state and
    // the recorded traces of every multi-worker restart diverged at it.
    // Emitted only when a frame was built, as before: a restart that found
    // no worker to deploy onto has not redeployed.
    if (protocol_trace::enabled() && !out.empty()) {
        protocol_trace::Event("Redeploy")
            .u("job", job.id)
            .u("restore", restore_from_checkpoint_id)
            .u("next", job.next_checkpoint_id)
            .emit();
    }
    return out;
}

void Coordinator::dispatch_begin_rescale_locked_(JobState& job,
                                                 const std::string& op_id,
                                                 std::uint64_t cutover_checkpoint,
                                                 std::uint32_t target_parallelism,
                                                 std::vector<PendingDeploy>& out) {
    // Find every worker that hosts at least one subtask of this operator
    // and send it a BeginRescale. The worker's drain dispatcher (29d-2)
    // looks up its registered drain callbacks by (job_id, op_id) and
    // fires them; each callback sets drain_target on the source-runner
    // side (29d-3) so the running subtask emits its DrainMarker and
    // shuts down via SubtaskFinished.
    std::unordered_set<std::string> workers_with_op;
    for (const auto& [worker_id, tasks] : job.tasks_by_worker) {
        for (const auto& t : tasks) {
            if (task_hosts_op(job.task_op_identity, t.role, t.subtask_idx, op_id)) {
                workers_with_op.insert(worker_id);
                break;
            }
        }
    }
    BeginRescaleMsg msg;
    msg.job_id = job.id;
    msg.op_id = op_id;
    msg.target_parallelism = target_parallelism;
    msg.cutover_checkpoint = cutover_checkpoint;
    const auto frame = fenced_frame_(MessageKind::BeginRescale, msg);
    std::size_t dispatched = 0;
    for (const auto& worker_id : workers_with_op) {
        auto worker_it = registered_.find(worker_id);
        if (worker_it == registered_.end() || worker_it->second->lost || !worker_it->second->conn) {
            continue;
        }
        out.push_back({worker_it->second->conn, frame});
        ++dispatched;
    }
    log::info("coordinator.rescale",
              "begin rescale dispatched job_id=" + std::to_string(job.id) + " op_id=" + op_id +
                  " target=" + std::to_string(target_parallelism) + " cutover_checkpoint=" +
                  std::to_string(cutover_checkpoint) + " workers=" + std::to_string(dispatched) +
                  "/" + std::to_string(workers_with_op.size()));
}

void Coordinator::dispatch_cutover_deploy_locked_(JobState& job,
                                                  const std::string& op_id,
                                                  std::vector<PendingDeploy>& out) {
    if (!job.rescale_coordinator) {
        return;
    }
    // A job draining for a replan redeploys EVERYTHING via
    // restart_job_locked_; a cutover deployment on top of that is a second
    // topology. The checkpoint-completed hook already refuses to advance
    // Preparing ops during the drain; this guards any other route into
    // CuttingOver while a replan owns the job.
    if (job.awaiting_restart) {
        log::warn("coordinator.rescale",
                  "declining cutover deploy for op '" + op_id + "' of job " +
                      std::to_string(job.id) + ": a replan drain owns this job");
        return;
    }
    auto status = job.rescale_coordinator->status(op_id);
    if (!status.has_value() || status->state != RescaleState::CuttingOver) {
        return;
    }

    // Snapshot the old subtasks of this operator from task_records:
    // we need their keys for teardown and one of them as the
    // DeploymentTask template (peers, extra_config, role).
    std::vector<std::string> old_keys;
    const DeploymentTask* templ = nullptr;
    for (const auto& [key, val] : job.task_records) {
        if (task_hosts_op(job.task_op_identity, val.second.role, val.second.subtask_idx, op_id)) {
            old_keys.push_back(key);
            if (templ == nullptr) {
                templ = &val.second;
            }
        }
    }
    if (templ == nullptr) {
        log::warn("coordinator.rescale",
                  "cutover deploy for op '" + op_id + "' has no template task (drain race?)");
        job.rescale_coordinator->abort(op_id, "no template task");
        return;
    }

    // Free-slot snapshot. Skip lost workers and any without an open conn, and,
    // while the job runs the Row sidecar's second layout, any worker that
    // cannot read it: the running subtasks keep that layout, and the new ones
    // join them on it.
    std::vector<std::pair<std::string, std::uint32_t>> worker_free_slots;
    for (const auto& [worker_id, worker] : registered_) {
        if (worker->lost || !worker->conn) {
            continue;
        }
        if (job.row_layout == kRowLayoutV2 &&
            effective_protocol_version(worker->protocol_version) < kRowLayoutV2ProtocolVersion) {
            continue;
        }
        const std::uint32_t free_slots = worker->slot_capacity > worker->slots_in_use
                                             ? (worker->slot_capacity - worker->slots_in_use)
                                             : 0;
        // Old subtasks of THIS op count as in-use right now but are
        // about to be torn down below, so they're effectively
        // available for the new placement. Add them back to the
        // snapshot the planner sees: exactly the ones whose slots the
        // teardown frees, which is those charged to this worker's current
        // session (release_task_slot_locked_). One stamped with a superseded
        // session holds a slot of that session, not of this one, and
        // counting it let the planner put more on the successor than it has.
        std::uint32_t old_on_this_worker = 0;
        auto it = job.tasks_by_worker.find(worker_id);
        if (it != job.tasks_by_worker.end()) {
            for (const auto& t : it->second) {
                if (t.role != op_id) {
                    continue;
                }
                const auto stamp =
                    job.pending_session.find(t.role + ":" + std::to_string(t.subtask_idx));
                if (stamp == job.pending_session.end() || stamp->second == worker->session) {
                    ++old_on_this_worker;
                }
            }
        }
        worker_free_slots.emplace_back(worker_id, free_slots + old_on_this_worker);
    }

    DeploymentTask cloned_template = *templ;
    auto plan = plan_operator_cutover(op_id,
                                      status->current_parallelism,
                                      status->target_parallelism,
                                      status->cutover_checkpoint,
                                      job.checkpoint.checkpoint_dir,
                                      cloned_template,
                                      old_keys,
                                      std::move(worker_free_slots));
    if (!plan.ok) {
        log::warn("coordinator.rescale",
                  "cutover deploy planning failed for op '" + op_id + "': " + plan.error);
        job.rescale_coordinator->abort(op_id, plan.error);
        return;
    }

    // Tear down the old subtasks' bookkeeping. Their SubtaskFinished
    // arrivals already counted as drained acks via 29d-3's
    // mark_old_drained wiring; remove their task_records / tasks_by_worker
    // entries and free the worker slots. Don't increment completed_count
    // for drained subtasks - they're being replaced, not finished.
    for (const auto& key : plan.teardown_keys) {
        auto rec_it = job.task_records.find(key);
        if (rec_it == job.task_records.end()) {
            continue;
        }
        const auto worker_id = rec_it->second.first;
        release_task_slot_locked_(job, worker_id, key);
        auto tbt_it = job.tasks_by_worker.find(worker_id);
        if (tbt_it != job.tasks_by_worker.end()) {
            // Match the record's own (role, subtask_idx), not role == op_id:
            // planner tasks all share the generic role, so the op_id
            // comparison erased nothing and left stale entries for
            // BeginRescale to keep addressing.
            std::erase_if(tbt_it->second, [&](const DeploymentTask& t) {
                return t.role == rec_it->second.second.role &&
                       rec_it->second.second.subtask_idx == t.subtask_idx;
            });
            if (tbt_it->second.empty()) {
                job.tasks_by_worker.erase(tbt_it);
            }
        }
        job.task_records.erase(rec_it);
        // The identity record travels with the task record: a torn-down
        // subtask's key must not translate to the operator any more.
        job.task_op_identity.erase(key);
        // The drained subtask's SubtaskFinished arrived with
        // had_error=false, so expected_completion has effectively
        // already been satisfied for it. Counterbalance by REDUCING
        // expected_completion so the new subtask we're about to add
        // (which bumps expected_completion +1) results in the right
        // total. Without this the rescale would never satisfy
        // expected_completion and the job would appear stuck.
        if (job.expected_completion > 0) {
            --job.expected_completion;
        }
    }

    // Stash the new subtasks into JobState + claim worker slots, then
    // build per-worker Deploy frames.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_worker;
    std::uint32_t idx_in_op = 0;
    for (auto& [worker_id, task] : plan.new_tasks) {
        const std::string key = task.role + ":" + std::to_string(task.subtask_idx);
        job.task_records[key] = {worker_id, task};
        // Identity for the new subtasks, so their SubtaskListening acks
        // translate back to this operator (mark_new_ready) and a later
        // request can address them. plan_operator_cutover emits new_tasks
        // in operator order, so the position IS the index within the op.
        job.task_op_identity[key] =
            TaskOpIdentity{.op_id = op_id, .subtask_idx_in_op = idx_in_op++};
        job.pending_per_worker[worker_id].emplace_back(task.role, task.subtask_idx);
        // The session the Deploy below goes to, resolved in this same hold.
        const auto worker_it = registered_.find(worker_id);
        job.pending_session[key] = worker_it == registered_.end() ? 0 : worker_it->second->session;
        job.tasks_by_worker[worker_id].push_back(task);
        by_worker[worker_id].push_back(std::move(task));
        ++job.expected_completion;
        ++job.expected_listenings;
    }

    // A rescale shifts key-group ownership; bump topology_version so
    // Queryable State route caches can invalidate.
    ++job.topology_version;
    clink::metrics::orch::rescale_cutover_deploy();

    log::info("coordinator.rescale",
              "cutover deploy job_id=" + std::to_string(job.id) + " op_id=" + op_id +
                  " new_parallelism=" + std::to_string(status->target_parallelism) +
                  " cutover_checkpoint=" + std::to_string(status->cutover_checkpoint));

    for (auto& [worker_id, tasks] : by_worker) {
        auto worker_it = registered_.find(worker_id);
        if (worker_it == registered_.end() || worker_it->second->lost || !worker_it->second->conn) {
            continue;
        }
        charge_slots_locked_(*worker_it->second, static_cast<std::uint32_t>(tasks.size()));
        DeployMsg deploy_msg;
        deploy_msg.job_id = job.id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = plugins_for_worker_locked_(job, *worker_it->second);
        deploy_msg.checkpoint_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = job.checkpoint.state_backend_uri;
        deploy_msg.capture_dir = job.checkpoint.capture_dir;
        deploy_msg.capture_records = job.checkpoint.capture_records;
        // The new subtasks restore from the cutover checkpoint, NOT
        // the latest. This is the key difference from restart_job_locked_:
        // we use the coordinator-chosen cutover_checkpoint so all new
        // subtasks load a consistent snapshot frozen at the drain barrier.
        deploy_msg.restore_from_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.restore_from_checkpoint_id = status->cutover_checkpoint;
        deploy_msg.unaligned_checkpoints =
            job.checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.adaptive_barrier_mode =
            job.checkpoint.alignment == CheckpointAlignment::Adaptive;
        deploy_msg.expected_state_versions_packed = job.expected_state_versions_packed;
        deploy_msg.udfs_packed = job.udfs_packed;
        // A partial deploy: the job's current layout, never a recomputed one.
        stamp_row_layout(deploy_msg.tasks, job.row_layout);
        out.push_back({worker_it->second->conn, fenced_frame_(MessageKind::Deploy, deploy_msg)});
    }
}

void Coordinator::handle_subtask_finished_(MessageReader& r, WorkerConnection& sender) {
    auto msg = decode_subtask_finished(r);
    // The slot is the sending session's. A superseded session's frames are
    // handled after its successor took the worker id, and releasing theirs
    // against registered_[worker_id] took slots off the successor, which may
    // already hold new placements: the worker was then over-placed. mu_ held.
    const auto release_slot_locked = [this, &sender] { release_slots_locked_(sender, 1); };

    bool retry = false;
    std::shared_ptr<WorkerConnection> target_conn;
    // The retry's socket, copied in the hold that picks the target: the
    // reaper resets WorkerConnection::conn under mu_, so it is never read
    // after the hold.
    std::shared_ptr<network::Connection> retry_conn;
    DeploymentTask retry_task;
    JobId job_id = msg.job_id;
    std::shared_ptr<JobState> job_to_signal;
    std::vector<PendingDeploy> restart_deploys;
    // CancelJob frames to drain still-running subtasks when a subtask error
    // triggers a whole-job restart of a checkpointed job; sent outside mu_.
    std::vector<std::pair<std::shared_ptr<network::Connection>, JobId>> error_restart_cancels;
    // Decoded on the sender's dispatch thread, which checked the session was
    // not lost before dispatching, and not yet under mu_: the watchdog can
    // declare the session lost in between.
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeSubtaskFinished);
    {
        std::lock_guard lock(mu_);
        // A lost session's finish is dropped here, in the hold that would
        // count it, and not only on the dispatch thread: the loss fold has
        // already completed, failed or queued for restart every subtask the
        // session held, and handling the finish after it counted the subtask
        // a second time (a job completing with a subtask still running
        // elsewhere), or reported a drain after the loss that killed it.
        if (sender.lost) {
            return;
        }
        auto job_it = jobs_.find(job_id);
        if (job_it == jobs_.end()) {
            return;
        }
        auto& job = *job_it->second;
        const std::string key = msg.role + ":" + std::to_string(msg.subtask_idx);
        if (auto tit = job.subtask_timing.find(key); tit != job.subtask_timing.end()) {
            tit->second.finished_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count();
        }

        // If the operator this subtask belonged to is in
        // the Draining state, count this SubtaskFinished as a drained
        // ack. The coordinator transitions Draining -> CuttingOver
        // when every old subtask has drained; the deploy step then
        // brings up the new subtasks. Failed (had_error=true)
        // shutdowns also count - the operator's old subtasks are
        // going away one way or another.
        //
        // If the mark_old_drained transition lands the
        // operator in CuttingOver, fire the cutover-deployment slice
        // here so the new subtasks come up without a separate trigger.
        // Drained subtasks must NOT flow through the regular
        // completed_count / slot-free accounting below: the cutover
        // deploy already adjusted expected_completion and removed the
        // task_records entry; double-counting would falsely signal
        // job completion.
        bool was_drain = false;
        if (job.rescale_coordinator) {
            // The ack names the shared generic role; the state machine is
            // keyed by operator. Translate before consulting it (F40).
            const auto ack = op_scoped_ack(job.task_op_identity, msg.role, msg.subtask_idx);
            if (auto st = job.rescale_coordinator->status(ack.op_id);
                st.has_value() && st->state == RescaleState::Draining) {
                // An old subtask has finished draining. Between the last of these
                // and the redeploy the job has no running topology.
                CLINK_FAULT_POINT(clink::fault::points::kRescaleAfterDrain);
                job.rescale_coordinator->mark_old_drained(ack.op_id, ack.subtask_idx_in_op);
                was_drain = true;
                if (auto post = job.rescale_coordinator->status(ack.op_id);
                    post.has_value() && post->state == RescaleState::CuttingOver) {
                    if (job.hot_cutover.has_value() && job.hot_cutover->op_id == ack.op_id) {
                        hot_cutover_begin_rebind_locked_(job, restart_deploys);
                    } else {
                        dispatch_cutover_deploy_locked_(job, ack.op_id, restart_deploys);
                    }
                }
            } else if (st.has_value() && st->state == RescaleState::Preparing && !msg.had_error &&
                       job.hot_cutover.has_value() && job.hot_cutover->op_id == ack.op_id &&
                       job.hot_cutover->phase == JobState::HotCutover::Phase::AwaitingCut) {
                // An old subtask of the hot operator ended at C before C's own
                // completion was processed: the old subtasks stop the moment
                // they forward the barrier, while C completes only on the last
                // participant's ack, the sink's. The exit IS the drained ack;
                // the state machine cannot count it until C completes, so hold
                // it there and let the completion replay it. Falling through to
                // the completion accounting below is what lost it before: the
                // drain tally never reached the old parallelism and the cut sat
                // at its phase deadline. A failed exit is not a drain; it takes
                // the error path below as before. The exit is a fact for the
                // worker's bookkeeping now: it leaves pending_per_worker, so a
                // replan staged before C completes drains only what still runs
                // (the abort releases its slot; the rebind teardown does when
                // the cut completes).
                job.hot_cutover->early_drains.push_back({.subtask_idx_in_op = ack.subtask_idx_in_op,
                                                         .worker_id = msg.worker_id,
                                                         .key = key});
                if (auto pending_it = job.pending_per_worker.find(msg.worker_id);
                    pending_it != job.pending_per_worker.end()) {
                    std::erase_if(pending_it->second, [&](const auto& p) {
                        return p.first == msg.role && p.second == msg.subtask_idx;
                    });
                }
                was_drain = true;
            }
        }

        // Per-subtask retry is the best-effort path for NON-checkpointed jobs
        // only. A checkpointed job recovers consistently by rolling the WHOLE
        // job back to its last checkpoint + replaying (the branch below), so a
        // per-subtask redeploy (which leaves the other subtasks un-rolled-back)
        // would break exactly-once - skip it when there is a checkpoint dir.
        //
        // Two exclusions, both found by driving a plugin job through this path:
        // - !msg.fatal: a FATAL error (damaged restore point, plugin gate
        //   refusal) is deterministic for the artefacts involved; retrying it
        //   converts one loud failure into max_restarts identical ones.
        // - role tasks only, never kGenericSubtaskRole: a chained generic
        //   subtask cannot be retried piecemeal - its peers keep running or
        //   complete, so a lone redeploy waits on input edges that will never
        //   reopen. (It also never worked: the clink_attempt marker appended
        //   below corrupted the chain-spec JSON in extra_config, so every
        //   generic retry died parsing its own chain and the retry loop only
        //   masked the original error. Role handlers own their extra_config
        //   format and parse the marker as a line, which is where it stays.)
        if (msg.had_error && !msg.fatal && cfg_.max_restarts > 0 &&
            job.checkpoint.checkpoint_dir.empty()) {
            auto rec_it = job.task_records.find(key);
            // The retry goes to the worker's current session, and only while
            // that session is live. A superseded session's error can be
            // handled after its successor was declared lost: the successor
            // still holds the worker's registration while it retires its
            // predecessor, but its loss fold has run, and its retirement folds
            // only what sessions before it held. A retry moved onto it was
            // stamped with a session nothing would fold again, and the job
            // never completed. With no live session the error completes the
            // subtask, as it does for any subtask on a lost worker.
            const auto worker_it = rec_it == job.task_records.end()
                                       ? registered_.end()
                                       : registered_.find(rec_it->second.first);
            const bool live_target = worker_it != registered_.end() && !worker_it->second->lost;
            if (rec_it != job.task_records.end() &&
                rec_it->second.second.role != kGenericSubtaskRole && live_target) {
                const int attempts = ++job.attempt_counts[key];
                if (attempts <= cfg_.max_restarts) {
                    retry = true;
                    retry_task = rec_it->second.second;
                    if (!retry_task.extra_config.empty() &&
                        retry_task.extra_config.back() != '\n') {
                        retry_task.extra_config += '\n';
                    }
                    retry_task.extra_config += "clink_attempt=" + std::to_string(attempts);
                    // Counted here, where the retry is DECIDED, not at the
                    // send below: a send that fails is still a retry the
                    // operator needs to see.
                    metrics::orch::subtask_redeployed();
                    target_conn = worker_it->second;
                    retry_conn = target_conn->conn;
                    // When the current session is not the one reporting the
                    // error (a superseded one's frame, handled after its
                    // successor registered), the subtask moves there: its
                    // stamp, so the old session's retirement does not fold it
                    // as lost while it runs again, and its slot, so its finish
                    // on the successor frees a slot the successor holds. The
                    // successor may already be full; the retry goes ahead
                    // regardless, as it does on a session that was never
                    // replaced, and every free-slot sum treats a session
                    // charged past its capacity as having none.
                    if (target_conn.get() != &sender) {
                        job.pending_session[key] = target_conn->session;
                        release_slots_locked_(sender, 1);
                        charge_slots_locked_(*target_conn, 1);
                    }
                }
            }
        }

        // Pending restart drain: when a worker was lost and the job is
        // awaiting_restart, surviving subtasks are being cancelled. As
        // their SubtaskFinished arrivals come in here we DO NOT count
        // them toward completion or errors - they're being retired
        // ahead of the redeploy. Free their slots and record the drain.
        if (was_drain) {
            // Drained subtasks have already been torn down
            // by dispatch_cutover_deploy_locked_ above. Skip the
            // completed_count / slot-free / signal_job_completion path
            // entirely - the rescale lifecycle owns the bookkeeping
            // for the old subtask now.
        } else if (!retry && job.awaiting_restart) {
            // A FATAL error reported by a draining subtask must not be
            // swallowed with the rest of the drain: fatal means the retry
            // this drain is preparing cannot succeed (the named restore
            // point is damaged), and the verdict text is the operator's
            // diagnosis. Record it; restart_job_locked_ turns the pending
            // restart into a failure carrying this cause.
            if (msg.had_error && msg.fatal && job.fatal_cause.empty()) {
                job.fatal_cause = msg.worker_id + "/" + msg.role + "[" +
                                  std::to_string(msg.subtask_idx) + "]: " + msg.error_message;
            }
            auto pending_it = job.pending_per_worker.find(msg.worker_id);
            if (pending_it != job.pending_per_worker.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            release_slot_locked();
            job.restart_drained_keys.insert(key);
            if (protocol_trace::enabled()) {
                protocol_trace::Event("SubtaskDrained")
                    .u("job", job.id)
                    .u("sub", msg.subtask_idx)
                    .emit();
            }
            // Every expected-to-drain surviving subtask has now reported
            // → time to redeploy.
            if (restart_drain_covered_(job) && !stage_in_doubt_resolution_locked_(job)) {
                restart_deploys = restart_job_locked_(job);
            }
        } else if (!retry && msg.had_error && msg.transport_only && !msg.fatal &&
                   !job.completion_signalled && !job.cancel_requested &&
                   !job.checkpoint.checkpoint_dir.empty() &&
                   job.restart_attempts < effective_max_restarts(job.checkpoint)) {
            // TRANSPORT-only: every failure this subtask saw was a send refused
            // by a departed peer. That is a SYMPTOM. The cause - the peer's own
            // exit, or its worker's loss - is already on its way here, and
            // restarting on the symptom races ahead of it: recovery is entered
            // from the bridge path instead of the worker-loss path, the other
            // survivors are still running and still failing on stale bridges,
            // and the redeploy does not converge. Measured: one killed worker
            // produced restart attempts 1 and 2 within a second, and the killed
            // worker then never wound down (item 83).
            //
            // So hold it. Free the slot and retire it from pending exactly as
            // the restart path does, record the cause, and arm a short deadline.
            // If a real cause arrives first it starts the restart and clears
            // this. If none arrives - a healthy job, where nothing else is
            // coming - the sweep acts on it, because a refused send that
            // nobody acts on is the silent short stream the bridge's throw
            // exists to prevent.
            auto pending_it = job.pending_per_worker.find(msg.worker_id);
            if (pending_it != job.pending_per_worker.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            release_slot_locked();
            job.errors.push_back(msg.worker_id + "/" + msg.role + "[" +
                                 std::to_string(msg.subtask_idx) + "]: " + msg.error_message);
            if (job.transport_pending_cause.empty()) {
                job.transport_pending_cause = msg.error_message;
                job.transport_error_deadline =
                    std::chrono::steady_clock::now() + cfg_.transport_symptom_grace;
                log::info("coordinator.restart",
                          "job_id=" + std::to_string(job.id) +
                              " transport failure held pending its cause for " +
                              std::to_string(cfg_.transport_symptom_grace.count()) +
                              "ms (a departed peer is a symptom; the cause restarts the job, "
                              "and if none arrives this does): " +
                              msg.error_message);
            }
        } else if (!retry && msg.had_error && !msg.fatal && !job.completion_signalled &&
                   !job.cancel_requested && !job.checkpoint.checkpoint_dir.empty() &&
                   job.restart_attempts < effective_max_restarts(job.checkpoint)) {
            // !msg.fatal: a FATAL subtask error must not enter this branch.
            // Restarting cannot fix it (today: the configured restore point
            // failed its integrity check), and a restart comes back up on
            // fresh state - converting a loud refusal into silently empty
            // output (item 19). A fatal error falls through to the ordinary
            // completion path below, which records the cause, cancels the
            // peers, and fails the job carrying the verdict.
            // Checkpointed job + restart budget: a subtask error (e.g. a
            // source's EOS final-checkpoint timeout that threw) rolls the WHOLE
            // job back to its last completed checkpoint and replays - exactly as
            // a worker loss does - rather than failing. Mirrors mark_worker_lost_locked_:
            // the failed subtask already finished, so it goes into restart_pending
            // (redeploy); the other in-flight subtasks drain via CancelJob
            // (restart_drain_expected) and are redeployed too. Gated on the same
            // max_restarts_on_worker_loss budget as worker-loss recovery.
            auto pending_it = job.pending_per_worker.find(msg.worker_id);
            if (pending_it != job.pending_per_worker.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            release_slot_locked();
            restart_deploys = initiate_job_restart_locked_(
                job, "subtask error", msg.error_message, error_restart_cancels);
        } else if (!retry) {
            ++job.completed_count;
            if (msg.had_error) {
                job.errors.push_back(msg.worker_id + "/" + msg.role + "[" +
                                     std::to_string(msg.subtask_idx) + "]: " + msg.error_message);
                SubtaskErrorRecord se;
                se.role = msg.role;
                se.subtask_idx = msg.subtask_idx;
                se.worker_id = msg.worker_id;
                se.attempt = job.restart_attempts;
                se.ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
                se.message = msg.error_message;
                job.subtask_errors.push_back(std::move(se));

                // An unrecoverable subtask error must take the job DOWN, not leave
                // it waiting. The job completes only when completed_count reaches
                // expected_completion, and a subtask that failed to BUILD never
                // closed its output channel - so every downstream peer blocks on an
                // open empty channel and never reports. completed_count then never
                // reaches the target and the job hangs until the watchdog declares
                // the worker lost, which is longer than a submit's own timeout.
                //
                // That is how a rejected parameter presented as a DEADLOCK with no
                // error attached: nexmark q5's HOP arguments were in Flink's order,
                // the operator's constructor correctly refused them, and the refusal
                // never surfaced. The parameter is now also rejected at bind time,
                // but the general shape - a task that cannot start wedging its peers
                // - applies to every operator that validates in its constructor, and
                // to every required connector option that is only checked there.
                //
                // Cancelling the peers makes each report SubtaskFinished, which
                // drives completed_count to expected_completion and completes the
                // job as FAILED, carrying the error above. The checkpointed-restart
                // path already does exactly this to drain before redeploying; this
                // is the same broadcast for the case where there is no restart to
                // make.
                //
                // It sets error_cancel_broadcast, NOT cancel_requested. The latter
                // decides the reported outcome, and it is checked before the error
                // list - so reusing it made a FAILED job report "cancelled", which
                // two history-server tests caught. Reordering that check instead
                // would be worse: a user cancel routinely produces teardown errors,
                // so a cancelled job would start reporting "failed".
                if (!job.error_cancel_broadcast && !job.cancel_requested &&
                    !job.completion_signalled) {
                    job.error_cancel_broadcast = true;
                    // Bound the count-based completion this broadcast relies
                    // on, and say the broadcast happened at all: the QUAL-06
                    // wedge was invisible precisely because this path used
                    // to cancel every peer without a log line and then wait
                    // on a count with no deadline (followups item 75a).
                    job.terminal_cancel_deadline =
                        std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
                    std::size_t peers = 0;
                    for (const auto& [worker_id2, _] : job.tasks_by_worker) {
                        auto cit = registered_.find(worker_id2);
                        if (cit != registered_.end() && !cit->second->lost && cit->second->conn) {
                            error_restart_cancels.emplace_back(cit->second->conn, job_id);
                            ++peers;
                        }
                    }
                    log::warn("coordinator.restart",
                              "job_id=" + std::to_string(job_id) +
                                  " unrecoverable subtask error from " + msg.worker_id + "/" +
                                  msg.role + "[" + std::to_string(msg.subtask_idx) +
                                  "]; cancelling peers on " + std::to_string(peers) +
                                  " worker(s) to complete the job as FAILED (deadline " +
                                  std::to_string(cfg_.restart_drain_timeout.count()) +
                                  "ms): " + msg.error_message);
                }
            }
            // Free this subtask's slot on the owning worker.
            auto pending_it = job.pending_per_worker.find(msg.worker_id);
            if (pending_it != job.pending_per_worker.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            release_slot_locked();
            if (job.completed_count >= job.expected_completion) {
                signal_job_completion_locked_(job);
                job_to_signal = job_it->second;
            }
        }
    }
    // Fire restart deploys outside the lock so a slow send doesn't
    // stall mu_-holders. Best-effort: if a send fails the watchdog
    // will catch the second worker loss on the next tick.
    for (auto& d : restart_deploys) {
        if (d.conn)
            send_frame(*d.conn, d.frame);
    }
    // Drain the still-running subtasks of a checkpointed job we just put into
    // awaiting_restart (their SubtaskFinished arrivals count toward the drain,
    // then restart_job_locked_ redeploys from the last checkpoint).
    for (const auto& [conn, jid] : error_restart_cancels) {
        if (conn) {
            CancelJobMsg cj;
            cj.job_id = jid;
            send_frame(*conn, fenced_frame_(MessageKind::CancelJob, cj));
        }
    }

    if (retry && retry_conn) {
        DeployMsg deploy_msg;
        deploy_msg.job_id = job_id;
        deploy_msg.tasks.push_back(std::move(retry_task));
        const auto frame = fenced_frame_(MessageKind::Deploy, deploy_msg);
        send_frame(*retry_conn, frame);
    }
    // Slot freed: wake any submit_job waiting on capacity.
    cv_.notify_all();
}

std::vector<PluginBinary> Coordinator::plugins_for_worker_locked_(const JobState& job,
                                                                  WorkerConnection& worker) {
    std::vector<PluginBinary> out;
    out.reserve(job.plugins.size());
    for (const auto& plug : job.plugins) {
        if (!plug.content_hash.empty() &&
            worker.shipped_plugin_hashes.count(plug.content_hash) != 0) {
            // Already delivered on this connection: send the reference.
            out.push_back(
                PluginBinary{.name = plug.name, .content_hash = plug.content_hash, .bytes = {}});
            metrics::coordinator::plugin_ship_deduped();
            continue;
        }
        out.push_back(plug);
        if (!plug.content_hash.empty()) {
            worker.shipped_plugin_hashes.insert(plug.content_hash);
        }
        metrics::coordinator::plugin_bytes_shipped(plug.bytes.size());
    }
    return out;
}

void Coordinator::retire_job_manifest_(JobId job_id, const char* status) {
    if (ha_dir_.empty()) {
        return;
    }
    try {
        const auto store = make_coordination_store(ha_dir_);
        const auto prefix = "jobs/" + std::to_string(job_id);
        store->put(prefix + "/TERMINAL", std::string{"{\"status\":\""} + status + "\"}");
        // Trailing slash: an object-store list is a string-prefix match,
        // and "jobs/1" would also sweep jobs/10's keys.
        for (const auto& key : store->list(prefix + "/")) {
            if (std::filesystem::path(key).filename() != "TERMINAL") {
                store->remove(key);
            }
        }
    } catch (const std::exception& e) {
        // Worst case is the pre-fix behaviour (a recoverable manifest),
        // and the tombstone may already have landed, which alone is
        // enough for recovery to skip the job.
        log::warn("coordinator.ha",
                  "job_id=" + std::to_string(job_id) +
                      " could not retire the HA manifest at terminal: " + e.what());
    }
}

void Coordinator::mark_job_finished_(const JobState& job) {
    if (!job.checkpoint.track_runs || job.checkpoint.checkpoint_dir.empty()) {
        return;
    }
    try {
        make_coordination_store(job.checkpoint.checkpoint_dir)
            ->put(finished_marker_key(job.id), std::to_string(job.latest_completed_checkpoint_id));
    } catch (const std::exception& e) {
        // Without the marker the next run resumes a job that had finished: its
        // sources restore at their end and it emits nothing. Loud, not fatal -
        // the job's own output is already complete.
        log::warn("coordinator.complete",
                  "job_id=" + std::to_string(job.id) + " could not record its clean finish in " +
                      job.checkpoint.checkpoint_dir + ": " + e.what() +
                      "; a rerun will resume it at its end");
    }
}

void Coordinator::signal_job_completion_locked_(JobState& job) {
    if (job.completion_signalled) {
        return;
    }
    job.completion_signalled = true;
    // The job allocates no more checkpoint ids, so its claimer stops, even
    // part way through retrying a store that keeps failing.
    {
        auto& record = *job.triggered_record;
        std::lock_guard record_lock(record.mu);
        record.retired = true;
        record.cv.notify_all();
    }
    const char* status = "ok";
    if (job.cancel_requested) {
        metrics::coordinator::job_cancelled();
        log::info("coordinator.complete", "job_id=" + std::to_string(job.id) + " cancelled");
        status = "cancelled";
    } else if (job.errors.empty()) {
        metrics::coordinator::job_completed_ok();
        log::info("coordinator.complete", "job_id=" + std::to_string(job.id) + " ok");
    } else {
        metrics::coordinator::job_failed();
        log::warn("coordinator.complete",
                  "job_id=" + std::to_string(job.id) +
                      " failed errors=" + std::to_string(job.errors.size()));
        status = "failed";
    }
    {
        // "errors" stays a count for existing consumers; "job_name" and the
        // first "error" string are additive (a lineage FAIL event uses them).
        std::string payload = "{\"job_id\":" + std::to_string(job.id) +
                              ",\"job_name\":" + js_quote(job.name) + ",\"status\":\"" + status +
                              "\"" + ",\"errors\":" + std::to_string(job.errors.size());
        if (!job.errors.empty()) {
            payload += ",\"error\":" + js_quote(job.errors.front());
        }
        payload += "}";
        events::publish("coordinator.job_completed", payload);
    }
    {
        CompletedJobRecord rec;
        rec.job_id = job.id;
        rec.status = status;
        rec.errors = job.errors;
        rec.restart_attempts = job.restarts_total;
        rec.latest_completed_checkpoint_id = job.latest_completed_checkpoint_id;
        rec.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - job.submit_time);
        rec.completed_at_unix_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count();
        persist_history_record_(ha_dir_, rec, epoch());
        // The history record preserves what the job WAS; the manifest is
        // what recovery would RE-RUN. A terminal job must keep the former
        // and lose the latter, or the next coordinator takeover resurrects
        // a job the operator cancelled (item 69 - it re-ran QUAL-05's
        // control arm mid-campaign and competed for the subject's slots).
        retire_job_manifest_(job.id, status);
        if (job.errors.empty() && !job.cancel_requested) {
            mark_job_finished_(job);
        }
        history_.push_back(std::move(rec));
        while (history_.size() > kCoordinatorHistoryCap) {
            history_.pop_front();
        }
        // Evict the JobState behind jobs that fell out of the ring (item
        // 32): the public record and the internal state share one
        // retention window, so "inspectable" means the same thing on both
        // surfaces. The guard is structural belt-and-braces - only
        // signalled jobs enter the deque - and the job being signalled
        // here is the newest entry, never the one evicted.
        terminal_job_order_.push_back(job.id);
        while (terminal_job_order_.size() > kCoordinatorHistoryCap) {
            const JobId evict_id = terminal_job_order_.front();
            terminal_job_order_.pop_front();
            auto evict_it = jobs_.find(evict_id);
            if (evict_it != jobs_.end() && evict_it->second->completion_signalled) {
                jobs_.erase(evict_it);
            }
        }
    }
    if (job.notify_client_conn != nullptr) {
        if (!job.submit_ack_sent) {
            // The submitting client has not been ACKED yet: this job ran to
            // completion inside handle_submit_'s admit-to-ack window (tiny
            // jobs do). Pushing now would put JobCompleted on the wire
            // ahead of SubmitJobAck and the submitter reads a protocol
            // violation. handle_submit_ flushes this after the ack.
            job.completion_push_deferred = true;
        } else {
            push_job_completed_locked_(job);
        }
    }
    cv_.notify_all();
}

void Coordinator::push_job_completed_locked_(JobState& job) {
    JobCompletedMsg jc;
    jc.job_id = job.id;
    // A client-initiated cancel always reports !ok with a
    // dedicated message so the submitter can distinguish "job
    // failed" from "I asked it to stop". The per-subtask errors
    // (typically "cancelled" from our run_task_ path, or
    // truncated-channel diagnostics) are preserved as additional
    // context in the errors list.
    if (job.cancel_requested) {
        jc.ok = false;
        jc.errors = job.errors;
        jc.errors.insert(jc.errors.begin(), "cancelled by client");
    } else {
        jc.ok = job.errors.empty();
        jc.errors = job.errors;
    }
    // Best-effort send under the lock; client_fd is only ever
    // touched here and at connection teardown.
    send_frame(*job.notify_client_conn, encode_frame(MessageKind::JobCompleted, jc));
}

void Coordinator::expect_workers(std::vector<std::string> worker_ids) {
    std::lock_guard lock(mu_);
    expected_workers_ = std::move(worker_ids);
}

bool Coordinator::await_registrations(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this] {
        for (const auto& id : expected_workers_) {
            if (registered_.find(id) == registered_.end()) {
                return false;
            }
        }
        return true;
    });
}

bool Coordinator::await_completion(std::chrono::milliseconds timeout) {
    return await_job_completion(legacy_active_job_id_, timeout);
}

bool Coordinator::await_job_completion(JobId job_id, std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this, job_id] {
        auto it = jobs_.find(job_id);
        if (it == jobs_.end()) {
            return false;
        }
        return it->second->completion_signalled ||
               (it->second->completed_count >= it->second->expected_completion);
    });
}

std::vector<std::string> Coordinator::errors() const {
    return job_errors(legacy_active_job_id_);
}

std::vector<std::string> Coordinator::job_errors(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it != jobs_.end()) {
        return it->second->errors;
    }
    // The JobState is evicted at the history cap (item 32), but the
    // errors travel in the CompletedJobRecord: a caller asking about a
    // terminal job keeps its answer for exactly as long as the history
    // ring remembers the job at all. This also answers for jobs that
    // terminated under a PREVIOUS process, whose records were recovered
    // from the HA dir and never had a JobState here.
    for (const auto& rec : history_) {
        if (rec.job_id == job_id) {
            return rec.errors;
        }
    }
    return {};
}

std::vector<CompletedJobRecord> Coordinator::job_history() const {
    std::lock_guard lock(mu_);
    return std::vector<CompletedJobRecord>(history_.begin(), history_.end());
}

std::optional<CompletedJobRecord> Coordinator::job_history(JobId job_id) const {
    std::lock_guard lock(mu_);
    for (const auto& rec : history_) {
        if (rec.job_id == job_id) {
            return rec;
        }
    }
    return std::nullopt;
}

std::size_t Coordinator::free_slots() const {
    std::lock_guard lock(mu_);
    std::size_t free = 0;
    for (const auto& [_, worker] : registered_) {
        if (!worker->lost && worker->slots_in_use < worker->slot_capacity) {
            free += (worker->slot_capacity - worker->slots_in_use);
        }
    }
    return free;
}

std::vector<std::string> Coordinator::lost_workers() const {
    std::lock_guard lock(mu_);
    return lost_worker_ids_;
}

void Coordinator::stop() {
    stop_.store(true, std::memory_order_release);
    // Wake every cv_ waiter whose predicate checks stop_: the recovery
    // retry loop, and any submit blocked in its slot wait.
    cv_.notify_all();
    wake_trigger_loop_();
    // Taken under mu_, which every spawn site holds: a dispatch thread or the
    // watchdog can still be staging a restart while stop() runs, and once
    // this hold has passed neither thread is started again (both spawns check
    // stop_ under mu_).
    std::thread recovery_retry;
    std::thread in_doubt_resolution;
    {
        std::lock_guard lock(mu_);
        recovery_retry = std::move(recovery_retry_thread_);
        in_doubt_resolution = std::move(in_doubt_resolution_thread_);
    }
    if (recovery_retry.joinable()) {
        recovery_retry.join();
    }
    if (in_doubt_resolution.joinable()) {
        in_doubt_resolution.join();
    }
    // Tear down per-job autoscalers before everything else.
    // Their polling threads might be sitting on mu_ trying to call
    // request_operator_rescale; joining them now (under the move-out +
    // destroy-outside-the-lock pattern) lets the rest of stop()
    // proceed without contending with autoscaler callbacks.
    stop_autoscalers_();
    // Wake the accept thread, join it, and only then close the listener.
    // Closing the listener to wake it is what this used to do, and it is
    // not safe: on Darwin a close() that lands while the accept thread is
    // entering accept() can miss it, and then both wait for ever - accept()
    // asleep, and close(), uninterruptibly, for accept() to let go of the
    // descriptor - until a connection happens to arrive. On Linux close()
    // does not wake accept() at all (the shutdown_read that went with it
    // did), and a descriptor closed under a thread about to use it can be
    // reissued to another listener in this process before it gets there.
    //
    // Write listener_fd_ = -1 only AFTER joining the accept thread, which
    // is the happens-before edge for its reads of listener_fd_.
    if (accept_wake_ != nullptr) {
        accept_wake_->wake();
    }
    // And every connection still being admitted: shut down under it, so its
    // thread leaves its handshake, its first frame, or a reply it is sending
    // from handle_first_frame_ to a peer that does not read, at once. No
    // admission starts after this (begin_admission_ checks stop_ under the
    // same lock). One whose first frame is in and that is waiting for the
    // first-frame lock checks stop_ once it has it.
    {
        std::lock_guard lock(admission_mu_);
        for (auto& [_, pending] : pending_admissions_) {
            interrupt_admission_locked_(
                pending, "the coordinator is stopping", /*queued_too=*/true);
        }
    }
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorStopAfterAcceptWake);
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    // Before the listener closes (on Linux the wake an admission thread's
    // handshake waits on is the listener's descriptor), and before anything
    // an admission could hand a connection to is torn down.
    for (auto& t : admission_threads_) {
        if (t.thread.joinable()) {
            t.thread.join();
        }
    }
    admission_threads_.clear();
    // The wake goes before the listener: on Linux it acts on the
    // listener's descriptor, which must not outlive the close.
    accept_wake_.reset();
    if (listener_fd_ >= 0) {
        network::NetworkSocket::close(listener_fd_);
    }
    listener_fd_ = -1;
    if (watchdog_thread_.joinable()) {
        watchdog_thread_.join();
    }
    if (checkpoint_thread_.joinable()) {
        checkpoint_thread_.join();
    }
    // Tear down client connections. shutdown_read() on each unblocks
    // its handler thread's recv() so the thread can exit; the shared_ptr
    // keeps the Connection alive even if the thread has already exited.
    {
        std::vector<ClientSession> sessions;
        {
            std::lock_guard lock(client_mu_);
            sessions = std::move(client_sessions_);
            client_sessions_.clear();
        }
        // Shut every socket down first, THEN join. Doing it one session
        // at a time would wait out each blocked recv() in turn.
        for (auto& session : sessions) {
            if (session.conn) {
                session.conn->shutdown_read();
            }
        }
        for (auto& session : sessions) {
            if (session.thread.joinable()) {
                session.thread.join();
            }
        }
    }
    std::vector<std::shared_ptr<WorkerConnection>> workers;
    {
        std::lock_guard lock(mu_);
        for (auto& [_, worker] : registered_) {
            workers.push_back(worker);
        }
        registered_.clear();
    }
    for (auto& worker : workers) {
        if (worker->conn) {
            worker->conn->shutdown_read();
        }
        if (worker->reader.joinable()) {
            worker->reader.join();
        }
        worker->conn.reset();  // destructor closes
    }
    // Last, once every thread that starts a checkpoint-id claimer (the
    // trigger loop, and a worker's dispatch thread holding an end-of-input
    // request, joined above with its reader) or claims for itself has gone. A claim in flight
    // finishes its store write first; a parked claimer, or one waiting out a retry delay, leaves at
    // once.
    std::vector<std::shared_ptr<JobState::TriggeredRecord>> recorders;
    {
        std::lock_guard lock(id_recorders_mu_);
        recorders.swap(id_recorders_);
    }
    for (const auto& record : recorders) {
        std::lock_guard lock(record->mu);
        record->stopping = true;
        record->cv.notify_all();
    }
    for (const auto& record : recorders) {
        if (record->thread.joinable()) {
            record->thread.join();
        }
    }
}

void Coordinator::handle_request_final_checkpoint_(
    MessageReader& r,
    const WorkerConnection& sender,
    const std::shared_ptr<network::Connection>& reply_conn) {
    auto msg = decode_request_final_checkpoint(r);
    // Holds a request in the window a cancel can open: decided here, the
    // CancelJob broadcast still in flight (see cancel_job).
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeFinalCheckpointRequest);
    // This runs on the requesting worker's dispatch thread, which handles
    // that worker's frames one at a time, so it never waits on the store. It
    // used to claim an id not yet on record itself, behind the job's claimer
    // on TriggeredRecord::write_mu when that claimer's write was out; a slow
    // object-store write then held every later frame from that worker (and,
    // before the dispatch thread existed, its heartbeats past
    // heartbeat_timeout, which got it declared lost). Such a request is held
    // instead: the
    // job's claimer is asked for the id, and the trigger loop, which a
    // landing claim wakes, answers it. While the id cannot be recorded the
    // request goes unanswered, and the source fails its subtask when its
    // bounded wait runs out, so the restart replays the tail under a
    // checkpoint; a decline would instead read as the job going away and
    // leave the tail uncommitted.
    std::optional<FinalCheckpointAssignedMsg> reply;
    std::shared_ptr<JobState::TriggeredRecord> record;
    std::uint64_t want = 0;
    {
        std::lock_guard lock(mu_);
        if (sender.lost) {
            return;  // the loss fold has already accounted for this session
        }
        auto it = jobs_.find(msg.job_id);
        auto* job = it == jobs_.end() ? nullptr : it->second.get();
        reply = decide_final_request_locked_(job, msg.job_id, msg.role, msg.subtask_idx);
        if (!reply.has_value()) {
            job->held_final_requests.push_back(
                {.role = msg.role, .subtask_idx = msg.subtask_idx, .reply_conn = reply_conn});
            record = job->triggered_record;
            want = job->next_checkpoint_id;
        }
    }
    if (!reply.has_value()) {
        log::info("coordinator.final_checkpoint",
                  "job_id=" + std::to_string(msg.job_id) + " " + msg.role + ":" +
                      std::to_string(msg.subtask_idx) +
                      " asked for its final checkpoint id before checkpoint id " +
                      std::to_string(want) + " was on record; it is answered once that id is");
        request_id_record_(record, want);
        // The claim may have landed since the decision; a pass of the trigger
        // loop now answers the request rather than at its next tick.
        wake_trigger_loop_();
        return;
    }
    send_frame(*reply_conn, fenced_frame_(MessageKind::FinalCheckpointAssigned, *reply));
}

std::optional<FinalCheckpointAssignedMsg> Coordinator::decide_final_request_locked_(
    JobState* job, JobId job_id, const std::string& role, std::uint32_t subtask_idx) {
    FinalCheckpointAssignedMsg reply;
    reply.job_id = job_id;
    reply.role = role;
    reply.subtask_idx = subtask_idx;
    reply.final_checkpoint_id = 0;  // 0 == declined; `decline` says why
    reply.decline = FinalCheckpointDecline::UnknownJob;
    if (job == nullptr) {
        return reply;
    }
    if (job->completion_signalled || job->cancel_requested) {
        reply.decline = FinalCheckpointDecline::Stopping;
        return reply;
    }
    if (job->checkpoint.checkpoint_dir.empty()) {
        reply.decline = FinalCheckpointDecline::NoCheckpointDir;
        return reply;
    }
    if (!job->final_checkpoint_id.has_value()) {
        // The reply is what sends this barrier: the source injects the id the
        // moment it lands, so the id must be on record before it is allocated
        // (claim_checkpoint_id_), and the job is renumbered first above any
        // ids another coordinator holds.
        (void)renumber_above_held_ids_locked_(*job);
        if (job->triggered_record->owned.load(std::memory_order_acquire) <
            job->next_checkpoint_id) {
            return std::nullopt;
        }
        // First source to reach EOS: assign ONE final id for the job, seed its
        // pending-ack set from the live task set (every subtask must ack it
        // before it completes), and stamp the start time. We do NOT broadcast
        // TriggerCheckpoint: the requesting source(s) inject this id through
        // their own EOS drain (so snapshot_offset captures the EOS offset),
        // and the barrier propagates downstream through the data plane exactly
        // like a periodic one - a broadcast would double-inject at the source.
        // handle_subtask_checkpointed_ completes it normally (COMPLETED-<id> +
        // CommitCheckpoint broadcast) once all ack.
        const auto final_id = job->next_checkpoint_id++;
        job->final_checkpoint_id = final_id;
        if (protocol_trace::enabled()) {
            protocol_trace::Event("Trigger")
                .u("job", job->id)
                .u("ckpt", final_id)
                .u("epoch", epoch())
                .b("final", true)
                .emit();
        }
        std::unordered_set<std::string> pending;
        for (const auto& [tkey, _] : job->task_records) {
            pending.insert(tkey);
        }
        job->pending_checkpoint_acks[final_id] = std::move(pending);
        {
            // Same participant record as the periodic and savepoint triggers.
            // Missing it here left the END-OF-STREAM final checkpoint with an
            // empty set in its marker, which the consistency check reported as
            // every subtask being an outsider - three trigger sites, and the
            // check found the one that was forgotten.
            auto& rec = job->checkpoint_participants[final_id];
            rec.generation = job->state_generation;
            auto& participants = rec.subtasks;
            participants.clear();
            for (const auto& [tkey, _p] : job->task_records) {
                const auto colon = tkey.rfind(':');
                if (colon == std::string::npos) {
                    continue;
                }
                try {
                    participants.insert(
                        static_cast<std::uint32_t>(std::stoul(tkey.substr(colon + 1))));
                } catch (const std::exception&) {
                    continue;
                }
            }
        }
        job->pending_checkpoint_start_times[final_id] = std::chrono::steady_clock::now();
        clink::metrics::ckpt::triggered();
        log::info("coordinator.final_checkpoint",
                  "job_id=" + std::to_string(job_id) + " final_id=" + std::to_string(final_id));
    }
    reply.decline = FinalCheckpointDecline::None;
    reply.final_checkpoint_id = *job->final_checkpoint_id;
    job->sources_requested_final.insert(role + ":" + std::to_string(subtask_idx));
    return reply;
}

bool Coordinator::answer_final_requests_locked_(
    JobState& job,
    std::vector<std::pair<std::shared_ptr<network::Connection>, FinalCheckpointAssignedMsg>>&
        replies) {
    // A job awaiting its restart answers nothing: the subtasks that asked are
    // being torn down (a request held past the source's own bounded wait is
    // what failed it), and an id assigned now would trigger a checkpoint in
    // the middle of the drain. The restart clears the held requests and the
    // redeployed sources ask again.
    if (job.awaiting_restart) {
        return false;
    }
    while (!job.held_final_requests.empty()) {
        auto& held = job.held_final_requests.front();
        auto reply = decide_final_request_locked_(&job, job.id, held.role, held.subtask_idx);
        if (!reply.has_value()) {
            // Still not on record, and the requests behind this one wait for
            // the same id.
            return false;
        }
        replies.emplace_back(std::move(held.reply_conn), std::move(*reply));
        job.held_final_requests.erase(job.held_final_requests.begin());
    }
    return true;
}

void Coordinator::handle_commit_confirmed_(MessageReader& r, const WorkerConnection& sender) {
    const auto msg = decode_commit_confirmed(r);
    const std::string key = msg.role + ":" + std::to_string(msg.subtask_idx);
    std::string marker_root;  // checkpoint_dir; empty = nothing to publish
    std::uint64_t confirmed_id = 0;
    std::uint64_t redeploys = 0;
    JobId jid{};
    {
        std::lock_guard lock(mu_);
        if (sender.lost) {
            return;  // the loss fold has already accounted for this session
        }
        auto job_it = jobs_.find(msg.job_id);
        if (job_it == jobs_.end()) {
            return;  // late confirmation for a finished/evicted job
        }
        auto& job = *job_it->second;
        auto pend_it = job.pending_confirms.find(msg.checkpoint_id);
        if (pend_it == job.pending_confirms.end()) {
            return;  // untracked job, unknown checkpoint, or already drained
        }
        pend_it->second.erase(key);
        if (!pend_it->second.empty()) {
            return;
        }
        // Every tracked task confirmed: the external commits for this
        // checkpoint provably EXECUTED. Publish that durably so restores
        // can select it, and prune tracking at and below it - an older
        // checkpoint that never drains can never become the newest
        // confirmed one now.
        job.pending_confirms.erase(job.pending_confirms.begin(),
                                   job.pending_confirms.upper_bound(msg.checkpoint_id));
        confirmed_id = msg.checkpoint_id;
        jid = msg.job_id;
        redeploys = job.redeploys;
        marker_root = job.checkpoint.checkpoint_dir;
        if (marker_root.empty()) {
            // No directory to publish to: memory is all there is.
            job.latest_confirmed_checkpoint_id =
                std::max(job.latest_confirmed_checkpoint_id, confirmed_id);
            return;
        }
    }
    // The state the specification has before WriteConfirmed (the marker
    // "due"): every tracked task confirmed, the checkpoint out of tracking,
    // and neither the marker nor latest_confirmed has moved.
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeConfirmedMarker);
    try {
        make_coordination_store(marker_root)
            ->put("_jobs/" + std::to_string(jid) + "/CONFIRMED-" + std::to_string(confirmed_id),
                  "job=" + std::to_string(jid) + "\ncheckpoint=" + std::to_string(confirmed_id) +
                      "\n");
    } catch (const std::exception& e) {
        clink::log::error("coordinator.checkpoint",
                          "could not durably record commit confirmation of checkpoint " +
                              std::to_string(confirmed_id) + ": " + e.what());
        return;
    }
    // The marker is durable: advance the confirmed restore point now, and only
    // now, as latest_completed advances only once COMPLETED is (the
    // specification's AdvanceConfirmed, after its WriteConfirmed). Advancing it
    // before the write held memory ahead of the disk, and a failed write left
    // memory advanced over no marker at all.
    //
    // The WriteConfirmed line is emitted in the same hold as the advance, so it
    // stands for both steps, and no decision taken under mu_ (a restart's
    // RestartProceeds, a Redeploy) can fall between the line and the memory it
    // describes. A restart that redeployed while the write was out has begun a
    // run this confirmation does not belong to: the specification's Redeploy
    // leaves the confirmation's put to land with nothing advancing after it,
    // so neither memory nor the trace moves. The marker stays: it is true, and
    // what reads it (a takeover, the in-doubt walk) reads it as true. A
    // takeover that reports it validates through LostWriteConfirmed.
    //
    // The line carries the coordinator's epoch. A superseded coordinator does
    // not know it was superseded and emits it like the leader would; the
    // specification turned its confirmation stale at the supersession, and
    // the epoch is what tells the trace validator that this line is that
    // stale put landing, not the leader's advance.
    {
        std::lock_guard lock(mu_);
        auto job_it = jobs_.find(jid);
        if (job_it == jobs_.end()) {
            return;  // finished or evicted while the marker was written
        }
        if (job_it->second->redeploys != redeploys) {
            clink::log::info("coordinator.checkpoint",
                             "job_id=" + std::to_string(jid) + " checkpoint " +
                                 std::to_string(confirmed_id) +
                                 " commit-CONFIRMED on disk; the job restarted while the marker "
                                 "was written, so its confirmed restore point is unchanged");
            return;
        }
        auto& job = *job_it->second;
        job.latest_confirmed_checkpoint_id =
            std::max(job.latest_confirmed_checkpoint_id, confirmed_id);
        if (protocol_trace::enabled()) {
            protocol_trace::Event("WriteConfirmed")
                .u("job", jid)
                .u("ckpt", confirmed_id)
                .u("epoch", epoch())
                .emit();
        }
    }
    clink::log::info("coordinator.checkpoint",
                     "job_id=" + std::to_string(jid) + " checkpoint " +
                         std::to_string(confirmed_id) + " commit-CONFIRMED");
}

std::vector<Coordinator::PendingDeploy> Coordinator::initiate_job_restart_locked_(
    JobState& job,
    const std::string& reason,
    const std::string& cause,
    std::vector<std::pair<std::shared_ptr<network::Connection>, JobId>>& cancels) {
    job.awaiting_restart = true;
    // The specification's RestartOnError: a whole-job restart the coordinator
    // starts for a subtask error or an unattributed transport failure, with
    // no worker lost in its view (yet). A loss declared during the drain folds
    // in afterwards, as its own event. The restart a FAILED checkpoint starts
    // is not recorded here: the model takes it as part of CoordComplete
    // (outcome failed), which the coordinator has already emitted.
    if (protocol_trace::enabled() && reason != "checkpoint failure") {
        protocol_trace::Event("RestartOnError").u("job", job.id).s("cause", reason).emit();
    }
    // A real cause is now driving recovery, so any transport failure held
    // pending is absorbed as the symptom it was (item 83). The error text
    // stays in job.errors for diagnosis; it just no longer needs its own
    // restart.
    job.transport_error_deadline = {};
    job.transport_pending_cause.clear();
    job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
    // Drain the still-IN-FLIGHT subtasks (pending_per_worker, minus anything
    // the caller already removed), and redeploy EVERY subtask of the job
    // (tasks_by_worker) - including any that ALREADY FINISHED. A bounded job
    // re-runs its whole topology from the last checkpoint, so finished
    // subtasks (e.g. a sink that exited at EOS before the source's
    // final-checkpoint timed out) must redeploy too; redeploying only the
    // in-flight ones would orphan them. Contrast mark_worker_lost_locked_,
    // whose redeploy set draws only from pending_per_worker.
    std::unordered_set<std::string> in_flight;
    for (const auto& [other_worker, pending] : job.pending_per_worker) {
        for (const auto& [role, sub] : pending) {
            in_flight.insert(role + ":" + std::to_string(sub));
        }
    }
    job.restart_drain_expected = in_flight;
    job.restart_pending.clear();
    for (const auto& [other_worker, dts] : job.tasks_by_worker) {
        for (const auto& dt : dts) {
            const std::string k = dt.role + ":" + std::to_string(dt.subtask_idx);
            if (in_flight.count(k) == 0) {
                job.restart_pending.emplace_back(dt.role, dt.subtask_idx);
                // Already finished: counted as drained (see the worker-loss path).
                if (protocol_trace::enabled()) {
                    auto wit = registered_.find(other_worker);
                    if (wit != registered_.end() && !wit->second->lost) {
                        protocol_trace::Event("SubtaskDrained")
                            .u("job", job.id)
                            .u("sub", dt.subtask_idx)
                            .emit();
                    }
                }
            }
        }
    }
    job.completed_count = 0;  // belongs to the failed attempt
    job.errors.clear();
    // Counted here rather than at the redeploy below, because both branches
    // of that redeploy are the same restart and counting in one would
    // undercount.
    metrics::orch::job_restarted();
    log::warn("coordinator.restart",
              "job_id=" + std::to_string(job.id) + " " + reason + " -> whole-job restart" +
                  " (attempt " + std::to_string(job.restart_attempts + 1) + "/" +
                  std::to_string(effective_max_restarts(job.checkpoint)) + ") drain_expected=" +
                  std::to_string(job.restart_drain_expected.size()) + " cause=" + cause);
    if (job.restart_drain_expected.empty()) {
        // No in-flight subtasks: redeploy now. Do NOT broadcast CancelJob -
        // there is nothing to drain, and a cancel would race the
        // just-redeployed subtasks. In-doubt resolution first, though: the
        // resolution thread fires the deferred restart when the broker has
        // answered.
        if (!stage_in_doubt_resolution_locked_(job)) {
            return restart_job_locked_(job);
        }
        return {};
    }
    // Cancel the in-flight subtasks so they drain; when all have reported,
    // restart_job_locked_ redeploys the whole job from the last checkpoint.
    for (const auto& [worker_id, _] : job.tasks_by_worker) {
        auto cit = registered_.find(worker_id);
        if (cit != registered_.end() && !cit->second->lost && cit->second->conn) {
            cancels.emplace_back(cit->second->conn, job.id);
        }
    }
    return {};
}

void Coordinator::handle_subtask_checkpointed_(MessageReader& r, const WorkerConnection& sender) {
    auto msg = decode_subtask_checkpointed(r);
    // The SubtaskAck protocol event is recorded by the worker where the ack is
    // sent (the subtask's own step), not here at the receipt.
    // What a completed checkpoint CONSISTS OF, not just that it happened: the
    // generation whose directories hold it, and the subtask indices that acked it.
    // Recorded in the marker so a checkpoint can be verified across subtasks
    // afterwards - see the COMPLETED-N write below.
    struct CompletedCheckpoint {
        JobId job_id{};
        std::uint64_t checkpoint_id{};
        std::uint32_t generation{1};
        std::set<std::uint32_t> subtasks;
        // A checkpoint completing while the job is draining for a restart
        // gets its COMPLETED marker (durability is unconditional) but NOT
        // the commit broadcast: half the sinks are being torn down, so a
        // broadcast lands on some and not others, and a completed
        // checkpoint with PARTIAL external commits is unrepairable at
        // restore granularity - qual01-20260818a replayed the committed
        // slices as 13,519 duplicates. Left completed-but-unconfirmed, the
        // restart's held in-doubt resolution finalises every prepared
        // transaction atomically (teardown now preserves them), or the
        // job restores below it with nothing committed. One decision, one
        // decider.
        bool suppress_commit{false};
        // The run the checkpoint completed in (JobState::redeploys). Its commit
        // goes to that run's sinks or nowhere: a restart that both begins and
        // redeploys while the marker is written has torn them down, and the
        // new run neither tracks nor commits the old run's checkpoint.
        std::uint64_t redeploys{0};
    };
    std::vector<CompletedCheckpoint> just_completed;
    std::string completed_marker_dir;
    // A group whose member failed gets AbortCheckpoint
    // broadcast to every worker hosting any of its members. Collected
    // under the lock; sent outside it.
    std::vector<std::pair<JobId, std::uint64_t>> groups_to_abort;
    // A FAILED checkpoint initiates a whole-job restart (the abort above
    // discards a staged interval only a rewind re-emits). Collected under
    // the lock; dispatched outside it, after the abort broadcast.
    std::vector<PendingDeploy> failed_ckpt_deploys;
    std::vector<std::pair<std::shared_ptr<network::Connection>, JobId>> failed_ckpt_cancels;
    // BeginRescale frames queued by the checkpoint-completed
    // path. When the cutover checkpoint ack closes, any operator still
    // in Preparing advances to Draining and we send BeginRescale to
    // every worker hosting it.
    std::vector<PendingDeploy> rescale_frames;
    // A hot cutover awaiting its cut, and this is an ack for C from a task the
    // rescaled operator feeds - the ack that closes the cut in practice, since
    // the fed side acks after every barrier has reached it. Reached before the
    // lock, on this connection's dispatch thread, so a Delay armed on the point
    // holds this ack back while the other connections' frames - the old
    // subtasks' exits at C - are processed. A test aid; one relaxed load.
    if (msg.checkpoint_id != 0 &&
        msg.checkpoint_id == hot_cut_ack_pin_ckpt_.load(std::memory_order_relaxed)) {
        std::shared_ptr<const std::unordered_set<std::string>> pinned;
        {
            std::lock_guard pin_lock(hot_cut_ack_pin_mu_);
            pinned = hot_cut_ack_pin_keys_;
        }
        if (pinned != nullptr &&
            pinned->count(msg.role + ":" + std::to_string(msg.subtask_idx)) != 0) {
            CLINK_FAULT_POINT(clink::fault::points::kHotCutoverCutAck);
        }
    }
    {
        std::lock_guard lock(mu_);
        if (sender.lost) {
            return;  // the loss fold has already accounted for this session
        }
        auto job_it = jobs_.find(msg.job_id);
        if (job_it == jobs_.end()) {
            return;
        }
        auto& job = *job_it->second;
        const std::string key = msg.role + ":" + std::to_string(msg.subtask_idx);
        auto ckpt_it = job.pending_checkpoint_acks.find(msg.checkpoint_id);
        if (ckpt_it == job.pending_checkpoint_acks.end()) {
            return;  // unknown / superseded checkpoint
        }
        // Test-only fault injection (env-gated, default off): drop EVERY ack for
        // a final checkpoint's first-acking subtask so its pending set never
        // empties and the source hits the no-crash EOS-timeout path (its
        // wait_final_committed times out -> it throws -> whole-job restart).
        //   FIRST: stall only the FIRST final id (bound once, not re-armed), so
        //   the replay's fresh final id commits -> exactly-once recovery (the
        //   eos_timeout_recovery leg).
        //   EVERY: re-arm on each new final id, so every attempt's final ckpt
        //   stalls -> the source errors every attempt -> the job exhausts its
        //   restart budget then fails loudly (the eos_budget_exhaustion leg,
        //   proving the recovery is bounded - no infinite restart loop).
        static const bool kStallFirst = std::getenv("CLINK_TEST_STALL_FIRST_FINAL_CKPT") != nullptr;
        static const bool kStallEvery = std::getenv("CLINK_TEST_STALL_EVERY_FINAL_CKPT") != nullptr;
        if ((kStallFirst || kStallEvery) && job.final_checkpoint_id.has_value() &&
            msg.checkpoint_id == *job.final_checkpoint_id) {
            const auto fid = *job.final_checkpoint_id;
            if (job.test_stalled_final_id != fid &&
                (kStallEvery || !job.test_stalled_final_id.has_value())) {
                job.test_stalled_final_id = fid;  // (re-)arm for this final id
                job.test_stall_key = key;         // first subtask that acks it
            }
            if (job.test_stalled_final_id == fid && key == job.test_stall_key) {
                return;  // drop EVERY ack for this (key, final id) -> never completes
            }
        }
        ckpt_it->second.erase(key);
        if (!msg.ok) {
            job.failed_checkpoint_acks[msg.checkpoint_id].insert(key);
        }

        // Commit-group progress accounting. A failed ack aborts the
        // group, marked here and broadcast below.
        //
        // Note what this does NOT do: it does not gate the commit. The
        // commit broadcast further down is per-checkpoint and job-wide,
        // fired once every subtask acked ok, so a job's sinks are told to
        // commit together with or without a group. `gs.pending` is
        // maintained but never tested - there is no group-scoped commit.
        // The group's only effect is that a failing ack aborts NOW rather
        // than after every subtask has answered, which matters because no
        // timeout ever abandons a pending checkpoint: if a peer never
        // answers, the checkpoint-level abort below never runs and staged
        // sink transactions would sit staged indefinitely.
        if (auto cg_it = job.subtask_commit_group.find(key);
            cg_it != job.subtask_commit_group.end()) {
            const auto& group_name = cg_it->second;
            auto& by_ckpt = job.commit_group_progress[msg.checkpoint_id];
            auto group_state_it = by_ckpt.find(group_name);
            if (group_state_it == by_ckpt.end()) {
                // First ack for (ckpt, group): initialise pending
                // from the static group membership.
                auto memberships_it = job.commit_groups.find(group_name);
                if (memberships_it != job.commit_groups.end()) {
                    auto& gs = by_ckpt[group_name];
                    gs.pending = memberships_it->second;
                    group_state_it = by_ckpt.find(group_name);
                }
            }
            if (group_state_it != by_ckpt.end()) {
                auto& gs = group_state_it->second;
                gs.pending.erase(key);
                if (!msg.ok && !gs.aborted) {
                    gs.aborted = true;
                    groups_to_abort.emplace_back(msg.job_id, msg.checkpoint_id);
                }
            }
        }

        // Per-subtask snapshot ack
        // counters fire regardless of whether the whole checkpoint
        // completes - they tell us how many subtask snapshots came
        // back ok vs failed, independent of the coordinator-level
        // completion path.
        if (msg.ok) {
            clink::metrics::ckpt::subtask_ack_ok();
        } else {
            clink::metrics::ckpt::subtask_ack_failure();
        }

        // Every subtask has answered. Whether the checkpoint COMPLETED is
        // a different question: an answer of "I could not snapshot" is
        // still an answer.
        //
        // Read the emptiness ONCE, before either branch touches the map:
        // the failure branch erases ckpt_it, and re-testing it afterwards
        // would be a use-after-free.
        const bool all_subtasks_answered = ckpt_it->second.empty();
        bool checkpoint_failed = false;
        if (all_subtasks_answered) {
            const auto failed_it = job.failed_checkpoint_acks.find(msg.checkpoint_id);
            if (failed_it != job.failed_checkpoint_acks.end() && !failed_it->second.empty()) {
                // Every checkpoint above this one that is still collecting
                // acks is invalid too: the rewind re-emits this interval, and
                // a later checkpoint completing on top of it would restore
                // the job past the aborted output (see above_rewind_floor).
                // Only when the job will not run on past this interval: a rewind
                // begins below, a restart is already pending and will restore
                // below this id, or the job has no budget left to rewind and so
                // fails (out_of_budget, below). A checkpoint completing above
                // the failed one would otherwise make a restore point past the
                // aborted interval.
                const bool running_with_checkpoints = !job.completion_signalled &&
                                                      !job.cancel_requested &&
                                                      !job.checkpoint.checkpoint_dir.empty();
                const bool out_of_budget =
                    running_with_checkpoints && !job.awaiting_restart &&
                    job.restart_attempts >= effective_max_restarts(job.checkpoint);
                const bool floor_needed = running_with_checkpoints;
                if (floor_needed && (job.rewind_floor_checkpoint_id == 0 ||
                                     msg.checkpoint_id < job.rewind_floor_checkpoint_id)) {
                    job.rewind_floor_checkpoint_id = msg.checkpoint_id;
                }
                // At least one subtask failed to take its snapshot, so
                // this checkpoint does not exist in full and must not be
                // recorded as if it did. Withholding COMPLETED-N is the
                // whole point: the marker is what recovery restores from,
                // and restoring from a checkpoint a subtask never wrote
                // means restoring that operator's state from nowhere.
                //
                // latest_completed_checkpoint_id is deliberately left
                // where it was, so recovery falls back to the last
                // checkpoint that really did complete.
                std::string who;
                for (const auto& k : failed_it->second) {
                    who += (who.empty() ? "" : ", ") + k;
                }
                log::error("coordinator.checkpoint",
                           "checkpoint " + std::to_string(msg.checkpoint_id) + " of job " +
                               std::to_string(msg.job_id) + " FAILED: subtask(s) " + who +
                               " could not snapshot. No COMPLETED marker is written and the "
                               "job's recovery point stays at checkpoint " +
                               std::to_string(job.latest_completed_checkpoint_id) +
                               "; staged sink transactions for this checkpoint are aborted.");
                clink::metrics::ckpt::failed();
                // Roll back anything staged for it. Sinks that pre-committed
                // must not be left holding a transaction no commit will ever
                // arrive for.
                groups_to_abort.emplace_back(msg.job_id, msg.checkpoint_id);
                job.failed_checkpoint_acks.erase(msg.checkpoint_id);
                job.pending_checkpoint_start_times.erase(msg.checkpoint_id);
                job.commit_group_progress.erase(msg.checkpoint_id);
                job.pending_checkpoint_acks.erase(msg.checkpoint_id);
                checkpoint_failed = true;
                if (protocol_trace::enabled()) {
                    protocol_trace::Event("CoordComplete")
                        .u("job", msg.job_id)
                        .u("ckpt", msg.checkpoint_id)
                        .s("outcome", "failed")
                        .u("epoch", epoch())
                        .emit();
                }
                // The abort above discards every sink's staged interval for
                // this checkpoint - a barrier-sealed transaction holds the
                // records of (K-1, K], and once aborted nothing re-emits
                // them unless the job REWINDS. This used to abort and sail
                // on: the runner survives its own capture failure (it acks
                // ok=false and keeps processing), so a transient snapshot
                // error silently cost one checkpoint interval of output.
                // A failed checkpoint therefore initiates the same
                // whole-job restart a subtask error does; the replay from
                // the last completed checkpoint re-produces the aborted
                // interval. A job already restarting, completing or
                // cancelling is left to that path. A job with no restart
                // budget left FAILS, as the subtask-error path does when out
                // of budget: carrying on past the aborted interval would lose
                // it silently, every later checkpoint completing over the gap
                // (it used to, and the budget is spent by every recovery a job
                // makes, so a long-running job reached this sooner or later).
                if (!job.awaiting_restart && !job.completion_signalled && !job.cancel_requested &&
                    !job.checkpoint.checkpoint_dir.empty() &&
                    job.restart_attempts < effective_max_restarts(job.checkpoint)) {
                    // The circuit-breaker (item 77b): a restart rewinds and
                    // re-emits an interval through every sink, which repairs
                    // a TRANSIENT snapshot failure and only amplifies a
                    // persistent one - QUAL-09's state volume at ENOSPC
                    // crashlooped ~100 rewind-restarts, each visibly
                    // shrinking upsert output, until a drain timeout ended
                    // it by accident. Consecutive means no completed
                    // checkpoint in between (the completion branch below
                    // resets the counter); at the limit the job FAILS
                    // carrying the cause, through the same
                    // deadline-protected cancel broadcast a fatal subtask
                    // error uses.
                    ++job.consecutive_ckpt_failure_restarts;
                    if (job.consecutive_ckpt_failure_restarts == 1) {
                        job.first_consecutive_ckpt_failure_at = std::chrono::steady_clock::now();
                    }
                    const auto limit = cfg_.checkpoint_failure_restart_limit;
                    // Persistence is a property of DURATION, not of a count
                    // (item 80): five failures one checkpoint interval apart
                    // is ~75 seconds at a 15s interval, and QUAL-09's cloud
                    // run watched a 109-second transient ENOSPC window get
                    // declared "persistent" 37 seconds before it released.
                    // The job fails only when the count is reached AND the
                    // run of failures has spanned the configured window; a
                    // zero window keeps count-only semantics.
                    const auto failure_span =
                        std::chrono::steady_clock::now() - job.first_consecutive_ckpt_failure_at;
                    if (limit != 0 && job.consecutive_ckpt_failure_restarts >= limit &&
                        failure_span >= cfg_.checkpoint_failure_restart_window) {
                        log::error(
                            "coordinator.checkpoint",
                            "job_id=" + std::to_string(msg.job_id) + " has restarted " +
                                std::to_string(job.consecutive_ckpt_failure_restarts) +
                                " consecutive times from FAILED checkpoints over " +
                                std::to_string(
                                    std::chrono::duration_cast<std::chrono::seconds>(failure_span)
                                        .count()) +
                                "s with none completing in between; the cause is persistent "
                                "and a rewind cannot repair it. Failing the job.");
                        job.errors.push_back(
                            std::to_string(job.consecutive_ckpt_failure_restarts) +
                            " consecutive restarts from failed checkpoints with no completed "
                            "checkpoint between them (last: checkpoint " +
                            std::to_string(msg.checkpoint_id) +
                            "); the cause is persistent - check the state volume (ENOSPC?) "
                            "and the workers' snapshot errors.");
                        if (!job.error_cancel_broadcast) {
                            job.error_cancel_broadcast = true;
                            job.terminal_cancel_deadline =
                                std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
                            for (const auto& [worker_id2, _] : job.tasks_by_worker) {
                                auto cit = registered_.find(worker_id2);
                                if (cit != registered_.end() && !cit->second->lost &&
                                    cit->second->conn) {
                                    failed_ckpt_cancels.emplace_back(cit->second->conn, msg.job_id);
                                }
                            }
                        }
                    } else {
                        failed_ckpt_deploys = initiate_job_restart_locked_(
                            job,
                            "checkpoint failure",
                            "checkpoint " + std::to_string(msg.checkpoint_id) +
                                " failed; its aborted sink transactions carried one interval "
                                "of output that only a rewind re-emits",
                            failed_ckpt_cancels);
                    }
                } else if (out_of_budget) {
                    log::error("coordinator.checkpoint",
                               "job_id=" + std::to_string(msg.job_id) + " checkpoint " +
                                   std::to_string(msg.checkpoint_id) +
                                   " failed and the job has no restart budget left (" +
                                   std::to_string(job.restart_attempts) + " of " +
                                   std::to_string(effective_max_restarts(job.checkpoint)) +
                                   " used) to rewind and re-emit its aborted interval. Failing "
                                   "the job rather than continuing past the gap.");
                    job.errors.push_back(
                        "checkpoint " + std::to_string(msg.checkpoint_id) +
                        " failed with no restart budget left to rewind; its aborted sink "
                        "transactions carried one interval of output that only a rewind "
                        "re-emits, so the job fails instead of continuing without it. Restore "
                        "it from checkpoint " +
                        std::to_string(job.latest_completed_checkpoint_id) +
                        " once the cause (see the workers' snapshot errors) is fixed.");
                    if (!job.error_cancel_broadcast) {
                        job.error_cancel_broadcast = true;
                        job.terminal_cancel_deadline =
                            std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
                        for (const auto& [worker_id2, _] : job.tasks_by_worker) {
                            auto cit = registered_.find(worker_id2);
                            if (cit != registered_.end() && !cit->second->lost &&
                                cit->second->conn) {
                                failed_ckpt_cancels.emplace_back(cit->second->conn, msg.job_id);
                            }
                        }
                    }
                }
            }
        }
        // A checkpoint above the rewind floor answered ok from every subtask,
        // but it must not complete. Found by the exactly-once model
        // (formal/ExactlyOnce.tla, design record 012): checkpoint k FAILED and
        // the job began its rewind, while checkpoint k+1, whose barrier was
        // already at the sinks, kept collecting acks through the drain and
        // completed. Its marker made it a restore point; in-doubt resolution
        // committed its transactions and confirmed it; the job restored from
        // it - past interval k, whose transactions the failure had aborted and
        // which only a rewind below k re-emits. Silent loss of one interval,
        // with every gate green. A checkpoint above the floor is therefore
        // discarded exactly like a failed one: no marker, and an abort for its
        // staged transactions, since the rewind re-produces its interval too.
        const bool above_rewind_floor = all_subtasks_answered && !checkpoint_failed &&
                                        job.rewind_floor_checkpoint_id != 0 &&
                                        msg.checkpoint_id > job.rewind_floor_checkpoint_id;
        if (above_rewind_floor) {
            if (protocol_trace::enabled()) {
                protocol_trace::Event("CoordComplete")
                    .u("job", msg.job_id)
                    .u("ckpt", msg.checkpoint_id)
                    .s("outcome", "discarded")
                    .u("epoch", epoch())
                    .emit();
            }
            log::warn("coordinator.checkpoint",
                      "checkpoint " + std::to_string(msg.checkpoint_id) + " of job " +
                          std::to_string(msg.job_id) + " completed above FAILED checkpoint " +
                          std::to_string(job.rewind_floor_checkpoint_id) +
                          " while the job rewinds for it; discarded (no COMPLETED marker, "
                          "staged sink transactions aborted) so the rewind re-emits both "
                          "intervals");
            clink::metrics::ckpt::failed();
            groups_to_abort.emplace_back(msg.job_id, msg.checkpoint_id);
            job.failed_checkpoint_acks.erase(msg.checkpoint_id);
            job.pending_checkpoint_start_times.erase(msg.checkpoint_id);
            job.commit_group_progress.erase(msg.checkpoint_id);
            job.checkpoint_participants.erase(msg.checkpoint_id);
            job.pending_checkpoint_acks.erase(ckpt_it);
        }
        if (all_subtasks_answered && !checkpoint_failed && !above_rewind_floor) {
            if (protocol_trace::enabled()) {
                protocol_trace::Event("CoordComplete")
                    .u("job", msg.job_id)
                    .u("ckpt", msg.checkpoint_id)
                    .s("outcome", "completed")
                    .u("epoch", epoch())
                    .emit();
            }
            job.failed_checkpoint_acks.erase(msg.checkpoint_id);
            // A completed checkpoint is the proof the failure cause was
            // transient: the 77b circuit-breaker counts only CONSECUTIVE
            // failure-restarts, and this is where consecutive ends.
            job.consecutive_ckpt_failure_restarts = 0;
            job.first_consecutive_ckpt_failure_at = {};
            // The same proof, held for long enough, forgives the restart
            // budget: it bounds a burst of failures, not a job's lifetime.
            if (job.restart_attempts > 0 &&
                std::chrono::steady_clock::now() - job.last_restart_at >=
                    cfg_.restart_budget_reset_after) {
                log::info("coordinator.restart",
                          "job_id=" + std::to_string(msg.job_id) + " checkpoint " +
                              std::to_string(msg.checkpoint_id) +
                              " completed after a clean run since its last restart; its " +
                              std::to_string(job.restart_attempts) +
                              " spent restart(s) no longer count against the budget");
                job.restart_attempts = 0;
            }
            // latest_completed_checkpoint_id is NOT advanced here. It used to
            // be, under this lock, with the COMPLETED marker written after the
            // lock was released - and a restart deciding its restore point in
            // that window redeployed from a checkpoint whose marker was not on
            // disk yet. Found by the exactly-once model (formal/ExactlyOnce.tla,
            // design record 012): the coordinator then died before the fsync
            // landed, its successor saw no marker and restored lower, and the
            // recoverable sinks, which had already re-committed the unmarked
            // checkpoint's handles at open, published its interval twice.
            // Memory now advances where the marker becomes durable, in the
            // write loop below, so the restore point never names a checkpoint
            // the next leader cannot see.
            {
                // The acking set is about to be erased; capture which subtasks it
                // covered. Keys are "role:subtask_idx".
                CompletedCheckpoint done;
                done.job_id = msg.job_id;
                done.checkpoint_id = msg.checkpoint_id;

                if (auto pit = job.checkpoint_participants.find(msg.checkpoint_id);
                    pit != job.checkpoint_participants.end()) {
                    // Both from the TRIGGER record - see CheckpointParticipants.
                    done.generation = pit->second.generation;
                    done.subtasks = pit->second.subtasks;
                    job.checkpoint_participants.erase(pit);
                }
                done.suppress_commit = job.awaiting_restart;
                done.redeploys = job.redeploys;
                just_completed.push_back(std::move(done));
            }
            completed_marker_dir = job.checkpoint.checkpoint_dir;
            job.pending_checkpoint_acks.erase(ckpt_it);
            if (auto sit = job.pending_checkpoint_start_times.find(msg.checkpoint_id);
                sit != job.pending_checkpoint_start_times.end()) {
                const auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - sit->second)
                                        .count();
                // The adaptive checkpoint-mode policy observes this at the
                // next trigger: duration relative to interval is its
                // pressure signal.
                job.last_checkpoint_duration_ms = static_cast<std::uint64_t>(dur_ms);
                clink::metrics::ckpt::completed(static_cast<std::uint64_t>(dur_ms));
                clink::metrics::ckpt::last_completed_now();
                job.pending_checkpoint_start_times.erase(sit);
                // Lifecycle span for OTLP export: trigger-to-completion of
                // this checkpoint. A no-op unless an exporter enabled the
                // span buffer (SpanBuffer::record checks first).
                if (clink::metrics::SpanBuffer::global().enabled()) {
                    clink::metrics::OtlpSpan span;
                    span.name = "clink.checkpoint";
                    span.end_unix_nano = clink::metrics::otlp_now_unix_nano();
                    span.start_unix_nano =
                        span.end_unix_nano - static_cast<std::uint64_t>(dur_ms) * 1'000'000ULL;
                    span.attributes = {{"clink.job_id", std::to_string(msg.job_id)},
                                       {"clink.checkpoint_id", std::to_string(msg.checkpoint_id)}};
                    clink::metrics::SpanBuffer::global().record(std::move(span));
                }
            } else {
                clink::metrics::ckpt::completed(0);
                clink::metrics::ckpt::last_completed_now();
            }

            // Any operator in Preparing uses THIS
            // checkpoint as its cutover_checkpoint. Advance to
            // Draining and dispatch BeginRescale to every worker hosting
            // an old subtask of the op. The drain emits DrainMarker,
            // which closes the source-runner loop, which triggers
            // SubtaskFinished, which calls mark_old_drained,
            // which fires dispatch_cutover_deploy_locked_ when
            // the last old subtask drains.
            //
            // NOT while the job is draining for a replan. The shipped
            // per-operator rescale holds its op in Preparing across the
            // whole-job drain and closes it with mark_replan_complete,
            // which requires Preparing. Advancing the op to Draining here
            // would let the drain's own SubtaskFinished acks - which now
            // translate to the operator - walk the state machine to
            // CuttingOver and fire a cutover deployment on top of the
            // replan's: two deployments of one operator. A checkpoint
            // completing inside the drain window is the only way in, so
            // this is the one gate needed.
            if (job.rescale_coordinator && !job.awaiting_restart) {
                if (job.hot_cutover.has_value()) {
                    // Hot cutover: the arm went out BEFORE this checkpoint
                    // was triggered (stopping at a barrier that has already
                    // passed would stop nothing), so no BeginRescale goes out
                    // here - only the state machine advances, and only for the
                    // reserved id. Old subtasks whose exits at C landed before
                    // this completion were held in early_drains (the finish
                    // handler cannot count a drain until the op is Draining);
                    // they are counted now, and if that is every one of them
                    // the cut is done and the rebind goes out from here. The
                    // order between an exit and C's completion no longer
                    // decides whether the cutover completes.
                    auto& hot = *job.hot_cutover;
                    if (msg.checkpoint_id == hot.cutover_checkpoint &&
                        hot.phase == JobState::HotCutover::Phase::AwaitingCut) {
                        hot.phase_deadline =
                            std::chrono::steady_clock::now() + cfg_.hot_cutover_phase_timeout;
                        if (job.rescale_coordinator->mark_checkpoint_ready(hot.op_id,
                                                                           msg.checkpoint_id)) {
                            for (const auto& early : hot.early_drains) {
                                (void)job.rescale_coordinator->mark_old_drained(
                                    hot.op_id, early.subtask_idx_in_op);
                            }
                            if (auto post = job.rescale_coordinator->status(hot.op_id);
                                post.has_value() && post->state == RescaleState::CuttingOver) {
                                // begin_rebind may abort and reset hot_cutover;
                                // nothing touches `hot` after this call.
                                hot_cutover_begin_rebind_locked_(job, rescale_frames);
                            }
                        }
                    }
                } else {
                    for (const auto& op_status : job.rescale_coordinator->all()) {
                        if (op_status.state != RescaleState::Preparing) {
                            continue;
                        }
                        if (job.rescale_coordinator->mark_checkpoint_ready(op_status.op_id,
                                                                           msg.checkpoint_id)) {
                            auto post = job.rescale_coordinator->status(op_status.op_id);
                            if (post.has_value()) {
                                dispatch_begin_rescale_locked_(job,
                                                               op_status.op_id,
                                                               msg.checkpoint_id,
                                                               post->target_parallelism,
                                                               rescale_frames);
                            }
                        }
                    }
                }
            }
        }
    }
    // Send BeginRescale frames outside the lock. Best-effort:
    // a send failure means the watchdog will catch the worker loss; the
    // coordinator's rescale will time out from the user's POV (no
    // dedicated timeout wired here yet) and a future re-request will
    // start fresh.
    for (auto& f : rescale_frames) {
        if (f.conn)
            send_frame(*f.conn, f.frame);
    }

    // Broadcast AbortCheckpoint to every worker hosting tasks
    // for this job. Each worker dispatches to its registered abort
    // callbacks; non-group sinks ignore (their Sink::on_abort is the
    // default no-op). We send abort BEFORE the commit broadcast below
    // so the receiving worker processes the abort first; aborted-group
    // sinks then no-op on the following CommitCheckpoint.
    for (const auto& [jid, ckpt_id] : groups_to_abort) {
        std::vector<std::shared_ptr<network::Connection>> worker_conns;
        {
            std::lock_guard lock(mu_);
            auto job_it = jobs_.find(jid);
            if (job_it != jobs_.end()) {
                for (const auto& [worker_id, _] : job_it->second->tasks_by_worker) {
                    auto worker_it = registered_.find(worker_id);
                    if (worker_it != registered_.end() && !worker_it->second->lost &&
                        worker_it->second->conn) {
                        worker_conns.push_back(worker_it->second->conn);
                    }
                }
            }
        }
        AbortCheckpointMsg ac;
        ac.job_id = jid;
        ac.checkpoint_id = ckpt_id;
        const auto frame = fenced_frame_(MessageKind::AbortCheckpoint, ac);
        for (const auto& c : worker_conns)
            send_frame(*c, frame);
    }
    // The failed-checkpoint restart: cancels drain the in-flight subtasks
    // (their SubtaskFinished arrivals complete the drain and fire the
    // redeploy); deploys are the immediate-redeploy branch when nothing was
    // in flight. After the aborts above so the sinks process the abort
    // before any teardown reaches them.
    for (const auto& [conn, jid] : failed_ckpt_cancels) {
        if (conn) {
            CancelJobMsg cj;
            cj.job_id = jid;
            send_frame(*conn, fenced_frame_(MessageKind::CancelJob, cj));
        }
    }
    for (auto& d : failed_ckpt_deploys) {
        if (d.conn) {
            send_frame(*d.conn, d.frame);
        }
    }
    // Write the COMPLETED marker outside the lock so a slow store does not
    // hold mu_. This runs on the acking worker's dispatch thread, not its
    // reader, so the write delays that worker's later frames but never its
    // heartbeats: a write outlasting heartbeat_timeout no longer gets a live
    // worker declared lost.
    for (const auto& [jid, ckpt_id, gen, subtasks, suppress_commit, completed_in_run] :
         just_completed) {
        // The COMPLETED-N marker is the authoritative record that a
        // checkpoint reached global completion, and the 2PC sinks key their
        // commit-on-restore on finding it. It therefore has to be durable
        // BEFORE any worker is told to commit, and a failure to write it has
        // to stop the commit rather than be ignored.
        //
        // It used to be a bare `std::ofstream out(marker); out << ...;` with
        // no fsync and no error check. Two ways that lost exactly-once:
        //   * ENOSPC / EACCES produced no marker at all, yet the commit
        //     broadcast went out anyway. The sinks committed externally, the
        //     coordinator restarted, found no COMPLETED-N, rewound to an
        //     older checkpoint and re-emitted already-committed output.
        //   * Even on success the bytes sat in the page cache, so a power
        //     loss after the external commits left the same hole.
        // Now: durable write first, and on failure abort this job's commit
        // for this checkpoint. Skipping the broadcast is the safe side of
        // the trade - the prepared transactions stay prepared and are
        // resolved on the next successful checkpoint or at restore, whereas
        // committing without a durable marker is unrecoverable.
        if (!completed_marker_dir.empty()) {
            // <checkpoint_dir>/_jobs/<job_id>/COMPLETED-<id>.
            //
            // The job id used to be missing, which broke two things at
            // once. Recovery could not find the marker at all -
            // latest_completed_id_on_disk reads the job-scoped path, so
            // every completed checkpoint was invisible and a recovered
            // job restarted from scratch, silently. And two jobs sharing
            // a checkpoint directory wrote COMPLETED-5 to the same file,
            // so one job's progress was read as the other's.
            //
            // Established by running it, not by reading: a recovered job
            // came back with restore_from_checkpoint_id=0 while
            // COMPLETED-1 sat on disk.
            //
            // The `_jobs/` component came second, and from the same
            // method. Scoping to a bare <job_id> put the markers into the
            // per-subtask state namespace - subtask directories are bare
            // integers, so job 1 wrote its markers inside subtask 1's
            // state directory. See completed_marker_dir_for.
            //
            // Markers written by an older build sit at the flat or bare
            // job-id path and are not migrated. Nothing reads them, so
            // nothing is lost by leaving them.
            const std::string marker_key =
                "_jobs/" + std::to_string(jid) + "/COMPLETED-" + std::to_string(ckpt_id);
            CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeCompletedMarker);
            try {
                // The marker records what the checkpoint CONSISTS OF, not only
                // that it completed: the state generation whose directories hold
                // it, and the subtask indices that acked it.
                //
                // That is what makes a checkpoint verifiable across subtasks rather
                // than only file by file. F65 was a file appearing for a checkpoint
                // it was never part of - written by a later topology into a
                // directory the checkpoint's restore point still named - and no
                // per-file integrity check can see that, because each file is
                // individually valid. A recorded participant set can.
                std::string subtask_list;
                for (const auto idx : subtasks) {
                    subtask_list += (subtask_list.empty() ? "" : ",") + std::to_string(idx);
                }
                make_coordination_store(completed_marker_dir)
                    ->put(marker_key,
                          "job=" + std::to_string(jid) + "\ncheckpoint=" + std::to_string(ckpt_id) +
                              "\ngeneration=" + std::to_string(gen) + "\nsubtasks=" + subtask_list +
                              "\n");
            } catch (const std::exception& e) {
                clink::log::error(
                    "coordinator.checkpoint",
                    "could not durably record completion of checkpoint " + std::to_string(ckpt_id) +
                        " for job " + std::to_string(jid) + " (" + e.what() +
                        "); withholding the commit broadcast so no sink commits externally "
                        "without a recoverable record of it");
                continue;
            }
        }
        // The checkpoint is durable (or the job keeps no directory, so memory
        // is all there is): advance the restore point now, and only now, in
        // the hold that emits WriteCompleted, so memory and the line move
        // together.
        //
        // Only while the run the checkpoint completed in is still the job's.
        // The put runs outside the lock, and a restart can begin and redeploy
        // while it is out: the new run restored from a point the checkpoint is
        // not part of, the specification's Redeploy has forgotten it, and
        // advancing would hand the new run another run's restore point. The
        // marker stays on disk, true, as a CONFIRMED marker landing after a
        // redeploy does.
        //
        // Nor while the restart is held for in-doubt resolution, or has been
        // and waits only to redeploy: the stage fixed the walk's range
        // (RestartProceeds reports it), the walk runs to the completed point
        // it read, and memory moving under it took the trace's walk past
        // where the engine's stopped. The line is still the put landing; the
        // redeploy restores from the confirmed point, which the walk alone
        // moves.
        {
            std::lock_guard lock(mu_);
            auto job_it = jobs_.find(jid);
            if (job_it != jobs_.end() && job_it->second->redeploys == completed_in_run) {
                auto& job = *job_it->second;
                // The trace witnesses the durable write before anything can
                // kill the process "after the marker": a validation run showed
                // a coordinator exiting at that fault point with the marker on
                // disk and no WriteCompleted in its trace, and the recovered
                // leader's restore point then read as impossible.
                if (!completed_marker_dir.empty() && protocol_trace::enabled()) {
                    protocol_trace::Event("WriteCompleted")
                        .u("job", jid)
                        .u("ckpt", ckpt_id)
                        .u("epoch", epoch())
                        .emit();
                }
                if (!job.resolving_in_doubt && !job.in_doubt_walked) {
                    job.latest_completed_checkpoint_id =
                        std::max(job.latest_completed_checkpoint_id, ckpt_id);
                    // The restore point has moved past the last rescale, so
                    // every subtask's own directory now holds state written
                    // under the CURRENT layout and the retained pre-rescale
                    // translation is not only unnecessary but wrong. Drop it.
                    if (!job.stale_layout_blocks.empty() &&
                        job.latest_completed_checkpoint_id > job.stale_layout_through) {
                        job.stale_layout_blocks.clear();
                        job.stale_layout_through = 0;
                    }
                }
            }
        }
        if (!completed_marker_dir.empty()) {
            CLINK_FAULT_POINT(clink::fault::points::kCoordinatorAfterCompletedMarker);
        }
        // A checkpoint that completed while the job was draining for a
        // restart keeps its durable marker but is NOT committed from here:
        // a broadcast into a half-torn-down job commits some sinks and not
        // others, and a completed checkpoint with partial external commits
        // forces the restore to either replay committed slices (duplicates)
        // or skip uncommitted ones (loss). The restart's held in-doubt
        // resolution finalises it as one decision instead.
        if (suppress_commit) {
            if (protocol_trace::enabled()) {
                // Traced only while the run the checkpoint completed in is
                // still the job's: after its Redeploy the specification has
                // forgotten the checkpoint, and has no withheld broadcast of it.
                std::lock_guard lock(mu_);
                const auto job_it = jobs_.find(jid);
                if (job_it != jobs_.end() && job_it->second->redeploys == completed_in_run) {
                    protocol_trace::Event("Broadcast")
                        .u("job", jid)
                        .u("ckpt", ckpt_id)
                        .b("withheld", true)
                        .u("epoch", epoch())
                        .emit();
                }
            }
            log::info("coordinator.checkpoint",
                      "job_id=" + std::to_string(jid) + " checkpoint " + std::to_string(ckpt_id) +
                          " completed during a restart drain; commit broadcast withheld for "
                          "the restart's in-doubt resolution");
            continue;
        }
        // The commit phase of the 2PC sink protocol: broadcast CommitCheckpoint
        // to every worker hosting tasks for this job. The marker write
        // ordering matters - by the time workers commit their pre-staged
        // transactions, the marker is durable, so a crash mid-broadcast
        // still lets recovery find COMPLETED-N and commit on restore.
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeCommitBroadcast);
        // One hold decides whether the commit goes out, seeds the checkpoint's
        // confirmation set, picks the connections and emits the Broadcast line.
        // Split across holds, a restart that began and redeployed between them
        // seeded the old run's checkpoint into the new run, whose restart had
        // just cleared pending_confirms, and committed it on the new run's
        // connections after the Redeploy line.
        std::vector<std::shared_ptr<network::Connection>> worker_conns;
        CommitCheckpointMsg cc;
        cc.job_id = jid;
        cc.checkpoint_id = ckpt_id;
        {
            std::lock_guard lock(mu_);
            auto job_it = jobs_.find(jid);
            if (job_it == jobs_.end()) {
                continue;
            }
            auto& job = *job_it->second;
            // The run the checkpoint completed in is gone: a restart began and
            // redeployed while the marker was written. Its sinks were torn
            // down, the new run's restore point and resolution account for the
            // checkpoint, and the specification's Redeploy forgets it, so
            // nothing is sent and nothing traced.
            if (job.redeploys != completed_in_run) {
                log::info("coordinator.checkpoint",
                          "job_id=" + std::to_string(jid) + " checkpoint " +
                              std::to_string(ckpt_id) +
                              " completed in a run the job has since redeployed past; no commit "
                              "broadcast");
                continue;
            }
            // The suppress decision is RE-TAKEN here, not merely carried
            // from the ack that completed the checkpoint: the marker
            // fsync above runs outside the lock (deliberately), and a
            // restart or cancel beginning inside that window would
            // otherwise receive the broadcast into a half-torn-down job
            // - the partial-commit state the captured flag was added to
            // prevent, re-opened one durable write later. Cancel gets
            // the same treatment as restart because a cancelled job has
            // no held resolution behind it: a partial commit there is
            // permanent.
            if (job.awaiting_restart || job.cancel_requested) {
                if (protocol_trace::enabled()) {
                    protocol_trace::Event("Broadcast")
                        .u("job", jid)
                        .u("ckpt", ckpt_id)
                        .b("withheld", true)
                        .u("epoch", epoch())
                        .emit();
                }
                log::info("coordinator.checkpoint",
                          "job_id=" + std::to_string(jid) + " checkpoint " +
                              std::to_string(ckpt_id) +
                              " completed but a restart or cancel began before its commit "
                              "broadcast; withheld for in-doubt resolution");
                continue;
            }
            for (const auto& [worker_id, _] : job.tasks_by_worker) {
                auto worker_it = registered_.find(worker_id);
                if (worker_it != registered_.end() && !worker_it->second->lost &&
                    worker_it->second->conn) {
                    worker_conns.push_back(worker_it->second->conn);
                }
            }
            // Commit-confirmed restore protocol. Seed this checkpoint's
            // pending-confirmation set from the tracked tasks, and tell the
            // workers the retention floor: nothing at or above the newest
            // CONFIRMED checkpoint may be purged, because that is the
            // restore target while newer checkpoints sit completed but
            // unconfirmed. max(latest_confirmed, 1): before the first
            // confirmation NOTHING may be purged for a tracked job, and a
            // floor of 1 says exactly that; 0 keeps the untracked
            // behaviour byte-identical.
            if (!job.confirm_task_keys.empty()) {
                job.pending_confirms[ckpt_id] = job.confirm_task_keys;
                // Bounded: a job whose confirmations stopped arriving must
                // not grow this map for ever. 64 outstanding checkpoints is
                // far beyond any healthy commit latency.
                while (job.pending_confirms.size() > 64) {
                    job.pending_confirms.erase(job.pending_confirms.begin());
                }
                cc.retain_floor = std::max<std::uint64_t>(job.latest_confirmed_checkpoint_id, 1);
            }
            // Savepoints ride every commit, whether or not the job runs the
            // confirmed-restore protocol above: a pin that only reached the
            // workers under one protocol would be a pin that sometimes holds.
            cc.pinned_checkpoint_ids.assign(job.pinned_checkpoint_ids.begin(),
                                            job.pinned_checkpoint_ids.end());
            if (protocol_trace::enabled()) {
                protocol_trace::Event("Broadcast")
                    .u("job", jid)
                    .u("ckpt", ckpt_id)
                    .b("withheld", false)
                    .u("epoch", epoch())
                    .emit();
            }
        }
        const auto frame = fenced_frame_(MessageKind::CommitCheckpoint, cc);
        for (const auto& c : worker_conns)
            send_frame(*c, frame);
    }
    // Wake anyone waiting on latest_completed_checkpoint_id to advance
    // (take_savepoint, recovery probes, tests).
    cv_.notify_all();
}

void Coordinator::checkpoint_trigger_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        // Visit every job with periodic checkpointing enabled. Each job
        // wakes at its own interval - we use the minimum live interval
        // as the loop's sleep so we don't oversleep any job.
        std::chrono::milliseconds sleep_for{500};
        std::vector<
            std::tuple<JobId, std::uint64_t, std::uint64_t, std::uint8_t, std::vector<std::string>>>
            to_trigger;
        // Jobs whose next checkpoint id is not on record yet, with the id.
        // Their claimers start outside the lock (request_id_record_), so the
        // store write never runs on this thread and a slow or failing record
        // for one job holds nobody else's checkpoints.
        std::vector<std::pair<std::shared_ptr<JobState::TriggeredRecord>, std::uint64_t>> to_record;
        // Answers to end-of-input requests held until their id was on record.
        std::vector<std::pair<std::shared_ptr<network::Connection>, FinalCheckpointAssignedMsg>>
            final_replies;
        {
            std::lock_guard lock(mu_);
            for (auto& [jid, job_ptr] : jobs_) {
                auto& job = *job_ptr;
                // End-of-input requests that arrived before their id was on
                // record (handle_request_final_checkpoint_): answered once it
                // is, which is what the claimer's wake-up is for, and before a
                // periodic trigger below can take the id they wait for. Any
                // job with a checkpoint directory takes a final checkpoint,
                // periodic or not, and a stopping one declines.
                if (!answer_final_requests_locked_(job, final_replies)) {
                    to_record.emplace_back(job.triggered_record, job.next_checkpoint_id);
                }
                // Ahead of time, for every running job that may still allocate
                // an id, periodic or not: a savepoint, an end-of-input final id
                // and a cutover then normally find their id on record too. Not
                // before the job is up, so the record, like everything else in
                // the checkpoint directory, appears once the job is running.
                if (!job.checkpoint.checkpoint_dir.empty() && !job.completion_signalled &&
                    !job.final_checkpoint_id.has_value() && job.peer_updates_sent &&
                    job.triggered_record->owned.load(std::memory_order_acquire) <
                        job.next_checkpoint_id) {
                    to_record.emplace_back(job.triggered_record, job.next_checkpoint_id);
                }
                if (job.checkpoint.checkpoint_dir.empty() || job.checkpoint.interval_ms <= 0) {
                    continue;
                }
                if (job.completion_signalled) {
                    continue;
                }
                // Once a bounded source has reached EOS and the final
                // checkpoint id is assigned, stop firing periodic checkpoints:
                // a higher periodic id would otherwise overtake the lower final
                // id on the wire and stall its barrier alignment (and a bounded
                // job needs no further periodic checkpoint after its final one).
                if (job.final_checkpoint_id.has_value()) {
                    continue;
                }
                // Hold off triggering until peer updates have been
                // resolved. Before that the subtasks are not yet
                // running, so a barrier would arrive before any source
                // injectors are registered. With peer_updates_sent the
                // chain is at least up; the worker still queues triggers
                // that race the source's own startup.
                // A job whose topology is mid-swap must not be checkpointed
                // (F84 / follow-up 49). Participants are captured from
                // task_records at trigger, and a trigger issued now STRADDLES
                // the swap: the worker queues it against no-sources-yet and
                // replays it into the NEW generation's sources, which snapshot
                // an id the outgoing generation's ledger owns. With the
                // transition window held open by a fault point, that produced
                // 120+ out-of-participant-set snapshots per run.
                if (job.awaiting_restart) {
                    const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                    if (interval < sleep_for) {
                        sleep_for = interval;
                    }
                    continue;
                }
                // The checkpoint clock pauses for a hot cutover (design
                // record 008 rule 5): the cutover checkpoint is the last of
                // the old layout and its successor the first of the new. A
                // periodic trigger inside the window would checkpoint a
                // topology that is mid-swap.
                if (job.hot_cutover.has_value()) {
                    const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                    if (interval < sleep_for) {
                        sleep_for = interval;
                    }
                    continue;
                }
                if (!job.peer_updates_sent) {
                    const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                    if (interval < sleep_for) {
                        sleep_for = interval;
                    }
                    continue;
                }
                // Another coordinator of the job holds ids from this job's next
                // one on: number above them, and ask for the new next id at
                // once. Nothing is allocated before it is on record, so the
                // claim this costs is the only cost. Not before the job is up:
                // a takeover's Redeploy is recorded after its deploy returns,
                // and the renumber is a step from the running job.
                if (renumber_above_held_ids_locked_(job)) {
                    to_record.emplace_back(job.triggered_record, job.next_checkpoint_id);
                }
                const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                if (interval < sleep_for) {
                    sleep_for = interval;
                }
                // Is this job actually DUE? Shortening the loop's sleep to the
                // smallest configured interval only guarantees we never oversleep
                // a job; on its own it triggered every eligible job on every pass,
                // so a job asking for a 10s interval got one every 500ms - the
                // loop tick - along with twenty times the intended state writes
                // and transactional sink commits. The interval has to gate the
                // trigger, not just the sleep.
                const auto now = std::chrono::steady_clock::now();
                if (job.last_checkpoint_trigger_at != std::chrono::steady_clock::time_point{}) {
                    const auto since = now - job.last_checkpoint_trigger_at;
                    if (since < interval) {
                        // Not due. Wake when it is, so a long interval does not
                        // get rounded up to the next 500ms tick either.
                        const auto remaining =
                            std::chrono::duration_cast<std::chrono::milliseconds>(interval - since);
                        if (remaining < sleep_for) {
                            sleep_for = remaining;
                        }
                        continue;
                    }
                }
                // The id goes on record before its barrier can leave, so a
                // takeover numbers above it whether or not its capture landed
                // (claim_checkpoint_id_). Its claimer was asked for it when the
                // previous id was allocated and has normally landed it by now;
                // when it has not, nothing is allocated, the round waits for
                // the claim, which wakes this loop, and a record that cannot
                // be written leaves no checkpoint half begun.
                if (job.triggered_record->owned.load(std::memory_order_acquire) <
                    job.next_checkpoint_id) {
                    continue;
                }
                job.last_checkpoint_trigger_at = now;
                const auto next_id = job.next_checkpoint_id++;
                // The next id, claimed now so it is on record before it is due.
                to_record.emplace_back(job.triggered_record, job.next_checkpoint_id);
                if (protocol_trace::enabled()) {
                    protocol_trace::Event("Trigger")
                        .u("job", job.id)
                        .u("ckpt", next_id)
                        .u("epoch", epoch())
                        .emit();
                }
                std::unordered_set<std::string> pending;
                for (const auto& [key, _] : job.task_records) {
                    pending.insert(key);
                }
                job.pending_checkpoint_acks[next_id] = std::move(pending);
                {
                    // What this checkpoint consists of, for the COMPLETED marker. Captured at
                    // TRIGGER because the ack set above is drained as acks arrive.
                    auto& rec = job.checkpoint_participants[next_id];
                    rec.generation = job.state_generation;
                    auto& participants = rec.subtasks;
                    participants.clear();
                    for (const auto& [key, _unused] : job.task_records) {
                        const auto colon = key.rfind(':');
                        if (colon == std::string::npos) {
                            continue;
                        }
                        try {
                            participants.insert(
                                static_cast<std::uint32_t>(std::stoul(key.substr(colon + 1))));
                        } catch (const std::exception&) {
                            continue;
                        }
                    }
                }
                job.pending_checkpoint_start_times[next_id] = std::chrono::steady_clock::now();
                clink::metrics::ckpt::triggered();
                // Adaptive alignment: decide this trigger's barrier mode
                // from the last completed checkpoint's duration relative
                // to the interval (1.0 = the checkpoint took the whole
                // interval; alignment stalls under backpressure are
                // exactly what stretches it). Hysteresis lives in the
                // policy, so one slow checkpoint never flips the mode
                // and the decision cannot oscillate. Statically-aligned
                // jobs leave the byte 0 = not stamped.
                std::uint8_t barrier_mode_plus1 = 0;
                if (job.checkpoint.alignment == CheckpointAlignment::Adaptive) {
                    if (!job.adaptive_ckpt_policy) {
                        job.adaptive_ckpt_policy =
                            std::make_unique<clink::checkpoint::AdaptiveModePolicy>();
                    }
                    const double pressure = static_cast<double>(job.last_checkpoint_duration_ms) /
                                            static_cast<double>(job.checkpoint.interval_ms);
                    const auto before = job.adaptive_ckpt_policy->mode();
                    const auto decided = job.adaptive_ckpt_policy->observe(pressure);
                    if (decided != before) {
                        clink::metrics::ckpt::adaptive_switch();
                        log::info("coordinator.checkpoint",
                                  "job " + std::to_string(jid) +
                                      ": adaptive checkpoint mode switched to " +
                                      (decided == CheckpointBarrier::Mode::Unaligned ? "unaligned"
                                                                                     : "aligned") +
                                      " (pressure " + std::to_string(pressure) + ")");
                    }
                    barrier_mode_plus1 = decided == CheckpointBarrier::Mode::Unaligned ? 2 : 1;
                    clink::metrics::ckpt::mode_stamped(decided ==
                                                       CheckpointBarrier::Mode::Unaligned);
                } else {
                    clink::metrics::ckpt::mode_stamped(job.checkpoint.alignment ==
                                                       CheckpointAlignment::Unaligned);
                }
                std::vector<std::string> worker_ids;
                for (const auto& [worker_id, _] : job.tasks_by_worker) {
                    worker_ids.push_back(worker_id);
                }
                to_trigger.emplace_back(
                    jid, next_id, job.state_generation, barrier_mode_plus1, std::move(worker_ids));
            }
        }
        for (auto& [conn, reply] : final_replies) {
            if (conn) {
                send_frame(*conn, fenced_frame_(MessageKind::FinalCheckpointAssigned, reply));
            }
        }
        for (const auto& [jid, ckpt_id, gen, mode_plus1, worker_ids] : to_trigger) {
            TriggerCheckpointMsg m;
            m.job_id = jid;
            m.checkpoint_id = ckpt_id;
            m.generation = gen;
            m.barrier_mode_plus1 = mode_plus1;
            const auto frame = fenced_frame_(MessageKind::TriggerCheckpoint, m);
            for (const auto& worker_id : worker_ids) {
                std::shared_ptr<network::Connection> c;
                {
                    std::lock_guard lock(mu_);
                    auto it = registered_.find(worker_id);
                    if (it != registered_.end() && !it->second->lost && it->second->conn) {
                        c = it->second->conn;
                    }
                }
                if (c) {
                    send_frame(*c, frame);
                }
            }
        }
        for (const auto& [record, id] : to_record) {
            request_id_record_(record, id);
        }
        reap_id_recorders_();
        // A claim that lands wakes the loop early, so a job waiting on its id
        // triggers as soon as it is on record rather than a sleep later.
        std::unique_lock wake_lock(trigger_wake_mu_);
        trigger_wake_cv_.wait_for(wake_lock, sleep_for, [this] {
            return trigger_wake_ || stop_.load(std::memory_order_acquire);
        });
        trigger_wake_ = false;
    }
}

bool Coordinator::claim_checkpoint_id_(JobState::TriggeredRecord& record, std::uint64_t id) {
    if (record.checkpoint_dir.empty() || record.owned.load(std::memory_order_acquire) >= id) {
        return true;
    }
    // One writer per job at a time, so the store's compare-and-set refuses
    // only another process's claim, and an id that two paths want at once is
    // claimed once.
    std::lock_guard lock(record.write_mu);
    if (record.owned.load(std::memory_order_acquire) >= id) {
        return true;
    }
    if (record.held_elsewhere.load(std::memory_order_acquire) >= id) {
        return false;
    }
    // Under this coordinator's own claimant, so a retry that finds an earlier
    // attempt's write on record, its answer lost, takes the id as claimed
    // rather than as held elsewhere.
    const auto claim =
        record_triggered_id(record.checkpoint_dir, record.job_id, id, record.claimant);
    if (claim.claimed) {
        // Claims are only ever for the job's next id, and nothing is
        // allocated before it is owned, so every id between the last owned
        // one and this was skipped by a renumber and is never allocated.
        record.owned.store(id, std::memory_order_release);
        return true;
    }
    auto held = record.held_elsewhere.load(std::memory_order_acquire);
    while (held < claim.on_record &&
           !record.held_elsewhere.compare_exchange_weak(held, claim.on_record)) {
    }
    return false;
}

bool Coordinator::renumber_above_held_ids_locked_(JobState& job) {
    const auto held = job.triggered_record->held_elsewhere.load(std::memory_order_acquire);
    if (held < job.next_checkpoint_id) {
        return false;
    }
    // A claim on this job's next id was refused because the record already
    // held it: another coordinator of the job claimed ids up to `held`. A
    // claim of this coordinator's own whose write landed and whose answer was
    // lost is not among them; the record names its claimant, and the retry
    // takes it as claimed (record_triggered_id). Two coordinators trigger one
    // job when one of them has been superseded and its trigger loop runs on,
    // and this one may be either: the leader, whose takeover numbered above
    // the record as it stood then, or the superseded one, which the leader
    // has since claimed past. A worker still bound to the superseded one's
    // epoch accepts its barriers, so an id both used would put two captures
    // at one path. The ids may also be an earlier run's of the same job in
    // this directory, when this run could not read the record at deploy and
    // so began claiming from 1 (deploy_internal_). Either way, number above
    // every id on record.
    job.next_checkpoint_id = held + 1;
    log::warn("coordinator.checkpoint",
              "job_id=" + std::to_string(job.id) + ": checkpoint ids up to " +
                  std::to_string(held) +
                  " are already claimed on this job's record by another coordinator of it (one "
                  "superseded and still triggering, the leader that superseded this one, or an "
                  "earlier run in this checkpoint directory whose record could not be read at "
                  "deploy); numbering from " +
                  std::to_string(job.next_checkpoint_id));
    if (protocol_trace::enabled()) {
        protocol_trace::Event("Renumber")
            .u("job", job.id)
            .u("next", job.next_checkpoint_id)
            .u("epoch", epoch())
            .emit();
    }
    return true;
}

namespace {
// A failing claim is retried after this delay, doubled up to the cap on every
// failure in a row: the store gets a breather, and the job's checkpoints
// resume within the cap of the record becoming writable again.
constexpr std::chrono::milliseconds kIdRecordRetryFirst{100};
constexpr std::chrono::milliseconds kIdRecordRetryCap{2000};
}  // namespace

void Coordinator::request_id_record_(const std::shared_ptr<JobState::TriggeredRecord>& record,
                                     std::uint64_t id) {
    if (record->checkpoint_dir.empty()) {
        return;
    }
    {
        std::lock_guard lock(record->mu);
        if (record->retired || record->stopping || stop_.load(std::memory_order_acquire)) {
            return;
        }
        record->wanted = std::max(record->wanted, id);
        if (record->started) {
            record->cv.notify_one();
            return;
        }
    }
    // The job's first request starts its claimer, which then stays parked
    // between claims until the job ends: one thread for the job's life, not
    // one per checkpoint. Started and registered in one step under
    // id_recorders_mu_, so stop() either finds the claimer to join or this
    // sees the coordinator stopping and starts none.
    std::lock_guard recorders_lock(id_recorders_mu_);
    std::lock_guard lock(record->mu);
    if (record->started || record->retired || record->stopping ||
        stop_.load(std::memory_order_acquire)) {
        record->cv.notify_one();
        return;
    }
    record->started = true;
    record->thread = std::thread([this, record] { id_record_loop_(record); });
    id_recorders_.push_back(record);
    id_claimers_started_.fetch_add(1, std::memory_order_relaxed);
}

void Coordinator::id_record_loop_(const std::shared_ptr<JobState::TriggeredRecord>& record) {
    auto& rec = *record;
    std::unique_lock lock(rec.mu);
    for (;;) {
        if (rec.retired || rec.stopping) {
            break;
        }
        const auto id = rec.wanted;
        if (rec.owned.load(std::memory_order_acquire) >= id ||
            rec.held_elsewhere.load(std::memory_order_acquire) >= id) {
            // Caught up, by this claimer or by a savepoint or a cutover
            // claiming for itself; or the record answered with ids another
            // coordinator holds. Either way the store is answering. Parked
            // until a later id is wanted, the job ends or the coordinator
            // stops, each of which notifies under `mu`.
            if (rec.failing.exchange(false, std::memory_order_relaxed)) {
                log::info("coordinator.checkpoint",
                          "job_id=" + std::to_string(rec.job_id) +
                              " checkpoint ids can be recorded again; checkpoints resume");
            }
            rec.cv.wait(lock);
            continue;
        }
        if (std::chrono::steady_clock::now() < rec.retry_at) {
            rec.cv.wait_until(lock, rec.retry_at);
            continue;
        }
        lock.unlock();
        std::string failure;
        try {
            (void)claim_checkpoint_id_(rec, id);
        } catch (const std::exception& e) {
            failure = e.what();
        }
        if (failure.empty()) {
            // A landed claim releases a trigger that is due, and an end-of-input
            // request held for the id; a refused one means a renumber, which the
            // trigger loop makes under its lock.
            wake_trigger_loop_();
        }
        lock.lock();
        // Once per outage, and only for a job still running: the claim is
        // retried with a growing delay for as long as the store refuses, and
        // each retry is quiet. A job that ended while its claim was out
        // needs the id no longer.
        if (!failure.empty() && !rec.retired && !rec.stopping &&
            !rec.failing.exchange(true, std::memory_order_relaxed)) {
            log::error("coordinator.checkpoint",
                       "job_id=" + std::to_string(rec.job_id) + " could not record checkpoint id " +
                           std::to_string(id) + " in " + rec.checkpoint_dir + " (" + failure +
                           "); checkpoints are held until it can, because no barrier may leave "
                           "before its id is on record");
        }
        if (failure.empty()) {
            rec.backoff = std::chrono::milliseconds{0};
            rec.retry_at = {};
        } else {
            rec.backoff = rec.backoff.count() == 0 ? kIdRecordRetryFirst
                                                   : std::min(rec.backoff * 2, kIdRecordRetryCap);
            rec.retry_at = std::chrono::steady_clock::now() + rec.backoff;
        }
    }
    // Its last act under `mu`: reap_id_recorders_ joins only a claimer that
    // has set this, so the join returns at once.
    rec.exited = true;
}

void Coordinator::reap_id_recorders_() {
    std::lock_guard lock(id_recorders_mu_);
    std::erase_if(id_recorders_, [](const std::shared_ptr<JobState::TriggeredRecord>& record) {
        std::thread finished;
        {
            std::lock_guard rec_lock(record->mu);
            if (!record->exited) {
                return false;
            }
            finished = std::move(record->thread);
        }
        if (finished.joinable()) {
            finished.join();
        }
        return true;
    });
}

void Coordinator::wake_trigger_loop_() {
    {
        std::lock_guard lock(trigger_wake_mu_);
        trigger_wake_ = true;
    }
    trigger_wake_cv_.notify_all();
}

bool assign_task_placement(std::vector<PlannedTask>& tasks, std::vector<PlacementWorker>& workers) {
    // Deterministic worker order: the coordinator's registry is an unordered_map whose order is
    // neither sorted nor stable across processes, which made placement - and any benchmark of
    // it - unrepeatable.
    std::sort(
        workers.begin(), workers.end(), [](const PlacementWorker& a, const PlacementWorker& b) {
            return a.worker_id < b.worker_id;
        });

    // Last resort for an instance no single worker can hold.
    const auto place_one = [&workers](PlannedTask& t) {
        for (auto& w : workers) {
            if (w.free_slots > 0) {
                t.worker_id = w.worker_id;
                --w.free_slots;
                return true;
            }
        }
        return false;
    };

    // Group by subtask index, in first-seen order so the result is a function of the plan
    // rather than of a hash order.
    std::vector<std::uint32_t> group_order;
    std::unordered_map<std::uint32_t, std::vector<PlannedTask*>> groups;
    for (auto& t : tasks) {
        if (!t.worker_id.empty()) {
            continue;  // caller pinned it
        }
        if (groups.find(t.subtask_idx) == groups.end()) {
            group_order.push_back(t.subtask_idx);
        }
        groups[t.subtask_idx].push_back(&t);
    }
    if (group_order.empty()) {
        return true;
    }
    if (workers.empty()) {
        return false;
    }

    bool all_placed = true;
    std::size_t rr = 0;
    for (const auto idx : group_order) {
        auto& group = groups[idx];
        const auto need = static_cast<std::uint32_t>(group.size());
        PlacementWorker* picked = nullptr;
        for (std::size_t step = 0; step < workers.size(); ++step) {
            auto& w = workers[(rr + step) % workers.size()];
            if (w.free_slots >= need) {
                picked = &w;
                rr = (rr + step + 1) % workers.size();
                break;
            }
        }
        if (picked != nullptr) {
            for (auto* t : group) {
                t->worker_id = picked->worker_id;
            }
            picked->free_slots -= need;
            continue;
        }
        for (auto* t : group) {
            if (!place_one(*t)) {
                all_placed = false;
            }
        }
    }
    return all_placed;
}

}  // namespace clink::cluster
