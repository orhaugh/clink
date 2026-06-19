#include "clink/cluster/job_manager.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/cluster/restore_compat_gate.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/runtime/event_bus.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/network_socket.hpp"

namespace clink::cluster {

// Out-of-line so the unique_ptr<JobBundle> field can hold a
// forward-declared type in the header; the .cpp pulls in
// job_bundle.hpp so JobBundle is complete here.
JobManager::JobState::JobState() = default;
JobManager::JobState::~JobState() = default;
JobManager::JobState::JobState(JobState&&) noexcept = default;
JobManager::JobState& JobManager::JobState::operator=(JobState&&) noexcept = default;

namespace {

// JSON-quote a string for embedding inside event payloads. Used for
// EventBus payloads where the only user-controlled inputs are tm_ids
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

// Read one length-prefixed frame from a Connection. Returns the payload
// without the 4-byte length header. Empty optional on connection close.
std::optional<std::vector<std::byte>> read_frame(network::Connection& conn) {
    std::array<std::byte, 4> hdr{};
    if (!conn.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (int i = 0; i < 4; ++i) {
        len = (len << 8) | static_cast<unsigned char>(hdr[i]);
    }
    if (len == 0) {
        return std::vector<std::byte>{};
    }
    std::vector<std::byte> body(len);
    if (!conn.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

bool send_frame(network::Connection& conn, const std::vector<std::byte>& frame) {
    return conn.send_all(frame.data(), frame.size());
}

}  // namespace

namespace {

// Default plain-TCP accept factory: block on accept_one, wrap the
// accepted client fd in a PlainTcpConnection. TLS callers replace
// this via set_accept_factory.
std::unique_ptr<network::Connection> default_accept_factory(int listener_fd) {
    const int fd = network::NetworkSocket::accept_one(listener_fd);
    if (fd < 0)
        return nullptr;
    return network::make_plain_connection(fd);
}

}  // namespace

JobManager::JobManager() {
    accept_factory_ = default_accept_factory;
}

JobManager::JobManager(Config cfg) : cfg_(cfg) {
    if (cfg_.advertise_host.empty()) {
        cfg_.advertise_host = cfg_.bind_host;
    }
    accept_factory_ = default_accept_factory;
}

void JobManager::set_accept_factory(AcceptFactory f) {
    accept_factory_ = std::move(f);
}

void JobManager::set_autoscaler_sample_fn(AutoscalerSampleFn fn) {
    autoscaler_sample_fn_ = std::move(fn);
}

std::optional<std::uint64_t> JobManager::autoscaler_ticks(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || !it->second->autoscaler) {
        return std::nullopt;
    }
    return it->second->autoscaler->ticks();
}

void JobManager::stop_autoscalers_() {
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

void JobManager::set_ha_dir(std::string dir) {
    ha_dir_ = std::move(dir);
    if (!ha_dir_.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path{ha_dir_} / "jobs", ec);
        std::filesystem::create_directories(std::filesystem::path{ha_dir_} / "history", ec);
        reload_history_from_disk_();
    }
}

namespace {

// Atomic-rename file write. Avoids a reader (or standby JM) seeing a
// partial JSON / .so file mid-write.
bool atomic_write_file(const std::filesystem::path& path, const std::string& body) {
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    const auto tmp = path.string() + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out)
            return false;
        out.write(body.data(), static_cast<std::streamsize>(body.size()));
        if (!out)
            return false;
    }
    std::filesystem::rename(tmp, path, ec);
    return !ec;
}

}  // namespace

// Serialize a terminal-state record to <ha_dir>/history/<job_id>.json.
// One file per job mirrors HistoryServer archive layout and
// avoids needing a DB. Atomic-rename so a partially-written file is
// never observed by reload.
void persist_history_record_(const std::string& ha_dir, const CompletedJobRecord& rec) {
    if (ha_dir.empty())
        return;
    const auto history_dir = std::filesystem::path{ha_dir} / "history";
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
    body += "}";
    atomic_write_file(history_dir / (std::to_string(rec.job_id) + ".json"), body);
}

void persist_job_manifest_(const std::string& ha_dir,
                           JobId job_id,
                           const std::string& graph_json,
                           const std::vector<PluginBinary>& plugins,
                           const CheckpointConfig& checkpoint) {
    if (ha_dir.empty())
        return;
    const auto job_dir = std::filesystem::path{ha_dir} / "jobs" / std::to_string(job_id);
    std::error_code ec;
    std::filesystem::create_directories(job_dir, ec);
    // Plugin bytes: idempotent by content-hash. Same hash = same
    // bytes, so re-writes are no-ops; cheaper to skip than to re-
    // verify content.
    for (const auto& p : plugins) {
        const auto path = job_dir / ("plugin-" + p.content_hash + ".so");
        if (std::filesystem::exists(path))
            continue;
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (out) {
            out.write(reinterpret_cast<const char*>(p.bytes.data()),
                      static_cast<std::streamsize>(p.bytes.size()));
        }
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
    manifest +=
        ",\"restore_from_checkpoint_id\":" + std::to_string(checkpoint.restore_from_checkpoint_id);
    manifest +=
        ",\"max_restarts_on_tm_loss\":" + std::to_string(checkpoint.max_restarts_on_tm_loss);
    manifest += ",\"plugins\":[";
    for (std::size_t i = 0; i < plugins.size(); ++i) {
        if (i > 0)
            manifest += ",";
        manifest += "{\"name\":" + q(plugins[i].name) +
                    ",\"content_hash\":" + q(plugins[i].content_hash) + "}";
    }
    manifest += "]}";
    atomic_write_file(job_dir / "manifest.json", manifest);
}

namespace {

// Hand-roll the "find latest COMPLETED-N marker under <ckpt_dir>/
// <job_id>/" lookup. JM's existing latest_completed_checkpoint_id is
// updated in-memory but NOT in the file system before a crash.
std::uint64_t latest_completed_id_on_disk(const std::string& checkpoint_dir, JobId job_id) {
    if (checkpoint_dir.empty())
        return 0;
    const auto job_dir = std::filesystem::path{checkpoint_dir} / std::to_string(job_id);
    if (!std::filesystem::exists(job_dir))
        return 0;
    std::uint64_t latest = 0;
    for (const auto& e : std::filesystem::directory_iterator(job_dir)) {
        if (!e.is_regular_file())
            continue;
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0)
            continue;
        try {
            const auto id = std::stoull(name.substr(std::string{"COMPLETED-"}.size()));
            if (id > latest)
                latest = id;
        } catch (...) {
        }
    }
    return latest;
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

void JobManager::recover_persisted_jobs() {
    if (ha_dir_.empty())
        return;
    const auto jobs_root = std::filesystem::path{ha_dir_} / "jobs";
    if (!std::filesystem::exists(jobs_root))
        return;
    std::vector<JobId> ids;
    for (const auto& e : std::filesystem::directory_iterator(jobs_root)) {
        if (!e.is_directory())
            continue;
        try {
            ids.push_back(static_cast<JobId>(std::stoull(e.path().filename().string())));
        } catch (...) {
        }
    }
    std::sort(ids.begin(), ids.end());
    for (auto job_id : ids) {
        // Skip if already in jobs_ (idempotent on repeated takeover).
        {
            std::lock_guard lock(mu_);
            if (jobs_.count(job_id) != 0)
                continue;
        }
        const auto job_dir = jobs_root / std::to_string(job_id);
        const auto manifest_path = job_dir / "manifest.json";
        std::ifstream in(manifest_path);
        if (!in)
            continue;
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        const auto graph_json = read_string_field(body, "graph_json");
        if (graph_json.empty())
            continue;
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = read_string_field(body, "checkpoint_dir");
        ckpt.state_backend_uri = read_string_field(body, "state_backend_uri");
        ckpt.interval_ms = static_cast<std::int64_t>(read_uint_field(body, "interval_ms"));
        ckpt.restore_from_dir = read_string_field(body, "restore_from_dir");
        ckpt.restore_from_checkpoint_id = read_uint_field(body, "restore_from_checkpoint_id");
        ckpt.max_restarts_on_tm_loss =
            static_cast<std::uint32_t>(read_uint_field(body, "max_restarts_on_tm_loss"));
        // For recovery, always restore from this job's checkpoint dir
        // at the latest COMPLETED-N marker the previous leader managed
        // to write.
        if (!ckpt.checkpoint_dir.empty()) {
            const auto latest = latest_completed_id_on_disk(ckpt.checkpoint_dir, job_id);
            ckpt.restore_from_dir = ckpt.checkpoint_dir;
            ckpt.restore_from_checkpoint_id = latest;
        }
        // Plugins: scan plugin-*.so files in this job dir, load each
        // into a fresh JobBundle.
        std::vector<PluginBinary> plugins;
        for (const auto& f : std::filesystem::directory_iterator(job_dir)) {
            if (!f.is_regular_file())
                continue;
            const auto name = f.path().filename().string();
            if (name.rfind("plugin-", 0) != 0 || name.size() < 11)
                continue;
            plugins.push_back(make_plugin_binary_from_file(f.path().string(), name));
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
                log::warn("jm.ha",
                          "plugin '" + p.name + "' failed to recover: " + load_result.error);
                plugins_ok = false;
                break;
            }
            plugin_so_paths.push_back(path);
        }
        if (!plugins_ok)
            continue;
        // Schema-evolution D: skip recovering a job whose persisted
        // savepoint can't migrate to the (possibly newer) binary's
        // expected versions. Best-effort, same contract as the submit gate.
        if (auto reject = check_restore_compatibility_via_plugins(
                plugin_so_paths, ckpt.restore_from_dir, ckpt.restore_from_checkpoint_id);
            !reject.empty()) {
            log::warn("jm.ha",
                      "recovery skipped for job_id=" + std::to_string(job_id) + ": " + reject);
            continue;
        }
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
            (void)submit_job(graph,
                             OperatorRegistry::default_instance(),
                             std::move(plugins),
                             ckpt,
                             std::move(bundle),
                             /*notify_client_conn=*/nullptr);
            clink::metrics::orch::ha_recovered_jobs_inc();
            log::info("jm.ha",
                      "recovered job_id=" + std::to_string(job_id) +
                          " restore_from_ckpt=" + std::to_string(ckpt.restore_from_checkpoint_id));
        } catch (const std::exception& e) {
            log::warn("jm.ha",
                      "recovery failed for job_id=" + std::to_string(job_id) + ": " + e.what());
        }
    }
}

void JobManager::reload_history_from_disk_() {
    if (ha_dir_.empty())
        return;
    const auto history_dir = std::filesystem::path{ha_dir_} / "history";
    if (!std::filesystem::exists(history_dir))
        return;
    std::vector<CompletedJobRecord> records;
    for (const auto& e : std::filesystem::directory_iterator(history_dir)) {
        if (!e.is_regular_file())
            continue;
        if (e.path().extension() != ".json")
            continue;
        std::ifstream in(e.path());
        if (!in)
            continue;
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
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
        while (history_.size() > kJobManagerHistoryCap) {
            history_.pop_front();
        }
    }
}

JobManager::~JobManager() {
    stop();
}

std::uint16_t JobManager::start(std::uint16_t port) {
    listener_fd_ = network::NetworkSocket::listen_on(port, cfg_.bind_host);
    if (listener_fd_ < 0) {
        throw std::runtime_error("JobManager::start: listen failed");
    }
    bound_port_ = port;
    if (cfg_.advertise_host.empty()) {
        cfg_.advertise_host = cfg_.bind_host;
    }
    accept_thread_ = std::thread([this] { accept_loop_(); });
    watchdog_thread_ = std::thread([this] { watchdog_loop_(); });
    checkpoint_thread_ = std::thread([this] { checkpoint_trigger_loop_(); });
    return bound_port_;
}

void JobManager::accept_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        // Factory does accept_one + any TLS handshake. On listener
        // shutdown, accept_one returns -1 → factory returns nullptr →
        // we exit. On a TLS handshake failure, the factory throws;
        // catch so one bad client can't kill the accept loop.
        std::unique_ptr<network::Connection> conn;
        try {
            conn = accept_factory_(listener_fd_);
        } catch (const std::exception& e) {
            log::warn("jm.accept", std::string{"connection rejected: "} + e.what());
            continue;
        }
        if (!conn) {
            if (stop_.load(std::memory_order_acquire))
                return;
            continue;  // transient: malformed handshake, peer disappeared
        }
        if (!handle_first_frame_(std::move(conn))) {
            continue;  // connection ended (rejected client / bad frame)
        }
    }
}

bool JobManager::handle_first_frame_(std::unique_ptr<network::Connection> conn) {
    auto frame = read_frame(*conn);
    if (!frame.has_value()) {
        return false;  // conn destructor closes
    }
    MessageReader r(std::move(*frame));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    if (kind == MessageKind::Register) {
        handle_register_(std::move(conn), r);
        return true;
    }
    if (kind == MessageKind::HelloClient) {
        // Client connection: spawn a per-client thread that reads
        // SubmitJob frames and writes acks/completions. shared_ptr
        // ownership lets stop() safely call shutdown_read() even if
        // the handler thread has already exited and dropped its share.
        std::shared_ptr<network::Connection> shared_conn(conn.release());
        std::lock_guard lock(client_mu_);
        client_conns_.push_back(shared_conn);
        client_threads_.emplace_back([this, shared_conn] { handle_client_loop_(shared_conn); });
        return true;
    }
    // Protocol violation - drop the connection.
    return false;
}

void JobManager::handle_register_(std::unique_ptr<network::Connection> conn, MessageReader& r) {
    auto reg = decode_register(r);

    auto tm = std::make_shared<TmConnection>();
    tm->tm_id = reg.tm_id;
    tm->data_host = reg.data_host;
    tm->conn = std::move(conn);
    tm->last_seen = std::chrono::steady_clock::now();
    tm->slot_capacity = reg.slot_count == 0 ? std::uint32_t{1} : reg.slot_count;
    tm->http_port = reg.http_port;

    // Send RegisterAck.
    const auto ack =
        encode_frame(MessageKind::RegisterAck, RegisterAckMsg{.ok = true, .message = ""});
    if (!send_frame(*tm->conn, ack)) {
        return;
    }

    {
        std::lock_guard lock(mu_);
        registered_[reg.tm_id] = tm;
    }
    cv_.notify_all();

    metrics::jm::tm_registered(tm->slot_capacity);
    log::info("jm.register",
              "tm=" + tm->tm_id + " host=" + tm->data_host +
                  " slots=" + std::to_string(tm->slot_capacity) +
                  " http_port=" + std::to_string(tm->http_port));
    events::publish("jm.tm_registered",
                    "{\"tm_id\":" + js_quote(tm->tm_id) +
                        ",\"data_host\":" + js_quote(tm->data_host) +
                        ",\"slots\":" + std::to_string(tm->slot_capacity) +
                        ",\"http_port\":" + std::to_string(tm->http_port) + "}");

    start_reader_for_(tm);
}

void JobManager::handle_client_loop_(std::shared_ptr<network::Connection> conn) {
    auto* conn_raw = conn.get();
    while (!stop_.load(std::memory_order_acquire)) {
        auto frame = read_frame(*conn);
        if (!frame.has_value()) {
            // Client closed. The job, if still in flight, continues -
            // we just lose the ability to push JobCompleted back.
            std::lock_guard lock(mu_);
            for (auto& [_, job] : jobs_) {
                if (job->notify_client_conn == conn_raw) {
                    job->notify_client_conn = nullptr;
                }
            }
            return;  // conn destructor closes
        }
        MessageReader r(std::move(*frame));
        const auto kind = static_cast<MessageKind>(r.read_u8());
        if (kind == MessageKind::SubmitJob) {
            handle_submit_(*conn, r);
            continue;
        }
        if (kind == MessageKind::ListJobs) {
            handle_list_jobs_(*conn);
            continue;
        }
        if (kind == MessageKind::CancelJob) {
            handle_cancel_job_(*conn, r);
            continue;
        }
        if (kind == MessageKind::RescaleOperator) {
            handle_rescale_operator_(*conn, r);
        } else if (kind == MessageKind::RescaleJob) {
            handle_rescale_job_(*conn, r);
            continue;
        }
        if (kind == MessageKind::Savepoint) {
            handle_savepoint_(*conn, r);
            continue;
        }
        // Unknown frame from a client - drop the connection.
        return;
    }
}

ClusterSnapshot JobManager::snapshot_cluster() const {
    ClusterSnapshot s;
    s.bind_host = cfg_.bind_host;
    s.advertise_host = cfg_.advertise_host;
    s.control_port = bound_port_;
    std::lock_guard lock(mu_);
    for (const auto& [tm_id, tm] : registered_) {
        TmSummary ts;
        ts.tm_id = tm->tm_id;
        ts.data_host = tm->data_host;
        ts.slot_capacity = tm->slot_capacity;
        ts.slots_in_use = tm->slots_in_use;
        ts.lost = tm->lost;
        ts.http_port = tm->http_port;
        if (!tm->lost) {
            s.total_slot_capacity += tm->slot_capacity;
            s.slots_in_use += tm->slots_in_use;
        }
        s.task_managers.push_back(std::move(ts));
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

std::vector<TmSummary> JobManager::snapshot_tms() const {
    std::vector<TmSummary> out;
    std::lock_guard lock(mu_);
    out.reserve(registered_.size());
    for (const auto& [_, tm] : registered_) {
        TmSummary ts;
        ts.tm_id = tm->tm_id;
        ts.data_host = tm->data_host;
        ts.slot_capacity = tm->slot_capacity;
        ts.slots_in_use = tm->slots_in_use;
        ts.lost = tm->lost;
        ts.http_port = tm->http_port;
        out.push_back(std::move(ts));
    }
    return out;
}

std::vector<JobSummary> JobManager::snapshot_jobs() const {
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
        out.push_back(std::move(s));
    }
    return out;
}

std::optional<JobDetail> JobManager::snapshot_job(JobId job_id) const {
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
    for (const auto& [tm_id, tasks] : job.tasks_by_tm) {
        for (const auto& t : tasks) {
            JobTaskRecord r;
            r.role = t.role;
            r.subtask_idx = t.subtask_idx;
            r.tm_id = tm_id;
            d.tasks.push_back(std::move(r));
        }
    }
    d.latest_completed_checkpoint_id = job.latest_completed_checkpoint_id;
    for (const auto& [ckpt_id, _] : job.pending_checkpoint_acks) {
        d.pending_checkpoint_ids.push_back(ckpt_id);
    }
    return d;
}

std::optional<std::pair<std::string, std::uint16_t>> JobManager::tm_http_target(
    const std::string& tm_id) const {
    std::lock_guard lock(mu_);
    auto it = registered_.find(tm_id);
    if (it == registered_.end()) {
        return std::nullopt;
    }
    const auto& tm = *it->second;
    if (tm.lost || tm.http_port == 0) {
        return std::nullopt;
    }
    return std::make_pair(tm.data_host, tm.http_port);
}

std::vector<std::pair<std::string, std::uint16_t>> JobManager::tms_hosting_job(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto job_it = jobs_.find(job_id);
    if (job_it == jobs_.end()) {
        return {};
    }
    std::vector<std::pair<std::string, std::uint16_t>> out;
    std::unordered_set<std::string> seen;
    for (const auto& [tm_id, _tasks] : job_it->second->tasks_by_tm) {
        if (!seen.insert(tm_id).second) {
            continue;
        }
        auto reg_it = registered_.find(tm_id);
        if (reg_it == registered_.end()) {
            continue;
        }
        const auto& tm = *reg_it->second;
        if (tm.lost || tm.http_port == 0) {
            continue;
        }
        out.emplace_back(tm.data_host, tm.http_port);
    }
    return out;
}

std::uint64_t JobManager::topology_version(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return 0;
    }
    return it->second->topology_version;
}

std::optional<JobManager::RouteTarget> JobManager::route_key_for_job(
    JobId job_id, const std::string& role, std::span<const std::byte> key_bytes) const {
    const auto kg = key_group_for_key(key_bytes);
    std::lock_guard lock(mu_);
    auto job_it = jobs_.find(job_id);
    if (job_it == jobs_.end()) {
        return std::nullopt;
    }
    for (const auto& [tm_id, tasks] : job_it->second->tasks_by_tm) {
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
            auto reg_it = registered_.find(tm_id);
            if (reg_it == registered_.end()) {
                continue;
            }
            const auto& tm = *reg_it->second;
            if (tm.lost || tm.http_port == 0) {
                continue;
            }
            return RouteTarget{tm.data_host, tm.http_port, task.subtask_idx};
        }
    }
    return std::nullopt;
}

CancelJobAckMsg JobManager::cancel_job(JobId job_id) {
    CancelJobAckMsg ack;
    ack.job_id = job_id;
    // Collect TM connections inside the lock, fan out outside. A blocked
    // send on one peer must not stall every other client / TM holding mu_
    // (heartbeats, SubtaskFinished, ...).
    std::vector<network::Connection*> tm_conns;
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
            for (const auto& [tm_id, _] : it->second->tasks_by_tm) {
                auto tm_it = registered_.find(tm_id);
                if (tm_it != registered_.end() && !tm_it->second->lost && tm_it->second->conn) {
                    tm_conns.push_back(tm_it->second->conn.get());
                }
            }
            ack.ok = true;
            ack.message = "cancel broadcast to " + std::to_string(tm_conns.size()) + " TM(s)";
        }
    }
    if (ack.ok) {
        CancelJobMsg cj;
        cj.job_id = job_id;
        const auto frame = encode_frame(MessageKind::CancelJob, cj);
        for (auto* c : tm_conns) {
            send_frame(*c, frame);
        }
    }
    return ack;
}

void JobManager::handle_cancel_job_(network::Connection& conn, MessageReader& r) {
    const auto req = decode_cancel_job(r);
    const auto ack = cancel_job(req.job_id);
    send_frame(conn, encode_frame(MessageKind::CancelJobAck, ack));
}

RescaleJobAckMsg JobManager::rescale_job(
    JobId job_id, const std::unordered_map<std::string, std::uint32_t>& role_p) {
    RescaleJobAckMsg ack;
    ack.job_id = job_id;

    std::vector<network::Connection*> tm_conns;
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
        // Sum free slots across alive TMs. Pre-rescale tasks still
        // hold their slots; restart_job_locked_ runs AFTER the drain,
        // so by the time it claims slots they've been freed. The
        // check here only matters when the rescale net-grows slot
        // usage. Scale-down frees slots and so always fits.
        if (slot_delta > 0) {
            std::size_t total_free = 0;
            for (const auto& [_, tm] : registered_) {
                if (!tm->lost && tm->slot_capacity > tm->slots_in_use) {
                    total_free += (tm->slot_capacity - tm->slots_in_use);
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
        job.awaiting_restart = true;
        // Bound the rescale drain the same way the TM-loss path does: a
        // survivor that hangs while still heartbeating would otherwise wedge
        // the job in awaiting_restart forever (the watchdog deadline scan is
        // gated on a non-epoch restart_deadline). Set it whenever the job
        // enters awaiting_restart, per the field's contract.
        job.restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
        for (const auto& [tm_id, pending] : job.pending_per_tm) {
            for (const auto& [role, sub] : pending) {
                job.restart_drain_expected.insert(role + ":" + std::to_string(sub));
            }
        }
        // Empty drain (no in-flight subtasks): fire restart immediately.
        // Typical case is mid-stream, so we just collect TM conns for
        // the cancel broadcast and let the existing drain do its work.
        for (const auto& [tm_id, _] : job.tasks_by_tm) {
            auto tm_it = registered_.find(tm_id);
            if (tm_it != registered_.end() && !tm_it->second->lost && tm_it->second->conn) {
                tm_conns.push_back(tm_it->second->conn.get());
            }
        }

        log::info("jm.rescale",
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
    const auto frame = encode_frame(MessageKind::CancelJob, cj);
    for (auto* c : tm_conns) {
        send_frame(*c, frame);
    }

    ack.ok = true;
    ack.message =
        "rescale initiated; draining " + std::to_string(tm_conns.size()) + " TM connection(s)";
    return ack;
}

void JobManager::handle_rescale_job_(network::Connection& conn, MessageReader& r) {
    auto req = decode_rescale_job(r);
    std::unordered_map<std::string, std::uint32_t> role_p;
    role_p.reserve(req.role_parallelism.size());
    for (const auto& [role, p] : req.role_parallelism) {
        role_p[role] = p;
    }
    const auto ack = rescale_job(req.job_id, role_p);
    send_frame(conn, encode_frame(MessageKind::RescaleJobAck, ack));
}

void JobManager::handle_rescale_operator_(network::Connection& conn, MessageReader& r) {
    auto req = decode_rescale_operator(r);
    const auto result = request_operator_rescale(req.job_id, req.op_id, req.new_parallelism);
    RescaleOperatorAckMsg ack;
    ack.job_id = req.job_id;
    ack.ok = result.ok;
    ack.accepted_target = result.accepted_target;
    ack.message = result.reason;
    send_frame(conn, encode_frame(MessageKind::RescaleOperatorAck, ack));
}

RescaleCoordinator::RequestResult JobManager::request_operator_rescale(
    JobId job_id, const std::string& op_id, std::uint32_t new_parallelism) {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return RescaleCoordinator::RequestResult{.ok = false, .reason = "unknown job_id"};
    }
    auto& job = *it->second;
    if (!job.rescale_coordinator) {
        return RescaleCoordinator::RequestResult{
            .ok = false, .reason = "job has no rescale coordinator (Phase 29 not enabled)"};
    }
    // A rescale advances Preparing -> Draining only when a checkpoint
    // lands (mark_checkpoint_ready, driven by the periodic-checkpoint
    // loop, which is itself gated on checkpoint_dir + interval_ms). With
    // no periodic checkpointing no checkpoint ever lands, so the rescale
    // would sit in Preparing indefinitely. Fail fast with a clear reason
    // instead of hanging silently. Uses the same predicate as the
    // periodic-checkpoint loop so the two cannot disagree.
    if (job.checkpoint.checkpoint_dir.empty() || job.checkpoint.interval_ms <= 0) {
        return RescaleCoordinator::RequestResult{
            .ok = false,
            .reason =
                "operator rescale requires periodic checkpointing (set checkpoint_dir and "
                "interval_ms > 0); without it the rescale would wait in Preparing forever "
                "for a checkpoint that never lands"};
    }
    return job.rescale_coordinator->request_rescale(op_id, new_parallelism);
}

std::optional<OperatorRescaleStatus> JobManager::operator_rescale_status(
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

SavepointAckMsg JobManager::take_savepoint(JobId job_id, std::chrono::milliseconds timeout) {
    SavepointAckMsg ack;
    ack.job_id = job_id;
    if (timeout == std::chrono::milliseconds{0}) {
        timeout = std::chrono::milliseconds{30'000};
    }

    // Stage the savepoint trigger under mu_: validate, assign a fresh
    // checkpoint id, register pending acks for every subtask of the
    // job, collect the TM connection list. Send the TriggerCheckpoint
    // frames outside the lock to avoid stalling readers.
    std::uint64_t ckpt_id = 0;
    std::vector<network::Connection*> tm_conns;
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
        ckpt_id = job.next_checkpoint_id++;
        std::unordered_set<std::string> pending;
        for (const auto& [key, _] : job.task_records) {
            pending.insert(key);
        }
        job.pending_checkpoint_acks[ckpt_id] = std::move(pending);
        job.pending_checkpoint_start_times[ckpt_id] = std::chrono::steady_clock::now();
        clink::metrics::ckpt::triggered();
        for (const auto& [tm_id, _] : job.tasks_by_tm) {
            auto tm_it = registered_.find(tm_id);
            if (tm_it != registered_.end() && !tm_it->second->lost && tm_it->second->conn) {
                tm_conns.push_back(tm_it->second->conn.get());
            }
        }
        ack.checkpoint_dir = job.checkpoint.checkpoint_dir;
    }

    TriggerCheckpointMsg tc;
    tc.job_id = job_id;
    tc.checkpoint_id = ckpt_id;
    const auto frame = encode_frame(MessageKind::TriggerCheckpoint, tc);
    for (auto* c : tm_conns) {
        send_frame(*c, frame);
    }
    log::info("jm.savepoint",
              "job_id=" + std::to_string(job_id) + " ckpt_id=" + std::to_string(ckpt_id) +
                  " tm_count=" + std::to_string(tm_conns.size()));

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
    ack.ok = true;
    ack.checkpoint_id = ckpt_id;
    ack.message = "savepoint complete";
    return ack;
}

void JobManager::handle_savepoint_(network::Connection& conn, MessageReader& r) {
    auto req = decode_savepoint(r);
    const auto ack = take_savepoint(req.job_id, std::chrono::milliseconds{req.timeout_ms});
    send_frame(conn, encode_frame(MessageKind::SavepointAck, ack));
}

void JobManager::handle_list_jobs_(network::Connection& conn) {
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
            ack.jobs.push_back(info);
        }
    }
    send_frame(conn, encode_frame(MessageKind::ListJobsAck, ack));
}

void JobManager::handle_submit_(network::Connection& conn, MessageReader& r) {
    auto sj = decode_submit_job(r);
    SubmitJobAckMsg ack;
    JobId assigned = 0;
    try {
        const auto graph = JobGraphSpec::from_json(sj.graph_json);
        // Allocate a per-job bundle whose registries are parented at
        // the process-wide defaults. Plugin .so registrations land in
        // this bundle (NOT the singletons) so two concurrent jobs that
        // mint overlapping _inline_<kind>_<n> names don't trample each
        // other.
        auto bundle = std::make_unique<JobBundle>();
        auto bundle_preg = bundle->as_plugin_registry();
        std::vector<std::string> plugin_so_paths;
        plugin_so_paths.reserve(sj.plugins.size());
        for (const auto& plug : sj.plugins) {
            const auto path = write_plugin_to_cache(plug);
            auto load_result = PluginLoader::default_instance().load_into(path, bundle_preg);
            if (!load_result.ok) {
                throw std::runtime_error("plugin '" + plug.name +
                                         "' failed to load on JM: " + load_result.error);
            }
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
}

JobId JobManager::allocate_job_id_() {
    return next_job_id_++;
}

JobId JobManager::submit_job(const JobGraphSpec& graph,
                             const OperatorRegistry& registry,
                             network::Connection* notify_client_conn) {
    return submit_job(graph, registry, std::vector<PluginBinary>{}, notify_client_conn);
}

JobId JobManager::submit_job(const JobGraphSpec& graph,
                             const OperatorRegistry& registry,
                             std::vector<PluginBinary> plugins,
                             network::Connection* notify_client_conn) {
    return submit_job(graph, registry, std::move(plugins), CheckpointConfig{}, notify_client_conn);
}

JobId JobManager::submit_job(const JobGraphSpec& graph,
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

JobId JobManager::submit_job(const JobGraphSpec& graph,
                             const OperatorRegistry& registry,
                             std::vector<PluginBinary> plugins,
                             CheckpointConfig checkpoint,
                             std::unique_ptr<JobBundle> bundle,
                             network::Connection* notify_client_conn) {
    // Use the bundle's OperatorRegistry (parent-fallback to default
    // singleton for built-ins) when one is provided so the planner's
    // chain-eligibility check can find inline-lambda ops registered by
    // the plugin's build_fn. Without this, plan_job sees only the
    // default singleton (which has no inline ops) and the chain is
    // refused.
    auto plan = bundle != nullptr
                    ? plan_job(graph, bundle->operator_registry(), bundle->runner_registry())
                    : plan_job(graph, registry);

    // Wait for spare slots if configured. This is a coarse-grained
    // policy: we just check `free_slots() >= required` periodically. A
    // production scheduler would integrate with the slot accountant.
    const auto required = plan.tasks.size();
    if (cfg_.submit_wait_for_slots.count() > 0) {
        std::unique_lock lock(mu_);
        const auto deadline = std::chrono::steady_clock::now() + cfg_.submit_wait_for_slots;
        cv_.wait_until(lock, deadline, [&] {
            std::size_t free = 0;
            for (const auto& [_, tm] : registered_) {
                if (!tm->lost) {
                    free += (tm->slot_capacity - tm->slots_in_use);
                }
            }
            return free >= required;
        });
    }
    {
        std::lock_guard lock(mu_);
        std::size_t free = 0;
        for (const auto& [_, tm] : registered_) {
            if (!tm->lost) {
                free += (tm->slot_capacity - tm->slots_in_use);
            }
        }
        if (free < required) {
            throw std::runtime_error("submit_job: insufficient free slots (need " +
                                     std::to_string(required) + ", have " + std::to_string(free) +
                                     ")");
        }
    }

    // Snapshot inputs for HA manifest write - deploy_internal_ moves
    // them into the JobState.
    const auto plugins_copy = plugins;
    const auto checkpoint_copy = checkpoint;
    const auto job_id = deploy_internal_(plan,
                                         notify_client_conn,
                                         std::move(plugins),
                                         std::move(checkpoint),
                                         std::move(bundle),
                                         graph.expected_state_versions.pack());
    // Phase 30b: derive commit-group memberships from sink-op params
    // and stash them on JobState so handle_subtask_checkpointed_ can
    // gate CommitCheckpoint broadcasts on the group's collective ack.
    //
    // Phase 29d: in the same walk, populate the RescaleCoordinator
    // with each operator's current parallelism + Phase 29a
    // min/max bounds. Operators with 0/0 bounds register too (the
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

            // Phase 29h: spin up the per-job autoscaler if the cluster
            // config opts in AND at least one op carries Phase 29a
            // bounds. The autoscaler captures `this` + `job_id` so its
            // callbacks route into JobManager::request_operator_rescale
            // / operator_rescale_status under the JM's lock discipline.
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
    if (!ha_dir_.empty()) {
        const auto graph_json = graph.to_json();
        {
            std::lock_guard lock(mu_);
            auto it = jobs_.find(job_id);
            if (it != jobs_.end())
                it->second->graph_json = graph_json;
        }
        persist_job_manifest_(ha_dir_, job_id, graph_json, plugins_copy, checkpoint_copy);
    }
    return job_id;
}

void JobManager::deploy(const JobPlan& plan) {
    legacy_active_job_id_ =
        deploy_internal_(plan, nullptr, std::vector<PluginBinary>{}, CheckpointConfig{}, nullptr);
}

JobId JobManager::deploy_internal_(const JobPlan& plan,
                                   network::Connection* notify_client_conn,
                                   std::vector<PluginBinary> plugins,
                                   CheckpointConfig checkpoint,
                                   std::unique_ptr<JobBundle> bundle,
                                   std::string expected_state_versions_packed) {
    // Resolve per-task placement (greedy first-fit). The plan's
    // data_port values are taken as-is: 0 means "the TM will bind
    // ephemerally and report via SubtaskListening", a non-zero port
    // means "the caller pre-bound this and the address is already
    // known". The legacy in-process API uses the latter.
    JobPlan resolved_plan = plan;
    {
        std::lock_guard lock(mu_);
        for (auto& t : resolved_plan.tasks) {
            if (!t.tm_id.empty()) {
                continue;
            }
            std::shared_ptr<TmConnection> picked;
            for (auto& [_, tm] : registered_) {
                if (tm->lost) {
                    continue;
                }
                if (tm->slots_in_use < tm->slot_capacity) {
                    picked = tm;
                    break;
                }
            }
            if (!picked) {
                throw std::runtime_error("JobManager::deploy: no TM with free slots for task '" +
                                         t.role + "[" + std::to_string(t.subtask_idx) + "]'");
            }
            t.tm_id = picked->tm_id;
            ++picked->slots_in_use;
        }
    }

    // Build a (role, subtask) → (tm_id, port) lookup so we can resolve
    // peer references into concrete host:port addresses. Hosts come
    // from the registered TM's data_host.
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
        index[TaskKey{t.role, t.subtask_idx}] = {t.tm_id, t.data_port};
    }

    const JobId job_id = allocate_job_id_();
    auto job = std::make_shared<JobState>();
    job->id = job_id;
    job->notify_client_conn = notify_client_conn;
    job->expected_completion = resolved_plan.tasks.size();
    job->submit_time = std::chrono::steady_clock::now();
    job->topology_version = 1;  // initial deploy is version 1
    job->bundle = std::move(bundle);
    job->expected_state_versions_packed = std::move(expected_state_versions_packed);
    // Only generic-role subtasks send SubtaskListening. Custom-role
    // (test-harness) tasks pre-bind their ports and never report.
    job->expected_listenings = 0;
    for (const auto& t : resolved_plan.tasks) {
        if (t.role == kGenericSubtaskRole) {
            ++job->expected_listenings;
        }
    }

    // Group tasks by tm_id and build DeploymentTask entries with peers
    // resolved against the lookup. Generic-role peer ports are 0 here
    // and get filled in via PeerUpdate after SubtaskListening arrives.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_tm;
    for (const auto& t : resolved_plan.tasks) {
        DeploymentTask d;
        d.role = t.role;
        d.subtask_idx = t.subtask_idx;
        d.data_port = t.data_port;
        d.extra_config = t.extra_config;
        for (const auto& [pr_role, pr_sub] : t.peer_refs) {
            auto it = index.find(TaskKey{pr_role, pr_sub});
            if (it == index.end()) {
                throw std::runtime_error("JobManager::deploy: unresolved peer ref " + pr_role +
                                         "/" + std::to_string(pr_sub));
            }
            const auto& [peer_tm_id, peer_port] = it->second;
            std::string peer_host;
            {
                std::lock_guard lock(mu_);
                auto tm_it = registered_.find(peer_tm_id);
                if (tm_it == registered_.end()) {
                    throw std::runtime_error("JobManager::deploy: peer TM not registered: " +
                                             peer_tm_id);
                }
                peer_host = tm_it->second->data_host;
            }
            d.peers.push_back(PeerAddress{
                .role = pr_role,
                .subtask_idx = pr_sub,
                .host = std::move(peer_host),
                .data_port = peer_port,
            });
        }
        by_tm[t.tm_id].push_back(std::move(d));
    }

    // Fill per-task key-group ranges based on the role's initial
    // parallelism. Same formula rescale_job uses (contiguous
    // ranges). Lets Queryable State kg-aware routing work on the
    // first deploy without waiting for a rescale; for non-keyed
    // operators the range field is just unread.
    {
        std::unordered_map<std::string, std::uint32_t> role_p;
        for (const auto& t : resolved_plan.tasks) {
            ++role_p[t.role];
        }
        for (auto& [_tm_id, tasks] : by_tm) {
            for (auto& d : tasks) {
                const auto it = role_p.find(d.role);
                if (it == role_p.end() || it->second == 0) {
                    continue;
                }
                const auto range = key_group_range_for_subtask(d.subtask_idx, it->second);
                d.key_group_first = range.first;
                d.key_group_last = range.second;
            }
        }
    }

    {
        std::lock_guard lock(mu_);
        for (const auto& [tm_id, tasks] : by_tm) {
            for (const auto& t : tasks) {
                const std::string key = t.role + ":" + std::to_string(t.subtask_idx);
                job->task_records[key] = {tm_id, t};
                job->pending_per_tm[tm_id].emplace_back(t.role, t.subtask_idx);
            }
            job->tasks_by_tm[tm_id] = tasks;
        }
        // If no generic-role tasks, there's no port-discovery handshake;
        // mark peer_updates_sent=true to short-circuit the bookkeeping.
        if (job->expected_listenings == 0) {
            job->peer_updates_sent = true;
        }
        job->plugins = plugins;  // copy; each Deploy gets its own copy below
        job->checkpoint = checkpoint;
        jobs_[job_id] = job;
    }

    metrics::jm::job_submitted();
    metrics::jm::slots_in_use_delta(static_cast<std::int64_t>(resolved_plan.tasks.size()));
    log::info("jm.submit",
              "job_id=" + std::to_string(job_id) +
                  " tasks=" + std::to_string(resolved_plan.tasks.size()));
    events::publish("jm.job_submitted",
                    "{\"job_id\":" + std::to_string(job_id) +
                        ",\"tasks\":" + std::to_string(resolved_plan.tasks.size()) + "}");

    // Send Deploy to each affected TM.
    for (auto& [tm_id, tasks] : by_tm) {
        std::shared_ptr<TmConnection> conn;
        {
            std::lock_guard lock(mu_);
            auto it = registered_.find(tm_id);
            if (it == registered_.end()) {
                throw std::runtime_error("JobManager::deploy: TM not registered: " + tm_id);
            }
            conn = it->second;
        }
        DeployMsg deploy_msg;
        deploy_msg.job_id = job_id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = plugins;
        deploy_msg.checkpoint_dir = checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = checkpoint.state_backend_uri;
        deploy_msg.restore_from_dir = checkpoint.restore_from_dir;
        deploy_msg.restore_from_checkpoint_id = checkpoint.restore_from_checkpoint_id;
        deploy_msg.unaligned_checkpoints = checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.expected_state_versions_packed = job->expected_state_versions_packed;
        const auto frame = encode_frame(MessageKind::Deploy, deploy_msg);
        if (!conn->conn || !send_frame(*conn->conn, frame)) {
            throw std::runtime_error("JobManager::deploy: send failed for " + tm_id);
        }
    }
    return job_id;
}

void JobManager::start_reader_for_(std::shared_ptr<TmConnection> tm) {
    tm->reader = std::thread([this, tm] {
        while (!stop_.load(std::memory_order_acquire)) {
            if (!tm->conn)
                return;
            auto frame = read_frame(*tm->conn);
            if (!frame.has_value()) {
                return;  // peer closed
            }
            bool drop = false;
            {
                std::lock_guard lock(mu_);
                if (tm->lost) {
                    drop = true;
                } else {
                    tm->last_seen = std::chrono::steady_clock::now();
                }
            }
            if (drop) {
                continue;
            }
            MessageReader r(std::move(*frame));
            const auto kind = static_cast<MessageKind>(r.read_u8());
            switch (kind) {
                case MessageKind::SubtaskFinished:
                    handle_subtask_finished_(r);
                    break;
                case MessageKind::SubtaskListening:
                    handle_subtask_listening_(r);
                    break;
                case MessageKind::Heartbeat:
                    (void)decode_heartbeat(r);
                    break;
                case MessageKind::SubtaskCheckpointed:
                    handle_subtask_checkpointed_(r);
                    break;
                default:
                    break;
            }
        }
    });
}

void JobManager::handle_subtask_listening_(MessageReader& r) {
    auto msg = decode_subtask_listening(r);
    {
        std::lock_guard lock(mu_);
        auto it = jobs_.find(msg.job_id);
        if (it == jobs_.end()) {
            return;  // stale message
        }
        auto& job = *it->second;
        // The TM advertises its own data_host via Register; we trust
        // that for the peer-resolution. Fall back to msg.host for
        // hostless deployments.
        std::string host = msg.host;
        auto tm_it = registered_.find(msg.tm_id);
        if (tm_it != registered_.end() && !tm_it->second->data_host.empty()) {
            host = tm_it->second->data_host;
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

        // Phase 29f: if the listening subtask's operator is in
        // CuttingOver, treat the SubtaskListening as the readiness
        // signal that closes the rescale. Mark the new subtask
        // ready; when every new subtask has reported the coordinator
        // transitions CuttingOver -> Complete.
        if (job.rescale_coordinator) {
            if (auto st = job.rescale_coordinator->status(msg.role);
                st.has_value() && st->state == RescaleState::CuttingOver) {
                job.rescale_coordinator->mark_new_ready(msg.role, msg.subtask_idx);
                if (auto post = job.rescale_coordinator->status(msg.role);
                    post.has_value() && post->state == RescaleState::Complete) {
                    log::info("jm.rescale",
                              "complete job_id=" + std::to_string(job.id) + " op_id=" + msg.role +
                                  " new_parallelism=" + std::to_string(post->target_parallelism));
                }
            }
        }
    }
}

void JobManager::send_peer_updates_locked_(JobState& job) {
    // Group per TM. Each TM's PeerUpdate carries the resolved peers
    // for every task on that TM that has a non-empty peers[] list.
    //
    // The peer-resolution key is the 4-tuple
    // (downstream_role, downstream_sub, upstream_role, upstream_sub).
    // Each task's `peers` entries identify the downstream this task
    // wants to connect to; combined with the task's own (role, sub)
    // those form the lookup key.
    std::unordered_map<std::string, PeerUpdateMsg> per_tm;
    for (const auto& [tm_id, tasks] : job.tasks_by_tm) {
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
            per_tm[tm_id].tasks.push_back(std::move(tp));
        }
    }
    for (auto& [tm_id, msg] : per_tm) {
        msg.job_id = job.id;
        auto tm_it = registered_.find(tm_id);
        if (tm_it == registered_.end() || tm_it->second->lost) {
            continue;
        }
        const auto frame = encode_frame(MessageKind::PeerUpdate, msg);
        if (tm_it->second->conn)
            send_frame(*tm_it->second->conn, frame);
    }

    // Subtasks with empty peers[] still need a "go" signal. Send an
    // empty PeerUpdate to each TM that hosts at least one such task and
    // hasn't already received a real update.
    std::unordered_set<std::string> tms_with_real_update;
    for (const auto& [tm_id, _] : per_tm) {
        tms_with_real_update.insert(tm_id);
    }
    for (const auto& [tm_id, _] : job.tasks_by_tm) {
        if (tms_with_real_update.count(tm_id) > 0) {
            continue;
        }
        auto tm_it = registered_.find(tm_id);
        if (tm_it == registered_.end() || tm_it->second->lost) {
            continue;
        }
        PeerUpdateMsg empty;
        empty.job_id = job.id;
        const auto frame = encode_frame(MessageKind::PeerUpdate, empty);
        if (tm_it->second->conn)
            send_frame(*tm_it->second->conn, frame);
    }
}

void JobManager::watchdog_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(cfg_.watchdog_interval);
        if (stop_.load(std::memory_order_acquire)) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        bool any_lost = false;
        std::vector<std::pair<network::Connection*, JobId>> survivor_cancels;
        std::vector<PendingDeploy> deferred_restart_deploys;
        {
            std::lock_guard lock(mu_);
            for (auto& [_, tm] : registered_) {
                if (tm->lost) {
                    continue;
                }
                if (now - tm->last_seen > cfg_.heartbeat_timeout) {
                    mark_tm_lost_locked_(*tm);
                    any_lost = true;
                }
            }
            if (any_lost) {
                // Scan jobs touched by lost TMs and broadcast CancelJob
                // to each of their surviving TMs. The survivor's role
                // handler is expected to poll was_cancelled() and exit,
                // which makes its TM emit a normal SubtaskFinished that
                // increments completed_count via the regular path.
                for (auto& [job_id, job] : jobs_) {
                    bool job_touched = false;
                    for (const auto& [tm_id, _] : job->tasks_by_tm) {
                        auto it = registered_.find(tm_id);
                        if (it != registered_.end() && it->second->lost) {
                            job_touched = true;
                            break;
                        }
                    }
                    if (!job_touched) {
                        continue;
                    }
                    for (const auto& [tm_id, _] : job->tasks_by_tm) {
                        auto it = registered_.find(tm_id);
                        if (it != registered_.end() && !it->second->lost && it->second->conn) {
                            survivor_cancels.emplace_back(it->second->conn.get(), job_id);
                        }
                    }
                    // If the job is awaiting_restart and no surviving
                    // subtasks need to drain (they finished before the
                    // watchdog tick), kick off the redeploy here.
                    if (job->awaiting_restart && job->restart_drain_expected.empty()) {
                        auto deploys = restart_job_locked_(*job);
                        for (auto& d : deploys)
                            deferred_restart_deploys.push_back(std::move(d));
                    }
                    // Only force-signal completion if the synthesised
                    // errors from mark_tm_lost_locked_ already brought
                    // completed_count to expected_completion - i.e. every
                    // task was on lost TMs. With a survivor still in
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
            // outrun its deadline. This catches the case a lost-TM fold
            // can't - a survivor that is hung but still heartbeating, so it
            // neither acks the cancel nor dies. Failing is the safe
            // escalation; force-restarting could double-run a slow-but-alive
            // survivor's subtask against shared state.
            for (auto& [_, job] : jobs_) {
                if (job->awaiting_restart && !job->completion_signalled &&
                    job->restart_deadline != std::chrono::steady_clock::time_point{} &&
                    now > job->restart_deadline) {
                    log::warn("jm.watchdog",
                              "job_id=" + std::to_string(job->id) +
                                  " restart drain timed out; failing job");
                    job->errors.push_back("restart drain timed out after " +
                                          std::to_string(cfg_.restart_drain_timeout.count()) +
                                          "ms (survivors did not drain)");
                    job->awaiting_restart = false;
                    job->restart_deadline = {};
                    job->restart_pending.clear();
                    job->restart_drained_keys.clear();
                    job->restart_drain_expected.clear();
                    job->completed_count = job->expected_completion;
                    signal_job_completion_locked_(*job);
                }
            }
        }
        for (const auto& [conn, jid] : survivor_cancels) {
            CancelJobMsg cj;
            cj.job_id = jid;
            send_frame(*conn, encode_frame(MessageKind::CancelJob, cj));
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

void JobManager::mark_tm_lost_locked_(TmConnection& tm) {
    tm.lost = true;
    lost_tm_ids_.push_back(tm.tm_id);
    for (auto& [_, job] : jobs_) {
        auto it = job->pending_per_tm.find(tm.tm_id);
        if (it == job->pending_per_tm.end()) {
            continue;
        }
        // Second TM lost while this job is already draining for a restart.
        // Its subtasks were survivors at the first loss, so they sit in
        // restart_drain_expected - but they can never drain now (their TM is
        // dead). Fold them into the in-progress restart: drop them from the
        // expected-drain set and queue them for redeploy (restart_pending is
        // unioned with restart_drain_expected when restart_job_locked_ builds
        // the task set). The empty-drain kick in watchdog_loop_, or the
        // remaining survivors' drains, then fires the restart onto the TMs
        // still alive. Without this the drain never completes and the job
        // wedges in awaiting_restart forever. We do NOT consume a restart
        // attempt here - this is still the same restart, now covering both
        // losses.
        if (job->awaiting_restart && !job->completion_signalled && !job->cancel_requested) {
            for (const auto& [role, sub] : it->second) {
                const std::string k = role + ":" + std::to_string(sub);
                job->restart_drain_expected.erase(k);
                job->restart_drained_keys.erase(k);
                job->restart_pending.emplace_back(role, sub);
            }
            log::warn("jm.watchdog",
                      "job_id=" + std::to_string(job->id) +
                          " second TM lost during restart drain; folded " +
                          std::to_string(it->second.size()) +
                          " subtask(s) into the pending restart, drain_expected=" +
                          std::to_string(job->restart_drain_expected.size()));
            it->second.clear();
            continue;
        }
        const bool can_restart = !job->awaiting_restart && !job->completion_signalled &&
                                 !job->cancel_requested &&
                                 !job->checkpoint.checkpoint_dir.empty() &&
                                 job->restart_attempts < job->checkpoint.max_restarts_on_tm_loss;
        if (can_restart) {
            // Defer error synthesis. Capture the lost-TM subtasks for
            // re-deployment, plus the surviving-TM subtasks we need
            // to drain. drain_expected pulls from pending_per_tm
            // (still-in-flight) rather than tasks_by_tm (all subtasks
            // including completed ones) so a sink that already reported
            // SubtaskFinished before the watchdog declared the TM lost
            // doesn't get treated as a still-pending drainee. The
            // empty-drain case (everyone else finished already)
            // triggers restart right here.
            job->awaiting_restart = true;
            // Bound the drain: if survivors don't all report within
            // restart_drain_timeout, the watchdog fails the job rather than
            // wedge (e.g. a survivor that hangs without acking the cancel).
            job->restart_deadline = std::chrono::steady_clock::now() + cfg_.restart_drain_timeout;
            for (const auto& [role, sub] : it->second) {
                job->restart_pending.emplace_back(role, sub);
            }
            for (const auto& [other_tm_id, pending] : job->pending_per_tm) {
                if (other_tm_id == tm.tm_id)
                    continue;
                auto other_it = registered_.find(other_tm_id);
                if (other_it == registered_.end() || other_it->second->lost)
                    continue;
                for (const auto& [role, sub] : pending) {
                    job->restart_drain_expected.insert(role + ":" + std::to_string(sub));
                }
            }
            log::warn("jm.watchdog",
                      "job_id=" + std::to_string(job->id) + " awaiting_restart (attempt " +
                          std::to_string(job->restart_attempts + 1) + "/" +
                          std::to_string(job->checkpoint.max_restarts_on_tm_loss) +
                          ") drain_expected=" + std::to_string(job->restart_drain_expected.size()));
            // Bookkeeping: completed_count and errors may already include
            // entries for surviving-TM subtasks that finished before this
            // tick. Clear them - they belong to the previous attempt and
            // restart_job_locked_ will reset transient state anyway.
            job->completed_count = 0;
            job->errors.clear();
        } else {
            // Fail-fast: synthesise an error per pending task on the
            // lost TM and free its slot.
            for (const auto& [role, sub] : it->second) {
                job->errors.push_back(tm.tm_id + "/" + role + "[" + std::to_string(sub) +
                                      "]: TM lost (heartbeat timeout)");
                ++job->completed_count;
            }
        }
        it->second.clear();
        // Slots used by this TM for this job are gone with the TM, but
        // we don't decrement slots_in_use because we're going to mark
        // the TM lost - it's no longer participating in scheduling.
    }
    if (tm.conn) {
        tm.conn->shutdown_read();
    }
    metrics::jm::tm_lost(tm.slot_capacity, tm.slots_in_use);
    log::warn("jm.watchdog", "tm lost: " + tm.tm_id);
    events::publish("jm.tm_lost", "{\"tm_id\":" + js_quote(tm.tm_id) + "}");
}

std::vector<JobManager::PendingDeploy> JobManager::restart_job_locked_(JobState& job) {
    // 1. Snapshot the task set we need to redeploy. Build a topology
    //    template (extra_config + original peer_refs) from task_records
    //    BEFORE clearing it. Without this, multi-stage jobs would lose
    //    cross-subtask wiring after restart.
    struct Template {
        DeploymentTask base;  // role/subtask_idx/extra_config/peer_refs preserved
    };
    std::unordered_map<std::string, Template> templates;  // key "role:idx"
    for (const auto& [key, tm_dt] : job.task_records) {
        Template t;
        t.base = tm_dt.second;
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
    if (is_rescale) {
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

    // 2. Pick survivor TMs (alive, with at least one slot free).
    std::vector<std::shared_ptr<TmConnection>> survivors;
    std::size_t total_free = 0;
    for (const auto& [tm_id, tm] : registered_) {
        if (tm->lost || !tm->conn)
            continue;
        if (tm->slots_in_use < tm->slot_capacity) {
            survivors.push_back(tm);
            total_free += (tm->slot_capacity - tm->slots_in_use);
        }
    }
    if (survivors.empty() || total_free < tasks_to_redeploy.size()) {
        // No room to restart. Fall back: synthesise errors and signal
        // completion so the client sees the failure.
        for (const auto& [role, sub] : tasks_to_redeploy) {
            job.errors.push_back("restart: no slot available for " + role + "[" +
                                 std::to_string(sub) + "]");
            ++job.completed_count;
        }
        job.awaiting_restart = false;
        job.restart_deadline = {};
        job.restart_pending.clear();
        job.restart_drained_keys.clear();
        job.restart_drain_expected.clear();
        if (job.completed_count >= job.expected_completion) {
            signal_job_completion_locked_(job);
        }
        return {};
    }

    // 3. Build new tm_id assignments round-robin across survivors.
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
    std::unordered_map<std::string, std::size_t> per_tm_remaining;
    for (const auto& tm : survivors) {
        per_tm_remaining[tm->tm_id] = tm->slot_capacity - tm->slots_in_use;
    }
    for (const auto& [role, sub] : tasks_to_redeploy) {
        // Skip duplicate task entries (restart_pending + restart_drain
        // might overlap if a lost subtask was already in tasks_by_tm
        // before - defensive).
        TaskKey k{role, sub};
        if (placement.count(k) != 0)
            continue;
        for (std::size_t i = 0; i < survivors.size(); ++i) {
            auto& s = survivors[(rr + i) % survivors.size()];
            if (per_tm_remaining[s->tm_id] > 0) {
                placement[k] = {s->tm_id, 0};
                --per_tm_remaining[s->tm_id];
                rr = (rr + i + 1) % survivors.size();
                break;
            }
        }
    }

    // 4. Reset transient JobState fields. expected_completion stays
    //    for plain restart (same task count); rescale resets it to the
    //    new total. attempt_counts retained - task-level retry budget
    //    is independent of TM-level restart budget.
    ++job.restart_attempts;
    job.awaiting_restart = false;
    job.restart_deadline = {};
    job.restart_pending.clear();
    job.restart_drained_keys.clear();
    job.restart_drain_expected.clear();
    job.completed_count = 0;
    job.errors.clear();
    job.peer_updates_sent = false;
    job.received_listenings = 0;
    job.ports.clear();
    job.task_records.clear();
    job.tasks_by_tm.clear();
    job.pending_per_tm.clear();
    job.pending_checkpoint_acks.clear();
    if (is_rescale) {
        job.expected_completion = tasks_to_redeploy.size();
    }

    // 5. Build new DeploymentTasks with refreshed peer addresses.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_tm;
    job.expected_listenings = 0;
    for (const auto& [k, tm_port] : placement) {
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
                auto tm_it = registered_.find(pit->second.first);
                if (tm_it != registered_.end()) {
                    p.host = tm_it->second->data_host;
                }
            }
            p.data_port = 0;
        }
        // Rescale: tag this new subtask with the parent old subtask
        // whose state file it should restore from, and the key-group
        // range it's responsible for. Roles not being rescaled fall
        // through with kRestoreFromSelf + {0, 0} so the TM sees no
        // rescale directive and follows the historic same-idx path.
        //
        // Scale-up   (new_p > old_p): k = new_p/old_p new subtasks
        //   per parent; parent_idx = new_idx / k; parent_count = 1.
        // Scale-down (new_p < old_p): k_down = old_p/new_p parents
        //   per new subtask; parent_idx = new_idx * k_down;
        //   parent_count = k_down (contiguous range merged at the
        //   state backend factory).
        if (is_rescale) {
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
                const auto range = key_group_range_for_subtask(k.subtask_idx, new_p);
                d.key_group_first = range.first;
                d.key_group_last = range.second;
            }
        }
        by_tm[tm_port.first].push_back(std::move(d));
        ++job.expected_listenings;
    }

    // 6. Stash the new shape into job state for the rest of the JM
    //    to address: SubtaskListening etc.
    for (auto& [tm_id, tasks] : by_tm) {
        for (const auto& t : tasks) {
            const std::string key = t.role + ":" + std::to_string(t.subtask_idx);
            job.task_records[key] = {tm_id, t};
            job.pending_per_tm[tm_id].emplace_back(t.role, t.subtask_idx);
        }
        job.tasks_by_tm[tm_id] = tasks;
    }

    // A rescale shifts key-group ownership; bump topology_version so
    // Queryable State route caches can invalidate. Restart-from-loss
    // also bumps - the placement might land on different TMs.
    ++job.topology_version;

    log::info(is_rescale ? "jm.rescale" : "jm.restart",
              "job_id=" + std::to_string(job.id) +
                  " attempt=" + std::to_string(job.restart_attempts) +
                  " survivors=" + std::to_string(survivors.size()) +
                  " tasks=" + std::to_string(tasks_to_redeploy.size()));
    if (is_rescale) {
        job.rescale_overrides.clear();
        job.pre_rescale_parallelism.clear();
    }

    // 7. Build Deploy frames + claim slots; return for caller to send.
    std::vector<PendingDeploy> out;
    for (auto& [tm_id, tasks] : by_tm) {
        auto tm_it = registered_.find(tm_id);
        if (tm_it == registered_.end() || tm_it->second->lost || !tm_it->second->conn)
            continue;
        tm_it->second->slots_in_use += tasks.size();
        metrics::jm::slots_in_use_delta(static_cast<std::int64_t>(tasks.size()));
        DeployMsg deploy_msg;
        deploy_msg.job_id = job.id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = job.plugins;
        deploy_msg.checkpoint_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = job.checkpoint.state_backend_uri;
        // Restart point: use the JM's own checkpoint dir as the
        // restore source, last completed checkpoint id we acknowledged.
        deploy_msg.restore_from_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.restore_from_checkpoint_id = job.latest_completed_checkpoint_id;
        deploy_msg.unaligned_checkpoints =
            job.checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.expected_state_versions_packed = job.expected_state_versions_packed;
        out.push_back({tm_it->second->conn.get(), encode_frame(MessageKind::Deploy, deploy_msg)});
    }
    return out;
}

void JobManager::dispatch_begin_rescale_locked_(JobState& job,
                                                const std::string& op_id,
                                                std::uint64_t cutover_checkpoint,
                                                std::uint32_t target_parallelism,
                                                std::vector<PendingDeploy>& out) {
    // Find every TM that hosts at least one subtask of this operator
    // and send it a BeginRescale. The TM's drain dispatcher (29d-2)
    // looks up its registered drain callbacks by (job_id, op_id) and
    // fires them; each callback sets drain_target on the source-runner
    // side (29d-3) so the running subtask emits its DrainMarker and
    // shuts down via SubtaskFinished.
    std::unordered_set<std::string> tms_with_op;
    for (const auto& [tm_id, tasks] : job.tasks_by_tm) {
        for (const auto& t : tasks) {
            if (t.role == op_id) {
                tms_with_op.insert(tm_id);
                break;
            }
        }
    }
    BeginRescaleMsg msg;
    msg.job_id = job.id;
    msg.op_id = op_id;
    msg.target_parallelism = target_parallelism;
    msg.cutover_checkpoint = cutover_checkpoint;
    const auto frame = encode_frame(MessageKind::BeginRescale, msg);
    for (const auto& tm_id : tms_with_op) {
        auto tm_it = registered_.find(tm_id);
        if (tm_it == registered_.end() || tm_it->second->lost || !tm_it->second->conn) {
            continue;
        }
        out.push_back({tm_it->second->conn.get(), frame});
    }
}

void JobManager::dispatch_cutover_deploy_locked_(JobState& job,
                                                 const std::string& op_id,
                                                 std::vector<PendingDeploy>& out) {
    if (!job.rescale_coordinator) {
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
        if (val.second.role == op_id) {
            old_keys.push_back(key);
            if (templ == nullptr) {
                templ = &val.second;
            }
        }
    }
    if (templ == nullptr) {
        log::warn("jm.rescale",
                  "cutover deploy for op '" + op_id + "' has no template task (drain race?)");
        job.rescale_coordinator->abort(op_id, "no template task");
        return;
    }

    // Free-slot snapshot. Skip lost TMs and any without an open conn.
    std::vector<std::pair<std::string, std::uint32_t>> tm_free_slots;
    for (const auto& [tm_id, tm] : registered_) {
        if (tm->lost || !tm->conn) {
            continue;
        }
        const std::uint32_t free_slots =
            tm->slot_capacity > tm->slots_in_use ? (tm->slot_capacity - tm->slots_in_use) : 0;
        // Old subtasks of THIS op count as in-use right now but are
        // about to be torn down below, so they're effectively
        // available for the new placement. Add them back to the
        // snapshot the planner sees.
        std::uint32_t old_on_this_tm = 0;
        auto it = job.tasks_by_tm.find(tm_id);
        if (it != job.tasks_by_tm.end()) {
            for (const auto& t : it->second) {
                if (t.role == op_id) {
                    ++old_on_this_tm;
                }
            }
        }
        tm_free_slots.emplace_back(tm_id, free_slots + old_on_this_tm);
    }

    DeploymentTask cloned_template = *templ;
    auto plan = plan_operator_cutover(op_id,
                                      status->current_parallelism,
                                      status->target_parallelism,
                                      status->cutover_checkpoint,
                                      job.checkpoint.checkpoint_dir,
                                      cloned_template,
                                      old_keys,
                                      std::move(tm_free_slots));
    if (!plan.ok) {
        log::warn("jm.rescale",
                  "cutover deploy planning failed for op '" + op_id + "': " + plan.error);
        job.rescale_coordinator->abort(op_id, plan.error);
        return;
    }

    // Tear down the old subtasks' bookkeeping. Their SubtaskFinished
    // arrivals already counted as drained acks via 29d-3's
    // mark_old_drained wiring; remove their task_records / tasks_by_tm
    // entries and free the TM slots. Don't increment completed_count
    // for drained subtasks - they're being replaced, not finished.
    for (const auto& key : plan.teardown_keys) {
        auto rec_it = job.task_records.find(key);
        if (rec_it == job.task_records.end()) {
            continue;
        }
        const auto tm_id = rec_it->second.first;
        auto tm_reg_it = registered_.find(tm_id);
        if (tm_reg_it != registered_.end() && tm_reg_it->second->slots_in_use > 0) {
            --tm_reg_it->second->slots_in_use;
            metrics::jm::slots_in_use_delta(-1);
        }
        auto tbt_it = job.tasks_by_tm.find(tm_id);
        if (tbt_it != job.tasks_by_tm.end()) {
            std::erase_if(tbt_it->second, [&](const DeploymentTask& t) {
                return t.role == op_id && (rec_it->second.second.subtask_idx == t.subtask_idx);
            });
            if (tbt_it->second.empty()) {
                job.tasks_by_tm.erase(tbt_it);
            }
        }
        job.task_records.erase(rec_it);
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

    // Stash the new subtasks into JobState + claim TM slots, then
    // build per-TM Deploy frames.
    std::unordered_map<std::string, std::vector<DeploymentTask>> by_tm;
    for (auto& [tm_id, task] : plan.new_tasks) {
        const std::string key = task.role + ":" + std::to_string(task.subtask_idx);
        job.task_records[key] = {tm_id, task};
        job.pending_per_tm[tm_id].emplace_back(task.role, task.subtask_idx);
        job.tasks_by_tm[tm_id].push_back(task);
        by_tm[tm_id].push_back(std::move(task));
        ++job.expected_completion;
        ++job.expected_listenings;
    }

    // A rescale shifts key-group ownership; bump topology_version so
    // Queryable State route caches can invalidate.
    ++job.topology_version;
    clink::metrics::orch::rescale_cutover_deploy();

    log::info("jm.rescale",
              "cutover deploy job_id=" + std::to_string(job.id) + " op_id=" + op_id +
                  " new_parallelism=" + std::to_string(status->target_parallelism) +
                  " cutover_checkpoint=" + std::to_string(status->cutover_checkpoint));

    for (auto& [tm_id, tasks] : by_tm) {
        auto tm_it = registered_.find(tm_id);
        if (tm_it == registered_.end() || tm_it->second->lost || !tm_it->second->conn) {
            continue;
        }
        tm_it->second->slots_in_use += tasks.size();
        metrics::jm::slots_in_use_delta(static_cast<std::int64_t>(tasks.size()));
        DeployMsg deploy_msg;
        deploy_msg.job_id = job.id;
        deploy_msg.tasks = std::move(tasks);
        deploy_msg.plugins = job.plugins;
        deploy_msg.checkpoint_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.state_backend_uri = job.checkpoint.state_backend_uri;
        // The new subtasks restore from the cutover checkpoint, NOT
        // the latest. This is the key difference from restart_job_locked_:
        // we use the coordinator-chosen cutover_checkpoint so all new
        // subtasks load a consistent snapshot frozen at the drain barrier.
        deploy_msg.restore_from_dir = job.checkpoint.checkpoint_dir;
        deploy_msg.restore_from_checkpoint_id = status->cutover_checkpoint;
        deploy_msg.unaligned_checkpoints =
            job.checkpoint.alignment == CheckpointAlignment::Unaligned;
        deploy_msg.expected_state_versions_packed = job.expected_state_versions_packed;
        out.push_back({tm_it->second->conn.get(), encode_frame(MessageKind::Deploy, deploy_msg)});
    }
}

void JobManager::handle_subtask_finished_(MessageReader& r) {
    auto msg = decode_subtask_finished(r);

    bool retry = false;
    std::shared_ptr<TmConnection> target_conn;
    DeploymentTask retry_task;
    JobId job_id = msg.job_id;
    std::shared_ptr<JobState> job_to_signal;
    std::vector<PendingDeploy> restart_deploys;
    {
        std::lock_guard lock(mu_);
        auto job_it = jobs_.find(job_id);
        if (job_it == jobs_.end()) {
            return;
        }
        auto& job = *job_it->second;
        const std::string key = msg.role + ":" + std::to_string(msg.subtask_idx);

        // Phase 29d-3: if the operator this subtask belonged to is in
        // the Draining state, count this SubtaskFinished as a drained
        // ack. The coordinator transitions Draining -> CuttingOver
        // when every old subtask has drained; 29d's deploy step then
        // brings up the new subtasks. Failed (had_error=true)
        // shutdowns also count - the operator's old subtasks are
        // going away one way or another.
        //
        // Phase 29f: if the mark_old_drained transition lands the
        // operator in CuttingOver, fire the cutover-deployment slice
        // here so the new subtasks come up without a separate trigger.
        // Drained subtasks must NOT flow through the regular
        // completed_count / slot-free accounting below: the cutover
        // deploy already adjusted expected_completion and removed the
        // task_records entry; double-counting would falsely signal
        // job completion.
        bool was_drain = false;
        if (job.rescale_coordinator) {
            if (auto st = job.rescale_coordinator->status(msg.role);
                st.has_value() && st->state == RescaleState::Draining) {
                job.rescale_coordinator->mark_old_drained(msg.role, msg.subtask_idx);
                was_drain = true;
                if (auto post = job.rescale_coordinator->status(msg.role);
                    post.has_value() && post->state == RescaleState::CuttingOver) {
                    dispatch_cutover_deploy_locked_(job, msg.role, restart_deploys);
                }
            }
        }

        if (msg.had_error && cfg_.max_restarts > 0) {
            const int attempts = ++job.attempt_counts[key];
            if (attempts <= cfg_.max_restarts) {
                auto rec_it = job.task_records.find(key);
                if (rec_it != job.task_records.end()) {
                    retry = true;
                    retry_task = rec_it->second.second;
                    if (!retry_task.extra_config.empty() &&
                        retry_task.extra_config.back() != '\n') {
                        retry_task.extra_config += '\n';
                    }
                    retry_task.extra_config += "clink_attempt=" + std::to_string(attempts);
                    auto tm_it = registered_.find(rec_it->second.first);
                    if (tm_it != registered_.end()) {
                        target_conn = tm_it->second;
                    } else {
                        retry = false;
                    }
                }
            }
        }

        // Pending restart drain: when a TM was lost and the job is
        // awaiting_restart, surviving subtasks are being cancelled. As
        // their SubtaskFinished arrivals come in here we DO NOT count
        // them toward completion or errors - they're being retired
        // ahead of the redeploy. Free their slots and record the drain.
        if (was_drain) {
            // Phase 29f: drained subtasks have already been torn down
            // by dispatch_cutover_deploy_locked_ above. Skip the
            // completed_count / slot-free / signal_job_completion path
            // entirely - the rescale lifecycle owns the bookkeeping
            // for the old subtask now.
        } else if (!retry && job.awaiting_restart) {
            auto pending_it = job.pending_per_tm.find(msg.tm_id);
            if (pending_it != job.pending_per_tm.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            auto tm_it = registered_.find(msg.tm_id);
            if (tm_it != registered_.end() && tm_it->second->slots_in_use > 0) {
                --tm_it->second->slots_in_use;
                metrics::jm::slots_in_use_delta(-1);
            }
            job.restart_drained_keys.insert(key);
            // Every expected-to-drain surviving subtask has now reported
            // → time to redeploy.
            bool ready_for_restart = true;
            for (const auto& expected : job.restart_drain_expected) {
                if (job.restart_drained_keys.count(expected) == 0) {
                    ready_for_restart = false;
                    break;
                }
            }
            if (ready_for_restart) {
                restart_deploys = restart_job_locked_(job);
            }
        } else if (!retry) {
            ++job.completed_count;
            if (msg.had_error) {
                job.errors.push_back(msg.tm_id + "/" + msg.role + "[" +
                                     std::to_string(msg.subtask_idx) + "]: " + msg.error_message);
            }
            // Free this subtask's slot on the owning TM.
            auto pending_it = job.pending_per_tm.find(msg.tm_id);
            if (pending_it != job.pending_per_tm.end()) {
                std::erase_if(pending_it->second, [&](const auto& p) {
                    return p.first == msg.role && p.second == msg.subtask_idx;
                });
            }
            auto tm_it = registered_.find(msg.tm_id);
            if (tm_it != registered_.end() && tm_it->second->slots_in_use > 0) {
                --tm_it->second->slots_in_use;
                metrics::jm::slots_in_use_delta(-1);
            }
            if (job.completed_count >= job.expected_completion) {
                signal_job_completion_locked_(job);
                job_to_signal = job_it->second;
            }
        }
    }
    // Fire restart deploys outside the lock so a slow send doesn't
    // stall mu_-holders. Best-effort: if a send fails the watchdog
    // will catch the second TM loss on the next tick.
    for (auto& d : restart_deploys) {
        if (d.conn)
            send_frame(*d.conn, d.frame);
    }

    if (retry && target_conn && target_conn->conn) {
        DeployMsg deploy_msg;
        deploy_msg.job_id = job_id;
        deploy_msg.tasks.push_back(std::move(retry_task));
        const auto frame = encode_frame(MessageKind::Deploy, deploy_msg);
        send_frame(*target_conn->conn, frame);
    }
    // Slot freed: wake any submit_job waiting on capacity.
    cv_.notify_all();
}

void JobManager::signal_job_completion_locked_(JobState& job) {
    if (job.completion_signalled) {
        return;
    }
    job.completion_signalled = true;
    const char* status = "ok";
    if (job.cancel_requested) {
        metrics::jm::job_cancelled();
        log::info("jm.complete", "job_id=" + std::to_string(job.id) + " cancelled");
        status = "cancelled";
    } else if (job.errors.empty()) {
        metrics::jm::job_completed_ok();
        log::info("jm.complete", "job_id=" + std::to_string(job.id) + " ok");
    } else {
        metrics::jm::job_failed();
        log::warn("jm.complete",
                  "job_id=" + std::to_string(job.id) +
                      " failed errors=" + std::to_string(job.errors.size()));
        status = "failed";
    }
    events::publish("jm.job_completed",
                    "{\"job_id\":" + std::to_string(job.id) + ",\"status\":\"" + status + "\"" +
                        ",\"errors\":" + std::to_string(job.errors.size()) + "}");
    {
        CompletedJobRecord rec;
        rec.job_id = job.id;
        rec.status = status;
        rec.errors = job.errors;
        rec.restart_attempts = job.restart_attempts;
        rec.latest_completed_checkpoint_id = job.latest_completed_checkpoint_id;
        rec.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - job.submit_time);
        rec.completed_at_unix_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count();
        persist_history_record_(ha_dir_, rec);
        history_.push_back(std::move(rec));
        while (history_.size() > kJobManagerHistoryCap) {
            history_.pop_front();
        }
    }
    if (job.notify_client_conn != nullptr) {
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
    cv_.notify_all();
}

void JobManager::expect_tms(std::vector<std::string> tm_ids) {
    std::lock_guard lock(mu_);
    expected_tms_ = std::move(tm_ids);
}

bool JobManager::await_registrations(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this] {
        for (const auto& id : expected_tms_) {
            if (registered_.find(id) == registered_.end()) {
                return false;
            }
        }
        return true;
    });
}

bool JobManager::await_completion(std::chrono::milliseconds timeout) {
    return await_job_completion(legacy_active_job_id_, timeout);
}

bool JobManager::await_job_completion(JobId job_id, std::chrono::milliseconds timeout) {
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

std::vector<std::string> JobManager::errors() const {
    return job_errors(legacy_active_job_id_);
}

std::vector<std::string> JobManager::job_errors(JobId job_id) const {
    std::lock_guard lock(mu_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return {};
    }
    return it->second->errors;
}

std::vector<CompletedJobRecord> JobManager::job_history() const {
    std::lock_guard lock(mu_);
    return std::vector<CompletedJobRecord>(history_.begin(), history_.end());
}

std::optional<CompletedJobRecord> JobManager::job_history(JobId job_id) const {
    std::lock_guard lock(mu_);
    for (const auto& rec : history_) {
        if (rec.job_id == job_id) {
            return rec;
        }
    }
    return std::nullopt;
}

std::size_t JobManager::free_slots() const {
    std::lock_guard lock(mu_);
    std::size_t free = 0;
    for (const auto& [_, tm] : registered_) {
        if (!tm->lost) {
            free += (tm->slot_capacity - tm->slots_in_use);
        }
    }
    return free;
}

std::vector<std::string> JobManager::lost_tms() const {
    std::lock_guard lock(mu_);
    return lost_tm_ids_;
}

void JobManager::stop() {
    stop_.store(true, std::memory_order_release);
    // Phase 29h: tear down per-job autoscalers before everything else.
    // Their polling threads might be sitting on mu_ trying to call
    // request_operator_rescale; joining them now (under the move-out +
    // destroy-outside-the-lock pattern) lets the rest of stop()
    // proceed without contending with autoscaler callbacks.
    stop_autoscalers_();
    // shutdown_read on the listener wakes accept() on Linux but is a
    // no-op on macOS / BSDs. Close the listener fd to portably wake
    // accept_one. Closing while accept is blocked is safe here: the
    // accept thread can only be in accept() (no other use of the fd),
    // and we join it before doing anything else with the descriptor.
    //
    // Write listener_fd_ = -1 only AFTER joining the accept thread.
    // Joining establishes the happens-before edge the accept thread
    // needs to safely retire its reads of listener_fd_ - without it
    // TSan (correctly) flags the unsynchronised write against the
    // accept_loop_'s read in accept_one(listener_fd_).
    if (listener_fd_ >= 0) {
        network::NetworkSocket::shutdown_read(listener_fd_);
        network::NetworkSocket::close(listener_fd_);
    }
    if (accept_thread_.joinable()) {
        accept_thread_.join();
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
        std::vector<std::shared_ptr<network::Connection>> conns;
        std::vector<std::thread> threads;
        {
            std::lock_guard lock(client_mu_);
            conns = std::move(client_conns_);
            threads = std::move(client_threads_);
        }
        for (auto& c : conns) {
            if (c)
                c->shutdown_read();
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    std::vector<std::shared_ptr<TmConnection>> tms;
    {
        std::lock_guard lock(mu_);
        for (auto& [_, tm] : registered_) {
            tms.push_back(tm);
        }
        registered_.clear();
    }
    for (auto& tm : tms) {
        if (tm->conn) {
            tm->conn->shutdown_read();
        }
        if (tm->reader.joinable()) {
            tm->reader.join();
        }
        tm->conn.reset();  // destructor closes
    }
}

void JobManager::handle_subtask_checkpointed_(MessageReader& r) {
    auto msg = decode_subtask_checkpointed(r);
    std::vector<std::pair<JobId, std::uint64_t>> just_completed;
    std::string completed_marker_dir;
    // Phase 30b: a group whose member failed gets AbortCheckpoint
    // broadcast to every TM hosting any of its members. Collected
    // under the lock; sent outside it.
    std::vector<std::pair<JobId, std::uint64_t>> groups_to_abort;
    // Phase 29f: BeginRescale frames queued by the checkpoint-completed
    // path. When the cutover checkpoint ack closes, any operator still
    // in Preparing advances to Draining and we send BeginRescale to
    // every TM hosting it.
    std::vector<PendingDeploy> rescale_frames;
    {
        std::lock_guard lock(mu_);
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
        ckpt_it->second.erase(key);

        // Phase 30b: commit-group progress accounting. If this subtask
        // belongs to a commit_group, update group state. A failed ack
        // aborts the whole group; we mark it and queue a broadcast.
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

        // Phase 30d (metrics-coverage pass): per-subtask snapshot ack
        // counters fire regardless of whether the whole checkpoint
        // completes - they tell us how many subtask snapshots came
        // back ok vs failed, independent of the JM-level
        // completion path.
        if (msg.ok) {
            clink::metrics::ckpt::subtask_ack_ok();
        } else {
            clink::metrics::ckpt::subtask_ack_failure();
        }

        if (ckpt_it->second.empty()) {
            job.latest_completed_checkpoint_id =
                std::max(job.latest_completed_checkpoint_id, msg.checkpoint_id);
            just_completed.emplace_back(msg.job_id, msg.checkpoint_id);
            completed_marker_dir = job.checkpoint.checkpoint_dir;
            job.pending_checkpoint_acks.erase(ckpt_it);
            if (auto sit = job.pending_checkpoint_start_times.find(msg.checkpoint_id);
                sit != job.pending_checkpoint_start_times.end()) {
                const auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - sit->second)
                                        .count();
                clink::metrics::ckpt::completed(static_cast<std::uint64_t>(dur_ms));
                job.pending_checkpoint_start_times.erase(sit);
            } else {
                clink::metrics::ckpt::completed(0);
            }

            // Phase 29f: any operator in Preparing uses THIS
            // checkpoint as its cutover_checkpoint. Advance to
            // Draining and dispatch BeginRescale to every TM hosting
            // an old subtask of the op. The drain emits DrainMarker,
            // which closes the source-runner loop, which triggers
            // SubtaskFinished, which (29d-3) calls mark_old_drained,
            // which (29f) fires dispatch_cutover_deploy_locked_ when
            // the last old subtask drains.
            if (job.rescale_coordinator) {
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
    // Phase 29f: send BeginRescale frames outside the lock. Best-effort:
    // a send failure means the watchdog will catch the TM loss; the
    // coordinator's rescale will time out from the user's POV (no
    // dedicated timeout wired here yet) and a future re-request will
    // start fresh.
    for (auto& f : rescale_frames) {
        if (f.conn)
            send_frame(*f.conn, f.frame);
    }

    // Phase 30b: broadcast AbortCheckpoint to every TM hosting tasks
    // for this job. Each TM dispatches to its registered abort
    // callbacks; non-group sinks ignore (their Sink::on_abort is the
    // default no-op). We send abort BEFORE the commit broadcast below
    // so the receiving TM processes the abort first; aborted-group
    // sinks then no-op on the following CommitCheckpoint.
    for (const auto& [jid, ckpt_id] : groups_to_abort) {
        std::vector<network::Connection*> tm_conns;
        {
            std::lock_guard lock(mu_);
            auto job_it = jobs_.find(jid);
            if (job_it != jobs_.end()) {
                for (const auto& [tm_id, _] : job_it->second->tasks_by_tm) {
                    auto tm_it = registered_.find(tm_id);
                    if (tm_it != registered_.end() && !tm_it->second->lost && tm_it->second->conn) {
                        tm_conns.push_back(tm_it->second->conn.get());
                    }
                }
            }
        }
        AbortCheckpointMsg ac;
        ac.job_id = jid;
        ac.checkpoint_id = ckpt_id;
        const auto frame = encode_frame(MessageKind::AbortCheckpoint, ac);
        for (auto* c : tm_conns)
            send_frame(*c, frame);
    }
    // Write the COMPLETED marker outside the lock so a slow filesystem
    // doesn't block reader threads.
    for (const auto& [jid, ckpt_id] : just_completed) {
        if (!completed_marker_dir.empty()) {
            std::error_code ec;
            std::filesystem::path marker = std::filesystem::path{completed_marker_dir} /
                                           ("COMPLETED-" + std::to_string(ckpt_id));
            std::filesystem::create_directories(marker.parent_path(), ec);
            std::ofstream out(marker);
            out << "job=" << jid << "\ncheckpoint=" << ckpt_id << "\n";
        }
        // Phase-2 of the 2PC sink protocol: broadcast CommitCheckpoint
        // to every TM hosting tasks for this job. The marker write
        // ordering matters - by the time TMs commit their pre-staged
        // transactions, the marker is durable, so a crash mid-broadcast
        // still lets recovery find COMPLETED-N and commit on restore.
        std::vector<network::Connection*> tm_conns;
        {
            std::lock_guard lock(mu_);
            auto job_it = jobs_.find(jid);
            if (job_it != jobs_.end()) {
                for (const auto& [tm_id, _] : job_it->second->tasks_by_tm) {
                    auto tm_it = registered_.find(tm_id);
                    if (tm_it != registered_.end() && !tm_it->second->lost && tm_it->second->conn) {
                        tm_conns.push_back(tm_it->second->conn.get());
                    }
                }
            }
        }
        CommitCheckpointMsg cc;
        cc.job_id = jid;
        cc.checkpoint_id = ckpt_id;
        const auto frame = encode_frame(MessageKind::CommitCheckpoint, cc);
        for (auto* c : tm_conns)
            send_frame(*c, frame);
    }
    // Wake anyone waiting on latest_completed_checkpoint_id to advance
    // (take_savepoint, recovery probes, tests).
    cv_.notify_all();
}

void JobManager::checkpoint_trigger_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        // Visit every job with periodic checkpointing enabled. Each job
        // wakes at its own interval - we use the minimum live interval
        // as the loop's sleep so we don't oversleep any job.
        std::chrono::milliseconds sleep_for{500};
        std::vector<std::tuple<JobId, std::uint64_t, std::vector<std::string>>> to_trigger;
        {
            std::lock_guard lock(mu_);
            for (auto& [jid, job_ptr] : jobs_) {
                auto& job = *job_ptr;
                if (job.checkpoint.checkpoint_dir.empty() || job.checkpoint.interval_ms <= 0) {
                    continue;
                }
                if (job.completion_signalled) {
                    continue;
                }
                // Hold off triggering until peer updates have been
                // resolved. Before that the subtasks are not yet
                // running, so a barrier would arrive before any source
                // injectors are registered. With peer_updates_sent the
                // chain is at least up; the TM still queues triggers
                // that race the source's own startup.
                if (!job.peer_updates_sent) {
                    const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                    if (interval < sleep_for) {
                        sleep_for = interval;
                    }
                    continue;
                }
                const auto interval = std::chrono::milliseconds{job.checkpoint.interval_ms};
                if (interval < sleep_for) {
                    sleep_for = interval;
                }
                const auto next_id = job.next_checkpoint_id++;
                std::unordered_set<std::string> pending;
                for (const auto& [key, _] : job.task_records) {
                    pending.insert(key);
                }
                job.pending_checkpoint_acks[next_id] = std::move(pending);
                job.pending_checkpoint_start_times[next_id] = std::chrono::steady_clock::now();
                clink::metrics::ckpt::triggered();
                std::vector<std::string> tm_ids;
                for (const auto& [tm_id, _] : job.tasks_by_tm) {
                    tm_ids.push_back(tm_id);
                }
                to_trigger.emplace_back(jid, next_id, std::move(tm_ids));
            }
        }
        for (const auto& [jid, ckpt_id, tm_ids] : to_trigger) {
            TriggerCheckpointMsg m;
            m.job_id = jid;
            m.checkpoint_id = ckpt_id;
            const auto frame = encode_frame(MessageKind::TriggerCheckpoint, m);
            for (const auto& tm_id : tm_ids) {
                network::Connection* c = nullptr;
                {
                    std::lock_guard lock(mu_);
                    auto it = registered_.find(tm_id);
                    if (it != registered_.end() && !it->second->lost && it->second->conn) {
                        c = it->second->conn.get();
                    }
                }
                if (c != nullptr) {
                    send_frame(*c, frame);
                }
            }
        }
        std::this_thread::sleep_for(sleep_for);
    }
}

}  // namespace clink::cluster
