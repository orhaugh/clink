// clink_node - one binary that runs as either JobManager or
// TaskManager based on `--role`. The binary itself ships no job-
// specific code: TMs auto-register the generic subtask role from the
// OperatorRegistry; the JM accepts pipelines submitted via the
// programmatic StreamExecutionEnvironment API (see
// include/clink/api/stream_execution_environment.hpp).
//
// Job submission is intentionally NOT exposed here.  doesn't
// configure jobs via JSON files and clink follows suit: applications
// link against libclink, build their pipeline with the typed fluent
// API, and submit via clink::application::JobSubmitter. See
// tests/integration/test_stream_env_end_to_end.cpp for the shape.
//
// Examples
//   # Run the JobManager on the default port (6123).
//   clink_node --role=jm
//
//   # Run a TaskManager that finds the JM at 127.0.0.1:6123.
//   clink_node --role=tm --id=tm-a
//
// All --port / --jm-port / --jm-host flags accept overrides.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/ha_coordinator.hpp"
#ifdef CLINK_LINKED_ETCD
#include "clink/etcd/etcd_ha_coordinator.hpp"
#endif
#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/task_manager.hpp"
#ifdef CLINK_HAS_HTTP
#include "clink/cluster/snapshots.hpp"
#include "clink/http/dashboard_assets.hpp"
#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"
#include "clink/http/json_writer.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/metrics/prometheus.hpp"
#endif
#include "clink/plugin/abi_version.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/event_bus.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/network/network_socket.hpp"

#ifdef CLINK_HAS_HTTP
#include <condition_variable>
#include <deque>
#endif

#ifdef CLINK_LINKED_KAFKA
#include "clink/kafka/install.hpp"
#endif
#ifdef CLINK_LINKED_TLS
#include "clink/runtime/network/tls_connection.hpp"
#endif
#ifdef CLINK_LINKED_POSTGRES
#include "clink/postgres/install.hpp"
#endif
#ifdef CLINK_LINKED_CLICKHOUSE
#include "clink/clickhouse/install.hpp"
#endif
#ifdef CLINK_LINKED_S3
#include "clink/s3/install.hpp"
#endif
#ifdef CLINK_LINKED_ROCKSDB
#include "clink/rocksdb/install.hpp"
#endif
#ifdef CLINK_LINKED_ROCKSDB_S3
#include "clink/rocksdb_s3/install.hpp"
#endif
#ifdef CLINK_LINKED_SQL
#include "clink/sql/install.hpp"
#endif

namespace {

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

// Process-wide "shutdown requested" flag the role mainloops poll. A
// SIGTERM / SIGINT handler sets it; the loop wakes within at most one
// poll interval (200ms) and calls JM::stop() / TM::stop() so the
// listener fd closes, the accept thread joins, in-flight subtasks
// drain via their existing cancellation paths, and the binary exits 0.
// Without this, the existing `while (true) sleep_for(1h)` made
// graceful container/k8s lifecycle impossible - every shutdown was a
// SIGKILL from above.
std::atomic<bool> g_shutdown_requested{false};

extern "C" void clink_shutdown_signal_handler(int /*sig*/) {
    g_shutdown_requested.store(true, std::memory_order_release);
}

void install_shutdown_signal_handler() {
    struct sigaction sa{};
    sa.sa_handler = &clink_shutdown_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    // SIGTERM is the canonical container/k8s shutdown signal; SIGINT
    // handles the ctrl-C development case. SIGHUP optional - leave to
    // default (terminate) so a closed terminal still kills the node.
    ::sigaction(SIGTERM, &sa, nullptr);
    ::sigaction(SIGINT, &sa, nullptr);
}

void wait_for_shutdown(std::chrono::seconds max_duration = std::chrono::seconds{0}) {
    const auto deadline = max_duration.count() > 0 ? std::chrono::steady_clock::now() + max_duration
                                                   : std::chrono::steady_clock::time_point::max();
    while (!g_shutdown_requested.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(200ms);
    }
}

#ifdef CLINK_HAS_HTTP
// JSON-escape a string so it's safe to embed inside a "..."
// literal. v1 only escapes the structural chars; non-ASCII passes
// through as-is (httplib serves UTF-8 by default).
std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
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
    return out;
}

// Compose a /api/v1/health JSON body for a role. The role-tag (jm/tm)
// is the discriminator the dashboard uses to decide which view to
// render. uptime is computed from a steady-clock anchor taken when
// the role starts. ok is always true today; future readiness checks
// (state backend reachable, etc.) flip it false.
std::string make_health_body(std::string_view role,
                             std::chrono::steady_clock::time_point start,
                             std::uint16_t bound_port) {
    const auto uptime_s =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
            .count();
    std::string body;
    body.reserve(128);
    body += R"({"role":")";
    body += json_escape(role);
    body += R"(","ok":true,"uptime_s":)";
    body += std::to_string(uptime_s);
    body += R"(,"bound_port":)";
    body += std::to_string(bound_port);
    body += "}";
    return body;
}

// ----- JSON serializers for the read API -----
//
// Each takes a plain snapshot value-type and a JsonWriter and writes
// one object. Keeps clink_node.cpp the only translation unit aware
// of the JSON shape - adding fields means touching both the snapshot
// struct (snapshots.hpp) and one of these serializers.

void write_tm_summary(clink::http::JsonWriter& w, const clink::cluster::TmSummary& t) {
    w.begin_object();
    w.kv("tm_id", t.tm_id);
    w.kv("data_host", t.data_host);
    w.kv("slot_capacity", t.slot_capacity);
    w.kv("slots_in_use", t.slots_in_use);
    w.kv("lost", t.lost);
    w.kv("http_port", t.http_port);
    w.end_object();
}

void write_cluster(clink::http::JsonWriter& w, const clink::cluster::ClusterSnapshot& s) {
    w.begin_object();
    w.kv("bind_host", s.bind_host);
    w.kv("advertise_host", s.advertise_host);
    w.kv("control_port", s.control_port);
    w.kv("total_slot_capacity", s.total_slot_capacity);
    w.kv("slots_in_use", s.slots_in_use);
    w.kv("jobs_total", s.jobs_total);
    w.kv("jobs_running", s.jobs_running);
    w.kv("jobs_completed", s.jobs_completed);
    w.key("task_managers").begin_array();
    for (const auto& tm : s.task_managers) {
        write_tm_summary(w, tm);
    }
    w.end_array();
    w.end_object();
}

void write_job_summary(clink::http::JsonWriter& w, const clink::cluster::JobSummary& j) {
    w.begin_object();
    w.kv("id", j.id);
    w.kv("expected_completion", j.expected_completion);
    w.kv("completed_count", j.completed_count);
    w.kv("completion_signalled", j.completion_signalled);
    w.kv("cancel_requested", j.cancel_requested);
    w.kv("error_count", j.error_count);
    w.end_object();
}

void write_job_detail(clink::http::JsonWriter& w, const clink::cluster::JobDetail& j) {
    w.begin_object();
    w.kv("id", j.id);
    w.kv("expected_completion", j.expected_completion);
    w.kv("completed_count", j.completed_count);
    w.kv("completion_signalled", j.completion_signalled);
    w.kv("cancel_requested", j.cancel_requested);
    w.key("errors").begin_array();
    for (const auto& e : j.errors) {
        w.string_value(e);
    }
    w.end_array();
    w.key("tasks").begin_array();
    for (const auto& t : j.tasks) {
        w.begin_object();
        w.kv("role", t.role);
        w.kv("subtask_idx", t.subtask_idx);
        w.kv("tm_id", t.tm_id);
        w.end_object();
    }
    w.end_array();
    w.kv("latest_completed_checkpoint_id", j.latest_completed_checkpoint_id);
    w.key("pending_checkpoint_ids").begin_array();
    for (auto id : j.pending_checkpoint_ids) {
        w.uint_value(id);
    }
    w.end_array();
    w.end_object();
}

void write_tm_snapshot(clink::http::JsonWriter& w, const clink::cluster::TmSnapshot& t) {
    w.begin_object();
    w.kv("tm_id", t.tm_id);
    w.kv("data_host", t.data_host);
    w.kv("slot_capacity", t.slot_capacity);
    w.kv("slots_in_use", t.slots_in_use);
    w.kv("jm_host", t.jm_host);
    w.kv("jm_port", t.jm_port);
    w.kv("active_subtasks", t.active_subtasks);
    w.end_object();
}

void write_subtask_record(clink::http::JsonWriter& w, const clink::cluster::SubtaskRecord& r) {
    w.begin_object();
    w.kv("job_id", r.job_id);
    w.kv("role", r.role);
    w.kv("subtask_idx", r.subtask_idx);
    w.kv("status", r.status);
    w.end_object();
}

void write_jm_config(clink::http::JsonWriter& w, const clink::cluster::JobManager::Config& c) {
    w.begin_object();
    w.kv("bind_host", c.bind_host);
    w.kv("advertise_host", c.advertise_host);
    w.kv("heartbeat_timeout_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(c.heartbeat_timeout).count());
    w.kv("watchdog_interval_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(c.watchdog_interval).count());
    w.kv("submit_wait_for_slots_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(c.submit_wait_for_slots).count());
    w.kv("restart_drain_timeout_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(c.restart_drain_timeout).count());
    w.end_object();
}

void write_tm_config(clink::http::JsonWriter& w, const clink::cluster::TaskManager::Config& c) {
    w.begin_object();
    w.kv("slot_count", c.slot_count);
    w.kv("heartbeat_interval_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(c.heartbeat_interval).count());
    w.end_object();
}

// Build a proxy response for JM-side /api/v1/tms/:id/<remote_path>.
// Pulls the (host, port) for `tm_id` from the JM's registry; 404 if
// the TM is unknown / lost / didn't enable HTTP. Forwards the GET
// POST /api/v1/jobs upload handler. Multipart form with one `job_so`
// file part and an optional `job_name` text part. Writes the bytes to
// a temp file, loads them via PluginLoader (which runs the user's
// build_fn under call_once and surfaces errors), extracts the
// JobGraphSpec via clink_job_build, and calls JobManager::submit_job
// with the bytes as a PluginBinary so every TM dlopens the same .so.
//
// Returns 200 with {"job_id":N,"name":"...","ok":true} on success,
// 400 on bad request (missing file), 500 on build/submit failure.
clink::http::HttpResponse handle_submit_job(clink::cluster::JobManager& jm,
                                            const clink::http::HttpRequest& req) {
    clink::http::HttpResponse resp;
    auto fail = [&](int status, std::string msg) {
        resp.status = status;
        resp.body = std::string{R"({"ok":false,"error":")"} + json_escape(msg) + "\"}";
        return resp;
    };

    auto it = req.files.find("job_so");
    if (it == req.files.end() || it->second.content.empty()) {
        return fail(400, "multipart field 'job_so' is required");
    }
    const auto& upload = it->second;

    // Pick a job name: explicit job_name part wins, else uploaded
    // filename minus extension, else "job".
    std::string job_name = "job";
    if (auto nit = req.files.find("job_name");
        nit != req.files.end() && !nit->second.content.empty()) {
        job_name = nit->second.content;
    } else if (auto qit = req.query.find("name"); qit != req.query.end()) {
        job_name = qit->second;
    } else if (!upload.filename.empty()) {
        std::filesystem::path p{upload.filename};
        job_name = p.stem().string();
    }

    // Write bytes to a temp file so PluginLoader (path-based API) can
    // dlopen. Per-PID temp dir keeps concurrent submits from colliding;
    // the file name is unique by upload-byte-hash.
    namespace fs = std::filesystem;
    const auto temp_root =
        fs::temp_directory_path() / ("clink-uploads-" + std::to_string(::getpid()));
    std::error_code ec;
    fs::create_directories(temp_root, ec);
    std::span<const std::byte> upload_bytes(
        reinterpret_cast<const std::byte*>(upload.content.data()), upload.content.size());
    const auto hash = clink::cluster::fnv1a_64_hex(upload_bytes);
    const auto temp_path = temp_root / (hash + ".so");
    if (!fs::exists(temp_path)) {
        std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            return fail(500, "failed to write temp .so");
        }
        out.write(upload.content.data(), static_cast<std::streamsize>(upload.content.size()));
        out.close();
    }

    // Per-job bundle: plugin registrations land here, not in the
    // process-wide singleton (cross-job isolation per HTTP-1 work).
    auto bundle = std::make_unique<clink::cluster::JobBundle>();
    auto bundle_preg = bundle->as_plugin_registry();
    auto load_result =
        clink::cluster::PluginLoader::default_instance().load_into(temp_path.string(), bundle_preg);
    if (!load_result.ok) {
        return fail(500, "dlopen/register failed: " + load_result.error);
    }

    using JobBuildFn = int (*)(const char**, std::size_t*);
    JobBuildFn job_build = nullptr;
    auto sym = ::dlsym(load_result.plugin.dl_handle, "clink_job_build");
    if (sym == nullptr) {
        return fail(400, ".so does not export clink_job_build (built with CLINK_REGISTER_JOB?)");
    }
    std::memcpy(&job_build, &sym, sizeof(job_build));

    const char* graph_json_data = nullptr;
    std::size_t graph_json_size = 0;
    if (job_build(&graph_json_data, &graph_json_size) != 0 || graph_json_data == nullptr ||
        graph_json_size == 0) {
        return fail(500, "clink_job_build returned no graph");
    }
    clink::cluster::JobGraphSpec graph;
    try {
        graph =
            clink::cluster::JobGraphSpec::from_json(std::string{graph_json_data, graph_json_size});
    } catch (const std::exception& e) {
        return fail(500, std::string{"failed to parse JobGraphSpec: "} + e.what());
    }

    std::vector<clink::cluster::PluginBinary> plugins;
    plugins.push_back(clink::cluster::make_plugin_binary_from_file(temp_path.string(), job_name));

    std::uint64_t job_id = 0;
    try {
        job_id = jm.submit_job(graph,
                               clink::cluster::OperatorRegistry::default_instance(),
                               std::move(plugins),
                               clink::cluster::CheckpointConfig{},
                               std::move(bundle),
                               /*notify_client_conn=*/nullptr);
    } catch (const std::exception& e) {
        return fail(500, std::string{"submit_job threw: "} + e.what());
    }

    clink::http::JsonWriter w;
    w.begin_object();
    w.kv("ok", true);
    w.kv("job_id", static_cast<std::int64_t>(job_id));
    w.kv("name", job_name);
    w.end_object();
    resp.body = w.str();
    return resp;
}

// POST /api/v1/jobs/spec - JSON-body job submission. The body is a
// JobGraphSpec JSON document; no plugin .so. Use this for SQL-
// compiled jobs and any other workflow that only references the
// built-in operator factories already registered on every TM.
//
// Optional ?name=<job_name> picks the display name (defaults to
// "sql_job"). Returns 200 with {ok:true,job_id,name} on success,
// 400 on bad JSON, 500 on submit failure.
clink::http::HttpResponse handle_submit_spec(clink::cluster::JobManager& jm,
                                             const clink::http::HttpRequest& req) {
    clink::http::HttpResponse resp;
    auto fail = [&](int status, std::string msg) {
        resp.status = status;
        resp.body = std::string{R"({"ok":false,"error":")"} + json_escape(msg) + "\"}";
        return resp;
    };

    if (req.body.empty()) {
        return fail(400, "request body is required");
    }

    std::string job_name = "sql_job";
    if (auto it = req.query.find("name"); it != req.query.end() && !it->second.empty()) {
        job_name = it->second;
    }

    clink::cluster::JobGraphSpec graph;
    try {
        graph = clink::cluster::JobGraphSpec::from_json(req.body);
    } catch (const std::exception& e) {
        return fail(400, std::string{"failed to parse JobGraphSpec: "} + e.what());
    }

    std::uint64_t job_id = 0;
    try {
        job_id = jm.submit_job(graph,
                               clink::cluster::OperatorRegistry::default_instance(),
                               /*plugins=*/{},
                               clink::cluster::CheckpointConfig{},
                               /*bundle=*/nullptr,
                               /*notify_client_conn=*/nullptr);
    } catch (const std::exception& e) {
        return fail(500, std::string{"submit_job threw: "} + e.what());
    }

    clink::http::JsonWriter w;
    w.begin_object();
    w.kv("ok", true);
    w.kv("job_id", static_cast<std::int64_t>(job_id));
    w.kv("name", job_name);
    w.end_object();
    resp.body = w.str();
    return resp;
}

// Build an SseFactory that subscribes each new connection to the
// process-wide EventBus and yields one SSE chunk per event. A heartbeat
// chunk (": heartbeat\n\n" comment line) is emitted every ~15s if no
// real event has fired in that window - keeps NAT/load-balancer idle
// timers from closing the stream.
//
// Lifetime: the queue + condvar + closed-flag are owned by a shared_ptr
// captured by both the EventBus subscriber callback and the puller
// closure. When the puller is destroyed (client disconnect) it drops
// its share; the subscription (also captured) drops in the same
// destructor, after which the bus stops calling the callback. Late
// callbacks that race the unsubscribe see the queue still alive
// because the callback owns its own shared_ptr ref.
clink::http::SseFactory make_event_bus_sse_factory() {
    return [](const clink::http::HttpRequest&) -> clink::http::SsePuller {
        struct State {
            std::mutex mu;
            std::condition_variable cv;
            std::deque<clink::Event> queue;
            bool closed{false};
            std::chrono::steady_clock::time_point last_heartbeat = std::chrono::steady_clock::now();
        };
        auto state = std::make_shared<State>();
        auto sub = std::make_shared<clink::Subscription>(
            clink::events::subscribe([state](const clink::Event& e) {
                std::lock_guard l(state->mu);
                state->queue.push_back(e);
                state->cv.notify_one();
            }));

        auto first_call = std::make_shared<bool>(true);
        return [state, sub, first_call]() -> std::optional<clink::http::SseChunk> {
            // First call always returns a heartbeat synchronously. This
            // forces cpp-httplib to flush response headers + start the
            // chunked stream before we block on the event queue - without
            // it, an idle stream sits silent for up to 15s and clients
            // (curl, Fetch's response.body reader, our test harness) see
            // no bytes at all.
            if (*first_call) {
                *first_call = false;
                return clink::http::SseChunk{};
            }
            std::unique_lock l(state->mu);
            // Block up to 1s for a real event before emitting another
            // heartbeat. Short interval keeps the dashboard live while
            // also letting cpp-httplib's write path observe a closed
            // peer within a second of disconnect.
            const auto wait_until = std::chrono::steady_clock::now() + std::chrono::seconds{1};
            state->cv.wait_until(l, wait_until, [&] { return !state->queue.empty(); });
            if (!state->queue.empty()) {
                auto e = std::move(state->queue.front());
                state->queue.pop_front();
                clink::http::SseChunk c;
                c.event = std::move(e.type);
                c.data = std::move(e.payload);
                return c;
            }
            // Heartbeat: empty event+data signals comment-line keepalive.
            return clink::http::SseChunk{};
        };
    };
}

// Build a /metrics body from the process-wide MetricsRegistry. Same
// renderer on JM and TM; the metric names differ by which helpers each
// side has wired (process_metrics.hpp).
clink::http::HttpResponse make_metrics_response() {
    clink::metrics::http::request_seen();
    clink::http::HttpResponse resp;
    auto snap = clink::MetricsRegistry::global().snapshot();
    resp.body = clink::metrics::render_prometheus(snap);
    resp.content_type = clink::metrics::kPrometheusContentType;
    return resp;
}

// Render the log ring buffer as JSON for /api/v1/logs. `?level=info`
// and `?limit=200` filters are read from query params; defaults are
// info-and-above, last 200 lines.
clink::http::HttpResponse make_logs_response(const clink::http::HttpRequest& req) {
    std::string level = "info";
    std::size_t limit = 200;
    if (auto it = req.query.find("level"); it != req.query.end()) {
        level = it->second;
    }
    if (auto it = req.query.find("limit"); it != req.query.end()) {
        try {
            limit = static_cast<std::size_t>(std::stoull(it->second));
        } catch (...) {
            // fall through with default
        }
    }
    // ?since_ms=<ts> is a follow/tail cursor: return only records strictly
    // newer than ts. ?source=<prefix> filters by the component source field.
    std::int64_t since_ms = 0;
    if (auto it = req.query.find("since_ms"); it != req.query.end()) {
        try {
            since_ms = std::stoll(it->second);
        } catch (...) {
            // fall through with no cursor
        }
    }
    std::string source_prefix;
    if (auto it = req.query.find("source"); it != req.query.end()) {
        source_prefix = it->second;
    }
    auto records = clink::LogBuffer::global().tail(limit, level, since_ms, source_prefix);
    clink::http::JsonWriter w;
    w.begin_object();
    w.key("logs").begin_array();
    for (const auto& r : records) {
        w.begin_object();
        w.kv("ts_ms", static_cast<std::int64_t>(r.ts_ms));
        w.kv("level", r.level);
        w.kv("source", r.source);
        w.kv("message", r.message);
        w.end_object();
    }
    w.end_array();
    w.end_object();
    clink::http::HttpResponse resp;
    resp.body = w.str();
    return resp;
}

// Render the distinct source/component values currently in the ring as JSON
// for /api/v1/logs/components, so a UI can populate a component filter
// dropdown without scanning every record.
clink::http::HttpResponse make_log_components_response() {
    auto sources = clink::LogBuffer::global().distinct_sources();
    clink::http::JsonWriter w;
    w.begin_object();
    w.key("components").begin_array();
    for (const auto& s : sources) {
        w.string_value(s);
    }
    w.end_array();
    w.end_object();
    clink::http::HttpResponse resp;
    resp.body = w.str();
    return resp;
}

// over HttpClient and surfaces status + body to the client. 502 with
// a JSON error body on transport failure (timeout, peer dead).
clink::http::HttpResponse proxy_to_tm(const clink::cluster::JobManager& jm,
                                      const std::string& tm_id,
                                      const std::string& remote_path) {
    clink::http::HttpResponse out;
    auto target = jm.tm_http_target(tm_id);
    if (!target.has_value()) {
        out.status = 404;
        out.body =
            std::string{R"({"error":"no such TM, or TM has no HTTP listener: )"} + tm_id + "\"}";
        return out;
    }
    clink::http::HttpClient client(target->first, target->second);
    auto r = client.get(remote_path);
    if (r.status == 0) {
        out.status = 502;
        out.body = std::string{R"({"error":"proxy to TM failed: )"} + r.error + "\"}";
        return out;
    }
    out.status = r.status;
    out.body = std::move(r.body);
    // Forwarded TM responses are always JSON for our /api/v1/* routes;
    // preserve the content-type so a curl piped into jq Just Works.
    out.content_type = "application/json";
    return out;
}
#endif  // CLINK_HAS_HTTP

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
        const std::string a = argv[i];
        if (a == needle) {
            return true;
        }
    }
    return false;
}

// Build the logging config from --log-* flags. Defaults reproduce the
// pre-spdlog behaviour: info level, console (stderr) on, no file sink. The
// log level resolves --log-level, then the CLINK_LOG_LEVEL env, then "info".
clink::logging::LoggingConfig make_logging_config(int argc, char** argv, std::string node_name) {
    clink::logging::LoggingConfig cfg;
    cfg.node_name = std::move(node_name);
    std::string level = get_arg(argc, argv, "log-level", "");
    if (level.empty()) {
        if (const char* env = std::getenv("CLINK_LOG_LEVEL"); env != nullptr) {
            level = env;
        }
    }
    if (!level.empty()) {
        cfg.level = level;
    }
    cfg.file_path = get_arg(argc, argv, "log-file", "");
    cfg.max_file_size_mb = std::stoul(get_arg(argc, argv, "log-max-size-mb", "50"));
    cfg.max_files = std::stoul(get_arg(argc, argv, "log-max-files", "10"));
    cfg.compress_rotated = get_arg(argc, argv, "log-compress", "true") != "false";
    cfg.zstd_level = std::stoi(get_arg(argc, argv, "log-zstd-level", "3"));
    cfg.console = !has_flag(argc, argv, "log-no-console");
    cfg.async = get_arg(argc, argv, "log-async", "true") != "false";
    return cfg;
}

// JM mode. Bind, idle, await jobs. Stops cleanly on SIGINT/SIGTERM. The
// binary stays up until killed; jobs come and go entirely over the
// submission protocol.
int run_jm(int argc, char** argv) {
    // Initialise logging before anything emits. Configures console + optional
    // rolling (zstd-compressed) file sink + the /api/v1/logs ring; all
    // clink::log calls and the daemon diagnostics below route through it.
    clink::logging::init(make_logging_config(argc, argv, "jm"));
#ifdef CLINK_HAS_HTTP
    clink::metrics::init_jm_metrics();
    clink::metrics::init_checkpoint_metrics();
#endif
    const auto port_str = get_arg(argc, argv, "port", std::to_string(kDefaultJobManagerPort));
    const auto bind_host = get_arg(argc, argv, "bind-host", "127.0.0.1");
    const auto advertise_host = get_arg(argc, argv, "advertise-host", bind_host);
    const auto duration_str = get_arg(argc, argv, "stop-after", "");
    const auto http_port_str = get_arg(argc, argv, "http-port", "0");
    const auto http_bind = get_arg(argc, argv, "http-bind", "127.0.0.1");
    // TLS for the control-plane listener. If --tls-cert is given, the
    // JM presents this cert to connecting TMs and clients, and accepts
    // only TLS handshakes. --tls-client-ca turns on mTLS (server
    // requires + verifies a TM/client cert).
    const auto tls_cert = get_arg(argc, argv, "tls-cert", "");
    const auto tls_key = get_arg(argc, argv, "tls-key", "");
    const auto tls_client_ca = get_arg(argc, argv, "tls-client-ca", "");
    // HA: shared directory used by the FileHaCoordinator for leader
    // election (leader.lock + active-leader.json) and job-manifest
    // persistence (jobs/<id>/manifest.json + plugin-*.so). When set
    // along with a non-zero --port, this JM races for leadership; if
    // it wins, it advertises the bound port and accepts TM connections.
    // If a previous leader had submitted jobs, on takeover it scans
    // <ha-dir>/jobs and replays each one with restore_from at the
    // latest checkpoint.
    const auto ha_dir = get_arg(argc, argv, "ha-dir", "");
    // Etcd-backed HA: when --etcd-endpoints is provided, the JM uses
    // the etcd coordinator for leader election instead of the file
    // lock. --ha-dir is STILL required for job manifest persistence -
    // etcd handles the election primitive, but plugin .so bytes are
    // too large to stash in etcd values and stay on shared/local
    // disk. --etcd-cluster names the logical cluster; the leader key
    // is /clink/jm/<cluster>/leader so multiple clink deployments
    // can share one etcd.
    const auto etcd_endpoints = get_arg(argc, argv, "etcd-endpoints", "");
    const auto etcd_cluster = get_arg(argc, argv, "etcd-cluster", "default");
    const auto etcd_ttl_str = get_arg(argc, argv, "etcd-lease-ttl-s", "10");
    // Upper bound on how long a job may sit draining survivors before a
    // TM-loss restart fires; on expiry the JM fails the job rather than
    // wedge on a hung survivor. Tunable per deployment.
    const auto restart_drain_timeout_str = get_arg(argc, argv, "restart-drain-timeout-ms", "30000");
    // How long the JM waits without hearing from a TM before declaring it
    // lost (and, if checkpointing + restart are configured, restarting the
    // job from the latest checkpoint). The default is conservative; lower
    // it to detect a crashed TM faster (it must stay above the TM's
    // heartbeat_interval, 500ms, to avoid false positives). The watchdog
    // poll cadence is tunable too.
    const auto heartbeat_timeout_str = get_arg(argc, argv, "heartbeat-timeout-ms", "5000");
    const auto watchdog_interval_str = get_arg(argc, argv, "watchdog-interval-ms", "200");
    // Cluster-level default state backend for jobs that submit without their
    // own --state-backend. Empty (default) keeps the legacy resolution
    // (memory / file-from-checkpoint-dir). Set it to a deferring backend to
    // make the async/disaggregated path the default cluster-wide, e.g.
    // --default-state-backend=remote-read://bucket/prefix. Note disagg-local://
    // is process-local + non-durable (dev/test only); use a durable tier here
    // on a production cluster. A per-job --state-backend still overrides.
    const auto default_state_backend = get_arg(argc, argv, "default-state-backend", "");

    JobManager::Config cfg;
    cfg.bind_host = bind_host;
    cfg.advertise_host = advertise_host;
    cfg.heartbeat_timeout = std::chrono::milliseconds{std::stoll(heartbeat_timeout_str)};
    cfg.watchdog_interval = std::chrono::milliseconds{std::stoll(watchdog_interval_str)};
    cfg.restart_drain_timeout = std::chrono::milliseconds{std::stoll(restart_drain_timeout_str)};
    cfg.default_state_backend_uri = default_state_backend;
    if (!ha_dir.empty()) {
        // Takeover recovery races a TM restart: the standby tries to
        // resubmit persisted jobs as soon as it wins leadership, but
        // the supervisor needs a beat to restart the TM whose
        // connection died with the old leader. Wait up to 15s for
        // slots before failing the recovery.
        cfg.submit_wait_for_slots = 15s;
    }

    JobManager jm(cfg);
    if (!ha_dir.empty()) {
        jm.set_ha_dir(ha_dir);
    }
#ifdef CLINK_LINKED_TLS
    if (!tls_cert.empty() && !tls_key.empty()) {
        auto server_ctx = std::make_shared<clink::network::TlsServerContext>(tls_cert, tls_key);
        if (!tls_client_ca.empty()) {
            server_ctx->set_client_ca_path(tls_client_ca);
        }
        jm.set_accept_factory([server_ctx](int listener_fd) {
            return clink::network::accept_tls_connection(listener_fd, server_ctx);
        });
        std::cout << "JM TLS enabled (cert=" << tls_cert
                  << (tls_client_ca.empty() ? "" : ", mTLS=on") << ")\n";
    }
#else
    if (!tls_cert.empty() || !tls_key.empty() || !tls_client_ca.empty()) {
        clink::log::warn("jm.tls", "--tls-* flags ignored (clink_tls not linked)");
    }
#endif
    const auto want_port = static_cast<std::uint16_t>(std::stoi(port_str));
    std::unique_ptr<clink::cluster::HaCoordinator> ha_coord;
    if (ha_dir.empty()) {
        const auto bound = jm.start(want_port);
        // Load-bearing readiness banner on STDOUT: test harnesses and operators
        // grep stdout for "JM listening". Keep it on std::cout (NOT the logger)
        // so it survives --log-no-console and is not reordered by async logging.
        std::cout << "JM listening on " << advertise_host << ":" << bound << "\n";
        std::cout.flush();
    } else {
        // HA mode: defer jm.start until this JM wins the leadership
        // election. Standby JMs just sit on the coordinator's poll
        // thread; on takeover the callback binds the control port and
        // replays any persisted jobs.
        clink::cluster::LeaderEndpoint advertise;
        advertise.host = advertise_host;
        advertise.port = want_port;
        if (!etcd_endpoints.empty()) {
#ifdef CLINK_LINKED_ETCD
            clink::cluster::EtcdHaConfig ecfg;
            ecfg.endpoints = etcd_endpoints;
            ecfg.cluster_name = etcd_cluster;
            ecfg.lease_ttl = std::chrono::seconds{std::stoi(etcd_ttl_str)};
            ha_coord = clink::cluster::make_etcd_ha_coordinator(ecfg, advertise);
            std::cout << "JM HA via etcd endpoints=" << etcd_endpoints
                      << " cluster=" << etcd_cluster << "\n";
#else
            std::cerr << "JM: --etcd-endpoints given but clink_etcd not linked; "
                         "rebuild with -DCLINK_WITH_ETCD=ON (and etcd-cpp-apiv3 installed)\n";
            return 1;
#endif
        } else {
            ha_coord = clink::cluster::make_file_ha_coordinator(ha_dir, advertise);
        }
        ha_coord->set_on_become_leader([&jm, want_port, advertise_host](std::uint64_t epoch) {
            try {
                const auto bound = jm.start(want_port);
                std::cout << "JM became leader (epoch=" << epoch << "), listening on "
                          << advertise_host << ":" << bound << "\n";
                std::cout.flush();
                jm.recover_persisted_jobs();
            } catch (const std::exception& e) {
                std::cerr << "JM HA takeover failed: " << e.what() << "\n";
            }
        });
        ha_coord->start();
        std::cout << "JM standby (ha-dir=" << ha_dir << ")\n";
        std::cout.flush();
    }

    const auto start_time = std::chrono::steady_clock::now();

#ifdef CLINK_HAS_HTTP
    // Optional HTTP server for the cluster dashboard / JSON API.
    // Lives next to the control-plane listener on a separate port so
    // it can be locked to 127.0.0.1 even when the control plane binds
    // to 0.0.0.0. Empty/zero --http-port disables.
    std::unique_ptr<clink::http::HttpServer> http_srv;
    const auto http_port = static_cast<std::uint16_t>(std::stoi(http_port_str));
    if (http_port != 0) {
        http_srv = std::make_unique<clink::http::HttpServer>();
        auto* jm_ptr = &jm;
        http_srv->get("/api/v1/health", [start_time, jm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            resp.body = make_health_body("jm", start_time, jm_ptr->bound_port());
            return resp;
        });
        http_srv->get("/api/v1/cluster", [jm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            write_cluster(w, jm_ptr->snapshot_cluster());
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/config", [jm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            write_jm_config(w, jm_ptr->config_snapshot());
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/jobs", [jm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            w.begin_object();
            w.key("jobs").begin_array();
            for (const auto& j : jm_ptr->snapshot_jobs()) {
                write_job_summary(w, j);
            }
            w.end_array();
            w.end_object();
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/jobs/:id", [jm_ptr](const clink::http::HttpRequest& req) {
            clink::http::HttpResponse resp;
            clink::cluster::JobId job_id = 0;
            if (auto it = req.path_params.find("id"); it != req.path_params.end()) {
                try {
                    job_id = static_cast<clink::cluster::JobId>(std::stoull(it->second));
                } catch (...) {
                    resp.status = 400;
                    resp.body = R"({"error":"invalid job id"})";
                    return resp;
                }
            }
            auto detail = jm_ptr->snapshot_job(job_id);
            if (!detail.has_value()) {
                resp.status = 404;
                resp.body = R"({"error":"no such job"})";
                return resp;
            }
            clink::http::JsonWriter w;
            write_job_detail(w, *detail);
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/tms", [jm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            w.begin_object();
            w.key("task_managers").begin_array();
            for (const auto& tm : jm_ptr->snapshot_tms()) {
                write_tm_summary(w, tm);
            }
            w.end_array();
            w.end_object();
            resp.body = w.str();
            return resp;
        });

        // POST /api/v1/jobs - multipart upload + submit. Closes the
        // dashboard's loop: the SPA can submit a job .so directly
        // without dropping to the clink_submit_job CLI.
        http_srv->post("/api/v1/jobs", [jm_ptr](const clink::http::HttpRequest& req) {
            clink::metrics::http::request_seen();
            return handle_submit_job(*jm_ptr, req);
        });

        // POST /api/v1/jobs/spec - JSON-body submission (no plugin .so).
        // Used by clink_submit_sql and any other tool that compiles to
        // a JobGraphSpec referencing built-in operator factories only.
        http_srv->post("/api/v1/jobs/spec", [jm_ptr](const clink::http::HttpRequest& req) {
            clink::metrics::http::request_seen();
            return handle_submit_spec(*jm_ptr, req);
        });

        // POST /api/v1/jobs/:id/cancel - HTTP equivalent of
        // clink_cancel_job. Surfaces the same ack JSON; maps
        // "no such job" / "already completed" / "already in progress"
        // to 404 / 409 / 409 so dashboards can display the right
        // outcome without sniffing the message string.
        http_srv->post("/api/v1/jobs/:id/cancel", [jm_ptr](const clink::http::HttpRequest& req) {
            clink::http::HttpResponse resp;
            clink::cluster::JobId job_id = 0;
            if (auto it = req.path_params.find("id"); it != req.path_params.end()) {
                try {
                    job_id = static_cast<clink::cluster::JobId>(std::stoull(it->second));
                } catch (...) {
                    resp.status = 400;
                    resp.body = R"({"error":"invalid job id"})";
                    return resp;
                }
            }
            const auto ack = jm_ptr->cancel_job(job_id);
            clink::http::JsonWriter w;
            w.begin_object();
            w.kv("job_id", static_cast<std::int64_t>(ack.job_id));
            w.kv("ok", ack.ok);
            w.kv("message", ack.message);
            w.end_object();
            resp.body = w.str();
            if (!ack.ok) {
                if (ack.message == "no such job") {
                    resp.status = 404;
                } else {
                    // "job already completed" / "cancel
                    // already in progress" - semantic
                    // conflict with current state.
                    resp.status = 409;
                }
            }
            return resp;
        });

        // Federation proxies: /api/v1/tms/:id/{tm,subtasks,config}
        // forward GETs to that TM's HTTP listener. Without these the
        // dashboard would have to know each TM's port and contact it
        // directly - fine on a flat network, broken behind any NAT
        // or DMZ where only the JM is reachable. The JM becomes the
        // single entry-point.
        http_srv->get("/api/v1/tms/:id/tm", [jm_ptr](const clink::http::HttpRequest& req) {
            const auto it = req.path_params.find("id");
            const auto& id = it != req.path_params.end() ? it->second : std::string{};
            return proxy_to_tm(*jm_ptr, id, "/api/v1/tm");
        });
        http_srv->get("/api/v1/tms/:id/subtasks", [jm_ptr](const clink::http::HttpRequest& req) {
            const auto it = req.path_params.find("id");
            const auto& id = it != req.path_params.end() ? it->second : std::string{};
            return proxy_to_tm(*jm_ptr, id, "/api/v1/tm/subtasks");
        });
        http_srv->get("/api/v1/tms/:id/config", [jm_ptr](const clink::http::HttpRequest& req) {
            const auto it = req.path_params.find("id");
            const auto& id = it != req.path_params.end() ? it->second : std::string{};
            return proxy_to_tm(*jm_ptr, id, "/api/v1/config");
        });
        http_srv->get("/api/v1/tms/:id/metrics", [jm_ptr](const clink::http::HttpRequest& req) {
            const auto it = req.path_params.find("id");
            const auto& id = it != req.path_params.end() ? it->second : std::string{};
            auto resp = proxy_to_tm(*jm_ptr, id, "/metrics");
            if (resp.status >= 200 && resp.status < 300) {
                resp.content_type = clink::metrics::kPrometheusContentType;
            }
            return resp;
        });
        http_srv->get("/api/v1/tms/:id/logs", [jm_ptr](const clink::http::HttpRequest& req) {
            const auto it = req.path_params.find("id");
            const auto& id = it != req.path_params.end() ? it->second : std::string{};
            // Forward query string so ?level=warn&limit=50 reach the TM.
            std::string remote = "/api/v1/logs";
            if (!req.query.empty()) {
                remote += '?';
                bool first = true;
                for (const auto& [k, v] : req.query) {
                    if (!first)
                        remote += '&';
                    first = false;
                    remote += k;
                    remote += '=';
                    remote += v;
                }
            }
            return proxy_to_tm(*jm_ptr, id, remote);
        });

        // Process-level observability: /metrics scrapes the in-process
        // registry; /api/v1/logs serves the bounded log ring buffer.
        // Both also live on TMs (see run_tm). Prometheus exposition is
        // served at the un-prefixed path to match scraper conventions.
        http_srv->get("/metrics",
                      [](const clink::http::HttpRequest&) { return make_metrics_response(); });
        http_srv->get("/api/v1/logs", [](const clink::http::HttpRequest& req) {
            clink::metrics::http::request_seen();
            return make_logs_response(req);
        });
        http_srv->get("/api/v1/logs/components", [](const clink::http::HttpRequest&) {
            clink::metrics::http::request_seen();
            return make_log_components_response();
        });

        // Embedded SPA. Both `/` and `/dashboard` serve the same page
        // so curl-friendly paths (`curl host/`) and -muscle-memory
        // paths (`/dashboard`) both work. TM HTTP servers don't mount
        // this - the JM is the single human-facing entry point;
        // individual TM data is reachable through /api/v1/tms/:id/*
        // proxies.
        auto dashboard_handler = [](const clink::http::HttpRequest&) {
            clink::metrics::http::request_seen();
            clink::http::HttpResponse resp;
            resp.body = clink::http::kDashboardHtml;
            resp.content_type = "text/html; charset=utf-8";
            return resp;
        };
        http_srv->get("/", dashboard_handler);
        http_srv->get("/dashboard", dashboard_handler);

        // Server-Sent Events stream - the dashboard's live wake-up signal.
        // Federated /api/v1/tms/:id/events is deliberately NOT wired:
        // proxying a chunked stream through HttpClient::Get is non-
        // trivial (httplib's blocking response API doesn't surface the
        // body in pieces). For HTTP-5 the JM stream is the canonical
        // feed; per-TM SSE comes in HTTP-6 if the dashboard needs it.
        http_srv->sse("/api/v1/events", make_event_bus_sse_factory());

        const auto http_bound = http_srv->start(http_bind, http_port);
        std::cout << "JM HTTP on " << http_bind << ":" << http_bound << "\n";
        std::cout.flush();
    }
#else
    (void)http_port_str;
    (void)http_bind;
#endif

    // Run until either the user-requested duration elapses or the
    // process catches SIGTERM/SIGINT (handler set in main()). The JM
    // threads do all the real work; we just gate the lifetime here.
    const auto max_duration = duration_str.empty() ? std::chrono::seconds{0}
                                                   : std::chrono::seconds{std::stoi(duration_str)};
    wait_for_shutdown(max_duration);
#ifdef CLINK_HAS_HTTP
    if (http_srv) {
        http_srv->stop();
    }
#endif
    if (ha_coord)
        ha_coord->stop();
    jm.stop();
    return 0;
}

// TM mode. Connect to JM, idle, run whatever subtasks the JM deploys
// via the generic role. No job-specific roles registered here.
int run_tm(int argc, char** argv) {
#ifdef CLINK_HAS_HTTP
    clink::metrics::init_tm_metrics();
    clink::metrics::init_checkpoint_metrics();
#endif
    const auto tm_id = get_arg(argc, argv, "id");
    const auto jm_host = get_arg(argc, argv, "jm-host", "127.0.0.1");
    const auto jm_port = get_arg(argc, argv, "jm-port", std::to_string(kDefaultJobManagerPort));
    const auto data_host = get_arg(argc, argv, "data-host", "127.0.0.1");
    const auto slot_str = get_arg(argc, argv, "slots", "4");
    const auto http_port_str = get_arg(argc, argv, "http-port", "0");
    const auto http_bind = get_arg(argc, argv, "http-bind", "127.0.0.1");
    // TLS for the control-plane connection. --tls-ca turns on TLS and
    // verifies the JM cert against this CA. --tls-client-cert/--key are
    // optional mTLS material (required if the JM was started with
    // --tls-client-ca).
    const auto tls_ca = get_arg(argc, argv, "tls-ca", "");
    const auto tls_client_cert = get_arg(argc, argv, "tls-client-cert", "");
    const auto tls_client_key = get_arg(argc, argv, "tls-client-key", "");
    // HA: when set, look up the current leader endpoint from
    // <ha-dir>/active-leader.json instead of using --jm-host/--jm-port
    // directly. On JM disconnect (reader_loop_ exits), this TM exits
    // non-zero so an external supervisor (systemd/k8s/test harness)
    // can restart it; the restart re-reads active-leader.json to find
    // the new (possibly-just-elected) leader.
    const auto ha_dir = get_arg(argc, argv, "ha-dir", "");
    const auto etcd_endpoints = get_arg(argc, argv, "etcd-endpoints", "");
    const auto etcd_cluster = get_arg(argc, argv, "etcd-cluster", "default");
    if (tm_id.empty()) {
        std::cerr << "tm requires --id\n";
        return 1;
    }
    // Initialise logging now that the id is known (root logger %n = tm@<id>).
    clink::logging::init(make_logging_config(argc, argv, "tm@" + tm_id));
    std::string discovered_jm_host = jm_host;
    std::uint16_t discovered_jm_port = static_cast<std::uint16_t>(std::stoi(jm_port));
    if (!etcd_endpoints.empty() || !ha_dir.empty()) {
        std::unique_ptr<clink::cluster::HaCoordinator> coord;
        if (!etcd_endpoints.empty()) {
#ifdef CLINK_LINKED_ETCD
            clink::cluster::EtcdHaConfig ecfg;
            ecfg.endpoints = etcd_endpoints;
            ecfg.cluster_name = etcd_cluster;
            coord = clink::cluster::make_etcd_ha_coordinator(ecfg, {});
#else
            std::cerr << "TM: --etcd-endpoints given but clink_etcd not linked\n";
            return 1;
#endif
        } else {
            coord = clink::cluster::make_file_ha_coordinator(ha_dir, {});
        }
        // Poll the active-leader file for up to 10s - covers the gap
        // between TM startup and the first JM acquiring leadership.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        std::optional<clink::cluster::LeaderEndpoint> ep;
        while (std::chrono::steady_clock::now() < deadline) {
            ep = coord->current_leader_endpoint();
            if (ep.has_value())
                break;
            std::this_thread::sleep_for(100ms);
        }
        if (!ep.has_value()) {
            std::cerr << "TM: no leader visible (etcd=\"" << etcd_endpoints << "\" file=\""
                      << ha_dir << "\") after 10s\n";
            return 2;
        }
        discovered_jm_host = ep->host;
        discovered_jm_port = ep->port;
        std::cout << "TM HA: discovered leader " << discovered_jm_host << ":" << discovered_jm_port
                  << " (epoch=" << ep->epoch << ")\n";
    }

    TaskManager::Config cfg;
    cfg.heartbeat_interval = 500ms;
    cfg.slot_count = static_cast<std::uint32_t>(std::stoi(slot_str));
    // cfg.http_port is set later via set_advertised_http_port AFTER
    // the HttpServer binds - so when --http-port=0 lets the OS pick,
    // the Register frame carries the actually-bound port.
    TaskManager tm(tm_id, data_host, cfg);
#ifdef CLINK_LINKED_TLS
    if (!tls_ca.empty()) {
        auto client_ctx = std::make_shared<clink::network::TlsClientContext>(tls_ca);
        if (!tls_client_cert.empty() && !tls_client_key.empty()) {
            client_ctx->set_client_cert(tls_client_cert, tls_client_key);
        }
        tm.set_connect_factory([client_ctx](const std::string& host, std::uint16_t port) {
            return clink::network::connect_tls_connection(host, port, client_ctx);
        });
        std::cout << "TM TLS enabled (ca=" << tls_ca << (tls_client_cert.empty() ? "" : ", mTLS=on")
                  << ")\n";
    }
#else
    if (!tls_ca.empty() || !tls_client_cert.empty() || !tls_client_key.empty()) {
        clink::log::warn("tm.tls", "--tls-* flags ignored (clink_tls not linked)");
    }
#endif

    // No register_role() calls: the TaskManager constructor wired up the
    // generic subtask role, which is everything a TM needs to execute
    // any submitted graph that uses operators in the OperatorRegistry.

    const auto start_time = std::chrono::steady_clock::now();

#ifdef CLINK_HAS_HTTP
    // Start the HTTP listener BEFORE connect_to_jm so the actual bound
    // port (which may differ from the request if --http-port=0 lets
    // the OS pick) can be included in the Register frame. The JM
    // stores it for /api/v1/tms/:id/* proxy routes.
    std::unique_ptr<clink::http::HttpServer> http_srv;
    const auto http_port_req = static_cast<std::uint16_t>(std::stoi(http_port_str));
    std::uint16_t http_bound{0};
    if (http_port_req != 0) {
        http_srv = std::make_unique<clink::http::HttpServer>();
        auto* tm_ptr = &tm;
        http_srv->get("/api/v1/health", [start_time, &http_bound](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            resp.body = make_health_body("tm", start_time, http_bound);
            return resp;
        });
        http_srv->get("/api/v1/tm", [tm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            write_tm_snapshot(w, tm_ptr->snapshot_tm());
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/tm/subtasks", [tm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            w.begin_object();
            w.key("subtasks").begin_array();
            for (const auto& s : tm_ptr->snapshot_subtasks()) {
                write_subtask_record(w, s);
            }
            w.end_array();
            w.end_object();
            resp.body = w.str();
            return resp;
        });
        http_srv->get("/api/v1/config", [tm_ptr](const clink::http::HttpRequest&) {
            clink::http::HttpResponse resp;
            clink::http::JsonWriter w;
            write_tm_config(w, tm_ptr->config_snapshot());
            resp.body = w.str();
            return resp;
        });
        // Same /metrics + /api/v1/logs surface as JM; TM-side metrics
        // are populated by run_task_ (subtasks_*) and connect_to_jm
        // (slot_capacity).
        http_srv->get("/metrics",
                      [](const clink::http::HttpRequest&) { return make_metrics_response(); });
        http_srv->get("/api/v1/logs", [](const clink::http::HttpRequest& req) {
            clink::metrics::http::request_seen();
            return make_logs_response(req);
        });
        http_srv->get("/api/v1/logs/components", [](const clink::http::HttpRequest&) {
            clink::metrics::http::request_seen();
            return make_log_components_response();
        });
        http_srv->sse("/api/v1/events", make_event_bus_sse_factory());
        http_bound = http_srv->start(http_bind, http_port_req);
        std::cout << "TM HTTP on " << http_bind << ":" << http_bound << "\n";
        std::cout.flush();
        // Tell the TaskManager which port to advertise to the JM at
        // register time. Must happen BEFORE connect_to_jm.
        tm.set_advertised_http_port(http_bound);
    }
#else
    (void)http_port_str;
    (void)http_bind;
#endif

    tm.connect_to_jm(discovered_jm_host, discovered_jm_port);
    // Load-bearing readiness banner on STDOUT: bench_failover_coldstart greps the
    // child's captured stdout for "registered" at several sites. Keep it on
    // std::cout (NOT the logger) so it survives --log-no-console and is not
    // reordered by async logging.
    std::cout << "TM " << tm_id << " registered with " << discovered_jm_host << ":"
              << discovered_jm_port << "\n";
    std::cout.flush();

    // Idle until SIGTERM/SIGINT - or, when --ha-dir is set, until the
    // JM disconnects. On disconnect we exit non-zero so a supervisor
    // restarts this TM; the next process reads active-leader.json and
    // picks up whatever JM has taken over leadership.
    const bool ha_mode = !ha_dir.empty();
    while (!g_shutdown_requested.load(std::memory_order_acquire)) {
        if (ha_mode && tm.disconnected()) {
            std::cerr << "TM " << tm_id << ": JM disconnected; exiting for restart\n";
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
#ifdef CLINK_HAS_HTTP
    if (http_srv) {
        http_srv->stop();
    }
#endif
    tm.stop();
    return (ha_mode && tm.disconnected() && !g_shutdown_requested.load()) ? 2 : 0;
}

}  // namespace

// Wire every linked clink::<x> impl into the process-wide registries
// at startup. The CLINK_LINKED_<X> defines are set by the top-level
// CMakeLists when the corresponding impl target was built.
void install_linked_impls() {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
#ifdef CLINK_LINKED_KAFKA
    clink::kafka::install(reg);
#endif
#ifdef CLINK_LINKED_POSTGRES
    clink::postgres::install(reg);
#endif
#ifdef CLINK_LINKED_CLICKHOUSE
    clink::clickhouse::install(reg);
#endif
#ifdef CLINK_LINKED_S3
    clink::s3::install(reg);
    clink::s3::install_state_backend();  // remote-read:// disaggregated state backend
#endif
    // RocksDB is unconditionally linked into clink_node (matches
    // bundled rocksdb-jni model). install() registers the
    // "rocksdb://" URI scheme on the global StateBackendFactory.
    clink::rocksdb::install();
#ifdef CLINK_LINKED_ROCKSDB_S3
    // Remote-state schemes (s3+rocksdb://, changelog+s3+rocksdb://,
    // changelog+s3://). Registered only when the rocksdb_s3 target is linked.
    clink::rocksdb_s3::install();
#endif
#ifdef CLINK_LINKED_SQL
    clink::sql::install(reg);
#endif
}

int main(int argc, char** argv) {
    try {
        if (has_flag(argc, argv, "version")) {
            std::cout << "clink " << clink::plugin::kClinkVersion << " (commit "
                      << clink::plugin::kAbiHash << (clink::plugin::kAbiHashIsClean ? "" : "-dirty")
                      << ")\n";
            return 0;
        }
        if (has_flag(argc, argv, "help")) {
            std::cout << "Usage: clink_node --role={jm|tm} [options]\n"
                      << "\n"
                      << "Roles:\n"
                      << "  jm       Run a JobManager.   --port=<n>\n"
                      << "  tm       Run a TaskManager.  --id=<name> --jm-host=<h> --jm-port=<n>\n"
                      << "\n"
                      << "Jobs are submitted programmatically via the C++ API\n"
                      << "(StreamExecutionEnvironment + JobSubmitter); clink does\n"
                      << "not accept JSON job configurations.\n"
                      << "\n"
                      << "JobManager flags:\n"
                      << "  --heartbeat-timeout-ms=<n>   TM-loss detection window (default 5000).\n"
                      << "  --watchdog-interval-ms=<n>   TM-liveness poll cadence (default 200).\n"
                      << "\n"
                      << "Logging flags:\n"
                      << "  --log-level=<lvl>            trace|debug|info|warn|error|off "
                         "(default info; CLINK_LOG_LEVEL env as fallback).\n"
                      << "  --log-file=<path>            Enable the rolling file sink at "
                         "<path> (default off: console + ring only).\n"
                      << "  --log-max-size-mb=<n>        Rotate the log file at this size "
                         "(default 50).\n"
                      << "  --log-max-files=<n>          Rotated segments to keep (default 10).\n"
                      << "  --log-compress=true|false    zstd-compress rotated segments "
                         "(default true).\n"
                      << "  --log-zstd-level=<1..19>     zstd compression level (default 3).\n"
                      << "  --log-async=true|false       Async logging (default true).\n"
                      << "  --log-no-console             Disable the console (stderr) sink.\n"
                      << "\n"
                      << "Global flags:\n"
                      << "  --version    Print version + commit and exit.\n"
                      << "  --help       Print this message and exit.\n";
            return 0;
        }
        install_linked_impls();
        // Install before any role thread spawns, so SIGTERM/SIGINT
        // arrived at any point catches an idempotent flag flip.
        install_shutdown_signal_handler();
        const auto role = get_arg(argc, argv, "role");
        if (role == "jm" || role == "tm") {
            const int rc = role == "jm" ? run_jm(argc, argv) : run_tm(argc, argv);
            // Flush + join the async worker/flush threads on the clean exit
            // path. Not called from the signal handler (atomic-only); the role
            // mainloops observe the flag and return here.
            clink::logging::shutdown();
            return rc;
        }
        std::cerr << "Usage: clink_node --role={jm|tm} ... (--help for details)\n";
        return 1;
    } catch (const std::exception& e) {
        // Fatal path stays on std::cerr (synchronous) so the message is not
        // lost to an un-drained async queue as the process unwinds.
        std::cerr << "fatal: " << e.what() << "\n";
        return 99;
    }
}
