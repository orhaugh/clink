#include "clink/lineage/openlineage_exporter.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>

#include "clink/http/http_client.hpp"
#include "clink/http/json_writer.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::lineage {
namespace {

// "00000000-0000-0000-0000-<job id as 12 hex digits>". A valid UUID
// shape, deterministic per job, so a START and its COMPLETE/FAIL/ABORT
// correlate without the exporter holding any cross-event state.
std::string run_id_for(std::uint64_t job_id) {
    char buf[40];
    std::snprintf(buf,
                  sizeof(buf),
                  "00000000-0000-0000-0000-%012llx",
                  static_cast<unsigned long long>(job_id & 0xFFFFFFFFFFFFULL));
    return buf;
}

// Milliseconds since epoch -> ISO-8601 UTC with millisecond precision.
std::string iso8601(std::int64_t ts_ms) {
    if (ts_ms <= 0) {
        ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
    }
    const std::time_t secs = static_cast<std::time_t>(ts_ms / 1000);
    const int ms = static_cast<int>(ts_ms % 1000);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &secs);
#else
    gmtime_r(&secs, &tm_utc);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);
    char out[40];
    std::snprintf(out, sizeof(out), "%s.%03dZ", buf, ms);
    return out;
}

const char* event_type_for(const LineageEvent& ev) {
    if (ev.kind == LineageEvent::Kind::JobStarted) {
        return "START";
    }
    if (ev.status == "ok") {
        return "COMPLETE";
    }
    if (ev.status == "cancelled") {
        return "ABORT";
    }
    return "FAIL";  // "failed" or anything unexpected
}

void write_datasets(http::JsonWriter& w,
                    const std::vector<LineageVertex>& vertices,
                    const std::string& producer) {
    w.begin_array();
    for (const auto& v : vertices) {
        for (const auto& d : v.datasets) {
            w.begin_object();
            w.kv("namespace", d.ns);
            w.kv("name", d.name);
            // clink metadata as a single custom OpenLineage facet. A facet
            // value is an object; _producer / _schemaURL keep it valid, and
            // receivers that don't know the facet ignore it.
            w.key("facets").begin_object();
            w.key("clink").begin_object();
            w.kv("_producer", producer);
            w.kv("_schemaURL", "https://github.com/clink/clink/lineage/clink-dataset-facet.json");
            for (const auto& [k, val] : d.facets) {
                w.kv(k, val);
            }
            w.end_object();
            w.end_object();
            w.end_object();
        }
    }
    w.end_array();
}

std::string build_event_json(const LineageEvent& ev, const OpenLineageConfig& cfg) {
    http::JsonWriter w;
    w.begin_object();
    w.kv("eventType", event_type_for(ev));
    w.kv("eventTime", iso8601(ev.ts_ms));
    w.kv("producer", cfg.producer);
    w.key("run").begin_object();
    w.kv("runId", run_id_for(ev.job_id));
    w.end_object();
    w.key("job").begin_object();
    w.kv("namespace", cfg.job_namespace);
    w.kv("name", "job_" + std::to_string(ev.job_id));
    w.end_object();
    // Inputs/outputs are established by the START event; the terminal
    // events carry none (correlated by runId).
    w.key("inputs");
    write_datasets(w, ev.graph.sources, cfg.producer);
    w.key("outputs");
    write_datasets(w, ev.graph.sinks, cfg.producer);
    w.end_object();
    return w.str();
}

}  // namespace

OpenLineageExporter::OpenLineageExporter(OpenLineageConfig cfg)
    : cfg_(std::move(cfg)),
      client_(std::make_unique<http::HttpClient>(cfg_.host, cfg_.port)),
      worker_([this] { run(); }) {}

OpenLineageExporter::~OpenLineageExporter() {
    {
        std::lock_guard l(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

void OpenLineageExporter::on_event(const LineageEvent& ev) {
    auto body = build_event_json(ev, cfg_);
    {
        std::lock_guard l(mu_);
        if (outbox_.size() >= cfg_.max_queue) {
            outbox_.pop_front();  // drop oldest
            dropped_.fetch_add(1);
        }
        outbox_.push_back(std::move(body));
    }
    cv_.notify_one();
}

void OpenLineageExporter::run() {
    for (;;) {
        std::string body;
        {
            std::unique_lock l(mu_);
            cv_.wait(l, [this] { return stop_ || !outbox_.empty(); });
            if (outbox_.empty()) {
                if (stop_) {
                    return;
                }
                continue;
            }
            body = std::move(outbox_.front());
            outbox_.pop_front();
        }
        const auto resp = client_->post(cfg_.path, body);
        if (resp.status >= 200 && resp.status < 300) {
            sent_.fetch_add(1);
        } else {
            // Best-effort: a failed POST is logged and dropped (no retry in
            // v1). The outbox keeps draining so a dead receiver does not wedge
            // the worker.
            dropped_.fetch_add(1);
            log::warn("lineage.openlineage",
                      "POST " + cfg_.path + " failed: status=" + std::to_string(resp.status) +
                          (resp.error.empty() ? "" : " (" + resp.error + ")"));
        }
    }
}

bool parse_openlineage_config(const LineageListenerConfig& cfg, OpenLineageConfig& out) {
    const auto get = [&](const char* k) -> std::string {
        auto it = cfg.find(k);
        return it == cfg.end() ? std::string{} : it->second;
    };

    std::string endpoint = get("endpoint");
    if (endpoint.empty()) {
        endpoint = get("url");
    }
    if (endpoint.empty()) {
        return false;
    }

    std::uint16_t default_port = 80;
    if (endpoint.rfind("https://", 0) == 0) {
        endpoint = endpoint.substr(8);
        default_port = 443;
        log::warn("lineage.openlineage",
                  "https endpoint configured but the HTTP client is plain HTTP; "
                  "connection will likely fail");
    } else if (endpoint.rfind("http://", 0) == 0) {
        endpoint = endpoint.substr(7);
    }
    // Trim any path component off the authority; the POST path is separate.
    if (const auto slash = endpoint.find('/'); slash != std::string::npos) {
        endpoint = endpoint.substr(0, slash);
    }
    std::string host = endpoint;
    std::uint16_t port = default_port;
    if (const auto colon = endpoint.find(':'); colon != std::string::npos) {
        host = endpoint.substr(0, colon);
        try {
            port = static_cast<std::uint16_t>(std::stoi(endpoint.substr(colon + 1)));
        } catch (...) {
            port = default_port;
        }
    }
    if (host.empty()) {
        return false;
    }

    out.host = host;
    out.port = port;
    if (const auto p = get("path"); !p.empty()) {
        out.path = p;
    }
    if (const auto ns = get("namespace"); !ns.empty()) {
        out.job_namespace = ns;
    }
    if (const auto pr = get("producer"); !pr.empty()) {
        out.producer = pr;
    }
    if (const auto mq = get("max_queue"); !mq.empty()) {
        try {
            out.max_queue = static_cast<std::size_t>(std::stoul(mq));
        } catch (...) {
            // keep default
        }
    }
    return true;
}

void register_openlineage_listener(LineageListenerRegistry& registry) {
    registry.register_factory(
        "openlineage", [](const LineageListenerConfig& cfg) -> std::unique_ptr<LineageListener> {
            OpenLineageConfig olc;
            if (!parse_openlineage_config(cfg, olc)) {
                log::warn("lineage.openlineage",
                          "openlineage listener requested but no 'endpoint' configured; skipping");
                return nullptr;
            }
            return std::make_unique<OpenLineageExporter>(std::move(olc));
        });
}

}  // namespace clink::lineage
