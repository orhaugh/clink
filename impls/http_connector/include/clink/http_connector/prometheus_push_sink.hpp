#pragma once

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_bulk_post.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::http_connector {

// Options for the Prometheus Pushgateway sink. job is mandatory (it is the first
// path segment of the push URL and, with grouping, identifies the group the
// push replaces). One sink instance pushes ONE gauge metric name; each input
// record contributes one sample (value_field -> the float; the other scalar
// fields -> labels).
struct PromPushOptions {
    std::string url;                         // pushgateway base scheme://host[:port] (required)
    std::string job;                         // grouping job -> /metrics/job/<job> (required)
    std::string grouping;                    // extra static path labels "k=v,k2=v2" (optional)
    std::string metric_name{"clink_value"};  // the single gauge name
    std::string value_field{"value"};        // record field carrying the float value
    std::string help;                        // optional # HELP text
    std::string headers;                     // optional "K: V; ..." (e.g. a fronting-proxy auth)
    bool verify_tls{true};
    std::size_t batch_records{500};  // flush when this many DISTINCT series buffer
    int max_retries{4};
    std::string name{"prometheus_sink"};
};

// Prometheus Pushgateway sink. Unlike the bulk POST sinks this buffers into a
// per-series map (keyed by the rendered label set) rather than concatenating,
// because the pushgateway REJECTS a push body that contains two lines with the
// same metric name + identical labels (400 "was collected before ..."). Dedup
// is last-write-wins, which matches the gateway's own latest-value-per-series
// model. At-least-once; POST replaces only the metric name(s) it carries within
// the grouping key.
//
// CAVEATS (baseline): the pushgateway is a latest-value store for low-cardinality
// service metrics, NOT a high-throughput event TSDB - for streaming metrics at
// scale, remote-write (protobuf+snappy) is the right transport and is deferred.
// Records whose value_field is missing/non-numeric are dropped (no sample).
class PrometheusPushSink : public Sink<std::string> {
public:
    explicit PrometheusPushSink(PromPushOptions o)
        : name_(std::move(o.name)),
          base_url_(std::move(o.url)),
          headers_spec_(std::move(o.headers)),
          metric_name_(sanitize_name(o.metric_name.empty() ? "clink_value" : o.metric_name)),
          value_field_(o.value_field.empty() ? "value" : std::move(o.value_field)),
          max_records_(o.batch_records ? o.batch_records : 500),
          max_retries_(o.max_retries),
          verify_tls_(o.verify_tls) {
        if (base_url_.empty()) {
            throw std::runtime_error(name_ + ": 'url' is required");
        }
        if (o.job.empty()) {
            throw std::runtime_error(name_ + ": 'job' is required");
        }
        if (!HttpRequest::tls_supported() && base_url_.rfind("https://", 0) == 0) {
            throw std::runtime_error(name_ +
                                     ": https URL but this build has no TLS support (rebuild the "
                                     "http_connector module with OpenSSL)");
        }
        push_path_ = build_push_path(o.job, o.grouping);
        // # HELP (optional) then # TYPE gauge, once per metric name, before any
        // sample. A second TYPE/HELP for the same name in one body is a 400, so
        // this prefix is emitted exactly once per push.
        if (!o.help.empty()) {
            type_prefix_ = "# HELP " + metric_name_ + " " + escape_help(o.help) + "\n";
        }
        type_prefix_ += "# TYPE " + metric_name_ + " gauge\n";
    }

    void open() override {
        HttpRequest::Options ho;
        ho.base_url = base_url_;
        ho.headers = parse_headers(headers_spec_);
        ho.verify_tls = verify_tls_;
        client_ = std::make_unique<HttpRequest>(std::move(ho));
        series_.clear();
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            ingest(rec.value());
            if (series_.size() >= max_records_) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (series_.empty()) {
            return;
        }
        std::string body = type_prefix_;
        for (const auto& [labels, value] : series_) {
            body += metric_name_;
            body += labels;  // "{a=\"x\",b=\"y\"}" or empty
            body.push_back(' ');
            body += value_text(value);
            body.push_back('\n');  // every line is LF-terminated, including the last
        }
        // 200 and 202 both mean success; 400 is the only documented rejection
        // and there is no "200-but-failed" case, so the default 2xx check fits.
        RetryPolicy policy{max_retries_, std::chrono::milliseconds{200}};
        const std::size_t n = series_.size();
        const std::size_t bytes = body.size();
        const auto t0 = std::chrono::steady_clock::now();
        try {
            post_with_retry(*client_,
                            push_path_,
                            body,
                            kContentType,
                            policy,
                            /*response_ok=*/{},
                            name_,
                            base_url_);
        } catch (...) {
            clink::metrics::connector::error_inc(name_, "sink");
            series_.clear();
            throw;
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc(name_, n);
        clink::metrics::connector::bytes_out_inc(name_, bytes);
        clink::metrics::connector::commit_latency_observe(name_, static_cast<std::uint64_t>(dt));
        series_.clear();
    }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kContentType = "text/plain; version=0.0.4; charset=utf-8";

    // Parse one record into a (label-set -> value) entry; dedup last-write-wins.
    void ingest(const std::string& rec) {
        clink::config::JsonValue j;
        try {
            j = clink::config::parse(rec);
        } catch (...) {
            return;  // unparseable record: drop
        }
        if (!j.is_object()) {
            return;
        }
        const auto& obj = j.as_object();
        auto vit = obj.find(value_field_);
        if (vit == obj.end()) {
            return;  // no value field: drop
        }
        auto value = to_metric_value(vit->second);
        if (!value) {
            return;  // value not numeric: drop
        }
        std::map<std::string, std::string> labels;  // sanitized name -> escaped value
        for (const auto& [k, v] : obj) {
            if (k == value_field_) {
                continue;
            }
            const std::string ln = sanitize_name(k);
            // 'job' and the grouping labels are the gateway's grouping key and
            // live in the URL path. The gateway 400s a push body that ALSO
            // carries any of them, which would wedge the pipeline (400 -> retry
            // exhaustion -> throw -> replay -> same poisoned record). So a
            // record field colliding with a path label name is dropped from the
            // body, mirroring build_push_path's exclusion.
            if (reserved_labels_.count(ln)) {
                continue;
            }
            auto lv = to_label_value(v);
            if (!lv) {
                continue;  // null / object / array: not a label
            }
            labels[ln] = escape_label_value(*lv);
        }
        series_[render_label_block(labels)] = *value;  // dedup
    }

    // ----- value/label rendering -----

    static std::string value_text(double d) {
        if (std::isnan(d)) {
            return "NaN";
        }
        if (std::isinf(d)) {
            return d > 0 ? "+Inf" : "-Inf";
        }
        char buf[64];
        auto res = std::to_chars(buf, buf + sizeof(buf), d);  // shortest round-trip
        return std::string(buf, res.ptr);
    }

    static std::optional<double> to_metric_value(const clink::config::JsonValue& v) {
        if (v.is_number()) {
            return v.as_number();
        }
        if (v.is_bool()) {
            return v.as_bool() ? 1.0 : 0.0;
        }
        if (v.is_string()) {
            const std::string& s = v.as_string();
            if (!s.empty()) {
                char* end = nullptr;
                const double d = std::strtod(s.c_str(), &end);
                if (end == s.c_str() + s.size()) {
                    return d;
                }
            }
        }
        return std::nullopt;
    }

    static std::optional<std::string> to_label_value(const clink::config::JsonValue& v) {
        if (v.is_string()) {
            return v.as_string();
        }
        if (v.is_bool()) {
            return std::string(v.as_bool() ? "true" : "false");
        }
        if (v.is_number()) {
            return value_text(v.as_number());
        }
        return std::nullopt;  // null / object / array
    }

    // Prometheus label values escape only backslash, double-quote and line feed.
    static std::string escape_label_value(const std::string& in) {
        std::string s;
        s.reserve(in.size() + 8);
        for (char c : in) {
            switch (c) {
                case '\\':
                    s += "\\\\";
                    break;
                case '"':
                    s += "\\\"";
                    break;
                case '\n':
                    s += "\\n";
                    break;
                default:
                    s.push_back(c);
            }
        }
        return s;
    }

    // # HELP text escapes backslash and line feed (it is not quoted, so no ").
    static std::string escape_help(const std::string& in) {
        std::string s;
        s.reserve(in.size() + 8);
        for (char c : in) {
            if (c == '\\') {
                s += "\\\\";
            } else if (c == '\n') {
                s += "\\n";
            } else {
                s.push_back(c);
            }
        }
        return s;
    }

    // Coerce an arbitrary field/metric name to the Prometheus name grammar
    // [a-zA-Z_][a-zA-Z0-9_]* (no ':' - reserved for recording rules), and keep
    // it out of the reserved '__' prefix space.
    static std::string sanitize_name(const std::string& in) {
        std::string s;
        s.reserve(in.size());
        for (char c : in) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || c == '_';
            s.push_back(ok ? c : '_');
        }
        if (s.empty() || (s[0] >= '0' && s[0] <= '9')) {
            s.insert(s.begin(), '_');
        }
        std::size_t underscores = 0;
        while (underscores < s.size() && s[underscores] == '_') {
            ++underscores;
        }
        if (underscores >= 2) {
            s = "_" + s.substr(underscores);  // collapse the reserved '__' prefix
        }
        return s;
    }

    // Render the sorted label map into "{a=\"x\",b=\"y\"}" (or "" when empty).
    // Values are already escaped. The sorted order makes this the canonical
    // dedup key for the series.
    static std::string render_label_block(const std::map<std::string, std::string>& labels) {
        if (labels.empty()) {
            return {};
        }
        std::string s = "{";
        bool first = true;
        for (const auto& [k, v] : labels) {
            if (!first) {
                s.push_back(',');
            }
            first = false;
            s += k;
            s += "=\"";
            s += v;
            s.push_back('"');
        }
        s.push_back('}');
        return s;
    }

    // ----- push path -----

    static std::string base64url_nopad(const std::string& in) {
        static const char t[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        auto uc = [](char c) { return static_cast<unsigned>(static_cast<unsigned char>(c)); };
        std::string out;
        std::size_t i = 0;
        while (i + 3 <= in.size()) {
            const unsigned n = (uc(in[i]) << 16) | (uc(in[i + 1]) << 8) | uc(in[i + 2]);
            out.push_back(t[(n >> 18) & 63]);
            out.push_back(t[(n >> 12) & 63]);
            out.push_back(t[(n >> 6) & 63]);
            out.push_back(t[n & 63]);
            i += 3;
        }
        const std::size_t rem = in.size() - i;
        if (rem == 1) {
            const unsigned n = uc(in[i]) << 16;
            out.push_back(t[(n >> 18) & 63]);
            out.push_back(t[(n >> 12) & 63]);
        } else if (rem == 2) {
            const unsigned n = (uc(in[i]) << 16) | (uc(in[i + 1]) << 8);
            out.push_back(t[(n >> 18) & 63]);
            out.push_back(t[(n >> 12) & 63]);
            out.push_back(t[(n >> 6) & 63]);
        }
        return out;
    }

    // One "/<label>/<value>" path segment, switching to the pushgateway's
    // "<label>@base64/<encoded>" form when the value is empty or contains a
    // char that is unsafe in a raw path segment.
    static std::string encode_path_segment(const std::string& label, const std::string& value) {
        bool needs_b64 = value.empty();
        for (char c : value) {
            const bool safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                              (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
            if (!safe) {
                needs_b64 = true;
                break;
            }
        }
        if (!needs_b64) {
            return "/" + label + "/" + value;
        }
        if (value.empty()) {
            return "/" + label + "@base64/=";  // documented empty-value form
        }
        return "/" + label + "@base64/" + base64url_nopad(value);
    }

    // Builds the push path AND records every label name that lives in the path
    // (job + each grouping label) into reserved_labels_, so ingest() can keep
    // those out of the push body (a body that repeats a grouping-key label is a
    // gateway 400).
    std::string build_push_path(const std::string& job, const std::string& grouping) {
        reserved_labels_.insert("job");
        std::string p = "/metrics";
        p += encode_path_segment("job", job);
        // grouping = "name=value,name2=value2"; each becomes a path label pair.
        std::size_t pos = 0;
        while (pos < grouping.size()) {
            auto comma = grouping.find(',', pos);
            if (comma == std::string::npos) {
                comma = grouping.size();
            }
            const std::string pair = grouping.substr(pos, comma - pos);
            const auto eq = pair.find('=');
            if (eq != std::string::npos) {
                const std::string name = sanitize_name(trim(pair.substr(0, eq)));
                const std::string value = trim(pair.substr(eq + 1));
                if (!name.empty() && name != "job") {  // 'job' already in the path
                    p += encode_path_segment(name, value);
                    reserved_labels_.insert(name);
                }
            }
            pos = comma + 1;
        }
        return p;
    }

    static std::string trim(std::string s) {
        const auto b = s.find_first_not_of(" \t");
        const auto e = s.find_last_not_of(" \t");
        return b == std::string::npos ? std::string{} : s.substr(b, e - b + 1);
    }

    std::string name_;
    std::string base_url_;
    std::string headers_spec_;
    std::string metric_name_;
    std::string value_field_;
    std::string push_path_;
    std::string type_prefix_;
    std::size_t max_records_;
    int max_retries_;
    bool verify_tls_;
    std::unique_ptr<HttpRequest> client_;
    std::map<std::string, double> series_;   // rendered label block -> latest value
    std::set<std::string> reserved_labels_;  // path label names kept out of the body
};

inline std::shared_ptr<Sink<std::string>> make_prometheus_pushgateway_sink(PromPushOptions o) {
    return std::make_shared<PrometheusPushSink>(std::move(o));
}

}  // namespace clink::http_connector
