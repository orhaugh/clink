#include "clink/http_connector/http_model_provider.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_bulk_post.hpp"  // parse_headers
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/token_source.hpp"  // make_file_token_provider
#include "clink/sql/row.hpp"

namespace clink::http_connector {

namespace {

// Split "scheme://host[:port]/path?query" into ("scheme://host[:port]", "/path?query").
std::pair<std::string, std::string> split_url(const std::string& url) {
    const auto scheme = url.find("://");
    const std::size_t host_start = scheme == std::string::npos ? 0 : scheme + 3;
    const auto slash = url.find('/', host_start);
    if (slash == std::string::npos) {
        return {url, "/"};
    }
    return {url.substr(0, slash), url.substr(slash)};
}

int opt_int(const std::map<std::string, std::string>& o, const std::string& k, int fallback) {
    const auto it = o.find(k);
    if (it == o.end() || it->second.empty()) {
        return fallback;
    }
    try {
        return std::stoi(it->second);
    } catch (...) {
        return fallback;
    }
}

std::string opt_str(const std::map<std::string, std::string>& o,
                    const std::string& k,
                    const std::string& fallback) {
    const auto it = o.find(k);
    return it == o.end() ? fallback : it->second;
}

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= s.size()) {
        const auto comma = s.find(',', start);
        const auto end = comma == std::string::npos ? s.size() : comma;
        if (end > start) {
            out.push_back(s.substr(start, end - start));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

// A tiny thread-safe pool of keep-alive HTTP clients. The async ML_PREDICT operator fans
// predict() across worker threads; without pooling each call builds a fresh HttpRequest
// and pays a new TCP (+ TLS) handshake. The pool hands each call an idle client (reusing
// its kept-alive connection) and takes it back on completion. cpp-httplib's Client is not
// safe for concurrent use, so a client is only ever held by one caller at a time.
class ClientPool {
public:
    ClientPool(HttpRequest::Options opts, std::size_t max_idle)
        : opts_(std::move(opts)), max_idle_(max_idle == 0 ? 1 : max_idle) {}

    std::unique_ptr<HttpRequest> acquire() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!idle_.empty()) {
                auto c = std::move(idle_.back());
                idle_.pop_back();
                return c;
            }
        }
        return std::make_unique<HttpRequest>(opts_);  // build a new client outside the lock
    }

    void release(std::unique_ptr<HttpRequest> c) {
        std::lock_guard<std::mutex> lk(mu_);
        if (idle_.size() < max_idle_) {
            idle_.push_back(std::move(c));
        }
        // else drop it (destroy) - bounds the kept-idle connections / file descriptors.
    }

private:
    HttpRequest::Options opts_;
    std::size_t max_idle_;
    std::mutex mu_;
    std::vector<std::unique_ptr<HttpRequest>> idle_;
};

// RAII lease: returns the client to the pool on scope exit, including on exception (a
// failed request does not corrupt the client - cpp-httplib re-establishes on next use).
class ClientLease {
public:
    ClientLease(ClientPool& pool, std::unique_ptr<HttpRequest> c)
        : pool_(&pool), client_(std::move(c)) {}
    ClientLease(const ClientLease&) = delete;
    ClientLease& operator=(const ClientLease&) = delete;
    ClientLease(ClientLease&&) = delete;
    ClientLease& operator=(ClientLease&&) = delete;
    ~ClientLease() {
        if (client_) {
            pool_->release(std::move(client_));
        }
    }
    HttpRequest& get() { return *client_; }

private:
    ClientPool* pool_;
    std::unique_ptr<HttpRequest> client_;
};

class HttpModelProvider final : public clink::sql::ModelProvider {
public:
    struct RetryPolicy {
        int max_retries = 2;   // extra attempts after the first, on a transient failure
        int backoff_ms = 100;  // base backoff; doubles each retry (exponential)
    };

    HttpModelProvider(HttpRequest::Options http_opts,
                      std::string path,
                      std::vector<std::string> output_columns,
                      std::string response_path,
                      std::string content_type,
                      std::size_t max_batch_size,
                      RetryPolicy retry,
                      std::size_t conn_pool_size)
        : path_(std::move(path)),
          output_columns_(std::move(output_columns)),
          response_path_(std::move(response_path)),
          content_type_(std::move(content_type)),
          max_batch_size_(max_batch_size < 1 ? 1 : max_batch_size),
          retry_(retry),
          pool_(std::make_shared<ClientPool>(std::move(http_opts), conn_pool_size)) {}

    // Inference is a blocking network round-trip, so the HTTP provider is async: the
    // ml_predict_row factory drives predict() on a thread pool with many requests in
    // flight instead of one blocking call at a time.
    [[nodiscard]] bool is_async() const override { return true; }

    // Concurrency-safe as the async contract requires: each call leases its OWN client
    // from the pool for the duration of the request (cpp-httplib's Client is not safe for
    // concurrent use), and the lease reuses a kept-alive connection instead of a fresh
    // handshake per call.
    clink::sql::Row predict(const clink::sql::Row& features) override {
        const std::string body =
            clink::config::JsonValue{clink::config::JsonObject{features.values}}.serialize(0);
        ClientLease lease(*pool_, pool_->acquire());
        clink::config::JsonValue root = post_and_root_(lease.get(), body);
        return extract_output_(root, output_columns_);
    }

    // Batching: when the model declares max_batch_size > 1, the batching operator hands
    // predict_batch a buffer of feature rows, and this issues ONE request whose body is a
    // JSON array of the per-row feature objects, expecting a JSON array of predictions in
    // the same order (optionally under response_path). One row in, one row out.
    [[nodiscard]] std::size_t max_batch_size() const override { return max_batch_size_; }

    std::vector<clink::sql::Row> predict_batch(
        const std::vector<clink::sql::Row>& features_batch) override {
        if (features_batch.empty()) {
            return {};
        }
        clink::config::JsonArray body_arr;
        body_arr.reserve(features_batch.size());
        for (const auto& f : features_batch) {
            body_arr.push_back(clink::config::JsonValue{clink::config::JsonObject{f.values}});
        }
        const std::string body = clink::config::JsonValue{std::move(body_arr)}.serialize(0);
        ClientLease lease(*pool_, pool_->acquire());
        clink::config::JsonValue root = post_and_root_(lease.get(), body);
        if (!root.is_array()) {
            throw std::runtime_error(
                "ML_PREDICT http provider: batch response must be a JSON array (one prediction per "
                "input row)");
        }
        const auto& arr = root.as_array();
        if (arr.size() != features_batch.size()) {
            throw std::runtime_error(
                "ML_PREDICT http provider: batch response length " + std::to_string(arr.size()) +
                " does not match request length " + std::to_string(features_batch.size()));
        }
        std::vector<clink::sql::Row> out;
        out.reserve(arr.size());
        for (const auto& el : arr) {
            out.push_back(extract_output_(el, output_columns_));
        }
        return out;
    }

    [[nodiscard]] std::string name() const override { return "http"; }

private:
    // POST `body`, check the status, parse the response, and resolve response_path (when
    // set and present) - returning the JSON root the caller reads (an object for a single
    // prediction, an array for a batch). Takes the HttpRequest by reference so callers can
    // hand it a fresh per-call client (predict() is fanned across threads; predict_batch
    // runs on the single operator thread).
    clink::config::JsonValue post_and_root_(HttpRequest& req, const std::string& body) const {
        // Retry transient failures (transport error / 5xx / 429) with exponential
        // backoff; a 4xx (other than 429) is a client error and is not retried. Inference
        // servers are flaky under load, so a few retries turn most blips into successes
        // instead of a hard failure the on_error policy then has to absorb.
        HttpResponse resp;
        for (int attempt = 0;; ++attempt) {
            resp = req.post(path_, body, content_type_);
            const bool transient = resp.status == 0 || resp.status == 429 || resp.status >= 500;
            if (!transient || attempt >= retry_.max_retries) {
                break;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds{retry_.backoff_ms * (1 << attempt)});
        }
        if (resp.status == 0) {
            throw std::runtime_error("ML_PREDICT http provider: transport error: " + resp.error);
        }
        if (resp.status < 200 || resp.status >= 300) {
            throw std::runtime_error("ML_PREDICT http provider: endpoint returned status " +
                                     std::to_string(resp.status));
        }
        clink::config::JsonValue parsed = clink::config::parse(resp.body);
        if (!response_path_.empty() && parsed.is_object() &&
            parsed.as_object().find(response_path_) != parsed.as_object().end()) {
            return parsed.at(response_path_);
        }
        return parsed;
    }

    // Map one prediction JSON object into the model's OUTPUT columns.
    static clink::sql::Row extract_output_(const clink::config::JsonValue& obj,
                                           const std::vector<std::string>& output_columns) {
        clink::sql::Row out;
        if (obj.is_object()) {
            const auto& o = obj.as_object();
            for (const auto& oc : output_columns) {
                const auto it = o.find(oc);
                if (it != o.end()) {
                    out.values[oc] = it->second;
                }
            }
        }
        return out;
    }

    std::string path_;
    std::vector<std::string> output_columns_;
    std::string response_path_;
    std::string content_type_;
    std::size_t max_batch_size_;
    RetryPolicy retry_;
    std::shared_ptr<ClientPool> pool_;
};

}  // namespace

std::shared_ptr<clink::sql::ModelProvider> make_http_model_provider(
    const std::map<std::string, std::string>& opts) {
    const auto ep = opts.find("endpoint");
    if (ep == opts.end() || ep->second.empty()) {
        throw std::runtime_error("ML_PREDICT http provider: 'endpoint' option is required");
    }
    auto [base_url, path] = split_url(ep->second);

    HttpRequest::Options http;
    http.base_url = base_url;
    const int timeout = opt_int(opts, "timeout_ms", 5000);
    http.connect_timeout_ms = timeout;
    http.rw_timeout_ms = timeout;
    http.verify_tls = opt_str(opts, "verify_tls", "true") != "false";
    // auth_token_file (refreshing) takes precedence over a static auth_token.
    if (const std::string tf = opt_str(opts, "auth_token_file", ""); !tf.empty()) {
        http.auth_token_provider = make_file_token_provider(tf);
    } else if (const std::string tok = opt_str(opts, "auth_token", ""); !tok.empty()) {
        http.headers["Authorization"] = "Bearer " + tok;
    }
    for (const auto& [k, v] : parse_headers(opt_str(opts, "headers", ""))) {
        http.headers[k] = v;
    }

    // max_batch_size > 1 opts the model into the batching operator (one request per
    // buffered batch, array in / array out). Default 1 = per-row requests.
    const auto max_batch_size = static_cast<std::size_t>(opt_int(opts, "max_batch_size", 1));
    HttpModelProvider::RetryPolicy retry;
    retry.max_retries = opt_int(opts, "max_retries", 2);
    if (retry.max_retries < 0) {
        retry.max_retries = 0;
    }
    retry.backoff_ms = opt_int(opts, "retry_backoff_ms", 100);
    if (retry.backoff_ms < 0) {
        retry.backoff_ms = 0;
    }
    // conn_pool_size caps the kept-alive clients; default matches the async operator's
    // pool thread count so a fully-busy operator keeps every worker's connection warm.
    const auto conn_pool_size = static_cast<std::size_t>(opt_int(opts, "conn_pool_size", 16));
    return std::make_shared<HttpModelProvider>(std::move(http),
                                               std::move(path),
                                               split_csv(opt_str(opts, "output_columns", "")),
                                               opt_str(opts, "response_path", ""),
                                               opt_str(opts, "content_type", "application/json"),
                                               max_batch_size,
                                               retry,
                                               conn_pool_size);
}

}  // namespace clink::http_connector
