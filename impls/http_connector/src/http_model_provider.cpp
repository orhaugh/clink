#include "clink/http_connector/http_model_provider.hpp"

#include <stdexcept>
#include <string>
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

class HttpModelProvider final : public clink::sql::ModelProvider {
public:
    HttpModelProvider(HttpRequest::Options http_opts,
                      std::string path,
                      std::vector<std::string> output_columns,
                      std::string response_path,
                      std::string content_type)
        : http_opts_(std::move(http_opts)),
          path_(std::move(path)),
          output_columns_(std::move(output_columns)),
          response_path_(std::move(response_path)),
          content_type_(std::move(content_type)) {}

    // Inference is a blocking network round-trip, so the HTTP provider is async: the
    // ml_predict_row factory drives predict() on a thread pool with many requests in
    // flight instead of one blocking call at a time.
    [[nodiscard]] bool is_async() const override { return true; }

    // Concurrency-safe as the async contract requires: each call builds its OWN
    // HttpRequest (its own client + socket). cpp-httplib's keep-alive Client holds a
    // single connection and is not safe for concurrent posts, so a shared client could
    // not fan out; a fresh client per call is the price of concurrency (no keep-alive
    // reuse across calls, which a connection-pooling client would restore - a follow-on).
    clink::sql::Row predict(const clink::sql::Row& features) override {
        HttpRequest req(http_opts_);
        return post_and_extract_(
            req, path_, content_type_, output_columns_, response_path_, features);
    }

    [[nodiscard]] std::string name() const override { return "http"; }

private:
    // POST the feature row as JSON and map the response object into the OUTPUT columns.
    // Takes the HttpRequest by reference so predict() can hand it a fresh per-call client
    // (the async operator fans predict() out across threads, so no client is shared).
    static clink::sql::Row post_and_extract_(HttpRequest& req,
                                             const std::string& path,
                                             const std::string& content_type,
                                             const std::vector<std::string>& output_columns,
                                             const std::string& response_path,
                                             const clink::sql::Row& features) {
        const std::string body =
            clink::config::JsonValue{clink::config::JsonObject{features.values}}.serialize(0);
        HttpResponse resp = req.post(path, body, content_type);
        if (resp.status == 0) {
            throw std::runtime_error("ML_PREDICT http provider: transport error: " + resp.error);
        }
        if (resp.status < 200 || resp.status >= 300) {
            throw std::runtime_error("ML_PREDICT http provider: endpoint returned status " +
                                     std::to_string(resp.status));
        }
        clink::config::JsonValue parsed = clink::config::parse(resp.body);
        const clink::config::JsonValue* root = &parsed;
        if (!response_path.empty() && parsed.is_object() &&
            parsed.as_object().find(response_path) != parsed.as_object().end()) {
            root = &parsed.at(response_path);
        }
        clink::sql::Row out;
        if (root->is_object()) {
            const auto& obj = root->as_object();
            for (const auto& oc : output_columns) {
                const auto it = obj.find(oc);
                if (it != obj.end()) {
                    out.values[oc] = it->second;
                }
            }
        }
        return out;
    }

    HttpRequest::Options http_opts_;
    std::string path_;
    std::vector<std::string> output_columns_;
    std::string response_path_;
    std::string content_type_;
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

    return std::make_shared<HttpModelProvider>(std::move(http),
                                               std::move(path),
                                               split_csv(opt_str(opts, "output_columns", "")),
                                               opt_str(opts, "response_path", ""),
                                               opt_str(opts, "content_type", "application/json"));
}

}  // namespace clink::http_connector
