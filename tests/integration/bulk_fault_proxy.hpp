#pragma once

// An HTTP proxy in front of an Elasticsearch or OpenSearch server, for the bulk
// sink's fault tests: the sink posts to url() instead of the server, and the
// proxy forwards each request to the server and the server's reply back,
// unless a fault is armed for the request:
//
//   * withhold(item, status, type): the next request reaches the server without
//     operation `item`, and its reply reports that operation failed with
//     `status` and error `type`. Every other operation has the server's own
//     outcome; the withheld one was never applied, as with a real item failure;
//   * garble(n): the next n requests reach the server, which applies them, and
//     each reply is replaced by the first half of the server's body: a 200 that
//     confirms nothing;
//   * refuse(n, status, body): the next n requests do not reach the server, and
//     each is answered `status` with `body`.
//
// Faults apply in the order armed, one request each (n requests for garble and
// refuse). Every request, its body and what the proxy did with it are counted.
// Where a TCP-level fault is wanted (a reply lost after the server applied the
// request) tests use TcpFaultProxy instead.
//
// Header-only, for both the connector's live tests and the cluster's recovery
// cells; the workers of a cell reach it on the loopback interface like the test.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <httplib.h>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

namespace clink::itest {

class BulkFaultProxy {
public:
    explicit BulkFaultProxy(std::string upstream_url) : upstream_(std::move(upstream_url)) {
        svr_.Post(R"(/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            handle(req, res);
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~BulkFaultProxy() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    BulkFaultProxy(const BulkFaultProxy&) = delete;
    BulkFaultProxy& operator=(const BulkFaultProxy&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    void withhold(std::size_t item, int status, std::string type) {
        const std::lock_guard<std::mutex> lock(mu_);
        faults_.push_back({Kind::Withhold, item, status, std::move(type)});
    }
    void garble(std::size_t n) {
        const std::lock_guard<std::mutex> lock(mu_);
        for (std::size_t i = 0; i < n; ++i) {
            faults_.push_back({Kind::Garble, 0, 200, {}});
        }
    }
    void refuse(std::size_t n, int status, std::string body) {
        const std::lock_guard<std::mutex> lock(mu_);
        for (std::size_t i = 0; i < n; ++i) {
            faults_.push_back({Kind::Refuse, 0, status, body});
        }
    }
    // Forget the faults not yet applied.
    void clear() {
        const std::lock_guard<std::mutex> lock(mu_);
        faults_.clear();
    }

    [[nodiscard]] std::size_t requests() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return bodies_.size();
    }
    [[nodiscard]] std::vector<std::string> bodies() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return bodies_;
    }
    [[nodiscard]] std::size_t faults_applied() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return applied_;
    }
    [[nodiscard]] std::size_t faults_pending() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return faults_.size();
    }

private:
    enum class Kind { Withhold, Garble, Refuse };
    struct Fault {
        Kind kind;
        std::size_t item;
        int status;
        std::string text;  // Withhold: the error type; Refuse: the body
    };

    static std::vector<std::string> lines_of(const std::string& body) {
        std::vector<std::string> out;
        std::size_t pos = 0;
        while (pos < body.size()) {
            auto nl = body.find('\n', pos);
            if (nl == std::string::npos) {
                nl = body.size();
            }
            out.push_back(body.substr(pos, nl - pos));
            pos = nl + 1;
        }
        return out;
    }

    void handle(const httplib::Request& req, httplib::Response& res) {
        std::optional<Fault> fault;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            bodies_.push_back(req.body);
            if (!faults_.empty()) {
                fault = std::move(faults_.front());
                faults_.pop_front();
                ++applied_;
            }
        }
        if (fault && fault->kind == Kind::Refuse) {
            res.status = fault->status;
            res.set_content(fault->text, "application/json");
            return;
        }
        std::string body = req.body;
        std::string withheld_id;
        if (fault && fault->kind == Kind::Withhold) {
            auto lines = lines_of(req.body);
            const std::size_t at = fault->item * 2;
            if (at + 1 < lines.size()) {
                try {
                    const auto action = clink::config::parse(lines[at]);
                    const auto& meta = action.as_object().begin()->second.as_object();
                    if (auto id = meta.find("_id"); id != meta.end() && id->second.is_string()) {
                        withheld_id = id->second.as_string();
                    }
                } catch (const std::exception&) {
                }
                lines.erase(lines.begin() + static_cast<std::ptrdiff_t>(at),
                            lines.begin() + static_cast<std::ptrdiff_t>(at + 2));
            }
            body.clear();
            for (const auto& l : lines) {
                body += l;
                body += '\n';
            }
        }
        clink::http_connector::HttpResponse up;
        if (!(fault && fault->kind == Kind::Withhold && body.empty())) {
            clink::http_connector::HttpRequest::Options o;
            o.base_url = upstream_;
            clink::http_connector::HttpRequest client(o);
            up = client.post(req.path, body, req.get_header_value("Content-Type"));
        } else {
            up.status = 200;
            up.body = R"({"took":0,"errors":false,"items":[]})";
        }
        if (up.status == 0) {
            res.status = 502;
            res.set_content("the proxy could not reach the server: " + up.error, "text/plain");
            return;
        }
        if (fault && fault->kind == Kind::Garble) {
            res.status = 200;
            res.set_content(up.body.substr(0, up.body.size() / 2), "application/json");
            return;
        }
        if (fault && fault->kind == Kind::Withhold) {
            try {
                const auto reply = clink::config::parse(up.body);
                clink::config::JsonObject obj = reply.as_object();
                clink::config::JsonArray items = obj.at("items").as_array();
                clink::config::JsonObject r;
                r["_index"] = clink::config::JsonValue{std::string{"withheld"}};
                if (!withheld_id.empty()) {
                    r["_id"] = clink::config::JsonValue{withheld_id};
                }
                r["status"] = clink::config::JsonValue{std::int64_t{fault->status}};
                clink::config::JsonObject err;
                err["type"] = clink::config::JsonValue{fault->text};
                err["reason"] = clink::config::JsonValue{std::string{"withheld by the test proxy"}};
                r["error"] = clink::config::JsonValue{std::move(err)};
                clink::config::JsonObject item;
                item["index"] = clink::config::JsonValue{std::move(r)};
                const auto at = std::min(fault->item, items.size());
                items.insert(items.begin() + static_cast<std::ptrdiff_t>(at),
                             clink::config::JsonValue{std::move(item)});
                obj["items"] = clink::config::JsonValue{std::move(items)};
                obj["errors"] = clink::config::JsonValue{true};
                res.status = 200;
                res.set_content(clink::config::JsonValue{std::move(obj)}.serialize(0),
                                "application/json");
                return;
            } catch (const std::exception&) {
                // Not a bulk reply: pass it on as it came.
            }
        }
        res.status = up.status;
        res.set_content(up.body, "application/json");
    }

    std::string upstream_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::deque<Fault> faults_;
    std::vector<std::string> bodies_;
    std::size_t applied_{0};
};

}  // namespace clink::itest
