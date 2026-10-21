#pragma once

// An in-process stand-in for the Neo4j Query API, for tests that need a server
// without a real one: an httplib server that answers the open-time EXPLAIN and
// replays a scripted reply for each batch request. It decodes every request it
// gets, so a test counts what the server received, not what the sink says it
// sent. Shared by the sink's unit tests and the SQL-level tests.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <httplib.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::neo4j::testing {

using namespace std::chrono_literals;
using clink::config::JsonValue;

constexpr char kCommitted[] = R"({"data":{"fields":[],"values":[]},"bookmarks":["FB:test"]})";
constexpr char kExplainOk[] =
    R"({"data":{"fields":[],"values":[]},"queryType":"w","queryPlan":{"operatorType":)"
    R"("ProduceResults@neo4j","arguments":{"runtime-version":{"$type":"String","_value":)"
    R"("2026.09.0"}},"children":[]},"bookmarks":["FB:explain"]})";

// One scripted reply to a batch request.
struct Scripted {
    enum class Kind { Reply, CutShort, Stall } kind{Kind::Reply};
    int status{202};
    std::string body{kCommitted};
    std::chrono::milliseconds stall{0};
};

struct Received {
    std::string statement;
    std::vector<std::int64_t> ids;  // the "id" (or "event_id") of every row in $rows
    std::string authorization;
    std::string accept;
    std::string content_type;
    std::size_t bytes{0};
};

class FakeQueryApi {
public:
    FakeQueryApi() {
        svr_.Post(
            R"(/db/([^/]+)/query/v2)",
            [this](const httplib::Request& req, httplib::Response& res) { handle(req, res); });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~FakeQueryApi() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    FakeQueryApi(const FakeQueryApi&) = delete;
    FakeQueryApi& operator=(const FakeQueryApi&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    void script(Scripted s) {
        const std::lock_guard<std::mutex> lock(mu_);
        script_.push_back(std::move(s));
    }
    void explain_reply(int status, std::string body) {
        const std::lock_guard<std::mutex> lock(mu_);
        explain_status_ = status;
        explain_body_ = std::move(body);
    }
    // Every batch request (not the EXPLAINs).
    [[nodiscard]] std::vector<Received> batches() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return batches_;
    }
    [[nodiscard]] std::vector<Received> explains() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return explains_;
    }

private:
    void handle(const httplib::Request& req, httplib::Response& res) {
        Received got;
        got.authorization = req.get_header_value("Authorization");
        got.accept = req.get_header_value("Accept");
        got.content_type = req.get_header_value("Content-Type");
        got.bytes = req.body.size();
        const JsonValue body = clink::config::parse(req.body);
        got.statement = body.at("statement").as_string();
        for (const auto& row : body.at("parameters").at("rows").at("_value").as_array()) {
            const auto& cols = row.at("_value");
            for (const char* key : {"id", "event_id"}) {
                if (cols.contains(key)) {
                    got.ids.push_back(std::stoll(cols.at(key).at("_value").as_string()));
                    break;
                }
            }
        }
        Scripted s;
        {
            std::unique_lock<std::mutex> lock(mu_);
            if (got.statement.rfind("EXPLAIN ", 0) == 0) {
                explains_.push_back(got);
                res.status = explain_status_;
                res.set_content(explain_body_, "application/vnd.neo4j.query");
                return;
            }
            batches_.push_back(got);
            if (!script_.empty()) {
                s = std::move(script_.front());
                script_.pop_front();
            }
            if (s.kind == Scripted::Kind::Stall) {
                cv_.wait_for(lock, s.stall, [this] { return released_; });
            }
        }
        res.status = s.status;
        if (s.kind == Scripted::Kind::CutShort) {
            // The status line and part of the body, then the connection ends.
            auto half = s.body.substr(0, s.body.size() / 2);
            res.set_chunked_content_provider("application/vnd.neo4j.query",
                                             [half](std::size_t offset, httplib::DataSink& sink) {
                                                 if (offset == 0) {
                                                     sink.write(half.data(), half.size());
                                                     return true;
                                                 }
                                                 return false;
                                             });
            return;
        }
        res.set_content(s.body, "application/vnd.neo4j.query");
    }

    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    bool released_{false};
    std::deque<Scripted> script_;
    int explain_status_{202};
    std::string explain_body_{kExplainOk};
    std::vector<Received> batches_;
    std::vector<Received> explains_;
};

}  // namespace clink::neo4j::testing
