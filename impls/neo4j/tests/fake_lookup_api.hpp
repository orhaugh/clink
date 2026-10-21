#pragma once

// An in-process stand-in for the Neo4j Query API as a lookup sees it: it
// answers the open-time EXPLAIN with a configurable plan, and each read with
// the rows scripted for its $key, after an optional delay. It records every
// request it decodes and the most requests it ever had in progress at once,
// so a test counts what the server saw, not what the lookup says it sent.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <httplib.h>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::neo4j::testing {

using namespace std::chrono_literals;

// A typed JSON value as the server writes it.
inline std::string typed_int(std::int64_t v) {
    return R"({"$type":"Integer","_value":")" + std::to_string(v) + "\"}";
}
inline std::string typed_str(const std::string& v) {
    std::string out = R"({"$type":"String","_value":)";
    clink::config::JsonValue{v}.serialize_into(out);
    return out + "}";
}
inline std::string typed_null() {
    return R"({"$type":"Null","_value":null})";
}

// The answer to one read.
struct LookupAnswer {
    enum class Kind { Reply, CutShort, Stall } kind{Kind::Reply};
    std::vector<std::vector<std::string>> rows;  // typed JSON values, one row each
    std::chrono::milliseconds delay{0};          // before the reply is written
    int status{202};
    std::string error_code;  // when set, the reply carries this error instead of rows
    std::string raw;         // when set, the whole reply body
};

struct LookupReceived {
    std::string statement;
    clink::config::JsonValue key;  // parameters.key, as sent
    std::string access_mode;
    std::string max_execution_time;  // as sent, "" when absent
    std::string authorization;
    std::chrono::steady_clock::time_point at;
};

class FakeLookupApi {
public:
    explicit FakeLookupApi(std::vector<std::string> fields) : fields_(std::move(fields)) {
        svr_.new_task_queue = [] { return new httplib::ThreadPool(32); };
        svr_.Post(
            R"(/db/([^/]+)/query/v2)",
            [this](const httplib::Request& req, httplib::Response& res) { handle(req, res); });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~FakeLookupApi() {
        release();
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    FakeLookupApi(const FakeLookupApi&) = delete;
    FakeLookupApi& operator=(const FakeLookupApi&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    // The answer for a key, by its value as text ("7", "acct-01").
    void answer(const std::string& key, LookupAnswer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        answers_[key] = std::move(a);
    }
    // Answers for the next reads in arrival order, before any per-key answer.
    void next(LookupAnswer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        queue_.push_back(std::move(a));
    }
    // The EXPLAIN reply: a query type and plan operators, or a whole body.
    void explain(std::string query_type, std::vector<std::string> operators) {
        const std::lock_guard<std::mutex> lock(mu_);
        query_type_ = std::move(query_type);
        operators_ = std::move(operators);
    }
    void explain_body(int status, std::string body) {
        const std::lock_guard<std::mutex> lock(mu_);
        explain_status_ = status;
        explain_raw_ = std::move(body);
    }
    // Ends every stall now, and every delay still to come.
    void release() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }

    [[nodiscard]] std::vector<LookupReceived> reads() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return reads_;
    }
    [[nodiscard]] std::size_t explains() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return explains_;
    }
    [[nodiscard]] int max_concurrent() const { return max_active_.load(); }
    [[nodiscard]] int active() const { return active_.load(); }

private:
    std::string explain_reply_() const {
        std::string fields;
        for (const auto& f : fields_) {
            fields += (fields.empty() ? "\"" : ",\"") + f + "\"";
        }
        // A chain of plan nodes, the first operator at the root.
        std::string plan = "{}";
        for (auto it = operators_.rbegin(); it != operators_.rend(); ++it) {
            plan = R"({"operatorType":")" + *it + R"(@neo4j","arguments":{},"children":[)" +
                   (plan == "{}" ? std::string{} : plan) + "]}";
        }
        std::string out = R"({"data":{"fields":[)" + fields + R"(],"values":[]})";
        if (!query_type_.empty()) {
            out += R"(,"queryType":")" + query_type_ + "\"";
        }
        if (plan != "{}") {
            out += R"(,"queryPlan":)" + plan;
        }
        return out + R"(,"bookmarks":["FB:explain"]})";
    }

    std::string reply_(const LookupAnswer& a) const {
        if (!a.raw.empty()) {
            return a.raw;
        }
        if (!a.error_code.empty()) {
            return R"({"errors":[{"code":")" + a.error_code + R"(","message":"scripted"}]})";
        }
        std::string fields;
        for (const auto& f : fields_) {
            fields += (fields.empty() ? "\"" : ",\"") + f + "\"";
        }
        std::string rows;
        for (const auto& row : a.rows) {
            std::string r;
            for (const auto& v : row) {
                r += (r.empty() ? "" : ",") + v;
            }
            rows += (rows.empty() ? "[" : ",[") + r + "]";
        }
        return R"({"data":{"fields":[)" + fields + R"(],"values":[)" + rows +
               R"(]},"bookmarks":["FB:read"]})";
    }

    void handle(const httplib::Request& req, httplib::Response& res) {
        const int now_active = ++active_;
        int seen = max_active_.load();
        while (now_active > seen && !max_active_.compare_exchange_weak(seen, now_active)) {
        }
        struct Leave {
            std::atomic<int>& n;
            ~Leave() { --n; }
        } leave{active_};

        const auto body = clink::config::parse(req.body);
        LookupReceived got;
        got.statement = body.at("statement").as_string();
        got.access_mode = body.contains("accessMode") ? body.at("accessMode").as_string() : "";
        got.max_execution_time =
            body.contains("maxExecutionTime") ? body.at("maxExecutionTime").serialize(0) : "";
        got.authorization = req.get_header_value("Authorization");
        got.at = std::chrono::steady_clock::now();
        if (body.contains("parameters") && body.at("parameters").contains("key")) {
            got.key = body.at("parameters").at("key");
        }
        LookupAnswer a;
        {
            std::unique_lock<std::mutex> lock(mu_);
            if (got.statement.rfind("EXPLAIN ", 0) == 0) {
                ++explains_;
                res.status = explain_status_;
                res.set_content(explain_raw_.empty() ? explain_reply_() : explain_raw_,
                                "application/vnd.neo4j.query");
                return;
            }
            reads_.push_back(got);
            if (!queue_.empty()) {
                a = std::move(queue_.front());
                queue_.pop_front();
            } else {
                const auto& v = got.key.at("_value");
                const std::string text = v.is_string() ? v.as_string() : v.serialize(0);
                if (const auto it = answers_.find(text); it != answers_.end()) {
                    a = it->second;
                }
            }
            if (a.kind == LookupAnswer::Kind::Stall) {
                cv_.wait(lock, [this] { return released_; });
            } else if (a.delay.count() > 0) {
                cv_.wait_for(lock, a.delay, [this] { return released_; });
            }
        }
        res.status = a.status;
        const std::string reply = reply_(a);
        if (a.kind == LookupAnswer::Kind::CutShort) {
            auto half = reply.substr(0, reply.size() / 2);
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
        res.set_content(reply, "application/vnd.neo4j.query");
    }

    std::vector<std::string> fields_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    bool released_{false};
    std::map<std::string, LookupAnswer> answers_;
    std::deque<LookupAnswer> queue_;
    std::string query_type_{"r"};
    std::vector<std::string> operators_{"ProduceResults", "Projection", "NodeUniqueIndexSeek"};
    int explain_status_{202};
    std::string explain_raw_;
    std::vector<LookupReceived> reads_;
    std::size_t explains_{0};
    std::atomic<int> active_{0};
    std::atomic<int> max_active_{0};
};

}  // namespace clink::neo4j::testing
