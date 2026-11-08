#pragma once

// An in-process stand-in for Kinesis's PutRecords endpoint, for the cases that
// run the sink's production SDK path (make_kinesis_write_sdk_service): the AWS
// SDK signs, serialises and sends real requests to it over HTTP and reads its
// replies, so what is checked is what the SDK sends and how its answers reach
// the sink. Each request is recorded as sent (the stream, and every record's
// partition key and base64-decoded data); each reply is what a step scripts:
// stored (all, or all but some entries), a hand-written body, an error with its
// HTTP status, a reply held past the client's timeout after storing, or a hang
// until released. It is not Kinesis and checks nothing a stream would.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <deque>
#include <httplib.h>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::aws::test {

class KinesisHttpStub {
public:
    struct Record {
        std::string partition_key;
        std::string data;
        bool operator==(const Record&) const = default;
    };
    struct Request {
        std::string target;  // X-Amz-Target
        std::string stream_name;
        std::string stream_arn;
        std::vector<Record> records;
        std::string body;  // as received
    };
    struct Step {
        enum class Kind { Store, Raw, Error, Stall, Hang };
        Kind kind{Kind::Store};
        std::set<std::size_t> failing;      // Store: entries that fail with throttling
        int status{200};                    // Raw, Error
        std::string body;                   // Raw: the whole reply
        std::string code;                   // Error: __type
        std::string message;                // Error
        std::chrono::milliseconds hold{0};  // Stall: how long to hold the reply after storing
    };

    static Step store(std::set<std::size_t> failing = {}) {
        Step s;
        s.failing = std::move(failing);
        return s;
    }
    static Step raw(int status, std::string body) {
        Step s;
        s.kind = Step::Kind::Raw;
        s.status = status;
        s.body = std::move(body);
        return s;
    }
    static Step error(int status, std::string code, std::string message = "scripted") {
        Step s;
        s.kind = Step::Kind::Error;
        s.status = status;
        s.code = std::move(code);
        s.message = std::move(message);
        return s;
    }
    static Step stall(std::chrono::milliseconds hold) {
        Step s;
        s.kind = Step::Kind::Stall;
        s.hold = hold;
        return s;
    }
    static Step hang() {
        Step s;
        s.kind = Step::Kind::Hang;
        return s;
    }

    KinesisHttpStub() {
        svr_.Post("/", [this](const httplib::Request& req, httplib::Response& res) {
            on_request(req, res);
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    ~KinesisHttpStub() {
        release();
        svr_.stop();
        thread_.join();
    }
    KinesisHttpStub(const KinesisHttpStub&) = delete;
    KinesisHttpStub& operator=(const KinesisHttpStub&) = delete;

    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    // Replies for the next requests, one step each; after them, Store.
    void script(std::vector<Step> steps) {
        const std::lock_guard<std::mutex> lock(mu_);
        steps_.assign(steps.begin(), steps.end());
    }
    // Ends every hang and stall now and later.
    void release() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }

    [[nodiscard]] std::vector<Request> requests() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return requests_;
    }
    [[nodiscard]] std::vector<Record> stored() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return stored_;
    }
    [[nodiscard]] bool hanging() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return hanging_;
    }

private:
    static std::string base64_decode(const std::string& in) {
        static const std::string kAlphabet =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string out;
        unsigned int bits = 0;
        int count = 0;
        for (const char c : in) {
            if (c == '=') {
                break;
            }
            const auto at = kAlphabet.find(c);
            if (at == std::string::npos) {
                continue;
            }
            bits = (bits << 6) | static_cast<unsigned int>(at);
            count += 6;
            if (count >= 8) {
                count -= 8;
                out.push_back(static_cast<char>((bits >> count) & 0xFF));
            }
        }
        return out;
    }

    static std::string sequence(std::size_t n) {
        char buf[48];
        std::snprintf(buf, sizeof buf, "4961%020zu", n);
        return buf;
    }

    static std::string json_string(const std::string& s) {
        std::string out = "\"";
        for (const char c : s) {
            if (c == '"' || c == '\\') {
                out.push_back('\\');
            }
            out.push_back(c);
        }
        return out + "\"";
    }

    void on_request(const httplib::Request& req, httplib::Response& res) {
        Request seen;
        seen.target = req.get_header_value("X-Amz-Target");
        seen.body = req.body;
        try {
            const auto j = clink::config::parse(req.body);
            const auto& obj = j.as_object();
            if (auto it = obj.find("StreamName"); it != obj.end()) {
                seen.stream_name = it->second.as_string();
            }
            if (auto it = obj.find("StreamARN"); it != obj.end()) {
                seen.stream_arn = it->second.as_string();
            }
            if (auto it = obj.find("Records"); it != obj.end()) {
                for (const auto& r : it->second.as_array()) {
                    const auto& ro = r.as_object();
                    Record rec;
                    if (auto k = ro.find("PartitionKey"); k != ro.end()) {
                        rec.partition_key = k->second.as_string();
                    }
                    if (auto d = ro.find("Data"); d != ro.end()) {
                        rec.data = base64_decode(d->second.as_string());
                    }
                    seen.records.push_back(std::move(rec));
                }
            }
        } catch (const std::exception&) {
            // Recorded as far as it was read; the reply below is as scripted.
        }
        Step step;
        {
            std::unique_lock<std::mutex> lock(mu_);
            requests_.push_back(seen);
            if (!steps_.empty()) {
                step = steps_.front();
                steps_.pop_front();
            }
            if (step.kind == Step::Kind::Hang) {
                hanging_ = true;
                cv_.wait_for(lock, std::chrono::seconds{30}, [this] { return released_; });
                hanging_ = false;
                res.status = 500;
                res.set_content(R"({"__type":"InternalFailure","message":"released"})",
                                "application/x-amz-json-1.1");
                return;
            }
        }
        if (step.kind == Step::Kind::Raw) {
            res.status = step.status;
            res.set_content(step.body, "application/x-amz-json-1.1");
            return;
        }
        if (step.kind == Step::Kind::Error) {
            res.status = step.status;
            res.set_header("x-amzn-ErrorType", step.code);
            res.set_content("{\"__type\":" + json_string(step.code) +
                                ",\"message\":" + json_string(step.message) + "}",
                            "application/x-amz-json-1.1");
            return;
        }
        std::string records;
        std::size_t failed = 0;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            for (std::size_t i = 0; i < seen.records.size(); ++i) {
                if (!records.empty()) {
                    records += ",";
                }
                if (step.failing.count(i) != 0) {
                    ++failed;
                    records +=
                        R"({"ErrorCode":"ProvisionedThroughputExceededException","ErrorMessage":)"
                        R"("Rate exceeded for shard shardId-000000000000 in stream )" +
                        seen.stream_name + R"( under account 111111111111."})";
                } else {
                    stored_.push_back(seen.records[i]);
                    records += R"({"SequenceNumber":")" + sequence(stored_.size()) +
                               R"(","ShardId":"shardId-000000000000"})";
                }
            }
        }
        if (step.kind == Step::Kind::Stall) {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, step.hold, [this] { return released_; });
        }
        res.status = 200;
        res.set_content(
            "{\"FailedRecordCount\":" + std::to_string(failed) + ",\"Records\":[" + records + "]}",
            "application/x-amz-json-1.1");
    }

    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Step> steps_;
    std::vector<Request> requests_;
    std::vector<Record> stored_;
    bool released_{false};
    bool hanging_{false};
};

}  // namespace clink::aws::test
