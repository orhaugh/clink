#pragma once

// A deterministic Splunk HTTP Event Collector for the HEC sink's tests and
// recovery cells. It answers the event and acknowledgement endpoints the way
// Splunk documents them, reads every request with its own parser (not the
// sink's), and keeps a log of each event it accepted: the "event" value's
// JSON text exactly as it arrived, the envelope's other members, the request
// and the channel. A case scripts what the next requests meet (refusals,
// partial acceptance, a busy server, a reply lost after acceptance,
// throttling, replies that are not HEC's) and how acknowledgements behave.
//
// It is a protocol stub, not Splunk: nothing here is evidence of what a
// Splunk server does beyond what its documentation says.
//
// Event requests, in this order:
//   - "Authorization: Splunk <token>" or 401 (code 2 without one, 3 for
//     another scheme) or 403 (code 4, another token);
//   - with require_ack, a channel (header or ?channel=) or 400 code 10, and a
//     GUID-shaped one or 400 code 11;
//   - the next scripted step, if any;
//   - envelopes, one JSON object after another with whitespace between: each
//     must parse, be an object and hold "event" (code 12 without, 13 for ""),
//     or the reply is 400 code 6 (12, 13) naming its position, the envelopes
//     before it accepted and those after it not read; an empty body is 400
//     code 5;
//   - 200 {"text":"Success","code":0}, with "ackId" under require_ack.
// Acknowledgement queries ({"acks":[...]}): 400 code 14 without require_ack;
// otherwise each asked ackId of the channel reads true once it has been asked
// about ack_after times before (never with ack_never), after which HEC forgets
// it and it reads false.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <httplib.h>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::http_connector::hec_stub {

// One event the stub accepted.
struct Event {
    std::string event;                        // the "event" value's JSON text, as it arrived
    std::map<std::string, std::string> meta;  // the envelope's other members' JSON text
    std::size_t request{0};                   // the event request that carried it (1-based)
    std::string channel;
};

// One request the stub received.
struct Request {
    std::string path;
    std::string auth;
    std::string channel;
    std::string content_type;
    std::string body;
    int status{0};
};

// The end of the JSON value starting at `i` (one past it), or npos when the
// text ends first. Only brackets and strings are tracked; parse() judges the
// value itself.
inline std::size_t value_end(const std::string& s, std::size_t i) {
    if (i >= s.size()) {
        return std::string::npos;
    }
    if (s[i] == '"') {
        for (std::size_t k = i + 1; k < s.size(); ++k) {
            if (s[k] == '\\') {
                ++k;
            } else if (s[k] == '"') {
                return k + 1;
            }
        }
        return std::string::npos;
    }
    if (s[i] == '{' || s[i] == '[') {
        int depth = 0;
        bool quoted = false;
        for (std::size_t k = i; k < s.size(); ++k) {
            const char c = s[k];
            if (quoted) {
                if (c == '\\') {
                    ++k;
                } else if (c == '"') {
                    quoted = false;
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == '{' || c == '[') {
                ++depth;
            } else if ((c == '}' || c == ']') && --depth == 0) {
                return k + 1;
            }
        }
        return std::string::npos;
    }
    std::size_t k = i;
    while (k < s.size() && s[k] != ',' && s[k] != '}' && s[k] != ']' && s[k] != ' ' &&
           s[k] != '\n' && s[k] != '\r' && s[k] != '\t') {
        ++k;
    }
    return k;
}

inline std::size_t skip_ws(const std::string& s, std::size_t i) {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\n' || s[i] == '\r' || s[i] == '\t')) {
        ++i;
    }
    return i;
}

// The members of one envelope (an object) as raw JSON text, keyed by their
// unescaped names, or nullopt when it is not an object parse() accepts.
inline std::optional<std::map<std::string, std::string>> members(const std::string& envelope) {
    try {
        if (!clink::config::parse(envelope).is_object()) {
            return std::nullopt;
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }
    std::map<std::string, std::string> out;
    std::size_t i = skip_ws(envelope, 1);
    while (i < envelope.size() && envelope[i] == '"') {
        const std::size_t key_end = value_end(envelope, i);
        const std::string key = clink::config::parse(envelope.substr(i, key_end - i)).as_string();
        i = skip_ws(envelope, key_end);
        i = skip_ws(envelope, i + 1);  // past ':'
        const std::size_t end = value_end(envelope, i);
        out[key] = envelope.substr(i, end - i);
        i = skip_ws(envelope, end);
        if (i < envelope.size() && envelope[i] == ',') {
            i = skip_ws(envelope, i + 1);
        }
    }
    return out;
}

inline bool guid_shaped(const std::string& s) {
    if (s.size() != 36) {
        return false;
    }
    for (std::size_t i = 0; i < s.size(); ++i) {
        const bool dash = i == 8 || i == 13 || i == 18 || i == 23;
        const char c = s[i];
        const bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (dash ? c != '-' : !hex) {
            return false;
        }
    }
    return true;
}

inline std::string hec_body(int code, const std::string& text) {
    return R"({"text":")" + text + R"(","code":)" + std::to_string(code) + "}";
}

class HecStub {
public:
    // What the stub does with one event request.
    struct Step {
        enum class Kind : std::uint8_t {
            Accept,           // read and accept as HEC would
            Reply,            // accept nothing; answer status and body
            AcceptThenReply,  // accept every event; answer status and body
            RefuseEvent,      // accept the events before `event`; 400 `code` naming it
            BusyAfter,        // accept the first `event` events; 503 code 9
            AcceptThenStall,  // accept every event; answer success after `stall`
        };
        Kind kind{Kind::Accept};
        int status{200};
        std::string body;
        std::string content_type{"application/json"};
        std::size_t event{0};
        int code{6};
        std::chrono::milliseconds stall{0};
    };
    static Step accept() { return Step{}; }
    static Step reply(int status, std::string body, std::string type = "application/json") {
        Step s;
        s.kind = Step::Kind::Reply;
        s.status = status;
        s.body = std::move(body);
        s.content_type = std::move(type);
        return s;
    }
    static Step hec(int status, int code, const std::string& text) {
        return reply(status, hec_body(code, text));
    }
    static Step accept_then_reply(int status, std::string body) {
        Step s = reply(status, std::move(body));
        s.kind = Step::Kind::AcceptThenReply;
        return s;
    }
    static Step refuse_event(std::size_t n, int code = 6) {
        Step s;
        s.kind = Step::Kind::RefuseEvent;
        s.event = n;
        s.code = code;
        return s;
    }
    static Step busy_after(std::size_t n) {
        Step s;
        s.kind = Step::Kind::BusyAfter;
        s.event = n;
        return s;
    }
    static Step accept_then_stall(std::chrono::milliseconds d) {
        Step s;
        s.kind = Step::Kind::AcceptThenStall;
        s.stall = d;
        return s;
    }

    struct Options {
        std::string token{"hec-token"};
        bool require_ack{false};   // the token uses indexer acknowledgement
        std::size_t ack_after{0};  // queries an ackId must meet before it reads true
        bool ack_never{false};     // no ackId ever reads true
    };

    HecStub() : HecStub(Options{}) {}
    explicit HecStub(Options o) : opts_(std::move(o)) {
        const auto events = [this](const httplib::Request& req, httplib::Response& res) {
            on_event(req, res);
        };
        svr_.Post("/services/collector/event", events);
        svr_.Post("/services/collector/event/1.0", events);
        svr_.Post("/services/collector", events);
        svr_.Post("/proxy/services/collector/event", events);
        svr_.Post(
            "/services/collector/ack",
            [this](const httplib::Request& req, httplib::Response& res) { on_ack(req, res); });
        svr_.Post(
            "/proxy/services/collector/ack",
            [this](const httplib::Request& req, httplib::Response& res) { on_ack(req, res); });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~HecStub() {
        svr_.stop();
        thread_.join();
    }
    HecStub(const HecStub&) = delete;
    HecStub& operator=(const HecStub&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    // The steps the next event requests meet, after any already scripted.
    void script(std::vector<Step> steps) {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto& s : steps) {
            script_.push_back(std::move(s));
        }
    }
    // The replies the next acknowledgement queries meet (Reply steps), before
    // the stub answers them itself again.
    void script_acks(std::vector<Step> steps) {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto& s : steps) {
            ack_script_.push_back(std::move(s));
        }
    }
    void clear_script() {
        const std::lock_guard<std::mutex> lock(mu_);
        script_.clear();
    }
    void set_ack_never(bool never) {
        const std::lock_guard<std::mutex> lock(mu_);
        opts_.ack_never = never;
    }

    [[nodiscard]] std::vector<Event> accepted() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return accepted_;
    }
    [[nodiscard]] std::vector<Request> requests() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return requests_;
    }
    [[nodiscard]] std::vector<Request> ack_queries() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return ack_queries_;
    }
    // ackIds reported true, in the order reported.
    [[nodiscard]] std::vector<std::uint64_t> acknowledged() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return acknowledged_;
    }
    // The "event" texts accepted, in order.
    [[nodiscard]] std::vector<std::string> events() const {
        std::vector<std::string> out;
        for (const auto& e : accepted()) {
            out.push_back(e.event);
        }
        return out;
    }

private:
    static std::string channel_of(const httplib::Request& req) {
        const std::string h = req.get_header_value("X-Splunk-Request-Channel");
        if (!h.empty()) {
            return h;
        }
        return req.has_param("channel") ? req.get_param_value("channel") : std::string{};
    }

    // 0 when the request may go on, else the status written to `res`.
    int refuse_auth(const httplib::Request& req, httplib::Response& res) const {
        const std::string auth = req.get_header_value("Authorization");
        if (auth.empty()) {
            res.status = 401;
            res.set_content(hec_body(2, "Token is required"), "application/json");
        } else if (auth.rfind("Splunk ", 0) != 0) {
            res.status = 401;
            res.set_content(hec_body(3, "Invalid authorization"), "application/json");
        } else if (auth != "Splunk " + opts_.token) {
            res.status = 403;
            res.set_content(hec_body(4, "Invalid token"), "application/json");
        } else {
            return 0;
        }
        return res.status;
    }

    void on_event(const httplib::Request& req, httplib::Response& res) {
        std::unique_lock<std::mutex> lock(mu_);
        Request logged{req.path,
                       req.get_header_value("Authorization"),
                       channel_of(req),
                       req.get_header_value("Content-Type"),
                       req.body,
                       0};
        const std::size_t number = requests_.size() + 1;
        const auto finish = [&](int status, const std::string& body, const std::string& type) {
            res.status = status;
            res.set_content(body, type);
            logged.status = status;
            requests_.push_back(logged);
        };
        if (refuse_auth(req, res) != 0) {
            logged.status = res.status;
            requests_.push_back(logged);
            return;
        }
        if (opts_.require_ack) {
            if (logged.channel.empty()) {
                finish(400, hec_body(10, "Data channel is missing"), "application/json");
                return;
            }
            if (!guid_shaped(logged.channel)) {
                finish(400, hec_body(11, "Invalid data channel"), "application/json");
                return;
            }
        }
        Step step;
        if (!script_.empty()) {
            step = std::move(script_.front());
            script_.pop_front();
        }
        if (step.kind == Step::Kind::Reply) {
            finish(step.status, step.body, step.content_type);
            return;
        }
        // Read the envelopes as HEC does, accepting each in turn.
        std::vector<Event> read;
        std::optional<std::pair<std::size_t, int>> bad;  // position, code
        std::size_t i = skip_ws(req.body, 0);
        if (i == req.body.size()) {
            finish(400, hec_body(5, "No data"), "application/json");
            return;
        }
        while (i < req.body.size()) {
            const std::size_t end = value_end(req.body, i);
            const auto m =
                end == std::string::npos ? std::nullopt : members(req.body.substr(i, end - i));
            const auto ev =
                m ? m->find("event") : std::map<std::string, std::string>::const_iterator{};
            if (!m) {
                bad = std::make_pair(read.size(), 6);
                break;
            }
            if (ev == m->end()) {
                bad = std::make_pair(read.size(), 12);
                break;
            }
            if (ev->second == "\"\"") {
                bad = std::make_pair(read.size(), 13);
                break;
            }
            Event e;
            e.event = ev->second;
            for (const auto& [k, v] : *m) {
                if (k != "event") {
                    e.meta[k] = v;
                }
            }
            e.request = number;
            e.channel = logged.channel;
            read.push_back(std::move(e));
            i = skip_ws(req.body, end);
        }
        std::size_t take = read.size();
        if (bad) {
            take = std::min(take, bad->first);
        }
        if (step.kind == Step::Kind::RefuseEvent || step.kind == Step::Kind::BusyAfter) {
            take = std::min(take, step.event);
        }
        for (std::size_t k = 0; k < take; ++k) {
            accepted_.push_back(read[k]);
        }
        const bool limited =
            step.kind == Step::Kind::RefuseEvent || step.kind == Step::Kind::BusyAfter;
        if (bad && (!limited || bad->first < step.event)) {
            const char* text = bad->second == 12   ? "Event field is required"
                               : bad->second == 13 ? "Event field cannot be blank"
                                                   : "Invalid data format";
            finish(400,
                   R"({"text":")" + std::string{text} + R"(","code":)" +
                       std::to_string(bad->second) + R"(,"invalid-event-number":)" +
                       std::to_string(bad->first) + "}",
                   "application/json");
            return;
        }
        switch (step.kind) {
            case Step::Kind::RefuseEvent:
                finish(400,
                       R"({"text":"Invalid data format","code":)" + std::to_string(step.code) +
                           R"(,"invalid-event-number":)" + std::to_string(step.event) + "}",
                       "application/json");
                return;
            case Step::Kind::BusyAfter:
                finish(503, hec_body(9, "Server is busy"), "application/json");
                return;
            case Step::Kind::AcceptThenReply:
                finish(step.status, step.body, step.content_type);
                return;
            case Step::Kind::AcceptThenStall: {
                logged.status = 200;
                requests_.push_back(logged);
                lock.unlock();
                std::this_thread::sleep_for(step.stall);
                res.status = 200;
                res.set_content(hec_body(0, "Success"), "application/json");
                return;
            }
            case Step::Kind::Accept:
            case Step::Kind::Reply:
                break;
        }
        if (opts_.require_ack) {
            const std::uint64_t id = next_ack_[logged.channel]++;
            pending_[logged.channel][id] = 0;
            finish(200,
                   R"({"text":"Success","code":0,"ackId":)" + std::to_string(id) + "}",
                   "application/json");
            return;
        }
        finish(200, hec_body(0, "Success"), "application/json");
    }

    void on_ack(const httplib::Request& req, httplib::Response& res) {
        const std::lock_guard<std::mutex> lock(mu_);
        Request logged{req.path,
                       req.get_header_value("Authorization"),
                       channel_of(req),
                       req.get_header_value("Content-Type"),
                       req.body,
                       0};
        const auto finish = [&](int status, const std::string& body, const std::string& type) {
            res.status = status;
            res.set_content(body, type);
            logged.status = status;
            ack_queries_.push_back(logged);
        };
        if (refuse_auth(req, res) != 0) {
            logged.status = res.status;
            ack_queries_.push_back(logged);
            return;
        }
        if (!opts_.require_ack) {
            finish(400, hec_body(14, "ACK is disabled"), "application/json");
            return;
        }
        if (logged.channel.empty()) {
            finish(400, hec_body(10, "Data channel is missing"), "application/json");
            return;
        }
        if (!ack_script_.empty()) {
            const Step step = std::move(ack_script_.front());
            ack_script_.pop_front();
            finish(step.status, step.body, step.content_type);
            return;
        }
        std::vector<std::uint64_t> asked;
        try {
            const auto v = clink::config::parse(req.body);
            for (const auto& a : v.at("acks").as_array()) {
                asked.push_back(static_cast<std::uint64_t>(a.as_int()));
            }
        } catch (const std::exception&) {
            finish(400, hec_body(6, "Invalid data format"), "application/json");
            return;
        }
        auto& pending = pending_[logged.channel];
        std::string body = R"({"acks":{)";
        bool first = true;
        for (const auto id : asked) {
            bool done = false;
            if (const auto it = pending.find(id); it != pending.end()) {
                if (!opts_.ack_never && it->second >= opts_.ack_after) {
                    done = true;
                    acknowledged_.push_back(id);
                    pending.erase(it);  // HEC forgets an ackId once it reported it true
                } else {
                    ++it->second;
                }
            }
            body += (first ? "\"" : ",\"") + std::to_string(id) + "\":" + (done ? "true" : "false");
            first = false;
        }
        body += "}}";
        finish(200, body, "application/json");
    }

    Options opts_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::deque<Step> script_;
    std::deque<Step> ack_script_;
    std::vector<Event> accepted_;
    std::vector<Request> requests_;
    std::vector<Request> ack_queries_;
    std::vector<std::uint64_t> acknowledged_;
    std::map<std::string, std::uint64_t> next_ack_;  // per channel
    std::map<std::string, std::map<std::uint64_t, std::size_t>>
        pending_;  // per channel: id -> queries
};

}  // namespace clink::http_connector::hec_stub
