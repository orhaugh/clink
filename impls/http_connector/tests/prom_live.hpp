#pragma once

// What the Pushgateway live tests and recovery cells share: the gateway they
// are pointed at, and a reader of their own that scrapes the gateway's
// /metrics and parses the exposition text, independent of the sink.
//
//   CLINK_PUSHGATEWAY_TEST_ENDPOINT  the gateway (the live cases skip without it)
//   CLINK_PUSHGATEWAY_TEST_VERSION   the version the gate started, checked by a test

#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

namespace clink::http_connector::prom_live {

inline std::string env_or(const char* name, std::string fallback = {}) {
    const char* v = std::getenv(name);
    return v == nullptr || *v == '\0' ? fallback : std::string{v};
}
inline bool configured() {
    return !env_or("CLINK_PUSHGATEWAY_TEST_ENDPOINT").empty();
}
inline std::string endpoint() {
    return env_or("CLINK_PUSHGATEWAY_TEST_ENDPOINT");
}

inline HttpRequest client(const std::string& url = endpoint()) {
    HttpRequest::Options o;
    o.base_url = url;
    return HttpRequest{o};
}

// One sample of a scrape: its metric, its labels (unescaped) and its value as
// the gateway wrote it.
struct Sample {
    std::string metric;
    std::map<std::string, std::string> labels;
    std::string value;
};

// Parses text exposition: one sample per line; a label value has \\, \" and
// \n unescaped and every other byte kept as it is, a carriage return or a NUL
// included. Comment lines are skipped.
inline std::vector<Sample> parse_exposition(const std::string& text) {
    std::vector<Sample> out;
    std::size_t pos = 0;
    while (pos < text.size()) {
        auto eol = text.find('\n', pos);
        if (eol == std::string::npos) {
            eol = text.size();
        }
        const std::string line = text.substr(pos, eol - pos);
        pos = eol + 1;
        if (line.empty() || line[0] == '#') {
            continue;
        }
        Sample s;
        std::size_t i = line.find_first_of("{ ");
        s.metric = line.substr(0, i);
        if (i < line.size() && line[i] == '{') {
            ++i;
            while (i < line.size() && line[i] != '}') {
                const auto eq = line.find('=', i);
                const std::string name = line.substr(i, eq - i);
                i = eq + 2;  // past ="
                std::string value;
                while (i < line.size() && line[i] != '"') {
                    if (line[i] == '\\' && i + 1 < line.size()) {
                        const char e = line[i + 1];
                        value.push_back(e == 'n' ? '\n' : e);
                        i += 2;
                    } else {
                        value.push_back(line[i]);
                        ++i;
                    }
                }
                ++i;  // past the closing quote
                s.labels[name] = value;
                if (i < line.size() && line[i] == ',') {
                    ++i;
                }
            }
            ++i;  // past }
        }
        while (i < line.size() && line[i] == ' ') {
            ++i;
        }
        const auto end = line.find(' ', i);
        s.value = line.substr(i, end == std::string::npos ? std::string::npos : end - i);
        out.push_back(std::move(s));
    }
    return out;
}

// Every sample the gateway exposes now.
inline std::vector<Sample> scrape(const std::string& url = endpoint()) {
    auto c = client(url);
    const auto res = c.get("/metrics");
    return res.status == 200 ? parse_exposition(res.body) : std::vector<Sample>{};
}

using Series = std::map<std::map<std::string, std::string>, std::string>;

// The samples of `metric` in the groups whose job is `job`, keyed by their
// labels without job, and without the empty instance label the gateway adds.
inline Series series(const std::string& metric, const std::string& job) {
    Series out;
    for (auto& s : scrape()) {
        const auto j = s.labels.find("job");
        if (s.metric != metric || j == s.labels.end() || j->second != job) {
            continue;
        }
        s.labels.erase("job");
        if (const auto in = s.labels.find("instance"); in != s.labels.end() && in->second.empty()) {
            s.labels.erase(in);
        }
        out[s.labels] = s.value;
    }
    return out;
}

// Pushes `body` (text exposition) as the gateway's own client would.
inline int push_raw(const std::string& path, const std::string& body) {
    auto c = client();
    return c.post(path, body, "text/plain; version=0.0.4; charset=utf-8").status;
}

// Deletes a group; the gateway queues the delete and answers 202.
inline int delete_group(const std::string& path) {
    auto c = client();
    return c.del(path).status;
}

// The version /api/v1/status reports.
inline std::string version() {
    auto c = client();
    const auto res = c.get("/api/v1/status");
    try {
        const auto j = clink::config::parse(res.body);
        const auto& data = j.as_object().find("data")->second.as_object();
        return data.find("build_information")
            ->second.as_object()
            .find("version")
            ->second.as_string();
    } catch (const std::exception&) {
        return {};
    }
}

}  // namespace clink::http_connector::prom_live
