#pragma once

// What the InfluxDB live tests and recovery cells share: the server they are
// pointed at, and a reader of their own that queries it with Flux and returns
// every point of a measurement with its tags, time, and each field's value and
// type, independent of the sink.
//
//   CLINK_INFLUXDB_TEST_ENDPOINT  the server (the live cases skip without it)
//   CLINK_INFLUXDB_TEST_ORG, _BUCKET, _TOKEN  (default clink, clink, clink-token)
//   CLINK_INFLUXDB_TEST_VERSION   the version the gate started, checked by a test

#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

namespace clink::http_connector::influx_live {

inline std::string env_or(const char* name, std::string fallback = {}) {
    const char* v = std::getenv(name);
    return v == nullptr || *v == '\0' ? fallback : std::string{v};
}
inline bool configured() {
    return !env_or("CLINK_INFLUXDB_TEST_ENDPOINT").empty();
}
inline std::string endpoint() {
    return env_or("CLINK_INFLUXDB_TEST_ENDPOINT");
}
inline std::string org() {
    return env_or("CLINK_INFLUXDB_TEST_ORG", "clink");
}
inline std::string bucket() {
    return env_or("CLINK_INFLUXDB_TEST_BUCKET", "clink");
}
inline std::string token() {
    return env_or("CLINK_INFLUXDB_TEST_TOKEN", "clink-token");
}

// One field of a point as InfluxDB returns it: the value's text and its
// type ("double", "long", "string", "boolean").
struct Field {
    std::string value;
    std::string type;
    friend bool operator==(const Field&, const Field&) = default;
};

// A point: its tags (name -> value), its time in RFC 3339 as Flux writes it,
// and its fields.
struct Point {
    std::map<std::string, std::string> tags;
    std::string time;
    std::map<std::string, Field> fields;
};

// Splits annotated CSV into records, honouring quoted fields with embedded
// line breaks and doubled quotes.
inline std::vector<std::vector<std::string>> csv_records(const std::string& text) {
    std::vector<std::vector<std::string>> out;
    std::vector<std::string> rec;
    std::string cell;
    bool quoted = false;
    bool any = false;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (quoted) {
            // Flux writes CSV with CRLF line ends: a line break inside a value
            // comes out as CRLF (and a lone CR is dropped), so CRLF reads as LF.
            if (c == '\r' && i + 1 < text.size() && text[i + 1] == '\n') {
                continue;
            }
            if (c == '"') {
                if (i + 1 < text.size() && text[i + 1] == '"') {
                    cell.push_back('"');
                    ++i;
                } else {
                    quoted = false;
                }
            } else {
                cell.push_back(c);
            }
            continue;
        }
        if (c == '"') {
            quoted = true;
            any = true;
        } else if (c == ',') {
            rec.push_back(std::move(cell));
            cell.clear();
            any = true;
        } else if (c == '\n') {
            if (any || !cell.empty()) {
                rec.push_back(std::move(cell));
            }
            out.push_back(std::move(rec));
            rec.clear();
            cell.clear();
            any = false;
        } else if (c != '\r') {
            cell.push_back(c);
            any = true;
        }
    }
    if (any || !cell.empty()) {
        rec.push_back(std::move(cell));
        out.push_back(std::move(rec));
    }
    return out;
}

// The _value cells a Flux query returns, in order (the query should end in a
// table with a _value column). For checks the annotated CSV cannot carry: it
// drops a carriage return from a string, so a case maps it to text first.
inline std::vector<std::string> flux_values(const std::string& flux) {
    HttpRequest::Options o;
    o.base_url = endpoint();
    o.headers["Authorization"] = "Token " + token();
    o.headers["Accept"] = "application/csv";
    HttpRequest c{o};
    clink::config::JsonObject q;
    q["query"] = clink::config::JsonValue{flux};
    const auto res = c.post("/api/v2/query?org=" + org(),
                            clink::config::JsonValue{std::move(q)}.serialize(0),
                            "application/json");
    std::vector<std::string> out;
    std::vector<std::string> header;
    for (const auto& rec : csv_records(res.body)) {
        if (rec.empty() || (rec.size() == 1 && rec[0].empty())) {
            header.clear();
            continue;
        }
        if (rec[0].starts_with("#")) {
            continue;
        }
        if (header.empty()) {
            header = rec;
            continue;
        }
        for (std::size_t i = 0; i < header.size() && i < rec.size(); ++i) {
            if (header[i] == "_value") {
                out.push_back(rec[i]);
            }
        }
    }
    return out;
}

// Every point of `measurement`, over InfluxDB's whole time range, keyed by its
// tags and time ("t1=v1,t2=v2@<time>").
inline std::map<std::string, Point> points(const std::string& measurement) {
    HttpRequest::Options o;
    o.base_url = endpoint();
    o.headers["Authorization"] = "Token " + token();
    o.headers["Accept"] = "application/csv";
    HttpRequest c{o};
    const std::string flux = "from(bucket: \"" + bucket() +
                             "\") |> range(start: 1677-09-21T00:12:43.145224194Z, stop: "
                             "2262-04-11T23:47:16.854775807Z) |> filter(fn: (r) => "
                             "r._measurement == \"" +
                             measurement + "\")";
    clink::config::JsonObject q;
    q["query"] = clink::config::JsonValue{flux};
    clink::config::JsonObject dialect;
    clink::config::JsonArray annotations;
    annotations.push_back(clink::config::JsonValue{std::string{"datatype"}});
    dialect["annotations"] = clink::config::JsonValue{std::move(annotations)};
    q["dialect"] = clink::config::JsonValue{std::move(dialect)};
    const auto res = c.post("/api/v2/query?org=" + org(),
                            clink::config::JsonValue{std::move(q)}.serialize(0),
                            "application/json");
    std::map<std::string, Point> out;
    if (res.status != 200) {
        return out;
    }
    std::vector<std::string> types;
    std::vector<std::string> header;
    for (const auto& rec : csv_records(res.body)) {
        if (rec.empty() || (rec.size() == 1 && rec[0].empty())) {
            header.clear();
            continue;
        }
        if (rec[0] == "#datatype") {
            types = rec;
            header.clear();
            continue;
        }
        if (rec[0].starts_with("#")) {
            continue;
        }
        if (header.empty()) {
            header = rec;
            continue;
        }
        std::map<std::string, std::string> row;
        std::string value_type;
        for (std::size_t i = 0; i < header.size() && i < rec.size(); ++i) {
            row[header[i]] = rec[i];
            if (header[i] == "_value" && i < types.size()) {
                value_type = types[i];
            }
        }
        Point p;
        for (const auto& [k, v] : row) {
            if (k.empty() || k == "result" || k == "table" || k.starts_with("_")) {
                continue;
            }
            p.tags[k] = v;
        }
        p.time = row["_time"];
        std::string key;
        for (const auto& [k, v] : p.tags) {
            key += (key.empty() ? "" : ",") + k + "=" + v;
        }
        key += "@" + p.time;
        auto& slot = out[key];
        slot.tags = p.tags;
        slot.time = p.time;
        slot.fields[row["_field"]] = Field{row["_value"], value_type};
    }
    return out;
}

}  // namespace clink::http_connector::influx_live
