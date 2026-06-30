#include "clink/lineage/lineage_graph.hpp"

#include <algorithm>
#include <cctype>
#include <deque>
#include <initializer_list>
#include <unordered_map>
#include <unordered_set>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/http/json_writer.hpp"

namespace clink::lineage {
namespace {

// Tokens in a factory type string that are channel suffixes or direction
// modifiers, not part of the connector family. Stripping them leaves the
// family, e.g. "s3_parquet_string_source" -> {s3, parquet}.
bool is_modifier_token(std::string_view t) {
    static const std::unordered_set<std::string_view> kModifiers = {"source",
                                                                    "sink",
                                                                    "2pc",
                                                                    "upsert",
                                                                    "cdc",
                                                                    "string",
                                                                    "int64",
                                                                    "row",
                                                                    "json",
                                                                    "text",
                                                                    "range",
                                                                    "poll",
                                                                    "partition",
                                                                    "double",
                                                                    "bool",
                                                                    "boolean"};
    return kModifiers.count(t) != 0;
}

std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    for (const char c : s) {
        if (c == delim) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(cur);
    return out;
}

// First present, non-empty value among the candidate keys.
std::string first_of(const std::map<std::string, std::string>& params,
                     std::initializer_list<const char*> keys) {
    for (const char* k : keys) {
        auto it = params.find(k);
        if (it != params.end() && !it->second.empty()) {
            return it->second;
        }
    }
    return {};
}

// Parse host/port/dbname out of a libpq-style conninfo DSN
// ("host=db port=5432 dbname=orders user=..."). Returns empty fields for
// anything not present.
struct ConnInfo {
    std::string host;
    std::string port;
    std::string dbname;
};

ConnInfo parse_conninfo(const std::string& conninfo) {
    ConnInfo ci;
    std::string key;
    std::string val;
    bool in_val = false;
    auto flush = [&] {
        if (key == "host" || key == "hostaddr") {
            ci.host = val;
        } else if (key == "port") {
            ci.port = val;
        } else if (key == "dbname" || key == "database") {
            ci.dbname = val;
        }
        key.clear();
        val.clear();
        in_val = false;
    };
    for (const char c : conninfo) {
        if (!in_val && c == '=') {
            in_val = true;
        } else if (std::isspace(static_cast<unsigned char>(c)) != 0) {
            if (in_val || !key.empty()) {
                flush();
            }
        } else if (in_val) {
            val.push_back(c);
        } else {
            key.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    if (in_val || !key.empty()) {
        flush();
    }
    return ci;
}

// "<host>" or "<host>:<port>" authority for jdbc-like systems, taking
// explicit host/port params first and falling back to a conninfo DSN.
std::string host_port(const std::map<std::string, std::string>& params) {
    std::string host = first_of(params, {"host", "hostname"});
    std::string port = first_of(params, {"port"});
    if (host.empty()) {
        const auto conninfo = first_of(params, {"conninfo", "connection_string", "dsn"});
        if (!conninfo.empty()) {
            const auto ci = parse_conninfo(conninfo);
            host = ci.host;
            if (port.empty()) {
                port = ci.port;
            }
        }
    }
    if (host.empty()) {
        return {};
    }
    return port.empty() ? host : host + ":" + port;
}

// "<db>.<table>" / "<table>" name for jdbc-like systems.
std::string db_table(const std::map<std::string, std::string>& params) {
    std::string db = first_of(params, {"database", "dbname", "db", "schema", "keyspace"});
    if (db.empty()) {
        const auto conninfo = first_of(params, {"conninfo", "connection_string", "dsn"});
        if (!conninfo.empty()) {
            db = parse_conninfo(conninfo).dbname;
        }
    }
    const std::string table = first_of(params, {"table", "table_name"});
    if (!table.empty()) {
        return db.empty() ? table : db + "." + table;
    }
    // No table: fall back to whatever query-like locator the connector has.
    return first_of(params, {"query", "slot_name", "topic"});
}

bool in_set(std::string_view family, std::initializer_list<const char*> set) {
    for (const char* s : set) {
        if (family == s) {
            return true;
        }
    }
    return false;
}

// Friendly name for a serialize_row_schema type code (row_columnar_batcher.hpp).
// Unknown codes pass through verbatim rather than being lost.
std::string friendly_type(const std::string& code) {
    if (code == "i64") {
        return "bigint";
    }
    if (code == "i32") {
        return "int";
    }
    if (code == "f64") {
        return "double";
    }
    if (code == "f32") {
        return "float";
    }
    if (code == "bool") {
        return "boolean";
    }
    if (code == "str") {
        return "string";
    }
    if (code.rfind("dec_", 0) == 0) {  // dec_<precision>_<scale>
        auto rest = code.substr(4);
        if (const auto us = rest.find('_'); us != std::string::npos) {
            return "decimal(" + rest.substr(0, us) + "," + rest.substr(us + 1) + ")";
        }
    }
    return code;
}

// Parse a schema_columns string ("name:code;name:code") into schema fields.
std::vector<SchemaField> parse_schema_columns(const std::string& spec) {
    std::vector<SchemaField> out;
    for (const auto& entry : split(spec, ';')) {
        if (entry.empty()) {
            continue;
        }
        const auto colon = entry.rfind(':');
        if (colon == std::string::npos) {
            continue;
        }
        out.push_back({entry.substr(0, colon), friendly_type(entry.substr(colon + 1))});
    }
    return out;
}

}  // namespace

std::string connector_family(const std::string& op_type) {
    const auto tokens = split(op_type, '_');
    std::vector<std::string> kept;
    for (const auto& t : tokens) {
        if (!is_modifier_token(t)) {
            kept.push_back(t);
        }
    }
    if (kept.empty()) {
        // All tokens were modifiers (e.g. "int64_range_source"): fall back
        // to the leading token so the family is never empty.
        return tokens.empty() ? op_type : tokens.front();
    }
    std::string out = kept.front();
    for (std::size_t i = 1; i < kept.size(); ++i) {
        out += "_" + kept[i];
    }
    return out;
}

LineageDataset dataset_for(const std::string& op_type,
                           const std::map<std::string, std::string>& params,
                           const std::string& out_channel) {
    return dataset_from_family(connector_family(op_type), params, out_channel);
}

LineageDataset dataset_from_family(const std::string& family,
                                   const std::map<std::string, std::string>& params,
                                   const std::string& out_channel) {
    LineageDataset d;
    d.facets["connector"] = family;
    if (!out_channel.empty()) {
        d.facets["channel"] = out_channel;  // coarse element-type hint (row/int64/string)
    }
    if (const auto fmt = first_of(params, {"format"}); !fmt.empty()) {
        d.facets["format"] = fmt;
    }
    if (const auto mode = first_of(params, {"mode"}); !mode.empty()) {
        d.facets["mode"] = mode;
    }
    // Column schema, when the op carries one (SQL Row-channel sources/sinks set
    // the schema_columns param: "name:code;name:code", codes per
    // serialize_row_schema in row_columnar_batcher.hpp).
    if (const auto sc = first_of(params, {"schema_columns"}); !sc.empty()) {
        d.schema = parse_schema_columns(sc);
    }

    // Message brokers and streaming systems.
    if (family == "kafka") {
        d.ns = "kafka://" + first_of(params, {"brokers", "bootstrap.servers", "bootstrap_servers"});
        d.name = first_of(params, {"topic"});
        return d;
    }
    if (family == "pulsar") {
        d.ns = "pulsar://" + first_of(params, {"service_url", "brokers", "url"});
        d.name = first_of(params, {"topic"});
        return d;
    }
    if (family == "nats") {
        d.ns = "nats://" + first_of(params, {"url", "servers", "brokers"});
        d.name = first_of(params, {"subject", "stream"});
        return d;
    }
    if (family == "rabbitmq") {
        d.ns = "amqp://" + first_of(params, {"uri", "url", "host"});
        d.name = first_of(params, {"queue", "routing_key", "exchange"});
        return d;
    }
    if (family == "redis") {
        const auto auth = host_port(params);
        d.ns = "redis://" + auth;
        d.name = first_of(params, {"stream", "key", "channel"});
        return d;
    }
    if (in_set(family, {"kinesis", "firehose"})) {
        d.ns = family + "://" + first_of(params, {"region"});
        d.name = first_of(params, {"stream", "stream_name", "delivery_stream"});
        return d;
    }
    if (family == "dynamodb") {
        d.ns = "dynamodb://" + first_of(params, {"region"});
        d.name = first_of(params, {"table", "table_name"});
        return d;
    }
    if (family == "pubsub") {
        d.ns = "pubsub://" + first_of(params, {"project", "project_id"});
        d.name = first_of(params, {"topic", "subscription"});
        return d;
    }

    // Relational / wide-column databases.
    if (in_set(family, {"postgres", "mysql", "clickhouse", "cassandra"})) {
        d.ns = family + "://" + host_port(params);
        d.name = db_table(params);
        return d;
    }

    // Object stores and file systems.
    if (in_set(family, {"s3", "s3_parquet"})) {
        d.ns = "s3://" + first_of(params, {"bucket"});
        d.name = first_of(params, {"key", "prefix", "key_prefix"});
        return d;
    }
    if (family == "gcs_parquet") {
        d.ns = "gs://" + first_of(params, {"bucket"});
        d.name = first_of(params, {"key", "prefix", "key_prefix"});
        return d;
    }
    if (family == "azure_parquet") {
        d.ns = "azure://" + first_of(params, {"container", "bucket", "account"});
        d.name = first_of(params, {"key", "prefix", "key_prefix"});
        return d;
    }
    if (family == "webhdfs_parquet") {
        d.ns = "webhdfs://" + first_of(params, {"host", "namenode", "url"});
        d.name = first_of(params, {"path", "key"});
        return d;
    }
    if (in_set(family, {"file", "filesystem", "parquet"})) {
        d.ns = "file";
        d.name = first_of(params, {"path", "base_path", "dir"});
        return d;
    }

    // Lakehouse table formats.
    if (family == "iceberg") {
        const auto cat = first_of(params, {"catalog_uri", "warehouse"});
        d.ns = cat.empty() ? "iceberg" : cat;
        const auto nslevels = first_of(params, {"namespace", "namespace_levels", "database"});
        const auto table = first_of(params, {"table", "table_name"});
        d.name = nslevels.empty() ? table : (table.empty() ? nslevels : nslevels + "." + table);
        return d;
    }
    if (family == "delta") {
        d.ns = "delta";
        d.name = first_of(params, {"path", "table_path", "location", "table"});
        return d;
    }

    // Search / observability / HTTP sinks.
    if (in_set(family, {"elasticsearch", "opensearch"})) {
        d.ns = family + "://" + first_of(params, {"host", "url", "endpoint"});
        d.name = first_of(params, {"index"});
        return d;
    }
    if (in_set(family, {"splunk", "splunk_hec"})) {
        d.ns = "splunk://" + first_of(params, {"host", "url", "endpoint"});
        d.name = first_of(params, {"index", "source"});
        return d;
    }
    if (family == "prometheus") {
        d.ns = "prometheus://" + first_of(params, {"endpoint", "url", "gateway"});
        d.name = first_of(params, {"job", "metric"});
        return d;
    }
    if (family == "influxdb") {
        d.ns = "influxdb://" + first_of(params, {"url", "host"});
        d.name = first_of(params, {"bucket", "measurement", "database"});
        return d;
    }
    if (family == "http") {
        d.ns = "http";
        d.name = first_of(params, {"url", "endpoint"});
        return d;
    }

    // Synthetic / in-process connectors. Represented for completeness so
    // the graph still shows a vertex; the namespace makes their nature
    // explicit.
    if (family == "nexmark") {
        d.ns = "nexmark";
        d.name = "events";
        return d;
    }
    if (family == "blackhole") {
        d.ns = "blackhole";
        return d;
    }
    if (family == "changelog") {
        d.ns = "changelog";
        d.name = first_of(params, {"topic", "name"});
        return d;
    }
    if (in_set(family, {"int64", "generator"})) {
        d.ns = "generator";
        d.name = family;
        return d;
    }

    // Generic fallback: still produce a usable identity for any connector
    // not enumerated above. The authority and name are drawn from the
    // common locator keys; the scheme is the family itself.
    const std::string name = first_of(params,
                                      {"topic",
                                       "table",
                                       "stream",
                                       "subject",
                                       "queue",
                                       "index",
                                       "name",
                                       "path",
                                       "key",
                                       "query",
                                       "url",
                                       "endpoint"});
    const std::string authority = first_of(params,
                                           {"brokers",
                                            "bootstrap.servers",
                                            "host",
                                            "conninfo",
                                            "bucket",
                                            "warehouse",
                                            "catalog_uri",
                                            "service_url",
                                            "url",
                                            "endpoint",
                                            "region",
                                            "uri"});
    d.ns = authority.empty() ? family : family + "://" + authority;
    d.name = name;
    return d;
}

namespace {

// Source boundedness, classified from the connector family (and mode for
// the CDC-vs-cursor connectors). Best-effort: returns "unknown" rather
// than guessing where the family alone does not decide it.
std::string source_boundedness(const std::string& family,
                               const std::map<std::string, std::string>& params) {
    if (in_set(family,
               {"file",
                "filesystem",
                "parquet",
                "s3_parquet",
                "gcs_parquet",
                "azure_parquet",
                "webhdfs_parquet",
                "s3",
                "int64",
                "generator",
                "clickhouse",
                "iceberg",
                "delta"})) {
        return "bounded";
    }
    if (in_set(family,
               {"kafka",
                "pulsar",
                "nats",
                "rabbitmq",
                "kinesis",
                "pubsub",
                "redis",
                "http",
                "nexmark"})) {
        return "unbounded";
    }
    if (in_set(family, {"postgres", "mysql"})) {
        const auto mode = first_of(params, {"mode"});
        return mode == "cdc" ? "unbounded" : "bounded";
    }
    return "unknown";
}

// Parse an input ref to its upstream op id, mirroring the planner /
// snapshot_job_graph: "id", "id.N" (split branch), "id::tag" (side
// output) all resolve to "id".
std::string upstream_id(const std::string& ref) {
    if (const auto p = ref.find("::"); p != std::string::npos) {
        return ref.substr(0, p);
    }
    if (const auto p = ref.rfind('.'); p != std::string::npos && p + 1 < ref.size()) {
        const auto suffix = ref.substr(p + 1);
        const bool all_digits = std::all_of(
            suffix.begin(), suffix.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
        if (all_digits) {
            return ref.substr(0, p);
        }
    }
    return ref;
}

LineageVertex make_vertex(const cluster::OperatorSpec& op) {
    LineageVertex v;
    v.id = op.id;
    v.uid = op.uid;
    v.name = op.display_name.empty() ? op.type : op.display_name;
    v.op_type = op.type;
    v.datasets.push_back(dataset_for(op.type, op.params, op.out_channel));
    return v;
}

// Write a column-lineage field list as a JSON array value into `w` (the
// caller has positioned the writer to receive a value).
void write_cl_array(http::JsonWriter& w, const std::vector<ColumnLineageField>& fields) {
    w.begin_array();
    for (const auto& f : fields) {
        w.begin_object();
        w.kv("output", f.output);
        if (!f.transformation.empty()) {
            w.kv("transformation", f.transformation);
        }
        w.key("inputs").begin_array();
        for (const auto& in : f.inputs) {
            w.begin_object();
            w.kv("namespace", in.ns);
            w.kv("name", in.name);
            w.kv("field", in.field);
            w.end_object();
        }
        w.end_array();
        w.end_object();
    }
    w.end_array();
}

std::vector<ColumnLineageField> parse_cl_array(const config::JsonValue& arr) {
    std::vector<ColumnLineageField> out;
    for (const auto& f : arr.as_array()) {
        if (!f.is_object()) {
            continue;
        }
        ColumnLineageField field;
        field.output = f.string_or("output", "");
        field.transformation = f.string_or("transformation", "");
        if (f.contains("inputs") && f.at("inputs").is_array()) {
            for (const auto& in : f.at("inputs").as_array()) {
                if (in.is_object()) {
                    field.inputs.push_back({in.string_or("namespace", ""),
                                            in.string_or("name", ""),
                                            in.string_or("field", "")});
                }
            }
        }
        out.push_back(std::move(field));
    }
    return out;
}

}  // namespace

LineageGraph extract_lineage(const cluster::JobGraphSpec& spec) {
    LineageGraph g;

    // Column lineage (SQL jobs only) is carried on the spec as a JSON object
    // keyed by sink op id: {"<sink_id>": [<field>...]}. Parse it once and
    // attach to the matching sink dataset below.
    std::unordered_map<std::string, std::vector<ColumnLineageField>> col_lineage_by_sink;
    if (!spec.column_lineage.empty()) {
        try {
            const auto root = config::parse(spec.column_lineage);
            if (root.is_object()) {
                for (const auto& [sink_id, arr] : root.as_object()) {
                    if (arr.is_array()) {
                        col_lineage_by_sink[sink_id] = parse_cl_array(arr);
                    }
                }
            }
        } catch (...) {
            // Malformed column lineage is non-fatal: table-level lineage stands.
        }
    }

    std::unordered_set<std::string> has_downstream;
    std::unordered_map<std::string, std::vector<std::string>> downstream;  // up -> [op ids]
    for (const auto& op : spec.ops) {
        for (const auto& in : op.inputs) {
            const auto up = upstream_id(in);
            has_downstream.insert(up);
            downstream[up].push_back(op.id);
        }
    }

    // Classify, mirroring snapshot_job_graph: a source is inputs-empty; a
    // sink has inputs and nothing reads from it. (An inputs-empty op with
    // no consumers is a source, not a sink.)
    std::unordered_set<std::string> sink_ids;
    for (const auto& op : spec.ops) {
        const bool is_source = op.inputs.empty();
        const bool is_sink =
            !op.inputs.empty() && has_downstream.find(op.id) == has_downstream.end();
        if (is_source) {
            auto v = make_vertex(op);
            v.boundedness = source_boundedness(connector_family(op.type), op.params);
            g.sources.push_back(std::move(v));
        } else if (is_sink) {
            auto v = make_vertex(op);
            if (auto it = col_lineage_by_sink.find(op.id);
                it != col_lineage_by_sink.end() && !v.datasets.empty()) {
                v.datasets.front().column_lineage = std::move(it->second);
            }
            g.sinks.push_back(std::move(v));
            sink_ids.insert(op.id);
        }
    }

    // Edges: real reachability from each source to each sink over the DAG.
    for (const auto& src : g.sources) {
        std::unordered_set<std::string> seen;
        std::deque<std::string> frontier{src.id};
        seen.insert(src.id);
        while (!frontier.empty()) {
            const auto cur = frontier.front();
            frontier.pop_front();
            const auto it = downstream.find(cur);
            if (it == downstream.end()) {
                continue;
            }
            for (const auto& next : it->second) {
                if (!seen.insert(next).second) {
                    continue;
                }
                if (sink_ids.count(next) != 0) {
                    g.edges.push_back({src.id, next});
                }
                frontier.push_back(next);
            }
        }
    }

    return g;
}

namespace {

void write_vertex(http::JsonWriter& w, const LineageVertex& v) {
    w.begin_object();
    w.kv("id", v.id);
    w.kv("uid", v.uid);
    w.kv("name", v.name);
    w.kv("op_type", v.op_type);
    if (!v.boundedness.empty()) {
        w.kv("boundedness", v.boundedness);
    }
    w.key("datasets").begin_array();
    for (const auto& d : v.datasets) {
        w.begin_object();
        w.kv("namespace", d.ns);
        w.kv("name", d.name);
        w.key("facets").begin_object();
        for (const auto& [k, val] : d.facets) {
            w.kv(k, val);
        }
        w.end_object();
        if (!d.schema.empty()) {
            w.key("schema").begin_array();
            for (const auto& f : d.schema) {
                w.begin_object();
                w.kv("name", f.name);
                w.kv("type", f.type);
                w.end_object();
            }
            w.end_array();
        }
        if (!d.column_lineage.empty()) {
            w.key("column_lineage");
            write_cl_array(w, d.column_lineage);
        }
        w.end_object();
    }
    w.end_array();
    w.end_object();
}

LineageDataset parse_dataset(const config::JsonValue& jv) {
    LineageDataset d;
    d.ns = jv.string_or("namespace", "");
    d.name = jv.string_or("name", "");
    if (jv.contains("facets") && jv.at("facets").is_object()) {
        for (const auto& [k, val] : jv.at("facets").as_object()) {
            if (val.is_string()) {
                d.facets[k] = val.as_string();
            }
        }
    }
    if (jv.contains("schema") && jv.at("schema").is_array()) {
        for (const auto& f : jv.at("schema").as_array()) {
            if (f.is_object()) {
                d.schema.push_back({f.string_or("name", ""), f.string_or("type", "")});
            }
        }
    }
    if (jv.contains("column_lineage") && jv.at("column_lineage").is_array()) {
        d.column_lineage = parse_cl_array(jv.at("column_lineage"));
    }
    return d;
}

LineageVertex parse_vertex(const config::JsonValue& jv) {
    LineageVertex v;
    v.id = jv.string_or("id", "");
    v.uid = jv.string_or("uid", "");
    v.name = jv.string_or("name", "");
    v.op_type = jv.string_or("op_type", "");
    v.boundedness = jv.string_or("boundedness", "");
    if (jv.contains("datasets") && jv.at("datasets").is_array()) {
        for (const auto& d : jv.at("datasets").as_array()) {
            if (d.is_object()) {
                v.datasets.push_back(parse_dataset(d));
            }
        }
    }
    return v;
}

}  // namespace

std::string LineageGraph::to_json() const {
    http::JsonWriter w;
    w.begin_object();
    w.key("sources").begin_array();
    for (const auto& v : sources) {
        write_vertex(w, v);
    }
    w.end_array();
    w.key("sinks").begin_array();
    for (const auto& v : sinks) {
        write_vertex(w, v);
    }
    w.end_array();
    w.key("edges").begin_array();
    for (const auto& e : edges) {
        w.begin_object();
        w.kv("from", e.from);
        w.kv("to", e.to);
        w.end_object();
    }
    w.end_array();
    w.end_object();
    return w.str();
}

LineageGraph LineageGraph::from_json(std::string_view json_text) {
    LineageGraph g;
    const auto root = config::parse(json_text);
    if (!root.is_object()) {
        return g;
    }
    const auto parse_array = [&](const char* key, std::vector<LineageVertex>& out) {
        if (root.contains(key) && root.at(key).is_array()) {
            for (const auto& v : root.at(key).as_array()) {
                if (v.is_object()) {
                    out.push_back(parse_vertex(v));
                }
            }
        }
    };
    parse_array("sources", g.sources);
    parse_array("sinks", g.sinks);
    if (root.contains("edges") && root.at("edges").is_array()) {
        for (const auto& e : root.at("edges").as_array()) {
            if (e.is_object()) {
                g.edges.push_back({e.string_or("from", ""), e.string_or("to", "")});
            }
        }
    }
    return g;
}

std::string column_lineage_to_json(const std::vector<ColumnLineageField>& fields) {
    http::JsonWriter w;
    write_cl_array(w, fields);
    return w.str();
}

std::vector<ColumnLineageField> column_lineage_from_json(std::string_view json_array) {
    const auto root = config::parse(json_array);
    if (!root.is_array()) {
        return {};
    }
    return parse_cl_array(root);
}

}  // namespace clink::lineage
