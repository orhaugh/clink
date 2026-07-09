#include "clink/cluster/job_graph.hpp"

#include <algorithm>
#include <cstdint>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::cluster {

std::string JobGraphSpec::serialize() const {
    std::string out;
    for (const auto& op : ops) {
        out += op.type;
        for (const auto& [k, v] : op.params) {
            out += ' ';
            out += k;
            out += '=';
            out += v;
        }
        out += '\n';
    }
    return out;
}

JobGraphSpec JobGraphSpec::parse(std::string_view text) {
    JobGraphSpec spec;
    std::size_t start = 0;
    while (start < text.size()) {
        const auto nl = text.find('\n', start);
        const auto end = (nl == std::string_view::npos) ? text.size() : nl;
        const auto line = text.substr(start, end - start);
        if (!line.empty()) {
            OperatorSpec op;
            std::size_t s = 0;
            while (s < line.size() && line[s] != ' ') {
                ++s;
            }
            op.type = std::string{line.substr(0, s)};
            while (s < line.size()) {
                while (s < line.size() && line[s] == ' ') {
                    ++s;
                }
                const auto kv_start = s;
                while (s < line.size() && line[s] != ' ') {
                    ++s;
                }
                const auto kv = line.substr(kv_start, s - kv_start);
                const auto eq = kv.find('=');
                if (eq == std::string_view::npos) {
                    continue;
                }
                op.params.emplace(std::string{kv.substr(0, eq)}, std::string{kv.substr(eq + 1)});
            }
            if (!op.type.empty()) {
                spec.ops.push_back(std::move(op));
            }
        }
        start = (nl == std::string_view::npos) ? text.size() : nl + 1;
    }
    return spec;
}

namespace {

// Tiny JSON helpers. The submitted graph is small (tens of ops); we
// reach for stringstream rather than a SAX-style writer.
std::string escape_json_string(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

// Rebuild a UDF list from its parsed JSON array (shared by from_json and
// unpack_udf_specs). Throws on a malformed entry so a bad spec fails at the
// parse site, consistent with from_json's auto-validate posture.
std::vector<UdfSpec> udf_specs_from_value(const config::JsonValue& arr) {
    std::vector<UdfSpec> out;
    out.reserve(arr.as_array().size());
    for (const auto& v : arr.as_array()) {
        if (!v.is_object()) {
            throw std::runtime_error("udfs: each entry must be a JSON object");
        }
        UdfSpec u;
        u.name = v.string_or("name", "");
        u.language = v.string_or("language", "");
        if (u.name.empty() || u.language.empty()) {
            throw std::runtime_error("udfs: entries need non-empty 'name' and 'language'");
        }
        if (v.contains("arg_types") && v.at("arg_types").is_array()) {
            for (const auto& t : v.at("arg_types").as_array()) {
                u.arg_types.push_back(t.as_string());
            }
        }
        u.return_type = v.string_or("return_type", "");
        if (v.contains("definitions") && v.at("definitions").is_array()) {
            for (const auto& d : v.at("definitions").as_array()) {
                u.definitions.push_back(d.as_string());
            }
        }
        u.module_b64 = v.string_or("module_b64", "");
        out.push_back(std::move(u));
    }
    return out;
}

}  // namespace

std::string pack_udf_specs(const std::vector<UdfSpec>& udfs) {
    std::string out = "[";
    for (std::size_t i = 0; i < udfs.size(); ++i) {
        const auto& u = udfs[i];
        if (i != 0) {
            out += ',';
        }
        out += "{\"name\":";
        out += escape_json_string(u.name);
        out += ",\"language\":";
        out += escape_json_string(u.language);
        out += ",\"arg_types\":[";
        for (std::size_t k = 0; k < u.arg_types.size(); ++k) {
            if (k != 0) {
                out += ',';
            }
            out += escape_json_string(u.arg_types[k]);
        }
        out += "],\"return_type\":";
        out += escape_json_string(u.return_type);
        out += ",\"definitions\":[";
        for (std::size_t k = 0; k < u.definitions.size(); ++k) {
            if (k != 0) {
                out += ',';
            }
            out += escape_json_string(u.definitions[k]);
        }
        out += "],\"module_b64\":";
        out += escape_json_string(u.module_b64);
        out += '}';
    }
    out += ']';
    return out;
}

std::vector<UdfSpec> unpack_udf_specs(std::string_view packed) {
    if (packed.empty()) {
        return {};
    }
    const auto v = config::parse(packed);
    if (!v.is_array()) {
        throw std::runtime_error("unpack_udf_specs: expected a JSON array");
    }
    return udf_specs_from_value(v);
}

std::string JobGraphSpec::to_json() const {
    std::string out = "{";
    if (!name.empty()) {
        out += "\"name\":";
        out += escape_json_string(name);
        out += ',';
    }
    out += "\"ops\":[";
    for (std::size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (i > 0) {
            out += ',';
        }
        out += "{\"id\":";
        out += escape_json_string(op.id);
        out += ",\"type\":";
        out += escape_json_string(op.type);
        if (!op.uid.empty()) {
            out += ",\"uid\":";
            out += escape_json_string(op.uid);
        }
        if (!op.display_name.empty()) {
            out += ",\"display_name\":";
            out += escape_json_string(op.display_name);
        }
        out += ",\"out_channel\":";
        out += escape_json_string(channel_type_name(op.out_channel));
        out += ",\"parallelism\":";
        out += std::to_string(op.parallelism);
        // Emit autoscale bounds only when set so older
        // consumers parsing this JSON without bounds-awareness see
        // unchanged shape.
        if (op.min_parallelism != 0 || op.max_parallelism != 0) {
            out += ",\"min_parallelism\":";
            out += std::to_string(op.min_parallelism);
            out += ",\"max_parallelism\":";
            out += std::to_string(op.max_parallelism);
        }
        out += ",\"inputs\":[";
        for (std::size_t j = 0; j < op.inputs.size(); ++j) {
            if (j > 0) {
                out += ',';
            }
            out += escape_json_string(op.inputs[j]);
        }
        out += "],\"params\":{";
        bool first = true;
        for (const auto& [k, v] : op.params) {
            if (!first) {
                out += ',';
            }
            first = false;
            out += escape_json_string(k);
            out += ':';
            out += escape_json_string(v);
        }
        out += '}';
        if (!op.key_by.empty()) {
            out += ",\"key_by\":";
            out += escape_json_string(op.key_by);
        }
        if (!op.side_outputs.empty()) {
            out += ",\"side_outputs\":[";
            for (std::size_t k = 0; k < op.side_outputs.size(); ++k) {
                if (k > 0) {
                    out += ',';
                }
                out += "{\"tag\":";
                out += escape_json_string(op.side_outputs[k].tag);
                out += ",\"channel_type\":";
                out += escape_json_string(channel_type_name(op.side_outputs[k].channel_type));
                out += '}';
            }
            out += ']';
        }
        out += '}';
    }
    out += "]";
    // State schema evolution: a single packed (op|type|ver\n...) string.
    // Emitted only when non-empty so the JSON shape is unchanged for
    // jobs that declare no expected versions.
    if (!expected_state_versions.empty()) {
        out += ",\"expected_state_versions\":";
        out += escape_json_string(expected_state_versions.pack());
    }
    // Shipped UDF declarations: pack_udf_specs emits a JSON array, so it
    // nests raw (like column_lineage below). Emitted only when non-empty.
    if (!udfs.empty()) {
        out += ",\"udfs\":";
        out += pack_udf_specs(udfs);
    }
    // column_lineage is already a JSON object string; emit it raw (not as an
    // escaped string) so it nests in the spec and parses back as an object.
    if (!column_lineage.empty()) {
        out += ",\"column_lineage\":";
        out += column_lineage;
    }
    out += "}";
    return out;
}

JobGraphSpec JobGraphSpec::from_json(std::string_view json_text) {
    auto root = config::parse(json_text);
    if (!root.is_object()) {
        throw std::runtime_error("JobGraphSpec::from_json: top-level must be an object");
    }
    if (!root.contains("ops")) {
        throw std::runtime_error("JobGraphSpec::from_json: missing 'ops' array");
    }
    const auto& ops_val = root.at("ops");
    if (!ops_val.is_array()) {
        throw std::runtime_error("JobGraphSpec::from_json: 'ops' must be an array");
    }
    JobGraphSpec spec;
    spec.ops.reserve(ops_val.as_array().size());
    for (const auto& v : ops_val.as_array()) {
        if (!v.is_object()) {
            throw std::runtime_error("JobGraphSpec::from_json: each op must be an object");
        }
        OperatorSpec op;
        op.type = v.string_or("type", "");
        if (op.type.empty()) {
            throw std::runtime_error("JobGraphSpec::from_json: op missing 'type'");
        }
        op.id = v.string_or("id", "");
        if (op.id.empty()) {
            throw std::runtime_error("JobGraphSpec::from_json: op missing 'id'");
        }
        op.uid = v.string_or("uid", "");
        op.display_name = v.string_or("display_name", "");
        const auto out_name = v.string_or("out_channel", "");
        if (out_name.empty()) {
            throw std::runtime_error("JobGraphSpec::from_json: op '" + op.id +
                                     "' missing 'out_channel'");
        }
        const auto out_ct = channel_type_from_name(out_name);
        if (!out_ct.has_value()) {
            throw std::runtime_error("JobGraphSpec::from_json: op '" + op.id +
                                     "' has unknown out_channel '" + out_name + "'");
        }
        op.out_channel = *out_ct;
        op.parallelism = static_cast<std::uint32_t>(v.int_or("parallelism", 1));
        if (op.parallelism == 0) {
            throw std::runtime_error("JobGraphSpec::from_json: op '" + op.id +
                                     "' parallelism must be >= 1");
        }
        // Optional autoscale bounds. Default 0 (no bounds).
        // validate() enforces the cross-field invariants below.
        op.min_parallelism = static_cast<std::uint32_t>(v.int_or("min_parallelism", 0));
        op.max_parallelism = static_cast<std::uint32_t>(v.int_or("max_parallelism", 0));
        if (v.contains("inputs")) {
            const auto& ins = v.at("inputs");
            if (!ins.is_array()) {
                throw std::runtime_error("JobGraphSpec::from_json: 'inputs' must be an array");
            }
            for (const auto& iv : ins.as_array()) {
                if (!iv.is_string()) {
                    throw std::runtime_error("JobGraphSpec::from_json: input ids must be strings");
                }
                op.inputs.push_back(iv.as_string());
            }
        }
        if (v.contains("params")) {
            const auto& pv = v.at("params");
            if (!pv.is_object()) {
                throw std::runtime_error("JobGraphSpec::from_json: 'params' must be an object");
            }
            for (const auto& [pk, pval] : pv.as_object()) {
                if (pval.is_string()) {
                    op.params.emplace(pk, pval.as_string());
                } else if (pval.is_number()) {
                    // Convert numbers to strings for the param bag - the
                    // factory already parses on demand via param_int64_or.
                    op.params.emplace(pk,
                                      std::to_string(static_cast<std::int64_t>(pval.as_number())));
                } else if (pval.is_bool()) {
                    op.params.emplace(pk, pval.as_bool() ? "true" : "false");
                } else {
                    throw std::runtime_error("JobGraphSpec::from_json: param '" + pk +
                                             "' must be string/number/bool");
                }
            }
        }
        op.key_by = v.string_or("key_by", "");
        if (v.contains("side_outputs")) {
            const auto& sv = v.at("side_outputs");
            if (!sv.is_array()) {
                throw std::runtime_error(
                    "JobGraphSpec::from_json: 'side_outputs' must be an array");
            }
            for (const auto& s : sv.as_array()) {
                if (!s.is_object()) {
                    throw std::runtime_error(
                        "JobGraphSpec::from_json: side_output entry must be an object");
                }
                SideOutputDecl d;
                d.tag = s.string_or("tag", "");
                if (d.tag.empty()) {
                    throw std::runtime_error("JobGraphSpec::from_json: side_output missing 'tag'");
                }
                const auto ct_name = s.string_or("channel_type", "");
                const auto ct = channel_type_from_name(ct_name);
                if (!ct.has_value()) {
                    throw std::runtime_error("JobGraphSpec::from_json: side_output '" + d.tag +
                                             "' has unknown channel_type '" + ct_name + "'");
                }
                d.channel_type = *ct;
                op.side_outputs.push_back(std::move(d));
            }
        }
        spec.ops.push_back(std::move(op));
    }
    // Optional human-readable job name (absent for unnamed jobs).
    spec.name = root.string_or("name", "");
    // Optional column-lineage object (absent for non-SQL jobs); stored as its
    // JSON text so it round-trips without a typed model in the spec layer.
    if (root.contains("column_lineage") && root.at("column_lineage").is_object()) {
        spec.column_lineage = root.at("column_lineage").serialize();
    }
    // State schema evolution: unpack the expected-version map if the
    // spec carries one (absent for jobs that declare nothing).
    if (const auto packed = root.string_or("expected_state_versions", ""); !packed.empty()) {
        spec.expected_state_versions = StateVersionMap::unpack(packed);
    }
    // Shipped UDF declarations (absent for jobs that use none).
    if (root.contains("udfs") && root.at("udfs").is_array()) {
        spec.udfs = udf_specs_from_value(root.at("udfs"));
    }
    // Auto-validate so callers parsing untrusted JSON get the same
    // invariant checks the planner would run (unique ids, inputs
    // resolve, no cycles). Without this, a malformed spec only fails
    // at plan_job time, which can be far from the parse site and is
    // exactly the kind of latent-error footgun we avoid elsewhere.
    spec.validate();
    return spec;
}

void JobGraphSpec::validate() const {
    // Unique ids.
    std::unordered_set<std::string> ids;
    for (const auto& op : ops) {
        if (op.id.empty()) {
            throw std::runtime_error("JobGraphSpec::validate: op type '" + op.type +
                                     "' has empty id");
        }
        if (!ids.insert(op.id).second) {
            throw std::runtime_error("JobGraphSpec::validate: duplicate op id '" + op.id + "'");
        }
        // Autoscale-bound invariants. Either both bounds
        // are zero (no autoscaling) or both are non-zero and form a
        // valid range bracketing the current parallelism.
        const bool min_set = op.min_parallelism != 0;
        const bool max_set = op.max_parallelism != 0;
        if (min_set != max_set) {
            throw std::runtime_error(
                "JobGraphSpec::validate: op '" + op.id +
                "' must set both min_parallelism and max_parallelism, or neither");
        }
        if (min_set) {
            if (op.min_parallelism > op.parallelism) {
                throw std::runtime_error("JobGraphSpec::validate: op '" + op.id +
                                         "' min_parallelism " + std::to_string(op.min_parallelism) +
                                         " exceeds current parallelism " +
                                         std::to_string(op.parallelism));
            }
            if (op.max_parallelism < op.parallelism) {
                throw std::runtime_error("JobGraphSpec::validate: op '" + op.id +
                                         "' max_parallelism " + std::to_string(op.max_parallelism) +
                                         " is below current parallelism " +
                                         std::to_string(op.parallelism));
            }
            if (op.min_parallelism > op.max_parallelism) {
                throw std::runtime_error("JobGraphSpec::validate: op '" + op.id +
                                         "' min_parallelism " + std::to_string(op.min_parallelism) +
                                         " exceeds max_parallelism " +
                                         std::to_string(op.max_parallelism));
            }
        }
    }
    // Strip the optional ".N" branch suffix from an input ref. Used so
    // validate() must look past split-branch suffix ("splitter.0") and
    // side-output tag suffix ("emitter::tagname") to find the bare op id.
    auto strip_branch = [&](const std::string& raw) {
        if (const auto colons = raw.find("::"); colons != std::string::npos) {
            return raw.substr(0, colons);
        }
        const auto dot = raw.rfind('.');
        if (dot == std::string::npos) {
            return raw;
        }
        const auto suffix = raw.substr(dot + 1);
        if (suffix.empty()) {
            return raw;
        }
        for (char c : suffix) {
            if (c < '0' || c > '9') {
                return raw;
            }
        }
        // Suffix is all digits - treat as branch index, return id only.
        return raw.substr(0, dot);
    };
    // All inputs resolve.
    for (const auto& op : ops) {
        for (const auto& raw : op.inputs) {
            const auto in = strip_branch(raw);
            if (!ids.contains(in)) {
                throw std::runtime_error("JobGraphSpec::validate: op '" + op.id +
                                         "' references unknown input '" + raw + "'");
            }
        }
    }
    // No cycles. Topo-sort by Kahn's algorithm; if any op remains
    // un-emitted at the end, there's a cycle.
    std::unordered_map<std::string, std::size_t> indegree;
    std::unordered_map<std::string, std::vector<std::string>> downstream;
    for (const auto& op : ops) {
        indegree[op.id] = op.inputs.size();
        for (const auto& raw : op.inputs) {
            downstream[strip_branch(raw)].push_back(op.id);
        }
    }
    std::vector<std::string> ready;
    for (const auto& [id, d] : indegree) {
        if (d == 0) {
            ready.push_back(id);
        }
    }
    std::size_t emitted = 0;
    while (!ready.empty()) {
        const auto next = std::move(ready.back());
        ready.pop_back();
        ++emitted;
        for (const auto& d : downstream[next]) {
            if (--indegree[d] == 0) {
                ready.push_back(d);
            }
        }
    }
    if (emitted != ops.size()) {
        throw std::runtime_error("JobGraphSpec::validate: cycle detected in graph");
    }
}

}  // namespace clink::cluster
