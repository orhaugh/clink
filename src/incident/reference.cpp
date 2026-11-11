#include "clink/incident/reference.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <map>
#include <set>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>

#include "clink/core/sha256.hpp"

namespace clink::incident::reconcile {

namespace fs = std::filesystem;

namespace {

std::string sha256_text(std::string_view bytes) {
    Sha256 h;
    h.update(bytes.data(), bytes.size());
    return Sha256::to_hex(h.finalize());
}

bool digits(std::string_view s) {
    return !s.empty() &&
           std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; });
}

// [-]digits, without a leading zero unless it is "0", and never "-0".
bool canonical_integer(std::string_view s, bool allow_negative) {
    if (!s.empty() && s.front() == '-') {
        if (!allow_negative || s == "-0") {
            return false;
        }
        s.remove_prefix(1);
    }
    return digits(s) && (s.size() == 1 || s.front() != '0');
}

bool lower_hex(std::string_view s, std::size_t n) {
    return s.size() == n && std::all_of(s.begin(), s.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

// <integer>@<want>
bool scaled(std::string_view body, int want) {
    const auto at = body.rfind('@');
    return at != std::string_view::npos && canonical_integer(body.substr(0, at), true) &&
           body.substr(at + 1) == std::to_string(want);
}

std::string key_of(const Row& row, const std::vector<std::size_t>& key) {
    std::string k;
    for (const auto i : key) {
        const auto& c = row.at(i);
        k += std::to_string(c.size());
        k += ':';
        k += c;
    }
    return k;
}

Row key_cells(const Row& row, const std::vector<std::size_t>& key) {
    Row out;
    for (const auto i : key) {
        out.push_back(row.at(i));
    }
    return out;
}

std::string graph_sha256(const Bundle& b) {
    if (b.find("job/manifest.json") == nullptr) {
        return "";
    }
    try {
        std::ifstream in(b.root / "job/manifest.json", std::ios::binary);
        std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        const auto m = config::parse(text);
        if (m.contains("graph_json") && m.at("graph_json").is_string()) {
            return sha256_text(m.at("graph_json").as_string());
        }
    } catch (const std::exception&) {
        // An unreadable job manifest leaves the scope without a graph fingerprint.
    }
    return "";
}

config::JsonValue or_null(const std::string& s) {
    return s.empty() ? config::JsonValue{} : config::JsonValue{s};
}

// The first field in which two scope documents differ, as text for a refusal.
std::string first_difference(const config::JsonValue& theirs, const config::JsonValue& ours) {
    static constexpr std::string_view kOrder[] = {"format",
                                                  "version",
                                                  "job",
                                                  "sink_op",
                                                  "attempts",
                                                  "graph_sha256",
                                                  "checkpoint",
                                                  "starts_after",
                                                  "from_run_start",
                                                  "starts_at_run_start",
                                                  "epochs",
                                                  "sources",
                                                  "sources_complete",
                                                  "output",
                                                  "transform",
                                                  "lineage",
                                                  "finality",
                                                  "scope_sha256"};
    for (const auto field : kOrder) {
        const bool a = theirs.contains(field);
        const bool b = ours.contains(field);
        if (a != b || (a && !(theirs.at(field) == ours.at(field)))) {
            const auto shown = [](const config::JsonValue& d, std::string_view f) {
                if (!d.contains(f)) {
                    return std::string{"(absent)"};
                }
                std::string t = d.at(f).serialize();
                return t.size() > 160 ? t.substr(0, 160) + "..." : t;
            };
            return std::string{field} + " is " + shown(theirs, field) + " in the reference and " +
                   shown(ours, field) + " in this audit's scope";
        }
    }
    return "the documents differ";
}

config::JsonValue strings_json(const std::vector<std::string>& v) {
    config::JsonArray a;
    for (const auto& s : v) {
        a.emplace_back(s);
    }
    return config::JsonValue{std::move(a)};
}

// The window's rules as the scope document carries them. The same field names
// are what a reference declares its own rules with (the artifact's
// "semantics"), so the two are compared field by field.
config::JsonValue transform_document(const WindowSemantics& w) {
    config::JsonObject t;
    t["kind"] = config::JsonValue{std::string{kTumblingWindowKind}};
    t["size_ms"] = config::JsonValue{w.size_ms};
    t["time_column"] = config::JsonValue{w.time_column};
    t["time_unit"] = config::JsonValue{std::string{"epoch milliseconds"}};
    t["group_keys"] = strings_json(w.group_keys);
    t["identity"] = strings_json(w.identity);
    t["start_column"] = config::JsonValue{w.start_column};
    t["end_column"] = config::JsonValue{w.end_column};
    config::JsonArray aggs;
    for (const auto& g : w.aggregates) {
        config::JsonObject o;
        o["function"] = config::JsonValue{g.function};
        o["input"] = g.input.empty() ? config::JsonValue{} : config::JsonValue{g.input};
        o["output"] = config::JsonValue{g.output};
        aggs.emplace_back(std::move(o));
    }
    t["aggregates"] = config::JsonValue{std::move(aggs)};
    config::JsonObject wm;
    wm["strategy"] = config::JsonValue{std::string{kWatermarkStrategy}};
    wm["lag_ms"] = config::JsonValue{w.lag_ms};
    wm["idleness"] = config::JsonValue{std::string{"none"}};
    wm["assigned_on"] = config::JsonValue{std::string{"every record the source delivers"}};
    t["watermark"] = config::JsonValue{std::move(wm)};
    config::JsonObject late;
    late["allowed_lateness_ms"] = config::JsonValue{w.allowed_lateness_ms};
    late["late_records"] = config::JsonValue{std::string{"dropped"}};
    t["lateness"] = config::JsonValue{std::move(late)};
    t["source_parallelism"] = config::JsonValue{static_cast<std::int64_t>(w.source_parallelism)};
    t["alignment"] = config::JsonValue{w.alignment};
    return config::JsonValue{std::move(t)};
}

// One window line of a version 2 reference: its identity's cells, each
// canonical for its column, and why it is left out.
ExcludedWindow window_line(const config::JsonValue& w,
                           const std::vector<std::string>& identity,
                           const std::vector<Column>& columns,
                           const std::map<std::string, std::size_t>& index,
                           std::size_t line_no) {
    const std::string where = "line " + std::to_string(line_no) + " (a window)";
    ExcludedWindow out;
    if (!w.contains("identity") || !w.at("identity").is_object() ||
        w.at("identity").as_object().size() != identity.size()) {
        throw ReferenceRefused(where + " does not name exactly the window identity's " +
                               std::to_string(identity.size()) + " column(s)");
    }
    for (const auto& name : identity) {
        const auto& cells = w.at("identity");
        const auto it = index.find(name);
        if (!cells.contains(name) || it == index.end()) {
            throw ReferenceRefused(where + " has no cell for " + name);
        }
        const auto& cell = cells.at(name);
        if (!cell.is_string() || !canonical_for(cell.as_string(), columns[it->second].type)) {
            throw ReferenceRefused(
                where + ": " + name + " holds " + cell.serialize().substr(0, 80) +
                ", which is not a canonical cell of " + columns[it->second].type.spelling);
        }
        out.identity.push_back(cell.as_string());
    }
    out.status = w.string_or("status", "");
    if (out.status != "open" && out.status != "undetermined") {
        throw ReferenceRefused(where + " has status '" + out.status +
                               "'; a window is left out as open or undetermined");
    }
    if (!w.contains("end_ms") || !w.at("end_ms").is_integral_number()) {
        throw ReferenceRefused(where + " gives no end_ms");
    }
    out.end_ms = w.at("end_ms").as_int();
    if (!w.contains("why") || !w.at("why").is_string()) {
        throw ReferenceRefused(where + " does not say why it is left out");
    }
    out.why = w.at("why").as_string();
    return out;
}

// The watermark bounds a version 2 reference computed: one per checkpoint of
// the scope's lineage, in its order.
std::vector<SourceBound> source_bounds_of(const config::JsonValue& end,
                                          const config::JsonValue& document) {
    std::vector<std::uint64_t> want;
    for (const auto& c : document.at("lineage").at("checkpoints").as_array()) {
        want.push_back(static_cast<std::uint64_t>(c.at("checkpoint").as_int()));
    }
    if (!end.contains("source_bounds") || !end.at("source_bounds").is_array()) {
        throw ReferenceRefused(
            "the reference's end line gives no source_bounds: the watermark the source's "
            "records allow at each checkpoint of the lineage");
    }
    std::vector<SourceBound> out;
    for (const auto& b : end.at("source_bounds").as_array()) {
        if (!b.is_object() || !b.contains("checkpoint") ||
            !b.at("checkpoint").is_integral_number() || !b.contains("watermark_ms") ||
            !(b.at("watermark_ms").is_null() || b.at("watermark_ms").is_integral_number())) {
            throw ReferenceRefused(
                "the reference's source_bounds hold an entry that is not "
                "{checkpoint, watermark_ms}");
        }
        SourceBound s;
        s.checkpoint = static_cast<std::uint64_t>(b.at("checkpoint").as_int());
        if (!b.at("watermark_ms").is_null()) {
            s.watermark_ms = b.at("watermark_ms").as_int();
        }
        out.push_back(s);
    }
    std::vector<std::uint64_t> have;
    for (const auto& s : out) {
        have.push_back(s.checkpoint);
    }
    if (have != want) {
        throw ReferenceRefused(
            "the reference's source_bounds do not name exactly the checkpoints of the scope's "
            "lineage, in order");
    }
    return out;
}

std::vector<std::string> strings_of(const config::JsonValue& o, std::string_view field) {
    std::vector<std::string> out;
    if (o.contains(field) && o.at(field).is_array()) {
        for (const auto& v : o.at(field).as_array()) {
            out.push_back(v.is_string() ? v.as_string() : v.serialize());
        }
    }
    return out;
}

}  // namespace

std::string scope_fingerprint(const config::JsonValue& document) {
    config::JsonObject o = document.as_object();
    if (const auto it = o.find("scope_sha256"); it != o.end()) {
        o.erase(it);
    }
    return sha256_text(config::JsonValue{std::move(o)}.serialize());
}

config::JsonValue scope_document(const Bundle& b, const Scope& sc) {
    config::JsonObject d;
    d["format"] = config::JsonValue{std::string{kScopeFormat}};
    d["version"] = config::JsonValue{kScopeVersion};
    d["job"] = config::JsonValue{std::to_string(sc.job)};
    d["sink_op"] = config::JsonValue{std::to_string(sc.sink_op)};
    {
        std::set<std::string> attempts;
        for (const auto& e : sc.intervals) {
            attempts.insert(e.attempt);
        }
        config::JsonArray a;
        for (const auto& s : attempts) {
            a.emplace_back(s);
        }
        d["attempts"] = config::JsonValue{std::move(a)};
    }
    d["graph_sha256"] = or_null(graph_sha256(b));
    d["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(sc.checkpoint)};
    d["starts_after"] = config::JsonValue{static_cast<std::int64_t>(sc.starts_after)};
    d["from_run_start"] = config::JsonValue{sc.whole_run};
    d["starts_at_run_start"] = config::JsonValue{sc.starts_at_run_start};
    {
        config::JsonArray e;
        for (const auto x : sc.epochs) {
            e.emplace_back(static_cast<std::int64_t>(x));
        }
        d["epochs"] = config::JsonValue{std::move(e)};
    }
    bool complete = sc.source.from_complete && sc.source.to_complete && !sc.source.ranges.empty();
    config::JsonArray sources;
    for (const auto& id : sc.source.identities) {
        config::JsonObject s;
        s["op"] = config::JsonValue{std::to_string(id.op)};
        const std::optional<SourceLog>& log = id.end ? id.end : id.start;
        s["topic"] = log ? config::JsonValue{log->topic} : config::JsonValue{};
        s["cluster_id"] = log ? or_null(log->cluster_id) : config::JsonValue{};
        s["topic_id"] = log ? or_null(log->topic_id) : config::JsonValue{};
        s["same_log"] = id.same_log() ? config::JsonValue{*id.same_log()} : config::JsonValue{};
        s["isolation_level"] = config::JsonValue{sc.source.isolation_level};
        s["isolation_set_by_job"] = config::JsonValue{sc.source.isolation_set_by_job};
        if (!id.end || id.same_log() == std::optional<bool>{false}) {
            complete = false;
        }
        config::JsonArray parts;
        for (const auto& r : sc.source.ranges) {
            if (r.op != id.op) {
                continue;
            }
            config::JsonObject p;
            p["partition"] = config::JsonValue{static_cast<std::int64_t>(r.partition)};
            p["from"] = r.from ? config::JsonValue{*r.from} : config::JsonValue{};
            p["to"] = r.to ? config::JsonValue{*r.to} : config::JsonValue{};
            if (!r.to || (!r.from && !sc.starts_at_run_start)) {
                complete = false;
            }
            parts.emplace_back(std::move(p));
        }
        s["partitions"] = config::JsonValue{std::move(parts)};
        sources.emplace_back(std::move(s));
    }
    d["sources"] = config::JsonValue{std::move(sources)};
    d["sources_complete"] = config::JsonValue{complete};
    {
        config::JsonObject out;
        config::JsonArray cols;
        for (const auto& c : sc.columns) {
            config::JsonObject col;
            col["name"] = config::JsonValue{c.name};
            col["type"] = config::JsonValue{c.type.spelling};
            cols.emplace_back(std::move(col));
        }
        out["columns"] = config::JsonValue{std::move(cols)};
        out["semantics"] = config::JsonValue{std::string{kCanonicalSemantics}};
        d["output"] = config::JsonValue{std::move(out)};
    }
    if (sc.window) {
        d["version"] =
            config::JsonValue{sc.base ? kWindowIncrementScopeVersion : kWindowScopeVersion};
        d["transform"] = transform_document(*sc.window);
        if (sc.base) {
            config::JsonObject base;
            base["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(sc.base->checkpoint)};
            config::JsonArray positions;
            for (const auto& p : sc.base->positions) {
                config::JsonObject po;
                po["op"] = config::JsonValue{std::to_string(p.op)};
                po["partition"] = config::JsonValue{static_cast<std::int64_t>(p.partition)};
                po["next_offset"] = p.to ? config::JsonValue{*p.to} : config::JsonValue{};
                positions.emplace_back(std::move(po));
            }
            base["positions"] = config::JsonValue{std::move(positions)};
            base["watermark_ms"] = sc.base->watermark_ms ? config::JsonValue{*sc.base->watermark_ms}
                                                         : config::JsonValue{};
            d["base"] = config::JsonValue{std::move(base)};
        }
        config::JsonObject lineage;
        config::JsonArray restores;
        for (const auto& r : sc.restores) {
            config::JsonObject o;
            o["restored_from"] = config::JsonValue{static_cast<std::int64_t>(r.restored_from)};
            o["first_epoch"] = config::JsonValue{static_cast<std::int64_t>(r.first_epoch)};
            restores.emplace_back(std::move(o));
        }
        lineage["restores"] = config::JsonValue{std::move(restores)};
        config::JsonArray checkpoints;
        std::optional<std::int64_t> final_watermark;
        for (const auto& lc : sc.lineage) {
            config::JsonObject o;
            o["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(lc.checkpoint)};
            config::JsonArray positions;
            for (const auto& p : lc.positions) {
                config::JsonObject po;
                po["op"] = config::JsonValue{std::to_string(p.op)};
                po["partition"] = config::JsonValue{static_cast<std::int64_t>(p.partition)};
                po["next_offset"] = p.to ? config::JsonValue{*p.to} : config::JsonValue{};
                positions.emplace_back(std::move(po));
            }
            o["positions"] = config::JsonValue{std::move(positions)};
            o["watermark_ms"] =
                lc.watermark_ms ? config::JsonValue{*lc.watermark_ms} : config::JsonValue{};
            if (lc.checkpoint == sc.checkpoint) {
                final_watermark = lc.watermark_ms;
            }
            checkpoints.emplace_back(std::move(o));
        }
        lineage["checkpoints"] = config::JsonValue{std::move(checkpoints)};
        d["lineage"] = config::JsonValue{std::move(lineage)};
        config::JsonObject finality;
        finality["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(sc.checkpoint)};
        finality["watermark_ms"] =
            final_watermark ? config::JsonValue{*final_watermark} : config::JsonValue{};
        d["finality"] = config::JsonValue{std::move(finality)};
        // A lineage from a start with no state, or an increment from its base.
        // (A base with no recorded watermark is one no window had closed by.)
        const bool starts = sc.base ? sc.starts_after == sc.base->checkpoint
                                    : sc.starts_at_run_start && sc.starts_after == 0;
        if (sc.lineage.empty() || sc.lineage.back().checkpoint != sc.checkpoint || !starts) {
            d["sources_complete"] = config::JsonValue{false};
        }
    }
    config::JsonValue doc{std::move(d)};
    const std::string fp = scope_fingerprint(doc);
    config::JsonObject withfp = doc.as_object();
    withfp["scope_sha256"] = config::JsonValue{fp};
    return config::JsonValue{std::move(withfp)};
}

std::vector<std::string> semantics_differences(const config::JsonValue& transform,
                                               const config::JsonValue& semantics) {
    // The rules that decide which window a record lands in, when a window
    // closes and what becomes of a record that arrives after: what a reference
    // has to implement the same way for its rows to be the job's.
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 8> kFields{{
        {"", "kind"},
        {"", "size_ms"},
        {"", "time_column"},
        {"watermark", "strategy"},
        {"watermark", "lag_ms"},
        {"watermark", "idleness"},
        {"lateness", "allowed_lateness_ms"},
        {"lateness", "late_records"},
    }};
    std::vector<std::string> out;
    const auto at = [](const config::JsonValue& d,
                       std::string_view group,
                       std::string_view field) -> std::optional<config::JsonValue> {
        const config::JsonValue* o = &d;
        if (!group.empty()) {
            if (!d.is_object() || !d.contains(group) || !d.at(group).is_object()) {
                return std::nullopt;
            }
            o = &d.at(group);
        }
        if (!o->is_object() || !o->contains(field)) {
            return std::nullopt;
        }
        return o->at(field);
    };
    for (const auto& [group, field] : kFields) {
        const std::string name =
            group.empty() ? std::string{field} : std::string{group} + "." + std::string{field};
        const auto job = at(transform, group, field);
        const auto ref = at(semantics, group, field);
        if (!ref) {
            out.push_back("the reference does not say its " + name + " (the job's is " +
                          (job ? job->serialize() : std::string{"unknown"}) + ")");
        } else if (!job || !(*job == *ref)) {
            out.push_back(name + ": the job's is " +
                          (job ? job->serialize() : std::string{"unknown"}) + ", the reference's " +
                          ref->serialize());
        }
    }
    return out;
}

bool canonical_for(std::string_view cell, const ColumnType& type) {
    if (cell == "N") {
        return true;
    }
    if (cell.empty()) {
        return false;
    }
    const std::string_view body = cell.substr(1);
    switch (type.kind) {
        case TypeKind::Int:
            return cell.front() == 'I' && canonical_integer(body, true);
        case TypeKind::UInt:
            return cell.front() == 'I' && canonical_integer(body, false);
        case TypeKind::Float64:
            return cell == "Fnan" || (cell.front() == 'F' && lower_hex(body, 16));
        case TypeKind::Float32:
            return cell == "fnan" || (cell.front() == 'f' && lower_hex(body, 8));
        case TypeKind::Bool:
            return cell == "B0" || cell == "B1";
        case TypeKind::String:
            return cell.front() == 'S';
        case TypeKind::Decimal:
            return cell.front() == 'D' && scaled(body, type.scale);
        case TypeKind::DateTime64:
            return cell.front() == 'T' && scaled(body, type.precision);
        case TypeKind::DateTime:
            return cell.front() == 'T' && scaled(body, 0);
        case TypeKind::Date:
            return cell.front() == 'd' && canonical_integer(body, true);
        case TypeKind::Unsupported:
            return false;
    }
    return false;
}

Reference read_reference(const fs::path& path,
                         const config::JsonValue& document,
                         const std::vector<Column>& columns,
                         std::uint64_t max_bytes) {
    Reference ref;
    ref.path = path.string();
    std::error_code ec;
    const auto size = fs::file_size(path, ec);
    if (ec || !fs::is_regular_file(path, ec)) {
        throw ReferenceRefused(path.string() + " is not a readable regular file");
    }
    if (size > max_bytes) {
        throw ReferenceRefused(path.string() + " is " + std::to_string(size) +
                               " bytes, more than --max-reference-bytes (" +
                               std::to_string(max_bytes) + ")");
    }
    std::string text;
    {
        std::ifstream in(path, std::ios::binary);
        text.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (!in && !in.eof()) {
            throw ReferenceRefused("cannot read " + path.string());
        }
    }
    // Lines, each with its newline (the last may have none).
    std::vector<std::string_view> lines;
    for (std::size_t at = 0; at < text.size();) {
        const auto nl = text.find('\n', at);
        const auto end = nl == std::string::npos ? text.size() : nl + 1;
        lines.emplace_back(std::string_view{text}.substr(at, end - at));
        at = end;
    }
    const auto body = [](std::string_view line) {
        return line.ends_with('\n') ? line.substr(0, line.size() - 1) : line;
    };
    if (lines.empty()) {
        throw ReferenceRefused(path.string() + " is empty");
    }
    config::JsonValue header;
    try {
        header = config::parse(body(lines.front()));
    } catch (const std::exception& e) {
        throw ReferenceRefused(std::string{"the reference's first line is not JSON: "} + e.what());
    }
    if (!header.is_object() || header.string_or("format", "") != kReferenceFormat) {
        throw ReferenceRefused("the reference's first line is not a " +
                               std::string{kReferenceFormat} + " header");
    }
    // A record-wise scope's reference is version 1, a windowed scope's 2, an
    // increment's 3.
    const bool windowed = document.contains("transform");
    const bool increment = document.contains("base");
    const std::int64_t want_version = increment  ? kWindowIncrementReferenceVersion
                                      : windowed ? kWindowReferenceVersion
                                                 : kReferenceVersion;
    if (!header.contains("version") || !header.at("version").is_integral_number() ||
        header.at("version").as_int() != want_version) {
        throw ReferenceRefused("the reference is not version " + std::to_string(want_version) +
                               " of its format, which this release reads for a " +
                               (increment  ? std::string{"windowed increment's"}
                                : windowed ? std::string{"windowed"}
                                           : std::string{"record-wise"}) +
                               " scope");
    }
    ref.version = want_version;
    if (!header.contains("scope") || !header.at("scope").is_object()) {
        throw ReferenceRefused("the reference names no scope");
    }
    const auto& scope = header.at("scope");
    if (!(scope == document)) {
        throw ReferenceRefused("the reference was computed for another scope: " +
                               first_difference(scope, document));
    }
    if (scope.string_or("scope_sha256", "") != scope_fingerprint(scope)) {
        throw ReferenceRefused("the reference's scope does not match its own fingerprint");
    }
    ref.verified.push_back("the reference is bound to this audit's scope (scope_sha256 " +
                           scope.string_or("scope_sha256", "") +
                           "): job, run attempts, graph, "
                           "checkpoints, Kafka logs and "
                           "ranges, and output columns");
    if (!header.contains("implementation") || !header.at("implementation").is_object()) {
        throw ReferenceRefused("the reference names no implementation");
    }
    const auto& impl = header.at("implementation");
    const std::string impl_name = impl.string_or("name", "");
    const std::string impl_version = impl.string_or("version", "");
    if (impl_name.empty() || impl_version.empty()) {
        throw ReferenceRefused("the reference's implementation has no name or no version");
    }
    ref.implementation = impl_name + " " + impl_version;
    ref.recorded.push_back("implementation " + ref.implementation +
                           (impl.contains("config")
                                ? ", configuration " + impl.at("config").serialize()
                                : std::string{}));
    if (header.contains("reader") && header.at("reader").is_object()) {
        ref.recorded.push_back("source reader " + header.at("reader").serialize());
    }
    if (header.contains("created") && header.at("created").is_string()) {
        ref.recorded.push_back("written " + header.at("created").as_string());
    }
    if (windowed) {
        if (!header.contains("semantics") || !header.at("semantics").is_object()) {
            throw ReferenceRefused(
                "the reference does not say which window rules it implements (its header's "
                "semantics)");
        }
        ref.semantics = header.at("semantics");
    }
    // The window state an increment's reference started from: its own, at the
    // increment's base. Whether it is the state the last increment left is the
    // chain's to show, by its hash; this reader binds it to the base.
    const auto state_of = [&](const config::JsonValue& v,
                              const std::string& where,
                              std::uint64_t at) -> std::pair<std::uint64_t, std::string> {
        if (!v.is_object() || !v.contains("checkpoint") ||
            !v.at("checkpoint").is_integral_number() || v.string_or("sha256", "").size() != 64) {
            throw ReferenceRefused("the reference's " + where +
                                   " is not {checkpoint, sha256} of a window state");
        }
        const auto c = static_cast<std::uint64_t>(v.at("checkpoint").as_int());
        if (c != at) {
            throw ReferenceRefused("the reference's " + where + " is the state at checkpoint " +
                                   std::to_string(c) + ", and the scope needs checkpoint " +
                                   std::to_string(at));
        }
        return {c, v.string_or("sha256", "")};
    };
    if (increment) {
        if (!header.contains("state_in")) {
            throw ReferenceRefused(
                "the reference to an increment names no state it started from (its header's "
                "state_in)");
        }
        ref.state_in =
            state_of(header.at("state_in"),
                     "state_in",
                     static_cast<std::uint64_t>(document.at("base").at("checkpoint").as_int()));
        ref.verified.push_back(
            "it says it started from its own window state at the increment's "
            "base, checkpoint " +
            std::to_string(ref.state_in->first));
        ref.recorded.push_back("started from window state " + ref.state_in->second +
                               " at checkpoint " + std::to_string(ref.state_in->first));
    }
    // The identity columns a window line names, in the transform's order.
    std::vector<std::string> identity;
    if (windowed) {
        for (const auto& v : document.at("transform").at("identity").as_array()) {
            identity.push_back(v.as_string());
        }
    }

    std::map<std::string, std::size_t> index;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        index.emplace(columns[i].name, i);
    }
    Sha256 rows_hash;
    Sha256 windows_hash;
    std::optional<config::JsonValue> end;
    for (std::size_t li = 1; li < lines.size(); ++li) {
        const std::string_view line = lines[li];
        if (end) {
            throw ReferenceRefused("the reference has lines after its end line");
        }
        config::JsonValue v;
        try {
            v = config::parse(body(line));
        } catch (const std::exception& e) {
            throw ReferenceRefused("line " + std::to_string(li + 1) + " is not JSON: " + e.what());
        }
        if (v.is_object() && v.contains("end") && v.as_object().size() == 1) {
            end = v.at("end");
            continue;
        }
        if (windowed && v.is_object() && v.contains("window") && v.as_object().size() == 1 &&
            v.at("window").is_object()) {
            if (!line.ends_with('\n')) {
                throw ReferenceRefused("the reference's last window has no end line after it");
            }
            windows_hash.update(line.data(), line.size());
            ref.windows.push_back(window_line(v.at("window"), identity, columns, index, li + 1));
            continue;
        }
        if (!v.is_object() || !v.contains("row") || v.as_object().size() != 1 ||
            !v.at("row").is_object()) {
            throw ReferenceRefused("line " + std::to_string(li + 1) + " is neither a row" +
                                   (windowed ? std::string{", a window"} : std::string{}) +
                                   " nor the end");
        }
        if (!line.ends_with('\n')) {
            throw ReferenceRefused("the reference's last row has no end line after it");
        }
        rows_hash.update(line.data(), line.size());
        const auto& obj = v.at("row").as_object();
        if (obj.size() != columns.size()) {
            throw ReferenceRefused("row " + std::to_string(ref.rows.size() + 1) + " has " +
                                   std::to_string(obj.size()) + " cells and the scope " +
                                   std::to_string(columns.size()) + " columns");
        }
        Row row(columns.size());
        for (const auto& [name, cell] : obj) {
            const auto it = index.find(name);
            if (it == index.end()) {
                throw ReferenceRefused("row " + std::to_string(ref.rows.size() + 1) +
                                       " has a cell for " + name +
                                       ", which is not a column of the scope");
            }
            if (!cell.is_string() || !canonical_for(cell.as_string(), columns[it->second].type)) {
                throw ReferenceRefused("row " + std::to_string(ref.rows.size() + 1) + ": " + name +
                                       " holds " + cell.serialize().substr(0, 80) +
                                       ", which is not a canonical cell of " +
                                       columns[it->second].type.spelling);
            }
            row[it->second] = cell.as_string();
        }
        ref.rows.push_back(std::move(row));
    }
    if (!end) {
        throw ReferenceRefused(
            "the reference has no end line: its writer did not finish it, so its rows are not "
            "known to be whole");
    }
    if (!end->is_object()) {
        throw ReferenceRefused("the reference's end line is not an object");
    }
    ref.rows_sha256 = Sha256::to_hex(rows_hash.finalize());
    ref.header_sha256 = sha256_text(lines.front());
    if (!end->contains("rows") || !end->at("rows").is_integral_number() ||
        end->at("rows").as_int() != static_cast<std::int64_t>(ref.rows.size())) {
        throw ReferenceRefused("the reference's end line counts " +
                               (end->contains("rows") ? end->at("rows").serialize() : "no") +
                               " rows, and it holds " + std::to_string(ref.rows.size()));
    }
    if (end->string_or("rows_sha256", "") != ref.rows_sha256) {
        throw ReferenceRefused("the reference's rows do not hash to the end line's rows_sha256");
    }
    if (end->string_or("header_sha256", "") != ref.header_sha256) {
        throw ReferenceRefused(
            "the reference's header does not hash to the end line's "
            "header_sha256");
    }
    if (!end->contains("complete") || !end->at("complete").is_bool()) {
        throw ReferenceRefused("the reference's end line does not say whether it is complete");
    }
    ref.complete = end->at("complete").as_bool();
    ref.incomplete = strings_of(*end, "incomplete");
    if (!ref.complete && ref.incomplete.empty()) {
        throw ReferenceRefused("the reference says it is incomplete and does not say why");
    }
    if (ref.complete && !ref.incomplete.empty()) {
        throw ReferenceRefused("the reference says it is complete and also why it is not");
    }
    if (windowed) {
        ref.windows_sha256 = Sha256::to_hex(windows_hash.finalize());
        if (!end->contains("windows") || !end->at("windows").is_integral_number() ||
            end->at("windows").as_int() != static_cast<std::int64_t>(ref.windows.size())) {
            throw ReferenceRefused(
                "the reference's end line counts " +
                (end->contains("windows") ? end->at("windows").serialize() : std::string{"no"}) +
                " windows left out, and it holds " + std::to_string(ref.windows.size()));
        }
        if (end->string_or("windows_sha256", "") != ref.windows_sha256) {
            throw ReferenceRefused(
                "the reference's window lines do not hash to the end line's windows_sha256");
        }
        ref.source_bounds = source_bounds_of(*end, document);
        ref.records = end->contains("records") ? end->at("records") : config::JsonValue{};
        if (end->contains("state_out")) {
            ref.state_out =
                state_of(end->at("state_out"),
                         "state_out",
                         static_cast<std::uint64_t>(document.at("checkpoint").as_int()));
            ref.recorded.push_back("left window state " + ref.state_out->second +
                                   " at checkpoint " + std::to_string(ref.state_out->first));
        } else if (increment && ref.complete) {
            // An incomplete reference computed no windows, and leaves no state.
            throw ReferenceRefused(
                "the reference to an increment names no state it left (its end line's "
                "state_out)");
        }
    }
    ref.writer_verified = strings_of(*end, "verified");
    for (auto& r : strings_of(*end, "recorded")) {
        ref.recorded.push_back(std::move(r));
    }
    ref.verified.push_back("its " + std::to_string(ref.rows.size()) +
                           " row(s) and header hash to what its end line records (rows_sha256 " +
                           ref.rows_sha256 +
                           "), which shows the file is whole as its writer "
                           "finished it, not who wrote it");
    ref.verified.push_back("every cell is a canonical cell of its column's type");
    if (windowed) {
        ref.verified.push_back(
            "its " + std::to_string(ref.windows.size()) +
            " window(s) left out of the comparison hash to its end line's windows_sha256, and "
            "it gives the source's watermark bound at every checkpoint of the scope's lineage");
    }
    return ref;
}

Comparison compare_multiset(const std::vector<Row>& expected,
                            const std::vector<std::size_t>& key,
                            const std::vector<Row>& first,
                            const std::vector<Row>& second,
                            std::size_t list_limit) {
    Comparison c;
    std::map<std::string, std::vector<Row>> want;
    std::map<std::string, std::vector<Row>> one;
    std::map<std::string, std::vector<Row>> two;
    for (const auto& r : expected) {
        want[key_of(r, key)].push_back(r);
    }
    for (const auto& r : first) {
        one[key_of(r, key)].push_back(r);
    }
    for (const auto& r : second) {
        two[key_of(r, key)].push_back(r);
    }
    std::set<std::string> keys;
    for (const auto* m : {&want, &one, &two}) {
        for (const auto& [k, _] : *m) {
            keys.insert(k);
        }
    }
    const auto list = [&](Discrepancy d) {
        if (c.listed.size() < list_limit) {
            c.listed.push_back(std::move(d));
        }
    };
    const auto make = [](Discrepancy::Kind kind, Row k, Row e, Row o) {
        Discrepancy d;
        d.kind = kind;
        d.key = std::move(k);
        d.expected = std::move(e);
        d.observed = std::move(o);
        return d;
    };
    const std::vector<Row> none;
    for (const auto& k : keys) {
        std::vector<Row> e = want.contains(k) ? want[k] : none;
        std::vector<Row> o1 = one.contains(k) ? one[k] : none;
        std::vector<Row> o2 = two.contains(k) ? two[k] : none;
        std::sort(e.begin(), e.end());
        std::sort(o1.begin(), o1.end());
        std::sort(o2.begin(), o2.end());
        c.expected_rows += e.size();
        if (o1 != o2) {
            ++c.unstable_keys;
            continue;
        }
        c.observed_rows += o1.size();
        // Exact matches, one for one.
        std::vector<Row> left_e;
        std::vector<Row> left_o;
        std::size_t i = 0;
        std::size_t j = 0;
        while (i < e.size() || j < o1.size()) {
            if (j == o1.size() || (i < e.size() && e[i] < o1[j])) {
                left_e.push_back(e[i++]);
            } else if (i == e.size() || o1[j] < e[i]) {
                left_o.push_back(o1[j++]);
            } else {
                ++c.matched;
                ++i;
                ++j;
            }
        }
        // Further copies of an expected row are duplicates.
        std::vector<Row> rest;
        std::map<Row, std::uint64_t> extra;
        for (auto& r : left_o) {
            if (std::binary_search(e.begin(), e.end(), r)) {
                ++extra[r];
            } else {
                rest.push_back(std::move(r));
            }
        }
        for (auto& [r, n] : extra) {
            c.duplicated += n;
            Discrepancy d = make(Discrepancy::Kind::Duplicate, key_cells(r, key), r, r);
            d.copies = n;
            list(std::move(d));
        }
        // What is left under the key pairs as changed; the rest is missing or
        // unexpected.
        const std::size_t paired = std::min(left_e.size(), rest.size());
        for (std::size_t p = 0; p < paired; ++p) {
            ++c.mismatched;
            Discrepancy d =
                make(Discrepancy::Kind::Mismatch, key_cells(left_e[p], key), left_e[p], rest[p]);
            for (std::size_t col = 0; col < left_e[p].size(); ++col) {
                if (col < rest[p].size() && rest[p][col] != left_e[p][col]) {
                    d.differing.push_back(col);
                }
            }
            list(std::move(d));
        }
        for (std::size_t p = paired; p < left_e.size(); ++p) {
            ++c.missing;
            list(make(Discrepancy::Kind::Missing, key_cells(left_e[p], key), left_e[p], {}));
        }
        for (std::size_t p = paired; p < rest.size(); ++p) {
            ++c.unexpected;
            list(make(Discrepancy::Kind::Unexpected, key_cells(rest[p], key), {}, rest[p]));
        }
    }
    return c;
}

}  // namespace clink::incident::reconcile
