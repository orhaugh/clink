#include "clink/incident/reference.hpp"

#include <algorithm>
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
    config::JsonValue doc{std::move(d)};
    const std::string fp = scope_fingerprint(doc);
    config::JsonObject withfp = doc.as_object();
    withfp["scope_sha256"] = config::JsonValue{fp};
    return config::JsonValue{std::move(withfp)};
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
    if (!header.contains("version") || !header.at("version").is_integral_number() ||
        header.at("version").as_int() != kReferenceVersion) {
        throw ReferenceRefused("the reference is not version " + std::to_string(kReferenceVersion) +
                               " of its format, which this release reads");
    }
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

    std::map<std::string, std::size_t> index;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        index.emplace(columns[i].name, i);
    }
    Sha256 rows_hash;
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
        if (!v.is_object() || !v.contains("row") || v.as_object().size() != 1 ||
            !v.at("row").is_object()) {
            throw ReferenceRefused("line " + std::to_string(li + 1) +
                                   " is neither a row nor the end");
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
