// The SQL conformance corpus (design record 011).
//
// tests/sql_conformance/cases/<name>/ holds a script, its inputs and the
// output the engine produced when the case was frozen. Each case runs
// through the embedded engine exactly as `clink run <file>.sql` would, and
// the rows its file sink wrote must equal expected.jsonl as a multiset of
// canonical JSON lines (object keys sorted, order of lines ignored: a
// parallel sink does not define row order, and neither does the promise).
// A changelog sink (changelog='true') is compared by its NET result: each
// row's inserts and deletes are netted, because the emission order of a
// retracting plan depends on how two sources interleave while the
// materialised result does not.
//
// The corpus is frozen the way tests/fixtures/ is: cases are ADDED, never
// edited to make a failing run pass. A case that has to change to stay
// green is a change in what a 1.0 script computes, which is a break of the
// Stable SQL tier and is reviewed as one. A bug fix that changes observed
// output supersedes a case (new case in, old case retired with the reason
// recorded in the commit) rather than editing it.
//
// Regeneration: CLINK_REGEN_SQL_CONFORMANCE=1 rewrites every expected.jsonl
// from the current engine. Legitimate only when adding cases; review the
// diff of an existing case as the contract change it is.
//
// Placeholders in script.sql: ${DIR} is the case directory (inputs),
// ${OUT} is the per-run output file the case's sink must write to.

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"

namespace {

namespace fs = std::filesystem;

fs::path corpus_root() {
    return fs::path{CLINK_SQL_CONFORMANCE_DIR} / "cases";
}

bool regen() {
    const char* env = std::getenv("CLINK_REGEN_SQL_CONFORMANCE");
    return env != nullptr && *env == '1';
}

std::vector<std::string> case_names() {
    std::vector<std::string> names;
    for (const auto& e : fs::directory_iterator(corpus_root())) {
        if (e.is_directory() && fs::exists(e.path() / "script.sql")) {
            names.push_back(e.path().filename().string());
        }
    }
    std::sort(names.begin(), names.end());
    return names;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::vector<std::string> read_lines(const fs::path& p) {
    std::vector<std::string> lines;
    std::ifstream in(p);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

void replace_all(std::string& s, const std::string& from, const std::string& to) {
    for (std::size_t pos = s.find(from); pos != std::string::npos;
         pos = s.find(from, pos + to.size())) {
        s.replace(pos, from.size(), to);
    }
}

// Canonical JSON: object keys sorted recursively, scalars in the engine's own
// serialisation. Two rows that differ only in key order compare equal.
std::string canonical(const clink::config::JsonValue& v) {
    if (v.is_object()) {
        std::vector<std::pair<std::string, const clink::config::JsonValue*>> entries;
        for (const auto& [k, val] : v.as_object()) {
            entries.emplace_back(k, &val);
        }
        std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        std::string out = "{";
        for (std::size_t i = 0; i < entries.size(); ++i) {
            if (i > 0) {
                out += ",";
            }
            out += clink::config::JsonValue{entries[i].first}.serialize(0);
            out += ":";
            out += canonical(*entries[i].second);
        }
        return out + "}";
    }
    if (v.is_array()) {
        std::string out = "[";
        bool first = true;
        for (const auto& e : v.as_array()) {
            if (!first) {
                out += ",";
            }
            first = false;
            out += canonical(e);
        }
        return out + "]";
    }
    return v.serialize(0);
}

// Canonical, sorted, and netted: a `__row_kind` marker counts +1 for an
// insert or update_after and -1 for a delete or update_before, and the
// marker itself is stripped so the result reads as the rows a consumer
// would hold after applying the changelog. Plain rows count +1 each.
std::vector<std::string> canonical_sorted(const std::vector<std::string>& lines) {
    std::map<std::string, long> net;
    for (const auto& l : lines) {
        auto value = clink::config::parse(l);
        long delta = 1;
        if (value.is_object() && value.as_object().contains("__row_kind")) {
            const std::string kind = value.as_object().at("__row_kind").as_string();
            delta = (kind == "insert" || kind == "update_after") ? 1 : -1;
            auto& obj = value.as_object();
            obj.erase(obj.find("__row_kind"));
        }
        net[canonical(value)] += delta;
    }
    std::vector<std::string> out;
    for (const auto& [row, count] : net) {
        if (count < 0) {
            ADD_FAILURE() << "changelog netted below zero for row " << row
                          << ": a delete without a matching insert";
        }
        for (long i = 0; i < count; ++i) {
            out.push_back(row);
        }
    }
    return out;
}

class SqlConformance : public ::testing::TestWithParam<std::string> {};

TEST_P(SqlConformance, ScriptProducesItsFrozenOutput) {
    const fs::path dir = corpus_root() / GetParam();
    const fs::path out_path = fs::temp_directory_path() / ("clink_sqlconf_" + GetParam() + "_" +
                                                           std::to_string(::getpid()) + ".ndjson");
    fs::remove(out_path);

    std::string script = read_file(dir / "script.sql");
    replace_all(script, "${DIR}", dir.string());
    replace_all(script, "${OUT}", out_path.string());

    clink::embed::EngineOptions opts;
    std::ostringstream diag;
    opts.out = &diag;
    opts.err = &diag;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script(script), 0) << diag.str();
    ASSERT_TRUE(engine.await_all()) << diag.str();

    const auto actual = canonical_sorted(read_lines(out_path));
    fs::remove(out_path);
    ASSERT_FALSE(actual.empty()) << "the case wrote no rows; a corpus case must observe something. "
                                 << diag.str();

    const fs::path expected_path = dir / "expected.jsonl";
    if (regen()) {
        std::ofstream out(expected_path, std::ios::trunc);
        for (const auto& l : actual) {
            out << l << "\n";
        }
        return;
    }
    const auto expected = canonical_sorted(read_lines(expected_path));
    EXPECT_EQ(actual, expected)
        << "the frozen output of '" << GetParam()
        << "' changed. This is a change in what a 1.0 "
           "script computes (design record 011): supersede the case rather than editing it, or fix "
           "the engine. Diagnostics: "
        << diag.str();
}

INSTANTIATE_TEST_SUITE_P(Corpus,
                         SqlConformance,
                         ::testing::ValuesIn(case_names()),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

}  // namespace
