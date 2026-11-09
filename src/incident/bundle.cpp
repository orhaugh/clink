#include "clink/incident/bundle.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unistd.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/core/sha256.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::incident {

namespace fs = std::filesystem;

const char* to_string(Verdict v) noexcept {
    switch (v) {
        case Verdict::Passed:
            return "passed";
        case Verdict::Failed:
            return "failed";
        case Verdict::Unavailable:
            return "unavailable";
        case Verdict::Unsupported:
            return "unsupported";
    }
    return "?";
}

std::vector<std::string> required_checks(const std::string& profile, bool expected_output_given) {
    std::vector<std::string> out = {"bundle.manifest", "bundle.artifacts", "bundle.completeness"};
    if (profile == "integrity") {
        return out;
    }
    const std::vector<std::string> consistency = {
        "checkpoint.marker", "checkpoint.restore_rule", "checkpoint.participants", "state.decode"};
    out.insert(out.end(), consistency.begin(), consistency.end());
    if (profile == "consistency") {
        return out;
    }
    out.emplace_back("replay.determinism");
    out.emplace_back("replay.recorded_output");
    if (expected_output_given) {
        out.emplace_back("replay.expected_output");
    }
    if (profile == "replay") {
        return out;
    }
    if (profile == "full") {
        out.emplace_back("protocol.trace_structure");
        out.emplace_back("protocol.conformance");
        return out;
    }
    throw std::invalid_argument("unknown profile '" + profile +
                                "' (integrity, consistency, replay or full)");
}

// ---- the report ---------------------------------------------------------------

namespace {

const Check* find_check(const std::vector<Check>& checks, const std::string& id) {
    for (const auto& c : checks) {
        if (c.id == id) {
            return &c;
        }
    }
    return nullptr;
}

const char* result_word(int code) {
    switch (code) {
        case 0:
            return "satisfied";
        case 1:
            return "failed";
        default:
            return "partial";
    }
}

std::string json_string(const std::string& s) {
    return config::JsonValue{s}.serialize(0);
}

std::string json_strings(const std::vector<std::string>& xs) {
    std::string out = "[";
    for (std::size_t i = 0; i < xs.size(); ++i) {
        out += (i == 0 ? "" : ",") + json_string(xs[i]);
    }
    return out + "]";
}

}  // namespace

int Report::exit_code() const {
    for (const auto& c : checks) {
        if (c.verdict == Verdict::Failed) {
            return 1;
        }
    }
    for (const auto& id : required) {
        const auto* c = find_check(checks, id);
        if (c == nullptr || c->verdict != Verdict::Passed) {
            return 3;
        }
    }
    return 0;
}

std::string Report::render_text() const {
    std::ostringstream out;
    out << "bundle:  " << bundle << "\n"
        << "profile: " << profile << " (requires " << required.size() << " check(s))\n\n";
    for (const auto& c : checks) {
        const bool req = std::find(required.begin(), required.end(), c.id) != required.end();
        out << "[" << to_string(c.verdict) << "] " << c.id << (req ? "" : " (not required)")
            << "\n    " << c.reason << "\n";
        for (const auto& d : c.details) {
            out << "    - " << d << "\n";
        }
        if (!c.evidence.empty()) {
            out << "    evidence: ";
            for (std::size_t i = 0; i < c.evidence.size() && i < 8; ++i) {
                out << (i == 0 ? "" : ", ") << c.evidence[i];
            }
            if (c.evidence.size() > 8) {
                out << ", and " << (c.evidence.size() - 8) << " more";
            }
            out << "\n";
        }
        if (!c.next_action.empty()) {
            out << "    next: " << c.next_action << "\n";
        }
    }
    for (const auto& id : required) {
        if (find_check(checks, id) == nullptr) {
            out << "[not run] " << id << "\n    required by the profile, and nothing ran it\n";
        }
    }
    const int code = exit_code();
    out << "\nresult: " << result_word(code) << " (exit " << code << ")";
    if (code == 3) {
        out << ": nothing failed, but the profile is not fully satisfied";
    }
    out << "\n";
    return out.str();
}

std::string Report::render_json() const {
    const int code = exit_code();
    std::string out =
        "{\"bundle\":" + json_string(bundle) + ",\"profile\":" + json_string(profile) +
        ",\"result\":" + json_string(result_word(code)) + ",\"exit_code\":" + std::to_string(code) +
        ",\"required\":" + json_strings(required) + ",\"checks\":[";
    for (std::size_t i = 0; i < checks.size(); ++i) {
        const auto& c = checks[i];
        const bool req = std::find(required.begin(), required.end(), c.id) != required.end();
        out += (i == 0 ? "" : ",");
        out +=
            "{\"id\":" + json_string(c.id) + ",\"verdict\":" + json_string(to_string(c.verdict)) +
            ",\"required\":" + (req ? "true" : "false") + ",\"reason\":" + json_string(c.reason) +
            ",\"evidence\":" + json_strings(c.evidence) +
            ",\"next_action\":" + json_string(c.next_action) +
            ",\"details\":" + json_strings(c.details) + "}";
    }
    out += "],\"not_run\":[";
    bool first = true;
    for (const auto& id : required) {
        if (find_check(checks, id) == nullptr) {
            out += (first ? "" : ",") + json_string(id);
            first = false;
        }
    }
    return out + "]}";
}

// ---- reading a bundle ---------------------------------------------------------

bool safe_relative_path(const std::string& path) noexcept {
    if (path.empty() || path.front() == '/' || path.find('\\') != std::string::npos ||
        path.find('\0') != std::string::npos) {
        return false;
    }
    std::size_t at = 0;
    while (at <= path.size()) {
        auto end = path.find('/', at);
        if (end == std::string::npos) {
            end = path.size();
        }
        const auto part = std::string_view{path}.substr(at, end - at);
        if (part.empty() || part == "." || part == "..") {
            return false;
        }
        at = end + 1;
    }
    return true;
}

namespace {

// The deepest nesting of arrays and objects in `text`, outside strings, so
// a manifest that would recurse the parser too deeply is refused first.
std::size_t json_depth(std::string_view text) {
    std::size_t depth = 0;
    std::size_t deepest = 0;
    bool in_string = false;
    bool escaped = false;
    for (const char c : text) {
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        if (c == '"') {
            in_string = true;
        } else if (c == '{' || c == '[') {
            deepest = std::max(deepest, ++depth);
        } else if ((c == '}' || c == ']') && depth > 0) {
            --depth;
        }
    }
    return deepest;
}

bool is_hex64(const std::string& s) {
    return s.size() == 64 && std::all_of(s.begin(), s.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

std::string read_small_file(const fs::path& path, std::uint64_t limit) {
    std::error_code ec;
    const auto size = fs::file_size(path, ec);
    if (ec) {
        throw BundleRefused("cannot read " + path.filename().string() + ": " + ec.message());
    }
    if (size > limit) {
        throw BundleRefused(path.filename().string() + " is " + std::to_string(size) +
                            " bytes, past the reader's bound of " + std::to_string(limit));
    }
    std::ifstream in(path, std::ios::binary);
    std::string body(static_cast<std::size_t>(size), '\0');
    in.read(body.data(), static_cast<std::streamsize>(size));
    if (!in) {
        throw BundleRefused("cannot read " + path.filename().string());
    }
    return body;
}

std::uint64_t uint_at(const config::JsonValue& obj, std::string_view key) {
    const auto& v = obj.at(key);
    if (!v.is_integral_number() || v.as_int() < 0) {
        throw BundleRefused("manifest field '" + std::string{key} +
                            "' is not a non-negative integer");
    }
    return static_cast<std::uint64_t>(v.as_int());
}

}  // namespace

JobId Bundle::job() const {
    return uint_at(manifest.at("job"), "id");
}

std::uint64_t Bundle::checkpoint() const {
    return uint_at(manifest.at("selection"), "checkpoint");
}

bool Bundle::contains(const std::string& role) const {
    const auto& contents = manifest.at("contents");
    return contents.contains(role) && contents.at(role).is_bool() && contents.at(role).as_bool();
}

const Artifact* Bundle::find(const std::string& path) const {
    for (const auto& a : artifacts) {
        if (a.path == path) {
            return &a;
        }
    }
    return nullptr;
}

Bundle read_bundle(const fs::path& root_in, const ReadLimits& limits) {
    std::error_code ec;
    const auto root = fs::absolute(root_in, ec);
    if (ec || !fs::is_directory(fs::symlink_status(root, ec))) {
        throw BundleRefused(root_in.string() + " is not a directory");
    }
    const auto manifest_path = root / "manifest.json";
    const auto st = fs::symlink_status(manifest_path, ec);
    if (ec || !fs::is_regular_file(st)) {
        throw BundleRefused("no manifest.json as a regular file in " + root_in.string());
    }
    const auto text = read_small_file(manifest_path, limits.max_manifest_bytes);
    if (json_depth(text) > limits.max_json_depth) {
        throw BundleRefused("manifest.json nests deeper than " +
                            std::to_string(limits.max_json_depth) + " levels");
    }
    Bundle b;
    b.root = root;
    try {
        b.manifest = config::parse(text);
    } catch (const std::exception& e) {
        throw BundleRefused(std::string{"manifest.json does not parse: "} + e.what());
    }
    try {
        if (!b.manifest.is_object()) {
            throw BundleRefused("manifest.json is not a JSON object");
        }
        if (!b.manifest.contains("format") || !b.manifest.at("format").is_string() ||
            b.manifest.at("format").as_string() != kFormat) {
            throw BundleRefused(std::string{"manifest.json is not a "} + kFormat + " manifest");
        }
        const auto version = uint_at(b.manifest, "version");
        if (version == 0 || version > static_cast<std::uint64_t>(kVersion)) {
            throw BundleRefused("bundle format version " + std::to_string(version) +
                                " is not one this reader knows (it reads up to " +
                                std::to_string(kVersion) + ")");
        }
        (void)b.job();
        (void)b.checkpoint();
        if (!b.manifest.at("contents").is_object()) {
            throw BundleRefused("manifest field 'contents' is not an object");
        }
        const auto& arts = b.manifest.at("artifacts");
        if (!arts.is_array()) {
            throw BundleRefused("manifest field 'artifacts' is not an array");
        }
        if (arts.as_array().size() > limits.max_files) {
            throw BundleRefused("the manifest lists " + std::to_string(arts.as_array().size()) +
                                " artifacts, past the reader's bound of " +
                                std::to_string(limits.max_files));
        }
        std::set<std::string> seen;
        std::uint64_t total = 0;
        for (const auto& a : arts.as_array()) {
            Artifact art;
            if (!a.is_object() || !a.contains("path") || !a.at("path").is_string() ||
                !a.contains("role") || !a.at("role").is_string() || !a.contains("sha256") ||
                !a.at("sha256").is_string()) {
                throw BundleRefused("an artifact entry lacks a string path, role or sha256");
            }
            art.path = a.at("path").as_string();
            art.role = a.at("role").as_string();
            art.sha256 = a.at("sha256").as_string();
            art.bytes = uint_at(a, "bytes");
            if (!safe_relative_path(art.path) || art.path == "manifest.json") {
                throw BundleRefused("artifact path '" + art.path +
                                    "' is not a plain relative path inside the bundle");
            }
            if (!is_hex64(art.sha256)) {
                throw BundleRefused("artifact '" + art.path + "' has no SHA-256 in lowercase hex");
            }
            if (art.bytes > limits.max_file_bytes) {
                throw BundleRefused("artifact '" + art.path + "' claims " +
                                    std::to_string(art.bytes) + " bytes, past the bound of " +
                                    std::to_string(limits.max_file_bytes));
            }
            total += art.bytes;
            if (total > limits.max_total_bytes) {
                throw BundleRefused("the artifacts claim more than " +
                                    std::to_string(limits.max_total_bytes) + " bytes in all");
            }
            if (!seen.insert(art.path).second) {
                throw BundleRefused("artifact '" + art.path + "' is listed twice");
            }
            b.artifacts.push_back(std::move(art));
        }
    } catch (const BundleRefused&) {
        throw;
    } catch (const std::exception& e) {
        throw BundleRefused(std::string{"manifest.json is malformed: "} + e.what());
    }
    return b;
}

std::string sha256_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot read " + path.string());
    }
    Sha256 hash;
    std::vector<char> buf(1 << 20);
    while (in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        const auto got = in.gcount();
        if (got > 0) {
            hash.update(buf.data(), static_cast<std::size_t>(got));
        }
    }
    return Sha256::to_hex(hash.finalize());
}

bool looks_like_credential(const std::string& name) noexcept {
    std::string lower = name;
    for (auto& c : lower) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    static constexpr std::array<std::string_view, 12> kWords = {"password",
                                                                "passwd",
                                                                "secret",
                                                                "token",
                                                                "credential",
                                                                "api_key",
                                                                "apikey",
                                                                "access_key",
                                                                "private_key",
                                                                "sasl",
                                                                "auth",
                                                                "conninfo"};
    return std::any_of(kWords.begin(), kWords.end(), [&](std::string_view w) {
        return lower.find(w) != std::string::npos;
    });
}

std::uint64_t epoch_starting_checkpoint(std::uint64_t epoch,
                                        std::optional<std::uint64_t> restored_from,
                                        std::optional<std::uint64_t> restored_next) noexcept {
    if (restored_from.has_value() && restored_next.has_value() && epoch == *restored_next) {
        return *restored_from;
    }
    return epoch > 0 ? epoch - 1 : 0;
}

Check refused_check(const std::string& reason) {
    return Check{.id = "bundle.manifest",
                 .verdict = Verdict::Failed,
                 .evidence = {"manifest.json"},
                 .reason = reason,
                 .next_action =
                     "collect the bundle again with clink incident-collect, or check "
                     "where this copy came from",
                 .details = {}};
}

// ---- the checks ---------------------------------------------------------------

namespace {

Check bundle_manifest_check(const Bundle& b) {
    Check c{.id = "bundle.manifest",
            .verdict = Verdict::Passed,
            .evidence = {"manifest.json"},
            .reason =
                "the manifest parses, is a known format and version, and every path in it "
                "is relative and inside the bundle",
            .next_action = {},
            .details = {}};
    c.details.push_back("format " + std::string{kFormat} + " version " +
                        std::to_string(uint_at(b.manifest, "version")) + ", job " +
                        std::to_string(b.job()) + ", checkpoint " + std::to_string(b.checkpoint()));
    if (b.manifest.contains("collector") && b.manifest.at("collector").is_object()) {
        const auto& col = b.manifest.at("collector");
        c.details.push_back("collected " + col.string_or("collected_at", "?") + " by clink " +
                            col.string_or("clink_version", "?") + " at " +
                            col.string_or("git_sha", "?"));
    }
    return c;
}

Check bundle_artifacts_check(const Bundle& b) {
    Check c{.id = "bundle.artifacts",
            .verdict = Verdict::Passed,
            .evidence = {"manifest.json"},
            .reason = {},
            .next_action = {},
            .details = {}};
    std::error_code ec;
    const auto canon_root = fs::weakly_canonical(b.root, ec);
    std::vector<std::string> problems;
    std::set<std::string> listed;
    for (const auto& a : b.artifacts) {
        listed.insert(a.path);
        const auto full = b.root / a.path;
        // Every component under the root must be a plain directory or, for the
        // last, a regular file: a symbolic link anywhere could point outside.
        fs::path walk = b.root;
        bool link = false;
        for (const auto& part : fs::path{a.path}) {
            walk /= part;
            if (fs::is_symlink(fs::symlink_status(walk, ec))) {
                link = true;
                break;
            }
        }
        if (link) {
            problems.push_back(a.path + ": a symbolic link is on its path");
            continue;
        }
        const auto st = fs::symlink_status(full, ec);
        if (ec || !fs::exists(st)) {
            problems.push_back(a.path + ": listed but absent");
            continue;
        }
        if (!fs::is_regular_file(st)) {
            problems.push_back(a.path + ": not a regular file");
            continue;
        }
        const auto canon = fs::weakly_canonical(full, ec);
        if (ec || canon.string().rfind(canon_root.string() + "/", 0) != 0) {
            problems.push_back(a.path + ": resolves outside the bundle");
            continue;
        }
        const auto size = fs::file_size(full, ec);
        if (ec || size != a.bytes) {
            problems.push_back(a.path + ": " + std::to_string(ec ? 0 : size) +
                               " bytes, listed as " + std::to_string(a.bytes));
            continue;
        }
        if (sha256_file(full) != a.sha256) {
            problems.push_back(a.path + ": SHA-256 differs from the manifest's");
        }
    }
    // Nothing unlisted: an evidence file the manifest does not vouch for is
    // not evidence, and a link could stand in for one.
    for (auto it = fs::recursive_directory_iterator(b.root, fs::directory_options::none, ec);
         !ec && it != fs::recursive_directory_iterator();
         it.increment(ec)) {
        const auto rel = fs::relative(it->path(), b.root, ec).generic_string();
        const auto st = it->symlink_status(ec);
        if (fs::is_symlink(st)) {
            problems.push_back(rel + ": a symbolic link in the bundle");
            it.disable_recursion_pending();
            continue;
        }
        if (fs::is_directory(st)) {
            continue;
        }
        if (rel == "manifest.json") {
            continue;
        }
        if (!fs::is_regular_file(st)) {
            problems.push_back(rel + ": not a regular file");
        } else if (listed.count(rel) == 0) {
            problems.push_back(rel + ": present but not in the manifest");
        }
    }
    if (problems.empty()) {
        c.reason = "every one of the " + std::to_string(b.artifacts.size()) +
                   " listed artifacts exists with its listed size and SHA-256, and nothing "
                   "unlisted is present. This shows the bundle is as collected, not who "
                   "collected it.";
        return c;
    }
    c.verdict = Verdict::Failed;
    c.reason = std::to_string(problems.size()) +
               " artifact problem(s): the bundle is not as its manifest describes";
    for (std::size_t i = 0; i < problems.size() && i < 50; ++i) {
        c.details.push_back(problems[i]);
    }
    if (problems.size() > 50) {
        c.details.push_back("and " + std::to_string(problems.size() - 50) + " more");
    }
    c.next_action =
        "collect the bundle again from the job's directories; a bundle that changed "
        "after collection cannot be repaired";
    return c;
}

std::string marker_path(const Bundle& b, std::uint64_t ckpt, const char* prefix) {
    return "checkpoint/_jobs/" + std::to_string(b.job()) + "/" + prefix + std::to_string(ckpt);
}

std::vector<std::uint32_t> selection_participants(const Bundle& b) {
    std::vector<std::uint32_t> out;
    const auto& sel = b.manifest.at("selection");
    if (sel.contains("participants") && sel.at("participants").is_array()) {
        for (const auto& p : sel.at("participants").as_array()) {
            if (p.is_integral_number() && p.as_int() >= 0) {
                out.push_back(static_cast<std::uint32_t>(p.as_int()));
            }
        }
    }
    return out;
}

Check bundle_completeness_check(const Bundle& b) {
    Check c{.id = "bundle.completeness",
            .verdict = Verdict::Passed,
            .evidence = {"manifest.json"},
            .reason = {},
            .next_action = {},
            .details = {}};
    std::vector<std::string> missing;
    const auto completed = marker_path(b, b.checkpoint(), "COMPLETED-");
    if (b.find(completed) == nullptr) {
        missing.push_back(completed + " (the selected checkpoint's marker)");
    }
    // What the manifest says it holds must be there: a manifest that claims
    // the state or the captures and lacks them is not a partial bundle, it is
    // a wrong one.
    const auto& sel = b.manifest.at("selection");
    const auto state_rel = sel.string_or("state_root", "");
    const auto generation = sel.int_or("generation", 0);
    for (const auto sub : selection_participants(b)) {
        if (state_rel.empty() || generation <= 0) {
            break;
        }
        const auto dir = state_dir_for(state_rel, static_cast<std::uint32_t>(generation), sub);
        const auto snap = dir + "/checkpoint-" + std::to_string(b.checkpoint()) + ".snap";
        if (b.find(snap + ".meta") == nullptr) {
            missing.push_back(snap + ".meta (a participant's integrity sidecar)");
        }
        if (b.contains("state") && b.find(snap) == nullptr) {
            missing.push_back(snap + " (a participant's snapshot; the manifest claims the state)");
        }
    }
    if (b.contains("captures")) {
        bool any = false;
        for (const auto& a : b.artifacts) {
            any = any || a.role == "capture_epoch";
        }
        if (!any) {
            missing.push_back("capture epochs (the manifest claims the captures)");
        }
    }
    if (b.contains("traces")) {
        bool any = false;
        for (const auto& a : b.artifacts) {
            any = any || a.role == "trace";
        }
        if (!any) {
            missing.push_back("protocol traces (the manifest claims the traces)");
        }
    }
    if (b.manifest.contains("absent") && b.manifest.at("absent").is_array()) {
        for (const auto& a : b.manifest.at("absent").as_array()) {
            if (a.is_object()) {
                c.details.push_back("absent at collection: " + a.string_or("path", "?") + " - " +
                                    a.string_or("reason", "?"));
            }
        }
    }
    if (b.manifest.contains("excluded") && b.manifest.at("excluded").is_array()) {
        for (const auto& e : b.manifest.at("excluded").as_array()) {
            if (e.is_string()) {
                c.details.push_back("excluded at collection: " + e.as_string());
            }
        }
    }
    for (const char* what : {"state", "captures", "traces", "job_manifest"}) {
        c.details.push_back(std::string{what} + ": " +
                            (b.contains(what) ? "collected" : "not collected"));
    }
    if (missing.empty()) {
        c.reason =
            "every artifact the manifest's selection and contents promise is listed; "
            "evidence that was absent or excluded at collection is named below";
        return c;
    }
    c.verdict = Verdict::Failed;
    c.reason = std::to_string(missing.size()) +
               " piece(s) of evidence the manifest promises are not in it";
    for (const auto& m : missing) {
        c.details.insert(c.details.begin(), "missing: " + m);
    }
    c.next_action =
        "collect the bundle again; a manifest that promises evidence it lacks was "
        "not written by a completed collection";
    return c;
}

std::string read_text(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

Check checkpoint_marker_check(const Bundle& b) {
    const auto rel = marker_path(b, b.checkpoint(), "COMPLETED-");
    Check c{.id = "checkpoint.marker",
            .verdict = Verdict::Passed,
            .evidence = {rel},
            .reason = {},
            .next_action = {},
            .details = {}};
    if (b.find(rel) == nullptr) {
        c.verdict = Verdict::Failed;
        c.reason = "the bundle selects checkpoint " + std::to_string(b.checkpoint()) +
                   " but holds no COMPLETED marker for it, and only a completed checkpoint can be "
                   "restored";
        c.next_action =
            "collect again; if the job has no completed checkpoint there is nothing "
            "to restore from";
        return c;
    }
    const auto m = cluster::parse_completed_marker(read_text(b.root / rel));
    std::vector<std::string> wrong;
    if (!m.job.has_value() || *m.job != b.job()) {
        wrong.push_back("it names job " + (m.job ? std::to_string(*m.job) : std::string{"none"}) +
                        ", the bundle job " + std::to_string(b.job()));
    }
    if (!m.checkpoint.has_value() || *m.checkpoint != b.checkpoint()) {
        wrong.push_back("it names checkpoint " +
                        (m.checkpoint ? std::to_string(*m.checkpoint) : std::string{"none"}));
    }
    if (!m.generation.has_value()) {
        wrong.push_back("it records no generation");
    }
    const auto& sel = b.manifest.at("selection");
    if (m.generation.has_value() &&
        static_cast<std::int64_t>(*m.generation) != sel.int_or("generation", -1)) {
        wrong.push_back("its generation " + std::to_string(*m.generation) +
                        " is not the manifest's " + std::to_string(sel.int_or("generation", -1)));
    }
    if (!wrong.empty()) {
        c.verdict = Verdict::Failed;
        c.reason = "the selected checkpoint's COMPLETED marker does not describe it";
        c.details = wrong;
        c.next_action =
            "collect again; a marker naming another job, checkpoint or generation "
            "means the evidence was mixed";
        return c;
    }
    c.reason = "COMPLETED-" + std::to_string(b.checkpoint()) +
               " parses and names this job, this checkpoint and generation " +
               std::to_string(*m.generation);
    if (m.subtasks.has_value()) {
        std::string list;
        for (const auto s : *m.subtasks) {
            list += (list.empty() ? "" : ",") + std::to_string(s);
        }
        c.details.push_back("participants: " + list);
    } else {
        c.details.push_back("the marker records no participant set (an older marker)");
    }
    if (m.state.has_value()) {
        c.details.push_back("state root: " + *m.state);
    }
    return c;
}

Check checkpoint_restore_rule_check(const Bundle& b) {
    Check c{.id = "checkpoint.restore_rule",
            .verdict = Verdict::Passed,
            .evidence = {"checkpoint/_jobs/" + std::to_string(b.job()) + "/"},
            .reason = {},
            .next_action = {},
            .details = {}};
    const auto& job = b.manifest.at("job");
    if (!job.contains("commit_confirmation") || !job.at("commit_confirmation").is_bool()) {
        c.verdict = Verdict::Unavailable;
        c.reason =
            "the bundle does not record whether the job's sinks need commit confirmation, "
            "so which restore rule applies is unknown";
        c.next_action =
            "collect with --ha-dir (the job manifest records it) or "
            "--commit-confirmation=yes|no";
        return c;
    }
    const bool confirm = job.at("commit_confirmation").as_bool();
    cluster::RestorePointOnDisk point;
    try {
        point = cluster::restore_point_on_disk(b.checkpoint_dir().string(), b.job(), confirm);
    } catch (const std::exception& e) {
        c.verdict = Verdict::Failed;
        c.reason =
            std::string{
                "the engine's restore rule cannot be evaluated over the bundle's "
                "markers: "} +
            e.what();
        c.next_action = "collect again; a RESTORED record that does not parse fails a restore too";
        return c;
    }
    const auto& sel = b.manifest.at("selection");
    const auto recorded = uint_at(sel, "engine_choice");
    c.details.push_back(std::string{"rule: "} +
                        (confirm ? "the newest CONFIRMED checkpoint (the job's sinks need commit "
                                   "confirmation)"
                                 : "the newest COMPLETED checkpoint the RESTORED record does not "
                                   "pass over"));
    c.details.push_back(
        "newest COMPLETED a restore may choose: " + std::to_string(point.completed) +
        ", newest CONFIRMED: " + std::to_string(point.confirmed));
    if (point.restored.has_value()) {
        c.details.push_back("RESTORED record: restored from " +
                            std::to_string(point.restored->restore) + ", numbering from " +
                            std::to_string(point.restored->next));
    }
    if (confirm) {
        c.details.push_back(
            "a takeover's in-doubt walk asks the sinks' systems and can advance "
            "this point; nothing read offline can predict it");
    }
    if (point.chosen != recorded) {
        c.verdict = Verdict::Failed;
        c.reason = "over the bundle's markers the engine's rule chooses checkpoint " +
                   std::to_string(point.chosen) + ", but the bundle records " +
                   std::to_string(recorded);
        c.next_action = "collect again: the markers and the selection were not taken together";
        return c;
    }
    const bool overridden = sel.bool_or("overridden", false);
    if (overridden && b.checkpoint() != point.chosen) {
        c.details.push_back("the collection was asked for checkpoint " +
                            std::to_string(b.checkpoint()) +
                            "; the checks of the checkpoint below are about that one, not the "
                            "one a restore would use");
    }
    c.reason = "over the bundle's markers the engine's rule chooses checkpoint " +
               std::to_string(point.chosen) + ", as the bundle records";
    return c;
}

Check checkpoint_participants_check(const Bundle& b) {
    Check c{.id = "checkpoint.participants",
            .verdict = Verdict::Passed,
            .evidence = {},
            .reason = {},
            .next_action = {},
            .details = {}};
    cluster::RestoreSource src;
    try {
        src = cluster::resolve_restore_source(
            cluster::completed_marker_dir_for(b.checkpoint_dir().string(), b.job()).string(),
            b.checkpoint());
    } catch (const std::exception& e) {
        c.verdict = Verdict::Failed;
        c.reason =
            std::string{"the restore source cannot be resolved over the bundle: "} + e.what();
        c.next_action = "collect again";
        return c;
    }
    if (src.legacy || !src.participants.has_value()) {
        c.verdict = Verdict::Unsupported;
        c.reason =
            "the selected checkpoint's marker predates the job-namespaced state layout or "
            "the participant set, which this release does not check";
        c.next_action = "verify the state with clink checkpoint-verify on the job's own directory";
        return c;
    }
    std::error_code ec;
    const auto canon_root = fs::weakly_canonical(b.root, ec).string() + "/";
    const auto canon_state = fs::weakly_canonical(src.state_root, ec).string();
    if (canon_state.rfind(canon_root, 0) != 0) {
        c.verdict = Verdict::Unsupported;
        c.reason = "the checkpoint's state was written to " + src.state_root +
                   ", outside the checkpoint directory, which a bundle does not carry in this "
                   "release";
        c.next_action = "check the state with clink checkpoint-verify where it lives";
        return c;
    }
    const auto name = "checkpoint-" + std::to_string(b.checkpoint()) + ".snap";
    std::vector<std::string> problems;
    std::size_t verified = 0;
    for (const auto sub : *src.participants) {
        const fs::path dir = state_dir_for(src.state_root, src.generation, sub);
        const auto snap = dir / name;
        const auto rel = fs::relative(snap, b.root, ec).generic_string();
        c.evidence.push_back(rel + ".meta");
        if (b.find(rel + ".meta") == nullptr) {
            problems.push_back("participant " + std::to_string(sub) + ": no " + rel + ".meta");
            continue;
        }
        if (!b.contains("state")) {
            continue;
        }
        if (b.find(rel) == nullptr) {
            problems.push_back("participant " + std::to_string(sub) + ": no " + rel);
            continue;
        }
        const auto verdict = state::verify_checkpoint(snap);
        if (!verdict.ok()) {
            problems.push_back("participant " + std::to_string(sub) + ": " + rel + " is " +
                               std::string{state::to_string(verdict.status)} + ": " +
                               verdict.detail);
            continue;
        }
        ++verified;
    }
    c.details.push_back("generation " + std::to_string(src.generation) + ", " +
                        std::to_string(src.participants->size()) + " participant(s)");
    if (!problems.empty()) {
        c.verdict = Verdict::Failed;
        c.reason =
            "the marker names participants whose state the bundle does not hold intact; "
            "a restore refuses a participant whose snapshot is absent or does not verify";
        c.details.insert(c.details.end(), problems.begin(), problems.end());
        c.next_action =
            "check the job's checkpoint directory with clink checkpoint-verify; "
            "a restore from this checkpoint would fail the same way";
        return c;
    }
    if (b.contains("state")) {
        c.reason = "every participant the marker records has its snapshot and sidecar, and all " +
                   std::to_string(verified) + " snapshots verify against their sidecars";
    } else {
        c.reason =
            "every participant the marker records has its integrity sidecar; the "
            "snapshots were not collected, so their bytes were not checked";
    }
    return c;
}

Check state_decode_check(const Bundle& b) {
    Check c{.id = "state.decode",
            .verdict = Verdict::Passed,
            .evidence = {},
            .reason = {},
            .next_action = {},
            .details = {}};
    if (!b.contains("state")) {
        c.verdict = Verdict::Unavailable;
        c.reason = "the snapshots were not collected";
        c.next_action = "collect with --include-state (the snapshots hold the job's keyed state)";
        return c;
    }
    std::vector<std::string> problems;
    std::size_t decoded = 0;
    for (const auto& a : b.artifacts) {
        if (a.role != "snapshot" && a.role != "replay_state") {
            continue;
        }
        c.evidence.push_back(a.path);
        try {
            std::ifstream in(b.root / a.path, std::ios::binary);
            std::vector<std::byte> bytes(static_cast<std::size_t>(a.bytes));
            in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(a.bytes));
            if (!in) {
                throw std::runtime_error("short read");
            }
            InMemoryStateBackend backend;
            backend.restore(Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = std::move(bytes)});
            const auto versions = backend.restored_state_versions();
            const auto fps = backend.restored_state_fingerprints();
            c.details.push_back(a.path + ": " + std::to_string(versions.size()) +
                                " versioned operator(s), " + std::to_string(fps.entries().size()) +
                                " fingerprinted slot(s)");
            ++decoded;
        } catch (const std::exception& e) {
            problems.push_back(a.path + ": " + e.what());
        }
    }
    if (decoded == 0 && problems.empty()) {
        c.verdict = Verdict::Unsupported;
        c.reason =
            "the bundle claims the state but holds no snapshot this release decodes (an "
            "SST checkpoint, for one)";
        c.next_action = "inspect the state with clink state-cat where it lives";
        return c;
    }
    if (!problems.empty()) {
        c.verdict = Verdict::Failed;
        c.reason = std::to_string(problems.size()) +
                   " snapshot(s) do not restore through the production restore path";
        c.details.insert(c.details.begin(), problems.begin(), problems.end());
        c.next_action =
            "a restore of these snapshots would fail too; check the job's checkpoint "
            "directory with clink checkpoint-verify";
        return c;
    }
    c.reason = "all " + std::to_string(decoded) +
               " snapshot(s) restore through the production restore path; their schema stamps "
               "are listed. Compatibility with another build is not established here.";
    return c;
}

Check trace_structure_check(const Bundle& b) {
    Check c{.id = "protocol.trace_structure",
            .verdict = Verdict::Passed,
            .evidence = {},
            .reason = {},
            .next_action = {},
            .details = {}};
    if (!b.contains("traces")) {
        c.verdict = Verdict::Unavailable;
        c.reason = "no protocol traces were collected";
        c.next_action =
            "run the job with CLINK_PROTOCOL_TRACE_DIR set and collect with "
            "--trace-dir";
        return c;
    }
    std::vector<std::string> problems;
    std::size_t files = 0;
    std::size_t events = 0;
    for (const auto& a : b.artifacts) {
        if (a.role != "trace") {
            continue;
        }
        ++files;
        c.evidence.push_back(a.path);
        const auto body = read_text(b.root / a.path);
        if (!body.empty() && body.back() != '\n') {
            problems.push_back(a.path + ": ends part-way through a line");
        }
        std::uint64_t expect = 1;
        std::string proc;
        std::size_t at = 0;
        std::size_t line_no = 0;
        while (at < body.size()) {
            auto end = body.find('\n', at);
            if (end == std::string::npos) {
                end = body.size();
            }
            const auto line = std::string_view{body}.substr(at, end - at);
            at = end + 1;
            ++line_no;
            if (line.empty()) {
                continue;
            }
            try {
                const auto ev = config::parse(line);
                const auto seq = uint_at(ev, "seq");
                const auto p = ev.at("proc").as_string();
                (void)ev.at("event").as_string();
                if (proc.empty()) {
                    proc = p;
                } else if (p != proc) {
                    problems.push_back(a.path + ":" + std::to_string(line_no) +
                                       ": another process's event in this file");
                    break;
                }
                if (seq != expect) {
                    problems.push_back(a.path + ":" + std::to_string(line_no) + ": seq " +
                                       std::to_string(seq) + " where " + std::to_string(expect) +
                                       " belongs; events are missing or out of order");
                    break;
                }
                ++expect;
                ++events;
            } catch (const std::exception& e) {
                problems.push_back(a.path + ":" + std::to_string(line_no) +
                                   ": not a trace event: " + e.what());
                break;
            }
        }
    }
    if (files == 0) {
        c.verdict = Verdict::Failed;
        c.reason = "the manifest claims protocol traces but lists none";
        c.next_action = "collect again";
        return c;
    }
    if (!problems.empty()) {
        c.verdict = Verdict::Failed;
        c.reason = "the traces are not complete runs of their processes";
        c.details = problems;
        c.next_action = "collect the whole run directory again, after every process has exited";
        return c;
    }
    c.reason = std::to_string(files) + " trace file(s), " + std::to_string(events) +
               " events: each file is one process's events, numbered from 1 with no gap";
    return c;
}

std::string env_or(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return v != nullptr && *v != '\0' ? std::string{v} : fallback;
}

// KEY=value lines of formal/tools.env.
std::map<std::string, std::string> read_tools_env(const fs::path& path) {
    std::map<std::string, std::string> out;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#' || line[0] == ':') {
            continue;
        }
        const auto eq = line.find('=');
        if (eq != std::string::npos) {
            out[line.substr(0, eq)] = line.substr(eq + 1);
        }
    }
    return out;
}

Check protocol_conformance_check(const Bundle& b, const VerifyOptions& o, const Check& structure) {
    Check c{.id = "protocol.conformance",
            .verdict = Verdict::Unavailable,
            .evidence = structure.evidence,
            .reason = {},
            .next_action = {},
            .details = {}};
    if (!b.contains("traces")) {
        c.reason = "no protocol traces were collected";
        c.next_action = structure.next_action;
        return c;
    }
    if (structure.verdict != Verdict::Passed) {
        c.reason = "the traces failed their structure check, so the validator was not run";
        c.next_action = "collect a complete run of traces";
        return c;
    }
    const fs::path formal = o.formal_dir.empty() ? env_or("CLINK_FORMAL_DIR", "") : o.formal_dir;
    if (formal.empty() || !fs::exists(formal / "trace") || !fs::exists(formal / "tools.env") ||
        !fs::exists(formal.parent_path() / "scripts" / "formal-check.sh")) {
        c.reason =
            "the trace validator (formal/ and scripts/formal-check.sh from a clink "
            "source tree) was not found";
        c.next_action = "pass --formal-dir=<clink source>/formal, or set CLINK_FORMAL_DIR";
        return c;
    }
    if (std::system("command -v java >/dev/null 2>&1") != 0) {
        c.reason = "the trace validator needs a Java runtime, and none is on PATH";
        c.next_action = "install a Java 11+ runtime";
        return c;
    }
    const auto pins = read_tools_env(formal / "tools.env");
    const std::string tools =
        o.formal_tools_dir.empty()
            ? env_or("CLINK_FORMAL_TOOLS_DIR",
                     env_or("CLINK_DEPS_PREFIX", env_or("HOME", "") + "/.clink-deps") +
                         "/formal-tools")
            : o.formal_tools_dir;
    const std::vector<std::pair<std::string, std::string>> needed = {
        {"tla2tools-" + (pins.count("TLA_TOOLS_VERSION") ? pins.at("TLA_TOOLS_VERSION") : "") +
             ".jar",
         pins.count("TLA_TOOLS_SHA256") ? pins.at("TLA_TOOLS_SHA256") : ""},
        {"CommunityModules-deps-" +
             (pins.count("COMMUNITY_MODULES_VERSION") ? pins.at("COMMUNITY_MODULES_VERSION") : "") +
             ".jar",
         pins.count("COMMUNITY_MODULES_SHA256") ? pins.at("COMMUNITY_MODULES_SHA256") : ""}};
    for (const auto& [jar, sha] : needed) {
        const auto path = fs::path{tools} / jar;
        if (sha.empty() || !fs::exists(path) || sha256_file(path) != sha) {
            c.reason = "the pinned TLA+ tools (" + jar +
                       ") are not in the jar cache; verification never downloads them";
            c.next_action =
                "run scripts/formal-check.sh once with network access to cache them, "
                "or set CLINK_FORMAL_TOOLS_DIR";
            c.details.push_back("jar cache: " + tools);
            return c;
        }
    }
    // The validator reads a run directory; give it a copy of the traces so the
    // bundle is never written to.
    std::error_code ec;
    const auto tmp = fs::temp_directory_path() /
                     ("clink-incident-traces-" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(tmp, ec);
    fs::create_directories(tmp / "run");
    for (const auto& a : b.artifacts) {
        if (a.role == "trace") {
            fs::copy_file(b.root / a.path,
                          tmp / "run" / fs::path{a.path}.filename(),
                          fs::copy_options::overwrite_existing,
                          ec);
        }
    }
    const auto out_file = tmp / "validator.log";
    const std::string cmd = "CLINK_FORMAL_TOOLS_DIR='" + tools + "' bash '" +
                            (formal.parent_path() / "scripts" / "formal-check.sh").string() +
                            "' --trace '" + (tmp / "run").string() + "' > '" + out_file.string() +
                            "' 2>&1";
    const int rc = std::system(cmd.c_str());
    const auto log = read_text(out_file);
    fs::remove_all(tmp, ec);
    std::vector<std::string> tail;
    {
        std::istringstream lines(log);
        std::string line;
        std::vector<std::string> all;
        while (std::getline(lines, line)) {
            all.push_back(line);
        }
        for (std::size_t i = all.size() > 12 ? all.size() - 12 : 0; i < all.size(); ++i) {
            tail.push_back(all[i]);
        }
    }
    c.details = tail;
    if (rc == 0) {
        c.verdict = Verdict::Passed;
        c.reason =
            "the formal trace validator accepts the run: its events are a behaviour of "
            "the exactly-once specification. This concerns the protocol's steps, not the "
            "records' contents.";
        return c;
    }
    c.verdict = Verdict::Failed;
    c.reason = "the formal trace validator rejects the run";
    c.next_action =
        "read the validator's output above; see docs/internals/"
        "exactly-once-specification.md, What a divergence means";
    return c;
}

}  // namespace

std::vector<Check> verify_core(const Bundle& b, const VerifyOptions& o) {
    std::vector<Check> out;
    out.push_back(bundle_manifest_check(b));
    out.push_back(bundle_artifacts_check(b));
    out.push_back(bundle_completeness_check(b));
    if (o.profile == "integrity") {
        return out;
    }
    // A bundle whose artifacts are not as listed cannot be checked further:
    // a check over altered evidence would vouch for the alteration.
    const bool intact = out[1].verdict == Verdict::Passed;
    const std::vector<std::string> later = {"checkpoint.marker",
                                            "checkpoint.restore_rule",
                                            "checkpoint.participants",
                                            "state.decode",
                                            "protocol.trace_structure",
                                            "protocol.conformance"};
    if (!intact) {
        for (const auto& id : later) {
            if (o.profile != "full" && id.rfind("protocol.", 0) == 0) {
                continue;
            }
            out.push_back(Check{.id = id,
                                .verdict = Verdict::Unavailable,
                                .evidence = {},
                                .reason = "not run: the bundle's artifacts are not as its manifest "
                                          "lists them",
                                .next_action = "collect the bundle again",
                                .details = {}});
        }
        return out;
    }
    out.push_back(checkpoint_marker_check(b));
    out.push_back(checkpoint_restore_rule_check(b));
    out.push_back(checkpoint_participants_check(b));
    out.push_back(state_decode_check(b));
    if (o.profile == "full") {
        auto structure = trace_structure_check(b);
        auto conformance = protocol_conformance_check(b, o, structure);
        out.push_back(std::move(structure));
        out.push_back(std::move(conformance));
        // Said in every full report, so that passing is never read as saying
        // what a destination holds. No profile requires it.
        out.push_back(Check{
            .id = "destination.reconciliation",
            .verdict = Verdict::Unsupported,
            .evidence = {},
            .reason = "offline verification reads no destination, so whether the job's sinks "
                      "hold what it wrote is not checked",
            .next_action = "compare the destination with the job's expected output where it lives",
            .details = {}});
    }
    return out;
}

}  // namespace clink::incident
