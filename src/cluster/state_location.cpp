#include "clink/cluster/state_location.hpp"

#include <algorithm>
#include <cctype>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/cluster/coordination_store.hpp"

namespace clink::cluster {

namespace {

constexpr std::string_view kSchemeSep = "://";
constexpr std::string_view kFileScheme = "file://";

std::string job_segment(JobId job_id) {
    return "_state/" + std::to_string(job_id);
}

std::string strip_trailing_slashes(std::string s) {
    while (s.size() > 1 && s.back() == '/') {
        s.pop_back();
    }
    return s;
}

std::string append_segment(std::string_view location, std::string_view segment) {
    auto out = strip_trailing_slashes(std::string{location});
    if (out != "/") {
        out.push_back('/');
    }
    out += segment;
    return out;
}

std::string without_file_scheme(std::string s) {
    if (s.rfind(kFileScheme, 0) == 0) {
        s.erase(0, kFileScheme.size());
    }
    return s;
}

// A location this process reads as a directory: a bare path, or file://.
bool is_local(const std::string& location) {
    return location.find(kSchemeSep) == std::string::npos || location.rfind(kFileScheme, 0) == 0;
}

bool all_digits(std::string_view s) {
    return !s.empty() && s.size() <= 20 &&
           std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
}

std::optional<std::uint64_t> parse_u64(std::string_view s) {
    if (!all_digits(s)) {
        return std::nullopt;
    }
    try {
        return static_cast<std::uint64_t>(std::stoull(std::string{s}));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// <root>/_jobs/<job id> -> {root, job id}; nullopt for anything else.
struct JobDir {
    std::string checkpoint_root;
    JobId job{};
};

std::optional<JobDir> split_job_dir(const std::string& dir) {
    const auto d = strip_trailing_slashes(dir);
    const auto last = d.rfind('/');
    if (last == std::string::npos) {
        return std::nullopt;
    }
    const auto id = parse_u64(std::string_view{d}.substr(last + 1));
    if (!id.has_value()) {
        return std::nullopt;
    }
    constexpr std::string_view kJobs = "_jobs";
    const auto parent = d.substr(0, last);
    if (parent == kJobs) {
        return JobDir{".", *id};  // a relative _jobs/<id>
    }
    if (parent.size() <= kJobs.size() ||
        parent.compare(parent.size() - kJobs.size(), kJobs.size(), kJobs) != 0 ||
        parent[parent.size() - kJobs.size() - 1] != '/') {
        return std::nullopt;
    }
    auto root = parent.substr(0, parent.size() - kJobs.size() - 1);
    if (root.empty()) {
        root = "/";
    }
    return JobDir{std::move(root), *id};
}

bool is_relative_state(const std::string& state) {
    return state.find(kSchemeSep) == std::string::npos && !state.empty() && state.front() != '/';
}

std::string join_location(const std::string& root, const std::string& relative) {
    return append_segment(root, relative);
}

struct FoundMarker {
    JobId job{};
    std::string body;
};

// Every job holding _jobs/<job id>/<marker_name> in a checkpoint directory,
// by job id. The filesystem store lists recursively and an object store by
// key prefix, so the key shape is matched exactly.
std::vector<FoundMarker> markers_named(CoordinationStore& store, const std::string& marker_name) {
    std::vector<FoundMarker> found;
    for (const auto& key : store.list("_jobs")) {
        const auto first = key.find('/');
        const auto second = first == std::string::npos ? first : key.find('/', first + 1);
        if (first == std::string::npos || second == std::string::npos ||
            key.compare(0, first, "_jobs") != 0 ||
            key.compare(second + 1, std::string::npos, marker_name) != 0) {
            continue;
        }
        const auto job = parse_u64(std::string_view{key}.substr(first + 1, second - first - 1));
        if (!job.has_value()) {
            continue;
        }
        auto body = store.get(key);
        if (!body.has_value()) {
            continue;  // pruned between the list and the read
        }
        found.push_back(FoundMarker{*job, std::move(*body)});
    }
    std::sort(found.begin(), found.end(), [](const FoundMarker& a, const FoundMarker& b) {
        return a.job < b.job;
    });
    return found;
}

}  // namespace

std::string state_base_of(const std::string& checkpoint_dir, const std::string& state_backend_uri) {
    return state_backend_uri.empty() ? checkpoint_dir : state_backend_uri;
}

std::string job_state_root(std::string_view base, JobId job_id) {
    const auto segment = job_segment(job_id);
    const auto sep = base.find(kSchemeSep);
    if (sep == std::string_view::npos) {
        // A bare path is a directory, '?' and all.
        return base.empty() ? std::string{} : append_segment(base, segment);
    }
    const auto scheme = base.substr(0, sep + kSchemeSep.size());
    auto rest = base.substr(sep + kSchemeSep.size());
    std::string_view query;
    bool has_query = false;
    if (const auto q = rest.find('?'); q != std::string_view::npos) {
        query = rest.substr(q + 1);
        rest = rest.substr(0, q);
        has_query = true;
    }
    if (rest.empty()) {
        return std::string{base};
    }
    std::string out{scheme};
    out += append_segment(rest, segment);
    if (has_query) {
        out.push_back('?');
        bool first = true;
        for (std::size_t start = 0; start <= query.size();) {
            const auto amp = query.find('&', start);
            const auto kv = query.substr(
                start, amp == std::string_view::npos ? std::string_view::npos : amp - start);
            if (!first) {
                out.push_back('&');
            }
            first = false;
            constexpr std::string_view kLocal = "local=";
            if (kv.size() > kLocal.size() && kv.substr(0, kLocal.size()) == kLocal) {
                out += kLocal;
                out += append_segment(kv.substr(kLocal.size()), segment);
            } else {
                out += kv;
            }
            if (amp == std::string_view::npos) {
                break;
            }
            start = amp + 1;
        }
    }
    return out;
}

CompletedMarker parse_completed_marker(std::string_view body) {
    CompletedMarker out;
    std::size_t pos = 0;
    while (pos <= body.size()) {
        const auto nl = body.find('\n', pos);
        const auto line =
            body.substr(pos, nl == std::string_view::npos ? std::string_view::npos : nl - pos);
        const auto eq = line.find('=');
        if (eq != std::string_view::npos) {
            const auto key = line.substr(0, eq);
            const auto value = line.substr(eq + 1);
            if (key == "job") {
                out.job = parse_u64(value);
            } else if (key == "checkpoint") {
                out.checkpoint = parse_u64(value);
            } else if (key == "generation") {
                if (const auto g = parse_u64(value);
                    g.has_value() && *g <= std::numeric_limits<std::uint32_t>::max()) {
                    out.generation = static_cast<std::uint32_t>(*g);
                }
            } else if (key == "subtasks") {
                // Any token that is not a plain index makes the whole set
                // untrustworthy rather than half-read.
                std::set<std::uint32_t> set;
                bool ok = !value.empty();
                for (std::size_t p = 0; ok && p <= value.size();) {
                    const auto comma = value.find(',', p);
                    const auto tok = value.substr(
                        p, comma == std::string_view::npos ? std::string_view::npos : comma - p);
                    const auto idx = parse_u64(tok);
                    ok = idx.has_value() && *idx <= std::numeric_limits<std::uint32_t>::max();
                    if (ok) {
                        set.insert(static_cast<std::uint32_t>(*idx));
                    }
                    if (comma == std::string_view::npos) {
                        break;
                    }
                    p = comma + 1;
                }
                if (ok) {
                    out.subtasks = std::move(set);
                }
            } else if (key == "state" && !value.empty()) {
                out.state = std::string{value};
            }
        }
        if (nl == std::string_view::npos) {
            break;
        }
        pos = nl + 1;
    }
    return out;
}

std::string completed_marker_body(JobId job,
                                  std::uint64_t checkpoint,
                                  std::uint32_t generation,
                                  const std::set<std::uint32_t>& subtasks,
                                  std::string_view state) {
    std::string list;
    for (const auto idx : subtasks) {
        list += (list.empty() ? "" : ",") + std::to_string(idx);
    }
    std::string body = "job=" + std::to_string(job) + "\ncheckpoint=" + std::to_string(checkpoint) +
                       "\ngeneration=" + std::to_string(generation) + "\nsubtasks=" + list + "\n";
    if (!state.empty()) {
        body += "state=";
        body += state;
        body += "\n";
    }
    return body;
}

std::string recorded_state_location(const std::string& checkpoint_dir,
                                    JobId job_id,
                                    const std::string& root) {
    if (!checkpoint_dir.empty() &&
        without_file_scheme(root) == job_state_root(without_file_scheme(checkpoint_dir), job_id)) {
        return job_segment(job_id);
    }
    return root;
}

std::uint32_t highest_generation_in(const std::string& root) {
    if (root.empty() || !is_local(root)) {
        return 1;
    }
    std::uint32_t best = 0;
    std::error_code ec;
    for (std::filesystem::directory_iterator it{without_file_scheme(root), ec};
         !ec && it != std::filesystem::directory_iterator{};
         it.increment(ec)) {
        std::error_code type_ec;
        if (!it->is_directory(type_ec) || type_ec) {
            continue;
        }
        const auto name = it->path().filename().string();
        if (name.size() < 2 || name.front() != 'v') {
            continue;
        }
        const auto g = parse_u64(std::string_view{name}.substr(1));
        if (g.has_value() && *g <= std::numeric_limits<std::uint32_t>::max()) {
            best = std::max(best, static_cast<std::uint32_t>(*g));
        }
    }
    return best == 0 ? 1U : best;
}

RestoreSource resolve_restore_source(const std::string& restore_dir,
                                     std::uint64_t checkpoint_id,
                                     const std::string& legacy_base) {
    RestoreSource out;
    out.checkpoint_id = checkpoint_id;
    out.legacy = true;
    out.state_root = restore_dir;
    if (restore_dir.empty() || checkpoint_id == 0) {
        return out;
    }
    const std::string dir = without_file_scheme(restore_dir);
    std::shared_ptr<CoordinationStore> store;
    try {
        store = make_coordination_store(dir);
    } catch (const std::exception&) {
        // Not a checkpoint directory this process can open: a state backend
        // URI, restored from as given.
        out.generation = highest_generation_in(restore_dir);
        return out;
    }
    const std::string marker_name = "COMPLETED-" + std::to_string(checkpoint_id);

    // The directory holding _jobs/, against which a relative state= resolves;
    // empty for a marker directory that is not <root>/_jobs/<id>.
    std::string checkpoint_root;
    // Where an older checkpoint's state lived: the checkpoint directory, as
    // the caller named it, unless the caller knows better.
    std::string legacy_location = restore_dir;
    std::optional<JobId> dir_job;
    std::string body;
    if (auto own = store->get(marker_name); own.has_value()) {
        // A job's marker directory.
        body = std::move(*own);
        out.marker = strip_trailing_slashes(dir) + "/" + marker_name;
        if (auto split = split_job_dir(dir)) {
            checkpoint_root = std::move(split->checkpoint_root);
            legacy_location = checkpoint_root;
            dir_job = split->job;
        }
    } else {
        auto found = markers_named(*store, marker_name);
        if (found.empty()) {
            // No marker names it: a directory of state alone, read as it is.
            out.generation = highest_generation_in(restore_dir);
            return out;
        }
        checkpoint_root = strip_trailing_slashes(dir);
        if (found.size() > 1) {
            const bool any_located = std::any_of(found.begin(), found.end(), [](const auto& f) {
                return parse_completed_marker(f.body).state.has_value();
            });
            if (!any_located) {
                // Older markers: those jobs' state shares one location, read
                // as it always was. Which job's participant set applies is
                // unknown, so none does.
                out.state_root = legacy_base.empty() ? legacy_location : legacy_base;
                out.generation = highest_generation_in(out.state_root);
                return out;
            }
            std::string jobs;
            for (const auto& f : found) {
                jobs += (jobs.empty() ? "" : ", ") + std::to_string(f.job);
            }
            throw std::runtime_error(
                "cannot restore checkpoint " + std::to_string(checkpoint_id) + " from " +
                restore_dir + ": jobs " + jobs + " each completed a checkpoint " +
                std::to_string(checkpoint_id) +
                " there, and each keeps its own state. Name the job to restore by its marker "
                "directory, for example " +
                checkpoint_root + "/_jobs/" + std::to_string(found.front().job) + ".");
        }
        dir_job = found.front().job;
        body = std::move(found.front().body);
        out.marker =
            checkpoint_root + "/_jobs/" + std::to_string(found.front().job) + "/" + marker_name;
    }

    const auto marker = parse_completed_marker(body);
    out.job = marker.job.has_value() ? marker.job : dir_job;
    out.participants = marker.subtasks;
    if (!marker.state.has_value() && !marker.subtasks.has_value() && dir_job.has_value() &&
        !checkpoint_root.empty() && is_local(checkpoint_root)) {
        // A marker recording neither its participants nor its state is
        // damaged: every marker this build writes records both. Where it
        // would have said the state is cannot be read from it, and the job's
        // own root in the checkpoint directory is where this build writes, so
        // the state is read there when that exists.
        const auto own = join_location(checkpoint_root, job_segment(*dir_job));
        std::error_code ec;
        if (std::filesystem::is_directory(without_file_scheme(own), ec)) {
            out.legacy = false;
            out.state_root = own;
            out.generation =
                marker.generation.has_value() ? *marker.generation : highest_generation_in(own);
            return out;
        }
    }
    if (!marker.state.has_value()) {
        // Written before state was namespaced by job.
        out.state_root = legacy_base.empty() ? legacy_location : legacy_base;
    } else if (is_relative_state(*marker.state)) {
        if (checkpoint_root.empty()) {
            throw std::runtime_error(
                "cannot restore checkpoint " + std::to_string(checkpoint_id) + " from " +
                restore_dir + ": its marker records the state at " + *marker.state +
                " relative to its checkpoint directory, and " + restore_dir +
                " is not a marker directory inside one (<checkpoint dir>/_jobs/<job id>). "
                "Restore from the checkpoint directory, or from the marker directory in it.");
        }
        out.legacy = false;
        out.state_root = join_location(checkpoint_root, *marker.state);
    } else {
        out.legacy = false;
        out.state_root = *marker.state;
    }
    out.generation =
        marker.generation.has_value() ? *marker.generation : highest_generation_in(out.state_root);
    return out;
}

}  // namespace clink::cluster
