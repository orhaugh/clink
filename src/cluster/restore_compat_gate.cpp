#include "clink/cluster/restore_compat_gate.hpp"

#include <cstddef>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <system_error>

#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace clink::cluster {

namespace {

// Both packed stamp maps a savepoint carries, from the one restore of
// subtask 0's snapshot into the reference reader (which unpacks both
// metadata keys).
struct StoredStamps {
    std::string versions_packed;
    std::string fingerprints_packed;
};

// Read subtask 0's snapshot bytes and recover its packed stamps. nullopt on
// any failure (missing file, I/O, decode) so the gate degrades to "could not
// check" rather than a false block.
std::optional<StoredStamps> read_stored_stamps(const std::string& restore_from_dir,
                                               std::uint64_t checkpoint_id) {
    namespace fs = std::filesystem;
    const fs::path path = fs::path{restore_from_dir} / "0" /
                          ("checkpoint-" + std::to_string(checkpoint_id) + ".snap");
    std::error_code ec;
    if (!fs::exists(path, ec) || ec) {
        return std::nullopt;
    }
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        return std::nullopt;
    }
    f.seekg(0, std::ios::end);
    const auto size = f.tellg();
    if (size < 0) {
        return std::nullopt;
    }
    f.seekg(0, std::ios::beg);
    std::vector<std::byte> bytes(static_cast<std::size_t>(size));
    if (size > 0) {
        f.read(reinterpret_cast<char*>(bytes.data()), size);
        if (!f) {
            return std::nullopt;
        }
    }
    try {
        clink::InMemoryStateBackend backend;
        backend.restore(clink::Snapshot{.checkpoint_id = clink::CheckpointId{checkpoint_id},
                                        .bytes = std::move(bytes)});
        return StoredStamps{.versions_packed = backend.restored_state_versions().pack(),
                            .fingerprints_packed = backend.restored_state_fingerprints().pack()};
    } catch (...) {
        return std::nullopt;
    }
}

std::string format_reject(const std::vector<clink::StateIncompatibility>& incompat,
                          const std::string& so_path) {
    std::string reason =
        "restore incompatible with job binary: " + std::to_string(incompat.size()) +
        " state slot(s) have no migration path to the job's expected versions [";
    bool first = true;
    for (const auto& e : incompat) {
        if (!first) {
            reason += ", ";
        }
        reason += "op=" + std::to_string(e.op_id.value()) + " '" + e.state_type + "' v" +
                  std::to_string(e.from_version) + "->v" + std::to_string(e.to_version);
        first = false;
    }
    reason +=
        "]; inspect with `clink check-savepoint --file=<savepoint> --expected=" + so_path + "`";
    return reason;
}

std::string format_fingerprint_reject(const std::vector<clink::StateFingerprintMismatch>& mm,
                                      const std::string& so_path) {
    std::ostringstream oss;
    oss << "restore incompatible with job binary: " << mm.size()
        << " state slot(s) changed shape with no declared version bump [";
    bool first = true;
    for (const auto& m : mm) {
        if (!first) {
            oss << ", ";
        }
        oss << "op=" << m.op_id.value() << " slot='" << m.slot << "' stored=" << std::hex
            << m.stored_fingerprint << " declared=" << m.declared_fingerprint << std::dec;
        first = false;
    }
    oss << "]; if the shape change is deliberate, bump SchemaVersionTrait and register a "
           "migration; inspect with `clink check-savepoint --file=<savepoint> --expected="
        << so_path << "`";
    return oss.str();
}

// "0-15", "0, 3-7": an index set as its runs, for a refusal a person reads. A
// layout after a hot cutover is not contiguous (the new block is appended), so
// the runs are what tell two same-sized sets apart.
std::string describe_indices(const std::set<std::uint32_t>& indices) {
    constexpr std::size_t kMaxRuns = 8;
    std::string out;
    std::size_t runs = 0;
    for (auto it = indices.begin(); it != indices.end();) {
        if (runs == kMaxRuns) {
            out += ", ...";
            break;
        }
        const auto first = *it;
        auto last = first;
        for (++it; it != indices.end() && *it == last + 1; ++it) {
            last = *it;
        }
        out += (runs == 0 ? "" : ", ") + std::to_string(first);
        if (last != first) {
            out += "-" + std::to_string(last);
        }
        ++runs;
    }
    return out;
}

std::string counted(std::size_t n, const char* noun) {
    return std::to_string(n) + " " + noun + (n == 1 ? "" : "s");
}

}  // namespace

std::string check_restore_compatibility_via_plugins(const std::vector<std::string>& plugin_so_paths,
                                                    const std::string& restore_from_dir,
                                                    std::uint64_t restore_checkpoint_id) {
    if (restore_from_dir.empty()) {
        return "";  // fresh start - nothing to gate
    }
    const auto stamps = read_stored_stamps(restore_from_dir, restore_checkpoint_id);
    if (!stamps.has_value()) {
        return "";  // best-effort: savepoint not coordinator-readable -> rely on C at worker start
    }

    for (const auto& so_path : plugin_so_paths) {
        // RTLD_NODELETE, and it is load-bearing.
        //
        // Calling clink_job_check_restore_compatibility runs the .so's
        // build_fn, which REGISTERS factories - std::function closures whose
        // code lives inside this module - into the process-wide registries.
        // The dlclose below then used to unmap it, leaving those closures
        // pointing at nothing, and the caller (recover_persisted_jobs) goes
        // straight on to submit and deploy that same job. The first call
        // through one of them jumps into unmapped memory: SIGSEGV with a
        // garbage PC and no recoverable stack.
        //
        // plugin_loader.hpp states the invariant this violated - "dlclose()
        // with registered std::function closures pointing back into plugin
        // code is risky without quiescing all in-flight subtasks first" - and
        // this was the one site doing it.
        //
        // Only the RESTORE path reaches here (the function returns early when
        // restore_from_dir is empty), which is why a normal submission was
        // unaffected and every recovery crashed the coordinator. Linux only,
        // because dlclose there really unmaps; macOS kept the pages resident
        // and the closures kept working.
        //
        // NODELETE rather than dropping the dlclose: the refcount is still
        // managed, the module simply is never unmapped. This path exports a
        // raw function pointer without a JobBundle to retain its module, so it
        // deliberately uses the process-lifetime exception.
        void* handle = ::dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL | RTLD_NODELETE);
        if (handle == nullptr) {
            continue;  // a connector .so we can't load; the job .so is what matters
        }
        using CheckFn = int (*)(const char*, const char**, std::size_t*);
        auto* sym = ::dlsym(handle, "clink_job_check_restore_compatibility");
        if (sym == nullptr) {
            ::dlclose(handle);
            continue;  // not a CLINK_REGISTER_JOB .so (e.g. a connector plugin)
        }
        CheckFn fn = nullptr;
        std::memcpy(&fn, &sym, sizeof(fn));
        const char* out_packed = nullptr;
        std::size_t out_size = 0;
        const int rc = fn(stamps->versions_packed.c_str(), &out_packed, &out_size);
        const std::string packed{out_packed != nullptr ? out_packed : "", out_size};
        if (rc != 0) {
            // build_fn failed or version map could not decode .so-side:
            // best-effort, don't block (the real submit/deploy will
            // surface the same build failure with a clearer message).
            ::dlclose(handle);
            return "";
        }
        if (!packed.empty()) {
            ::dlclose(handle);
            std::vector<clink::StateIncompatibility> incompat;
            try {
                incompat = clink::unpack_incompatibilities(packed);
            } catch (...) {
                return "";  // malformed result -> don't block
            }
            return format_reject(incompat, so_path);
        }

        // Versions compatible. Second, OPTIONAL gate on the same handle: the
        // fingerprint check (design record 009 follow-on). An older .so lacks
        // the symbol and is simply not fingerprint-gated - absence gates
        // nothing, and the bind-time gate remains the restore-time backstop.
        using FpCheckFn = int (*)(const char*, const char*, const char**, std::size_t*);
        auto* fp_sym = ::dlsym(handle, "clink_job_check_restore_fingerprints");
        if (fp_sym == nullptr) {
            ::dlclose(handle);
            return "";
        }
        FpCheckFn fp_fn = nullptr;
        std::memcpy(&fp_fn, &fp_sym, sizeof(fp_fn));
        const char* fp_out = nullptr;
        std::size_t fp_out_size = 0;
        const int fp_rc = fp_fn(stamps->fingerprints_packed.c_str(),
                                stamps->versions_packed.c_str(),
                                &fp_out,
                                &fp_out_size);
        const std::string fp_packed{fp_out != nullptr ? fp_out : "", fp_out_size};
        ::dlclose(handle);
        if (fp_rc != 0 || fp_packed.empty()) {
            return "";  // best-effort on error; empty = compatible
        }
        std::vector<clink::StateFingerprintMismatch> mismatches;
        try {
            mismatches = clink::unpack_fingerprint_mismatches(fp_packed);
        } catch (...) {
            return "";  // malformed result -> don't block
        }
        return format_fingerprint_reject(mismatches, so_path);
    }
    return "";  // no .so exported the check -> cannot gate
}

std::optional<std::set<std::uint32_t>> recorded_restore_participants(
    const std::string& restore_from_dir, std::uint64_t checkpoint_id) {
    namespace fs = std::filesystem;
    if (restore_from_dir.empty() || checkpoint_id == 0) {
        return std::nullopt;
    }
    // A plain path, or a file:// URI naming one. Any other scheme is a state
    // backend URI (remote-read://, an object store) whose markers, if it has
    // any, are not a directory this process lists.
    std::string root = restore_from_dir;
    if (const auto sep = root.find("://"); sep != std::string::npos) {
        if (root.compare(0, sep, "file") != 0) {
            return std::nullopt;
        }
        root.erase(0, sep + 3);
    }
    // Exactly one job's marker for this id, as the restore requires: a root that
    // several jobs have written into can hold COMPLETED-<id> for more than one of
    // them, and picking one would be a guess. Non-throwing iteration throughout,
    // because a filesystem error here must mean "cannot check", never a failed
    // submit.
    const std::string marker_name = "COMPLETED-" + std::to_string(checkpoint_id);
    std::optional<fs::path> marker;
    std::error_code ec;
    fs::directory_iterator it{fs::path{root} / "_jobs", ec};
    for (; !ec && it != fs::directory_iterator{}; it.increment(ec)) {
        std::error_code type_ec;
        if (!it->is_directory(type_ec) || type_ec) {
            continue;
        }
        auto candidate = it->path() / marker_name;
        std::error_code file_ec;
        if (!fs::is_regular_file(candidate, file_ec) || file_ec) {
            continue;
        }
        if (marker.has_value()) {
            return std::nullopt;
        }
        marker = std::move(candidate);
    }
    if (ec || !marker.has_value()) {
        return std::nullopt;
    }
    std::ifstream in(*marker);
    if (!in) {
        return std::nullopt;
    }
    // The body the coordinator writes: job=, checkpoint=, generation=, then
    // subtasks=<comma-separated job-global indices>. Any token that is not a
    // plain index makes the whole set untrustworthy rather than half-read.
    constexpr std::string_view kKey = "subtasks=";
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind(kKey, 0) != 0) {
            continue;
        }
        std::set<std::uint32_t> out;
        std::size_t pos = kKey.size();
        for (;;) {
            const auto comma = line.find(',', pos);
            const auto tok =
                line.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
            if (tok.empty() || tok.size() > 10 ||
                tok.find_first_not_of("0123456789") != std::string::npos) {
                return std::nullopt;
            }
            const auto value = std::stoull(tok);
            if (value > std::numeric_limits<std::uint32_t>::max()) {
                return std::nullopt;
            }
            out.insert(static_cast<std::uint32_t>(value));
            if (comma == std::string::npos) {
                break;
            }
            pos = comma + 1;
        }
        return out;
    }
    return std::nullopt;  // a marker predating the participant set
}

std::string restore_layout_refusal(const std::string& restore_from_dir,
                                   std::uint64_t checkpoint_id,
                                   const std::set<std::uint32_t>& recorded,
                                   const std::set<std::uint32_t>& planned,
                                   const std::vector<std::string>& single_instance_ops) {
    if (recorded == planned) {
        return "";
    }
    std::string reason = "refusing to restore checkpoint " + std::to_string(checkpoint_id) +
                         " from " + restore_from_dir + ": its COMPLETED marker records " +
                         counted(recorded.size(), "participating subtask") + " (" +
                         describe_indices(recorded) + ") and this plan deploys " +
                         std::to_string(planned.size()) + " (" + describe_indices(planned) +
                         "). State is restored by job-global subtask index, so under a "
                         "different layout the operators would receive each other's state.";
    if (!single_instance_ops.empty()) {
        std::string names;
        for (const auto& op : single_instance_ops) {
            names += (names.empty() ? "" : ", ") + op;
        }
        reason += single_instance_ops.size() == 1
                      ? " This plan runs " + names +
                            " as a single instance whatever the submitted parallelism; a "
                            "checkpoint taken by an engine version that ran it in parallel has "
                            "a different layout, which is the likely cause after an upgrade."
                      : " This plan runs " + std::to_string(single_instance_ops.size()) +
                            " operators as single instances whatever the submitted parallelism (" +
                            names +
                            "); a checkpoint taken by an engine version that ran any of them in "
                            "parallel has a different layout, which is the likely cause after "
                            "an upgrade.";
    }
    reason +=
        " Resubmit with the layout the checkpoint was taken with (the same graph and "
        "parallelism, on the engine version that took it), or submit without the savepoint to "
        "start from empty state.";
    return reason;
}

std::string check_restore_layout(const std::string& restore_from_dir,
                                 std::uint64_t restore_checkpoint_id,
                                 const std::set<std::uint32_t>& planned,
                                 const std::vector<std::string>& single_instance_ops) {
    if (restore_from_dir.empty() || restore_checkpoint_id == 0) {
        return "";  // nothing is restored, so there is no layout to match
    }
    const auto recorded = recorded_restore_participants(restore_from_dir, restore_checkpoint_id);
    if (!recorded.has_value()) {
        return "";  // cannot identify the layout; the deploy proceeds as it always has
    }
    return restore_layout_refusal(
        restore_from_dir, restore_checkpoint_id, *recorded, planned, single_instance_ops);
}

}  // namespace clink::cluster
