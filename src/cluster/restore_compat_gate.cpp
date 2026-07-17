#include "clink/cluster/restore_compat_gate.hpp"

#include <cstddef>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <optional>

#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace clink::cluster {

namespace {

// Read subtask 0's snapshot bytes and recover its packed StateVersionMap.
// nullopt on any failure (missing file, I/O, decode) so the gate degrades
// to "could not check" rather than a false block.
std::optional<std::string> read_stored_versions_packed(const std::string& restore_from_dir,
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
        return backend.restored_state_versions().pack();
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

}  // namespace

std::string check_restore_compatibility_via_plugins(const std::vector<std::string>& plugin_so_paths,
                                                    const std::string& restore_from_dir,
                                                    std::uint64_t restore_checkpoint_id) {
    if (restore_from_dir.empty()) {
        return "";  // fresh start - nothing to gate
    }
    const auto stored_packed = read_stored_versions_packed(restore_from_dir, restore_checkpoint_id);
    if (!stored_packed.has_value()) {
        return "";  // best-effort: savepoint not coordinator-readable -> rely on C at worker start
    }

    for (const auto& so_path : plugin_so_paths) {
        void* handle = ::dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
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
        const int rc = fn(stored_packed->c_str(), &out_packed, &out_size);
        const std::string packed{out_packed != nullptr ? out_packed : "", out_size};
        ::dlclose(handle);
        if (rc != 0) {
            // build_fn failed or version map could not decode .so-side:
            // best-effort, don't block (the real submit/deploy will
            // surface the same build failure with a clearer message).
            return "";
        }
        if (packed.empty()) {
            return "";  // the job .so says compatible
        }
        std::vector<clink::StateIncompatibility> incompat;
        try {
            incompat = clink::unpack_incompatibilities(packed);
        } catch (...) {
            return "";  // malformed result -> don't block
        }
        return format_reject(incompat, so_path);
    }
    return "";  // no .so exported the check -> cannot gate
}

}  // namespace clink::cluster
