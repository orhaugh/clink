// clink_check_savepoint - inspect the state-schema version stamps
// embedded in a savepoint produced by InMemoryStateBackend (or a
// derivative). Surfaces the StateVersionMap to operators without a
// running coordinator: read the file, decode the Arrow IPC stream, print the
// (op_id, state_type, version) tuples plus a row count.
//
// This is the inspector form. A future enhancement loads the live
// job's expected version map (via `clink_job_expected_state_versions`
// from a job .so or a manifest file) and reports a per-operator
// compatibility matrix, refusing the deploy if any required
// migration is missing.
//
// Wire-compatible with any Arrow consumer: pyarrow can open the same
// file and inspect the schema metadata directly. The CLI exists so
// operators don't need an Arrow toolchain in their deploy path.
//
// Usage:
//   clink check-savepoint --file=path/to/snapshot.snap [--quiet]

#include <cstring>
#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace {

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

void usage() {
    std::cerr << "Usage: clink check-savepoint --file=<path> [--expected=<job.so>] [--quiet]\n"
              << "\n"
              << "Inspects state-schema version stamps and shape fingerprints\n"
              << "inside a snapshot file. Prints one row per stamped\n"
              << "(op_id, state_type, version) and per (op_id, slot,\n"
              << "fingerprint), plus a summary count of keyed entries.\n"
              << "\n"
              << "With --expected=<job.so>, additionally checks whether the\n"
              << "savepoint can be restored into that job binary: the .so is\n"
              << "dlopened and asked (.so-side, where its migration registry\n"
              << "and shape declarations live) to report any (op, state_type)\n"
              << "it cannot migrate to its expected versions, and any declared\n"
              << "slot whose stored shape changed with no version bump. A .so\n"
              << "built before the fingerprint export is version-checked only.\n"
              << "\n"
              << "Exit codes: 0 = read OK (and compatible if --expected set),\n"
              << "1 = I/O or decode error, 2 = .so load / check error,\n"
              << "3 = incompatible savepoint (migration path missing or\n"
              << "undeclared shape change).\n";
}

// Call a job .so's .so-side compatibility check. Returns the CLI exit
// code: 0 compatible, 2 load/check error, 3 incompatible. The check runs
// inside the .so because the StateMigrationRegistry is .so-local
// (clink_core is statically linked, so this process's global() is a
// separate, empty instance).
int run_compat_check(const std::string& so_path,
                     const std::string& stored_packed,
                     const std::string& stored_fps_packed,
                     bool quiet) {
    void* handle = ::dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        std::cerr << "clink check-savepoint: dlopen failed: " << ::dlerror() << "\n";
        return 2;
    }
    using CheckFn = int (*)(const char*, const char**, std::size_t*);
    auto sym = ::dlsym(handle, "clink_job_check_restore_compatibility");
    if (sym == nullptr) {
        std::cerr << "clink check-savepoint: .so does not export "
                     "clink_job_check_restore_compatibility\n"
                  << "  (was it built with CLINK_REGISTER_JOB?)\n";
        ::dlclose(handle);
        return 2;
    }
    CheckFn check = nullptr;
    std::memcpy(&check, &sym, sizeof(check));

    const char* out_packed = nullptr;
    std::size_t out_size = 0;
    const int rc = check(stored_packed.c_str(), &out_packed, &out_size);
    if (rc != 0) {
        std::cerr << "clink check-savepoint: .so check returned " << rc
                  << (rc == 1 ? " (job build_fn failed)" : " (could not decode version map)")
                  << "\n";
        ::dlclose(handle);
        return 2;
    }
    // Copy the result out before dlclose invalidates the .so's memory.
    const std::string packed{out_packed != nullptr ? out_packed : "", out_size};

    // Shape fingerprints: the optional second export. An older .so lacks it
    // and stays version-checked only - said out loud rather than silently.
    std::string fp_packed;
    bool fp_checked = false;
    int fp_rc = 0;
    {
        using FpCheckFn = int (*)(const char*, const char*, const char**, std::size_t*);
        auto fp_sym = ::dlsym(handle, "clink_job_check_restore_fingerprints");
        if (fp_sym != nullptr) {
            FpCheckFn fp_check = nullptr;
            std::memcpy(&fp_check, &fp_sym, sizeof(fp_check));
            const char* fp_out = nullptr;
            std::size_t fp_out_size = 0;
            fp_rc =
                fp_check(stored_fps_packed.c_str(), stored_packed.c_str(), &fp_out, &fp_out_size);
            fp_packed.assign(fp_out != nullptr ? fp_out : "", fp_out_size);
            fp_checked = true;
        }
    }
    ::dlclose(handle);

    std::vector<clink::StateIncompatibility> incompat;
    try {
        incompat = clink::unpack_incompatibilities(packed);
    } catch (const std::exception& e) {
        std::cerr << "clink check-savepoint: malformed compatibility result: " << e.what() << "\n";
        return 2;
    }

    if (incompat.empty()) {
        if (!fp_checked) {
            if (!quiet) {
                std::cout << "compatible: savepoint restores into " << so_path << "\n";
                std::cout << "  (fingerprint check skipped: the .so predates "
                             "clink_job_check_restore_fingerprints)\n";
            }
            return 0;
        }
        if (fp_rc != 0) {
            std::cerr << "clink check-savepoint: .so fingerprint check returned " << fp_rc
                      << (fp_rc == 1 ? " (job build_fn failed)" : " (could not decode stamp maps)")
                      << "\n";
            return 2;
        }
        std::vector<clink::StateFingerprintMismatch> mismatches;
        try {
            mismatches = clink::unpack_fingerprint_mismatches(fp_packed);
        } catch (const std::exception& e) {
            std::cerr << "clink check-savepoint: malformed fingerprint result: " << e.what()
                      << "\n";
            return 2;
        }
        if (mismatches.empty()) {
            if (!quiet) {
                std::cout << "compatible: savepoint restores into " << so_path << "\n";
            }
            return 0;
        }
        std::cerr << "INCOMPATIBLE: " << mismatches.size()
                  << " state slot(s) changed shape with no declared version bump in " << so_path
                  << "\n";
        std::cerr << "  op_id  slot                        stored -> declared\n";
        std::cerr << "  -----  --------------------------  ------------------\n";
        for (const auto& m : mismatches) {
            std::cerr << "  " << m.op_id.value() << "      " << m.slot;
            if (m.slot.size() < 26) {
                std::cerr << std::string(26 - m.slot.size(), ' ');
            }
            std::cerr << "  " << std::hex << m.stored_fingerprint << " -> "
                      << m.declared_fingerprint << std::dec << "\n";
        }
        std::cerr << "  remedy: bump SchemaVersionTrait and register a migration\n";
        return 3;
    }
    std::cerr << "INCOMPATIBLE: " << incompat.size() << " state slot(s) have no migration path in "
              << so_path << "\n";
    std::cerr << "  op_id  state_type                  from -> to\n";
    std::cerr << "  -----  --------------------------  ----------\n";
    for (const auto& e : incompat) {
        std::cerr << "  " << e.op_id.value() << "      " << e.state_type;
        if (e.state_type.size() < 26) {
            std::cerr << std::string(26 - e.state_type.size(), ' ');
        }
        std::cerr << "  v" << e.from_version << " -> v" << e.to_version << "\n";
    }
    return 3;
}

std::vector<std::byte> read_file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        throw std::runtime_error("clink check-savepoint: cannot open file: " + path);
    }
    f.seekg(0, std::ios::end);
    const auto size = f.tellg();
    if (size < 0) {
        throw std::runtime_error("clink check-savepoint: stat failed for " + path);
    }
    f.seekg(0, std::ios::beg);
    std::vector<std::byte> bytes(static_cast<std::size_t>(size));
    if (size > 0) {
        f.read(reinterpret_cast<char*>(bytes.data()), size);
        if (!f) {
            throw std::runtime_error("clink check-savepoint: read failed for " + path);
        }
    }
    return bytes;
}

std::size_t count_state_entries(clink::InMemoryStateBackend& backend) {
    // No direct row-count accessor; sum scan() emissions across
    // every operator id that participated in the restore.
    std::size_t total = 0;
    for (auto op : backend.operator_ids()) {
        backend.scan(op, [&](auto, auto) { ++total; });
    }
    return total;
}

}  // namespace

int clink_cmd_check_savepoint(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto file = get_arg(argc, argv, "file");
    if (file.empty()) {
        std::cerr << "clink check-savepoint: --file is required\n\n";
        usage();
        return 1;
    }
    const bool quiet = has_flag(argc, argv, "quiet");
    const auto expected_so = get_arg(argc, argv, "expected");

    try {
        auto bytes = read_file_bytes(file);
        clink::InMemoryStateBackend backend;
        backend.restore(
            clink::Snapshot{.checkpoint_id = clink::CheckpointId{0}, .bytes = std::move(bytes)});
        auto versions = backend.restored_state_versions();
        auto fingerprints = backend.restored_state_fingerprints();
        const auto entry_count = count_state_entries(backend);
        const auto fp_entries = fingerprints.entries();

        if (!quiet) {
            std::cout << "file=" << file << " entries=" << entry_count
                      << " versioned_ops=" << versions.size()
                      << " fingerprinted_slots=" << fp_entries.size() << "\n";
            if (versions.empty()) {
                std::cout << "  (no state version stamps recorded)\n";
            } else {
                std::cout << "  op_id  state_type                  version\n";
                std::cout << "  -----  --------------------------  -------\n";
                for (const auto& e : versions.entries()) {
                    std::cout << "  " << e.op_id.value() << "      " << e.state_type;
                    // Pad state_type to 26 chars for alignment without
                    // dragging iomanip in for one column.
                    if (e.state_type.size() < 26) {
                        std::cout << std::string(26 - e.state_type.size(), ' ');
                    }
                    std::cout << "  " << e.version << "\n";
                }
            }
            if (fp_entries.empty()) {
                std::cout << "  (no shape fingerprints recorded)\n";
            } else {
                std::cout << "  op_id  slot                        fingerprint\n";
                std::cout << "  -----  --------------------------  ----------------\n";
                for (const auto& e : fp_entries) {
                    std::cout << "  " << e.op_id.value() << "      " << e.slot;
                    if (e.slot.size() < 26) {
                        std::cout << std::string(26 - e.slot.size(), ' ');
                    }
                    std::cout << "  " << std::hex << e.fingerprint << std::dec << "\n";
                }
            }
        }

        // Pre-deploy compatibility check against a live job binary.
        if (!expected_so.empty()) {
            return run_compat_check(expected_so, versions.pack(), fingerprints.pack(), quiet);
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink check-savepoint: " << e.what() << "\n";
        return 1;
    }
}
