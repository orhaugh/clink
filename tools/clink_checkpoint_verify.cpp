// `clink checkpoint-verify` - inspect a checkpoint directory's integrity,
// and (with --repair) mint the integrity sidecars for a directory that
// predates them.
//
// Two jobs. Operationally it answers "can this job actually recover, and
// from which checkpoint" WITHOUT starting the job - the alternative being
// to find out during an incident. Migrationally it is the supported route
// for a checkpoint directory written by clink <= 0.6.0, which has valid
// payloads and no sidecars: --repair computes each payload's checksum as
// it stands NOW and records it. That certifies "these bytes have not
// changed since the repair", which is strictly weaker than "these bytes
// are what the writer intended" - the tool says so, because a repair that
// quietly presented itself as full verification would be worse than no
// repair at all.

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "clink/state/checkpoint_integrity.hpp"

namespace {

struct Options {
    std::filesystem::path dir;
    bool repair{false};
    bool json{false};
    bool recursive{true};
};

void usage() {
    std::cerr << "Usage: clink checkpoint-verify --dir <path> [--repair] [--json]\n"
              << "\n"
              << "  --dir <path>   Checkpoint directory (searched recursively for\n"
              << "                 checkpoint-<id>.snap files).\n"
              << "  --repair       Write an integrity sidecar for any payload that has none.\n"
              << "                 Certifies the bytes AS THEY ARE NOW - it cannot tell you\n"
              << "                 they are what the writer intended. Never touches a payload\n"
              << "                 whose existing sidecar disagrees with it.\n"
              << "  --json         Machine-readable output.\n"
              << "\n"
              << "Exit status: 0 all payloads valid, 1 at least one is not, 2 bad usage.\n";
}

struct Entry {
    std::filesystem::path path;
    clink::state::VerifyResult verdict;
    bool repaired{false};
};

}  // namespace

int clink_cmd_checkpoint_verify(int argc, char** argv) {
    Options opts;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--help" || a == "-h") {
            usage();
            return 0;
        }
        if (a == "--repair") {
            opts.repair = true;
        } else if (a == "--json") {
            opts.json = true;
        } else if (a == "--dir" && i + 1 < argc) {
            opts.dir = argv[++i];
        } else if (a.rfind("--dir=", 0) == 0) {
            opts.dir = a.substr(std::strlen("--dir="));
        } else {
            std::cerr << "checkpoint-verify: unknown argument '" << a << "'\n\n";
            usage();
            return 2;
        }
    }
    if (opts.dir.empty()) {
        std::cerr << "checkpoint-verify: --dir is required\n\n";
        usage();
        return 2;
    }
    std::error_code ec;
    if (!std::filesystem::exists(opts.dir, ec)) {
        std::cerr << "checkpoint-verify: no such directory: " << opts.dir << "\n";
        return 2;
    }

    std::vector<std::filesystem::path> payloads;
    for (const auto& e : std::filesystem::recursive_directory_iterator(opts.dir, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("checkpoint-", 0) == 0 && name.size() > 5 &&
            name.compare(name.size() - 5, 5, ".snap") == 0) {
            payloads.push_back(e.path());
        }
    }
    std::sort(payloads.begin(), payloads.end());

    std::vector<Entry> entries;
    entries.reserve(payloads.size());
    for (const auto& p : payloads) {
        Entry entry{.path = p, .verdict = clink::state::verify_checkpoint(p)};
        const bool missing_sidecar =
            entry.verdict.status == clink::state::CheckpointStatus::Incomplete &&
            entry.verdict.detail.find("no integrity sidecar") != std::string::npos;
        if (opts.repair && missing_sidecar) {
            std::ifstream in(p, std::ios::binary);
            const std::string bytes{std::istreambuf_iterator<char>(in),
                                    std::istreambuf_iterator<char>()};
            // The checkpoint id is in the filename; it is what the sidecar
            // has to agree with for restore to address it.
            const auto name = p.filename().string();
            const auto digits = name.substr(std::strlen("checkpoint-"),
                                            name.size() - std::strlen("checkpoint-") - 5);
            std::uint64_t id = 0;
            try {
                id = std::stoull(digits);
            } catch (const std::exception&) {
                entry.verdict = {clink::state::CheckpointStatus::Corrupt,
                                 "cannot parse a checkpoint id out of " + name};
                entries.push_back(std::move(entry));
                continue;
            }
            try {
                clink::state::write_checkpoint_meta(
                    p, id, reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
                entry.repaired = true;
                entry.verdict = clink::state::verify_checkpoint(p);
            } catch (const std::exception& e) {
                entry.verdict = {clink::state::CheckpointStatus::Corrupt,
                                 std::string("repair failed: ") + e.what()};
            }
        }
        entries.push_back(std::move(entry));
    }

    std::size_t bad = 0;
    for (const auto& e : entries) {
        if (!e.verdict.ok()) {
            ++bad;
        }
    }

    if (opts.json) {
        std::cout << "{\"checked\":" << entries.size() << ",\"invalid\":" << bad
                  << ",\"entries\":[";
        for (std::size_t i = 0; i < entries.size(); ++i) {
            const auto& e = entries[i];
            if (i > 0) {
                std::cout << ",";
            }
            std::cout << "{\"path\":\"" << e.path.string() << "\",\"status\":\""
                      << clink::state::to_string(e.verdict.status)
                      << "\",\"repaired\":" << (e.repaired ? "true" : "false") << ",\"detail\":\""
                      << e.verdict.detail << "\"}";
        }
        std::cout << "]}\n";
    } else {
        if (entries.empty()) {
            std::cout << "no checkpoint payloads found under " << opts.dir << "\n";
        }
        for (const auto& e : entries) {
            std::cout << (e.verdict.ok() ? "  ok        " : "  NOT OK    ")
                      << clink::state::to_string(e.verdict.status) << "  " << e.path.string();
            if (e.repaired) {
                std::cout << "   [sidecar minted by --repair: certifies the bytes as they "
                             "stand now, not as the writer intended them]";
            }
            std::cout << "\n";
            if (!e.verdict.ok()) {
                std::cout << "              " << e.verdict.detail << "\n";
            }
        }
        std::cout << "\n" << entries.size() << " checked, " << bad << " not usable for restore\n";
    }
    return bad == 0 ? 0 : 1;
}
