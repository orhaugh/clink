#pragma once

// Exactly-once verification of the 2PC job's committed output.
//
// The 2PC job emits "record-0" through "record-(N-1)", once each, and
// checkpoints its offset; the sink commits by atomic rename from staging/
// into committed/. Only committed/ is visible downstream - a file left in
// staging/ is a transaction nobody ever agreed to - so that is what gets
// read. Duplicates and losses are reported SEPARATELY, because they are
// different failures: a duplicate means the recovery replayed work already
// published (an at-least-once leak), a loss means it published nothing for
// records the source had already passed (data loss). A single "mismatch"
// count would hide which.
//
// Shared between the fault-recovery suite and the coordinator-HA compound
// failure test so both compare output against the same contract.

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace clink::itest {

// Every line under <out>/committed/, which is the output an external
// consumer sees.
inline std::vector<std::string> committed_records(const std::filesystem::path& out_dir) {
    std::vector<std::string> lines;
    const auto dir = out_dir / "committed";
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return lines;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

struct OutputVerdict {
    std::vector<std::string> duplicated;  // published more than once
    std::vector<std::string> missing;     // never published
    std::vector<std::string> unexpected;  // published but never emitted
    std::size_t total_lines{0};

    [[nodiscard]] bool clean() const {
        return duplicated.empty() && missing.empty() && unexpected.empty();
    }
};

// Compare the committed output against "record-0".."record-(total-1)",
// each exactly once.
inline OutputVerdict verify_exactly_once(const std::filesystem::path& out_dir, int total) {
    OutputVerdict v;
    std::map<std::string, int> seen;
    for (const auto& line : committed_records(out_dir)) {
        ++seen[line];
        ++v.total_lines;
    }
    for (int i = 0; i < total; ++i) {
        const auto want = "record-" + std::to_string(i);
        const auto it = seen.find(want);
        if (it == seen.end()) {
            v.missing.push_back(want);
        } else if (it->second > 1) {
            v.duplicated.push_back(want + " x" + std::to_string(it->second));
        }
        seen.erase(want);
    }
    for (const auto& [line, count] : seen) {
        v.unexpected.push_back(line + " x" + std::to_string(count));
    }
    return v;
}

inline std::string describe(const OutputVerdict& v) {
    std::ostringstream os;
    os << v.total_lines << " committed lines";
    const auto list = [&os](const char* label, const std::vector<std::string>& xs) {
        if (xs.empty()) {
            return;
        }
        os << "; " << xs.size() << " " << label << ": ";
        for (std::size_t i = 0; i < xs.size() && i < 8; ++i) {
            os << (i ? ", " : "") << xs[i];
        }
        if (xs.size() > 8) {
            os << ", ... (+" << (xs.size() - 8) << ")";
        }
    };
    list("DUPLICATED", v.duplicated);
    list("MISSING", v.missing);
    list("UNEXPECTED", v.unexpected);
    return os.str();
}

}  // namespace clink::itest
