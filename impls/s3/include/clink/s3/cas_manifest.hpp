#pragma once

// CasManifest - the per-checkpoint manifest of a content-addressed checkpoint
// (DISAGG-6). One manifest object names every file in a RocksDB checkpoint dir
// by (logical filename -> content hash, size); the bytes themselves live once
// each under objects/<hash> in the object store. Reconstructing a checkpoint is
// "read the manifest, fetch each referenced object into <filename>".
//
// Format: a small, versioned, line-oriented text blob - NOT JSON. The codebase
// has a JSON *writer* (clink/http/json_writer.hpp) but deliberately no parser;
// a content-addressed restore is correctness-critical, so a hand-rolled JSON
// parser is the wrong risk. This format parses with a trivial, unambiguous
// scanner and is still externally greppable:
//
//   clink-cas-manifest-v1
//   id <checkpoint-id>
//   subtask <subtask-index>
//   entries <count>
//   <64-hex-hash> <size-bytes> <filename>
//   ... one line per file ...
//
// Entry lines lead with the fixed-shape tokens (64-hex hash, decimal size) so
// the trailing filename is just the rest of the line - no escaping, no
// delimiter ambiguity (RocksDB names contain no newlines). encode() sorts
// entries by name so the same checkpoint dir always serialises to byte-
// identical bytes (idempotent re-write, and a stable manifest hash if ever
// needed). decode() is tolerant: a wrong format tag or malformed line yields
// std::nullopt rather than a partial parse.

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "clink/core/sha256.hpp"

namespace clink::s3 {

struct CasManifestEntry {
    std::string name;  // RocksDB-relative filename, e.g. "000123.sst", "CURRENT"
    std::string hash;  // 64-char lowercase hex SHA-256 of the file bytes
    std::uint64_t size{0};
};

struct CasManifest {
    static constexpr const char* kFormatTag = "clink-cas-manifest-v1";

    std::uint64_t checkpoint_id{0};
    std::uint32_t subtask{0};
    std::vector<CasManifestEntry> entries;

    [[nodiscard]] std::string encode() const {
        std::vector<CasManifestEntry> sorted = entries;
        std::sort(
            sorted.begin(), sorted.end(), [](const CasManifestEntry& a, const CasManifestEntry& b) {
                return a.name < b.name;
            });
        std::string out;
        out += kFormatTag;
        out += '\n';
        out += "id " + std::to_string(checkpoint_id) + '\n';
        out += "subtask " + std::to_string(subtask) + '\n';
        out += "entries " + std::to_string(sorted.size()) + '\n';
        for (const auto& e : sorted) {
            out += e.hash + ' ' + std::to_string(e.size) + ' ' + e.name + '\n';
        }
        return out;
    }

    static std::optional<CasManifest> decode(const std::string& text) {
        std::vector<std::string> lines;
        for (std::size_t start = 0; start <= text.size();) {
            const auto nl = text.find('\n', start);
            if (nl == std::string::npos) {
                if (start < text.size()) {
                    lines.push_back(text.substr(start));
                }
                break;
            }
            lines.push_back(text.substr(start, nl - start));
            start = nl + 1;
        }
        if (lines.size() < 4 || lines[0] != kFormatTag) {
            return std::nullopt;
        }
        CasManifest m;
        std::uint64_t declared_entries = 0;
        if (!parse_kv_u64_(lines[1], "id", m.checkpoint_id) ||
            !parse_kv_u32_(lines[2], "subtask", m.subtask) ||
            !parse_kv_u64_(lines[3], "entries", declared_entries)) {
            return std::nullopt;
        }
        for (std::size_t i = 4; i < lines.size(); ++i) {
            if (lines[i].empty()) {
                continue;  // tolerate a trailing blank line
            }
            const auto sp1 = lines[i].find(' ');
            if (sp1 == std::string::npos) {
                return std::nullopt;
            }
            const auto sp2 = lines[i].find(' ', sp1 + 1);
            if (sp2 == std::string::npos) {
                return std::nullopt;
            }
            CasManifestEntry e;
            e.hash = lines[i].substr(0, sp1);
            if (!Sha256::is_hex_digest(e.hash)) {
                return std::nullopt;
            }
            const std::string size_str = lines[i].substr(sp1 + 1, sp2 - sp1 - 1);
            if (size_str.empty() || size_str.find_first_not_of("0123456789") != std::string::npos) {
                return std::nullopt;
            }
            try {
                e.size = std::stoull(size_str);
            } catch (...) {
                return std::nullopt;
            }
            e.name = lines[i].substr(sp2 + 1);
            // A name must be a single safe path component. fetch reconstructs
            // <staging>/<name>, so an unconstrained name from a crafted/corrupt
            // manifest ("../x", "/abs", "sub/x") could write OUTSIDE the staging
            // dir on restore. RocksDB checkpoint filenames are always flat
            // single components, so this rejects only malformed manifests.
            if (e.name.empty() || e.name == "." || e.name == ".." ||
                e.name.find('/') != std::string::npos || e.name.find('\\') != std::string::npos) {
                return std::nullopt;
            }
            m.entries.push_back(std::move(e));
        }
        if (m.entries.size() != declared_entries) {
            return std::nullopt;  // count mismatch: truncated / corrupt manifest
        }
        return m;
    }

private:
    static bool parse_kv_u64_(const std::string& line, const std::string& key, std::uint64_t& out) {
        const std::string prefix = key + " ";
        if (line.rfind(prefix, 0) != 0) {
            return false;
        }
        const std::string v = line.substr(prefix.size());
        if (v.empty() || v.find_first_not_of("0123456789") != std::string::npos) {
            return false;
        }
        try {
            out = std::stoull(v);
        } catch (...) {
            return false;
        }
        return true;
    }

    static bool parse_kv_u32_(const std::string& line, const std::string& key, std::uint32_t& out) {
        std::uint64_t v = 0;
        if (!parse_kv_u64_(line, key, v) || v > 0xffffffffULL) {
            return false;
        }
        out = static_cast<std::uint32_t>(v);
        return true;
    }
};

}  // namespace clink::s3
