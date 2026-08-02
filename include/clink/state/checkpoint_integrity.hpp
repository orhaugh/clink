#pragma once

// Checkpoint integrity: versioned metadata, checksums, and the
// incomplete-versus-corrupt distinction that recovery needs to decide
// whether it may fall back.
//
// Why a sidecar rather than an envelope around the payload
// --------------------------------------------------------
// A checkpoint payload written by the in-memory backend family IS an
// Apache Arrow IPC stream, and docs/internals/state-snapshot-format.md
// publishes that as a stable contract: pyarrow, DuckDB, Polars and Spark
// open a `.snap` directly with no clink code involved. Prefixing a magic
// header onto the payload would break every one of those readers. So the
// integrity record lives beside the payload in `<name>.meta`, and the
// payload file stays byte-for-byte the Arrow stream it always was.
//
// The sidecar is also what makes publication atomic at the CHECKPOINT
// level rather than only the file level. Writing order is:
//
//   1. payload -> temp -> fsync -> rename -> `<id>.snap`   (durable)
//   2. sidecar -> temp -> fsync -> rename -> `<id>.meta`   (durable)
//
// so a `.snap` with no `.meta` is definitionally an unfinished checkpoint,
// whatever killed the writer between the two. Recovery treats it as
// incomplete and looks further back, which is exactly right: the
// alternative - loading a payload nobody ever certified - is how a
// half-written checkpoint silently becomes "restored" state.
//
// Status taxonomy (the brief's "clear distinction"):
//
//   Missing      no payload at this id. Not an error; there is simply
//                nothing here. Recovery keeps looking.
//   Incomplete   the checkpoint never finished publishing: sidecar
//                absent, or present and disagreeing with the payload
//                LENGTH. Nothing was lost, because it was never
//                promised. Fall back.
//   Corrupt      the checkpoint finished publishing and the bytes have
//                since changed: length agrees, checksum does not. Storage
//                lied to us. Fall back, loudly.
//   Unsupported  the sidecar is from a newer format version than this
//                binary understands. Do NOT guess. Refuse and report.
//   Valid        length and checksum both agree.

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/state/durable_file_write.hpp"

namespace clink::state {

// CRC-32C (Castagnoli, polynomial 0x1EDC6F41 reflected as 0x82F63B78).
// Chosen over a plain sum or FNV because burst errors - the truncation and
// block-level tearing that storage actually produces - are what this has to
// catch. Software table-driven: no SSE4.2 / ARMv8-CRC assumption, so the
// checksum a macOS host writes is the checksum a Debian container verifies.
namespace detail {

using Crc32cTable = std::array<std::uint32_t, 256>;

inline const Crc32cTable& crc32c_table() {
    static const Crc32cTable table = [] {
        Crc32cTable t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t crc = i;
            for (int k = 0; k < 8; ++k) {
                crc = (crc & 1U) != 0U ? (crc >> 1U) ^ 0x82F63B78U : crc >> 1U;
            }
            t[i] = crc;
        }
        return t;
    }();
    return table;
}

}  // namespace detail

[[nodiscard]] inline std::uint32_t crc32c(const void* data,
                                          std::size_t size,
                                          std::uint32_t seed = 0) noexcept {
    const auto& table = detail::crc32c_table();
    const auto* p = static_cast<const unsigned char*>(data);
    std::uint32_t crc = ~seed;
    for (std::size_t i = 0; i < size; ++i) {
        crc = table[(crc ^ p[i]) & 0xFFU] ^ (crc >> 8U);
    }
    return ~crc;
}

[[nodiscard]] inline std::uint32_t crc32c(std::string_view s) noexcept {
    return crc32c(s.data(), s.size());
}

// Sidecar format version. Bump only for a change a v1 reader could not
// safely ignore; new trailing key=value lines do not need a bump because
// an unknown key is skipped (see parse()).
inline constexpr std::uint32_t kCheckpointMetaVersion = 1;

enum class CheckpointStatus : std::uint8_t {
    Valid,
    Missing,
    Incomplete,
    Corrupt,
    Unsupported,
};

[[nodiscard]] inline std::string_view to_string(CheckpointStatus s) noexcept {
    switch (s) {
        case CheckpointStatus::Valid:
            return "valid";
        case CheckpointStatus::Missing:
            return "missing";
        case CheckpointStatus::Incomplete:
            return "incomplete";
        case CheckpointStatus::Corrupt:
            return "corrupt";
        case CheckpointStatus::Unsupported:
            return "unsupported";
    }
    return "?";
}

// The sidecar record. Line-oriented `key=value` text, not JSON: it is
// written on the checkpoint path where a parser dependency and an
// allocation-heavy serialiser are both unwelcome, and a human debugging a
// failed restore can read it with cat.
struct CheckpointMeta {
    std::uint32_t version{kCheckpointMetaVersion};
    std::uint64_t checkpoint_id{0};
    std::uint64_t payload_bytes{0};
    std::uint32_t payload_crc32c{0};

    [[nodiscard]] std::string serialise() const {
        return "clink_checkpoint_meta=" + std::to_string(version) +
               "\ncheckpoint_id=" + std::to_string(checkpoint_id) +
               "\npayload_bytes=" + std::to_string(payload_bytes) +
               "\npayload_crc32c=" + std::to_string(payload_crc32c) + "\n";
    }

    // Returns false when the text is not a recognisable sidecar at all
    // (missing the leading version key, or unparseable numbers). Unknown
    // trailing keys are ignored so a v1 reader survives additive fields.
    [[nodiscard]] static bool parse(std::string_view text, CheckpointMeta& out) {
        bool saw_version = false;
        std::size_t pos = 0;
        while (pos < text.size()) {
            auto eol = text.find('\n', pos);
            if (eol == std::string_view::npos) {
                eol = text.size();
            }
            const auto line = text.substr(pos, eol - pos);
            pos = eol + 1;
            const auto eq = line.find('=');
            if (eq == std::string_view::npos) {
                continue;
            }
            const auto key = line.substr(0, eq);
            const auto value = std::string(line.substr(eq + 1));
            try {
                if (key == "clink_checkpoint_meta") {
                    out.version = static_cast<std::uint32_t>(std::stoul(value));
                    saw_version = true;
                } else if (key == "checkpoint_id") {
                    out.checkpoint_id = std::stoull(value);
                } else if (key == "payload_bytes") {
                    out.payload_bytes = std::stoull(value);
                } else if (key == "payload_crc32c") {
                    out.payload_crc32c = static_cast<std::uint32_t>(std::stoul(value));
                }
            } catch (const std::exception&) {
                return false;
            }
        }
        return saw_version;
    }
};

// Result of verifying one checkpoint on disk.
struct VerifyResult {
    CheckpointStatus status{CheckpointStatus::Missing};
    std::string detail;  // human-readable "why", for the log and the error

    [[nodiscard]] bool ok() const noexcept { return status == CheckpointStatus::Valid; }
};

[[nodiscard]] inline std::filesystem::path meta_path_for(const std::filesystem::path& payload) {
    return payload.string() + ".meta";
}

// Write the sidecar for an already-durable payload. Call AFTER the payload
// rename: this is the publication point, and its ordering is the whole
// reason an interrupted checkpoint reads as incomplete rather than valid.
inline void write_checkpoint_meta(const std::filesystem::path& payload_path,
                                  std::uint64_t checkpoint_id,
                                  const std::byte* payload,
                                  std::size_t size) {
    const CheckpointMeta meta{.version = kCheckpointMetaVersion,
                              .checkpoint_id = checkpoint_id,
                              .payload_bytes = size,
                              .payload_crc32c = crc32c(payload, size)};
    detail::write_string_fsync_rename(meta_path_for(payload_path), meta.serialise());
}

// Verify one checkpoint payload against its sidecar. Reads the payload to
// checksum it, so this is a recovery-time cost, not a per-checkpoint one.
[[nodiscard]] inline VerifyResult verify_checkpoint(const std::filesystem::path& payload_path) {
    std::error_code ec;
    if (!std::filesystem::exists(payload_path, ec)) {
        return {CheckpointStatus::Missing, "no payload at " + payload_path.string()};
    }
    const auto meta_path = meta_path_for(payload_path);
    if (!std::filesystem::exists(meta_path, ec)) {
        // Publication order guarantees the sidecar lands last, so its
        // absence means the writer died before finishing. It is also what a
        // pre-integrity checkpoint directory looks like; both cases are
        // handled the same way, and correctly, by falling back.
        return {CheckpointStatus::Incomplete,
                "no integrity sidecar beside " + payload_path.string() +
                    " - the checkpoint was never published (or predates integrity metadata)"};
    }

    std::string meta_text;
    {
        std::ifstream in(meta_path, std::ios::binary);
        if (!in) {
            return {CheckpointStatus::Corrupt, "cannot read sidecar " + meta_path.string()};
        }
        meta_text.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    CheckpointMeta meta;
    if (!CheckpointMeta::parse(meta_text, meta)) {
        return {CheckpointStatus::Corrupt, "unparseable sidecar " + meta_path.string()};
    }
    if (meta.version > kCheckpointMetaVersion) {
        return {CheckpointStatus::Unsupported,
                "sidecar " + meta_path.string() + " is format version " +
                    std::to_string(meta.version) + "; this build understands up to " +
                    std::to_string(kCheckpointMetaVersion)};
    }

    const auto actual_size = std::filesystem::file_size(payload_path, ec);
    if (ec) {
        return {CheckpointStatus::Corrupt,
                "cannot size " + payload_path.string() + ": " + ec.message()};
    }
    if (actual_size != meta.payload_bytes) {
        // Length disagreement is the short-write signature. The checkpoint
        // was interrupted, not damaged after the fact - a distinction that
        // matters because one is expected on any crash and the other means
        // the storage layer is misbehaving.
        return {CheckpointStatus::Incomplete,
                payload_path.string() + " is " + std::to_string(actual_size) + " bytes, sidecar " +
                    "declares " + std::to_string(meta.payload_bytes)};
    }

    std::vector<char> buf(static_cast<std::size_t>(actual_size));
    if (actual_size > 0) {
        std::ifstream in(payload_path, std::ios::binary);
        if (!in) {
            return {CheckpointStatus::Corrupt, "cannot read " + payload_path.string()};
        }
        in.read(buf.data(), static_cast<std::streamsize>(actual_size));
        if (in.gcount() != static_cast<std::streamsize>(actual_size)) {
            return {CheckpointStatus::Incomplete, "short read of " + payload_path.string()};
        }
    }
    const auto actual_crc = crc32c(buf.data(), buf.size());
    if (actual_crc != meta.payload_crc32c) {
        return {CheckpointStatus::Corrupt,
                payload_path.string() + " checksum mismatch: computed " +
                    std::to_string(actual_crc) + ", sidecar declares " +
                    std::to_string(meta.payload_crc32c)};
    }
    return {CheckpointStatus::Valid, {}};
}

// Escape hatch for checkpoint directories written before integrity
// metadata existed (clink <= 0.6.0). Those payloads are perfectly good
// Arrow, they simply have no sidecar, so a strict reader classifies them
// Incomplete and refuses. Setting CLINK_ALLOW_UNVERIFIED_CHECKPOINTS=1
// downgrades EXACTLY that case - a missing sidecar - to a pass.
//
// It deliberately does not extend to Corrupt, Unsupported, or a length
// mismatch: those are damage or a format this binary cannot read, and no
// environment variable should make loading them look acceptable. The
// supported migration is `clink checkpoint-verify --repair`, which mints
// sidecars for an existing directory once, on purpose, with a record of
// having done so. Reported by `clink --capabilities` because a cluster
// running with this on has a weaker recovery guarantee than one without.
[[nodiscard]] inline bool unverified_checkpoints_allowed() {
    const char* p = std::getenv("CLINK_ALLOW_UNVERIFIED_CHECKPOINTS");
    return p != nullptr && std::string_view(p) != "0" && std::string_view(p) != "false" &&
           *p != '\0';
}

// True when `verdict` is the specific "legacy directory, no sidecar" case
// AND the operator has opted into tolerating it.
[[nodiscard]] inline bool unverified_checkpoints_allowed(const VerifyResult& verdict) {
    return verdict.status == CheckpointStatus::Incomplete &&
           verdict.detail.find("no integrity sidecar") != std::string::npos &&
           unverified_checkpoints_allowed();
}

// Thrown when a restore is asked for a checkpoint that exists but cannot be
// trusted. Distinct from "no checkpoint here", which is not an error.
class CheckpointIntegrityError : public std::runtime_error {
public:
    CheckpointIntegrityError(CheckpointStatus status, const std::string& detail)
        : std::runtime_error("checkpoint " + std::string(to_string(status)) + ": " + detail),
          status_(status) {}

    [[nodiscard]] CheckpointStatus status() const noexcept { return status_; }

private:
    CheckpointStatus status_;
};

}  // namespace clink::state
