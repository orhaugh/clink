#pragma once

// FileSink2PC<T> - a two-phase-commit file sink.
//
// Rides the generic CommittingSink base: this file supplies only the verbs
// (stage, prepare, commit, abort) and the base owns the 2PC choreography,
// the per-checkpoint handle persistence, and recover-and-re-commit at open.
//
// Layout under `output_dir`:
//   staging/   pre-committed transaction files (one per (subtask, ckpt))
//   committed/ finalized output files; an atomic rename from staging
//              is the commit step
//
// The committable is the staging file path. commit() derives the committed
// path from its basename (staging/sub<N>-<ckpt>.dat -> committed/sub<N>-<ckpt>.dat),
// so no checkpoint id needs to travel in the handle.
//
// Lifecycle on a clean run (base -> verb):
//   open()           - on_open() creates staging/ and committed/ and bridges any
//                      pre-framework handle; then the base recovers any handle
//                      this framework left prepared-but-uncommitted.
//   on_data(batch)   - write(batch) appends records to staging/sub<N>-pending.tmp.
//   on_barrier(b)    - prepare_commit(b) closes the pending file, renames it to
//                      staging/sub<N>-<b.id>.dat, and returns that path; the base
//                      persists it under "_xo_pending_sub<N>_<b.id>".
//   on_commit(id)    - commit(handle) atomic-renames staging -> committed; the
//                      base erases the handle.
//   flush()/close()  - best-effort; an in-progress staging file is effectively
//                      abandoned (it has no checkpoint id and so no persisted
//                      handle - recovery will not commit it).
//
// Crash semantics: any pre-committed file in staging/ whose persisted handle
// matches a COMPLETED-N marker on the JM gets committed at next restart. Files
// for checkpoints that did not complete are left in staging/ until the user
// cleans them up; recovery does not auto-abort because a half-written file is
// ambiguous (commit-failed vs. crashed mid-write).

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "clink/connectors/committing_sink.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class FileSink2PC final : public CommittingSink<T, std::string> {
public:
    FileSink2PC(std::filesystem::path output_dir,
                TextFormat<T> format,
                std::uint32_t subtask_idx,
                std::string name = "file_2pc_sink")
        : CommittingSink<T, std::string>(subtask_idx),
          output_dir_(std::move(output_dir)),
          format_(std::move(format)),
          subtask_idx_(subtask_idx),
          name_(std::move(name)) {
        if (!format_.encode) {
            throw std::invalid_argument("FileSink2PC: TextFormat::encode is required");
        }
    }

    void on_open() override {
        std::error_code ec;
        std::filesystem::create_directories(staging_dir(), ec);
        std::filesystem::create_directories(committed_dir(), ec);
        // Bridge any handle left by the pre-framework FileSink2PC (raw
        // "_2pc_pending_" key). New handles use the operator-state path.
        this->recover_legacy_handles("_2pc_pending_");
    }

    void write(const Batch<T>& batch) override {
        ensure_pending_open_();
        for (const auto& r : batch) {
            pending_ << format_.encode(r.value()) << '\n';
        }
    }

    // Close the in-flight pending file and rename it to its checkpoint-tagged
    // staging path. Always returns a handle (even for an empty interval) so
    // every (subtask, ckpt) yields a uniform path.
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        ensure_pending_open_();
        pending_.flush();
        pending_.close();
        const auto target =
            staging_dir() / (sub_prefix_() + "-" + std::to_string(checkpoint_id) + ".dat");
        std::error_code ec;
        std::filesystem::rename(pending_path_, target, ec);
        if (ec) {
            throw std::runtime_error("FileSink2PC::prepare_commit: rename to staging failed: " +
                                     ec.message());
        }
        pending_path_.clear();
        return target.string();
    }

    // Atomic-rename staging -> committed. Idempotent: a missing staging file
    // (already committed) is a no-op.
    bool commit(const std::string& staging_path) override {
        const std::filesystem::path src{staging_path};
        const std::filesystem::path dst = committed_dir() / src.filename();
        std::error_code ec;
        std::filesystem::rename(src, dst, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("FileSink2PC::commit: rename to committed failed: " +
                                     ec.message());
        }
        return true;
    }

    // Roll back: delete the staging file. Idempotent (ENOENT tolerated).
    void abort(const std::string& staging_path) override {
        std::error_code ec;
        std::filesystem::remove(std::filesystem::path{staging_path}, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("FileSink2PC::abort: remove of staging file failed: " +
                                     ec.message());
        }
    }

    std::string serialize(const std::string& staging_path) const override { return staging_path; }
    std::string deserialize(std::string_view bytes) const override { return std::string(bytes); }

    void flush() override {
        if (pending_.is_open()) {
            pending_.flush();
            pending_.close();
        }
    }

    void close() override {
        if (pending_.is_open()) {
            pending_.close();
        }
    }

    std::string name() const override { return name_; }

private:
    std::filesystem::path staging_dir() const { return output_dir_ / "staging"; }
    std::filesystem::path committed_dir() const { return output_dir_ / "committed"; }
    std::string sub_prefix_() const { return "sub" + std::to_string(subtask_idx_); }

    void ensure_pending_open_() {
        if (pending_.is_open())
            return;
        pending_path_ = staging_dir() / (sub_prefix_() + "-pending.tmp");
        pending_.open(pending_path_, std::ios::out | std::ios::trunc);
        if (!pending_.is_open()) {
            throw std::runtime_error("FileSink2PC: cannot open " + pending_path_.string());
        }
    }

    std::filesystem::path output_dir_;
    TextFormat<T> format_;
    std::uint32_t subtask_idx_;
    std::string name_;
    std::ofstream pending_;
    std::filesystem::path pending_path_;
};

}  // namespace clink
