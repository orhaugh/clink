#pragma once

// FileSink2PC<T> - a two-phase-commit file sink.
//
// Layout under `output_dir`:
//   staging/   pre-committed transaction files (one per (subtask, ckpt))
//   committed/ finalized output files; an atomic rename from staging
//              is the commit step
//
// Lifecycle on a clean run:
//   open()           - create staging/ and committed/, run recovery
//                      (commits any leftover pre-committed files whose
//                       checkpoint_id is in state).
//   on_data(batch)   - append records to staging/sub<N>-pending.tmp.
//   on_barrier(b)    - close current file, rename to
//                      staging/sub<N>-<b.id>.dat, store path in state
//                      under "_2pc_pending_<b.id>".
//   on_commit(id)    - atomic-rename staging/sub<N>-<id>.dat ->
//                      committed/sub<N>-<id>.dat, erase state key.
//   flush()/close()  - best-effort; an in-progress staging file is
//                      effectively abandoned (it has no checkpoint_id
//                      and so no state-tracked path - recovery will
//                      not commit it).
//
// Crash semantics: any pre-committed file in staging/ whose state-
// tracked checkpoint_id matches a COMPLETED-N marker on the JM gets
// committed at next restart. Files for checkpoints that didn't
// complete are left in staging/ until the user cleans them up;
// recovery does not auto-abort because a half-written file is
// ambiguous (commit-failed vs. crashed mid-write).

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

template <typename T>
class FileSink2PC final : public Sink<T> {
public:
    FileSink2PC(std::filesystem::path output_dir,
                TextFormat<T> format,
                std::uint32_t subtask_idx,
                std::string name = "file_2pc_sink")
        : output_dir_(std::move(output_dir)),
          format_(std::move(format)),
          subtask_idx_(subtask_idx),
          name_(std::move(name)) {
        if (!format_.encode) {
            throw std::invalid_argument("FileSink2PC: TextFormat::encode is required");
        }
    }

    void open() override {
        std::error_code ec;
        std::filesystem::create_directories(staging_dir(), ec);
        std::filesystem::create_directories(committed_dir(), ec);
        recover_pending_();
    }

    void on_data(const Batch<T>& batch) override {
        ensure_pending_open_();
        for (const auto& r : batch) {
            pending_ << format_.encode(r.value()) << '\n';
        }
    }

    void on_barrier(CheckpointBarrier b) override {
        const auto ckpt = b.id().value();
        // Close out the in-flight pending file (if any) and rename it to
        // its checkpoint-tagged staging path. If no records have flowed
        // since the last barrier, create an empty staging file anyway so
        // recovery has a uniform path-per-(sub,ckpt) lookup.
        ensure_pending_open_();
        pending_.flush();
        pending_.close();
        const auto target = staging_dir() / (sub_prefix_() + "-" + std::to_string(ckpt) + ".dat");
        std::error_code ec;
        std::filesystem::rename(pending_path_, target, ec);
        if (ec) {
            throw std::runtime_error("FileSink2PC::on_barrier: rename to staging failed: " +
                                     ec.message());
        }
        pending_path_.clear();
        write_pending_state_(ckpt, target.string());
    }

    void on_commit(std::uint64_t checkpoint_id) override {
        const auto key = state_key_(checkpoint_id);
        auto state = state_backend_();
        if (state == nullptr)
            return;
        auto stored = state->get(this->id(), key);
        if (!stored.has_value())
            return;  // already committed (idempotent)
        const std::string staging_path(reinterpret_cast<const char*>(stored->data()),
                                       stored->size());
        const std::filesystem::path src{staging_path};
        const std::filesystem::path dst =
            committed_dir() / (sub_prefix_() + "-" + std::to_string(checkpoint_id) + ".dat");
        std::error_code ec;
        std::filesystem::rename(src, dst, ec);
        // ENOENT here means the staging file is already gone (a previous
        // commit + state-clear must have happened post-snapshot but
        // pre-checkpoint-completion). Treat as no-op.
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("FileSink2PC::on_commit: rename to committed failed: " +
                                     ec.message());
        }
        state->erase(this->id(), key);
    }

    // Abort the pre-committed file for this checkpoint.
    // Mirrors on_commit but rolls back instead of finalising: delete
    // the staging file and clear the state key. Idempotent - a
    // second on_abort for the same id is a no-op.
    void on_abort(std::uint64_t checkpoint_id) override {
        const auto key = state_key_(checkpoint_id);
        auto state = state_backend_();
        if (state == nullptr)
            return;
        auto stored = state->get(this->id(), key);
        if (!stored.has_value())
            return;
        const std::string staging_path(reinterpret_cast<const char*>(stored->data()),
                                       stored->size());
        std::error_code ec;
        std::filesystem::remove(std::filesystem::path{staging_path}, ec);
        // ENOENT is fine - the file is gone; we still want to clear
        // the state key so recovery doesn't try to commit it.
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("FileSink2PC::on_abort: remove of staging file failed: " +
                                     ec.message());
        }
        state->erase(this->id(), key);
    }

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
    std::string state_key_(std::uint64_t ckpt) const {
        return "_2pc_pending_" + sub_prefix_() + "_" + std::to_string(ckpt);
    }

    void ensure_pending_open_() {
        if (pending_.is_open())
            return;
        pending_path_ = staging_dir() / (sub_prefix_() + "-pending.tmp");
        pending_.open(pending_path_, std::ios::out | std::ios::trunc);
        if (!pending_.is_open()) {
            throw std::runtime_error("FileSink2PC: cannot open " + pending_path_.string());
        }
    }

    void write_pending_state_(std::uint64_t ckpt, const std::string& staging_path) {
        auto state = state_backend_();
        if (state == nullptr)
            return;
        state->put(this->id(),
                   state_key_(ckpt),
                   std::string_view{staging_path.data(), staging_path.size()});
    }

    // Recovery: walk state for "_2pc_pending_<sub>_*" keys; commit each.
    // Backend doesn't expose a prefix scan, so we use scan() and filter.
    void recover_pending_() {
        auto state = state_backend_();
        if (state == nullptr)
            return;
        const std::string prefix = "_2pc_pending_" + sub_prefix_() + "_";
        std::vector<std::pair<std::string, std::string>> to_commit;
        state->scan(this->id(), [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            const std::string key{k};
            if (key.rfind(prefix, 0) != 0)
                return;
            to_commit.emplace_back(key, std::string{v});
        });
        for (const auto& [key, staging_path] : to_commit) {
            // Parse checkpoint id from the key tail.
            std::uint64_t ckpt = 0;
            try {
                ckpt = std::stoull(key.substr(prefix.size()));
            } catch (...) {
                continue;
            }
            const std::filesystem::path src{staging_path};
            const std::filesystem::path dst =
                committed_dir() / (sub_prefix_() + "-" + std::to_string(ckpt) + ".dat");
            std::error_code ec;
            std::filesystem::rename(src, dst, ec);
            // If src is already missing, the previous run committed but
            // crashed before clearing state. Erase the stale key.
            if (ec && ec != std::errc::no_such_file_or_directory) {
                throw std::runtime_error("FileSink2PC::recover: rename failed for " + src.string() +
                                         ": " + ec.message());
            }
            state->erase(this->id(), key);
        }
    }

    StateBackend* state_backend_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->state_backend() : nullptr;
    }

    std::filesystem::path output_dir_;
    TextFormat<T> format_;
    std::uint32_t subtask_idx_;
    std::string name_;
    std::ofstream pending_;
    std::filesystem::path pending_path_;
};

}  // namespace clink
