#pragma once

// State Processor API - `Savepoint.load(env, path)` analogue.
//
// Open a savepoint produced by a running job, read or mutate keyed
// state offline, and write a new savepoint that a later job can be
// restored from. Use this for:
//
//   * migration - bulk-rewrite keyed state across schema changes
//   * inspection - dump state for an audit / debug
//   * seeding - build a savepoint from scratch and start a fresh job
//     pre-populated with state (e.g., onboarding a new tenant)
//
// V1 scope (deliberately tight):
//
//   * Keyed state only. Broadcast / operator-list state is not
//     surfaced. Timers are not exposed (they live in TimerService,
//     not the state backend).
//   * The savepoint is materialised in-memory while open - backed by
//     InMemoryStateBackend. The on-disk format is the same byte blob
//     the FileBackedStateBackend writes (.snap files), so a
//     savepoint produced by a job-side checkpoint loads correctly
//     and a savepoint produced by this API restores into a job
//     correctly. Large savepoints fit in RAM for v1.
//   * No concurrency. A Savepoint is a single-owner object; if
//     multiple threads need read access, make copies of the data
//     out via KeyedState::scan().

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::state_processor {

class Savepoint {
public:
    // Build an empty savepoint, ready for writes. Use
    // .keyed_state<K, V>(op, slot, kc, vc).put(...) to seed entries
    // and .write_to_file(path) to persist.
    [[nodiscard]] static Savepoint create() {
        Savepoint sp;
        sp.backend_ = std::make_shared<InMemoryStateBackend>();
        return sp;
    }

    // Load a savepoint from a .snap blob on disk. The path must point
    // at a Snapshot byte blob (the FileBackedStateBackend produces
    // these at <ha-dir>/checkpoint-<id>.snap or under savepoint dirs
    // via JobManager::take_savepoint). Throws if the file is missing
    // or unreadable.
    [[nodiscard]] static Savepoint load_from_file(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Savepoint::load_from_file: cannot open " + path.string());
        }
        std::vector<std::byte> bytes;
        // Read the whole file. Savepoints are bounded by what fits in
        // RAM in v1; large-state migration is a v2 problem.
        std::istreambuf_iterator<char> it{in}, end;
        for (; it != end; ++it) {
            bytes.push_back(static_cast<std::byte>(*it));
        }
        Snapshot snap{.checkpoint_id = CheckpointId{0}, .bytes = std::move(bytes)};
        return load_from_snapshot(std::move(snap));
    }

    // Load from an in-memory Snapshot blob (e.g., one obtained by
    // calling backend->snapshot(id) in-process). The checkpoint id is
    // copied across so subsequent .write_to_file() preserves it.
    [[nodiscard]] static Savepoint load_from_snapshot(Snapshot snap) {
        Savepoint sp;
        sp.backend_ = std::make_shared<InMemoryStateBackend>();
        sp.checkpoint_id_ = snap.checkpoint_id;
        sp.backend_->restore(snap);
        return sp;
    }

    // Typed view over one operator's keyed-state slot. Reads/writes
    // go to this savepoint's backing InMemoryStateBackend; mutations
    // become visible to subsequent .write_to_file() / .snapshot()
    // calls. The codec pair must match the codecs the original job
    // used for this slot - there's no schema check.
    template <typename K, typename V>
    [[nodiscard]] KeyedState<K, V> keyed_state(OperatorId op,
                                               std::string slot,
                                               Codec<K> kc,
                                               Codec<V> vc) const {
        return KeyedState<K, V>(*backend_, op, std::move(slot), std::move(kc), std::move(vc));
    }

    // Enumerate the OperatorIds that have any keyed state in this
    // savepoint. Queries the backing InMemoryStateBackend directly
    // (no byte parsing) so it stays correct across snapshot-format
    // changes.
    [[nodiscard]] std::vector<OperatorId> operators() const {
        auto ids = backend_->operator_ids();
        std::set<OperatorId> seen(ids.begin(), ids.end());
        return {seen.begin(), seen.end()};
    }

    // Enumerate the slot names present under `op`. The slot prefix is
    // recovered from stored-key bytes via the documented layout in
    // keyed_state.hpp ([kg][slot]['|'][user_key]).
    [[nodiscard]] std::vector<std::string> slots(OperatorId op) const {
        std::set<std::string> seen;
        backend_->scan(op, [&](StateBackend::KeyView k, StateBackend::ValueView) {
            if (k.size() < 2) {
                return;  // not a KeyedState entry
            }
            const auto pipe = k.find('|', 1);
            if (pipe == std::string::npos || pipe < 1) {
                return;
            }
            seen.emplace(k.substr(1, pipe - 1));
        });
        return {seen.begin(), seen.end()};
    }

    // Persist the savepoint as a byte blob at `path`. The bytes are
    // the same Snapshot format the runtime produces, so the file is
    // restore-compatible with JobConfig::restore_from /
    // FileBackedStateBackend. Creates parent directories on demand.
    void write_to_file(const std::filesystem::path& path, CheckpointId id = CheckpointId{0}) const {
        if (auto parent = path.parent_path(); !parent.empty()) {
            std::filesystem::create_directories(parent);
        }
        auto snap = backend_->snapshot(id);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Savepoint::write_to_file: cannot open " + path.string());
        }
        if (!snap.bytes.empty()) {
            out.write(reinterpret_cast<const char*>(snap.bytes.data()),
                      static_cast<std::streamsize>(snap.bytes.size()));
        }
    }

    // Get the in-memory Snapshot blob (for tests / in-process use).
    [[nodiscard]] Snapshot snapshot(CheckpointId id = CheckpointId{0}) const {
        return backend_->snapshot(id);
    }

    // Direct access to the backing in-memory state backend. Use this
    // sparingly - typed keyed_state<>() views are the supported path.
    [[nodiscard]] StateBackend& backend() noexcept { return *backend_; }

private:
    Savepoint() = default;
    std::shared_ptr<InMemoryStateBackend> backend_;
    CheckpointId checkpoint_id_{0};
};

}  // namespace clink::state_processor
