#pragma once

// The PostgreSQL CDC source's position and acknowledgement bookkeeping, kept
// apart from libpq so the rules are tested without a server.
//
// TxnCursor holds where a restart must resume. The server sends whole
// transactions in commit order (pgoutput version 1 sends nothing until a
// transaction commits), so the resume position is the end LSN of the last
// transaction whose changes were all emitted. A checkpoint can fall inside a
// transaction - produce() emits a bounded batch and returns - so the cursor
// also records that transaction's identity and how many of its changes were
// processed. After a restart from the resume position the server sends that
// transaction again, first, and the cursor skips the changes already emitted.
//
// AckLedger holds what may be confirmed to the server. A checkpoint records
// its resume position as pending; the position becomes confirmable only when
// the runtime reports that checkpoint (or a later one) complete, and an
// aborted checkpoint's entry is discarded without confirming anything.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/postgres_decoder.hpp"

namespace clink::postgres::cdc {

// A position the source cannot honour: the history it must replay is no
// longer what the server sends. Never retried.
class HistoryUnavailable : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

inline std::string format_lsn_text(std::uint64_t lsn) {
    return clink::pg::format_lsn(lsn);
}

enum class Phase : std::uint8_t { Snapshot = 1, Stream = 2 };

// What a checkpoint records.
struct CursorState {
    Phase phase{Phase::Stream};
    // Snapshot phase: the slot position streaming will start from.
    // Stream phase: the end LSN of the last fully emitted transaction.
    std::uint64_t resume_lsn{0};
    // A transaction partly emitted at the checkpoint: its identity (pgoutput:
    // the commit LSN its Begin message names; test_decoding: its xid) and how
    // many of its changes to the captured table were processed.
    bool in_flight{false};
    std::uint64_t txn_id{0};
    std::uint64_t txn_changes{0};
    // A snapshot taken fresh, without the slot's export, reads the table as
    // of some WAL position past where streaming resumes, so the changes
    // before that position replay what the snapshot already holds. This is
    // that position (0 = no such snapshot, or the stream has passed it).
    std::uint64_t converge_until{0};
};

class TxnCursor {
public:
    // Start (or restart) streaming from `resume_lsn`. `interrupted` names a
    // transaction partly emitted before the restart, which the server will
    // send first.
    void start(std::uint64_t resume_lsn,
               std::optional<std::pair<std::uint64_t, std::uint64_t>> interrupted) {
        resume_lsn_ = resume_lsn;
        pending_skip_ = interrupted;
        in_txn_ = false;
        skip_remaining_ = 0;
    }

    // A transaction begins. Returns false when its Begin marker was already
    // emitted (the interrupted transaction of a restart). Throws
    // HistoryUnavailable when a restart expected the interrupted transaction
    // and the server sent another.
    bool on_begin(std::uint64_t txn_id) {
        if (in_txn_) {
            throw std::runtime_error("postgres_cdc: a transaction began inside transaction " +
                                     std::to_string(txn_id_) + " without its commit");
        }
        in_txn_ = true;
        txn_id_ = txn_id;
        txn_changes_ = 0;
        skip_remaining_ = 0;
        if (!pending_skip_) {
            return true;
        }
        const auto [want, changes] = *pending_skip_;
        pending_skip_.reset();
        if (want != txn_id) {
            throw HistoryUnavailable(
                "postgres_cdc: resumed at " + format_lsn_text(resume_lsn_) +
                " expecting the transaction that was interrupted by the restart (" +
                format_lsn_text(want) + ") first, but the server sent " + format_lsn_text(txn_id) +
                ". The slot no longer serves the changes this job's position needs; refusing "
                "rather than skipping them");
        }
        skip_remaining_ = changes;
        return false;
    }

    // A change to the captured table inside the current transaction. Returns
    // false when it was emitted before the restart and must be skipped.
    bool on_change() {
        if (!in_txn_) {
            throw std::runtime_error("postgres_cdc: a change arrived outside a transaction");
        }
        ++txn_changes_;
        if (skip_remaining_ > 0) {
            --skip_remaining_;
            return false;
        }
        return true;
    }

    // The current transaction committed; its commit record ends at `end_lsn`.
    void on_commit(std::uint64_t end_lsn) {
        if (!in_txn_) {
            throw std::runtime_error("postgres_cdc: a commit arrived outside a transaction");
        }
        if (skip_remaining_ > 0) {
            throw HistoryUnavailable(
                "postgres_cdc: the transaction interrupted by the restart (" +
                format_lsn_text(txn_id_) + ") came back with " + std::to_string(txn_changes_) +
                " changes, fewer than the " + std::to_string(txn_changes_ + skip_remaining_) +
                " the checkpoint recorded; refusing rather than guessing which were emitted");
        }
        in_txn_ = false;
        resume_lsn_ = std::max(resume_lsn_, end_lsn);
    }

    // A keepalive reporting that the server has sent everything before
    // `wal_end`. Outside a transaction that is a safe resume position: every
    // transaction committed before it has been received.
    void on_keepalive(std::uint64_t wal_end) {
        if (!in_txn_ && !pending_skip_) {
            resume_lsn_ = std::max(resume_lsn_, wal_end);
        }
    }

    // The connection is gone. A transaction in progress becomes the
    // interrupted transaction of the next start().
    [[nodiscard]] std::optional<std::pair<std::uint64_t, std::uint64_t>> interrupted() const {
        if (in_txn_) {
            return std::make_pair(txn_id_, txn_changes_);
        }
        return pending_skip_;
    }

    [[nodiscard]] CursorState state(Phase phase) const {
        CursorState s;
        s.phase = phase;
        s.resume_lsn = resume_lsn_;
        if (const auto i = interrupted()) {
            s.in_flight = true;
            s.txn_id = i->first;
            s.txn_changes = i->second;
        }
        return s;
    }

    [[nodiscard]] std::uint64_t resume_lsn() const noexcept { return resume_lsn_; }
    [[nodiscard]] bool in_transaction() const noexcept { return in_txn_; }

private:
    std::uint64_t resume_lsn_{0};
    bool in_txn_{false};
    std::uint64_t txn_id_{0};
    std::uint64_t txn_changes_{0};
    std::uint64_t skip_remaining_{0};
    std::optional<std::pair<std::uint64_t, std::uint64_t>> pending_skip_;
};

class AckLedger {
public:
    // A checkpoint captured `resume_lsn`.
    void record(std::uint64_t ckpt, std::uint64_t resume_lsn) {
        std::lock_guard<std::mutex> lk(mu_);
        pending_[ckpt] = resume_lsn;
    }

    // Checkpoint `ckpt` is globally durable: it and every earlier checkpoint
    // still pending become confirmable.
    void complete(std::uint64_t ckpt) {
        std::lock_guard<std::mutex> lk(mu_);
        std::uint64_t safe = confirmable_.load(std::memory_order_relaxed);
        for (auto it = pending_.begin(); it != pending_.end() && it->first <= ckpt;) {
            safe = std::max(safe, it->second);
            it = pending_.erase(it);
        }
        confirmable_.store(safe, std::memory_order_release);
    }

    // Checkpoint `ckpt` will never complete: forget it, confirming nothing.
    void abort(std::uint64_t ckpt) {
        std::lock_guard<std::mutex> lk(mu_);
        pending_.erase(ckpt);
    }

    // The highest position a completed checkpoint resumes from (0 = none).
    [[nodiscard]] std::uint64_t confirmable() const noexcept {
        return confirmable_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t pending() const {
        std::lock_guard<std::mutex> lk(mu_);
        return pending_.size();
    }

private:
    mutable std::mutex mu_;
    std::map<std::uint64_t, std::uint64_t> pending_;
    std::atomic<std::uint64_t> confirmable_{0};
};

// The checkpoint encoding: version 3, little-endian.
//   [0] version (3)  [1] phase  [2] flags (bit 0: in flight)
//   [3..10] resume LSN  [11..18] transaction id  [19..26] changes processed
//   [27..34] the position a fresh snapshot converges at
// Version 2 is the same without the last field, and still read.
inline constexpr std::uint8_t kCursorVersion = 3;
inline constexpr std::size_t kCursorBytes = 35;
inline constexpr std::size_t kCursorV2Bytes = 27;

inline std::array<char, kCursorBytes> encode_cursor(const CursorState& s) {
    std::array<char, kCursorBytes> out{};
    const auto put64 = [&](std::size_t at, std::uint64_t v) {
        for (std::size_t i = 0; i < 8; ++i) {
            out[at + i] = static_cast<char>((v >> (i * 8)) & 0xFF);
        }
    };
    out[0] = static_cast<char>(kCursorVersion);
    out[1] = static_cast<char>(s.phase);
    out[2] = static_cast<char>(s.in_flight ? 1 : 0);
    put64(3, s.resume_lsn);
    put64(11, s.txn_id);
    put64(19, s.txn_changes);
    put64(27, s.converge_until);
    return out;
}

// nullopt for bytes this release cannot read (an unknown version is refused
// by the caller rather than misread).
inline std::optional<CursorState> decode_cursor(const std::vector<char>& bytes) {
    if (bytes.empty()) {
        return std::nullopt;
    }
    const auto version = static_cast<std::uint8_t>(bytes[0]);
    if (!(version == kCursorVersion && bytes.size() >= kCursorBytes) &&
        !(version == 2 && bytes.size() >= kCursorV2Bytes)) {
        return std::nullopt;
    }
    const auto get64 = [&](std::size_t at) {
        std::uint64_t v = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[at + i])) << (i * 8);
        }
        return v;
    };
    CursorState s;
    const auto phase = static_cast<std::uint8_t>(bytes[1]);
    if (phase != static_cast<std::uint8_t>(Phase::Snapshot) &&
        phase != static_cast<std::uint8_t>(Phase::Stream)) {
        return std::nullopt;
    }
    s.phase = static_cast<Phase>(phase);
    s.in_flight = (static_cast<std::uint8_t>(bytes[2]) & 1U) != 0;
    s.resume_lsn = get64(3);
    s.txn_id = get64(11);
    s.txn_changes = get64(19);
    if (version == kCursorVersion) {
        s.converge_until = get64(27);
    }
    return s;
}

}  // namespace clink::postgres::cdc
