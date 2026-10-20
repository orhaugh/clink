#pragma once

// The stage that keeps the latest row of every key of a PostgreSQL CDC table
// declared with keep_rows='true' (docs/connectors/postgres.md, "Keeping the
// rows"). The SQL planner places it right after the table's JSON bridge, as one
// subtask like the source, under the factory name cdc_keep_rows_row.
//
// The source's rows carry three fields for this stage (written by
// impls/postgres/src/cdc_row_encoder.cpp):
//   __cdc_kind       snapshot, insert, update, delete or truncate
//   __cdc_version    the change's WAL position, 0 for a snapshot row
//   __cdc_unchanged  declared columns an UPDATE left unchanged and stored out
//                    of line without carrying them (null in the row)
//   __cdc_converging true on a change from before the position a snapshot
//                    taken fresh was read at, which may replay what that
//                    snapshot already holds
//
// From the row it holds for each primary key the stage
//   * fills each unchanged column in, from the held row, or from the old
//     key's row when the same change moved the row to a new key;
//   * turns a truncate into a delete of every row it holds;
//   * drops a change no newer than the row it holds, which only a replay can
//     deliver, and a snapshot row that changes nothing about the row it holds
//     (a snapshot taken again after a restart);
//   * in changelog_mode='upsert', emits an update as update_before with the
//     row it replaced and update_after with the new one, and a delete with the
//     row it removed, so a consumer that retracts (a grouping, a join, a
//     filter on any column) computes from whole rows; an update that changes
//     nothing is not emitted;
//   * in changelog_mode='append', completes a delete's row from the row it
//     held, keeping the delete's own change metadata (__op and the rest), so
//     every change row is whole.
//
// Where the table declares __version, each row the stage emits for a key
// carries a __version above every one it emitted for that key before (an
// update_before, and a delete in upsert mode, repeat the row they retract), so
// a store that keeps the row with the highest version per key (ClickHouse's
// ReplacingMergeTree, an argMax over __version) keeps the row emitted last,
// whatever order the rows reached it in. Two rows of one key under one version
// are not ordered by such a store at all. The version is the change's WAL
// position, which per key increases with every change, except where that
// would not outrank what the stage emitted for the key before:
//   * a snapshot row of a key it holds refreshes that key (a backfill after a
//     column was declared, a snapshot taken again) and carries one above the
//     held row's version;
//   * a change of a key it holds, and in append mode a delete, carries its WAL
//     position or one above the held row's version, whichever is higher. The
//     second is higher only after at least as many refreshes since the key's
//     last change as there are bytes between the two changes, 24 or more;
//   * a key it does not hold was last emitted, if ever, by a delete. A
//     snapshot row of one carries one above the highest version a delete
//     carried, 0 while no delete has been emitted (the first snapshot); a
//     change of one carries its WAL position or one above the highest version a
//     delete carried above its own WAL position, whichever is higher. The two
//     highest versions are operator state, checkpointed with the rows.
// Each version depends only on the checkpointed state and on the rows that
// arrive after it, so a restore gives a row it emits again the version the row
// had. A snapshot taken again after a failure can read a row that changed
// since the first attempt read it; the two attempts' rows then share a
// version, and the change that made the difference, which the stream replays
// after the snapshot, outranks both.
//
// A delete or update of a key it holds no row for is a change it cannot
// complete. While converging, an update of such a key is skipped: the row was
// gone before the snapshot was read, and its delete follows. Otherwise an
// update without unchanged columns is emitted as an insert, and one with them
// is an error. A delete is emitted with the columns it carries when
// `emit_unheld_deletes` is set (append mode, or an upsert table whose consumers
// apply rows by key, where removing a key the stage never held is harmless and
// can remove a row an earlier run wrote), and dropped otherwise, since a
// consumer that retracts would retract a row it never counted.
//
// The rows live in keyed state, written through on every change, so they are
// checkpointed with the job and held by whichever state backend it runs on.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/state/broadcast_state.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink::sql {

// The fields the CDC source writes for this stage; the PostgreSQL connector
// spells the same names in cdc_row_encoder.hpp.
inline constexpr std::string_view kCdcKindField = "__cdc_kind";
inline constexpr std::string_view kCdcVersionField = "__cdc_version";
inline constexpr std::string_view kCdcUnchangedField = "__cdc_unchanged";
inline constexpr std::string_view kCdcConvergingField = "__cdc_converging";

class CdcKeepRowsOperator final : public Operator<Row, Row> {
public:
    enum class Mode : std::uint8_t { Upsert, Append };

    CdcKeepRowsOperator(std::vector<std::string> key_columns, Mode mode, bool emit_unheld_deletes);

    void process(const StreamElement<Row>& element, Emitter<Row>& out) override;

    [[nodiscard]] std::string name() const override { return "cdc_keep_rows_row"; }

    // Changes dropped as no newer than the held row, and deletes of keys the
    // stage held no row for that it dropped. Neither happens in a run whose
    // source and state were restored from the same checkpoint.
    [[nodiscard]] std::uint64_t stale_changes() const noexcept { return stale_; }
    [[nodiscard]] std::uint64_t unheld_deletes_dropped() const noexcept { return unheld_dropped_; }
    // Updates of keys it held no row for, skipped while converging.
    [[nodiscard]] std::uint64_t converging_skips() const noexcept { return converging_skips_; }

private:
    // The highest __version a delete carried, and the highest a delete carried
    // above its own WAL position: what a row of a key the stage no longer holds
    // must outrank.
    struct DeleteVersions {
        std::uint64_t highest{0};
        std::uint64_t highest_raised{0};
        bool operator==(const DeleteVersions&) const = default;
        // A delete that leaves `top` the highest version its key has had.
        void note(std::uint64_t top, std::uint64_t wal_position);
    };

    KeyedState<std::string, Row> rows_();
    BroadcastState<std::pair<std::uint64_t, std::uint64_t>> deletes_();
    [[nodiscard]] std::string key_of_(const Row& row) const;
    void apply_(const Record<Row>& rec,
                KeyedState<std::string, Row>& kv,
                DeleteVersions& deletes,
                Batch<Row>& out);
    void truncate_(const Record<Row>& rec,
                   KeyedState<std::string, Row>& kv,
                   DeleteVersions& deletes,
                   Emitter<Row>& out);

    std::vector<std::string> key_columns_;
    Mode mode_;
    bool emit_unheld_deletes_;
    // The row the last delete removed, with that delete's version: an UPDATE
    // that changes a key arrives as a delete of the old key and an update of
    // the new one at the same position, and the update's unchanged columns
    // are the old row's.
    std::optional<std::pair<std::uint64_t, Row>> moved_;
    std::uint64_t stale_{0};
    std::uint64_t unheld_dropped_{0};
    std::uint64_t converging_skips_{0};
    bool warned_stale_{false};
};

}  // namespace clink::sql
