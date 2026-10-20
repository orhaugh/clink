// The stage that keeps the latest row of every key of a CDC table declared
// with keep_rows='true' (include/clink/sql/cdc_keep_rows.hpp): the old row an
// update replaced and the row a delete removed, unchanged out-of-line values
// filled in, a truncate turned into deletes, replays dropped, and the rows
// carried through a checkpoint in keyed state.

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/types.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/cdc_keep_rows.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace clink;
using namespace clink::sql;
using Mode = CdcKeepRowsOperator::Mode;

// One change as the PostgreSQL source writes it for the stage.
Row change(const std::string& kind,
           std::int64_t version,
           std::int64_t id,
           const std::string& status,
           const std::string& body = "b") {
    Row r;
    r.values["id"] = config::JsonValue{id};
    r.values["status"] = status.empty() ? config::JsonValue{} : config::JsonValue{status};
    r.values["body"] = body.empty() ? config::JsonValue{} : config::JsonValue{body};
    r.values["__cdc_kind"] = config::JsonValue{kind};
    r.values["__cdc_version"] = config::JsonValue{version};
    return r;
}

Row with_unchanged(Row r, const std::vector<std::string>& columns) {
    config::JsonArray names;
    for (const auto& c : columns) {
        names.emplace_back(c);
        r.values[c] = config::JsonValue{};
    }
    r.values["__cdc_unchanged"] = config::JsonValue{std::move(names)};
    return r;
}

// The append-mode metadata a change row carries.
Row with_metadata(Row r, const std::string& op, std::int64_t version, bool deleted) {
    r.values["__op"] = config::JsonValue{op};
    r.values["__version"] = config::JsonValue{version};
    r.values["__deleted"] = config::JsonValue{deleted};
    return r;
}

class Stage {
public:
    Stage(Mode mode, bool emit_unheld, InMemoryStateBackend* backend = nullptr)
        : backend_(backend != nullptr ? backend : &own_),
          ctx_(operator_id_from_uid("cdc-keep-rows-test"), "cdc_keep_rows_row", backend_, nullptr),
          op_(std::make_shared<CdcKeepRowsOperator>(
              std::vector<std::string>{"id"}, mode, emit_unheld)) {
        op_->attach_runtime(&ctx_);
        op_->open();
    }
    ~Stage() {
        op_->close();
        op_->attach_runtime(nullptr);
    }
    Stage(const Stage&) = delete;
    Stage& operator=(const Stage&) = delete;
    Stage(Stage&&) = delete;
    Stage& operator=(Stage&&) = delete;

    // The rows the stage emits for `rows`, in order.
    std::vector<Row> feed(std::vector<Row> rows) {
        std::vector<Row> out;
        Emitter<Row> em([&](StreamElement<Row> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    out.push_back(r.value());
                }
            }
            return true;
        });
        Batch<Row> batch;
        for (auto& r : rows) {
            batch.emplace(std::move(r));
        }
        op_->process(StreamElement<Row>::data(std::move(batch)), em);
        return out;
    }

    CdcKeepRowsOperator& op() { return *op_; }
    InMemoryStateBackend& backend() { return *backend_; }

private:
    InMemoryStateBackend own_;
    InMemoryStateBackend* backend_;
    RuntimeContext ctx_;
    std::shared_ptr<CdcKeepRowsOperator> op_;
};

std::string kind(const Row& r) {
    return row_kind_of(r);
}
std::string str(const Row& r, const std::string& c) {
    return r.get_string(c).value_or("NULL");
}
bool has_stage_fields(const Row& r) {
    return r.has_column("__cdc_kind") || r.has_column("__cdc_version") ||
           r.has_column("__cdc_unchanged");
}

}  // namespace

TEST(SqlCdcKeepRows, UpsertGivesAnUpdateItsOldRowAndADeleteTheRowItRemoved) {
    Stage s(Mode::Upsert, /*emit_unheld=*/false);
    auto out = s.feed({change("snapshot", 0, 1, "new"), change("insert", 100, 2, "new")});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(kind(out[0]), "insert");
    EXPECT_EQ(kind(out[1]), "insert");
    EXPECT_FALSE(has_stage_fields(out[0]));

    out = s.feed({change("update", 200, 1, "paid")});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(kind(out[0]), "update_before");
    EXPECT_EQ(str(out[0], "status"), "new");
    EXPECT_EQ(kind(out[1]), "update_after");
    EXPECT_EQ(str(out[1], "status"), "paid");

    // An update that changes nothing retracts nothing.
    EXPECT_TRUE(s.feed({change("update", 210, 1, "paid")}).empty());

    // A delete under REPLICA IDENTITY DEFAULT carries the key alone; the
    // retraction carries the whole row.
    out = s.feed({change("delete", 300, 1, "", "")});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "delete");
    EXPECT_EQ(str(out[0], "status"), "paid");
    EXPECT_EQ(str(out[0], "body"), "b");
    // Gone: the key comes back as an insert.
    out = s.feed({change("insert", 400, 1, "again")});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "insert");
}

TEST(SqlCdcKeepRows, AnUnchangedValueIsFilledFromTheHeldRow) {
    Stage s(Mode::Upsert, false);
    (void)s.feed({change("insert", 100, 1, "new", "a large body")});
    auto out = s.feed({with_unchanged(change("update", 200, 1, "paid"), {"body"})});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(str(out[1], "status"), "paid");
    EXPECT_EQ(str(out[1], "body"), "a large body");
    EXPECT_FALSE(has_stage_fields(out[1]));
    // And the filled row is the one held next.
    out = s.feed({change("delete", 300, 1, "", "")});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "body"), "a large body");
}

TEST(SqlCdcKeepRows, AKeyChangeTakesUnchangedValuesFromTheOldKeysRow) {
    Stage s(Mode::Upsert, false);
    (void)s.feed({change("insert", 100, 1, "new", "a large body")});
    // UPDATE ... SET id = 2: a delete of the old key and the new row, at one
    // position, the new row without the value it left unchanged.
    auto out = s.feed({change("delete", 200, 1, "", ""),
                       with_unchanged(change("update", 200, 2, "new"), {"body"})});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(kind(out[0]), "delete");
    EXPECT_EQ(str(out[0], "id"), "1");
    EXPECT_EQ(kind(out[1]), "insert");
    EXPECT_EQ(str(out[1], "id"), "2");
    EXPECT_EQ(str(out[1], "body"), "a large body");
}

TEST(SqlCdcKeepRows, AnUnchangedValueWithNoRowToTakeItFromStopsTheStage) {
    Stage s(Mode::Upsert, false);
    try {
        (void)s.feed({with_unchanged(change("update", 100, 7, "paid"), {"body"})});
        FAIL() << "an update without its unchanged value was emitted";
    } catch (const std::runtime_error& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("id=7"), std::string::npos) << what;
        EXPECT_NE(what.find("holds no row"), std::string::npos) << what;
    }
    // A moved row only fills the update at its own position.
    Stage t(Mode::Upsert, false);
    (void)t.feed({change("insert", 100, 1, "new")});
    EXPECT_THROW((void)t.feed({change("delete", 200, 1, "", ""),
                               with_unchanged(change("update", 300, 2, "new"), {"body"})}),
                 std::runtime_error);
}

TEST(SqlCdcKeepRows, ATruncateDeletesEveryHeldRow) {
    Stage s(Mode::Upsert, false);
    (void)s.feed({change("snapshot", 0, 1, "a"),
                  change("snapshot", 0, 2, "b"),
                  change("insert", 100, 3, "c")});
    Row truncate = change("truncate", 500, 0, "", "");
    truncate.values.erase(std::string{"id"});
    auto out = s.feed({truncate});
    ASSERT_EQ(out.size(), 3U);
    std::vector<std::string> statuses;
    for (const auto& r : out) {
        EXPECT_EQ(kind(r), "delete");
        EXPECT_FALSE(has_stage_fields(r));
        statuses.push_back(str(r, "status"));
    }
    std::sort(statuses.begin(), statuses.end());
    EXPECT_EQ(statuses, (std::vector<std::string>{"a", "b", "c"}));
    // Nothing held any more: an update of a removed key is an insert.
    out = s.feed({change("update", 600, 1, "back")});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "insert");
}

TEST(SqlCdcKeepRows, AppendModeCompletesDeletesAndKeepsEveryChange) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    auto out =
        s.feed({with_metadata(change("insert", 100, 1, "new", "body1"), "insert", 100, false),
                with_metadata(change("update", 200, 1, "new", "body1"), "update", 200, false)});
    // Every change is a row, even one that changes no value.
    ASSERT_EQ(out.size(), 2U);
    EXPECT_FALSE(has_row_kind(out[0]));
    EXPECT_FALSE(has_stage_fields(out[1]));

    out = s.feed({with_metadata(change("delete", 300, 1, "", ""), "delete", 300, true)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_FALSE(has_row_kind(out[0]));
    EXPECT_EQ(str(out[0], "status"), "new");  // from the row it removed
    EXPECT_EQ(str(out[0], "body"), "body1");
    EXPECT_EQ(str(out[0], "__op"), "delete");  // the delete's own metadata
    EXPECT_EQ(str(out[0], "__version"), "300");
    EXPECT_EQ(str(out[0], "__deleted"), "true");

    // A truncate: each removed row with the truncate's metadata.
    (void)s.feed({with_metadata(change("insert", 400, 2, "x", "b2"), "insert", 400, false)});
    Row truncate = with_metadata(change("truncate", 500, 0, "", ""), "delete", 500, true);
    truncate.values.erase(std::string{"id"});
    out = s.feed({truncate});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "id"), "2");
    EXPECT_EQ(str(out[0], "status"), "x");
    EXPECT_EQ(str(out[0], "__version"), "500");
    EXPECT_EQ(str(out[0], "__deleted"), "true");
}

TEST(SqlCdcKeepRows, AChangeNoNewerThanTheHeldRowIsDropped) {
    Stage s(Mode::Upsert, false);
    (void)s.feed({change("insert", 200, 1, "new")});
    EXPECT_TRUE(s.feed({change("update", 150, 1, "older")}).empty());
    EXPECT_TRUE(s.feed({change("update", 200, 1, "same position")}).empty());
    EXPECT_TRUE(s.feed({change("delete", 180, 1, "", "")}).empty());
    EXPECT_EQ(s.op().stale_changes(), 3U);
    // A snapshot row is the table's current state and always applies.
    auto out = s.feed({change("snapshot", 0, 1, "current")});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(str(out[1], "status"), "current");
    // A newer change still does.
    out = s.feed({change("update", 250, 1, "newer")});
    ASSERT_EQ(out.size(), 2U);
}

TEST(SqlCdcKeepRows, ADeleteOfAKeyNotHeldFollowsThePolicy) {
    Stage emit(Mode::Upsert, /*emit_unheld=*/true);
    auto out = emit.feed({change("delete", 100, 5, "", "")});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "delete");
    EXPECT_EQ(str(out[0], "id"), "5");

    Stage drop(Mode::Upsert, /*emit_unheld=*/false);
    EXPECT_TRUE(drop.feed({change("delete", 100, 5, "", "")}).empty());
    EXPECT_EQ(drop.op().unheld_deletes_dropped(), 1U);
}

// After a snapshot taken fresh, the stream replays changes the snapshot
// already holds, among them updates of rows deleted before it was read. Those
// are skipped; the same update outside that window stops the stage.
TEST(SqlCdcKeepRows, WhileConvergingAnUpdateOfARowTheSnapshotLacksIsSkipped) {
    Stage s(Mode::Upsert, /*emit_unheld=*/false);
    Row replayed = with_unchanged(change("update", 100, 9, "paid"), {"body"});
    replayed.values["__cdc_converging"] = config::JsonValue{true};
    EXPECT_TRUE(s.feed({replayed}).empty());
    Row replayed_whole = change("update", 110, 9, "paid");
    replayed_whole.values["__cdc_converging"] = config::JsonValue{true};
    EXPECT_TRUE(s.feed({replayed_whole}).empty());
    EXPECT_EQ(s.op().converging_skips(), 2U);
    // An insert is never skipped: the row may have been inserted after the
    // snapshot was read.
    Row inserted = change("insert", 120, 10, "new");
    inserted.values["__cdc_converging"] = config::JsonValue{true};
    auto out = s.feed({inserted});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_FALSE(out[0].has_column("__cdc_converging"));
    EXPECT_THROW((void)s.feed({with_unchanged(change("update", 200, 9, "paid"), {"body"})}),
                 std::runtime_error);
}

// A column declared since the checkpoint: the table is read again, and each
// snapshot row refreshes the row held for its key, one version above it.
// Replayed changes that predate the column take it from the refreshed row, and
// inserts that predate it are passed over while converging.
TEST(SqlCdcKeepRows, ABackfillCompletesTheChangesReplayedAfterIt) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    Row backfill = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
    backfill.values["note"] = config::JsonValue{std::string{"from the backfill"}};
    auto out = s.feed({backfill});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "101");
    EXPECT_EQ(str(out[0], "note"), "from the backfill");

    // A replayed update from before the column: the column is the held row's.
    Row replayed = with_metadata(change("update", 150, 1, "paid"), "update", 150, false);
    replayed = with_unchanged(replayed, {"note"});
    replayed.values["__cdc_converging"] = config::JsonValue{true};
    out = s.feed({replayed});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "note"), "from the backfill");
    EXPECT_EQ(str(out[0], "status"), "paid");

    // An insert from before the column, of a row the backfill did not find.
    Row gone = with_unchanged(with_metadata(change("insert", 160, 7, "x"), "insert", 160, false),
                              {"note"});
    gone.values["__cdc_converging"] = config::JsonValue{true};
    EXPECT_TRUE(s.feed({gone}).empty());
    EXPECT_EQ(s.op().converging_skips(), 1U);
    // Outside the window such an insert cannot be completed.
    gone.values.erase(std::string{"__cdc_converging"});
    EXPECT_THROW((void)s.feed({gone}), std::runtime_error);
}

// The __version of the rows the stage emits for a key strictly increases in the
// order it emits them, so a store that keeps the highest version per key
// (ClickHouse's ReplacingMergeTree, an argMax over __version) keeps the latest
// whatever order the rows reached it in. Equal versions are not ordered by the
// store at all: argMax picks either.

// A backfill row of a key the stage holds outranks the row it refreshes: one
// above that row's version, which every later change of the key outranks.
TEST(SqlCdcKeepRows, ABackfillRowOutranksTheRowItRefreshes) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false),
                  with_metadata(change("snapshot", 0, 2, "seed"), "snapshot", 0, false)});
    Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
    refresh.values["note"] = config::JsonValue{std::string{"from the backfill"}};
    Row seed = with_metadata(change("snapshot", 0, 2, "seed"), "snapshot", 0, false);
    seed.values["note"] = config::JsonValue{std::string{"also backfilled"}};
    auto out = s.feed({refresh, seed});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(str(out[0], "__version"), "101") << "ties the row it refreshes";
    EXPECT_EQ(str(out[1], "__version"), "1") << "ties the snapshot row it refreshes";

    // A later change of the key outranks the refresh.
    Row later = with_metadata(change("update", 150, 1, "paid"), "update", 150, false);
    later.values["note"] = config::JsonValue{std::string{"from the backfill"}};
    out = s.feed({later});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "150");
}

// The same in upsert mode, where the refresh is an update_after.
TEST(SqlCdcKeepRows, AnUpsertBackfillCarriesTheNextVersion) {
    Stage s(Mode::Upsert, /*emit_unheld=*/false);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
    refresh.values["note"] = config::JsonValue{std::string{"from the backfill"}};
    const auto out = s.feed({refresh});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(kind(out[1]), "update_after");
    EXPECT_EQ(str(out[1], "__version"), "101");
}

// A snapshot row that changes nothing about the row held for its key (a
// snapshot read again after a restart) is not emitted, and moves no version.
TEST(SqlCdcKeepRows, ARefreshThatChangesNothingIsNotEmitted) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    EXPECT_TRUE(
        s.feed({with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false)}).empty());
    Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
    refresh.values["note"] = config::JsonValue{std::string{"n"}};
    const auto out = s.feed({refresh});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "101");
}

// Refreshes in a row each take the next version. A change whose WAL position
// is not above the key's latest version (possible only after more refreshes
// than the change's WAL record is long) still outranks them.
TEST(SqlCdcKeepRows, AChangeOutranksEveryRefreshBeforeIt) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    for (const std::string note : {"a", "b", "c"}) {
        Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
        refresh.values["note"] = config::JsonValue{note};
        (void)s.feed({refresh});
    }
    const auto out =
        s.feed({with_metadata(change("update", 102, 1, "paid"), "update", 102, false)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "104") << "lost to the refresh at 103";
}

// A delete outranks the refreshes before it too, and a key inserted again after
// such a delete outranks the delete, though the stage no longer holds the key.
TEST(SqlCdcKeepRows, AKeyDeletedAfterRefreshesAndInsertedAgainOutranksTheDelete) {
    InMemoryStateBackend backend;
    {
        Stage s(Mode::Append, /*emit_unheld=*/true, &backend);
        (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
        for (const std::string note : {"a", "b", "c"}) {
            Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
            refresh.values["note"] = config::JsonValue{note};
            (void)s.feed({refresh});
        }
        const auto gone =
            s.feed({with_metadata(change("delete", 102, 1, "", ""), "delete", 102, true)});
        ASSERT_EQ(gone.size(), 1U);
        EXPECT_EQ(str(gone[0], "__version"), "104");
    }
    // Across a checkpoint.
    const auto snap = backend.snapshot(CheckpointId{1});
    InMemoryStateBackend restored;
    restored.restore(snap);
    Stage s(Mode::Append, /*emit_unheld=*/true, &restored);
    const auto again =
        s.feed({with_metadata(change("insert", 103, 1, "back"), "insert", 103, false)});
    ASSERT_EQ(again.size(), 1U);
    EXPECT_EQ(str(again[0], "__version"), "105");
}

// A snapshot read again finds keys the stage no longer holds: one deleted
// before it and inserted again, or one it never saw. Their rows outrank every
// delete emitted before; in the first snapshot, before any delete, they carry 0.
TEST(SqlCdcKeepRows, ASnapshotRowOfAKeyNotHeldOutranksEveryDeleteBeforeIt) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    auto out = s.feed({with_metadata(change("snapshot", 0, 1, "seed"), "snapshot", 0, false)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "0");
    (void)s.feed({with_metadata(change("delete", 200, 1, "", ""), "delete", 200, true)});

    out = s.feed({with_metadata(change("snapshot", 0, 1, "back"), "snapshot", 0, false),
                  with_metadata(change("snapshot", 0, 2, "new"), "snapshot", 0, false)});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(str(out[0], "__version"), "201") << "ranks below the delete it follows";
    EXPECT_EQ(str(out[1], "__version"), "201");
    // The stream's insert of the key, replayed after the snapshot, outranks it
    // with its own WAL position.
    Row insert = with_metadata(change("insert", 250, 1, "back"), "insert", 250, false);
    insert.values["__cdc_converging"] = config::JsonValue{true};
    out = s.feed({insert});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "250");
}

// Each row a truncate removes outranks the row it removes, as a delete does.
TEST(SqlCdcKeepRows, ATruncateOutranksEveryRowItRemoves) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({with_metadata(change("insert", 90, 2, "new"), "insert", 90, false),
                  with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    for (const std::string note : {"a", "b", "c"}) {
        Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
        refresh.values["note"] = config::JsonValue{note};
        (void)s.feed({refresh});
    }
    Row truncate = with_metadata(change("truncate", 102, 0, "", ""), "delete", 102, true);
    truncate.values.erase(std::string{"id"});
    auto out = s.feed({truncate});
    ASSERT_EQ(out.size(), 2U);
    std::sort(out.begin(), out.end(), [](const Row& a, const Row& b) {
        return str(a, "id") < str(b, "id");
    });
    EXPECT_EQ(str(out[0], "__version"), "104") << "lost to the refresh at 103";
    EXPECT_EQ(str(out[1], "__version"), "102");
    out = s.feed({with_metadata(change("insert", 103, 1, "back"), "insert", 103, false)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "105");
}

// A key change after a backfill: the old key's delete outranks the row the
// backfill refreshed, and the new key's row, completed from the old key's,
// carries the change's own position.
TEST(SqlCdcKeepRows, AKeyChangeAfterABackfillKeepsBothKeysInOrder) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed(
        {with_metadata(change("insert", 100, 1, "new", "a large body"), "insert", 100, false)});
    Row refresh =
        with_metadata(change("snapshot", 0, 1, "new", "a large body"), "snapshot", 0, false);
    refresh.values["note"] = config::JsonValue{std::string{"n"}};
    auto out = s.feed({refresh});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(str(out[0], "__version"), "101");

    // UPDATE ... SET id = 2: a delete of the old key and the new row at one
    // position, the new row without the value it left unchanged.
    Row moved = with_unchanged(with_metadata(change("update", 200, 2, "new"), "update", 200, false),
                               {"body"});
    moved.values["note"] = config::JsonValue{std::string{"n"}};
    out = s.feed({with_metadata(change("delete", 200, 1, "", ""), "delete", 200, true), moved});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(str(out[0], "id"), "1");
    EXPECT_EQ(str(out[0], "__version"), "200");
    EXPECT_EQ(str(out[0], "__deleted"), "true");
    EXPECT_EQ(str(out[1], "id"), "2");
    EXPECT_EQ(str(out[1], "__version"), "200");
    EXPECT_EQ(str(out[1], "body"), "a large body");
}

// In upsert mode a delete retracts the row it removes, version included; a key
// read again after it still outranks it.
TEST(SqlCdcKeepRows, AnUpsertDeleteRetractsTheRowWithItsVersion) {
    Stage s(Mode::Upsert, /*emit_unheld=*/false);
    (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false)});
    Row refresh = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
    refresh.values["note"] = config::JsonValue{std::string{"n"}};
    (void)s.feed({refresh});
    auto out = s.feed({with_metadata(change("delete", 200, 1, "", ""), "delete", 200, true)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "delete");
    EXPECT_EQ(str(out[0], "__version"), "101");
    EXPECT_EQ(str(out[0], "note"), "n");
    out = s.feed({with_metadata(change("snapshot", 0, 1, "back"), "snapshot", 0, false)});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(kind(out[0]), "insert");
    EXPECT_EQ(str(out[0], "__version"), "102");
}

// The versions depend only on the checkpointed state and the rows after it:
// restored and fed the same rows, the stage emits the same rows again.
TEST(SqlCdcKeepRows, ARestoreEmitsTheSameRowsWithTheSameVersions) {
    const auto rows_after = [] {
        Row a = with_metadata(change("snapshot", 0, 1, "new"), "snapshot", 0, false);
        a.values["note"] = config::JsonValue{std::string{"backfilled"}};
        Row b = with_metadata(change("snapshot", 0, 2, "seed"), "snapshot", 0, false);
        b.values["note"] = config::JsonValue{std::string{"backfilled"}};
        Row c = with_metadata(change("snapshot", 0, 3, "new"), "snapshot", 0, false);
        c.values["note"] = config::JsonValue{std::string{"backfilled"}};
        Row d = with_metadata(change("delete", 300, 1, "", ""), "delete", 300, true);
        Row e = with_metadata(change("insert", 310, 1, "again"), "insert", 310, false);
        e.values["note"] = config::JsonValue{std::string{"again"}};
        return std::vector<Row>{a, b, c, d, e};
    };
    const auto text = [](const std::vector<Row>& rows) {
        std::vector<std::string> out;
        for (const auto& r : rows) {
            out.push_back(str(r, "id") + "|" + str(r, "status") + "|" + str(r, "note") + "|" +
                          str(r, "__version") + "|" + str(r, "__deleted"));
        }
        return out;
    };
    InMemoryStateBackend backend;
    std::vector<std::string> first;
    {
        Stage s(Mode::Append, /*emit_unheld=*/true, &backend);
        (void)s.feed({with_metadata(change("insert", 100, 1, "new"), "insert", 100, false),
                      with_metadata(change("snapshot", 0, 2, "seed"), "snapshot", 0, false),
                      with_metadata(change("insert", 120, 3, "new"), "insert", 120, false),
                      with_metadata(change("delete", 130, 3, "", ""), "delete", 130, true)});
        const auto snap = backend.snapshot(CheckpointId{1});
        first = text(s.feed(rows_after()));
        InMemoryStateBackend restored;
        restored.restore(snap);
        Stage again(Mode::Append, /*emit_unheld=*/true, &restored);
        EXPECT_EQ(text(again.feed(rows_after())), first);
    }
    EXPECT_EQ(first,
              (std::vector<std::string>{"1|new|backfilled|101|false",
                                        "2|seed|backfilled|1|false",
                                        "3|new|backfilled|131|false",
                                        "1|new|backfilled|300|true",
                                        "1|again|again|310|false"}));
}

// A table that does not declare __version is given none.
TEST(SqlCdcKeepRows, ATableWithoutAVersionColumnIsGivenNone) {
    Stage s(Mode::Append, /*emit_unheld=*/true);
    (void)s.feed({change("insert", 100, 1, "new")});
    Row refresh = change("snapshot", 0, 1, "new");
    refresh.values["note"] = config::JsonValue{std::string{"n"}};
    auto out = s.feed({refresh, change("delete", 200, 1, "", "")});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_FALSE(out[0].has_column("__version"));
    EXPECT_FALSE(out[1].has_column("__version"));
}

TEST(SqlCdcKeepRows, TheRowsAreRestoredFromACheckpoint) {
    InMemoryStateBackend first;
    {
        Stage s(Mode::Upsert, false, &first);
        (void)s.feed({change("insert", 100, 1, "new", "kept body")});
    }
    const auto snap = first.snapshot(CheckpointId{1});
    InMemoryStateBackend second;
    second.restore(snap);
    Stage s(Mode::Upsert, false, &second);
    auto out = s.feed({with_unchanged(change("update", 200, 1, "paid"), {"body"})});
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(kind(out[0]), "update_before");
    EXPECT_EQ(str(out[0], "status"), "new");
    EXPECT_EQ(str(out[1], "body"), "kept body");
    // The restored version still guards against a replay.
    EXPECT_TRUE(s.feed({change("update", 100, 1, "replayed")}).empty());
}

TEST(SqlCdcKeepRows, TheFactoryChecksItsParamsAndRefusesRowsFromElsewhere) {
    cluster::ensure_built_ins_registered();
    plugin::PluginRegistry registry;
    install(registry);
    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        "cdc_keep_rows_row", std::string{kChannelRow}, std::string{kChannelRow});
    ASSERT_NE(factory, nullptr);
    cluster::OperatorBuildContext ctx;
    ctx.params = {{"key_columns", "id"}, {"changelog_mode", "retract"}};
    EXPECT_THROW((void)factory->build(ctx), std::runtime_error);
    ctx.params = {{"changelog_mode", "upsert"}};
    EXPECT_THROW((void)factory->build(ctx), std::runtime_error);  // no key
    ctx.params = {{"key_columns", "id"}, {"changelog_mode", "upsert"}, {"unheld_deletes", "keep"}};
    EXPECT_THROW((void)factory->build(ctx), std::runtime_error);

    Stage s(Mode::Upsert, false);
    Row plain;
    plain.values["id"] = config::JsonValue{1};
    EXPECT_THROW((void)s.feed({plain}), std::runtime_error);
}
