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
