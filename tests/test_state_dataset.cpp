// Checkpoint datasets (clink/state_processor/checkpoint_dataset.hpp): one
// completed checkpoint of a whole job exported, read back and compared.
//
// Every snapshot here is written by SnapshotArrowWriter, the writer every
// state producer uses, with the sidecar and completion marker the engine
// writes, so the export reads exactly what a job leaves behind.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/config/json.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/checkpoint_dataset.hpp"

namespace fs = std::filesystem;
namespace sp = clink::state_processor;

namespace {

constexpr std::uint64_t kDatasetJob = 11;
constexpr std::uint64_t kCounts = 42;   // a keyed operator
constexpr std::uint64_t kSource = 900;  // an operator holding operator state

std::string keyed(std::uint8_t kg, const std::string& slot, const std::string& user_key) {
    return std::string(1, static_cast<char>(kg)) + slot + "|" + user_key;
}

std::string op_state(const std::string& slot, const std::string& user_key) {
    return std::string(1, '\xff') + slot + "|" + user_key;
}

struct StoredRow {
    std::uint64_t op;
    std::string key;
    std::string value;
};

struct Stamps {
    std::vector<std::tuple<std::uint64_t, std::string, std::uint32_t, std::string>> versions;
    std::vector<std::tuple<std::uint64_t, std::string, std::uint64_t>> fingerprints;
};

// A keyed entry as the test describes it; who holds it follows from the key
// group and the parallelism.
struct KeyedEntry {
    std::uint8_t kg;
    std::string user_key;
    std::string value;
};

std::uint32_t owner(std::uint8_t kg, std::uint32_t parallelism) {
    return static_cast<std::uint32_t>(kg) * parallelism / 128;
}

class StateDatasetTest : public ::testing::Test {
protected:
    fs::path root;
    fs::path ckpt;

    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        root = fs::temp_directory_path() /
               ("clink_state_dataset_" + std::to_string(::getpid()) + "_" + info->name());
        fs::remove_all(root);
        ckpt = root / "ckpt";
        fs::create_directories(ckpt);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    [[nodiscard]] std::string state_root() const {
        return clink::cluster::job_state_root(ckpt.string(), kDatasetJob);
    }

    [[nodiscard]] fs::path snapshot_path(std::uint64_t id,
                                         std::uint32_t sub,
                                         std::uint32_t generation) const {
        return fs::path{clink::state_dir_for(state_root(), generation, sub)} /
               ("checkpoint-" + std::to_string(id) + ".snap");
    }

    void write_snapshot(std::uint64_t id,
                        std::uint32_t sub,
                        const std::vector<StoredRow>& rows,
                        std::uint32_t generation = 1,
                        const Stamps& stamps = {}) const {
        clink::SnapshotArrowWriter w;
        for (const auto& r : rows) {
            w.append(r.op, r.key, r.value);
        }
        clink::StateVersionMap versions;
        for (const auto& [op, type, v, slot] : stamps.versions) {
            versions.set(clink::OperatorId{op}, type, v, slot);
        }
        clink::StateFingerprintMap fps;
        for (const auto& [op, slot, fp] : stamps.fingerprints) {
            fps.set(clink::OperatorId{op}, slot, fp);
        }
        const auto bytes = w.finish(versions, fps);
        const auto path = snapshot_path(id, sub, generation);
        fs::create_directories(path.parent_path());
        std::ofstream(path, std::ios::binary | std::ios::trunc)
            .write(reinterpret_cast<const char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
        clink::state::write_checkpoint_meta(path, id, bytes.data(), bytes.size());
    }

    void write_marker(std::uint64_t id,
                      const std::set<std::uint32_t>& subs,
                      std::uint32_t generation = 1,
                      const std::optional<std::string>& state = std::nullopt) const {
        const auto body = clink::cluster::completed_marker_body(
            kDatasetJob,
            id,
            generation,
            subs,
            state.value_or(
                clink::cluster::recorded_state_location(ckpt.string(), kDatasetJob, state_root())));
        const auto dir = clink::cluster::completed_marker_dir_for(ckpt.string(), kDatasetJob);
        fs::create_directories(dir);
        std::ofstream(dir / ("COMPLETED-" + std::to_string(id)), std::ios::trunc) << body;
    }

    void confirm(std::uint64_t id) const {
        const auto dir = clink::cluster::completed_marker_dir_for(ckpt.string(), kDatasetJob);
        std::ofstream(dir / ("CONFIRMED-" + std::to_string(id)), std::ios::trunc)
            << "checkpoint=" << id << "\n";
    }

    // A whole checkpoint: keyed entries spread over `parallelism` subtasks by
    // key group, and each subtask's source position as operator state.
    void write_job_checkpoint(std::uint64_t id,
                              const std::vector<KeyedEntry>& entries,
                              std::uint32_t parallelism = 2,
                              std::uint32_t generation = 1,
                              const Stamps& stamps = {}) const {
        std::map<std::uint32_t, std::vector<StoredRow>> per;
        for (std::uint32_t p = 0; p < parallelism; ++p) {
            per[p].push_back(
                StoredRow{kSource, op_state("offsets", "partition"), "pos-" + std::to_string(id)});
        }
        for (const auto& e : entries) {
            per[owner(e.kg, parallelism)].push_back(
                StoredRow{kCounts, keyed(e.kg, "counts", e.user_key), e.value});
        }
        std::set<std::uint32_t> subs;
        for (const auto& [p, rows] : per) {
            write_snapshot(id, p, rows, generation, stamps);
            subs.insert(p);
        }
        write_marker(id, subs, generation);
    }

    [[nodiscard]] sp::DatasetOptions options(const std::string& out,
                                             std::optional<std::uint64_t> checkpoint) const {
        sp::DatasetOptions o;
        o.checkpoint_dir = ckpt.string();
        o.job = kDatasetJob;
        o.commit_confirmation = false;
        o.checkpoint = checkpoint;
        o.out = (root / out).string();
        return o;
    }

    sp::Dataset export_open(const std::string& out, std::uint64_t checkpoint) const {
        (void)sp::export_checkpoint_dataset(options(out, checkpoint));
        return sp::open_dataset(root / out);
    }

    [[nodiscard]] static clink::config::JsonValue doc(const fs::path& dataset) {
        std::ifstream in(dataset / sp::kDatasetFile);
        std::ostringstream s;
        s << in.rdbuf();
        return clink::config::parse(s.str());
    }

    static std::vector<sp::DatasetEntry> entries_of(const sp::Dataset& d) {
        std::vector<sp::DatasetEntry> out;
        sp::for_each_entry(d, [&](const sp::DatasetEntry& e) {
            out.push_back(e);
            return true;
        });
        return out;
    }

    static const sp::SlotComparison* slot(const sp::DatasetDiff& r,
                                          std::uint64_t op,
                                          const std::string& name,
                                          const std::string& scope) {
        for (const auto& s : r.slots) {
            if (s.op == op && s.slot == name && s.scope == scope) {
                return &s;
            }
        }
        return nullptr;
    }

    static std::string refusal(const sp::DatasetOptions& o, std::string* kind = nullptr) {
        try {
            (void)sp::export_checkpoint_dataset(o);
        } catch (const sp::DatasetRefused& e) {
            if (kind != nullptr) {
                *kind = e.kind;
            }
            return e.what();
        }
        return {};
    }
};

const std::vector<KeyedEntry> kBase = {
    {3, "alice", "a1"}, {40, "bob", "b1"}, {70, "carol", "c1"}, {120, "dave", "d1"}};

// ---- export ------------------------------------------------------------------

TEST_F(StateDatasetTest, ExportsEveryParticipantOfTheCheckpointAsOneDataset) {
    write_job_checkpoint(1, kBase);
    const auto r = sp::export_checkpoint_dataset(options("ds", 1));
    EXPECT_TRUE(r.complete);
    EXPECT_EQ(r.participants, 2U);
    EXPECT_EQ(r.entries, 6U);  // four keyed entries and one position per subtask

    const auto d = sp::open_dataset(root / "ds");
    EXPECT_TRUE(d.complete);
    EXPECT_EQ(d.job, kDatasetJob);
    EXPECT_EQ(d.checkpoint, 1U);
    const auto all = entries_of(d);
    ASSERT_EQ(all.size(), 6U);
    // The two subtasks' positions share an encoded key and are both kept.
    std::set<std::uint32_t> holders;
    for (const auto& e : all) {
        if (e.op == kSource) {
            EXPECT_EQ(sp::scope_name(e.scope), std::string{"operator"});
            EXPECT_EQ(e.slot, "offsets");
            EXPECT_EQ(e.user_key, "partition");
            EXPECT_EQ(e.key_group, -1);
            holders.insert(e.participant);
        } else {
            EXPECT_EQ(e.op, kCounts);
            EXPECT_EQ(e.participant, owner(static_cast<std::uint8_t>(e.key_group), 2));
        }
    }
    EXPECT_EQ(holders, (std::set<std::uint32_t>{0, 1}));
    // Identity order: keyed rows by key group, whatever subtask holds them.
    std::vector<std::string> keys;
    for (const auto& e : all) {
        if (e.op == kCounts) {
            keys.push_back(e.user_key);
        }
    }
    EXPECT_EQ(keys, (std::vector<std::string>{"alice", "bob", "carol", "dave"}));

    const auto j = doc(root / "ds");
    EXPECT_EQ(j.at("format").as_string(), sp::kDatasetFormat);
    EXPECT_EQ(j.at("checkpoint").at("status").as_string(), "completed");
    EXPECT_EQ(j.at("selection").at("rule").as_string(), "named");
    EXPECT_EQ(j.at("participants").as_array().size(), 2U);
    EXPECT_EQ(j.at("participants").as_array()[0].at("sidecar").as_string(), "valid");
    EXPECT_NE(j.at("in_doubt").at("external_outcomes").as_string().find("not verified"),
              std::string::npos);
    EXPECT_FALSE(fs::exists(root / ".ds.dataset-incomplete"));
}

TEST_F(StateDatasetTest, KeysOutsideTheLayoutAreKeptExactly) {
    write_snapshot(1,
                   0,
                   {{kSource, std::string("\xff__off__:0", 10), "p0"},
                    {kCounts, "plainkey", "v"},
                    {kCounts, keyed(5, "counts", "k"), "v5"}});
    write_marker(1, {0});
    const auto d = export_open("ds", 1);
    ASSERT_TRUE(d.complete);
    std::map<std::string, sp::DatasetEntry> by_scope;
    for (const auto& e : entries_of(d)) {
        by_scope[sp::scope_name(e.scope)] = e;
    }
    // A 0xFF key with no slot separator is operator state under <raw>.
    EXPECT_EQ(by_scope["operator"].slot, "<raw>");
    EXPECT_EQ(by_scope["operator"].user_key, "__off__:0");
    // A key that follows no layout is raw, whole.
    EXPECT_EQ(by_scope["raw"].slot, "<raw>");
    EXPECT_EQ(by_scope["raw"].user_key, "plainkey");
    EXPECT_EQ(by_scope["keyed"].key_group, 5);
}

TEST_F(StateDatasetTest, TheRestorePointIsTheEnginesChoiceAndConfirmationIsRecorded) {
    write_job_checkpoint(1, kBase);
    write_job_checkpoint(2, kBase);
    auto o = options("rp", std::nullopt);
    const auto r = sp::export_checkpoint_dataset(o);
    EXPECT_EQ(r.checkpoint, 2U);
    auto j = doc(root / "rp");
    EXPECT_EQ(j.at("selection").at("rule").as_string(), "restore-point");
    EXPECT_FALSE(j.at("in_doubt").at("needs_resolution").as_bool());

    // Sinks that need confirmation: the restore point is the newest
    // confirmed checkpoint, and a completed one that is not confirmed says it
    // still needs in-doubt resolution.
    confirm(1);
    auto oc = options("confirmed", std::nullopt);
    oc.commit_confirmation = true;
    EXPECT_EQ(sp::export_checkpoint_dataset(oc).checkpoint, 1U);
    j = doc(root / "confirmed");
    EXPECT_EQ(j.at("checkpoint").at("status").as_string(), "confirmed");
    EXPECT_FALSE(j.at("in_doubt").at("needs_resolution").as_bool());

    auto on = options("unconfirmed", 2);
    on.commit_confirmation = true;
    (void)sp::export_checkpoint_dataset(on);
    j = doc(root / "unconfirmed");
    EXPECT_EQ(j.at("checkpoint").at("status").as_string(), "completed");
    EXPECT_TRUE(j.at("in_doubt").at("needs_resolution").as_bool());
}

TEST_F(StateDatasetTest, AMissingParticipantIsNeverOmittedSilently) {
    write_job_checkpoint(1, kBase);
    fs::remove(snapshot_path(1, 1, 1));
    std::string kind;
    const auto why = refusal(options("ds", 1), &kind);
    EXPECT_EQ(kind, "incomplete");
    EXPECT_NE(why.find("participant 1"), std::string::npos) << why;
    EXPECT_FALSE(fs::exists(root / "ds"));

    auto o = options("partial", 1);
    o.allow_partial = true;
    const auto r = sp::export_checkpoint_dataset(o);
    EXPECT_FALSE(r.complete);
    const auto d = sp::open_dataset(root / "partial");
    EXPECT_FALSE(d.complete);
    const auto j = doc(root / "partial");
    EXPECT_FALSE(j.at("complete").as_bool());
    bool named = false;
    for (const auto& s : j.at("incomplete").as_array()) {
        named = named || s.as_string().find("participant 1") != std::string::npos;
    }
    EXPECT_TRUE(named);
    // Unsupported is not claimed for what is simply missing.
    EXPECT_TRUE(j.at("unsupported").as_array().empty());
}

TEST_F(StateDatasetTest, OnlyTheMarkersParticipantsAndGenerationAreRead) {
    write_job_checkpoint(1, kBase, 2, /*generation=*/2);
    // A third subtask's file the marker does not name, and the same id in an
    // older generation, are not part of the checkpoint.
    write_snapshot(1, 2, {{kCounts, keyed(9, "counts", "intruder"), "x"}}, 2);
    write_snapshot(1, 0, {{kCounts, keyed(9, "counts", "stale"), "x"}}, 1);
    const auto d = export_open("ds", 1);
    EXPECT_TRUE(d.complete);
    EXPECT_EQ(d.generation, 2U);
    for (const auto& e : entries_of(d)) {
        EXPECT_NE(e.user_key, "intruder");
        EXPECT_NE(e.user_key, "stale");
    }
    EXPECT_EQ(d.participants.size(), 2U);
}

TEST_F(StateDatasetTest, AKeyGroupHeldByTwoParticipantsIsRefused) {
    write_snapshot(1, 0, {{kCounts, keyed(5, "counts", "a"), "1"}});
    write_snapshot(1, 1, {{kCounts, keyed(5, "counts", "b"), "2"}});
    write_marker(1, {0, 1});
    std::string kind;
    const auto why = refusal(options("ds", 1), &kind);
    EXPECT_EQ(kind, "incomplete");
    EXPECT_NE(why.find("key group 5"), std::string::npos) << why;
}

TEST_F(StateDatasetTest, ACorruptSnapshotIsRefused) {
    write_job_checkpoint(1, kBase);
    const auto p = snapshot_path(1, 0, 1);
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(20);
    f.put('\x7f');
    f.close();
    const auto why = refusal(options("ds", 1));
    EXPECT_NE(why.find("checkpoint.participants"), std::string::npos) << why;
    EXPECT_FALSE(fs::exists(root / "ds"));
}

TEST_F(StateDatasetTest, AChangeDuringCollectionPublishesNothing) {
    write_job_checkpoint(1, kBase);
    auto o = options("ds", 1);
    o.after_copy = [&](const fs::path& source) {
        if (source.filename() == "checkpoint-1.snap") {
            std::ofstream(source, std::ios::app) << "more";
        }
    };
    std::string kind;
    (void)refusal(o, &kind);
    EXPECT_EQ(kind, "changed");
    EXPECT_FALSE(fs::exists(root / "ds"));
    for (const auto& e : fs::directory_iterator(root)) {
        EXPECT_EQ(e.path().filename().string().find(".ds."), std::string::npos)
            << "left behind: " << e.path();
    }
}

TEST_F(StateDatasetTest, StateOutsideTheCheckpointDirectoryIsUnsupported) {
    write_job_checkpoint(1, kBase);
    write_marker(1, {0, 1}, 1, std::string{"rocksdb:///elsewhere/state"});
    std::string kind;
    const auto why = refusal(options("ds", 1), &kind);
    EXPECT_EQ(kind, "unsupported") << why;
}

TEST_F(StateDatasetTest, AnEntryBoundMakesAPartialDatasetNeverATruncatedComplete) {
    write_job_checkpoint(1, kBase);
    auto o = options("ds", 1);
    o.max_entries = 2;
    std::string kind;
    const auto why = refusal(o, &kind);
    EXPECT_EQ(kind, "incomplete");
    EXPECT_NE(why.find("--max-entries"), std::string::npos) << why;
    o.allow_partial = true;
    const auto r = sp::export_checkpoint_dataset(o);
    EXPECT_FALSE(r.complete);
    EXPECT_LE(r.entries, 2U);
}

// ---- reading -----------------------------------------------------------------

TEST_F(StateDatasetTest, AMovedDatasetReadsTheSameAndADamagedOneIsRefused) {
    write_job_checkpoint(1, kBase);
    const auto before = entries_of(export_open("ds", 1));
    fs::rename(root / "ds", root / "moved");
    const auto after = entries_of(sp::open_dataset(root / "moved"));
    ASSERT_EQ(after.size(), before.size());
    for (std::size_t i = 0; i < after.size(); ++i) {
        EXPECT_EQ(after[i].user_key, before[i].user_key);
        EXPECT_EQ(after[i].value, before[i].value);
        EXPECT_EQ(after[i].participant, before[i].participant);
    }

    fs::copy(root / "moved", root / "damaged", fs::copy_options::recursive);
    std::ofstream(root / "damaged" / sp::kEntriesFile, std::ios::app) << "x";
    EXPECT_THROW((void)sp::open_dataset(root / "damaged"), sp::DatasetInvalid);

    fs::copy(root / "moved", root / "linked", fs::copy_options::recursive);
    fs::remove(root / "linked" / sp::kEntriesFile);
    fs::create_symlink(root / "moved" / sp::kEntriesFile, root / "linked" / sp::kEntriesFile);
    EXPECT_THROW((void)sp::open_dataset(root / "linked"), sp::DatasetInvalid);

    fs::create_directory_symlink(root / "moved", root / "dirlink");
    EXPECT_THROW((void)sp::open_dataset(root / "dirlink"), sp::DatasetInvalid);
}

// ---- comparing ---------------------------------------------------------------

TEST_F(StateDatasetTest, ADiffCountsWhatWasAddedRemovedAndChanged) {
    write_job_checkpoint(1, kBase);
    write_job_checkpoint(2,
                         {{3, "alice", "a1"},    // unchanged
                          {40, "bob", "b2"},     // changed
                          {120, "dave", "d1"},   // unchanged; carol removed
                          {90, "erin", "e1"}});  // added
    const auto a = export_open("a", 1);
    const auto b = export_open("b", 2);
    const auto r = sp::diff_datasets(a, b, {});
    EXPECT_TRUE(r.established);
    const auto* counts = slot(r, kCounts, "counts", "keyed");
    ASSERT_NE(counts, nullptr);
    EXPECT_EQ(counts->status, "compared");
    EXPECT_EQ(counts->added, 1U);
    EXPECT_EQ(counts->removed, 1U);
    EXPECT_EQ(counts->changed, 1U);
    EXPECT_EQ(counts->unchanged, 2U);
    EXPECT_EQ(counts->relocated, 0U);
    // Each subtask's position changed, compared with its own.
    const auto* pos = slot(r, kSource, "offsets", "operator");
    ASSERT_NE(pos, nullptr);
    EXPECT_EQ(pos->status, "compared");
    EXPECT_EQ(pos->changed, 2U);
    EXPECT_EQ(r.exit_code(), 1);
    ASSERT_FALSE(counts->samples.empty());
    // Samples are hex with lengths, never a typed reading.
    const auto json = clink::config::parse(r.render_json());
    EXPECT_EQ(json.at("format").as_string(), sp::kStateDiffFormat);
    EXPECT_EQ(json.at("totals").at("added").as_int(), 1);

    const auto same = sp::diff_datasets(a, export_open("a2", 1), {});
    EXPECT_TRUE(same.established);
    EXPECT_FALSE(same.differences());
    EXPECT_EQ(same.exit_code(), 0);
}

TEST_F(StateDatasetTest, ARescaleRelocatesEntriesWithoutChangingThem) {
    write_job_checkpoint(1, kBase, /*parallelism=*/2, /*generation=*/1);
    // The same entries after a rescale to four subtasks, in a new generation.
    write_job_checkpoint(1 + 1, kBase, /*parallelism=*/4, /*generation=*/2);
    const auto a = export_open("a", 1);
    const auto b = export_open("b", 2);
    const auto r = sp::diff_datasets(a, b, {});
    const auto* counts = slot(r, kCounts, "counts", "keyed");
    ASSERT_NE(counts, nullptr);
    EXPECT_EQ(counts->status, "compared");
    EXPECT_EQ(counts->added + counts->removed + counts->changed, 0U);
    EXPECT_EQ(counts->unchanged, 4U);
    EXPECT_GT(counts->relocated, 0U);
    // Operator state belongs to the subtask that wrote it: not comparable.
    const auto* pos = slot(r, kSource, "offsets", "operator");
    ASSERT_NE(pos, nullptr);
    EXPECT_EQ(pos->status, "subtasks-changed");
    EXPECT_EQ(pos->added + pos->removed + pos->changed, 0U);
    EXPECT_FALSE(r.established);
    EXPECT_EQ(r.exit_code(), 3);
}

TEST_F(StateDatasetTest, ASchemaChangeIsNotComparedUnlessRawBytesAreAskedFor) {
    Stamps v1;
    v1.versions.emplace_back(kCounts, "Count", 1, "counts");
    v1.fingerprints.emplace_back(kCounts, "counts", 0x1111);
    Stamps v2;
    v2.versions.emplace_back(kCounts, "Count", 2, "counts");
    v2.fingerprints.emplace_back(kCounts, "counts", 0x2222);
    write_job_checkpoint(1, kBase, 2, 1, v1);
    write_job_checkpoint(2, kBase, 2, 1, v2);
    const auto a = export_open("a", 1);
    const auto b = export_open("b", 2);

    const auto r = sp::diff_datasets(a, b, {});
    const auto* counts = slot(r, kCounts, "counts", "keyed");
    ASSERT_NE(counts, nullptr);
    EXPECT_EQ(counts->status, "schema-changed");
    EXPECT_TRUE(counts->schema_changed);
    EXPECT_EQ(counts->unchanged + counts->changed, 0U);
    EXPECT_NE(counts->note.find("Count v1"), std::string::npos) << counts->note;
    EXPECT_NE(counts->note.find("Count v2"), std::string::npos) << counts->note;
    EXPECT_EQ(r.exit_code(), 3);

    sp::StateDiffOptions raw;
    raw.raw_bytes = true;
    const auto rr = sp::diff_datasets(a, b, raw);
    const auto* rc = slot(rr, kCounts, "counts", "keyed");
    ASSERT_NE(rc, nullptr);
    EXPECT_EQ(rc->status, "raw-compared");
    EXPECT_EQ(rc->unchanged, 4U);
    // Identical bytes under a changed schema are still a difference.
    EXPECT_TRUE(rr.differences());
    EXPECT_EQ(rr.exit_code(), 1);
    const auto json = clink::config::parse(rr.render_json());
    bool labelled = false;
    for (const auto& s : json.at("slots").as_array()) {
        if (s.at("slot").as_string() == "counts") {
            labelled = s.at("comparison").as_string() == "raw-bytes";
        }
    }
    EXPECT_TRUE(labelled);
}

TEST_F(StateDatasetTest, ALimitNeverReportsNoDifferences) {
    write_job_checkpoint(1, kBase);
    const auto a = export_open("a", 1);
    const auto b = export_open("b", 1);
    sp::StateDiffOptions o;
    o.max_entries = 3;
    const auto r = sp::diff_datasets(a, b, o);
    EXPECT_TRUE(r.limit_reached);
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.differences());
    EXPECT_EQ(r.exit_code(), 3);
}

TEST_F(StateDatasetTest, APartialSideIsNotEstablished) {
    write_job_checkpoint(1, kBase);
    write_job_checkpoint(2, kBase);
    fs::remove(snapshot_path(2, 0, 1));
    auto o = options("b", 2);
    o.allow_partial = true;
    (void)sp::export_checkpoint_dataset(o);
    const auto a = export_open("a", 1);
    const auto b = sp::open_dataset(root / "b");
    const auto refused = sp::diff_datasets(a, b, {});
    EXPECT_EQ(refused.exit_code(), 3);
    EXPECT_TRUE(refused.slots.empty());
    sp::StateDiffOptions allow;
    allow.allow_partial = true;
    const auto r = sp::diff_datasets(a, b, allow);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_FALSE(r.slots.empty());
}

}  // namespace
