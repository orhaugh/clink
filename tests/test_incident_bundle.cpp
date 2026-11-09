// Incident evidence bundles (design record 014): collection, the reader's
// refusals and the checks that need no SQL runtime, each against the mutation
// it exists to catch. Checkpoints are written the way the engine writes them
// (completed_marker_body, write_checkpoint_meta, record_restore_point), so the
// checks read the engine's own layout.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/config/json.hpp"
#include "clink/core/codec.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"

namespace fs = std::filesystem;
using namespace clink::incident;
using clink::config::JsonValue;

namespace {

constexpr JobId kIncidentJob = 7;

std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

void spill(const fs::path& p, const std::string& body) {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary | std::ios::trunc) << body;
}

Verdict verdict_of(const std::vector<Check>& checks, const std::string& id) {
    for (const auto& c : checks) {
        if (c.id == id) {
            return c.verdict;
        }
    }
    ADD_FAILURE() << "no check " << id;
    return Verdict::Unavailable;
}

std::string reason_of(const std::vector<Check>& checks, const std::string& id) {
    for (const auto& c : checks) {
        if (c.id == id) {
            std::string all = c.reason;
            for (const auto& d : c.details) {
                all += "\n" + d;
            }
            return all;
        }
    }
    return {};
}

class IncidentBundleTest : public ::testing::Test {
protected:
    fs::path root;
    fs::path ckpt;

    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        root = fs::temp_directory_path() /
               ("clink_incident_" + std::to_string(::getpid()) + "_" + info->name());
        fs::remove_all(root);
        ckpt = root / "ckpt";
        fs::create_directories(ckpt);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    [[nodiscard]] std::string state_root() const {
        return clink::cluster::job_state_root(ckpt.string(), kIncidentJob);
    }

    [[nodiscard]] fs::path snapshot_path(std::uint64_t id,
                                         std::uint32_t sub,
                                         std::uint32_t generation = 1) const {
        return fs::path{clink::state_dir_for(state_root(), generation, sub)} /
               ("checkpoint-" + std::to_string(id) + ".snap");
    }

    // A checkpoint as the engine writes one: each participant's snapshot, then
    // its integrity sidecar, then the COMPLETED marker naming the generation,
    // the participants and the state root.
    void write_checkpoint(std::uint64_t id,
                          const std::set<std::uint32_t>& subs = {0, 1},
                          std::uint32_t generation = 1,
                          const std::set<std::uint32_t>& marker_subs = {}) const {
        for (const auto sub : subs) {
            auto sp = clink::state_processor::Savepoint::create();
            auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
                clink::OperatorId{42}, "counts", clink::int64_codec(), clink::int64_codec());
            ks.put(static_cast<std::int64_t>(sub), static_cast<std::int64_t>(id * 100));
            const auto snap = sp.snapshot(clink::CheckpointId{id});
            const auto path = snapshot_path(id, sub, generation);
            std::string bytes(snap.bytes.size(), '\0');
            for (std::size_t i = 0; i < bytes.size(); ++i) {
                bytes[i] = static_cast<char>(snap.bytes[i]);
            }
            spill(path, bytes);
            clink::state::write_checkpoint_meta(path, id, snap.bytes.data(), snap.bytes.size());
        }
        write_marker(
            "COMPLETED-" + std::to_string(id),
            clink::cluster::completed_marker_body(kIncidentJob,
                                                  id,
                                                  generation,
                                                  marker_subs.empty() ? subs : marker_subs,
                                                  clink::cluster::recorded_state_location(
                                                      ckpt.string(), kIncidentJob, state_root())));
    }

    void write_marker(const std::string& name, const std::string& body) const {
        spill(clink::cluster::completed_marker_dir_for(ckpt.string(), kIncidentJob) / name, body);
    }

    [[nodiscard]] CollectOptions options(const std::string& out_name = "bundle") const {
        CollectOptions o;
        o.checkpoint_dir = ckpt.string();
        o.job = kIncidentJob;
        o.commit_confirmation = false;
        o.include_state = true;
        o.out = (root / out_name).string();
        return o;
    }

    static void edit_manifest(const fs::path& bundle, const std::function<void(JsonValue&)>& f) {
        auto m = clink::config::parse(slurp(bundle / "manifest.json"));
        f(m);
        spill(bundle / "manifest.json", m.serialize(2));
    }

    // Re-list an artifact's size and digest after a test changed it, so the
    // change reaches the checks behind bundle.artifacts, as evidence that was
    // already wrong when it was collected would.
    static void relist(const fs::path& bundle, const std::string& rel) {
        edit_manifest(bundle, [&](JsonValue& m) {
            for (auto& a : m.as_object()["artifacts"].as_array()) {
                if (a.string_or("path", "") == rel) {
                    a.as_object()["bytes"] =
                        JsonValue{static_cast<std::int64_t>(fs::file_size(bundle / rel))};
                    a.as_object()["sha256"] = JsonValue{sha256_file(bundle / rel)};
                }
            }
        });
    }

    static void add_listed(const fs::path& bundle,
                           const std::string& rel,
                           const std::string& role,
                           const std::string& body) {
        spill(bundle / rel, body);
        edit_manifest(bundle, [&](JsonValue& m) {
            clink::config::JsonObject a;
            a["path"] = JsonValue{rel};
            a["role"] = JsonValue{role};
            a["bytes"] = JsonValue{static_cast<std::int64_t>(body.size())};
            a["sha256"] = JsonValue{sha256_file(bundle / rel)};
            m.as_object()["artifacts"].as_array().emplace_back(std::move(a));
        });
    }

    static std::vector<Check> verify(const fs::path& bundle,
                                     const std::string& profile = "consistency") {
        VerifyOptions o;
        o.profile = profile;
        return verify_core(read_bundle(bundle), o);
    }

    static int exit_code_of(const fs::path& bundle, const std::string& profile) {
        Report r;
        r.bundle = bundle.string();
        r.profile = profile;
        r.required = required_checks(profile, false);
        r.checks = verify(bundle, profile);
        return r.exit_code();
    }

    // Nothing but the published bundle (if any) beside the output: no
    // half-written temporary directory survives a refusal.
    [[nodiscard]] std::vector<std::string> leftovers() const {
        std::vector<std::string> out;
        for (const auto& e : fs::directory_iterator(root)) {
            const auto name = e.path().filename().string();
            if (name.find(".incomplete-") != std::string::npos) {
                out.push_back(name);
            }
        }
        return out;
    }
};

// ---- the positive path ----------------------------------------------------------

TEST_F(IncidentBundleTest, CollectsTheEnginesRestorePointAndVerifiesItAfterAMove) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_checkpoint(3);
    const auto r = collect(options());
    EXPECT_EQ(r.checkpoint, 3u);
    EXPECT_TRUE(fs::exists(root / "bundle" / "manifest.json"));
    EXPECT_TRUE(leftovers().empty());

    // Moved somewhere else, with the job's own directory gone, it still verifies:
    // nothing in it points back at the machine it came from.
    const auto moved = root / "elsewhere" / "incident";
    fs::create_directories(moved.parent_path());
    fs::rename(root / "bundle", moved);
    fs::remove_all(ckpt);
    const auto checks = verify(moved);
    for (const auto& id : required_checks("consistency", false)) {
        EXPECT_EQ(verdict_of(checks, id), Verdict::Passed) << id << ": " << reason_of(checks, id);
    }
    EXPECT_EQ(exit_code_of(moved, "consistency"), 0);
}

TEST_F(IncidentBundleTest, TheRestoredRecordSteersTheChoiceAsATakeoverWould) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_checkpoint(3);
    // A deploy restored from 1 and numbered from 4: 2 and 3 were passed over.
    ASSERT_TRUE(clink::cluster::record_restore_point(
        ckpt.string(), kIncidentJob, 1, {.restore = 1, .next = 4}));
    EXPECT_EQ(collect(options()).checkpoint, 1u);
    EXPECT_EQ(verdict_of(verify(root / "bundle"), "checkpoint.restore_rule"), Verdict::Passed);
}

TEST_F(IncidentBundleTest, TheConfirmationFamilyRestoresFromTheNewestConfirmedCheckpoint) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_checkpoint(3);
    write_marker("CONFIRMED-2", "");
    auto o = options();
    o.commit_confirmation = true;
    EXPECT_EQ(collect(o).checkpoint, 2u);
    const auto checks = verify(root / "bundle");
    EXPECT_EQ(verdict_of(checks, "checkpoint.restore_rule"), Verdict::Passed);
    EXPECT_NE(reason_of(checks, "checkpoint.restore_rule").find("in-doubt"), std::string::npos)
        << "the walk a takeover runs for this family cannot be predicted offline, and the "
           "check says so";
}

TEST_F(IncidentBundleTest,
       WithoutStateTheParticipantsAreCheckedOnTheirSidecarsAndTheResultIsPartial) {
    write_checkpoint(1);
    auto o = options();
    o.include_state = false;
    (void)collect(o);
    const auto checks = verify(root / "bundle");
    EXPECT_EQ(verdict_of(checks, "checkpoint.participants"), Verdict::Passed);
    EXPECT_EQ(verdict_of(checks, "state.decode"), Verdict::Unavailable);
    EXPECT_EQ(exit_code_of(root / "bundle", "consistency"), 3);
    EXPECT_EQ(exit_code_of(root / "bundle", "integrity"), 0);
}

// ---- collection refuses rather than guess or mix -------------------------------

TEST_F(IncidentBundleTest, CollectionRefusesToGuessTheRestoreRule) {
    write_checkpoint(1);
    auto o = options();
    o.commit_confirmation.reset();
    EXPECT_THROW((void)collect(o), CollectError);
    EXPECT_FALSE(fs::exists(root / "bundle"));
    EXPECT_TRUE(leftovers().empty());
    o.checkpoint = 1;  // naming the checkpoint is an answer
    EXPECT_EQ(collect(o).checkpoint, 1u);
}

TEST_F(IncidentBundleTest, CollectionNeverWritesOverAnExistingDirectory) {
    write_checkpoint(1);
    spill(root / "bundle" / "keep.txt", "mine");
    EXPECT_THROW((void)collect(options()), CollectError);
    EXPECT_EQ(slurp(root / "bundle" / "keep.txt"), "mine");
    EXPECT_TRUE(leftovers().empty());
}

TEST_F(IncidentBundleTest, AFileRewrittenWhileItIsCopiedRefusesTheWholeBundle) {
    write_checkpoint(1);
    auto o = options();
    const auto target = snapshot_path(1, 0);
    o.after_copy = [&](const fs::path& source) {
        if (source == target) {
            std::ofstream(source, std::ios::binary | std::ios::app) << "x";
        }
    };
    try {
        (void)collect(o);
        FAIL() << "a changed source was published";
    } catch (const CollectError& e) {
        EXPECT_NE(std::string{e.what()}.find(target.filename().string()), std::string::npos)
            << e.what();
    }
    EXPECT_FALSE(fs::exists(root / "bundle"));
    EXPECT_TRUE(leftovers().empty());
}

TEST_F(IncidentBundleTest, AFileRemovedWhileItIsCopiedRefusesTheWholeBundle) {
    write_checkpoint(1);
    auto o = options();
    const auto target = snapshot_path(1, 1);
    o.after_copy = [&](const fs::path& source) {
        if (source == target) {
            fs::remove(source);
        }
    };
    EXPECT_THROW((void)collect(o), CollectError);
    EXPECT_FALSE(fs::exists(root / "bundle"));
    EXPECT_TRUE(leftovers().empty());
}

TEST_F(IncidentBundleTest, ACheckpointCompletingDuringCollectionRefusesTheBundle) {
    write_checkpoint(1);
    auto o = options();
    bool once = false;
    o.after_copy = [&](const fs::path&) {
        if (!once) {
            once = true;
            write_checkpoint(2);
        }
    };
    try {
        (void)collect(o);
        FAIL() << "a bundle mixing two marker sets was published";
    } catch (const CollectError& e) {
        EXPECT_NE(std::string{e.what()}.find("marker"), std::string::npos) << e.what();
    }
    EXPECT_FALSE(fs::exists(root / "bundle"));
    EXPECT_TRUE(leftovers().empty());
}

TEST_F(IncidentBundleTest, RequiredEvidencePastABoundRefusesAndOptionalEvidenceIsListedAsExcluded) {
    write_checkpoint(1);
    auto o = options();
    o.max_files = 2;
    EXPECT_THROW((void)collect(o), CollectError);
    EXPECT_FALSE(fs::exists(root / "bundle"));

    // Captures are optional: past the bound they are left out and listed.
    const auto cap = root / "capture" / "op-5" / "subtask-0";
    spill(cap / "op.json", R"({"op_type":"identity_row","in_channel":"row","out_channel":"row"})");
    spill(cap / "epoch-1.cap", std::string(64, 'c'));
    spill(cap / "epoch-2.cap", std::string(64, 'c'));
    o = options();
    o.capture_dir = (root / "capture").string();
    o.include_captures = true;
    o.max_bytes = 1;
    for (const auto& e : fs::recursive_directory_iterator(ckpt)) {
        if (e.is_regular_file()) {
            o.max_bytes += fs::file_size(e.path());
        }
    }
    const auto r = collect(o);
    const auto m = clink::config::parse(slurp(r.bundle / "manifest.json"));
    EXPECT_FALSE(m.at("excluded").as_array().empty());
    EXPECT_FALSE(fs::exists(r.bundle / "capture" / "op-5" / "subtask-0" / "epoch-1.cap"));
}

TEST_F(IncidentBundleTest, CredentialLikeGraphParametersAreRedactedFromTheJobManifest) {
    write_checkpoint(1);
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.type = "kafka_source";
    src.id = "src";
    src.params = {{"topic", "orders"},
                  {"sasl.password", "hunter2"},
                  {"api_key", "k-123"},
                  {"conninfo", "host=db password=s3cret"}};
    clink::cluster::OperatorSpec sink;
    sink.type = "print_sink";
    sink.id = "out";
    sink.inputs = {"src"};
    g.ops = {src, sink};
    clink::config::JsonObject jm;
    jm["graph_json"] = JsonValue{g.to_json()};
    jm["requires_commit_confirmation"] = JsonValue{false};
    jm["prior_graph_json"] = JsonValue{g.to_json()};
    spill(root / "ha" / "jobs" / std::to_string(kIncidentJob) / "manifest.json",
          JsonValue{std::move(jm)}.serialize(2));
    auto o = options();
    o.commit_confirmation.reset();  // read from the job manifest
    o.ha_dir = (root / "ha").string();
    const auto r = collect(o);
    const auto text = slurp(r.bundle / "job" / "manifest.json");
    EXPECT_EQ(text.find("hunter2"), std::string::npos);
    EXPECT_EQ(text.find("k-123"), std::string::npos);
    EXPECT_EQ(text.find("s3cret"), std::string::npos);
    EXPECT_NE(text.find("orders"), std::string::npos) << "only credential-like names go";
    EXPECT_NE(text.find("<redacted>"), std::string::npos);
    EXPECT_TRUE(looks_like_credential("sasl.password"));
    EXPECT_TRUE(looks_like_credential("aws_secret_access_key"));
    EXPECT_FALSE(looks_like_credential("topic"));
    EXPECT_FALSE(looks_like_credential("key_field"));
}

// ---- the reader refuses what it may not read ----------------------------------

TEST_F(IncidentBundleTest, UnsafePathsAreRefusedBeforeAnythingIsRead) {
    for (const char* bad : {"../escape", "/etc/hosts", "a/../../b", "./a", "a//b", "a\\b", ""}) {
        EXPECT_FALSE(safe_relative_path(bad)) << bad;
    }
    EXPECT_TRUE(safe_relative_path("checkpoint/_jobs/7/COMPLETED-3"));
    write_checkpoint(1);
    (void)collect(options());
    for (const char* bad : {"../outside", "/etc/hosts", "checkpoint/../../x"}) {
        const auto copy = root / ("copy" + std::to_string(std::hash<std::string>{}(bad) % 1000));
        fs::copy(root / "bundle", copy, fs::copy_options::recursive);
        edit_manifest(copy, [&](JsonValue& m) {
            m.as_object()["artifacts"].as_array().front().as_object()["path"] =
                JsonValue{std::string{bad}};
        });
        EXPECT_THROW((void)read_bundle(copy), BundleRefused) << bad;
    }
}

TEST_F(IncidentBundleTest, AnUnknownFormatOrANewerVersionIsRefused) {
    write_checkpoint(1);
    (void)collect(options());
    const auto b = root / "bundle";
    edit_manifest(b, [](JsonValue& m) { m.as_object()["version"] = JsonValue{kVersion + 1}; });
    EXPECT_THROW((void)read_bundle(b), BundleRefused);
    edit_manifest(b, [](JsonValue& m) {
        m.as_object()["version"] = JsonValue{kVersion};
        m.as_object()["format"] = JsonValue{"something-else"};
    });
    EXPECT_THROW((void)read_bundle(b), BundleRefused);
    spill(b / "manifest.json", "{\"format\": \"clink-incident-bundle\", ");
    EXPECT_THROW((void)read_bundle(b), BundleRefused);
}

TEST_F(IncidentBundleTest, TheReadersBoundsHold) {
    write_checkpoint(1);
    (void)collect(options());
    const auto b = root / "bundle";
    ReadLimits few;
    few.max_files = 1;
    EXPECT_THROW((void)read_bundle(b, few), BundleRefused);
    ReadLimits small;
    small.max_manifest_bytes = 64;
    EXPECT_THROW((void)read_bundle(b, small), BundleRefused);
    ReadLimits tiny_files;
    tiny_files.max_file_bytes = 1;
    EXPECT_THROW((void)read_bundle(b, tiny_files), BundleRefused);
    std::string deep = "{\"format\":\"clink-incident-bundle\",\"x\":";
    for (int i = 0; i < 100; ++i) {
        deep += "[";
    }
    spill(b / "manifest.json", deep);
    EXPECT_THROW((void)read_bundle(b), BundleRefused);
}

// ---- integrity ----------------------------------------------------------------

TEST_F(IncidentBundleTest, AModifiedArtifactFailsIntegrity) {
    write_checkpoint(1);
    const auto r = collect(options());
    fs::path victim;
    for (const auto& e : fs::recursive_directory_iterator(r.bundle / "checkpoint")) {
        if (e.path().extension() == ".snap") {
            victim = e.path();
        }
    }
    ASSERT_FALSE(victim.empty());
    auto bytes = slurp(victim);
    bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 0x5a);
    spill(victim, bytes);
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Failed);
    EXPECT_EQ(exit_code_of(r.bundle, "integrity"), 1);
}

TEST_F(IncidentBundleTest, AMissingArtifactFailsIntegrity) {
    write_checkpoint(1);
    const auto r = collect(options());
    fs::remove(r.bundle / "checkpoint" / "_jobs" / std::to_string(kIncidentJob) / "COMPLETED-1");
    EXPECT_EQ(verdict_of(verify(r.bundle), "bundle.artifacts"), Verdict::Failed);
}

TEST_F(IncidentBundleTest, AnUnlistedFileFailsIntegrity) {
    write_checkpoint(1);
    const auto r = collect(options());
    spill(r.bundle / "checkpoint" / "_jobs" / std::to_string(kIncidentJob) / "COMPLETED-9",
          "job=7\ncheckpoint=9\n");
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Failed);
    EXPECT_NE(reason_of(checks, "bundle.artifacts").find("COMPLETED-9"), std::string::npos);
}

TEST_F(IncidentBundleTest, ASymbolicLinkInTheBundleFailsIntegrity) {
    write_checkpoint(1);
    const auto r = collect(options());
    const auto marker =
        r.bundle / "checkpoint" / "_jobs" / std::to_string(kIncidentJob) / "COMPLETED-1";
    const auto real = root / "outside-marker";
    fs::copy_file(marker, real);
    fs::remove(marker);
    fs::create_symlink(real, marker);
    EXPECT_EQ(verdict_of(verify(r.bundle), "bundle.artifacts"), Verdict::Failed);
}

TEST_F(IncidentBundleTest, AManifestClaimingEvidenceItLacksFailsCompleteness) {
    write_checkpoint(1);
    auto o = options();
    o.include_state = false;
    const auto r = collect(o);
    edit_manifest(r.bundle, [](JsonValue& m) {
        m.as_object()["contents"].as_object()["state"] = JsonValue{true};
        m.as_object()["contents"].as_object()["traces"] = JsonValue{true};
    });
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "bundle.completeness"), Verdict::Failed);
    EXPECT_NE(reason_of(checks, "bundle.completeness").find("traces"), std::string::npos);
}

// ---- consistency --------------------------------------------------------------

TEST_F(IncidentBundleTest, ATornSnapshotOnDiskIsCollectedAndFailsTheParticipantsCheck) {
    write_checkpoint(1);
    // Torn after its sidecar was written: the bytes the sidecar vouches for
    // are gone. Collection copies what is there; verification says so.
    const auto snap = snapshot_path(1, 1);
    const auto bytes = slurp(snap);
    spill(snap, bytes.substr(0, bytes.size() / 2));
    const auto r = collect(options());
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Passed);
    EXPECT_EQ(verdict_of(checks, "checkpoint.participants"), Verdict::Failed)
        << reason_of(checks, "checkpoint.participants");
    EXPECT_EQ(verdict_of(checks, "state.decode"), Verdict::Failed);
}

TEST_F(IncidentBundleTest, AMarkerNamingAnotherJobCheckpointOrGenerationFails) {
    write_checkpoint(3);
    const auto r = collect(options());
    const auto rel = "checkpoint/_jobs/" + std::to_string(kIncidentJob) + "/COMPLETED-3";
    const auto good = slurp(r.bundle / rel);
    for (const auto& [from, to] :
         std::vector<std::pair<std::string, std::string>>{{"job=7", "job=8"},
                                                          {"checkpoint=3", "checkpoint=2"},
                                                          {"generation=1", "generation=2"}}) {
        auto bad = good;
        bad.replace(bad.find(from), from.size(), to);
        spill(r.bundle / rel, bad);
        relist(r.bundle, rel);
        const auto checks = verify(r.bundle);
        EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Passed);
        EXPECT_EQ(verdict_of(checks, "checkpoint.marker"), Verdict::Failed) << to;
    }
}

TEST_F(IncidentBundleTest, AParticipantTheMarkerNamesWithoutStateFails) {
    // The marker records three participants; only two wrote state, which a
    // restore would refuse.
    write_checkpoint(1, {0, 1}, 1, {0, 1, 2});
    const auto r = collect(options());
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "checkpoint.participants"), Verdict::Failed);
    EXPECT_NE(reason_of(checks, "checkpoint.participants").find("participant 2"), std::string::npos)
        << reason_of(checks, "checkpoint.participants");
}

TEST_F(IncidentBundleTest, AMarkerSetThatChoosesAnotherCheckpointFailsTheRestoreRule) {
    write_checkpoint(1);
    write_checkpoint(2);
    const auto r = collect(options());
    // A COMPLETED-3 in the bundle that production never had: the rule,
    // recomputed over the bundle, now names 3 while the selection says 2.
    add_listed(r.bundle,
               "checkpoint/_jobs/" + std::to_string(kIncidentJob) + "/COMPLETED-3",
               "completed_marker",
               clink::cluster::completed_marker_body(kIncidentJob, 3, 1, {0, 1}, "7"));
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Passed);
    EXPECT_EQ(verdict_of(checks, "checkpoint.restore_rule"), Verdict::Failed)
        << reason_of(checks, "checkpoint.restore_rule");
}

TEST_F(IncidentBundleTest, AnUnknownRestoreRuleIsUnavailableNotGuessed) {
    write_checkpoint(1);
    auto o = options();
    o.commit_confirmation.reset();
    o.checkpoint = 1;
    const auto r = collect(o);
    const auto checks = verify(r.bundle);
    EXPECT_EQ(verdict_of(checks, "checkpoint.restore_rule"), Verdict::Unavailable);
    EXPECT_EQ(exit_code_of(r.bundle, "consistency"), 3);
}

// ---- protocol traces ----------------------------------------------------------

TEST_F(IncidentBundleTest, ATraceWithAGapFailsItsStructureAndConformanceNeedsTheTools) {
    write_checkpoint(1);
    const auto traces = root / "traces";
    spill(traces / "coordinator-1.ndjson",
          R"({"seq":1,"ts":1,"proc":"coordinator-1","event":"a"})"
          "\n"
          R"({"seq":2,"ts":2,"proc":"coordinator-1","event":"b"})"
          "\n");
    spill(traces / "worker-1.ndjson",
          R"({"seq":1,"ts":1,"proc":"worker-1","event":"a"})"
          "\n"
          R"({"seq":3,"ts":3,"proc":"worker-1","event":"c"})"
          "\n");
    auto o = options();
    o.trace_dir = traces.string();
    const auto r = collect(o);
    VerifyOptions v;
    v.profile = "full";
    v.formal_dir = (root / "no-formal").string();
    v.formal_tools_dir = (root / "no-tools").string();
    const auto checks = verify_core(read_bundle(r.bundle), v);
    EXPECT_EQ(verdict_of(checks, "protocol.trace_structure"), Verdict::Failed);
    EXPECT_NE(reason_of(checks, "protocol.trace_structure").find("worker-1"), std::string::npos);
    EXPECT_EQ(verdict_of(checks, "protocol.conformance"), Verdict::Unavailable);
}

TEST_F(IncidentBundleTest, WithoutTheValidatorsToolsConformanceIsUnavailableAndNothingIsFetched) {
    write_checkpoint(1);
    const auto traces = root / "traces";
    spill(traces / "coordinator-1.ndjson",
          R"({"seq":1,"ts":1,"proc":"coordinator-1","event":"a"})"
          "\n");
    auto o = options();
    o.trace_dir = traces.string();
    const auto r = collect(o);
    VerifyOptions v;
    v.profile = "full";
    v.formal_dir = (root / "formal").string();  // exists, but holds no validator
    fs::create_directories(v.formal_dir);
    v.formal_tools_dir = (root / "tools").string();
    fs::create_directories(v.formal_tools_dir);
    const auto checks = verify_core(read_bundle(r.bundle), v);
    EXPECT_EQ(verdict_of(checks, "protocol.trace_structure"), Verdict::Passed);
    EXPECT_EQ(verdict_of(checks, "protocol.conformance"), Verdict::Unavailable);
    EXPECT_TRUE(fs::is_empty(v.formal_tools_dir)) << "the validator's jars are never downloaded";
}

// ---- replay's starting state -----------------------------------------------------

// ---- the review's findings ------------------------------------------------------

TEST_F(IncidentBundleTest, OnlyTheEvidenceDirectoriesAreScannedAndAListingErrorIsAProblem) {
    write_checkpoint(1);
    const auto r = collect(options());
    // A report written beside the manifest, and the metadata macOS leaves in a
    // directory it shows, are not evidence and fail nothing.
    spill(r.bundle / "report.json", "{}");
    spill(r.bundle / ".DS_Store", "x");
    spill(r.bundle / "checkpoint" / ".DS_Store", "x");
    spill(r.bundle / "checkpoint" / "._COMPLETED-1", "x");
    EXPECT_EQ(verdict_of(verify(r.bundle), "bundle.artifacts"), Verdict::Passed)
        << reason_of(verify(r.bundle), "bundle.artifacts");
    // A directory that cannot be listed is not a clean one.
    const auto hidden = r.bundle / "capture" / "sealed";
    fs::create_directories(hidden);
    spill(hidden / "epoch-1.cap", "unlisted");
    fs::permissions(hidden, fs::perms::none);
    const auto checks = verify(r.bundle);
    fs::permissions(hidden, fs::perms::owner_all);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Failed);
    EXPECT_NE(reason_of(checks, "bundle.artifacts").find("cannot be listed"), std::string::npos)
        << reason_of(checks, "bundle.artifacts");
}

TEST_F(IncidentBundleTest, AListedFileThatCannotBeReadFailsIntegrityInsteadOfEndingTheRun) {
    write_checkpoint(1);
    const auto r = collect(options());
    const auto marker =
        r.bundle / "checkpoint" / "_jobs" / std::to_string(kIncidentJob) / "COMPLETED-1";
    fs::permissions(marker, fs::perms::none);
    std::vector<Check> checks;
    EXPECT_NO_THROW(checks = verify(r.bundle));
    fs::permissions(marker, fs::perms::owner_read | fs::perms::owner_write);
    EXPECT_EQ(verdict_of(checks, "bundle.artifacts"), Verdict::Failed);
}

TEST_F(IncidentBundleTest, MalformedManifestFieldsAreRefusedRatherThanFatal) {
    write_checkpoint(1);
    (void)collect(options());
    const auto b = root / "bundle";
    const auto refused = [&](const std::function<void(JsonValue&)>& f, const char* what) {
        const auto copy = root / ("copy-" + std::string{what});
        fs::copy(b, copy, fs::copy_options::recursive);
        edit_manifest(copy, f);
        EXPECT_THROW((void)read_bundle(copy), BundleRefused) << what;
    };
    refused(
        [](JsonValue& m) { m.as_object()["collector"].as_object()["collected_at"] = JsonValue{7}; },
        "numeric-time");
    refused([](JsonValue& m) { m.as_object()["selection"].as_object().erase("engine_choice"); },
            "no-engine-choice");
    refused(
        [](JsonValue& m) {
            m.as_object()["job"].as_object()["commit_confirmation"] = JsonValue{"yes"};
        },
        "text-confirmation");
    refused(
        [](JsonValue& m) {
            m.as_object()["selection"].as_object()["participants"] = JsonValue{"0,1"};
        },
        "text-participants");
    // An artifact outside the evidence directories.
    refused(
        [](JsonValue& m) {
            m.as_object()["artifacts"].as_array().front().as_object()["path"] =
                JsonValue{"notes.txt"};
        },
        "outside-evidence");
}

TEST_F(IncidentBundleTest, ACapturedEpochMustBeTheFileTheReplayReads) {
    write_checkpoint(1);
    const auto cap = root / "capture";
    for (const char* op : {"op-11", "op-22"}) {
        spill(cap / op / "subtask-0" / "op.json", R"({"op_type":"identity_row"})");
        spill(cap / op / "subtask-0" / "epoch-1.cap", std::string(32, 'c'));
    }
    auto o = options();
    o.capture_dir = cap.string();
    o.include_captures = true;
    const auto r = collect(o);
    // Operator ids are text in the manifest: they use all 64 bits.
    const auto manifest = clink::config::parse(slurp(r.bundle / "manifest.json"));
    ASSERT_FALSE(manifest.at("captures").as_array().empty());
    EXPECT_TRUE(manifest.at("captures").as_array().front().at("op").is_string());
    EXPECT_EQ(read_bundle(r.bundle).captures.size(), 2u);
    // An entry naming one operator's file as another's is refused: the replay
    // would read one file while the manifest vouched for the other.
    edit_manifest(r.bundle, [](JsonValue& m) {
        m.as_object()["captures"].as_array().front().as_object()["op"] = JsonValue{"22"};
    });
    EXPECT_THROW((void)read_bundle(r.bundle), BundleRefused);
}

TEST_F(IncidentBundleTest, AStateRootThatIsAUriIsOutsideTheBundleWhereverItIsReadFrom) {
    write_marker(
        "COMPLETED-1",
        clink::cluster::completed_marker_body(kIncidentJob, 1, 1, {0}, "rocksdb://_state/7"));
    auto o = options();
    o.checkpoint = 1;
    const auto r = collect(o);
    const auto outside = verdict_of(verify(r.bundle), "checkpoint.participants");
    const auto here = fs::current_path();
    fs::current_path(r.bundle);
    const auto within = verdict_of(verify(r.bundle), "checkpoint.participants");
    fs::current_path(here);
    EXPECT_EQ(outside, Verdict::Unsupported);
    EXPECT_EQ(within, outside) << "the verdict must not depend on the working directory";
}

TEST_F(IncidentBundleTest, AMarkerWritersTemporaryFileIsNotCollectedAsAMarker) {
    write_checkpoint(1);
    write_marker("COMPLETED-9.tmp.1.0", "job=7\ncheckpoint=9\n");
    const auto r = collect(options());
    EXPECT_FALSE(fs::exists(r.bundle / "checkpoint" / "_jobs" / std::to_string(kIncidentJob) /
                            "COMPLETED-9.tmp.1.0"));
    EXPECT_EQ(verdict_of(verify(r.bundle), "checkpoint.restore_rule"), Verdict::Passed);
}

TEST_F(IncidentBundleTest, TheDefaultCaptureWindowHoldsTheRestoredRunsFirstEpoch) {
    write_checkpoint(1);
    // A run restored from 1 numbers from 5: its first epoch is 5, not 2.
    ASSERT_TRUE(clink::cluster::record_restore_point(
        ckpt.string(), kIncidentJob, 1, {.restore = 1, .next = 5}));
    const auto cap = root / "capture" / "op-3" / "subtask-0";
    spill(cap / "op.json", R"({"op_type":"identity_row"})");
    for (int e : {1, 2, 5, 6}) {
        spill(cap / ("epoch-" + std::to_string(e) + ".cap"), std::string(32, 'c'));
    }
    auto o = options();
    o.capture_dir = (root / "capture").string();
    o.include_captures = true;
    o.out = (root / "bundle/").string();  // a trailing separator publishes the same bundle
    const auto r = collect(o);
    EXPECT_EQ(r.bundle.filename(), "bundle");
    EXPECT_TRUE(leftovers().empty());
    std::set<std::uint64_t> epochs;
    for (const auto& c : read_bundle(r.bundle).captures) {
        epochs.insert(c.epoch);
    }
    EXPECT_EQ(epochs, (std::set<std::uint64_t>{1, 2, 5}));
}

TEST_F(IncidentBundleTest, AnEpochsStartIsKnownOnlyWhereTheRestoresAllowIt) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_checkpoint(3);
    write_marker("CONFIRMED-1", "");
    write_marker("CONFIRMED-3", "");
    ASSERT_TRUE(clink::cluster::record_restore_point(
        ckpt.string(), kIncidentJob, 1, {.restore = 3, .next = 6}));
    const auto traces = root / "traces";
    // An earlier restore the RESTORED record no longer names, in the traces.
    spill(
        traces / "coordinator-1.ndjson",
        R"({"seq":1,"ts":1,"proc":"coordinator-1","event":"Redeploy","job":7,"restore":1,"next":3})"
        "\n"
        R"({"seq":2,"ts":2,"proc":"coordinator-1","event":"Redeploy","job":8,"restore":0,"next":4})"
        "\n");
    auto o = options();
    o.commit_confirmation = true;
    o.trace_dir = traces.string();
    const auto r = collect(o);
    const auto b = read_bundle(r.bundle);
    const auto restores = recorded_restores(b);
    ASSERT_EQ(restores.size(), 2u) << "the record and this job's traced redeploy, not job 8's";
    EXPECT_TRUE(epoch_start(b, restores, 6).known);
    EXPECT_EQ(epoch_start(b, restores, 6).checkpoint, 3u) << "the newest run's first epoch";
    EXPECT_EQ(epoch_start(b, restores, 3).checkpoint, 1u) << "the traced run's first epoch";
    EXPECT_EQ(epoch_start(b, restores, 7).checkpoint, 6u);
    EXPECT_EQ(epoch_start(b, restores, 2).checkpoint, 1u) << "checkpoint 1 was confirmed";
    EXPECT_EQ(epoch_start(b, restores, 4).checkpoint, 3u)
        << "checkpoint 3 was confirmed, so whichever run processed epoch 4 started there";
    EXPECT_FALSE(epoch_start(b, restores, 5).known)
        << "before the newest restore, after an unconfirmed checkpoint: a run the bundle does "
           "not record may have started there";
    EXPECT_TRUE(epoch_discarded(restores, 4));
    EXPECT_TRUE(epoch_discarded(restores, 5));
    EXPECT_FALSE(epoch_discarded(restores, 6));
}

TEST(IncidentEpochs, ARestoredRunsFirstEpochStartsWhereThatRunRestored) {
    // A run restored from checkpoint 3 numbers from 6: its first epoch, 6,
    // holds the events after checkpoint 3, so it starts there. Every other
    // epoch starts from the one before it, epoch 1 from fresh state.
    EXPECT_EQ(epoch_starting_checkpoint(6, 3, 6), 3u);
    EXPECT_EQ(epoch_starting_checkpoint(7, 3, 6), 6u);
    EXPECT_EQ(epoch_starting_checkpoint(5, 3, 6), 4u)
        << "an epoch of the run the restore discarded";
    EXPECT_EQ(epoch_starting_checkpoint(2, 0, 2), 0u) << "a run restored from nothing starts fresh";
    EXPECT_EQ(epoch_starting_checkpoint(1, std::nullopt, std::nullopt), 0u);
    EXPECT_EQ(epoch_starting_checkpoint(4, std::nullopt, std::nullopt), 3u);
}

// ---- reports ------------------------------------------------------------------

Check check_with(const std::string& id, Verdict v) {
    Check c;
    c.id = id;
    c.verdict = v;
    return c;
}

TEST_F(IncidentBundleTest, TheReportSaysWhatEachExitCodeMeans) {
    Report r;
    r.bundle = "b";
    r.profile = "integrity";
    r.required = required_checks("integrity", false);
    for (const auto& id : r.required) {
        r.checks.push_back(check_with(id, Verdict::Passed));
    }
    EXPECT_EQ(r.exit_code(), 0);
    r.checks.push_back(check_with("state.decode", Verdict::Unavailable));
    EXPECT_EQ(r.exit_code(), 0) << "a check the profile does not require cannot make it partial";
    r.checks[1].verdict = Verdict::Unsupported;
    EXPECT_EQ(r.exit_code(), 3);
    r.checks.push_back(check_with("replay.determinism", Verdict::Failed));
    EXPECT_EQ(r.exit_code(), 1) << "any failure fails the run";
    const auto json = clink::config::parse(r.render_json());
    EXPECT_EQ(json.at("exit_code").as_int(), 1);
    EXPECT_EQ(json.at("checks").as_array().size(), r.checks.size());
    EXPECT_THROW((void)required_checks("everything", false), std::invalid_argument);
}

SelectionTally tally(std::uint64_t selected,
                     std::uint64_t passed,
                     std::uint64_t failed,
                     std::uint64_t unavailable,
                     std::uint64_t unsupported) {
    SelectionTally t;
    t.selected = selected;
    for (const auto& [n, v] : {std::pair{passed, Verdict::Passed},
                               {failed, Verdict::Failed},
                               {unavailable, Verdict::Unavailable},
                               {unsupported, Verdict::Unsupported}}) {
        for (std::uint64_t i = 0; i < n; ++i) {
            t.add(v);
        }
    }
    return t;
}

TEST(IncidentSelection, PassesOnlyWhenEverySelectedItemWasCheckedAndPassed) {
    EXPECT_EQ(tally(3, 3, 0, 0, 0).verdict(), Verdict::Passed);
    // A bound that stopped after one of three: the rest are omitted, not
    // dropped from the selection.
    const auto bounded = tally(3, 1, 0, 0, 0);
    EXPECT_EQ(bounded.verdict(), Verdict::Unavailable);
    EXPECT_EQ(bounded.checked(), 1u);
    EXPECT_EQ(bounded.omitted(), 2u);
    EXPECT_EQ(tally(0, 0, 0, 0, 0).verdict(), Verdict::Unavailable) << "nothing selected";
    EXPECT_EQ(tally(2, 1, 0, 1, 0).verdict(), Verdict::Unavailable);
    // Supported and unsupported items together are not a pass.
    EXPECT_EQ(tally(2, 1, 0, 0, 1).verdict(), Verdict::Unsupported);
    EXPECT_EQ(tally(2, 0, 0, 0, 2).verdict(), Verdict::Unsupported);
    // An unsupported item does not stand in for an omitted or unavailable one.
    EXPECT_EQ(tally(3, 1, 0, 0, 1).verdict(), Verdict::Unavailable);
    EXPECT_EQ(tally(3, 1, 0, 1, 1).verdict(), Verdict::Unavailable);
}

TEST(IncidentSelection, AFailureFailsTheSelectionWhateverTheRestShow) {
    EXPECT_EQ(tally(1, 0, 1, 0, 0).verdict(), Verdict::Failed);
    // Passed, unavailable, unsupported and omitted items beside one failure.
    const auto mixed = tally(5, 1, 1, 1, 1);
    EXPECT_EQ(mixed.omitted(), 1u);
    EXPECT_EQ(mixed.verdict(), Verdict::Failed);
}

TEST(IncidentSelection, TheCountsAreReportedInTextAndJson) {
    Report r;
    r.bundle = "b";
    r.profile = "replay";
    r.required = required_checks("replay", false);
    for (const auto& id : r.required) {
        r.checks.push_back(check_with(id, Verdict::Passed));
    }
    auto& det = r.checks[r.checks.size() - 2];
    ASSERT_EQ(det.id, "replay.determinism");
    const auto t = tally(6, 1, 0, 0, 0);
    det.verdict = t.verdict();
    det.counts = t.counts();
    EXPECT_EQ(r.exit_code(), 3) << "a bounded replay is a partial result";
    EXPECT_NE(r.render_text().find("counts: selected 6, checked 1, passed 1, failed 0, "
                                   "unavailable 0, unsupported 0, omitted 5"),
              std::string::npos)
        << r.render_text();
    const auto json = clink::config::parse(r.render_json());
    EXPECT_EQ(json.at("exit_code").as_int(), 3);
    for (const auto& c : json.at("checks").as_array()) {
        if (c.string_or("id", "") == "replay.determinism") {
            const auto& counts = c.at("counts");
            EXPECT_EQ(counts.at("selected").as_int(), 6);
            EXPECT_EQ(counts.at("checked").as_int(), 1);
            EXPECT_EQ(counts.at("passed").as_int(), 1);
            EXPECT_EQ(counts.at("omitted").as_int(), 5);
        } else {
            EXPECT_FALSE(c.contains("counts")) << "only a check over a selection";
        }
    }
}

}  // namespace
