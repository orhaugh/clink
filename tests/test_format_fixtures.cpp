// Frozen-bytes compatibility fixtures.
//
// Every file under tests/fixtures/ was written by ONE build and must stay
// readable by every later one. The unit suites already round-trip these
// encodings, but a round trip cannot catch an encode/decode pair that
// co-evolves: change both sides together and encode(decode(x)) still holds
// while every artefact the previous build persisted has become unreadable.
// The fixture is the previous build's artefact, held still.
//
// Domains pinned here and their contracts:
// docs/internals/protocol-compatibility.md.
//
// Regeneration: run with CLINK_REGEN_FORMAT_FIXTURES=1 to (re)write the
// files with the current writers. That is legitimate ONLY for adding a new
// fixture or extending one alongside a deliberate, documented version bump.
// Regenerating to make a failing read pass defeats the fixture's entire
// purpose - a failing read here IS a compatibility break with shipped
// artefacts, and the fix belongs in the reader.

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state_processor/savepoint.hpp"
#include "clink/state_processor/state_diff.hpp"

namespace {

std::filesystem::path fixture_path(const std::string& name) {
    return std::filesystem::path{CLINK_FORMAT_FIXTURE_DIR} / name;
}

bool regen() {
    const char* env = std::getenv("CLINK_REGEN_FORMAT_FIXTURES");
    return env != nullptr && *env == '1';
}

void write_file(const std::filesystem::path& p, const void* data, std::size_t n) {
    std::filesystem::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.good()) << p;
    out.write(static_cast<const char*>(data), static_cast<std::streamsize>(n));
    ASSERT_TRUE(out.good()) << p;
}

std::vector<std::byte> read_bytes(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    EXPECT_TRUE(in.good()) << "fixture missing: " << p
                           << " (a moved or deleted fixture would leave this suite green over "
                              "nothing; regenerate ONLY to add fixtures, never to make a "
                              "failing read pass)";
    std::vector<std::byte> out;
    char c;
    while (in.get(c)) {
        out.push_back(static_cast<std::byte>(c));
    }
    return out;
}

std::string read_text(const std::filesystem::path& p) {
    const auto b = read_bytes(p);
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// --- checkpoint metadata sidecar (kCheckpointMetaVersion = 1) ---------------

TEST(FormatFixtures, CheckpointMetaV1SidecarStaysReadable) {
    const auto path = fixture_path("checkpoint-meta-v1.txt");
    if (regen()) {
        const clink::state::CheckpointMeta meta{
            .version = 1, .checkpoint_id = 42, .payload_bytes = 1234, .payload_crc32c = 0xDEADBEEF};
        // The trailing unknown key freezes the additive rule itself: a v1
        // reader must skip keys it does not know, or additions stop being
        // compatible changes.
        const auto text = meta.serialise() + "a_key_from_the_future=7\n";
        write_file(path, text.data(), text.size());
    }
    const auto text = read_text(path);
    ASSERT_FALSE(text.empty());
    clink::state::CheckpointMeta meta;
    ASSERT_TRUE(clink::state::CheckpointMeta::parse(text, meta))
        << "a v1 sidecar written by an earlier build no longer parses";
    EXPECT_EQ(meta.version, 1u);
    EXPECT_EQ(meta.checkpoint_id, 42u);
    EXPECT_EQ(meta.payload_bytes, 1234u);
    EXPECT_EQ(meta.payload_crc32c, 0xDEADBEEFu);
}

// --- cluster handshake (RegisterMsg, protocol v1) ----------------------------

TEST(FormatFixtures, RegisterMsgV1FrameStaysDecodable) {
    const auto path = fixture_path("register-msg-v1.bin");
    if (regen()) {
        clink::cluster::RegisterMsg msg;
        msg.worker_id = "fixture-worker";
        msg.data_host = "127.0.0.1";
        msg.slot_count = 4;
        msg.http_port = 8081;
        clink::cluster::MessageBuilder b;
        clink::cluster::encode_body(b, msg);
        const auto frame = b.finalize();
        write_file(path, frame.data(), frame.size());
    }
    auto frame = read_bytes(path);
    ASSERT_GT(frame.size(), 4u);
    std::uint32_t len = 0;
    for (int i = 0; i < 4; ++i) {
        len = (len << 8) | static_cast<std::uint8_t>(frame[static_cast<std::size_t>(i)]);
    }
    ASSERT_EQ(len, frame.size() - 4) << "length prefix no longer matches the payload";
    clink::cluster::MessageReader r(std::vector<std::byte>(frame.begin() + 4, frame.end()));
    const auto msg = clink::cluster::decode_register(r);
    EXPECT_EQ(msg.worker_id, "fixture-worker");
    EXPECT_EQ(msg.data_host, "127.0.0.1");
    EXPECT_EQ(msg.slot_count, 4u);
    EXPECT_EQ(msg.http_port, 8081);
    EXPECT_EQ(msg.protocol_version, 1u);
    EXPECT_EQ(msg.min_compatible_protocol_version, 1u);
}

// --- job plan JSON (additive-only, absent-tolerant) ---------------------------

TEST(FormatFixtures, JobSpecJsonWithoutOptionalKeysStaysParseable) {
    const auto path = fixture_path("job-spec-v1.json");
    if (regen()) {
        clink::cluster::JobGraphSpec spec;
        clink::cluster::OperatorSpec src;
        src.id = "src";
        src.type = "file_line_source";
        src.out_channel = clink::cluster::ChannelType{std::string{clink::cluster::kChannelString}};
        spec.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.id = "snk";
        snk.type = "file_line_sink";
        snk.inputs = {"src"};
        snk.out_channel = clink::cluster::ChannelType{std::string{clink::cluster::kChannelString}};
        spec.ops.push_back(snk);
        const auto json = spec.to_json();
        write_file(path, json.data(), json.size());
    }
    const auto spec = clink::cluster::JobGraphSpec::from_json(read_text(path));
    ASSERT_EQ(spec.ops.size(), 2u);
    EXPECT_EQ(spec.ops[0].type, "file_line_source");
    EXPECT_EQ(spec.ops[1].inputs, std::vector<std::string>{"src"});
    // The optional tail keys are ABSENT in this fixture; a reader that
    // starts requiring any of them breaks every persisted spec in every
    // HA directory.
    EXPECT_TRUE(spec.name.empty());
    EXPECT_TRUE(spec.determinism_coverage.empty());
    EXPECT_TRUE(spec.column_lineage.empty());
}

TEST(FormatFixtures, JobSpecJsonWithOptionalKeysStaysParseable) {
    const auto path = fixture_path("job-spec-v1-full.json");
    if (regen()) {
        clink::cluster::JobGraphSpec spec;
        spec.name = "fixture-job";
        spec.determinism_coverage = "sql-planner";
        clink::cluster::OperatorSpec src;
        src.id = "src";
        src.type = "file_line_source";
        src.uid = "fixture-src";
        src.out_channel = clink::cluster::ChannelType{std::string{clink::cluster::kChannelString}};
        spec.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.id = "snk";
        snk.type = "file_line_sink";
        snk.inputs = {"src"};
        snk.params["path"] = "/tmp/out.txt";
        snk.out_channel = clink::cluster::ChannelType{std::string{clink::cluster::kChannelString}};
        spec.ops.push_back(snk);
        const auto json = spec.to_json();
        write_file(path, json.data(), json.size());
    }
    const auto spec = clink::cluster::JobGraphSpec::from_json(read_text(path));
    EXPECT_EQ(spec.name, "fixture-job");
    EXPECT_EQ(spec.determinism_coverage, "sql-planner");
    ASSERT_EQ(spec.ops.size(), 2u);
    EXPECT_EQ(spec.ops[0].uid, "fixture-src");
    EXPECT_EQ(spec.ops[1].params.at("path"), "/tmp/out.txt");
}

// --- state snapshot / savepoint stream (clink.format_version = 1) ------------

TEST(FormatFixtures, SnapshotV1StreamStaysLoadable) {
    const auto path = fixture_path("snapshot-v1.snap");
    const clink::OperatorId op{42};
    const auto kc = clink::int64_codec();
    const auto vc = clink::int64_codec();
    if (regen()) {
        auto sp = clink::state_processor::Savepoint::create();
        auto counts = sp.keyed_state<std::int64_t, std::int64_t>(op, "counts", kc, vc);
        counts.put(1, 100);
        counts.put(2, 200);
        counts.put(7, 700);
        std::filesystem::create_directories(path.parent_path());
        sp.write_to_file(path);
    }
    ASSERT_TRUE(std::filesystem::exists(path))
        << "fixture missing: " << path << " (see the regeneration rule at the top of this file)";
    auto sp = clink::state_processor::Savepoint::load_from_file(path);
    auto counts = sp.keyed_state<std::int64_t, std::int64_t>(op, "counts", kc, vc);
    ASSERT_TRUE(counts.get(1).has_value())
        << "a v1 snapshot written by an earlier build no longer restores";
    EXPECT_EQ(*counts.get(1), 100);
    EXPECT_EQ(*counts.get(2), 200);
    EXPECT_EQ(*counts.get(7), 700);
    const auto entries = clink::state_processor::collect_entries(sp);
    ASSERT_EQ(entries.count(op), 1u);
    EXPECT_EQ(entries.at(op).at("counts").size(), 3u);
}

// --- incident capture header (kCaptureVersion = 2) ----------------------------

TEST(FormatFixtures, CaptureHeaderV2StaysDecodable) {
    const auto path = fixture_path("capture-v2.cap");
    if (regen()) {
        clink::capture::CaptureFileHeader h;
        h.records_seen = 12345;
        h.truncated = true;  // exercise the flag byte, not just zero
        const auto blob =
            clink::capture::encode_capture_file<std::int64_t>(h, {}, clink::int64_codec());
        write_file(path, blob.data(), blob.size());
    }
    const auto blob = read_bytes(path);
    const auto parsed = clink::capture::decode_capture_header(blob);
    ASSERT_TRUE(parsed.has_value())
        << "a v2 capture header written by an earlier build no longer decodes";
    EXPECT_EQ(parsed->first.version, clink::capture::kCaptureVersion);
    EXPECT_EQ(parsed->first.records_seen, 12345u);
    EXPECT_TRUE(parsed->first.truncated);
}

}  // namespace
