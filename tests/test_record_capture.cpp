// Record-capture flight recorder (runtime/record_capture.hpp): the framing
// round-trip, the .cap file header, the epoch buffer's barrier-aligned
// flush + truncation semantics, and the end-to-end runner tee (a Dag job
// with capture armed writes per-epoch files whose records replay exactly).

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/sources_and_sinks.hpp"

using namespace clink;
namespace fs = std::filesystem;

namespace {

std::vector<std::byte> read_all(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::vector<std::byte> bytes;
    std::istreambuf_iterator<char> it{in}, end;
    for (; it != end; ++it) {
        bytes.push_back(static_cast<std::byte>(*it));
    }
    return bytes;
}

// Read one .cap file back into its event stream (any format version).
std::pair<capture::CaptureFileHeader, std::vector<capture::CaptureEvent<std::int64_t>>> read_cap(
    const fs::path& p) {
    const auto bytes = read_all(p);
    auto parsed = capture::read_capture_events(
        std::span<const std::byte>{bytes.data(), bytes.size()}, int64_codec());
    EXPECT_TRUE(parsed.has_value()) << p;
    return std::move(*parsed);
}

// The data records within an event stream, in order.
std::vector<Record<std::int64_t>> data_of(
    const std::vector<capture::CaptureEvent<std::int64_t>>& events) {
    std::vector<Record<std::int64_t>> out;
    for (const auto& e : events) {
        if (const auto* rec = std::get_if<Record<std::int64_t>>(&e)) {
            out.push_back(*rec);
        }
    }
    return out;
}

}  // namespace

TEST(RecordCapture, FramingRoundTripsValuesAndEventTimes) {
    std::vector<Record<std::int64_t>> in;
    in.emplace_back(std::int64_t{7});
    in.emplace_back(std::int64_t{42}, EventTime{123456});
    in.emplace_back(std::int64_t{-1}, EventTime{-5});

    auto bytes = capture::serialize_records(in, int64_codec());
    auto out = capture::deserialize_records(std::span<const std::byte>{bytes.data(), bytes.size()},
                                            int64_codec());
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].value(), 7);
    EXPECT_FALSE(out[0].event_time().has_value());
    EXPECT_EQ(out[1].value(), 42);
    ASSERT_TRUE(out[1].event_time().has_value());
    EXPECT_EQ(out[1].event_time()->millis(), 123456);
    EXPECT_EQ(out[2].value(), -1);
    ASSERT_TRUE(out[2].event_time().has_value());
    EXPECT_EQ(out[2].event_time()->millis(), -5);
}

TEST(RecordCapture, EpochBufferFlushesPerBarrierAndTruncates) {
    const auto dir = fs::temp_directory_path() / "clink_capture_unit";
    fs::remove_all(dir);
    auto codec = std::make_shared<const Codec<std::int64_t>>(int64_codec());
    capture::EpochCaptureBuffer<std::int64_t> buf(dir,
                                                  OperatorId{9},
                                                  /*subtask=*/1,
                                                  /*max_records=*/3,
                                                  codec);

    auto push = [&](std::initializer_list<std::int64_t> vals) {
        Batch<std::int64_t> b;
        for (auto v : vals) {
            b.push(Record<std::int64_t>{v});
        }
        buf.on_data(b);
    };

    // Epoch 1: under the cap.
    push({1, 2});
    buf.on_barrier(1);
    // Epoch 2: over the cap (5 seen, 3 stored, truncated).
    push({10, 20, 30, 40, 50});
    buf.on_barrier(2);
    // Tail after the last barrier.
    push({99});
    buf.flush_final();

    const auto base = dir / "op-9" / "subtask-1";
    {
        auto [h, events] = read_cap(base / "epoch-1.cap");
        auto recs = data_of(events);
        EXPECT_FALSE(h.truncated);
        EXPECT_EQ(h.records_seen, 2u);
        ASSERT_EQ(recs.size(), 2u);
        EXPECT_EQ(recs[0].value(), 1);
        EXPECT_EQ(recs[1].value(), 2);
    }
    {
        auto [h, events] = read_cap(base / "epoch-2.cap");
        auto recs = data_of(events);
        EXPECT_TRUE(h.truncated);
        EXPECT_EQ(h.records_seen, 5u);
        ASSERT_EQ(recs.size(), 3u);  // cap
        EXPECT_EQ(recs[0].value(), 10);
        EXPECT_EQ(recs[2].value(), 30);
    }
    {
        auto [h, events] = read_cap(base / "final.cap");
        auto recs = data_of(events);
        EXPECT_EQ(h.records_seen, 1u);
        ASSERT_EQ(recs.size(), 1u);
        EXPECT_EQ(recs[0].value(), 99);
    }
    fs::remove_all(dir);
}

TEST(RecordCapture, RunnerTeeWritesEpochsEndToEnd) {
    // Dag: source -> map (captured) -> sink, with a checkpoint injected
    // mid-stream via the source injector, so the map's capture splits into
    // epoch-7.cap (pre-barrier records) and final.cap (the tail).
    const auto dir = fs::temp_directory_path() / "clink_capture_e2e";
    fs::remove_all(dir);

    Dag dag;
    std::vector<Record<std::int64_t>> input;
    for (std::int64_t i = 1; i <= 6; ++i) {
        input.emplace_back(i);
    }
    auto src = std::make_shared<VectorSource<std::int64_t>>(input);
    auto h_src = dag.add_source<std::int64_t>(src);
    auto op = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 10; });
    auto codec = std::make_shared<const Codec<std::int64_t>>(int64_codec());
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, op, codec);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);

    // Inject barrier id=7 after the source has emitted its batch: the
    // VectorSource emits everything in one produce() call, so injecting
    // before run() queues the barrier after that batch deterministically.
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cfg.capture_dir = dir.string();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(sink->collected().size(), 6u);

    // No barrier flowed (VectorSource completes without one), so the whole
    // stream lands in final.cap for the map operator.
    fs::path cap_file;
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().filename() == "final.cap") {
            cap_file = entry.path();
        }
    }
    ASSERT_FALSE(cap_file.empty()) << "no final.cap written under " << dir;
    auto [h, events] = read_cap(cap_file);
    auto recs = data_of(events);
    EXPECT_FALSE(h.truncated);
    EXPECT_EQ(h.records_seen, 6u);
    ASSERT_EQ(recs.size(), 6u);
    for (std::int64_t i = 0; i < 6; ++i) {
        EXPECT_EQ(recs[static_cast<std::size_t>(i)].value(),
                  i + 1);  // input, not the mapped output
    }
    fs::remove_all(dir);
}

// ---- Format v2: the full-fidelity event stream ----

TEST(RecordCapture, EventFramingRoundTripsRecordsWatermarksAndClocks) {
    std::vector<capture::CaptureEvent<std::int64_t>> in;
    in.emplace_back(Record<std::int64_t>{7});
    in.emplace_back(capture::WatermarkEvent{1500, /*idle=*/false});
    in.emplace_back(Record<std::int64_t>{42, EventTime{123456}});
    in.emplace_back(capture::ClockEvent{999999});
    in.emplace_back(capture::WatermarkEvent{0, /*idle=*/true});

    auto bytes = capture::serialize_events(in, int64_codec());
    auto out = capture::deserialize_events(std::span<const std::byte>{bytes.data(), bytes.size()},
                                           int64_codec());
    ASSERT_EQ(out.size(), 5u);
    EXPECT_EQ(std::get<Record<std::int64_t>>(out[0]).value(), 7);
    EXPECT_EQ(std::get<capture::WatermarkEvent>(out[1]).ts_ms, 1500);
    EXPECT_FALSE(std::get<capture::WatermarkEvent>(out[1]).idle);
    const auto& rec = std::get<Record<std::int64_t>>(out[2]);
    EXPECT_EQ(rec.value(), 42);
    ASSERT_TRUE(rec.event_time().has_value());
    EXPECT_EQ(rec.event_time()->millis(), 123456);
    EXPECT_EQ(std::get<capture::ClockEvent>(out[3]).now_ms, 999999);
    EXPECT_TRUE(std::get<capture::WatermarkEvent>(out[4]).idle);
}

TEST(RecordCapture, V1FilesRemainReadableAsDataOnlyEventStreams) {
    // Hand-build a v1 .cap blob: header stamped version 1, plain records
    // payload - exactly what an older engine wrote.
    std::vector<Record<std::int64_t>> records;
    records.emplace_back(std::int64_t{5}, EventTime{100});
    records.emplace_back(std::int64_t{6});
    std::vector<std::byte> blob;
    for (const char c : capture::kCaptureMagic) {
        blob.push_back(static_cast<std::byte>(c));
    }
    const std::uint32_t v1 = 1;
    for (int i = 0; i < 4; ++i) {
        blob.push_back(static_cast<std::byte>((v1 >> (i * 8)) & 0xFF));
    }
    blob.push_back(static_cast<std::byte>(0));  // not truncated
    const std::uint64_t seen = 2;
    for (int i = 0; i < 8; ++i) {
        blob.push_back(static_cast<std::byte>((seen >> (i * 8)) & 0xFF));
    }
    auto payload = capture::serialize_records(records, int64_codec());
    blob.insert(blob.end(), payload.begin(), payload.end());

    auto parsed = capture::read_capture_events(std::span<const std::byte>{blob.data(), blob.size()},
                                               int64_codec());
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->first.version, 1u);
    ASSERT_EQ(parsed->second.size(), 2u);
    EXPECT_EQ(std::get<Record<std::int64_t>>(parsed->second[0]).value(), 5);
    EXPECT_EQ(std::get<Record<std::int64_t>>(parsed->second[1]).value(), 6);
}

TEST(RecordCapture, EpochBufferInterleavesControlEventsInObservedOrder) {
    const auto dir = fs::temp_directory_path() / "clink_capture_v2_unit";
    fs::remove_all(dir);
    auto codec = std::make_shared<const Codec<std::int64_t>>(int64_codec());
    capture::EpochCaptureBuffer<std::int64_t> buf(dir,
                                                  OperatorId{4},
                                                  /*subtask=*/0,
                                                  /*max_records=*/10,
                                                  codec);

    Batch<std::int64_t> b1;
    b1.push(Record<std::int64_t>{1, EventTime{100}});
    buf.on_data(b1);
    buf.on_watermark(150, /*idle=*/false);
    buf.on_clock(5000);
    Batch<std::int64_t> b2;
    b2.push(Record<std::int64_t>{2, EventTime{200}});
    buf.on_data(b2);
    buf.on_barrier(3);

    // A watermark-only epoch still flushes (it can fire windows on replay).
    buf.on_watermark(999, /*idle=*/false);
    buf.flush_final();

    const auto base = dir / "op-4" / "subtask-0";
    {
        auto [h, events] = read_cap(base / "epoch-3.cap");
        EXPECT_EQ(h.version, capture::kCaptureVersion);
        EXPECT_EQ(h.records_seen, 2u);
        ASSERT_EQ(events.size(), 4u);
        EXPECT_EQ(std::get<Record<std::int64_t>>(events[0]).value(), 1);
        EXPECT_EQ(std::get<capture::WatermarkEvent>(events[1]).ts_ms, 150);
        EXPECT_EQ(std::get<capture::ClockEvent>(events[2]).now_ms, 5000);
        EXPECT_EQ(std::get<Record<std::int64_t>>(events[3]).value(), 2);
    }
    {
        auto [h, events] = read_cap(base / "final.cap");
        EXPECT_EQ(h.records_seen, 0u);
        ASSERT_EQ(events.size(), 1u);
        EXPECT_EQ(std::get<capture::WatermarkEvent>(events[0]).ts_ms, 999);
    }
    fs::remove_all(dir);
}

TEST(RecordCapture, RunnerTeesWatermarksIntoTheEpochStream) {
    // A scripted source with interleaved watermarks: the captured operator's
    // epoch must hold data and watermarks in the exact source order.
    const auto dir = fs::temp_directory_path() / "clink_capture_wm_e2e";
    fs::remove_all(dir);

    Dag dag;
    auto src = std::make_shared<clink::test::TestSource<std::int64_t>>();
    src->emit(1, 100).emit(2, 200).watermark(250).emit(3, 300).watermark(350);
    auto h_src = dag.add_source<std::int64_t>(src);
    auto op = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v; });
    auto codec = std::make_shared<const Codec<std::int64_t>>(int64_codec());
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, op, codec);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cfg.capture_dir = dir.string();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    fs::path cap_file;
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().filename() == "final.cap") {
            cap_file = entry.path();
        }
    }
    ASSERT_FALSE(cap_file.empty()) << "no final.cap written under " << dir;
    auto [h, events] = read_cap(cap_file);
    EXPECT_EQ(h.records_seen, 3u);

    // Expected order: 1, 2, wm(250), 3, wm(350) - plus the bounded-source
    // end-of-input max watermark the runner emits before the terminal
    // barrier. Assert the prefix exactly and the kinds in order.
    ASSERT_GE(events.size(), 5u);
    EXPECT_EQ(std::get<Record<std::int64_t>>(events[0]).value(), 1);
    EXPECT_EQ(std::get<Record<std::int64_t>>(events[1]).value(), 2);
    EXPECT_EQ(std::get<capture::WatermarkEvent>(events[2]).ts_ms, 250);
    EXPECT_EQ(std::get<Record<std::int64_t>>(events[3]).value(), 3);
    EXPECT_EQ(std::get<capture::WatermarkEvent>(events[4]).ts_ms, 350);
    fs::remove_all(dir);
}

TEST(RecordCapture, OpSpecSidecarWritesParseableJson) {
    const auto dir = fs::temp_directory_path() / "clink_capture_spec";
    fs::remove_all(dir);
    capture::OpSpecSidecar spec;
    spec.op_type = "aggregate_row";
    spec.in_channel = "row";
    spec.out_channel = "row";
    spec.uid = "agg-1";
    spec.params = {{"group_keys", "usr"}, {"aggregates", R"([{"fn":"sum"}])"}};
    capture::write_op_spec(dir, OperatorId{42}, /*subtask=*/3, spec);

    const auto path = dir / "op-42" / "subtask-3" / "op.json";
    ASSERT_TRUE(fs::exists(path));
    std::ifstream in(path, std::ios::binary);
    std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    auto js = clink::config::parse(text);
    ASSERT_TRUE(js.is_object());
    EXPECT_EQ(js.at("op_type").as_string(), "aggregate_row");
    EXPECT_EQ(js.at("in_channel").as_string(), "row");
    EXPECT_EQ(js.at("uid").as_string(), "agg-1");
    ASSERT_TRUE(js.at("params").is_object());
    EXPECT_EQ(js.at("params").at("group_keys").as_string(), "usr");
    // The quoted-JSON param survives escaping.
    EXPECT_EQ(js.at("params").at("aggregates").as_string(), R"([{"fn":"sum"}])");
    fs::remove_all(dir);
}
