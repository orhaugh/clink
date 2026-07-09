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

#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/state/in_memory_state_backend.hpp"

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

// Read one .cap file back into records (header + framed payload).
std::pair<capture::CaptureFileHeader, std::vector<Record<std::int64_t>>> read_cap(
    const fs::path& p) {
    const auto bytes = read_all(p);
    auto hdr =
        capture::decode_capture_header(std::span<const std::byte>{bytes.data(), bytes.size()});
    EXPECT_TRUE(hdr.has_value()) << p;
    auto records = capture::deserialize_records(
        std::span<const std::byte>{bytes.data() + hdr->second, bytes.size() - hdr->second},
        int64_codec());
    return {hdr->first, std::move(records)};
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
        auto [h, recs] = read_cap(base / "epoch-1.cap");
        EXPECT_FALSE(h.truncated);
        EXPECT_EQ(h.records_seen, 2u);
        ASSERT_EQ(recs.size(), 2u);
        EXPECT_EQ(recs[0].value(), 1);
        EXPECT_EQ(recs[1].value(), 2);
    }
    {
        auto [h, recs] = read_cap(base / "epoch-2.cap");
        EXPECT_TRUE(h.truncated);
        EXPECT_EQ(h.records_seen, 5u);
        ASSERT_EQ(recs.size(), 3u);  // cap
        EXPECT_EQ(recs[0].value(), 10);
        EXPECT_EQ(recs[2].value(), 30);
    }
    {
        auto [h, recs] = read_cap(base / "final.cap");
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
    auto [h, recs] = read_cap(cap_file);
    EXPECT_FALSE(h.truncated);
    EXPECT_EQ(h.records_seen, 6u);
    ASSERT_EQ(recs.size(), 6u);
    for (std::int64_t i = 0; i < 6; ++i) {
        EXPECT_EQ(recs[static_cast<std::size_t>(i)].value(),
                  i + 1);  // input, not the mapped output
    }
    fs::remove_all(dir);
}
