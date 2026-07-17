// Tests for the spdlog-backed logging core: the LogBuffer ring + its frontend
// query filters, the operator log seam fallback, the LogBufferSink bridge, and
// the zstd compressing rotating file sink.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/logger.h>
#include <spdlog/spdlog.h>

#include "clink/runtime/compressing_rotating_file_sink.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/log_buffer_sink.hpp"
#include "clink/runtime/logging.hpp"

namespace {

clink::LogRecord rec(std::int64_t ts, std::string level, std::string source, std::string message) {
    clink::LogRecord r;
    r.ts_ms = ts;
    r.level = std::move(level);
    r.source = std::move(source);
    r.message = std::move(message);
    return r;
}

std::filesystem::path temp_dir(const std::string& leaf) {
    auto dir = std::filesystem::temp_directory_path() / ("clink_logtest_" + leaf);
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    return dir;
}

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

// --------------------------------------------------------------------------
// LogBuffer ring + frontend query filters
// --------------------------------------------------------------------------

TEST(LogBufferTest, TailLevelFilterMatchesRankOrdering) {
    clink::LogBuffer buf(16);
    buf.push(rec(1, "debug", "s", "d"));
    buf.push(rec(2, "info", "s", "i"));
    buf.push(rec(3, "warn", "s", "w"));
    buf.push(rec(4, "error", "s", "e"));

    EXPECT_EQ(buf.tail(100, "").size(), 4U);
    EXPECT_EQ(buf.tail(100, "debug").size(), 4U);
    EXPECT_EQ(buf.tail(100, "info").size(), 3U);   // drops debug
    EXPECT_EQ(buf.tail(100, "warn").size(), 2U);   // warn + error
    EXPECT_EQ(buf.tail(100, "error").size(), 1U);  // error only
    EXPECT_EQ(buf.tail(100, "error").front().message, "e");
}

TEST(LogBufferTest, TailSinceMsCursor) {
    clink::LogBuffer buf(16);
    buf.push(rec(100, "info", "s", "a"));
    buf.push(rec(200, "info", "s", "b"));
    buf.push(rec(300, "info", "s", "c"));

    auto after = buf.tail(100, "", /*since_ms=*/150);
    ASSERT_EQ(after.size(), 2U);
    EXPECT_EQ(after.front().message, "b");
    EXPECT_EQ(after.back().message, "c");

    EXPECT_TRUE(buf.tail(100, "", /*since_ms=*/300).empty());  // strictly newer
}

TEST(LogBufferTest, TailSourcePrefixFilter) {
    clink::LogBuffer buf(16);
    buf.push(rec(1, "info", "coordinator.register", "x"));
    buf.push(rec(2, "info", "coordinator.watchdog", "y"));
    buf.push(rec(3, "info", "worker.run", "z"));

    EXPECT_EQ(buf.tail(100, "", 0, "coordinator.").size(), 2U);
    EXPECT_EQ(buf.tail(100, "", 0, "worker.").size(), 1U);
    EXPECT_EQ(buf.tail(100, "", 0, "coordinator.register").size(), 1U);
    EXPECT_TRUE(buf.tail(100, "", 0, "nope").empty());
}

TEST(LogBufferTest, TailAppliesFiltersBeforeLimit) {
    clink::LogBuffer buf(16);
    buf.push(rec(1, "info", "a", "1"));
    buf.push(rec(2, "error", "a", "2"));
    buf.push(rec(3, "info", "a", "3"));
    buf.push(rec(4, "error", "a", "4"));
    // limit 1 over the error-filtered set returns the NEWEST matching error.
    auto out = buf.tail(1, "error");
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out.front().message, "4");
}

TEST(LogBufferTest, DistinctSourcesSortedUnique) {
    clink::LogBuffer buf(16);
    buf.push(rec(1, "info", "worker.run", "x"));
    buf.push(rec(2, "info", "coordinator.register", "y"));
    buf.push(rec(3, "info", "worker.run", "z"));  // dup
    auto sources = buf.distinct_sources();
    ASSERT_EQ(sources.size(), 2U);
    EXPECT_EQ(sources[0], "coordinator.register");  // sorted
    EXPECT_EQ(sources[1], "worker.run");
}

// --------------------------------------------------------------------------
// Operator log seam fallback (null host logger -> legacy ring path)
// --------------------------------------------------------------------------

TEST(OperatorLogSeam, NullLoggerFallsBackToGlobalRingWithLevelMapping) {
    const std::string marker = "optest.fallback.unique";
    clink::logging::op_log(nullptr, clink::LogSeverity::Debug, marker, "dmsg");
    clink::logging::op_log(nullptr, clink::LogSeverity::Info, marker, "imsg");
    clink::logging::op_log(nullptr, clink::LogSeverity::Warn, marker, "wmsg");
    clink::logging::op_log(nullptr, clink::LogSeverity::Error, marker, "emsg");

    auto all = clink::LogBuffer::global().tail(1000, "", 0, marker);
    ASSERT_EQ(all.size(), 4U);
    EXPECT_EQ(all[0].level, "debug");
    EXPECT_EQ(all[1].level, "info");
    EXPECT_EQ(all[2].level, "warn");
    EXPECT_EQ(all[3].level, "error");
    EXPECT_EQ(all[0].source, marker);
    EXPECT_EQ(all[3].message, "emsg");

    // The level strings must satisfy the ring's own filter ordering.
    EXPECT_EQ(clink::LogBuffer::global().tail(1000, "error", 0, marker).size(), 1U);
    EXPECT_EQ(clink::LogBuffer::global().tail(1000, "warn", 0, marker).size(), 2U);
}

// --------------------------------------------------------------------------
// LogBufferSink: maps a spdlog log_msg onto the four-field LogRecord
// --------------------------------------------------------------------------

TEST(LogBufferSinkDirect, MapsLevelSourceAndMessage) {
    clink::LogBuffer priv(32);
    auto sink = std::make_shared<clink::logging::LogBufferSinkMt>(priv);
    spdlog::logger lg("my.component", sink);
    lg.set_level(spdlog::level::trace);

    lg.trace("t");
    lg.debug("d");
    lg.info("i");
    lg.warn("w");
    lg.error("e");

    auto out = priv.tail(100, "");
    ASSERT_EQ(out.size(), 5U);
    // %n (logger name) becomes the record source.
    EXPECT_EQ(out[0].source, "my.component");
    // trace and debug both collapse to "debug" (no "trace" rank in the ring).
    EXPECT_EQ(out[0].level, "debug");
    EXPECT_EQ(out[1].level, "debug");
    EXPECT_EQ(out[2].level, "info");
    EXPECT_EQ(out[3].level, "warn");
    EXPECT_EQ(out[4].level, "error");
    // message is the raw payload, no pattern prefix.
    EXPECT_EQ(out[2].message, "i");
}

// Asymmetric-construction guard: a sink built against a PRIVATE buffer must
// never leak into LogBuffer::global(). This catches a regression that reverts
// operator logging to a process-global ring resolved per-.so.
TEST(LogBufferSinkDirect, PrivateBufferDoesNotTouchGlobalRing) {
    const std::string marker = "private.buffer.guard.unique";
    const auto before = clink::LogBuffer::global().tail(10000, "", 0, marker).size();

    clink::LogBuffer priv(8);
    auto sink = std::make_shared<clink::logging::LogBufferSinkMt>(priv);
    spdlog::logger lg(marker, sink);
    lg.info("should-stay-private");

    EXPECT_EQ(priv.tail(10, "").size(), 1U);
    const auto after = clink::LogBuffer::global().tail(10000, "", 0, marker).size();
    EXPECT_EQ(before, after);  // global ring untouched
}

// --------------------------------------------------------------------------
// CompressingRotatingFileSink: rotation, zstd compression, retention
// --------------------------------------------------------------------------

TEST(CompressingSink, RotatesAndZstdCompressesSegments) {
    auto dir = temp_dir("zstd");
    const auto base = (dir / "clink.log").string();
    {
        // ~120-byte rotation threshold so a handful of lines rotate.
        auto sink = std::make_shared<clink::logging::CompressingRotatingFileSinkMt>(
            base, /*max_size=*/120, /*max_files=*/5, /*compress=*/true, /*zstd_level=*/3);
        spdlog::logger lg("rot", sink);
        lg.set_pattern("%v");
        for (int i = 0; i < 60; ++i) {
            lg.info("line-{:04d}-padding-padding-padding", i);
        }
        lg.flush();
    }

    // At least one compressed segment exists, with the zstd frame magic.
    auto seg1 = dir / "clink.1.log.zst";
    ASSERT_TRUE(std::filesystem::exists(seg1)) << "expected a .zst rotated segment";
    auto bytes = read_file(seg1);
    ASSERT_GE(bytes.size(), 4U);
    EXPECT_EQ(static_cast<unsigned char>(bytes[0]), 0x28U);
    EXPECT_EQ(static_cast<unsigned char>(bytes[1]), 0xB5U);
    EXPECT_EQ(static_cast<unsigned char>(bytes[2]), 0x2FU);
    EXPECT_EQ(static_cast<unsigned char>(bytes[3]), 0xFDU);

    // Retention: never more than max_files (5) compressed segments.
    int zst_count = 0;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".zst") {
            ++zst_count;
        }
    }
    EXPECT_LE(zst_count, 5);
    EXPECT_GE(zst_count, 1);
}

TEST(CompressingSink, PlainRotationPreservesContent) {
    auto dir = temp_dir("plain");
    const auto base = (dir / "clink.log").string();
    {
        auto sink = std::make_shared<clink::logging::CompressingRotatingFileSinkMt>(
            base, /*max_size=*/100, /*max_files=*/5, /*compress=*/false, /*zstd_level=*/3);
        spdlog::logger lg("rot", sink);
        lg.set_pattern("%v");
        for (int i = 0; i < 30; ++i) {
            lg.info("UNIQUEMARK-{:03d}", i);
        }
        lg.flush();
    }
    // With compression off, rotated segments are plain .log and human-readable.
    // Concatenate every segment + the active file: all 30 marks survive.
    std::string all;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        all += read_file(e.path());
    }
    for (int i = 0; i < 30; ++i) {
        const std::string num = std::to_string(i);
        const std::string want = "UNIQUEMARK-" + std::string(3 - num.size(), '0') + num;
        EXPECT_NE(all.find(want), std::string::npos) << "lost segment line " << want;
    }
}

// --------------------------------------------------------------------------
// Integrated init() + facade: source becomes %n, level config gates, file +
// ring both fed. init()/shutdown() bracket keeps this isolated within the
// process-global logging state.
// --------------------------------------------------------------------------

TEST(LoggingInit, FacadeFeedsRingAndFileWithSourceAsName) {
    auto dir = temp_dir("init");
    const auto file = (dir / "node.log").string();
    clink::logging::LoggingConfig cfg;
    cfg.level = "debug";
    cfg.node_name = "coordinator";
    cfg.file_path = file;
    cfg.console = false;  // keep test output clean
    cfg.async = false;    // synchronous: assert immediately after logging
    cfg.compress_rotated = false;
    clink::logging::init(cfg);

    const std::string marker = "init.facade.unique";
    clink::log::debug(marker, "dbg-line");
    clink::log::info(marker, "worker=foo slots=4");
    clink::log::error(marker, "err-line");

    // Ring: source == the call-site source (the %n routing), level mapped.
    auto ring = clink::LogBuffer::global().tail(1000, "", 0, marker);
    ASSERT_EQ(ring.size(), 3U);
    EXPECT_EQ(ring[0].source, marker);
    EXPECT_EQ(ring[0].level, "debug");
    EXPECT_EQ(ring[1].message, "worker=foo slots=4");
    EXPECT_EQ(ring[2].level, "error");

    // ?level=error drops the lower levels (the /api/v1/logs filter contract).
    EXPECT_EQ(clink::LogBuffer::global().tail(1000, "error", 0, marker).size(), 1U);

    // File got the rendered lines (pattern carries [source] and the message).
    const auto contents = read_file(file);
    EXPECT_NE(contents.find(marker), std::string::npos);
    EXPECT_NE(contents.find("worker=foo slots=4"), std::string::npos);

    clink::logging::shutdown();
}

// Async pipeline under concurrent producers: exercises spdlog's worker pool +
// flush thread + the mutex-guarded sinks (the surface ThreadSanitizer probes).
// shutdown() drains the async queue, so after it the ring holds every record.
TEST(LoggingInit, AsyncConcurrentProducersAllRecordsLandNoRace) {
    auto dir = temp_dir("async");
    clink::logging::LoggingConfig cfg;
    cfg.level = "info";
    cfg.node_name = "coordinator";
    cfg.file_path = (dir / "node.log").string();
    cfg.console = false;
    cfg.async = true;  // the path clink_node uses by default
    clink::logging::init(cfg);

    const std::string marker = "async.concurrent.unique";
    constexpr int kThreads = 4;
    constexpr int kPerThread = 200;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                clink::log::info(marker, "t" + std::to_string(t) + "-" + std::to_string(i));
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    // shutdown() flushes + joins the worker/flush threads; all queued records
    // are durable in the ring afterwards.
    clink::logging::shutdown();

    auto ring = clink::LogBuffer::global().tail(100000, "", 0, marker);
    EXPECT_EQ(ring.size(), static_cast<std::size_t>(kThreads * kPerThread));
}

TEST(LoggingInit, ConfiguredLevelGatesLowerSeverities) {
    auto dir = temp_dir("gate");
    const auto file = (dir / "node.log").string();
    clink::logging::LoggingConfig cfg;
    cfg.level = "warn";
    cfg.node_name = "worker@x";
    cfg.file_path = file;
    cfg.console = false;
    cfg.async = false;
    clink::logging::init(cfg);

    const std::string marker = "init.gate.unique";
    clink::log::info(marker, "info-dropped");
    clink::log::warn(marker, "warn-kept");
    clink::log::error(marker, "error-kept");

    auto ring = clink::LogBuffer::global().tail(1000, "", 0, marker);
    ASSERT_EQ(ring.size(), 2U);  // info gated out at the logger level
    EXPECT_EQ(ring[0].level, "warn");
    EXPECT_EQ(ring[1].level, "error");

    const auto contents = read_file(file);
    EXPECT_EQ(contents.find("info-dropped"), std::string::npos);
    EXPECT_NE(contents.find("warn-kept"), std::string::npos);

    clink::logging::shutdown();
}
