// Tests for the WebHDFS Parquet sink + source: required-param validation, clean failure against a
// dead endpoint, and a full write->read round-trip against an in-process httplib server that
// emulates WebHDFS's CREATE/OPEN 307-redirect protocol (CI-runnable, no external Hadoop). The
// gated round-trip against a real WebHDFS/HttpFS lives in test_webhdfs_parquet_live.cpp.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <httplib.h>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/connectors/webhdfs_filesystem.hpp"
#include "clink/connectors/webhdfs_parquet_sink.hpp"
#include "clink/connectors/webhdfs_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::int64_arrow_batcher;
using clink::OperatorId;
using clink::RuntimeContext;
using clink::StreamElement;
using clink::string_arrow_batcher;
using clink::WebHdfsMultiObjectParquetSource;
using clink::WebHdfsParquetSink;
using clink::WebHdfsParquetSink2PC;
using clink::WebHdfsParquetSource;

namespace {

// In-process WebHDFS/HttpFS emulator: PUT ?op=CREATE -> 307 to a datanode URL (itself + &dn=1);
// the redirected PUT stores the body keyed by path; GET ?op=OPEN -> 307 -> GET returns the body.
// Faithfully exercises the connector's two-step redirect transport over a real socket.
class MockWebHdfs {
public:
    MockWebHdfs() {
        svr_.Put(R"(/webhdfs/v1/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            const std::string op = req.has_param("op") ? req.get_param_value("op") : "";
            if (op == "CREATE" && !req.has_param("dn")) {
                res.status = 307;
                res.set_header("Location", base_url() + req.path + "?op=CREATE&dn=1");
                return;
            }
            if (op == "MKDIRS") {
                res.set_content(R"({"boolean":true})", "application/json");
                return;
            }
            if (op == "RENAME") {
                const std::string dest =
                    req.has_param("destination") ? req.get_param_value("destination") : "";
                const std::string dest_key = "/webhdfs/v1" + dest;
                bool ok = false;
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    auto it = files_.find(req.path);
                    if (it != files_.end() && !files_.contains(dest_key)) {
                        files_[dest_key] = it->second;
                        files_.erase(it);
                        ok = true;
                    }
                }
                res.set_content(ok ? R"({"boolean":true})" : R"({"boolean":false})",
                                "application/json");
                return;
            }
            {
                std::lock_guard<std::mutex> lk(mu_);
                files_[req.path] = req.body;
            }
            res.status = 201;
        });
        svr_.Delete(R"(/webhdfs/v1/.*)",
                    [this](const httplib::Request& req, httplib::Response& res) {
                        {
                            std::lock_guard<std::mutex> lk(mu_);
                            files_.erase(req.path);
                        }
                        res.set_content(R"({"boolean":true})", "application/json");
                    });
        svr_.Get(R"(/webhdfs/v1/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            const std::string op = req.has_param("op") ? req.get_param_value("op") : "";
            if (op == "GETFILESTATUS") {
                std::lock_guard<std::mutex> lk(mu_);
                if (files_.contains(req.path)) {
                    res.set_content(R"({"FileStatus":{"type":"FILE"}})", "application/json");
                    return;
                }
                // A directory is any path some stored file lives under.
                const std::string child_prefix = req.path + "/";
                for (const auto& [k, v] : files_) {
                    if (k.rfind(child_prefix, 0) == 0) {
                        res.set_content(R"({"FileStatus":{"type":"DIRECTORY"}})",
                                        "application/json");
                        return;
                    }
                }
                res.status = 404;
                return;
            }
            if (op == "LISTSTATUS") {
                // List the direct file children of req.path as a WebHDFS FileStatuses document.
                const std::string child_prefix = req.path + "/";
                std::string entries;
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    for (const auto& [k, v] : files_) {
                        if (k.rfind(child_prefix, 0) != 0) {
                            continue;
                        }
                        const std::string suffix = k.substr(child_prefix.size());
                        if (suffix.find('/') != std::string::npos) {
                            continue;  // not a direct child
                        }
                        if (!entries.empty()) {
                            entries += ",";
                        }
                        entries += R"({"pathSuffix":")" + suffix + R"(","type":"FILE"})";
                    }
                }
                res.set_content(R"({"FileStatuses":{"FileStatus":[)" + entries + "]}}",
                                "application/json");
                return;
            }
            if (op == "OPEN" && !req.has_param("dn")) {
                res.status = 307;
                res.set_header("Location", base_url() + req.path + "?op=OPEN&dn=1");
                return;
            }
            std::string body;
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(mu_);
                auto it = files_.find(req.path);
                if (it != files_.end()) {
                    body = it->second;
                    found = true;
                }
            }
            if (!found) {
                res.status = 404;
                return;
            }
            res.set_content(body, "application/octet-stream");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~MockWebHdfs() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string base_url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    // The HDFS paths of every stored file, for asserting what a sink left behind.
    std::vector<std::string> paths() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::string> out;
        for (const auto& [k, v] : files_) {
            out.push_back(k.substr(std::string{"/webhdfs/v1"}.size()));
        }
        return out;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::map<std::string, std::string> files_;
};

WebHdfsParquetSink<std::int64_t>::Options dead_sink_opts() {
    WebHdfsParquetSink<std::int64_t>::Options o;
    o.base_url = "http://127.0.0.1:1";  // no server
    o.path = "/x.parquet";
    o.connect_timeout_ms = 500;
    return o;
}

WebHdfsParquetSource<std::int64_t>::Options dead_source_opts() {
    WebHdfsParquetSource<std::int64_t>::Options o;
    o.base_url = "http://127.0.0.1:1";
    o.path = "/x.parquet";
    o.connect_timeout_ms = 500;
    return o;
}

}  // namespace

TEST(WebHdfsPathBuild, EncodesPathSegmentsButKeepsSlashesAndQuery) {
    using clink::webhdfs_detail::build_webhdfs_path;
    // A normal path (unreserved chars only) is passed through untouched.
    EXPECT_EQ(build_webhdfs_path("/clink/out.parquet", "CREATE", {}),
              "/webhdfs/v1/clink/out.parquet?op=CREATE");
    // Spaces and reserved query chars in a user path are percent-encoded per segment, so they
    // cannot corrupt the request target; the '/' separators and the op query are preserved.
    EXPECT_EQ(build_webhdfs_path("/a b/c%d.parquet", "CREATE", {}),
              "/webhdfs/v1/a%20b/c%25d.parquet?op=CREATE");
    EXPECT_EQ(build_webhdfs_path("/x?y#z", "OPEN", {}), "/webhdfs/v1/x%3Fy%23z?op=OPEN");
}

TEST(WebHdfsParquetSink, RequiresBaseUrlAndPath) {
    {
        WebHdfsParquetSink<std::int64_t>::Options o;  // both empty
        EXPECT_THROW((WebHdfsParquetSink<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
    {
        WebHdfsParquetSink<std::int64_t>::Options o;
        o.base_url = "http://nn:9870";  // path still empty
        EXPECT_THROW((WebHdfsParquetSink<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
}

TEST(WebHdfsParquetSource, RequiresBaseUrlAndPath) {
    {
        WebHdfsParquetSource<std::int64_t>::Options o;  // both empty
        EXPECT_THROW((WebHdfsParquetSource<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
    {
        WebHdfsParquetSource<std::int64_t>::Options o;
        o.base_url = "http://nn:9870";  // path still empty
        EXPECT_THROW((WebHdfsParquetSource<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
}

TEST(WebHdfsParquetSink, UploadAgainstDeadEndpointFailsCleanly) {
    // open() + on_data() only touch the in-memory Parquet buffer; the upload (and so the failure)
    // happens at close(). Assert the full cycle fails cleanly with a runtime_error.
    EXPECT_THROW(
        {
            WebHdfsParquetSink<std::int64_t> sink(dead_sink_opts(), int64_arrow_batcher());
            sink.open();
            Batch<std::int64_t> b;
            b.emplace(1);
            b.emplace(2);
            sink.on_data(b);
            sink.close();
        },
        std::runtime_error);
}

TEST(WebHdfsParquetSource, OpenAgainstDeadEndpointFailsCleanly) {
    WebHdfsParquetSource<std::int64_t> src(dead_source_opts(), int64_arrow_batcher());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());  // close after a failed open must be safe
}

TEST(WebHdfsParquetRoundTrip, Int64WriteThenRead) {
    MockWebHdfs srv;
    const std::string path = "/clink/rt_int64.parquet";

    {
        WebHdfsParquetSink<std::int64_t>::Options so;
        so.base_url = srv.base_url();
        so.path = path;
        WebHdfsParquetSink<std::int64_t> sink(so, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        b.emplace(10);
        b.emplace(20);
        b.emplace(30);
        sink.on_data(b);
        sink.close();
    }

    std::vector<std::int64_t> got;
    Emitter<std::int64_t> em([&](StreamElement<std::int64_t> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    WebHdfsParquetSource<std::int64_t>::Options ro;
    ro.base_url = srv.base_url();
    ro.path = path;
    WebHdfsParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
    src.open();
    while (src.produce(em)) {
    }
    src.close();

    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], 10);
    EXPECT_EQ(got[1], 20);
    EXPECT_EQ(got[2], 30);
}

TEST(WebHdfsParquetRoundTrip, StringWriteThenRead) {
    MockWebHdfs srv;
    const std::string path = "/clink/rt_string.parquet";

    {
        WebHdfsParquetSink<std::string>::Options so;
        so.base_url = srv.base_url();
        so.path = path;
        WebHdfsParquetSink<std::string> sink(so, string_arrow_batcher());
        sink.open();
        Batch<std::string> b;
        b.emplace(std::string{"alpha"});
        b.emplace(std::string{"beta"});
        sink.on_data(b);
        sink.close();
    }

    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    WebHdfsParquetSource<std::string>::Options ro;
    ro.base_url = srv.base_url();
    ro.path = path;
    WebHdfsParquetSource<std::string> src(ro, string_arrow_batcher());
    src.open();
    while (src.produce(em)) {
    }
    src.close();

    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], "alpha");
    EXPECT_EQ(got[1], "beta");
}

namespace {

void write_int64_object(MockWebHdfs& srv,
                        const std::string& path,
                        const std::vector<std::int64_t>& vals) {
    WebHdfsParquetSink<std::int64_t>::Options so;
    so.base_url = srv.base_url();
    so.path = path;
    WebHdfsParquetSink<std::int64_t> sink(so, int64_arrow_batcher());
    sink.open();
    Batch<std::int64_t> b;
    for (auto v : vals) {
        b.emplace(v);
    }
    sink.on_data(b);
    sink.close();
}

std::vector<std::int64_t> drain_dir_source(WebHdfsMultiObjectParquetSource<std::int64_t>& src) {
    std::vector<std::int64_t> out;
    Emitter<std::int64_t> em([&out](StreamElement<std::int64_t> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                out.push_back(rec.value());
            }
        }
        return true;
    });
    src.open();
    while (src.produce(em)) {
    }
    src.close();
    return out;
}

}  // namespace

TEST(WebHdfsMultiObjectParquet, ReadsEveryObjectUnderDir) {
    MockWebHdfs srv;
    write_int64_object(srv, "/clink/multi/a.parquet", {1, 2, 3});
    write_int64_object(srv, "/clink/multi/b.parquet", {4, 5});
    write_int64_object(srv, "/clink/multi/c.parquet", {6});

    WebHdfsMultiObjectParquetSource<std::int64_t>::Options ro;
    ro.base_url = srv.base_url();
    ro.dir = "/clink/multi";
    WebHdfsMultiObjectParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
    auto got = drain_dir_source(src);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}));
}

TEST(WebHdfsMultiObjectParquet, ShardsDisjointlyAndCompletelyAcrossSubtasks) {
    MockWebHdfs srv;
    write_int64_object(srv, "/clink/shard/f0.parquet", {0});
    write_int64_object(srv, "/clink/shard/f1.parquet", {10});
    write_int64_object(srv, "/clink/shard/f2.parquet", {20});
    write_int64_object(srv, "/clink/shard/f3.parquet", {30});

    std::vector<std::int64_t> all;
    for (int s = 0; s < 2; ++s) {
        WebHdfsMultiObjectParquetSource<std::int64_t>::Options ro;
        ro.base_url = srv.base_url();
        ro.dir = "/clink/shard";
        ro.subtask_idx = s;
        ro.parallelism = 2;
        WebHdfsMultiObjectParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
        auto part = drain_dir_source(src);
        all.insert(all.end(), part.begin(), part.end());
    }
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, (std::vector<std::int64_t>{0, 10, 20, 30}));
    EXPECT_EQ(all.size(), 4u);  // disjoint: no object read twice
}

TEST(WebHdfsMultiObjectParquet, EmptyDirYieldsNoRecords) {
    MockWebHdfs srv;
    WebHdfsMultiObjectParquetSource<std::int64_t>::Options ro;
    ro.base_url = srv.base_url();
    ro.dir = "/clink/does-not-exist";
    WebHdfsMultiObjectParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
    EXPECT_TRUE(drain_dir_source(src).empty());
}

namespace {

std::shared_ptr<WebHdfsParquetSink2PC<std::int64_t>> make_2pc_sink(MockWebHdfs& srv,
                                                                   const std::string& base,
                                                                   RuntimeContext& rctx,
                                                                   OperatorId id) {
    WebHdfsParquetSink2PC<std::int64_t>::Options o;
    o.base_url = srv.base_url();
    o.base = base;
    auto sink = std::make_shared<WebHdfsParquetSink2PC<std::int64_t>>(o, int64_arrow_batcher());
    sink->set_id(id);
    sink->attach_runtime(&rctx);
    return sink;
}

std::vector<std::int64_t> read_committed_dir(MockWebHdfs& srv, const std::string& committed_dir) {
    WebHdfsMultiObjectParquetSource<std::int64_t>::Options ro;
    ro.base_url = srv.base_url();
    ro.dir = committed_dir;
    WebHdfsMultiObjectParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
    return drain_dir_source(src);
}

}  // namespace

TEST(WebHdfsParquetSink2PC, CommitRenamesStagingToCommittedAndIsReadable) {
    MockWebHdfs srv;
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "wh2pc", &state, nullptr);
    auto sink = make_2pc_sink(srv, "/clink/exactly", rctx, OperatorId{42});

    sink->open();
    Batch<std::int64_t> b;
    b.emplace(10);
    b.emplace(20);
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{5}});
    EXPECT_TRUE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_5").has_value());
    EXPECT_TRUE(read_committed_dir(srv, "/clink/exactly/committed").empty());  // not committed yet

    sink->on_commit(5);
    EXPECT_FALSE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_5").has_value());

    auto got = read_committed_dir(srv, "/clink/exactly/committed");
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::int64_t>{10, 20}));
}

TEST(WebHdfsParquetSink2PC, FreshSinkCommitsPendingOnOpen) {
    MockWebHdfs srv;
    InMemoryStateBackend state;
    {
        RuntimeContext rctx(OperatorId{5}, "wh2pc", &state, nullptr);
        auto s1 = make_2pc_sink(srv, "/clink/rec", rctx, OperatorId{5});
        s1->open();
        Batch<std::int64_t> b;
        b.emplace(100);
        s1->on_data(b);
        s1->on_barrier(CheckpointBarrier{CheckpointId{7}});
        // "crash": no on_commit.
    }
    EXPECT_TRUE(read_committed_dir(srv, "/clink/rec/committed").empty());

    {
        RuntimeContext rctx(OperatorId{5}, "wh2pc", &state, nullptr);
        auto s2 = make_2pc_sink(srv, "/clink/rec", rctx, OperatorId{5});
        s2->open();  // recovery RENAMEs the pending staging file
    }
    EXPECT_FALSE(state.get_operator_state(OperatorId{5}, "_xo_pending_sub0_7").has_value());
    EXPECT_EQ(read_committed_dir(srv, "/clink/rec/committed"), (std::vector<std::int64_t>{100}));
}

TEST(WebHdfsParquetSink2PC, AHandleAnEarlierReleaseKeptAsARawKeyIsStillCommitted) {
    // Before the sink was a CommittingSink it kept the staging path under a raw
    // keyed "_2pc_pending_sub<N>_<ckpt>" row. A snapshot taken by that release
    // still holds such rows; opening on it must commit them.
    MockWebHdfs srv;
    {
        InMemoryStateBackend scratch;  // the stage only: its handle is thrown away
        RuntimeContext rctx(OperatorId{6}, "wh2pc", &scratch, nullptr);
        auto staged = make_2pc_sink(srv, "/clink/legacy", rctx, OperatorId{6});
        staged->open();
        Batch<std::int64_t> b;
        b.emplace(64);
        staged->on_data(b);
        staged->on_barrier(CheckpointBarrier{CheckpointId{3}});
    }
    InMemoryStateBackend state;
    const std::string staging = "/clink/legacy/staging/sub0-3.parquet";
    state.put(OperatorId{6}, "_2pc_pending_sub0_3", staging);
    RuntimeContext rctx(OperatorId{6}, "wh2pc", &state, nullptr);
    auto sink = make_2pc_sink(srv, "/clink/legacy", rctx, OperatorId{6});
    sink->open();
    EXPECT_FALSE(state.get(OperatorId{6}, "_2pc_pending_sub0_3").has_value());
    EXPECT_EQ(read_committed_dir(srv, "/clink/legacy/committed"), (std::vector<std::int64_t>{64}));
}

TEST(WebHdfsParquetSink2PC, AbortDeletesStagingAndClearsState) {
    MockWebHdfs srv;
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{9}, "wh2pc", &state, nullptr);
    auto sink = make_2pc_sink(srv, "/clink/ab", rctx, OperatorId{9});

    sink->open();
    Batch<std::int64_t> b;
    b.emplace(1);
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{9}});
    EXPECT_TRUE(state.get_operator_state(OperatorId{9}, "_xo_pending_sub0_9").has_value());

    sink->on_abort(9);
    EXPECT_FALSE(state.get_operator_state(OperatorId{9}, "_xo_pending_sub0_9").has_value());
    EXPECT_TRUE(read_committed_dir(srv, "/clink/ab/committed").empty());
}

// The plain webhdfs_parquet sink (`path`): a directory of part files over WebHDFS,
// each uploaded under a .inprogress name and renamed into place, so the
// directory never shows a partly written part.
TEST(WebHdfsParquetRollingSink, WritesPartsAKilledRunNeverPublishesAndARestoreKeeps) {
    MockWebHdfs srv;
    clink::WebHdfsFileSystem::Options fo;
    fo.base_url = srv.base_url();
    const auto make = [&]() {
        clink::ParquetRollingSink<std::int64_t>::Options o;
        o.dir = "/clink/rolling";
        return std::make_unique<clink::ParquetRollingSink<std::int64_t>>(
            [fo]() -> std::shared_ptr<arrow::fs::FileSystem> {
                return std::make_shared<clink::WebHdfsFileSystem>(fo);
            },
            std::move(o),
            int64_arrow_batcher());
    };
    const auto batch = [](std::int64_t from, std::int64_t to) {
        Batch<std::int64_t> b;
        for (auto v = from; v < to; ++v) {
            b.emplace(v);
        }
        return b;
    };
    const auto read_back = [&] {
        WebHdfsMultiObjectParquetSource<std::int64_t>::Options ro;
        ro.base_url = srv.base_url();
        ro.dir = "/clink/rolling";
        WebHdfsMultiObjectParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
        auto got = drain_dir_source(src);
        std::sort(got.begin(), got.end());
        return got;
    };

    {
        auto first = make();
        first->open();
        first->on_data(batch(0, 4));
        first->on_barrier(CheckpointBarrier{CheckpointId{1}});
        first->on_data(batch(4, 6));  // killed before the next barrier
    }
    for (const auto& p : srv.paths()) {
        EXPECT_TRUE(p.ends_with(".parquet")) << p << ": nothing half-written is left";
    }
    EXPECT_EQ(read_back(), (std::vector<std::int64_t>{0, 1, 2, 3}));

    RuntimeContext ctx(OperatorId{3}, "pq", nullptr, nullptr);
    ctx.set_commit_receipts("unused", /*restore_from_ckpt=*/1);
    auto resumed = make();
    resumed->attach_runtime(&ctx);
    resumed->open();
    resumed->on_data(batch(4, 6));
    resumed->flush();
    resumed->close();
    EXPECT_EQ(read_back(), (std::vector<std::int64_t>{0, 1, 2, 3, 4, 5}));

    auto fresh = make();
    fresh->open();  // no restore point: the previous run's parts are replaced
    fresh->on_data(batch(100, 101));
    fresh->flush();
    fresh->close();
    EXPECT_EQ(read_back(), (std::vector<std::int64_t>{100}));
}

// The factory reads a source `path` that names a directory as a directory of parts
// (register_factories.cpp make_source); it decides on this status.
TEST(WebHdfsFileSystem, ReportsFilesDirectoriesAndMissingPaths) {
    MockWebHdfs srv;
    write_int64_object(srv, "/clink/fsinfo/a.parquet", {1});
    clink::WebHdfsFileSystem::Options fo;
    fo.base_url = srv.base_url();
    clink::WebHdfsFileSystem fs(fo);
    EXPECT_EQ(fs.GetFileInfo("/clink/fsinfo/a.parquet").ValueOrDie().type(),
              arrow::fs::FileType::File);
    EXPECT_EQ(fs.GetFileInfo("/clink/fsinfo").ValueOrDie().type(), arrow::fs::FileType::Directory);
    EXPECT_EQ(fs.GetFileInfo("/clink/nothing").ValueOrDie().type(), arrow::fs::FileType::NotFound);
}
