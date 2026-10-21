// S3Sink2PC tests - the exactly-once raw-object S3 sink (multipart-upload-
// complete-on-commit) riding CommittingSink.
//
// The codec + construction tests run anywhere the AWS SDK is linked (no server).
// The LIVE integration tests are SKIPPED unless CLINK_S3_TEST_ENDPOINT +
// CLINK_S3_TEST_BUCKET are set (MinIO / LocalStack; credentials via the AWS env
// chain). They prove against a real object store:
//   * commit round-trip: the object is invisible after the barrier and appears
//     atomically on commit with the interval's data;
//   * crash recovery: a multipart upload survives the session, and a fresh sink
//     sharing the checkpoint state CompleteMultipartUploads it at open;
//   * abort discards the staged parts;
//   * idempotent commit; empty interval uploads nothing.

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/connectors/s3_sink.hpp"
#include "clink/connectors/s3_sink_2pc.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::RuntimeContext;
using clink::S3MultipartHandle;
using clink::S3Sink2PC;

namespace {

// --- codec + construction (no server) -----------------------------------

TEST(S3Sink2PC, RejectsEmptyBucket) {
    S3Sink2PC::Options o;
    o.key_prefix = "p";
    EXPECT_THROW(S3Sink2PC{std::move(o)}, std::runtime_error);
}

TEST(S3Sink2PC, CommittableCodecRoundTrips) {
    S3Sink2PC::Options o;
    o.bucket = "b";
    S3Sink2PC sink(std::move(o));

    S3MultipartHandle h;
    h.key = "prefix/sub0-7.ndjson";
    h.upload_id = "abc.DEF-123_xyz";
    h.parts = {{1, "\"etag-one\""}, {2, "\"etag-two-2\""}};

    const S3MultipartHandle back = sink.deserialize(sink.serialize(h));
    EXPECT_EQ(back.key, h.key);
    EXPECT_EQ(back.upload_id, h.upload_id);
    ASSERT_EQ(back.parts.size(), 2u);
    EXPECT_EQ(back.parts[0].first, 1);
    EXPECT_EQ(back.parts[0].second, "\"etag-one\"");
    EXPECT_EQ(back.parts[1].first, 2);
    EXPECT_EQ(back.parts[1].second, "\"etag-two-2\"");
}

#ifdef CLINK_HAS_AWS_S3

// --- live integration ---------------------------------------------------

bool s3_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}
std::string s3_endpoint() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT");
}
std::string s3_bucket() {
    return std::getenv("CLINK_S3_TEST_BUCKET");
}
std::string uniq() {
    return std::to_string(static_cast<long>(::getpid()));
}

Aws::S3::S3Client make_client() {
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = "us-east-1";
    cfg.endpointOverride = s3_endpoint();
    cfg.useVirtualAddressing = false;  // path-style for MinIO
    return Aws::S3::S3Client(cfg);
}

void ensure_bucket() {
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(s3_bucket());
    (void)make_client().CreateBucket(req);  // ignore "already owned by you"
}

bool object_exists(const std::string& key) {
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(s3_bucket());
    req.SetKey(key);
    return make_client().HeadObject(req).IsSuccess();
}

std::string get_object(const std::string& key) {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(s3_bucket());
    req.SetKey(key);
    auto out = make_client().GetObject(req);
    if (!out.IsSuccess()) {
        return {};
    }
    auto& body = out.GetResultWithOwnership().GetBody();
    std::string s((std::istreambuf_iterator<char>(body)), std::istreambuf_iterator<char>());
    return s;
}

S3Sink2PC::Options opts_for(const std::string& prefix) {
    S3Sink2PC::Options o;
    o.bucket = s3_bucket();
    o.key_prefix = prefix;
    o.region = "us-east-1";
    o.endpoint_override = s3_endpoint();
    return o;
}

std::shared_ptr<S3Sink2PC> make_sink(const std::string& prefix, RuntimeContext& rctx) {
    auto sink = std::make_shared<S3Sink2PC>(opts_for(prefix));
    sink->set_id(OperatorId{88});
    sink->set_uid("s3xo");
    sink->attach_runtime(&rctx);
    return sink;
}

Batch<std::string> lines(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs)
        b.emplace(s);
    return b;
}

#define REQUIRE_LIVE_S3()                                                        \
    do {                                                                         \
        if (!s3_configured())                                                    \
            GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET"; \
        clink::aws_sdk::ensure_initialized();                                    \
        ensure_bucket();                                                         \
    } while (0)

TEST(S3Sink2PCLive, CommitRoundTrips) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_commit_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"a", "b", "c"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    // Parts uploaded but the object is not visible until CompleteMultipartUpload.
    EXPECT_FALSE(object_exists(key));

    sink->on_commit(1);
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "a\nb\nc\n");
    sink->close();
}

TEST(S3Sink2PCLive, MultipartUploadSurvivesCrashAndRecovers) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_recover_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;  // survives the "crash"
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);

    {
        auto crashed = make_sink(prefix, rctx);
        crashed->open();
        crashed->on_data(lines({"x", "y"}));
        crashed->on_barrier(CheckpointBarrier{CheckpointId{1}});
        // No on_commit - process "crashes". The multipart upload's parts persist
        // on the server; the handle is durable in state.
    }
    EXPECT_FALSE(object_exists(key));

    auto restarted = make_sink(prefix, rctx);
    restarted->open();  // recover_all_ CompleteMultipartUploads the pending handle
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "x\ny\n");
    restarted->close();
}

TEST(S3Sink2PCLive, AbortDiscardsStagedParts) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_abort_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"z"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_abort(1);
    EXPECT_FALSE(object_exists(key));
    EXPECT_NO_THROW(sink->on_commit(1));  // key already gone -> no-op
    EXPECT_FALSE(object_exists(key));
    sink->close();
}

TEST(S3Sink2PCLive, CommitIsIdempotent) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_idem_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"only"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    EXPECT_NO_THROW(sink->on_commit(1));  // second (recovery-time) commit is a no-op
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "only\n");
    sink->close();
}

TEST(S3Sink2PCLive, EmptyIntervalUploadsNothing) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_empty_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // no data
    EXPECT_NO_THROW(sink->on_commit(1));
    EXPECT_FALSE(object_exists(key));
    sink->close();
}

// Beside another subtask, each subtask puts its parts under keys of its own,
// and a run restored from a checkpoint numbers its parts after the ones
// already there. The text sink used to name every subtask's first part
// part-0, so subtasks replaced each other's objects, and a restored run put
// its first part over part-0, whose records its sources would not replay.
TEST(S3TextSinkLive, SubtasksKeepTheirOwnPartsAndARestoredRunAppends) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3txt_" + uniq();
    const auto text_sink = [&](std::uint32_t idx, std::uint32_t par) {
        clink::S3Sink::Options o;
        o.bucket = s3_bucket();
        o.key_prefix = prefix;
        o.region = "us-east-1";
        o.endpoint_override = s3_endpoint();
        o.rollover_bytes = 1;  // one part per record
        o.subtask_idx = idx;
        o.parallelism = par;
        return std::make_unique<clink::S3Sink>(o);
    };
    {
        auto a = text_sink(0, 2);
        auto b = text_sink(1, 2);
        a->open();
        b->open();
        a->on_data(lines({"a0"}));
        b->on_data(lines({"b0"}));
        a->close();
        b->close();
    }
    EXPECT_EQ(get_object(prefix + "/sub0-part-0"), "a0\n");
    EXPECT_EQ(get_object(prefix + "/sub1-part-0"), "b0\n");

    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{90}, "s3txt", &state, nullptr);
    rctx.set_restore_from_checkpoint_id(1);
    auto restored = text_sink(0, 2);
    restored->attach_runtime(&rctx);
    restored->open();
    restored->on_data(lines({"a1"}));
    restored->close();
    EXPECT_EQ(get_object(prefix + "/sub0-part-0"), "a0\n") << "the restored run replaced a part";
    EXPECT_EQ(get_object(prefix + "/sub0-part-1"), "a1\n");

    // One subtask keeps the original layout.
    auto single = text_sink(0, 1);
    single->open();
    single->on_data(lines({"s0"}));
    single->close();
    EXPECT_EQ(get_object(prefix + "/part-0"), "s0\n");
}

// --- a refused part (stand-in endpoint, no server needed) ---------------

// send() flags: a client that closed its end must not raise SIGPIPE in the test.
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

// A stand-in S3 endpoint that starts a multipart upload, refuses every part
// with a non-retryable 400, and accepts an abort, recording each request line
// so a test can see what the sink sent. One connection at a time, each closed
// after its response.
class RefusingPartEndpoint {
public:
    RefusingPartEndpoint() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(fd_, 16) != 0) {
            throw std::runtime_error("RefusingPartEndpoint: cannot listen");
        }
        socklen_t len = sizeof(addr);
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve_(); });
    }

    ~RefusingPartEndpoint() {
        stop_.store(true);
        thread_.join();
        ::close(fd_);
    }

    RefusingPartEndpoint(const RefusingPartEndpoint&) = delete;
    RefusingPartEndpoint& operator=(const RefusingPartEndpoint&) = delete;

    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    [[nodiscard]] std::vector<std::string> requests() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return requests_;
    }

private:
    void serve_() {
        while (!stop_.load()) {
            pollfd p{fd_, POLLIN, 0};
            if (::poll(&p, 1, 50) <= 0) {
                continue;
            }
            const int c = ::accept(fd_, nullptr, nullptr);
            if (c >= 0) {
#ifdef SO_NOSIGPIPE
                int one = 1;
                ::setsockopt(c, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
                answer_(c);
                ::close(c);
            }
        }
    }

    // Reads one request (its head, then a Content-Length or chunked body) and
    // answers it. Returns quietly on a connection the client dropped.
    void answer_(int c) {
        std::string in;
        char buf[4096];
        std::size_t head_end = std::string::npos;
        while ((head_end = in.find("\r\n\r\n")) == std::string::npos) {
            const auto n = ::recv(c, buf, sizeof(buf), 0);
            if (n <= 0) {
                return;
            }
            in.append(buf, static_cast<std::size_t>(n));
        }
        const std::string head = in.substr(0, head_end);
        std::string lower = head;
        for (auto& ch : lower) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (lower.find("expect: 100-continue") != std::string::npos) {
            const std::string cont = "HTTP/1.1 100 Continue\r\n\r\n";
            ::send(c, cont.data(), cont.size(), kSendFlags);
        }
        std::string body = in.substr(head_end + 4);
        const auto read_more = [&]() {
            const auto n = ::recv(c, buf, sizeof(buf), 0);
            if (n <= 0) {
                return false;
            }
            body.append(buf, static_cast<std::size_t>(n));
            return true;
        };
        if (const auto at = lower.find("content-length:"); at != std::string::npos) {
            const std::size_t want = std::stoul(head.substr(at + 15));
            while (body.size() < want && read_more()) {
            }
        } else if (lower.find("transfer-encoding: chunked") != std::string::npos) {
            // The last chunk is a zero size, then optional trailers, then a blank line.
            while (body.find("\r\n0\r\n") == std::string::npos && body.rfind("0\r\n", 0) != 0) {
                if (!read_more()) {
                    return;
                }
            }
            while (body.size() < 4 || body.compare(body.size() - 4, 4, "\r\n\r\n") != 0) {
                if (!read_more()) {
                    return;
                }
            }
        }
        const std::string line = head.substr(0, head.find("\r\n"));
        {
            const std::lock_guard<std::mutex> lock(mu_);
            requests_.push_back(line);
        }
        std::string status = "404 Not Found";
        std::string payload;
        if (line.rfind("POST ", 0) == 0 && line.find("?uploads") != std::string::npos) {
            status = "200 OK";
            payload =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult>"
                "<Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId>"
                "</InitiateMultipartUploadResult>";
        } else if (line.rfind("PUT ", 0) == 0 && line.find("partNumber=") != std::string::npos) {
            status = "400 Bad Request";
            payload =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code>"
                "<Message>the stand-in refuses every part</Message></Error>";
        } else if (line.rfind("DELETE ", 0) == 0 && line.find("uploadId=") != std::string::npos) {
            status = "204 No Content";
        }
        const std::string response =
            "HTTP/1.1 " + status + "\r\nContent-Type: application/xml\r\nContent-Length: " +
            std::to_string(payload.size()) + "\r\nConnection: close\r\n\r\n" + payload;
        ::send(c, response.data(), response.size(), kSendFlags);
    }

    int fd_{-1};
    std::uint16_t port_{0};
    std::atomic<bool> stop_{false};
    std::thread thread_;
    mutable std::mutex mu_;
    std::vector<std::string> requests_;
};

// A part the store refuses fails the prepare, and the partial upload is
// aborted first. The abort used to go through abort(), which locks the mutex
// prepare_commit() already holds, so the task thread deadlocked inside the
// checkpoint: this test's prepare never returned, and the upload was left
// behind.
TEST(S3Sink2PC, ARefusedPartAbortsTheUploadAndFailsThePrepare) {
    clink::aws_sdk::ensure_initialized();
    ::setenv("AWS_ACCESS_KEY_ID", "stand-in", 0);
    ::setenv("AWS_SECRET_ACCESS_KEY", "stand-in", 0);
    ::setenv("AWS_EC2_METADATA_DISABLED", "true", 0);
    RefusingPartEndpoint store;
    S3Sink2PC::Options o;
    o.bucket = "bucket";
    o.key_prefix = "refused";
    o.region = "us-east-1";
    o.endpoint_override = store.endpoint();
    // Shared with the prepare's thread, which a deadlock would leave holding it.
    auto sink = std::make_shared<S3Sink2PC>(o);
    sink->open();
    sink->on_data(lines({"a", "b"}));

    auto outcome = std::make_shared<std::promise<std::string>>();
    auto done = outcome->get_future();
    std::thread prepare([sink, outcome] {
        try {
            (void)sink->prepare_commit(1);
            outcome->set_value("returned without an error");
        } catch (const std::exception& e) {
            outcome->set_value(e.what());
        }
    });
    if (done.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
        // The prepare's thread is stuck and cannot be joined; exit the binary
        // rather than hang it.
        std::fprintf(stderr,
                     "S3Sink2PC.ARefusedPartAbortsTheUploadAndFailsThePrepare: prepare_commit "
                     "did not return within 30 s: it deadlocked aborting the upload\n");
        std::fflush(stderr);
        std::_Exit(1);
    }
    prepare.join();
    const std::string error = done.get();
    EXPECT_NE(error.find("UploadPart"), std::string::npos) << error;
    bool aborted = false;
    for (const auto& line : store.requests()) {
        aborted = aborted || (line.rfind("DELETE ", 0) == 0 &&
                              line.find("uploadId=upload-1") != std::string::npos);
    }
    EXPECT_TRUE(aborted) << "no AbortMultipartUpload reached the store";
    sink->close();
}

#endif  // CLINK_HAS_AWS_S3

}  // namespace
