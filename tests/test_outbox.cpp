// The outbox: one connection's outbound frames, queued without waiting on the
// peer and written by one thread (src/cluster/outbox.hpp).
//
// Every test runs the outbox over a real loopback TCP connection, because the
// behaviours pinned here are about a socket: a peer that never reads holds
// the writer in send, a reset fails it, and closing the transport is what
// wakes it. The TLS case of the abort wake is in impls/tls/tests.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/cluster/frame_io.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/network/network_socket.hpp"

#include "src/cluster/outbox.hpp"
#include "tests/test_helpers/sanitizer_slack.hpp"

namespace {

namespace fault = clink::fault;
using clink::cluster::FrameClass;
using clink::cluster::Outbox;
using clink::cluster::OutboxLimits;
using clink::cluster::OutboxState;
using clink::cluster::OutboxStopReason;
using clink::cluster::PostResult;
using clink::network::Connection;
using clink::network::NetworkSocket;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

constexpr std::size_t kMiB = std::size_t{1024} * 1024;

// Far more than a loopback connection's two socket buffers hold on any
// platform, so a writer sending it to a peer that does not read is held in
// send until something wakes it.
constexpr std::size_t kMoreThanTheBuffersHold = 32 * kMiB;

// Bounds a wait for something that should happen promptly: a pass is never a
// slow machine, and a fail is a missing behaviour rather than a hang.
const auto kPrompt = clink::test_support::scale_slack(Clock::duration{5s});

// How long a writer held in send may take to return once the outbox has
// closed the transport under it. Not scaled for sanitizers, because this
// timing is the contract: it must stay inside the five seconds after which
// Darwin's zero-window probe lets one more chunk through to a peer that has
// read nothing, or a writer that returned only because that chunk went out,
// and then saw the stop, would pass for one the close woke.
constexpr auto kWake = 2s;

// A frame as encode_frame lays one out: a 4-byte big-endian length, then a
// body whose first 8 bytes are `tag` and whose rest is a pattern drawn from
// it, so a frame that arrives cut short, or out of order, shows.
Outbox::Frame outbox_test_frame(std::uint64_t tag, std::size_t body_bytes) {
    body_bytes = std::max<std::size_t>(body_bytes, 8);
    auto bytes = std::make_shared<std::vector<std::byte>>(4 + body_bytes);
    auto& b = *bytes;
    const auto len = static_cast<std::uint32_t>(body_bytes);
    for (std::size_t i = 0; i < 4; ++i) {
        b[i] = static_cast<std::byte>((len >> (24 - 8 * i)) & 0xffU);
    }
    for (std::size_t i = 0; i < 8; ++i) {
        b[4 + i] = static_cast<std::byte>((tag >> (56 - 8 * i)) & 0xffU);
    }
    for (std::size_t i = 8; i < body_bytes; ++i) {
        b[4 + i] = static_cast<std::byte>((tag + i) & 0xffU);
    }
    return bytes;
}

// The tag of a frame body built by outbox_test_frame, or nullopt when the
// body is not the one built for that tag.
std::optional<std::uint64_t> outbox_test_tag(const std::vector<std::byte>& body) {
    if (body.size() < 8) {
        return std::nullopt;
    }
    std::uint64_t tag = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        tag = (tag << 8) | static_cast<std::uint64_t>(body[i]);
    }
    for (std::size_t i = 8; i < body.size(); ++i) {
        if (body[i] != static_cast<std::byte>((tag + i) & 0xffU)) {
            return std::nullopt;
        }
    }
    return tag;
}

// A loopback TCP connection. The outbox writes on `local`; the test is the
// peer on `remote`. With `small_buffers`, both ends' buffers are pinned as
// small as the platform lets a test pin them, so a peer that does not read
// holds the writer after little data.
struct OutboxTestPair {
    std::shared_ptr<Connection> local;
    std::shared_ptr<Connection> remote;
    int remote_fd{-1};
};

OutboxTestPair outbox_test_pair(bool small_buffers = true) {
    OutboxTestPair pair;
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    if (listener < 0) {
        ADD_FAILURE() << "could not listen on loopback";
        return pair;
    }
    const int remote = ::socket(AF_INET, SOCK_STREAM, 0);
    const int small = 16 * 1024;
    if (small_buffers) {
        // Before the connect, so the window advertised is small from the start.
        (void)::setsockopt(remote, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(remote, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        ADD_FAILURE() << "could not connect on loopback";
        ::close(remote);
        NetworkSocket::close(listener);
        return pair;
    }
    const int local = NetworkSocket::accept_one(listener);
    NetworkSocket::close(listener);
    if (small_buffers) {
        (void)::setsockopt(local, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    }
    pair.local = clink::network::make_plain_connection(local);
    pair.remote = clink::network::make_plain_connection(remote);
    pair.remote_fd = remote;
    return pair;
}

// Resets the connection from the peer's end, so a writer held in send fails
// at once. The cleanup for a test whose outbox did not free its writer.
void outbox_test_hang_up(OutboxTestPair& pair) {
    if (pair.remote && pair.remote_fd >= 0) {
        const linger reset{1, 0};
        (void)::setsockopt(pair.remote_fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    }
    pair.remote.reset();
    pair.remote_fd = -1;
}

// Runs an outbox's writer on a thread of its own, as an owner does.
class OutboxWriterThread {
public:
    explicit OutboxWriterThread(std::shared_ptr<Outbox> outbox)
        : outbox_(std::move(outbox)), thread_([this] { run_(); }) {}
    // A test that leaves its outbox open is stopped here, so the join cannot
    // wait on a writer with nothing left to do.
    ~OutboxWriterThread() {
        outbox_->abort("the test is over");
        thread_.join();
    }
    OutboxWriterThread(const OutboxWriterThread&) = delete;
    OutboxWriterThread& operator=(const OutboxWriterThread&) = delete;
    OutboxWriterThread(OutboxWriterThread&&) = delete;
    OutboxWriterThread& operator=(OutboxWriterThread&&) = delete;

    // True once run_writer has returned, waiting up to `bound`.
    bool returned_within(Clock::duration bound) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, bound, [this] { return returned_; });
    }

private:
    void run_() {
        outbox_->run_writer();
        std::lock_guard lock(mu_);
        returned_ = true;
        cv_.notify_all();
    }

    std::shared_ptr<Outbox> outbox_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool returned_{false};
    std::thread thread_;
};

// The peer, reading frames until the connection ends and recording each
// frame's tag in the order read.
class OutboxTestReader {
public:
    explicit OutboxTestReader(std::shared_ptr<Connection> conn)
        : conn_(std::move(conn)), thread_([this] { run_(); }) {}
    // A read the test left waiting is woken by closing the connection.
    ~OutboxTestReader() {
        if (!finished_within(Clock::duration::zero())) {
            conn_->close();
        }
        thread_.join();
    }
    OutboxTestReader(const OutboxTestReader&) = delete;
    OutboxTestReader& operator=(const OutboxTestReader&) = delete;
    OutboxTestReader(OutboxTestReader&&) = delete;
    OutboxTestReader& operator=(OutboxTestReader&&) = delete;

    // True once the connection has ended (or a frame did not check out).
    bool finished_within(Clock::duration bound) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, bound, [this] { return finished_; });
    }
    std::vector<std::uint64_t> tags() {
        std::lock_guard lock(mu_);
        return tags_;
    }
    bool intact() {
        std::lock_guard lock(mu_);
        return intact_;
    }

private:
    void run_() {
        while (true) {
            auto body = clink::cluster::read_frame(*conn_);
            if (!body.has_value()) {
                break;
            }
            const auto tag = outbox_test_tag(*body);
            std::lock_guard lock(mu_);
            if (!tag.has_value()) {
                intact_ = false;
                break;
            }
            tags_.push_back(*tag);
        }
        std::lock_guard lock(mu_);
        finished_ = true;
        cv_.notify_all();
    }

    std::shared_ptr<Connection> conn_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<std::uint64_t> tags_;
    bool intact_{true};
    bool finished_{false};
    std::thread thread_;
};

// True when the peer reads the end of the stream, waiting up to kPrompt for
// it; false when the stream stays open, or data is still there to read.
bool outbox_test_peer_sees_end(const OutboxTestPair& pair) {
    pollfd readable{pair.remote_fd, POLLIN, 0};
    const auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(kPrompt).count();
    if (::poll(&readable, 1, static_cast<int>(wait_ms)) != 1) {
        return false;
    }
    std::byte b{};
    return ::recv(pair.remote_fd, &b, 1, MSG_PEEK) == 0;
}

template <typename Pred>
bool outbox_test_await(Pred pred, Clock::duration bound) {
    const auto deadline = Clock::now() + bound;
    while (!pred()) {
        if (Clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(2ms);
    }
    return true;
}

std::uint64_t outbox_test_discards(FrameClass cls, OutboxStopReason reason) {
    return clink::MetricsRegistry::global()
        .counter(clink::metrics::outbound_frames_discarded_name(clink::cluster::to_string(cls),
                                                                clink::cluster::to_string(reason)))
        .value();
}

// True when bytes the writer sent have reached the peer, waiting up to
// kPrompt: the writer has started on a frame, and is past every check it
// makes before the first chunk.
bool outbox_test_peer_has_bytes(const OutboxTestPair& pair) {
    pollfd readable{pair.remote_fd, POLLIN, 0};
    const auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(kPrompt).count();
    return ::poll(&readable, 1, static_cast<int>(wait_ms)) == 1 && (readable.revents & POLLIN) != 0;
}

std::size_t outbox_test_log_lines_naming(const std::string& peer) {
    std::size_t n = 0;
    for (const auto& r : clink::LogBuffer::global().tail(1024, "warn", 0, "coordinator.outbox")) {
        if (r.message.find(peer) != std::string::npos) {
            ++n;
        }
    }
    return n;
}

}  // namespace

// --- Order --------------------------------------------------------------------

// Each posting thread's frames reach the peer in the order it posted them,
// whole, however the writer interleaves the threads.
TEST(Outbox, FramesFromEightPostingThreadsKeepEachThreadsOrder) {
    auto pair = outbox_test_pair(/*small_buffers=*/false);
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'fifo'");
    OutboxTestReader reader(pair.remote);
    OutboxWriterThread writer(outbox);

    constexpr std::uint64_t kThreads = 8;
    constexpr std::uint64_t kPerThread = 400;
    std::atomic<int> refused{0};
    {
        std::vector<std::thread> posters;
        for (std::uint64_t t = 0; t < kThreads; ++t) {
            posters.emplace_back([&outbox, &refused, t] {
                for (std::uint64_t i = 0; i < kPerThread; ++i) {
                    // Every fiftieth frame spans several write chunks.
                    const std::size_t body =
                        i % 50 == 0 ? 3 * Outbox::kWriteChunkBytes + 100 : std::size_t{64};
                    const auto frame = outbox_test_frame((t << 32) | i, body);
                    if (outbox->post(frame, FrameClass::Control) != PostResult::Queued) {
                        refused.fetch_add(1);
                    }
                }
            });
        }
        for (auto& p : posters) {
            p.join();
        }
    }
    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
    ASSERT_TRUE(reader.finished_within(kPrompt));

    EXPECT_EQ(refused.load(), 0);
    EXPECT_TRUE(reader.intact()) << "a frame arrived cut short or mixed with another";
    const auto tags = reader.tags();
    ASSERT_EQ(tags.size(), kThreads * kPerThread);
    std::array<std::uint64_t, kThreads> next{};
    for (const auto tag : tags) {
        const auto t = tag >> 32;
        const auto i = tag & 0xffffffffU;
        ASSERT_LT(t, kThreads);
        ASSERT_EQ(i, next[t]) << "thread " << t
                              << "'s frames arrived out of the order it posted them";
        next[t] = i + 1;
    }
}

// Frames posted under one lock reach the peer in the order the lock was
// taken: the order of frames on a connection is the order of the decisions
// that posted them.
TEST(Outbox, FramesPostedUnderAnExternalLockArriveInThatLocksOrder) {
    auto pair = outbox_test_pair(/*small_buffers=*/false);
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'decisions'");
    OutboxTestReader reader(pair.remote);
    OutboxWriterThread writer(outbox);

    constexpr int kThreads = 8;
    constexpr int kPerThread = 400;
    std::mutex decisions;
    std::uint64_t next_decision = 0;
    std::atomic<int> refused{0};
    {
        std::vector<std::thread> posters;
        for (int t = 0; t < kThreads; ++t) {
            posters.emplace_back([&] {
                for (int i = 0; i < kPerThread; ++i) {
                    std::lock_guard lock(decisions);
                    const auto frame = outbox_test_frame(next_decision, 64);
                    ++next_decision;
                    if (outbox->post(frame, FrameClass::Control) != PostResult::Queued) {
                        refused.fetch_add(1);
                    }
                }
            });
        }
        for (auto& p : posters) {
            p.join();
        }
    }
    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
    ASSERT_TRUE(reader.finished_within(kPrompt));

    EXPECT_EQ(refused.load(), 0);
    EXPECT_TRUE(reader.intact());
    const auto tags = reader.tags();
    ASSERT_EQ(tags.size(), static_cast<std::size_t>(kThreads * kPerThread));
    for (std::size_t i = 0; i < tags.size(); ++i) {
        ASSERT_EQ(tags[i], i) << "frame " << i << " arrived out of the order it was decided in";
    }
}

// --- Posting never waits on the peer ----------------------------------------

// With the writer held in send by a peer that never reads, every post still
// returns at once: posting is safe under the coordinator's lock.
TEST(Outbox, PostsReturnAtOnceWhileThePeerReadsNothing) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'never-reads'");
    OutboxWriterThread writer(outbox);

    // Every post runs on a thread of its own, the first included, so an
    // outbox that makes a caller wait on the peer fails the test rather than
    // hanging it.
    constexpr int kPosts = 5000;
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    bool large_queued = false;
    bool held = false;
    Clock::time_point posts_started{};
    Clock::duration worst{};
    int refused = 0;
    std::thread poster([&] {
        large_queued = outbox->post(outbox_test_frame(0, kMoreThanTheBuffersHold),
                                    FrameClass::Bulk) == PostResult::Queued;
        // No chunk for 200 ms: the writer is in send, waiting on the peer.
        held =
            outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 200ms; }, kPrompt);
        posts_started = Clock::now();
        for (int i = 1; i <= kPosts; ++i) {
            const auto frame = outbox_test_frame(static_cast<std::uint64_t>(i), 64);
            const auto started = Clock::now();
            const auto result = outbox->post(frame, FrameClass::Control);
            const auto took = Clock::now() - started;
            worst = std::max(worst, took);
            if (result != PostResult::Queued) {
                ++refused;
            }
        }
        std::lock_guard lock(mu);
        done = true;
        cv.notify_all();
    });
    bool finished = false;
    {
        std::unique_lock lock(mu);
        finished = cv.wait_for(lock, 2 * kPrompt, [&] { return done; });
    }
    if (!finished) {
        outbox_test_hang_up(pair);  // frees a writer that holds the posts back
    }
    poster.join();
    ASSERT_TRUE(finished) << "posting waited on a peer that never reads";
    ASSERT_TRUE(large_queued);
    ASSERT_TRUE(held) << "the writer was never held in send";

    EXPECT_EQ(refused, 0);
    // A post that waited on the peer would wait for ever; this bound only has
    // to be clear of a thread descheduled on a busy machine.
    EXPECT_LT(worst, clink::test_support::scale_slack(Clock::duration{250ms}))
        << "a post took " << std::chrono::duration_cast<std::chrono::milliseconds>(worst).count()
        << " ms with the writer held in send";
    // No chunk went out while the posts ran: they were all made with the
    // writer held in send.
    const auto now = Clock::now();
    EXPECT_GE(outbox->stalled_for(now), now - posts_started);
    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Open);
    EXPECT_TRUE(s.writing);
    EXPECT_EQ(s.queued_control_frames, static_cast<std::size_t>(kPosts));
}

// --- Bounds ---------------------------------------------------------------------

// The post that finds the control frames at their bound is refused and stops
// the outbox, which records why, discards and counts what was queued, closes
// the transport, refuses every later post, and logs once.
TEST(Outbox, AControlFrameAtTheFrameBoundIsRefusedWithItsReason) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    const std::string peer = "worker 'overflow-frames'";
    // No writer runs, so every frame posted stays queued.
    Outbox outbox(pair.local, peer, OutboxLimits{.max_control_frames = 3});
    const auto discards_before =
        outbox_test_discards(FrameClass::Control, OutboxStopReason::Overflow);
    const auto bulk_discards_before =
        outbox_test_discards(FrameClass::Bulk, OutboxStopReason::Overflow);
    const auto lines_before = outbox_test_log_lines_naming(peer);

    for (std::uint64_t i = 0; i < 3; ++i) {
        ASSERT_EQ(outbox.post(outbox_test_frame(i, 64), FrameClass::Control), PostResult::Queued);
    }
    EXPECT_EQ(outbox.post(outbox_test_frame(3, 64), FrameClass::Control), PostResult::Refused);

    const auto s = outbox.snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::Overflow);
    EXPECT_NE(s.stop_detail.find("3 control frames queued"), std::string::npos) << s.stop_detail;
    EXPECT_EQ(s.frames_discarded, 3U);
    EXPECT_EQ(s.queued_control_frames, 0U);
    EXPECT_EQ(
        outbox_test_discards(FrameClass::Control, OutboxStopReason::Overflow) - discards_before,
        3U);
    EXPECT_EQ(
        outbox_test_discards(FrameClass::Bulk, OutboxStopReason::Overflow) - bulk_discards_before,
        0U);

    // Refused from here on, whatever the class, and an abort changes nothing.
    EXPECT_EQ(outbox.post(outbox_test_frame(4, 64), FrameClass::Control), PostResult::Refused);
    EXPECT_EQ(outbox.post(outbox_test_frame(5, 64), FrameClass::Bulk), PostResult::Refused);
    outbox.abort("a later abort");
    EXPECT_EQ(outbox.snapshot().stop_reason, OutboxStopReason::Overflow);
    EXPECT_EQ(outbox_test_log_lines_naming(peer) - lines_before, 1U)
        << "a stop is logged once per connection";

    // The transport is closed: the peer reads the end of the stream.
    EXPECT_TRUE(outbox_test_peer_sees_end(pair)) << "the transport was left open";
}

// A control frame is admitted while the control bytes queued are under their
// bound, so the queue holds at most the bound plus one frame.
TEST(Outbox, AControlFrameAtTheByteBoundIsRefusedWithItsReason) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    Outbox outbox(pair.local, "worker 'overflow-bytes'", OutboxLimits{.max_control_bytes = 1000});
    EXPECT_EQ(outbox.post(outbox_test_frame(0, 600), FrameClass::Control), PostResult::Queued);
    EXPECT_EQ(outbox.post(outbox_test_frame(1, 600), FrameClass::Control), PostResult::Queued)
        << "604 bytes queued is under the bound of 1000";
    EXPECT_EQ(outbox.post(outbox_test_frame(2, 600), FrameClass::Control), PostResult::Refused);
    const auto s = outbox.snapshot();
    EXPECT_EQ(s.stop_reason, OutboxStopReason::Overflow);
    EXPECT_NE(s.stop_detail.find("1208 bytes of control frames queued"), std::string::npos)
        << s.stop_detail;
    EXPECT_EQ(s.frames_discarded, 2U);
}

// Frames carrying plugin bytes have a bound of their own, and each class is
// judged against its own bounds alone: plugin bytes queued never refuse a
// control frame, and control frames at their bound never refuse a frame
// carrying plugin bytes.
TEST(Outbox, BulkFramesAreBoundedApartFromControlFrames) {
    const OutboxLimits limits{.max_control_frames = 2,
                              .max_control_bytes = 1024,
                              .max_bulk_bytes = std::size_t{1} * kMiB};
    {
        auto pair = outbox_test_pair();
        ASSERT_TRUE(pair.local && pair.remote);
        Outbox outbox(pair.local, "worker 'bulk-bound'", limits);
        EXPECT_EQ(outbox.post(outbox_test_frame(0, 600 * 1024), FrameClass::Bulk),
                  PostResult::Queued);
        EXPECT_EQ(outbox.post(outbox_test_frame(1, 600 * 1024), FrameClass::Bulk),
                  PostResult::Queued);
        // 1.2 MiB of plugin bytes queued, far over the control byte bound.
        EXPECT_EQ(outbox.post(outbox_test_frame(2, 64), FrameClass::Control), PostResult::Queued)
            << "plugin bytes counted against the control frames' bound";
        EXPECT_EQ(outbox.post(outbox_test_frame(3, 64), FrameClass::Control), PostResult::Queued);
        // The plugin bytes are at their own bound.
        EXPECT_EQ(outbox.post(outbox_test_frame(4, 64), FrameClass::Bulk), PostResult::Refused);
        const auto s = outbox.snapshot();
        EXPECT_EQ(s.stop_reason, OutboxStopReason::Overflow);
        EXPECT_NE(s.stop_detail.find("plugin bytes"), std::string::npos) << s.stop_detail;
        EXPECT_EQ(s.frames_discarded, 4U);
    }
    {
        auto pair = outbox_test_pair();
        ASSERT_TRUE(pair.local && pair.remote);
        Outbox outbox(pair.local, "worker 'control-bound'", limits);
        EXPECT_EQ(outbox.post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
        EXPECT_EQ(outbox.post(outbox_test_frame(1, 64), FrameClass::Control), PostResult::Queued);
        // The control frames are at their frame bound.
        EXPECT_EQ(outbox.post(outbox_test_frame(2, 600 * 1024), FrameClass::Bulk),
                  PostResult::Queued)
            << "the control frames' bound refused a frame carrying plugin bytes";
        EXPECT_EQ(outbox.post(outbox_test_frame(3, 64), FrameClass::Control), PostResult::Refused);
        const auto s = outbox.snapshot();
        EXPECT_EQ(s.stop_reason, OutboxStopReason::Overflow);
        EXPECT_NE(s.stop_detail.find("2 control frames queued"), std::string::npos)
            << s.stop_detail;
        EXPECT_EQ(s.frames_discarded, 3U);
    }
}

// Several jobs' Deploys posted to one worker at once, each with 40 MiB of
// plugin bytes, as a restart of every job on a re-registered worker posts
// them: with the default bounds none is refused, even with none of them
// written yet, and the heartbeat answer behind them is queued too.
TEST(Outbox, ThreeFortyMebibyteDeploysToOneConnectionAreNotRefused) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    // No writer runs: every Deploy stays queued, the worst case for a burst.
    Outbox outbox(pair.local, "worker 'three-deploys'");
    for (std::uint64_t i = 0; i < 3; ++i) {
        EXPECT_EQ(outbox.post(outbox_test_frame(i, 40 * kMiB), FrameClass::Bulk),
                  PostResult::Queued)
            << "Deploy " << i << " was refused";
    }
    EXPECT_EQ(outbox.post(outbox_test_frame(3, 64), FrameClass::Control), PostResult::Queued);
    const auto s = outbox.snapshot();
    EXPECT_EQ(s.state, OutboxState::Open);
    EXPECT_EQ(s.queued_bulk_frames, 3U);
    EXPECT_EQ(s.queued_bulk_bytes, 3 * (4 + 40 * kMiB));
    EXPECT_EQ(s.queued_control_frames, 1U);
    outbox.abort("the test is over");
}

// The frame the writer has taken never counts against the bounds: a large
// one in flight cannot make a small one overflow, for either class.
TEST(Outbox, TheFrameBeingWrittenDoesNotCountAgainstTheBounds) {
    fault::Registry::instance().reset();
    auto pair = outbox_test_pair(/*small_buffers=*/false);
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local,
                                           "worker 'in-flight'",
                                           OutboxLimits{.max_control_frames = 1,
                                                        .max_control_bytes = 1024,
                                                        .max_bulk_bytes = std::size_t{1} * kMiB});
    OutboxTestReader reader(pair.remote);
    OutboxWriterThread writer(outbox);
    // Hold the writer before it writes the first frame, and again before the
    // third, each with that frame in flight. Armed after the writer starts, so
    // a failed assertion releases it before the writer is joined.
    fault::ScopedFault hold_first{fault::Rule{.point = fault::points::kCoordinatorOutboxBeforeWrite,
                                              .ordinal = 1,
                                              .action = fault::Action::Block}};
    fault::Registry::instance().arm(
        fault::Rule{.point = fault::points::kCoordinatorOutboxBeforeWrite,
                    .ordinal = 3,
                    .action = fault::Action::Block});
    const auto held = [](std::uint64_t n) {
        return outbox_test_await(
            [n] {
                return fault::Registry::instance().hits(
                           fault::points::kCoordinatorOutboxBeforeWrite) >= n;
            },
            kPrompt);
    };

    // A control frame far over the byte bound is admitted to an empty queue,
    // and the writer takes it.
    ASSERT_EQ(outbox->post(outbox_test_frame(0, 64 * 1024), FrameClass::Control),
              PostResult::Queued);
    ASSERT_TRUE(held(1));
    ASSERT_TRUE(outbox->snapshot().writing);
    EXPECT_EQ(outbox->post(outbox_test_frame(1, 64), FrameClass::Control), PostResult::Queued)
        << "the control frame in flight was counted against the control bound";

    fault::Registry::instance().release(fault::points::kCoordinatorOutboxBeforeWrite);
    ASSERT_TRUE(outbox_test_await([&] { return outbox->snapshot().frames_written == 2; }, kPrompt));

    // The same for plugin bytes.
    ASSERT_EQ(outbox->post(outbox_test_frame(2, 2 * kMiB), FrameClass::Bulk), PostResult::Queued);
    ASSERT_TRUE(held(3));
    ASSERT_TRUE(outbox->snapshot().writing);
    EXPECT_EQ(outbox->post(outbox_test_frame(3, 600 * 1024), FrameClass::Bulk), PostResult::Queued)
        << "the bulk frame in flight was counted against the bulk bound";

    fault::Registry::instance().release(fault::points::kCoordinatorOutboxBeforeWrite);
    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
    ASSERT_TRUE(reader.finished_within(kPrompt));
    EXPECT_EQ(reader.tags(), (std::vector<std::uint64_t>{0, 1, 2, 3}));
}

// --- The stall clock ----------------------------------------------------------

// A connection with nothing pending is never stalled, however long it has
// been idle: before anything is posted, and once everything posted is written.
TEST(Outbox, AnIdleConnectionIsNeverStalled) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'idle'");
    OutboxTestReader reader(pair.remote);
    OutboxWriterThread writer(outbox);

    EXPECT_EQ(outbox->stalled_for(Clock::now() + 24h), Clock::duration::zero())
        << "an outbox nothing was ever posted to read as stalled";
    ASSERT_EQ(outbox->post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
    ASSERT_TRUE(outbox_test_await([&] { return outbox->snapshot().frames_written == 1; }, kPrompt));
    EXPECT_EQ(outbox->stalled_for(Clock::now() + 24h), Clock::duration::zero())
        << "an outbox with everything written read as stalled";

    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
}

// The stall clock starts when the outbox goes from nothing pending to
// something pending. Started any earlier, at construction or at the last
// progress of an earlier busy spell, the first post after a quiet spell
// reads as a stall as long as the quiet.
TEST(Outbox, TheStallClockStartsWhenAFrameBecomesPending) {
    constexpr auto kQuiet = 300ms;
    {
        // No writer runs, so the frame stays pending once posted.
        auto pair = outbox_test_pair();
        ASSERT_TRUE(pair.local && pair.remote);
        Outbox outbox(pair.local, "worker 'quiet-since-created'");
        // The quiet spell is what is under test, so it is waited out.
        std::this_thread::sleep_for(kQuiet);
        const auto posted = Clock::now();
        ASSERT_EQ(outbox.post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
        const auto now = Clock::now();
        EXPECT_LE(outbox.stalled_for(now), now - posted)
            << "the time before the first post counted as a stall";
        EXPECT_GT(outbox.stalled_for(now + 1s), Clock::duration::zero())
            << "a pending frame was not judged at all";
        outbox.abort("the test is over");
    }
    {
        fault::Registry::instance().reset();
        auto pair = outbox_test_pair();
        ASSERT_TRUE(pair.local && pair.remote);
        auto outbox = std::make_shared<Outbox>(pair.local, "worker 'quiet-after-busy'");
        OutboxTestReader reader(pair.remote);
        OutboxWriterThread writer(outbox);
        // The writer takes the second frame and is held before writing it.
        // Armed after the writer starts, so it is released before the join.
        fault::ScopedFault hold{fault::Rule{.point = fault::points::kCoordinatorOutboxBeforeWrite,
                                            .ordinal = 2,
                                            .action = fault::Action::Block}};
        ASSERT_EQ(outbox->post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
        ASSERT_TRUE(
            outbox_test_await([&] { return outbox->snapshot().frames_written == 1; }, kPrompt));
        std::this_thread::sleep_for(kQuiet);
        const auto posted = Clock::now();
        ASSERT_EQ(outbox->post(outbox_test_frame(1, 64), FrameClass::Control), PostResult::Queued);
        ASSERT_TRUE(outbox_test_await(
            [] {
                return fault::Registry::instance().hits(
                           fault::points::kCoordinatorOutboxBeforeWrite) >= 2;
            },
            kPrompt));
        const auto now = Clock::now();
        EXPECT_LE(outbox->stalled_for(now), now - posted)
            << "the quiet spell after the last frame counted as a stall";
        fault::Registry::instance().release(fault::points::kCoordinatorOutboxBeforeWrite);
        outbox->close_after_flush(Clock::now() + kPrompt);
        EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
    }
}

// The clock runs while a frame is pending and the peer reads nothing, every
// chunk written restarts it, and it stops once nothing is pending: a large
// frame a peer reads slowly is not a stall.
TEST(Outbox, TheStallClockRunsOnlyWithoutProgressAndEachChunkRestartsIt) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'slow-reader'");
    OutboxWriterThread writer(outbox);
    const std::size_t frame_bytes = 4 + kMoreThanTheBuffersHold;
    ASSERT_EQ(outbox->post(outbox_test_frame(7, kMoreThanTheBuffersHold), FrameClass::Bulk),
              PostResult::Queued);

    // The peer reads nothing: once the buffers are full no chunk goes out,
    // and the clock runs on.
    ASSERT_TRUE(
        outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 300ms; }, kPrompt));
    ASSERT_TRUE(
        outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 600ms; }, kPrompt));

    // The peer reads part of the frame, so more chunks go out: the clock is
    // restarted by them, never left running from the post.
    const auto written_before = outbox->snapshot().bytes_written;
    const auto read_started = Clock::now();
    std::vector<std::byte> buf(std::size_t{2} * kMiB);
    ASSERT_TRUE(pair.remote->recv_all(buf.data(), buf.size()));
    ASSERT_TRUE(outbox_test_await([&] { return outbox->snapshot().bytes_written > written_before; },
                                  kPrompt));
    const auto now = Clock::now();
    EXPECT_LE(outbox->stalled_for(now), now - read_started)
        << "chunks written after the peer read did not restart the stall clock";
    EXPECT_EQ(outbox->snapshot().frames_written, 0U) << "the frame is still being written";

    // The peer stops reading again, and the clock runs again.
    ASSERT_TRUE(
        outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 300ms; }, kPrompt));

    // The peer reads the rest: with nothing pending, the clock stops.
    std::size_t left = frame_bytes - buf.size();
    while (left > 0) {
        const auto n = std::min(left, buf.size());
        ASSERT_TRUE(pair.remote->recv_all(buf.data(), n));
        left -= n;
    }
    ASSERT_TRUE(outbox_test_await([&] { return outbox->snapshot().frames_written == 1; }, kPrompt));
    EXPECT_EQ(outbox->stalled_for(Clock::now() + 24h), Clock::duration::zero());
    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
}

// A caller that finds the process itself was paused restarts the clock: no
// writer could make progress during the pause, so it is not the peer's
// stall. A restart never moves the clock back, and one made with nothing
// pending is not carried over to the next post.
TEST(Outbox, RestartingTheStallClockLeavesOutAPauseOfTheProcess) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    // No writer runs, so a frame posted stays pending.
    Outbox outbox(pair.local, "worker 'paused'");

    outbox.restart_stall_clock(Clock::now() + 1h);
    ASSERT_EQ(outbox.post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
    const auto posted = Clock::now();
    EXPECT_GE(outbox.stalled_for(posted + 2s), Clock::duration{2s})
        << "a restart made with nothing pending held the clock back at the next post";

    // The process stops for an hour with the frame pending, then resumes.
    const auto resumed = posted + 1h;
    EXPECT_GE(outbox.stalled_for(resumed), Clock::duration{1h});
    outbox.restart_stall_clock(resumed);
    EXPECT_EQ(outbox.stalled_for(resumed), Clock::duration::zero());
    EXPECT_EQ(outbox.stalled_for(resumed + 2s), Clock::duration{2s});
    outbox.restart_stall_clock(resumed - 30min);
    EXPECT_EQ(outbox.stalled_for(resumed + 2s), Clock::duration{2s})
        << "a restart at an earlier time moved the clock back";
    outbox.abort("the test is over");
}

// --- Stopping ---------------------------------------------------------------------

// abort wakes a writer blocked in send by closing the transport under it,
// discards and counts the frame in flight and the queue, and refuses posts.
TEST(Outbox, AbortFreesAWriterBlockedInSendOnPlainTcp) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'aborted'");
    OutboxWriterThread writer(outbox);
    ASSERT_EQ(outbox->post(outbox_test_frame(0, kMoreThanTheBuffersHold), FrameClass::Bulk),
              PostResult::Queued);
    for (std::uint64_t i = 1; i <= 3; ++i) {
        ASSERT_EQ(outbox->post(outbox_test_frame(i, 64), FrameClass::Control), PostResult::Queued);
    }
    ASSERT_TRUE(
        outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 200ms; }, kPrompt));
    const auto control_before =
        outbox_test_discards(FrameClass::Control, OutboxStopReason::Aborted);
    const auto bulk_before = outbox_test_discards(FrameClass::Bulk, OutboxStopReason::Aborted);

    outbox->abort("worker lost: the test gave it up");
    const bool returned = writer.returned_within(kWake);
    if (!returned) {
        outbox_test_hang_up(pair);  // so the test ends
    }
    ASSERT_TRUE(returned) << "abort did not wake a writer blocked in send";

    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::Aborted);
    EXPECT_EQ(s.stop_detail, "worker lost: the test gave it up");
    EXPECT_EQ(s.frames_discarded, 4U) << "the frame in flight and the three queued";
    // Counted by class: the frame in flight carried plugin bytes.
    EXPECT_EQ(outbox_test_discards(FrameClass::Control, OutboxStopReason::Aborted) - control_before,
              3U);
    EXPECT_EQ(outbox_test_discards(FrameClass::Bulk, OutboxStopReason::Aborted) - bulk_before, 1U);
    EXPECT_EQ(outbox->post(outbox_test_frame(9, 64), FrameClass::Control), PostResult::Refused);
}

// A transport that fails under the writer stops the outbox with that reason.
TEST(Outbox, AWriteToAPeerThatHasGoneStopsTheOutboxWithItsReason) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "worker 'gone'");
    OutboxWriterThread writer(outbox);
    outbox_test_hang_up(pair);
    // The first send after a reset can still be taken by the kernel, so posts
    // go on until one fails.
    std::uint64_t tag = 0;
    ASSERT_TRUE(outbox_test_await(
        [&] {
            (void)outbox->post(outbox_test_frame(tag++, 64), FrameClass::Control);
            return outbox->snapshot().state == OutboxState::Stopped;
        },
        kPrompt));
    const auto s = outbox->snapshot();
    EXPECT_EQ(s.stop_reason, OutboxStopReason::WriteFailed);
    EXPECT_NE(s.stop_detail.find("the transport failed"), std::string::npos) << s.stop_detail;
    EXPECT_EQ(outbox->post(outbox_test_frame(tag, 64), FrameClass::Control), PostResult::Refused);
    EXPECT_TRUE(writer.returned_within(kPrompt));
}

// close_after_flush refuses new posts, writes everything queued, and closes
// the transport: the peer reads every frame, then the end of the stream.
TEST(Outbox, CloseAfterFlushWritesWhatIsQueuedThenClosesTheTransport) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "client 'flushed'");
    const std::array<std::size_t, 5> sizes{64, 200 * 1024, 64, kMiB, 64};
    for (std::size_t i = 0; i < sizes.size(); ++i) {
        ASSERT_EQ(outbox->post(outbox_test_frame(i, sizes[i]), FrameClass::Control),
                  PostResult::Queued);
    }
    outbox->close_after_flush(Clock::now() + kPrompt);
    EXPECT_EQ(outbox->post(outbox_test_frame(99, 64), FrameClass::Control), PostResult::Refused)
        << "a post after close_after_flush was taken";
    EXPECT_EQ(outbox->snapshot().state, OutboxState::Flushing);

    OutboxTestReader reader(pair.remote);
    OutboxWriterThread writer(outbox);
    EXPECT_TRUE(outbox->wait_closed(Clock::now() + kPrompt));
    ASSERT_TRUE(reader.finished_within(kPrompt)) << "the peer never saw the end of the stream";
    EXPECT_TRUE(reader.intact());
    EXPECT_EQ(reader.tags(), (std::vector<std::uint64_t>{0, 1, 2, 3, 4}));
    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Closed);
    EXPECT_EQ(s.frames_written, 5U);
    EXPECT_EQ(s.frames_discarded, 0U);
    EXPECT_TRUE(writer.returned_within(kPrompt));
}

// A flush whose peer reads nothing ends at its deadline: wait_closed aborts
// the outbox there, which frees the writer held in send, and what was left
// is counted against the deadline.
TEST(Outbox, CloseAfterFlushStopsAtItsDeadlineWhenThePeerReadsNothing) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "client 'never-reads'");
    OutboxWriterThread writer(outbox);
    ASSERT_EQ(outbox->post(outbox_test_frame(0, kMoreThanTheBuffersHold), FrameClass::Bulk),
              PostResult::Queued);
    for (std::uint64_t i = 1; i <= 3; ++i) {
        ASSERT_EQ(outbox->post(outbox_test_frame(i, 64), FrameClass::Control), PostResult::Queued);
    }
    ASSERT_TRUE(
        outbox_test_await([&] { return outbox->stalled_for(Clock::now()) >= 200ms; }, kPrompt));
    const auto control_before =
        outbox_test_discards(FrameClass::Control, OutboxStopReason::FlushDeadline);
    const auto bulk_before =
        outbox_test_discards(FrameClass::Bulk, OutboxStopReason::FlushDeadline);

    const auto deadline = Clock::now() + 300ms;
    outbox->close_after_flush(deadline);
    EXPECT_FALSE(outbox->wait_closed(deadline));
    const auto waited_until = Clock::now();
    EXPECT_GE(waited_until, deadline);
    EXPECT_LT(waited_until - deadline, clink::test_support::scale_slack(Clock::duration{1s}));
    const bool returned = writer.returned_within(kWake);
    if (!returned) {
        outbox_test_hang_up(pair);  // so the test ends
    }
    ASSERT_TRUE(returned) << "the writer was still held in send after the flush deadline";

    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::FlushDeadline);
    EXPECT_EQ(s.frames_discarded, 4U);
    EXPECT_EQ(
        outbox_test_discards(FrameClass::Control, OutboxStopReason::FlushDeadline) - control_before,
        3U);
    EXPECT_EQ(outbox_test_discards(FrameClass::Bulk, OutboxStopReason::FlushDeadline) - bulk_before,
              1U);
}

// A writer still making progress stops at the flush deadline by itself,
// between frames, with nothing written after it: what reaches the peer is a
// prefix of what was queued.
TEST(Outbox, CloseAfterFlushStopsTheWriterAtTheDeadlineBetweenFrames) {
    fault::Registry::instance().reset();
    constexpr auto kPerFrame = 200ms;
    // Each frame is held this long before its first chunk: a slow writer.
    fault::ScopedFault slow{fault::Rule{.point = fault::points::kCoordinatorOutboxBeforeWrite,
                                        .action = fault::Action::Delay,
                                        .arg = kPerFrame.count()}};
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "client 'slow-flush'");
    constexpr std::uint64_t kFrames = 20;
    for (std::uint64_t i = 0; i < kFrames; ++i) {
        ASSERT_EQ(outbox->post(outbox_test_frame(i, 64), FrameClass::Control), PostResult::Queued);
    }
    OutboxTestReader reader(pair.remote);
    const auto started = Clock::now();
    outbox->close_after_flush(started + 500ms);
    OutboxWriterThread writer(outbox);
    // Nothing aborts the outbox: the writer stops by itself.
    ASSERT_TRUE(writer.returned_within(kPrompt));
    EXPECT_LT(Clock::now() - started, kPerFrame * static_cast<int>(kFrames))
        << "every frame was written anyway";

    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::FlushDeadline);
    EXPECT_LT(s.frames_written, kFrames);
    EXPECT_EQ(s.frames_written + s.frames_discarded, kFrames);
    ASSERT_TRUE(reader.finished_within(kPrompt));
    const auto tags = reader.tags();
    ASSERT_EQ(tags.size(), s.frames_written);
    for (std::size_t i = 0; i < tags.size(); ++i) {
        EXPECT_EQ(tags[i], i);
    }
}

// A writer part-way through a frame when the flush deadline passes stops at
// the next chunk, rather than writing the rest of the frame.
TEST(Outbox, CloseAfterFlushStopsTheWriterAtTheDeadlineBetweenChunks) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    auto outbox = std::make_shared<Outbox>(pair.local, "client 'mid-frame'");
    OutboxWriterThread writer(outbox);
    // Many chunks, and far more than the small buffers hold.
    constexpr std::size_t kBody = 16 * Outbox::kWriteChunkBytes;
    ASSERT_EQ(outbox->post(outbox_test_frame(0, kBody), FrameClass::Bulk), PostResult::Queued);
    // Bytes of the frame reached the peer, which reads none of them: the
    // writer is part-way through the frame, past the check it makes before
    // the first chunk, and held in send.
    ASSERT_TRUE(outbox_test_peer_has_bytes(pair));

    outbox->close_after_flush(Clock::now());
    // The peer reads everything from here, so chunks go out again, and the
    // first of them finds the deadline passed. Nothing aborts the outbox.
    OutboxTestReader reader(pair.remote);
    ASSERT_TRUE(writer.returned_within(kPrompt));

    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::FlushDeadline);
    EXPECT_EQ(s.frames_written, 0U) << "the rest of the frame was written after the deadline";
    EXPECT_LT(s.bytes_written, 4 + kBody);
    EXPECT_EQ(s.frames_discarded, 1U);
    ASSERT_TRUE(reader.finished_within(kPrompt));
    EXPECT_TRUE(reader.tags().empty()) << "a frame cut short was read as whole";
}

// With nothing pending, a flush has nothing to wait for: it closes at once,
// with no writer needed, and is not logged as a stop.
TEST(Outbox, CloseAfterFlushWithNothingPendingClosesAtOnce) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    const std::string peer = "client 'nothing-pending'";
    // No writer runs.
    Outbox outbox(pair.local, peer);
    const auto lines_before = outbox_test_log_lines_naming(peer);

    outbox.close_after_flush(Clock::now() + 24h);
    EXPECT_EQ(outbox.snapshot().state, OutboxState::Closed);
    EXPECT_TRUE(outbox.wait_closed(Clock::now()))
        << "a flush with nothing to write waited for a writer";
    EXPECT_TRUE(outbox_test_peer_sees_end(pair)) << "the transport was left open";
    EXPECT_EQ(outbox_test_log_lines_naming(peer) - lines_before, 0U);

    // A writer started now has nothing to do.
    outbox.run_writer();
    const auto s = outbox.snapshot();
    EXPECT_EQ(s.state, OutboxState::Closed);
    EXPECT_EQ(s.frames_discarded, 0U);
}

// --- Releasing the transport ------------------------------------------------------

// An owner that keeps the outbox past its session lets go of the transport
// when the session is over, so the descriptor closes then rather than with
// the outbox. An outbox still open is stopped first, and refuses every later
// post.
TEST(Outbox, ReleasingTheTransportWithNoWriterClosesTheDescriptorAtOnce) {
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    const std::weak_ptr<Connection> transport = pair.local;
    // The outbox holds the only reference to the transport.
    Outbox outbox(std::move(pair.local), "worker 'released'");
    ASSERT_EQ(outbox.post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);

    outbox.release_transport();
    EXPECT_TRUE(transport.expired()) << "the outbox held on to the transport it released";
    const auto s = outbox.snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::Aborted);
    EXPECT_EQ(s.frames_discarded, 1U);
    EXPECT_EQ(outbox.post(outbox_test_frame(1, 64), FrameClass::Control), PostResult::Refused);
    EXPECT_TRUE(outbox_test_peer_sees_end(pair));

    // A second release, or a writer started after the release, changes nothing.
    outbox.release_transport();
    outbox.run_writer();
    EXPECT_EQ(outbox.snapshot().stop_reason, OutboxStopReason::Aborted);
}

// A writer still running keeps the transport until it returns: a release
// never frees the transport under the writer using it, and the descriptor
// closes once the writer has gone.
TEST(Outbox, ReleasingTheTransportUnderAWriterClosesTheDescriptorWhenItReturns) {
    fault::Registry::instance().reset();
    auto pair = outbox_test_pair();
    ASSERT_TRUE(pair.local && pair.remote);
    const std::weak_ptr<Connection> transport = pair.local;
    auto outbox = std::make_shared<Outbox>(std::move(pair.local), "worker 'released-busy'");
    OutboxWriterThread writer(outbox);
    // The writer takes the frame and is held before writing it. Armed after
    // the writer starts, so it is released before the join.
    fault::ScopedFault hold{fault::Rule{.point = fault::points::kCoordinatorOutboxBeforeWrite,
                                        .ordinal = 1,
                                        .action = fault::Action::Block}};
    ASSERT_EQ(outbox->post(outbox_test_frame(0, 64), FrameClass::Control), PostResult::Queued);
    ASSERT_TRUE(outbox_test_await(
        [] {
            return fault::Registry::instance().hits(fault::points::kCoordinatorOutboxBeforeWrite) >=
                   1;
        },
        kPrompt));

    outbox->release_transport();
    EXPECT_EQ(outbox->snapshot().state, OutboxState::Stopped);
    EXPECT_FALSE(transport.expired()) << "the transport was freed under the writer";

    fault::Registry::instance().release(fault::points::kCoordinatorOutboxBeforeWrite);
    ASSERT_TRUE(writer.returned_within(kPrompt));
    EXPECT_TRUE(transport.expired()) << "the transport outlived the release and the writer";
    const auto s = outbox->snapshot();
    EXPECT_EQ(s.frames_written, 0U);
    EXPECT_EQ(s.frames_discarded, 1U);
    EXPECT_TRUE(outbox_test_peer_sees_end(pair));
}
