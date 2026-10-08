#pragma once

// One connection's outbound frames, written by one thread.
//
// An Outbox is a FIFO of encoded frames for one network::Connection. post()
// queues a frame and returns at once. Its lock is the outbox's own and a
// leaf: no other lock is taken, and nothing waits, while it is held. So a
// post is safe under any lock of the caller's, the coordinator's mu_
// included, and never waits on the peer; a post that stops the outbox closes
// the transport and logs after letting go of that lock. The writer,
// run_writer() on a thread the owner creates (the outbox owns no thread), is
// the only thing that writes the socket. It takes frames in the order they
// were posted, so frames posted in a known order, under one lock say, reach
// the peer in that order.
//
// A frame is shared, not copied: a broadcast is encoded once and posted to
// every connection. It must be a whole frame, length prefix included, as
// encode_frame produces it.
//
// The writer sends each frame in chunks of at most kWriteChunkBytes and
// records progress after each one. stalled_for() is how long a frame has been
// pending with no progress: the clock starts when the outbox goes from
// nothing pending to something pending, and restarts at every chunk written
// and at restart_stall_clock(). A connection with nothing pending is never
// stalled, however long it has been idle, and a large frame that a peer
// reads slowly is not stalled while chunks keep going out.
//
// Bounds. Only queued frames count: the frame being written never does, so a
// large Deploy in flight cannot make a heartbeat answer overflow. The caller
// says what each frame is. Control frames (everything without plugin bytes)
// are bounded in frames and in bytes; Bulk frames (those carrying plugin
// bytes) only in bytes, against a separate and larger bound, so a burst of
// Deploys never pushes out the protocol traffic queued beside it, nor the
// other way round. A frame is admitted while its class is under its bounds,
// so a class holds at most its byte bound plus one frame. See OutboxLimits.
//
// Outcomes. post() answers Queued or Refused. Refused means the frame will
// never be written: the outbox is flushing, closed or stopped, or the post
// found a bound reached or could not allocate. The last two stop the outbox.
// Queued means only that: a queued frame is later written, which nothing but
// the peer's reply confirms, or discarded when the outbox stops. Writing is
// prefix-shaped: once a frame is discarded, nothing after it is written.
// Discarded means given up, not undelivered: the frame being written when
// the outbox stops is discarded with the queue, though the transport may
// already have taken some or all of it, and those bytes can still reach the
// peer. Nothing may conclude from a discard that the peer never had a frame.
//
// Stopping. An outbox stops on overflow, on a failed allocation, when the
// transport fails under the writer, when a flush runs out of time, or when
// its owner calls abort() or release_transport(). Stopping records the
// reason, refuses every later post, discards what is queued (and the frame
// in flight), and closes the transport, which wakes a writer blocked in send
// or SSL_write on either transport. The frames discarded are counted by
// class and reason in clink_coordinator_outbound_frames_discarded_total and
// reported in one log line, once per connection.
//
// close_after_flush() refuses new posts and lets the writer write what is
// queued, then close the transport; with nothing pending it closes at once,
// so a flush needs no writer to end. The writer gives up at the deadline
// when it reaches it between chunks; wait_closed() covers a writer blocked
// in send at the deadline by aborting the outbox there. Only a writer writes
// what is queued, so an owner whose writer never ran aborts rather than
// flushes.
//
// Lifetime. Every thread that calls into the outbox holds a shared_ptr to it
// for the whole call, the writer's thread included. A stop goes on using the
// outbox after it shows as stopped (it closes the transport, frees what it
// discarded and logs), so the writer and wait_closed() can return while the
// call that stopped it is still inside. The destructor frees the queue and
// lets go of the transport, nothing else; it joins nothing and counts
// nothing, so an owner stops the outbox (abort, or a flush) before letting
// go of it, and every frame queued is then written or counted. An owner that
// keeps the outbox as long as its session record calls release_transport()
// once the session is over, so that the record does not hold the descriptor
// open.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/frame_io.hpp"
#include "clink/runtime/network/connection.hpp"

namespace clink::cluster {

// What a frame carries, for the outbox's bounds.
enum class FrameClass : std::uint8_t {
    // Every frame without plugin bytes: acks, barriers, commits, cancels, peer
    // updates, heartbeat answers, and a Deploy that refers to its plugins by
    // hash only.
    Control,
    // A frame carrying plugin bytes: a Deploy that ships modules the peer has
    // not been sent on this connection.
    Bulk,
};

enum class PostResult : std::uint8_t {
    Queued,   // in the queue: written later, unless the outbox stops first
    Refused,  // not queued, and never written
};

enum class OutboxState : std::uint8_t {
    Open,      // takes posts and writes them
    Flushing,  // close_after_flush: refuses posts, writes what is queued
    Closed,    // everything queued was written, and the transport closed
    Stopped,   // gave up for the stop reason; the queue was discarded and the transport closed
};

// Why an outbox stopped. Each value but None is also the `reason` tag of the
// discard counter (to_string).
enum class OutboxStopReason : std::uint8_t {
    None,           // has not stopped
    Overflow,       // a post found its class at a bound
    Allocation,     // queueing a frame could not allocate
    WriteFailed,    // the transport failed under the writer: the peer went away
    FlushDeadline,  // close_after_flush's deadline passed with frames unwritten
    Aborted,        // the owner gave the connection up
};

[[nodiscard]] std::string_view to_string(OutboxStopReason reason) noexcept;
// The `class` tag of the discard counter: "control" or "bulk".
[[nodiscard]] std::string_view to_string(FrameClass cls) noexcept;

// The outbox's bounds. A post is refused, and the outbox stopped, when the
// frame's class is at a bound; zero refuses every frame of that class.
struct OutboxLimits {
    // Control frames are a few hundred bytes each and every one of them is
    // protocol state, so they are bounded in number as well as in bytes. The
    // defaults match the inbound bound on a worker's dispatch backlog: well
    // above anything a peer that reads at all lets build up.
    std::size_t max_control_frames{10000};
    std::size_t max_control_bytes{std::size_t{64} * 1024 * 1024};
    // Plugin bytes. A Deploy carries every module of its job that the peer
    // has not been sent, up to kMaxFrameBytes, and one decision can post one
    // Deploy per job hosted on a worker (a restart, a recovery): a burst of
    // several large frames is the normal case for this class, and a slow
    // reader is the stall clock's to judge, not a bound's. Four of the
    // largest frames allowed, so a burst of four maximum-size Deploys, or of
    // twenty-six of 40 MiB, is admitted even when the writer has taken none
    // of it. The bound is a backstop against memory growing without limit:
    // a module's bytes go to a connection once, and later Deploys on it
    // refer to the module by hash.
    std::size_t max_bulk_bytes{4 * kMaxFrameBytes};
};

class Outbox {
public:
    using Clock = std::chrono::steady_clock;
    using Frame = std::shared_ptr<const std::vector<std::byte>>;

    // The most the writer hands the transport at once. Progress is recorded
    // between chunks, and the stop checks and the flush deadline are looked
    // at there too.
    static constexpr std::size_t kWriteChunkBytes = std::size_t{64} * 1024;

    // A consistent view, for diagnostics and tests.
    struct Snapshot {
        OutboxState state{OutboxState::Open};
        OutboxStopReason stop_reason{OutboxStopReason::None};
        std::string stop_detail;
        std::size_t queued_control_frames{0};
        std::size_t queued_control_bytes{0};
        std::size_t queued_bulk_frames{0};
        std::size_t queued_bulk_bytes{0};
        // A frame taken off the queue and not yet all written.
        bool writing{false};
        std::uint64_t frames_written{0};
        std::uint64_t bytes_written{0};
        std::uint64_t frames_discarded{0};
    };

    // `conn` must not be null. `peer` names the connection in the log line a
    // stop writes ("worker 'w1'", say).
    Outbox(std::shared_ptr<network::Connection> conn, std::string peer, OutboxLimits limits = {});
    ~Outbox();

    Outbox(const Outbox&) = delete;
    Outbox& operator=(const Outbox&) = delete;
    Outbox(Outbox&&) = delete;
    Outbox& operator=(Outbox&&) = delete;

    // Queue `frame`, or refuse it. Never blocks on the peer. A post that finds
    // `cls` at a bound, or cannot allocate, stops the outbox; the caller
    // learns only Refused, and stop_reason in a snapshot says why.
    PostResult post(Frame frame, FrameClass cls) noexcept;

    // The writer. Writes queued frames in order until the outbox is closed
    // or stopped, then returns; a second call returns at once. Run it on a
    // thread of the owner's, which holds a shared_ptr to the outbox. It holds
    // a reference of its own to the transport until it returns.
    void run_writer() noexcept;

    // How long the frames pending have gone without progress at `now`: zero
    // with nothing pending, and once the outbox has closed or stopped.
    [[nodiscard]] Clock::duration stalled_for(Clock::time_point now) const noexcept;

    // Restart the stall clock at `now`, never moving it back. For a caller
    // that finds the process itself was paused, a watchdog that woke far
    // later than it asked to: no writer could make progress then, so the
    // pause is not the peer's stall.
    void restart_stall_clock(Clock::time_point now) noexcept;

    // Refuse every post from here on, let the writer write what is queued,
    // then close the transport; with nothing pending, close it at once. A
    // later call can bring the deadline forward, never back. No effect once
    // closed or stopped.
    void close_after_flush(Clock::time_point deadline) noexcept;

    // Wait until the outbox has closed or stopped, or until `deadline`. At
    // the deadline it is aborted with reason FlushDeadline, which wakes a
    // writer blocked in send. True when it closed with everything written.
    // Called after close_after_flush, with the same deadline.
    bool wait_closed(Clock::time_point deadline) noexcept;

    // Stop the outbox: discard what is queued and close the transport, which
    // wakes a writer blocked in send. `why` is recorded and logged. No effect
    // once closed or stopped.
    void abort(std::string_view why) noexcept;

    // Let go of the transport, stopping the outbox first (reason Aborted)
    // unless it has closed or stopped. A writer still running keeps its own
    // reference until it returns, and the descriptor closes when the last
    // reference goes. For an owner that keeps the outbox past its session:
    // called once the session's reader and writer have finished.
    void release_transport() noexcept;

    [[nodiscard]] Snapshot snapshot() const;
    [[nodiscard]] const std::string& peer() const noexcept { return peer_; }

private:
    struct Entry {
        Frame frame;
        std::size_t bytes{0};
        FrameClass cls{FrameClass::Control};
    };

    // What a stop discarded, by class.
    struct Discards {
        std::uint64_t control{0};
        std::uint64_t bulk{0};
        [[nodiscard]] std::uint64_t total() const noexcept { return control + bulk; }
    };

    void write_loop_(network::Connection& conn);
    // Writes one frame. False once the outbox has stopped, whoever stopped it.
    bool write_frame_(network::Connection& conn, const std::vector<std::byte>& frame);
    // Called with `lock` held on mu_, and returns with it released: moves to
    // Stopped unless already closed or stopped, then discards the queue,
    // closes the transport, wakes every waiter, and counts and logs the
    // discards.
    void stop_(std::unique_lock<std::mutex>& lock,
               OutboxStopReason reason,
               std::string detail) noexcept;
    static void close_transport_(network::Connection* conn) noexcept;
    void report_stop_(OutboxStopReason reason, Discards discards) const noexcept;
    [[nodiscard]] bool finished_locked_() const noexcept {
        return state_ == OutboxState::Closed || state_ == OutboxState::Stopped;
    }

    const std::string peer_;
    const OutboxLimits limits_;

    mutable std::mutex mu_;
    // Null once release_transport() has let go of it. Read and reset only
    // under mu_; the writer and a stop each take a reference of their own.
    std::shared_ptr<network::Connection> conn_;
    // The writer waits here for a frame or a change of state.
    std::condition_variable writer_cv_;
    // wait_closed waits here for the outbox to close or stop.
    std::condition_variable finished_cv_;
    std::deque<Entry> queue_;
    // What a stop discards, freed off the lock. A member, built with the
    // outbox, because a deque can allocate when it is constructed, and the
    // stop that follows a failed allocation must not.
    std::deque<Entry> graveyard_;
    std::size_t control_frames_{0};
    std::size_t control_bytes_{0};
    std::size_t bulk_frames_{0};
    std::size_t bulk_bytes_{0};
    bool writing_{false};
    // The class of the frame being written, while writing_.
    FrameClass writing_cls_{FrameClass::Control};
    bool writer_claimed_{false};
    OutboxState state_{OutboxState::Open};
    OutboxStopReason stop_reason_{OutboxStopReason::None};
    // Written once, by the transition to Stopped, and never again.
    std::string stop_detail_;
    Clock::time_point flush_deadline_{Clock::time_point::max()};
    // The last progress: a chunk written, or the outbox going from nothing
    // pending to something pending.
    Clock::time_point progress_at_{};
    std::uint64_t frames_written_{0};
    std::uint64_t bytes_written_{0};
    std::uint64_t frames_discarded_{0};
};

}  // namespace clink::cluster
