#include "outbox.hpp"

#include <algorithm>
#include <exception>
#include <initializer_list>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::cluster {

namespace {

// The parts joined, or an empty string when that cannot allocate: the detail
// of a stop is built on paths that must not throw.
std::string joined(std::initializer_list<std::string_view> parts) noexcept {
    try {
        std::string out;
        for (const auto part : parts) {
            out += part;
        }
        return out;
    } catch (...) {
        return {};
    }
}

std::string number(std::uint64_t n) noexcept {
    try {
        return std::to_string(n);
    } catch (...) {
        return {};
    }
}

std::string overflow_detail(bool bulk,
                            const OutboxLimits& limits,
                            std::size_t frames,
                            std::size_t bytes) noexcept {
    if (bulk) {
        return joined({"a frame carrying plugin bytes found ",
                       number(bytes),
                       " bytes of them queued, against a bound of ",
                       number(limits.max_bulk_bytes)});
    }
    if (frames >= limits.max_control_frames) {
        return joined({"a control frame found ",
                       number(frames),
                       " control frames queued, against a bound of ",
                       number(limits.max_control_frames)});
    }
    return joined({"a control frame found ",
                   number(bytes),
                   " bytes of control frames queued, against a bound of ",
                   number(limits.max_control_bytes)});
}

}  // namespace

std::string_view to_string(OutboxStopReason reason) noexcept {
    switch (reason) {
        case OutboxStopReason::None:
            return "none";
        case OutboxStopReason::Overflow:
            return "overflow";
        case OutboxStopReason::Allocation:
            return "allocation";
        case OutboxStopReason::WriteFailed:
            return "write_failed";
        case OutboxStopReason::FlushDeadline:
            return "flush_deadline";
        case OutboxStopReason::Aborted:
            return "aborted";
    }
    return "unknown";
}

std::string_view to_string(FrameClass cls) noexcept {
    switch (cls) {
        case FrameClass::Control:
            return "control";
        case FrameClass::Bulk:
            return "bulk";
    }
    return "unknown";
}

Outbox::Outbox(std::shared_ptr<network::Connection> conn, std::string peer, OutboxLimits limits)
    : peer_(std::move(peer)), limits_(limits), conn_(std::move(conn)) {}

Outbox::~Outbox() = default;

PostResult Outbox::post(Frame frame, FrameClass cls) noexcept {
    if (!frame) {
        return PostResult::Refused;
    }
    const std::size_t bytes = frame->size();
    const bool bulk = cls == FrameClass::Bulk;
    std::unique_lock lock(mu_);
    if (state_ != OutboxState::Open) {
        return PostResult::Refused;
    }
    // Only queued frames count: the frame the writer has taken is no longer
    // here, so a large one in flight cannot make a small one overflow.
    const bool at_bound = bulk ? bulk_bytes_ >= limits_.max_bulk_bytes
                               : (control_frames_ >= limits_.max_control_frames ||
                                  control_bytes_ >= limits_.max_control_bytes);
    if (at_bound) {
        auto detail = overflow_detail(bulk,
                                      limits_,
                                      bulk ? bulk_frames_ : control_frames_,
                                      bulk ? bulk_bytes_ : control_bytes_);
        stop_(lock, OutboxStopReason::Overflow, std::move(detail));
        return PostResult::Refused;
    }
    const bool was_idle = queue_.empty() && !writing_;
    try {
        queue_.push_back(Entry{std::move(frame), bytes, cls});
    } catch (...) {
        stop_(lock,
              OutboxStopReason::Allocation,
              joined({"queueing a frame of ", number(bytes), " bytes could not allocate"}));
        return PostResult::Refused;
    }
    if (bulk) {
        ++bulk_frames_;
        bulk_bytes_ += bytes;
    } else {
        ++control_frames_;
        control_bytes_ += bytes;
    }
    if (was_idle) {
        // The stall clock starts here. Left at the last progress of an earlier
        // busy spell, a frame posted after a quiet hour would read as stalled
        // for an hour.
        progress_at_ = Clock::now();
    }
    lock.unlock();
    writer_cv_.notify_one();
    return PostResult::Queued;
}

void Outbox::run_writer() noexcept {
    std::shared_ptr<network::Connection> conn;
    {
        std::lock_guard lock(mu_);
        if (writer_claimed_) {
            return;
        }
        writer_claimed_ = true;
        // The writer's own reference: release_transport() can let go of the
        // outbox's while the writer is still on its way out.
        conn = conn_;
    }
    try {
        if (!conn) {
            // Released before the writer ran, which stopped the outbox then,
            // or never given a transport.
            std::unique_lock lock(mu_);
            stop_(lock, OutboxStopReason::WriteFailed, joined({"the outbox has no transport"}));
            return;
        }
        write_loop_(*conn);
    } catch (const std::exception& e) {
        // An injected throw, or an allocation in the fault registry. The frame
        // in flight is lost either way, so the outbox stops as for a transport
        // failure rather than the exception leaving the thread.
        std::unique_lock lock(mu_);
        stop_(lock, OutboxStopReason::WriteFailed, joined({"the writer failed: ", e.what()}));
    } catch (...) {
        std::unique_lock lock(mu_);
        stop_(lock, OutboxStopReason::WriteFailed, joined({"the writer failed"}));
    }
}

void Outbox::write_loop_(network::Connection& conn) {
    while (true) {
        // Declared in the loop, so the frame is freed after each write, off
        // the lock.
        Entry entry;
        {
            std::unique_lock lock(mu_);
            writer_cv_.wait(lock,
                            [this] { return state_ != OutboxState::Open || !queue_.empty(); });
            if (finished_locked_()) {
                return;
            }
            if (queue_.empty()) {
                // Flushing, and everything queued has been written.
                state_ = OutboxState::Closed;
                lock.unlock();
                close_transport_(&conn);
                finished_cv_.notify_all();
                return;
            }
            entry = std::move(queue_.front());
            queue_.pop_front();
            if (entry.cls == FrameClass::Bulk) {
                --bulk_frames_;
                bulk_bytes_ -= entry.bytes;
            } else {
                --control_frames_;
                control_bytes_ -= entry.bytes;
            }
            writing_ = true;
            writing_cls_ = entry.cls;
        }
        const auto injected = CLINK_FAULT_POINT(fault::points::kCoordinatorOutboxBeforeWrite);
        if (injected.is_error()) {
            std::unique_lock lock(mu_);
            stop_(lock,
                  OutboxStopReason::WriteFailed,
                  joined({"an injected fault failed the write"}));
            return;
        }
        if (!write_frame_(conn, *entry.frame)) {
            return;
        }
    }
}

bool Outbox::write_frame_(network::Connection& conn, const std::vector<std::byte>& frame) {
    const std::size_t total = frame.size();
    std::size_t written = 0;
    {
        std::unique_lock lock(mu_);
        // Stopped while this frame was being taken: it was counted with the
        // rest of the queue.
        if (state_ == OutboxState::Stopped) {
            return false;
        }
        if (total == 0) {
            writing_ = false;
            ++frames_written_;
            return true;
        }
        if (state_ == OutboxState::Flushing && Clock::now() >= flush_deadline_) {
            stop_(lock,
                  OutboxStopReason::FlushDeadline,
                  joined({"the flush deadline passed with frames unwritten"}));
            return false;
        }
    }
    while (true) {
        const std::size_t chunk = std::min(kWriteChunkBytes, total - written);
        // Never under mu_: a post must not wait on the peer.
        const bool sent = conn.send_all(frame.data() + written, chunk);
        std::unique_lock lock(mu_);
        if (state_ == OutboxState::Stopped) {
            // Stopped while the chunk was going out (an abort's close is what
            // woke a send that failed): the frame was counted then.
            return false;
        }
        if (!sent) {
            stop_(lock,
                  OutboxStopReason::WriteFailed,
                  joined({"the transport failed with ",
                          number(written),
                          " of a frame's ",
                          number(total),
                          " bytes written"}));
            return false;
        }
        written += chunk;
        bytes_written_ += chunk;
        progress_at_ = Clock::now();
        if (written == total) {
            writing_ = false;
            ++frames_written_;
            return true;
        }
        if (state_ == OutboxState::Flushing && progress_at_ >= flush_deadline_) {
            stop_(lock,
                  OutboxStopReason::FlushDeadline,
                  joined({"the flush deadline passed with frames unwritten"}));
            return false;
        }
    }
}

Outbox::Clock::duration Outbox::stalled_for(Clock::time_point now) const noexcept {
    std::lock_guard lock(mu_);
    if (finished_locked_() || (queue_.empty() && !writing_)) {
        return Clock::duration::zero();
    }
    return now > progress_at_ ? now - progress_at_ : Clock::duration::zero();
}

void Outbox::restart_stall_clock(Clock::time_point now) noexcept {
    std::lock_guard lock(mu_);
    // With nothing pending this changes nothing that is read: the next post
    // that makes a frame pending starts the clock afresh.
    progress_at_ = std::max(progress_at_, now);
}

void Outbox::close_after_flush(Clock::time_point deadline) noexcept {
    bool closed_here = false;
    std::shared_ptr<network::Connection> conn;
    {
        std::lock_guard lock(mu_);
        if (state_ == OutboxState::Open) {
            state_ = OutboxState::Flushing;
            flush_deadline_ = deadline;
        } else if (state_ == OutboxState::Flushing) {
            flush_deadline_ = std::min(flush_deadline_, deadline);
        } else {
            return;
        }
        if (queue_.empty() && !writing_) {
            // Nothing to flush, so nothing to wait for: closed here rather
            // than by the writer, which an owner may never have started.
            state_ = OutboxState::Closed;
            closed_here = true;
            // A reference of its own, as a stop takes one.
            conn = conn_;
        }
    }
    writer_cv_.notify_all();
    if (closed_here) {
        close_transport_(conn.get());
        finished_cv_.notify_all();
    }
}

bool Outbox::wait_closed(Clock::time_point deadline) noexcept {
    std::unique_lock lock(mu_);
    const auto finished = [this] { return finished_locked_(); };
    if (deadline == Clock::time_point::max()) {
        finished_cv_.wait(lock, finished);
    } else if (!finished_cv_.wait_until(lock, deadline, finished)) {
        stop_(lock,
              OutboxStopReason::FlushDeadline,
              joined({"the flush deadline passed with frames unwritten"}));
        return false;
    }
    return state_ == OutboxState::Closed;
}

void Outbox::abort(std::string_view why) noexcept {
    auto detail = joined({why});
    std::unique_lock lock(mu_);
    stop_(lock, OutboxStopReason::Aborted, std::move(detail));
}

void Outbox::release_transport() noexcept {
    // Declared before the lock, so it is dropped after the lock is: as the
    // last reference, it closes the descriptor, and never under mu_.
    std::shared_ptr<network::Connection> released;
    std::unique_lock lock(mu_);
    if (!finished_locked_()) {
        stop_(lock, OutboxStopReason::Aborted, joined({"the transport was released"}));
        lock.lock();
    }
    released = std::move(conn_);
}

Outbox::Snapshot Outbox::snapshot() const {
    std::lock_guard lock(mu_);
    Snapshot s;
    s.state = state_;
    s.stop_reason = stop_reason_;
    s.stop_detail = stop_detail_;
    s.queued_control_frames = control_frames_;
    s.queued_control_bytes = control_bytes_;
    s.queued_bulk_frames = bulk_frames_;
    s.queued_bulk_bytes = bulk_bytes_;
    s.writing = writing_;
    s.frames_written = frames_written_;
    s.bytes_written = bytes_written_;
    s.frames_discarded = frames_discarded_;
    return s;
}

void Outbox::stop_(std::unique_lock<std::mutex>& lock,
                   OutboxStopReason reason,
                   std::string detail) noexcept {
    if (finished_locked_()) {
        lock.unlock();
        return;
    }
    state_ = OutboxState::Stopped;
    stop_reason_ = reason;
    stop_detail_ = std::move(detail);
    // The frame in flight is given up too: the writer sees the stop at its
    // next look and writes nothing more. Given up, not undelivered: the
    // transport may already have taken some or all of it.
    const Discards discards{
        control_frames_ + (writing_ && writing_cls_ == FrameClass::Control ? 1U : 0U),
        bulk_frames_ + (writing_ && writing_cls_ == FrameClass::Bulk ? 1U : 0U)};
    frames_discarded_ += discards.total();
    // A reference of its own, taken under the lock, since release_transport()
    // can let go of the outbox's once the lock is released. When that happens,
    // dropping this one closes the descriptor here, which waits on nothing.
    const auto conn = conn_;
    writing_ = false;
    control_frames_ = 0;
    control_bytes_ = 0;
    bulk_frames_ = 0;
    bulk_bytes_ = 0;
    // Swapped out rather than cleared, so the frames are freed off the lock.
    // Only the one stop gets this far, so nothing else touches graveyard_.
    graveyard_.swap(queue_);
    lock.unlock();
    writer_cv_.notify_all();
    // Wakes a writer blocked in send or SSL_write: both transports shut the
    // socket down both ways, without taking their send lock.
    close_transport_(conn.get());
    finished_cv_.notify_all();
    graveyard_.clear();
    report_stop_(reason, discards);
}

void Outbox::close_transport_(network::Connection* conn) noexcept {
    if (conn == nullptr) {
        return;
    }
    try {
        conn->close();
    } catch (...) {
        // close() is a wake that does not throw on either transport; nothing
        // more could be done here if one did.
    }
}

void Outbox::report_stop_(OutboxStopReason reason, Discards discards) const noexcept {
    try {
        if (discards.control > 0) {
            metrics::orch::outbound_frames_discarded(
                to_string(FrameClass::Control), to_string(reason), discards.control);
        }
        if (discards.bulk > 0) {
            metrics::orch::outbound_frames_discarded(
                to_string(FrameClass::Bulk), to_string(reason), discards.bulk);
        }
        const std::uint64_t discarded = discards.total();
        // An owner that gives a connection up with nothing pending logs its
        // own reason; an outbox that failed by itself is always worth a line.
        if (discarded == 0 && reason == OutboxStopReason::Aborted) {
            return;
        }
        std::string with_plugins;
        if (discards.bulk > 0) {
            with_plugins = " (" + std::to_string(discards.bulk) + " carrying plugin bytes)";
        }
        // stop_detail_ was written by the transition this follows and is
        // never written again, so it is read here without the lock.
        log::warn("coordinator.outbox",
                  peer_ + ": stopped sending (" + std::string{to_string(reason)} + "), " +
                      std::to_string(discarded) +
                      (discarded == 1 ? " frame discarded" : " frames discarded") + with_plugins +
                      ": " + stop_detail_);
    } catch (...) {
        // Only the count or the log line was lost.
    }
}

}  // namespace clink::cluster
