// Pulsar sink. See pulsar_sink.hpp.

#include "clink/pulsar/pulsar_sink.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "fault_points.hpp"

namespace clink::pulsar {

namespace {

constexpr const char* kLabel = "pulsar";
// The longest a wait sleeps before it looks at the task's cancel again.
constexpr std::chrono::milliseconds kSlice{100};
// How long past send_timeout a wait gives the client to report its timeouts.
constexpr std::chrono::milliseconds kGrace{2000};

PulsarSink::Options checked(PulsarSink::Options o) {
    if (o.topic.empty()) {
        throw std::runtime_error(o.name + ": 'topic' is required");
    }
    if (o.send_timeout.count() <= 0) {
        // The client reads 0 as no timeout at all, and a barrier would then wait
        // for ever on a message the broker never answers.
        throw std::runtime_error(o.name + ": the send timeout must be positive");
    }
    if (o.max_pending < 1) {
        o.max_pending = 1;
    }
    if (o.max_pending_bytes == 0) {
        o.max_pending_bytes = PulsarSink::kDefaultMaxPendingBytes;
    }
    if (!o.publisher_factory) {
        o.publisher_factory = [](const PulsarPublisherSettings& s) {
            return connect_pulsar_publisher(s);
        };
    }
    return o;
}

}  // namespace

struct PulsarSink::Impl {
    explicit Impl(Options o) : opts(checked(std::move(o))) {}

    Options opts;
    std::unique_ptr<PulsarPublisher> publisher;
    RuntimeContext* rt{nullptr};
    std::string failure;
    bool open{false};
    std::uint64_t published{0};        // since open()
    std::uint64_t published_bytes{0};  // since open()
    // The answers when the last complete flush ended, and what was published
    // since.
    PulsarAnswers base;
    std::uint64_t interval_published{0};

    [[nodiscard]] std::uint64_t unanswered(const PulsarAnswers& a) const {
        return published - std::min(published, a.stored + a.failed);
    }
    [[nodiscard]] std::uint64_t unanswered_bytes(const PulsarAnswers& a) const {
        return published_bytes - std::min(published_bytes, a.answered_bytes);
    }

    [[nodiscard]] std::string tally(const PulsarAnswers& a) const {
        std::string s = "of the " + std::to_string(interval_published) +
                        " messages published since the last complete flush, " +
                        std::to_string(a.stored - base.stored) + " were persisted";
        if (a.failed > base.failed) {
            // A send the client failed on its timeout was on the wire, and the
            // broker can still have persisted it.
            s += ", " + std::to_string(a.failed - base.failed) +
                 " failed (one failed on the client's send timeout may still be on the topic)";
        }
        return s + ", and the other " + std::to_string(unanswered(a)) +
               " are unanswered and may or may not be on the topic. The job restarts from its "
               "last checkpoint and publishes its records again";
    }

    void require_not_failed(const std::string& what) const {
        if (!failure.empty()) {
            throw std::runtime_error(opts.name + ": " + what +
                                     " refused: the sink failed earlier (" + failure +
                                     ") and has not been opened again, so nothing more is "
                                     "published");
        }
    }
    void require_usable(const std::string& what) const {
        require_not_failed(what);
        if (!open) {
            throw std::runtime_error(opts.name + ": " + what + " before open()");
        }
    }

    [[noreturn]] void fail(const std::string& why) {
        failure = why;
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(opts.name + ": " + why);
    }

    [[nodiscard]] bool cancel_requested() const noexcept {
        return rt != nullptr && rt->cancel_requested();
    }

    // Waits for answers until `done(answers)`, in slices, so a cancel ends the
    // wait at once. The client fails each message after send_timeout, so a wait
    // longer than that, and a little grace, means the client has stopped
    // answering, and the sink fails. `what` names the wait.
    PulsarAnswers wait_until(const std::function<bool(const PulsarAnswers&)>& done,
                             const std::string& what) {
        const auto deadline = std::chrono::steady_clock::now() + opts.send_timeout + kGrace;
        PulsarAnswers a = publisher->answers();
        while (!done(a)) {
            if (cancel_requested()) {
                fail("the task was cancelled while " + what + "; " + tally(a));
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                fail("the client had not answered every message within the send timeout (" +
                     std::to_string(opts.send_timeout.count()) + " ms) while " + what + "; " +
                     tally(a));
            }
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            a = publisher->wait(a.stored + a.failed,
                                std::min(kSlice, left + std::chrono::milliseconds{1}));
        }
        return a;
    }

    void close_publisher() noexcept {
        if (publisher) {
            publisher->close();
            publisher.reset();
        }
        open = false;
    }
};

PulsarSink::PulsarSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {}

PulsarSink::~PulsarSink() {
    impl_->close_publisher();
}

void PulsarSink::open() {
    auto& m = *impl_;
    m.close_publisher();
    m.failure.clear();
    m.published = m.published_bytes = m.interval_published = 0;
    m.base = PulsarAnswers{};
    m.rt = runtime();
    PulsarPublisherSettings s;
    s.conn = m.opts.conn;
    s.topic = m.opts.topic;
    s.batching = m.opts.batching;
    s.send_timeout = m.opts.send_timeout;
    // Above the sink's own window, so the client's queue is never full.
    s.max_pending = m.opts.max_pending * 2;
    m.publisher = m.opts.publisher_factory(s);
    m.base = m.publisher->answers();
    m.open = true;
}

void PulsarSink::on_data(const Batch<std::string>& batch) {
    auto& m = *impl_;
    m.require_usable("a record");
    if (batch.empty()) {
        return;
    }
    if (m.cancel_requested()) {
        m.fail("cancelled before publishing to '" + m.opts.topic + "'");
    }
    std::size_t bytes = 0;
    std::size_t count = 0;
    const auto max_pending = static_cast<std::uint64_t>(m.opts.max_pending);
    const auto max_bytes = static_cast<std::uint64_t>(m.opts.max_pending_bytes);
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        const auto now = m.publisher->answers();
        if (m.unanswered(now) >= max_pending || m.unanswered_bytes(now) >= max_bytes) {
            // What the producer batches is sent only on its delay or a flush.
            m.publisher->push();
            (void)m.wait_until(
                [&](const PulsarAnswers& a) {
                    return m.unanswered(a) < max_pending && m.unanswered_bytes(a) < max_bytes;
                },
                "waiting with max_pending (" + std::to_string(m.opts.max_pending) +
                    ") or max_pending_bytes (" + std::to_string(m.opts.max_pending_bytes) +
                    ") reached");
        }
        try {
            m.publisher->publish(payload);
        } catch (const std::exception& e) {
            ++m.published;  // it may have reached the broker
            ++m.interval_published;
            m.fail(std::string{"publishing to '"} + m.opts.topic + "' failed (" + e.what() + "); " +
                   m.tally(m.publisher->answers()));
        }
        ++m.published;
        m.published_bytes += payload.size();
        ++m.interval_published;
        bytes += payload.size();
        ++count;
    }
    clink::metrics::connector::records_out_inc(kLabel, count);
    clink::metrics::connector::bytes_out_inc(kLabel, bytes);
}

void PulsarSink::on_barrier(CheckpointBarrier /*b*/) {
    auto& m = *impl_;
    m.require_usable("a checkpoint barrier");
    const bool publishes = m.interval_published > 0;
    flush();
    if (publishes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterAck);
    }
}

void PulsarSink::flush() {
    auto& m = *impl_;
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    m.require_not_failed("a flush");
    if (!m.open || m.interval_published == 0) {
        return;
    }
    m.publisher->push();
    (void)CLINK_FAULT_POINT(points::kSinkBeforeAck);
    const auto t0 = std::chrono::steady_clock::now();
    const auto a = m.wait_until([&m](const PulsarAnswers& x) { return m.unanswered(x) == 0; },
                                "waiting for a flush's answers");
    if (a.failed > m.base.failed) {
        m.fail("the client failed " + std::to_string(a.failed - m.base.failed) + " send(s) to '" +
               m.opts.topic + "' (first: " + a.first_failure + "); " + m.tally(a));
    }
    m.base = a;
    m.interval_published = 0;
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void PulsarSink::close() {
    // Unanswered messages are not waited for here: a clean end of input flushes
    // before close(), and the client's close fails what is left.
    impl_->close_publisher();
}

std::string PulsarSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::pulsar
