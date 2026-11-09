// NATS JetStream sink. See nats_sink.hpp.

#include "clink/nats/nats_sink.hpp"

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

namespace clink::nats {

namespace {

constexpr const char* kLabel = "nats";
// The longest a wait sleeps before it looks at the task's cancel again.
constexpr std::chrono::milliseconds kSlice{100};

NatsSink::Options checked(NatsSink::Options o) {
    if (o.subject.empty()) {
        throw std::runtime_error(o.name + ": 'subject' is required");
    }
    if (o.publish_timeout.count() <= 0) {
        throw std::runtime_error(o.name + ": the publish timeout must be positive");
    }
    if (o.max_pending < 1) {
        o.max_pending = 1;
    }
    if (o.max_pending_bytes == 0) {
        o.max_pending_bytes = NatsSink::kDefaultMaxPendingBytes;
    }
    if (!o.publisher_factory) {
        o.publisher_factory = [](const NatsPublisherSettings& s) {
            return connect_nats_publisher(s);
        };
    }
    return o;
}

}  // namespace

struct NatsSink::Impl {
    explicit Impl(Options o) : opts(checked(std::move(o))) {}

    Options opts;
    std::unique_ptr<NatsPublisher> publisher;
    RuntimeContext* rt{nullptr};
    std::string failure;
    bool open{false};
    std::uint64_t published{0};        // since open()
    std::uint64_t published_bytes{0};  // since open()
    // The answers when the last complete flush ended, and what was published
    // since.
    NatsAnswers base;
    std::uint64_t interval_published{0};

    [[nodiscard]] std::uint64_t unanswered(const NatsAnswers& a) const {
        return published - std::min(published, a.acked + a.refused);
    }
    [[nodiscard]] std::uint64_t unanswered_bytes(const NatsAnswers& a) const {
        return published_bytes - std::min(published_bytes, a.answered_bytes);
    }

    [[nodiscard]] std::string tally(const NatsAnswers& a) const {
        std::string s = "of the " + std::to_string(interval_published) +
                        " messages published since the last complete flush, " +
                        std::to_string(a.acked - base.acked) + " were stored";
        if (a.refused > base.refused) {
            s += ", " + std::to_string(a.refused - base.refused) + " refused";
        }
        return s + ", and the other " + std::to_string(unanswered(a)) +
               " are unanswered and may or may not be in the stream. The job restarts from its "
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

    // Waits for answers until `done(answers)` or publish_timeout passes, in
    // slices, so a cancel ends the wait at once. Fails the sink on the
    // timeout, the cancel or a closed connection. `what` names the wait.
    NatsAnswers wait_until(const std::function<bool(const NatsAnswers&)>& done,
                           const std::string& what) {
        const auto deadline = std::chrono::steady_clock::now() + opts.publish_timeout;
        NatsAnswers a = publisher->answers();
        while (!done(a)) {
            if (cancel_requested()) {
                fail("the task was cancelled while " + what + "; " + tally(a));
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                fail("the server had not answered every message within the publish timeout (" +
                     std::to_string(opts.publish_timeout.count()) + " ms) while " + what + "; " +
                     tally(a));
            }
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            try {
                a = publisher->wait(a.acked + a.refused,
                                    std::min(kSlice, left + std::chrono::milliseconds{1}));
            } catch (const std::exception& e) {
                fail(std::string{"the connection to the server failed ("} + e.what() + ") while " +
                     what + "; " + tally(publisher->answers()));
            }
        }
        return a;
    }
};

NatsSink::NatsSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {}

NatsSink::~NatsSink() {
    if (impl_->publisher) {
        impl_->publisher->close();
    }
}

void NatsSink::open() {
    auto& m = *impl_;
    if (m.publisher) {
        m.publisher->close();
        m.publisher.reset();
    }
    m.open = false;
    m.failure.clear();
    m.published = m.published_bytes = m.interval_published = 0;
    m.base = NatsAnswers{};
    m.rt = runtime();
    NatsPublisherSettings s;
    s.conn = m.opts.conn;
    s.subject = m.opts.subject;
    s.max_pending = m.opts.max_pending;
    m.publisher = m.opts.publisher_factory(s);
    m.base = m.publisher->answers();
    m.open = true;
}

void NatsSink::on_data(const Batch<std::string>& batch) {
    auto& m = *impl_;
    m.require_usable("a record");
    if (batch.empty()) {
        return;
    }
    if (m.cancel_requested()) {
        m.fail("cancelled before publishing to '" + m.opts.subject + "'");
    }
    std::size_t bytes = 0;
    std::size_t count = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        const auto max_pending = static_cast<std::uint64_t>(m.opts.max_pending);
        const auto max_bytes = static_cast<std::uint64_t>(m.opts.max_pending_bytes);
        if (m.unanswered(m.publisher->answers()) >= max_pending ||
            m.unanswered_bytes(m.publisher->answers()) >= max_bytes) {
            (void)m.wait_until(
                [&](const NatsAnswers& a) {
                    return m.unanswered(a) < max_pending && m.unanswered_bytes(a) < max_bytes;
                },
                "waiting with max_pending (" + std::to_string(m.opts.max_pending) +
                    ") or max_pending_bytes (" + std::to_string(m.opts.max_pending_bytes) +
                    ") reached");
        }
        try {
            m.publisher->publish(payload);
        } catch (const std::exception& e) {
            ++m.published;  // it may have reached the server
            ++m.interval_published;
            m.fail(std::string{"publishing to '"} + m.opts.subject + "' failed (" + e.what() +
                   "); " + m.tally(m.publisher->answers()));
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

void NatsSink::on_barrier(CheckpointBarrier /*b*/) {
    auto& m = *impl_;
    m.require_usable("a checkpoint barrier");
    const bool publishes = m.interval_published > 0;
    flush();
    if (publishes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterAck);
    }
}

void NatsSink::flush() {
    auto& m = *impl_;
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    m.require_not_failed("a flush");
    if (!m.open || m.interval_published == 0) {
        return;
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeAck);
    const auto t0 = std::chrono::steady_clock::now();
    const auto a = m.wait_until([&m](const NatsAnswers& x) { return m.unanswered(x) == 0; },
                                "waiting for a flush's answers");
    if (a.refused > m.base.refused) {
        m.fail("the server refused " + std::to_string(a.refused - m.base.refused) +
               " publish(es) to '" + m.opts.subject + "' (first: " + a.first_refusal +
               "); is a JetStream stream bound to the subject, with room for them? " + m.tally(a));
    }
    m.base = a;
    m.interval_published = 0;
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void NatsSink::close() {
    auto& m = *impl_;
    // Unanswered messages are not waited for: a clean end of input flushes
    // before close(), and a cancelled or failed task must not wait on the
    // server.
    if (m.publisher) {
        m.publisher->close();
        m.publisher.reset();
    }
    m.open = false;
}

std::string NatsSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::nats
