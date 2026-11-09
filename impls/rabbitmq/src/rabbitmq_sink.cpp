// RabbitMQ sink. See rabbitmq_sink.hpp.

#include "clink/rabbitmq/rabbitmq_sink.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "fault_points.hpp"

namespace clink::rabbitmq {

namespace {

constexpr const char* kLabel = "rabbitmq";

RabbitMqSink::Options checked(RabbitMqSink::Options o) {
    if (o.routing_key.empty()) {
        throw std::runtime_error(o.name + ": 'routing_key' is required");
    }
    if (o.confirm_timeout.count() <= 0) {
        throw std::runtime_error(o.name + ": the confirm timeout must be positive");
    }
    if (o.max_unconfirmed == 0) {
        o.max_unconfirmed = 1;
    }
    if (o.max_record_bytes == 0) {
        o.max_record_bytes = RabbitMqSink::kDefaultMaxRecordBytes;
    }
    if (!o.publisher_factory) {
        o.publisher_factory = [](const PublisherSettings& s) {
            return connect_rabbitmq_publisher(s);
        };
    }
    return o;
}

// Shuts the connection's socket once the task is cancelled, so a publish the
// broker no longer reads, or a wait for confirms, ends at once. One thread per
// sink, asleep unless armed.
class Watch {
public:
    Watch(RabbitMqPublisher& publisher, CancelSignal cancel)
        : publisher_(publisher), cancel_(std::move(cancel)), thread_([this] { run(); }) {}
    ~Watch() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }
    Watch(const Watch&) = delete;
    Watch& operator=(const Watch&) = delete;

    void arm() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            armed_ = true;
        }
        cv_.notify_all();
    }
    void disarm() {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = false;
    }
    // True once the watch has shut the socket.
    [[nodiscard]] bool fired() {
        const std::lock_guard<std::mutex> lock(mu_);
        return fired_;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            if (!armed_ || fired_) {
                cv_.wait(lock, [this] { return stop_ || (armed_ && !fired_); });
                continue;
            }
            cv_.wait_for(lock, std::chrono::milliseconds{20});
            if (stop_ || !armed_ || !cancel_.requested()) {
                continue;
            }
            fired_ = true;
            lock.unlock();
            publisher_.cancel();
            lock.lock();
        }
    }

    RabbitMqPublisher& publisher_;
    CancelSignal cancel_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool fired_{false};
    std::thread thread_;
};

}  // namespace

struct RabbitMqSink::Impl {
    explicit Impl(Options o) : opts(checked(std::move(o))) {}

    Options opts;
    std::unique_ptr<RabbitMqPublisher> publisher;
    std::unique_ptr<Watch> watch;
    RuntimeContext* rt{nullptr};
    std::string failure;
    bool open{false};
    std::uint64_t published{0};           // delivery tags assigned since open()
    std::set<std::uint64_t> outstanding;  // published and not yet confirmed
    // Since the last flush that had every message confirmed.
    std::size_t interval_published{0};
    std::size_t interval_acked{0};
    std::size_t interval_returned{0};
    std::size_t interval_nacked{0};
    std::string first_return;

    [[nodiscard]] std::string target() const {
        return "exchange '" + opts.exchange + "', routing key '" + opts.routing_key + "'";
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

    // What the messages published since the last complete flush came to.
    [[nodiscard]] std::string tally() const {
        const auto confirmed = interval_acked - std::min(interval_acked, interval_returned);
        std::string s = "of the " + std::to_string(interval_published) +
                        " messages published since the last complete flush, " +
                        std::to_string(confirmed) + " were confirmed";
        if (interval_returned > 0) {
            s += ", " + std::to_string(interval_returned) + " returned unroutable";
        }
        if (interval_nacked > 0) {
            s += ", " + std::to_string(interval_nacked) + " refused (basic.nack)";
        }
        return s + ", and the other " + std::to_string(outstanding.size()) +
               " are unconfirmed and may or may not be in a queue. The job restarts from its "
               "last checkpoint and publishes its records again";
    }

    [[noreturn]] void fail(const std::string& why) {
        failure = why;
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(opts.name + ": " + why);
    }

    [[nodiscard]] bool cancel_requested() const noexcept {
        return rt != nullptr && rt->cancel_requested();
    }

    // Removes the tags an ack or nack covers; returns how many it covered.
    std::size_t settle(std::uint64_t tag, bool multiple) {
        if (!multiple) {
            return outstanding.erase(tag);
        }
        const auto end = outstanding.upper_bound(tag);
        std::size_t n = 0;
        for (auto it = outstanding.begin(); it != end; ++it) {
            ++n;
        }
        outstanding.erase(outstanding.begin(), end);
        return n;
    }

    // Reads the broker's confirms until `done()` or confirm_timeout passes, and
    // fails the sink unless every message they settle was confirmed. A nack or a
    // return does not stop the reading, since the broker confirms out of order
    // (a queue refuses a publish at once and confirms a persistent one once it
    // is on disk): the failure then says exactly what became of each message.
    // `what` names the wait.
    void drain(const std::function<bool()>& done, const std::string& what) {
        const auto deadline = std::chrono::steady_clock::now() + opts.confirm_timeout;
        if (watch) {
            watch->arm();
        }
        while (!done()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                disarm();
                fail("the broker had not confirmed every message within the confirm timeout (" +
                     std::to_string(opts.confirm_timeout.count()) + " ms) while " + what + ": " +
                     tally());
            }
            const auto left =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) +
                std::chrono::milliseconds{1};
            const auto e = publisher->next(left);
            switch (e.kind) {
                case PublisherEvent::Kind::Ack:
                    interval_acked += settle(e.delivery_tag, e.multiple);
                    break;
                case PublisherEvent::Kind::Return:
                    if (interval_returned++ == 0) {
                        first_return = e.text;
                    }
                    break;
                case PublisherEvent::Kind::Nack:
                    interval_nacked += settle(e.delivery_tag, e.multiple);
                    break;
                case PublisherEvent::Kind::Timeout:
                    break;  // the deadline check above decides
                case PublisherEvent::Kind::Closed:
                    disarm();
                    fail(e.text + ", while " + what + "; " + tally());
                case PublisherEvent::Kind::Lost: {
                    const bool cancelled = watch && watch->fired();
                    disarm();
                    fail((cancelled ? std::string{"the task was cancelled"}
                                    : "the connection to the broker failed (" + e.text + ")") +
                         " while " + what + "; " + tally());
                }
            }
        }
        disarm();
        if (interval_nacked > 0 || interval_returned > 0) {
            std::string lead;
            if (interval_nacked > 0) {
                lead = "the broker refused " + std::to_string(interval_nacked) +
                       " message(s) (basic.nack)";
            }
            if (interval_returned > 0) {
                lead += (lead.empty() ? "the broker" : ", and") + std::string{" returned "} +
                        std::to_string(interval_returned) +
                        " message(s) that no queue is bound to receive (" + target() +
                        ", first: " + first_return +
                        "; bind a queue, or set mandatory='false' to let the exchange drop such "
                        "messages)";
            }
            fail(lead + " while " + what + "; " + tally());
        }
    }

    void disarm() {
        if (watch) {
            watch->disarm();
        }
    }

    void close_publisher(bool orderly) noexcept {
        watch.reset();
        if (publisher) {
            publisher->close(orderly);
            publisher.reset();
        }
        open = false;
    }
};

RabbitMqSink::RabbitMqSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {}

RabbitMqSink::~RabbitMqSink() {
    impl_->close_publisher(false);
}

void RabbitMqSink::open() {
    auto& m = *impl_;
    m.close_publisher(false);
    m.failure.clear();
    m.outstanding.clear();
    m.published = 0;
    m.interval_published = m.interval_acked = m.interval_returned = m.interval_nacked = 0;
    m.first_return.clear();
    m.rt = runtime();
    PublisherSettings s;
    s.conn = m.opts.conn;
    s.exchange = m.opts.exchange;
    s.routing_key = m.opts.routing_key;
    s.persistent = m.opts.persistent;
    s.mandatory = m.opts.mandatory;
    s.content_type = m.opts.content_type;
    m.publisher = m.opts.publisher_factory(s);
    if (m.rt != nullptr) {
        m.watch = std::make_unique<Watch>(*m.publisher, m.rt->cancel_signal());
    }
    m.open = true;
}

void RabbitMqSink::on_data(const Batch<std::string>& batch) {
    auto& m = *impl_;
    m.require_usable("a record");
    if (batch.empty()) {
        return;
    }
    if (m.cancel_requested()) {
        m.fail("cancelled before publishing to " + m.target());
    }
    std::size_t bytes = 0;
    std::size_t count = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        if (payload.size() > m.opts.max_record_bytes) {
            m.fail("a record is " + std::to_string(payload.size()) +
                   " bytes, more than max_record_bytes (" +
                   std::to_string(m.opts.max_record_bytes) + "); " + m.tally());
        }
        if (m.outstanding.size() >= m.opts.max_unconfirmed) {
            m.drain([&m] { return m.outstanding.size() < m.opts.max_unconfirmed; },
                    "waiting with max_unconfirmed (" + std::to_string(m.opts.max_unconfirmed) +
                        ") messages unconfirmed");
        }
        if (m.watch) {
            m.watch->arm();
        }
        try {
            m.publisher->publish(payload);
        } catch (const std::exception& e) {
            const bool cancelled = m.watch && m.watch->fired();
            m.disarm();
            ++m.interval_published;  // it may have reached the broker
            m.outstanding.insert(++m.published);
            m.fail((cancelled ? std::string{"the task was cancelled"}
                              : std::string{"publishing failed ("} + e.what() + ")") +
                   " publishing to " + m.target() + "; " + m.tally());
        }
        m.disarm();
        m.outstanding.insert(++m.published);
        ++m.interval_published;
        bytes += payload.size();
        ++count;
    }
    clink::metrics::connector::records_out_inc(kLabel, count);
    clink::metrics::connector::bytes_out_inc(kLabel, bytes);
}

void RabbitMqSink::on_barrier(CheckpointBarrier /*b*/) {
    auto& m = *impl_;
    m.require_usable("a checkpoint barrier");
    const bool publishes = m.interval_published > 0;
    flush();
    if (publishes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterConfirm);
    }
}

void RabbitMqSink::flush() {
    auto& m = *impl_;
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    m.require_not_failed("a flush");
    if (!m.open || m.interval_published == 0) {
        return;
    }
    if (m.cancel_requested() && !m.outstanding.empty()) {
        m.fail("cancelled before the broker confirmed every message; " + m.tally());
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeConfirm);
    const auto t0 = std::chrono::steady_clock::now();
    m.drain([&m] { return m.outstanding.empty(); }, "waiting for a flush's confirms");
    m.interval_published = m.interval_acked = 0;
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void RabbitMqSink::close() {
    auto& m = *impl_;
    // Unconfirmed messages are not waited for: a clean end of input flushes
    // before close(), and a cancelled or failed task must not wait on the
    // broker.
    m.close_publisher(m.failure.empty() && !m.cancel_requested());
    m.outstanding.clear();
}

std::string RabbitMqSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::rabbitmq
