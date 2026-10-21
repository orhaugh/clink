#include "clink/nats/nats_sink.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#include "nats_conn.hpp"

namespace clink::nats {

namespace {
constexpr const char* kLabel = "nats";
}  // namespace

struct NatsSink::Impl {
    Options opts;
    natsConnection* conn{nullptr};
    jsCtx* js{nullptr};
    std::uint64_t pending{0};  // async publishes since the last flush (acks awaited at flush)
    // Publishes the server refused since the last flush: a negative PubAck, or
    // no stream bound to the subject. nats.c reports those only to the
    // context's error handler, on its own thread, and without one it drops
    // the message and counts the publish as complete.
    std::atomic<std::uint64_t> refused{0};
    std::mutex refusal_mu;
    std::string last_refusal;
    explicit Impl(Options o) : opts(std::move(o)) {}

    // nats.c calls this before it counts the publish as complete, so a
    // js_PublishAsyncComplete that returns has seen every refusal. The
    // library destroys the message once this returns.
    static void on_refused(jsCtx* /*js*/, jsPubAckErr* pae, void* closure) {
        auto* self = static_cast<Impl*>(closure);
        std::string why = pae->ErrText != nullptr && pae->ErrText[0] != '\0'
                              ? std::string{pae->ErrText}
                              : std::string{natsStatus_GetText(pae->Err)};
        {
            const std::lock_guard<std::mutex> lock(self->refusal_mu);
            self->last_refusal = std::move(why);
        }
        self->refused.fetch_add(1, std::memory_order_acq_rel);
    }
};

NatsSink::NatsSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.subject.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'subject' is required");
    }
}

NatsSink::~NatsSink() {
    jsCtx_Destroy(impl_->js);  // before the connection (nats.c requirement)
    natsConnection_Destroy(impl_->conn);
}

void NatsSink::open() {
    impl_->conn = detail::connect(impl_->opts.conn, impl_->opts.name);
    jsOptions jsOpts;
    detail::check(jsOptions_Init(&jsOpts), impl_->opts.name + ": jsOptions_Init");
    jsOpts.PublishAsync.MaxPending = impl_->opts.max_pending;  // backpressure on in-flight acks
    jsOpts.PublishAsync.ErrHandler = &Impl::on_refused;
    jsOpts.PublishAsync.ErrHandlerClosure = impl_.get();
    impl_->refused.store(0, std::memory_order_relaxed);
    detail::check(natsConnection_JetStream(&impl_->js, impl_->conn, &jsOpts),
                  impl_->opts.name + ": JetStream ctx");
    impl_->pending = 0;
}

void NatsSink::on_data(const Batch<std::string>& batch) {
    if (impl_->js == nullptr) {
        throw std::runtime_error(impl_->opts.name + ": on_data before open()");
    }
    std::size_t bytes = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        // Async publish: queues the message and returns (blocks up to StallWait when MaxPending
        // is reached). The server ack is awaited in flush() via js_PublishAsyncComplete.
        const natsStatus s = js_PublishAsync(impl_->js,
                                             impl_->opts.subject.c_str(),
                                             payload.data(),
                                             static_cast<int>(payload.size()),
                                             nullptr);
        if (s != NATS_OK) {
            clink::metrics::connector::error_inc(kLabel, "sink");
            throw std::runtime_error(impl_->opts.name +
                                     ": js_PublishAsync: " + natsStatus_GetText(s));
        }
        ++impl_->pending;
        bytes += payload.size();
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_out_inc(kLabel, batch.size());
        clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    }
}

void NatsSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();  // align durable delivery to the checkpoint (at-least-once)
}

void NatsSink::flush() {
    if (impl_->js == nullptr || impl_->pending == 0) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    jsPubOptions pubOpts;
    detail::check(jsPubOptions_Init(&pubOpts), impl_->opts.name + ": jsPubOptions_Init");
    pubOpts.MaxWait = static_cast<int64_t>(impl_->opts.publish_timeout.count());
    // Block until the server has ack'd every pending async publish. A timeout (or any non-OK)
    // throws so the job replays from the last checkpoint rather than losing un-acked messages.
    const natsStatus s = js_PublishAsyncComplete(impl_->js, &pubOpts);
    if (s != NATS_OK) {
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(impl_->opts.name +
                                 ": js_PublishAsyncComplete: " + natsStatus_GetText(s));
    }
    // Every publish is answered, but an answer can be a refusal, which leaves
    // the message in no stream. Failing here makes the job restore from its
    // last checkpoint and publish it again.
    if (const auto refused = impl_->refused.exchange(0, std::memory_order_acq_rel); refused > 0) {
        clink::metrics::connector::error_inc(kLabel, "sink");
        std::string last;
        {
            const std::lock_guard<std::mutex> lock(impl_->refusal_mu);
            last = impl_->last_refusal;
        }
        throw std::runtime_error(impl_->opts.name + ": the server refused " +
                                 std::to_string(refused) + " publish(es) to '" +
                                 impl_->opts.subject + "' (last: " + last +
                                 "); is a JetStream stream bound to the subject?");
    }
    impl_->pending = 0;
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void NatsSink::close() {
    if (impl_->js != nullptr) {
        flush();  // confirm anything published since the last barrier (may throw)
    }
    jsCtx_Destroy(impl_->js);
    impl_->js = nullptr;
    natsConnection_Destroy(impl_->conn);
    impl_->conn = nullptr;
}

std::string NatsSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::nats
