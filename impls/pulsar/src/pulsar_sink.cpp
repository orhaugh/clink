#include "clink/pulsar/pulsar_sink.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <pulsar/c/message.h>
#include <pulsar/c/producer.h>
#include <pulsar/c/producer_configuration.h>

#include "clink/metrics/connector_metrics.hpp"

#include "pulsar_conn.hpp"

namespace clink::pulsar {

namespace {
constexpr const char* kLabel = "pulsar";

// Error sink for the async send callback. A standalone struct (NOT nested in the private
// PulsarSink::Impl) so the C callback can reference it. The callback runs on a Pulsar client
// thread, so it touches only the atomic flag + the mutex-guarded string.
struct SendErrorBox {
    std::atomic<bool> failed{false};
    std::mutex mu;
    std::string err;
};

// Async send completion callback. ctx is a SendErrorBox*.
void on_send(pulsar_result result, pulsar_message_id_t* /*msgId*/, void* ctx) {
    if (result == pulsar_result_Ok) {
        return;
    }
    auto* box = static_cast<SendErrorBox*>(ctx);
    box->failed.store(true, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(box->mu);
    if (box->err.empty()) {
        box->err = pulsar_result_str(result);
    }
}
}  // namespace

struct PulsarSink::Impl {
    Options opts;
    pulsar_client_t* client{nullptr};
    pulsar_producer_t* producer{nullptr};
    // Messages handed to send_async; freed at flush() once all sends have completed. Touched only
    // on the sink thread (on_data / flush) - the send callback does NOT touch this vector.
    std::vector<pulsar_message_t*> pending;
    SendErrorBox send_box;  // error flag + first error message from the async send callback
    explicit Impl(Options o) : opts(std::move(o)) {}
};

PulsarSink::PulsarSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.topic.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'topic' is required");
    }
}

PulsarSink::~PulsarSink() {
    for (pulsar_message_t* m : impl_->pending) {
        pulsar_message_free(m);
    }
    impl_->pending.clear();
    if (impl_->producer != nullptr) {
        pulsar_producer_close(impl_->producer);
        pulsar_producer_free(impl_->producer);
    }
    if (impl_->client != nullptr) {
        pulsar_client_close(impl_->client);
        pulsar_client_free(impl_->client);
    }
}

void PulsarSink::open() {
    impl_->client = detail::connect(impl_->opts.conn, impl_->opts.name);
    pulsar_producer_configuration_t* conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(conf, impl_->opts.batching ? 1 : 0);
    pulsar_producer_configuration_set_send_timeout(
        conf, static_cast<int>(impl_->opts.send_timeout.count()));
    const pulsar_result r = pulsar_client_create_producer(
        impl_->client, impl_->opts.topic.c_str(), conf, &impl_->producer);
    pulsar_producer_configuration_free(conf);
    detail::check(r, impl_->opts.name + ": create_producer");
}

void PulsarSink::on_data(const Batch<std::string>& batch) {
    if (impl_->producer == nullptr) {
        throw std::runtime_error(impl_->opts.name + ": on_data before open()");
    }
    std::size_t bytes = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        pulsar_message_t* msg = pulsar_message_create();
        pulsar_message_set_content(msg, payload.data(), payload.size());
        pulsar_producer_send_async(impl_->producer, msg, &on_send, &impl_->send_box);
        impl_->pending.push_back(msg);  // freed at flush(), after all sends complete
        bytes += payload.size();
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_out_inc(kLabel, batch.size());
        clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    }
}

void PulsarSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();  // align durable delivery to the checkpoint (at-least-once)
}

void PulsarSink::flush() {
    if (impl_->producer == nullptr || impl_->pending.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    // Block until every buffered/async-sent message has been persisted by the broker. After this
    // returns, all send callbacks have run, so send_failed reflects the whole window.
    const pulsar_result r = pulsar_producer_flush(impl_->producer);
    for (pulsar_message_t* m : impl_->pending) {
        pulsar_message_free(m);
    }
    impl_->pending.clear();
    if (r != pulsar_result_Ok) {
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(impl_->opts.name + ": flush: " + pulsar_result_str(r));
    }
    if (impl_->send_box.failed.exchange(false, std::memory_order_relaxed)) {
        std::string e;
        {
            std::lock_guard<std::mutex> lk(impl_->send_box.mu);
            e = std::move(impl_->send_box.err);
            impl_->send_box.err.clear();
        }
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(impl_->opts.name + ": async send failed: " + e);
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void PulsarSink::close() {
    if (impl_->producer != nullptr) {
        flush();  // confirm anything published since the last barrier (may throw)
        pulsar_producer_close(impl_->producer);
        pulsar_producer_free(impl_->producer);
        impl_->producer = nullptr;
    }
    if (impl_->client != nullptr) {
        pulsar_client_close(impl_->client);
        pulsar_client_free(impl_->client);
        impl_->client = nullptr;
    }
}

std::string PulsarSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::pulsar
