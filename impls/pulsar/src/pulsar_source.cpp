#include "clink/pulsar/pulsar_source.hpp"

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <pulsar/c/consumer.h>
#include <pulsar/c/consumer_configuration.h>
#include <pulsar/c/message.h>

#include "clink/metrics/connector_metrics.hpp"

#include "pulsar_conn.hpp"

namespace clink::pulsar {

namespace {
constexpr const char* kLabel = "pulsar";
}  // namespace

struct PulsarSource::Impl {
    Options opts;
    pulsar_client_t* client{nullptr};
    pulsar_consumer_t* consumer{nullptr};
    std::vector<pulsar_message_t*> held;  // emitted-but-unacked messages, ack'd at the next barrier
    explicit Impl(Options o) : opts(std::move(o)) {}
};

PulsarSource::PulsarSource(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.topic.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'topic' is required");
    }
}

PulsarSource::~PulsarSource() {
    for (pulsar_message_t* m : impl_->held) {
        pulsar_message_free(m);
    }
    impl_->held.clear();
    if (impl_->consumer != nullptr) {
        pulsar_consumer_close(impl_->consumer);
        pulsar_consumer_free(impl_->consumer);
    }
    if (impl_->client != nullptr) {
        pulsar_client_close(impl_->client);
        pulsar_client_free(impl_->client);
    }
}

void PulsarSource::open() {
    impl_->client = detail::connect(impl_->opts.conn, impl_->opts.name);
    pulsar_consumer_configuration_t* conf = pulsar_consumer_configuration_create();
    // Shared subscription so parallel subtasks distribute the topic and each acks only its own
    // messages (cumulative ack is not allowed on Shared; we ack individually at the barrier).
    pulsar_consumer_configuration_set_consumer_type(conf, pulsar_ConsumerShared);
    pulsar_consumer_configuration_set_receiver_queue_size(conf, impl_->opts.receiver_queue_size);
    const pulsar_result r = pulsar_client_subscribe(impl_->client,
                                                    impl_->opts.topic.c_str(),
                                                    impl_->opts.subscription.c_str(),
                                                    conf,
                                                    &impl_->consumer);
    pulsar_consumer_configuration_free(conf);
    detail::check(r, impl_->opts.name + ": subscribe");
}

bool PulsarSource::produce(Emitter<std::string>& out) {
    Batch<std::string> batch;
    std::size_t bytes = 0;
    for (std::size_t i = 0; i < impl_->opts.max_batch_size && !this->cancelled(); ++i) {
        pulsar_message_t* msg = nullptr;
        const pulsar_result r = pulsar_consumer_receive_with_timeout(
            impl_->consumer, &msg, static_cast<int>(impl_->opts.receive_timeout.count()));
        if (r == pulsar_result_Timeout) {
            break;  // no message within the receive window
        }
        if (r != pulsar_result_Ok) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(impl_->opts.name + ": receive: " + pulsar_result_str(r));
        }
        const auto len = pulsar_message_get_length(msg);
        std::string body(static_cast<const char*>(pulsar_message_get_data(msg)), len);
        bytes += body.size();
        batch.emplace(std::move(body));
        impl_->held.push_back(msg);  // keep alive until the barrier ack
    }
    if (!batch.empty()) {
        const std::size_t n = batch.size();
        out.emit_data(std::move(batch));
        clink::metrics::connector::records_in_inc(kLabel, n);
        clink::metrics::connector::bytes_in_inc(kLabel, bytes);
    }
    return !this->cancelled();
}

void PulsarSource::snapshot_offset(StateBackend& /*backend*/,
                                   OperatorId /*op_id*/,
                                   CheckpointId /*ckpt_id*/) {
    // Acknowledge + free every message emitted since the last barrier (at-least-once). Runs on
    // the produce() thread, so it is serialised with receive() on the single consumer.
    pulsar_result first_err = pulsar_result_Ok;
    for (pulsar_message_t* m : impl_->held) {
        const pulsar_result r = pulsar_consumer_acknowledge(impl_->consumer, m);
        if (r != pulsar_result_Ok && first_err == pulsar_result_Ok) {
            first_err = r;
        }
        pulsar_message_free(m);
    }
    impl_->held.clear();
    if (first_err != pulsar_result_Ok) {
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(impl_->opts.name +
                                 ": acknowledge: " + pulsar_result_str(first_err));
    }
}

bool PulsarSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // Pulsar's subscription cursor is durable server-side; unacked are redelivered
}

void PulsarSource::close() {
    for (pulsar_message_t* m : impl_->held) {
        pulsar_message_free(m);  // unacked on close -> Pulsar redelivers them (at-least-once)
    }
    impl_->held.clear();
    if (impl_->consumer != nullptr) {
        pulsar_consumer_close(impl_->consumer);
        pulsar_consumer_free(impl_->consumer);
        impl_->consumer = nullptr;
    }
    if (impl_->client != nullptr) {
        pulsar_client_close(impl_->client);
        pulsar_client_free(impl_->client);
        impl_->client = nullptr;
    }
}

}  // namespace clink::pulsar
