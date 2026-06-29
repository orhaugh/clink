#include "clink/rabbitmq/rabbitmq_sink.hpp"

#include <chrono>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#include "amqp_conn.hpp"

namespace clink::rabbitmq {

namespace {
constexpr const char* kLabel = "rabbitmq";
constexpr amqp_channel_t kChannel = 1;

struct timeval to_timeval(std::chrono::milliseconds ms) {
    struct timeval tv;
    tv.tv_sec = static_cast<long>(ms.count() / 1000);
    tv.tv_usec = static_cast<long>((ms.count() % 1000) * 1000);
    return tv;
}
}  // namespace

struct RabbitMqSink::Impl {
    Options opts;
    amqp_connection_state_t conn{nullptr};
    std::uint64_t next_tag{1};            // broker assigns 1,2,... per publish on a confirm channel
    std::set<std::uint64_t> outstanding;  // published-but-unconfirmed delivery tags
    explicit Impl(Options o) : opts(std::move(o)) {}
};

RabbitMqSink::RabbitMqSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.routing_key.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'routing_key' is required");
    }
}

RabbitMqSink::~RabbitMqSink() {
    detail::close_and_destroy(impl_->conn, kChannel);  // no flush in the dtor (must not throw)
    impl_->conn = nullptr;
}

void RabbitMqSink::open() {
    impl_->conn = detail::connect_and_open(impl_->opts.conn, kChannel, impl_->opts.name);
    // Publisher confirms: the broker basic.ack's each publish once it is safe (persisted for a
    // durable queue). flush() waits for those acks, giving at-least-once delivery.
    amqp_confirm_select(impl_->conn, kChannel);
    detail::check_reply(amqp_get_rpc_reply(impl_->conn), impl_->opts.name + ": confirm.select");
    impl_->next_tag = 1;
    impl_->outstanding.clear();
}

void RabbitMqSink::on_data(const Batch<std::string>& batch) {
    if (impl_->conn == nullptr) {
        throw std::runtime_error(impl_->opts.name + ": on_data before open()");
    }
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes(impl_->opts.content_type.c_str());
    props.delivery_mode = impl_->opts.persistent ? 2 : 1;  // 2 = persistent
    const amqp_bytes_t exchange = amqp_cstring_bytes(impl_->opts.exchange.c_str());
    const amqp_bytes_t routing_key = amqp_cstring_bytes(impl_->opts.routing_key.c_str());

    std::size_t bytes = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        amqp_bytes_t body;
        body.len = payload.size();
        body.bytes = const_cast<char*>(payload.data());
        const int rc = amqp_basic_publish(impl_->conn,
                                          kChannel,
                                          exchange,
                                          routing_key,
                                          0 /*mandatory*/,
                                          0 /*immediate*/,
                                          &props,
                                          body);
        if (rc != AMQP_STATUS_OK) {
            clink::metrics::connector::error_inc(kLabel, "sink");
            throw std::runtime_error(impl_->opts.name +
                                     ": basic.publish failed: " + amqp_error_string2(rc));
        }
        impl_->outstanding.insert(impl_->next_tag);
        ++impl_->next_tag;
        bytes += payload.size();
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_out_inc(kLabel, batch.size());
        clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    }
}

void RabbitMqSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();  // align durable delivery to the checkpoint (at-least-once)
}

void RabbitMqSink::flush() {
    if (impl_->conn == nullptr || impl_->outstanding.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    // Drain broker confirms until every outstanding publish has been ack'd. A nack/reject or a
    // timeout throws, so the job replays from the last checkpoint rather than losing data.
    while (!impl_->outstanding.empty()) {
        amqp_publisher_confirm_t result;
        struct timeval tv = to_timeval(impl_->opts.confirm_timeout);
        const amqp_rpc_reply_t r = amqp_publisher_confirm_wait(impl_->conn, &tv, &result);
        if (r.reply_type != AMQP_RESPONSE_NORMAL) {
            clink::metrics::connector::error_inc(kLabel, "sink");
            throw std::runtime_error(
                detail::reply_error(r, impl_->opts.name + ": publisher confirm wait"));
        }
        if (result.method == AMQP_BASIC_ACK_METHOD) {
            const auto& ack = result.payload.ack;
            if (ack.multiple != 0) {
                impl_->outstanding.erase(impl_->outstanding.begin(),
                                         impl_->outstanding.upper_bound(ack.delivery_tag));
            } else {
                impl_->outstanding.erase(ack.delivery_tag);
            }
        } else {
            // basic.nack or basic.reject: the broker could not durably accept the message.
            clink::metrics::connector::error_inc(kLabel, "sink");
            throw std::runtime_error(impl_->opts.name +
                                     ": broker nacked/rejected a publish (delivery not confirmed)");
        }
    }
    amqp_maybe_release_buffers(impl_->conn);
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void RabbitMqSink::close() {
    if (impl_->conn != nullptr) {
        flush();  // deliver + confirm anything published since the last barrier (may throw)
    }
    detail::close_and_destroy(impl_->conn, kChannel);
    impl_->conn = nullptr;
}

std::string RabbitMqSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::rabbitmq
