#include "clink/rabbitmq/rabbitmq_source.hpp"

#include <cstdint>
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

struct RabbitMqSource::Impl {
    Options opts;
    amqp_connection_state_t conn{nullptr};
    std::uint64_t highest_tag{0};  // latest consumed delivery tag (emitted downstream)
    std::uint64_t last_acked{0};   // highest tag already basic.ack'd
    explicit Impl(Options o) : opts(std::move(o)) {}
};

RabbitMqSource::RabbitMqSource(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.queue.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'queue' is required");
    }
}

RabbitMqSource::~RabbitMqSource() {
    detail::close_and_destroy(impl_->conn, kChannel);
    impl_->conn = nullptr;
}

void RabbitMqSource::open() {
    impl_->conn = detail::connect_and_open(impl_->opts.conn, kChannel, impl_->opts.name);
    // Declare the queue (durable) so the source works out of the box; idempotent if it already
    // exists with matching parameters. Disable via declare_queue=false for a pre-provisioned queue.
    if (impl_->opts.declare_queue) {
        amqp_queue_declare(impl_->conn,
                           kChannel,
                           amqp_cstring_bytes(impl_->opts.queue.c_str()),
                           0 /*passive*/,
                           1 /*durable*/,
                           0 /*exclusive*/,
                           0 /*auto_delete*/,
                           amqp_empty_table);
        detail::check_reply(amqp_get_rpc_reply(impl_->conn), impl_->opts.name + ": queue.declare");
    }
    // Bound the number of unacked messages the broker will push to us.
    amqp_basic_qos(impl_->conn, kChannel, 0 /*prefetch_size*/, impl_->opts.prefetch, 0 /*global*/);
    detail::check_reply(amqp_get_rpc_reply(impl_->conn), impl_->opts.name + ": basic.qos");
    amqp_basic_consume(impl_->conn,
                       kChannel,
                       amqp_cstring_bytes(impl_->opts.queue.c_str()),
                       impl_->opts.consumer_tag.empty()
                           ? amqp_empty_bytes
                           : amqp_cstring_bytes(impl_->opts.consumer_tag.c_str()),
                       0 /*no_local*/,
                       0 /*no_ack: manual ack for at-least-once*/,
                       0 /*exclusive*/,
                       amqp_empty_table);
    detail::check_reply(amqp_get_rpc_reply(impl_->conn), impl_->opts.name + ": basic.consume");
}

bool RabbitMqSource::produce(Emitter<std::string>& out) {
    Batch<std::string> batch;
    std::size_t bytes = 0;
    for (std::size_t i = 0; i < impl_->opts.max_batch_size && !this->cancelled(); ++i) {
        amqp_maybe_release_buffers(impl_->conn);
        amqp_envelope_t env;
        struct timeval tv = to_timeval(impl_->opts.poll_timeout);
        const amqp_rpc_reply_t r = amqp_consume_message(impl_->conn, &env, &tv, 0);
        if (r.reply_type == AMQP_RESPONSE_NORMAL) {
            std::string body(static_cast<const char*>(env.message.body.bytes),
                             env.message.body.len);
            bytes += body.size();
            impl_->highest_tag = env.delivery_tag;
            batch.emplace(std::move(body));
            amqp_destroy_envelope(&env);
            continue;
        }
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            r.library_error == AMQP_STATUS_TIMEOUT) {
            break;  // no message within the poll window
        }
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            r.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
            // A non-Basic.Deliver frame is waiting (heartbeat, or a channel/connection method).
            // Drain one frame and continue; a genuine close surfaces on the next consume.
            amqp_frame_t frame;
            if (amqp_simple_wait_frame(impl_->conn, &frame) != AMQP_STATUS_OK) {
                break;
            }
            continue;
        }
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(detail::reply_error(r, impl_->opts.name + ": consume"));
    }
    if (!batch.empty()) {
        const std::size_t n = batch.size();
        out.emit_data(std::move(batch));
        clink::metrics::connector::records_in_inc(kLabel, n);
        clink::metrics::connector::bytes_in_inc(kLabel, bytes);
    }
    return !this->cancelled();
}

void RabbitMqSource::snapshot_offset(StateBackend& /*backend*/,
                                     OperatorId /*op_id*/,
                                     CheckpointId /*ckpt_id*/) {
    // At-least-once ack point. Runs on the produce() thread (see dag.hpp), so the basic.ack is
    // serialised with basic.consume on the single AMQP connection. ack(multiple=true) confirms
    // every message up to the latest emitted tag in one frame. We persist nothing to state -
    // AMQP has no seekable offset; recovery relies on the broker redelivering unacked messages.
    if (impl_->conn == nullptr || impl_->highest_tag <= impl_->last_acked) {
        return;
    }
    if (amqp_basic_ack(impl_->conn, kChannel, impl_->highest_tag, 1 /*multiple*/) !=
        AMQP_STATUS_OK) {
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(impl_->opts.name + ": basic.ack failed");
    }
    impl_->last_acked = impl_->highest_tag;
}

bool RabbitMqSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // nothing to restore; the broker redelivers whatever was left unacked
}

void RabbitMqSource::close() {
    detail::close_and_destroy(impl_->conn, kChannel);
    impl_->conn = nullptr;
}

}  // namespace clink::rabbitmq
