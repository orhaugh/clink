#include "clink/rabbitmq/rabbitmq_source.hpp"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/checkpoint_ack_ledger.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

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
    // Delivery tags awaiting basic.ack, each held until the checkpoint that covers it completes
    // (snapshot_offset seals, notify_checkpoint_* move, produce/close take on this thread).
    clink::connectors::CheckpointAckLedger<std::uint64_t> ledger;
    // Every tag at or below this is acknowledged, so a basic.ack(multiple) up to a tag above it
    // acknowledges only the tags between them (produce thread only).
    std::uint64_t acked_floor{0};
    // Tags of aborted checkpoints. Never acknowledged on this channel; the broker requeues them
    // when it closes. While one sits above acked_floor a cumulative ack would take it too, so
    // later tags are acknowledged one by one (produce thread only).
    std::set<std::uint64_t> released;
    explicit Impl(Options o) : opts(std::move(o)) {}

    // Acknowledge, on the produce() thread, what completed checkpoints cover. Returns false on a
    // broker ack failure.
    bool drain_acks() {
        auto handover = ledger.take();
        released.insert(handover.released.begin(), handover.released.end());
        auto& ready = handover.ready;
        if (ready.empty() || conn == nullptr) {
            return true;
        }
        std::sort(ready.begin(), ready.end());
        const bool contiguous =
            ready.front() == acked_floor + 1 && ready.back() - ready.front() + 1 == ready.size();
        const bool released_below = !released.empty() && *released.begin() <= ready.back();
        if (contiguous && !released_below) {
            if (amqp_basic_ack(conn, kChannel, ready.back(), 1 /*multiple*/) != AMQP_STATUS_OK) {
                return false;
            }
            acked_floor = ready.back();
            return true;
        }
        for (const std::uint64_t tag : ready) {
            if (amqp_basic_ack(conn, kChannel, tag, 0 /*multiple*/) != AMQP_STATUS_OK) {
                return false;
            }
        }
        return true;
    }
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
    // Ack messages whose capturing checkpoint has committed since the last turn. Done here (the
    // produce thread) so every AMQP call stays on the single non-thread-safe connection.
    if (!impl_->drain_acks()) {
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(impl_->opts.name + ": basic.ack failed");
    }
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
            impl_->ledger.delivered(env.delivery_tag);
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
                                     CheckpointId ckpt_id) {
    // Seal the tags delivered since the last barrier as this checkpoint's (no ack yet). Nothing is
    // persisted to state: AMQP has no seekable offset, and recovery is the broker's redelivery of
    // whatever was not acknowledged.
    impl_->ledger.seal(ckpt_id.value());
}

void RabbitMqSource::notify_checkpoint_complete(CheckpointId ckpt_id) {
    // Every interval sealed at or below the completed id is safe to acknowledge; the next
    // produce() turn (or close) sends the basic.ack. Commit-dispatch thread: no AMQP call here.
    impl_->ledger.complete(ckpt_id.value());
}

void RabbitMqSource::notify_checkpoint_aborted(CheckpointId ckpt_id) {
    // The interval is released, never acknowledged on this channel: the broker redelivers it
    // once the channel closes, and no later completion acknowledges it with a cumulative ack.
    impl_->ledger.abort(ckpt_id.value());
}

bool RabbitMqSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // nothing to restore; the broker redelivers whatever was left unacked
}

void RabbitMqSource::close() {
    // Acknowledge what completed checkpoints cover, and nothing else: closing the channel hands
    // every other unacknowledged message back to the queue, whether this is a cancel, a failure
    // or the end of a stop. Best effort: what fails here is delivered again.
    try {
        if (!impl_->drain_acks()) {
            clink::log::warn("rabbitmq.source",
                             impl_->opts.name +
                                 ": basic.ack at close failed; the messages a "
                                 "completed checkpoint covers are delivered again");
        }
    } catch (const std::exception& e) {
        clink::log::warn("rabbitmq.source", impl_->opts.name + ": ack at close: " + e.what());
    }
    detail::close_and_destroy(impl_->conn, kChannel);
    impl_->conn = nullptr;
}

}  // namespace clink::rabbitmq
