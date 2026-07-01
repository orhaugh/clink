#include "clink/rabbitmq/rabbitmq_source.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
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
    std::uint64_t last_acked{0};   // highest tag already basic.ack'd (produce thread only)
    // Highest tag whose capturing checkpoint has globally committed - safe to ack. Written by
    // notify_checkpoint_complete (commit-dispatch thread), drained by produce() (produce thread).
    std::atomic<std::uint64_t> commit_watermark{0};
    // checkpoint id -> highest tag emitted before that barrier. Written by snapshot_offset
    // (produce thread), read/erased by notify_checkpoint_* (commit thread); guarded by mu.
    std::mutex mu;
    std::map<std::uint64_t, std::uint64_t> pending;
    explicit Impl(Options o) : opts(std::move(o)) {}

    // Ack everything up to the committed watermark, on the produce() thread. Returns false on a
    // broker ack failure. ack(multiple=true) confirms every tag up to the watermark in one frame.
    bool drain_acks() {
        const std::uint64_t safe = commit_watermark.load(std::memory_order_acquire);
        if (conn == nullptr || safe <= last_acked) {
            return true;
        }
        if (amqp_basic_ack(conn, kChannel, safe, 1 /*multiple*/) != AMQP_STATUS_OK) {
            return false;
        }
        last_acked = safe;
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
                                     CheckpointId ckpt_id) {
    // Record (not ack) the highest tag emitted before this barrier against the checkpoint id.
    // The ack is deferred to notify_checkpoint_complete so a message is confirmed to the broker
    // only after the checkpoint that captured it is globally durable - an aborted checkpoint
    // must not consume it. Runs on the produce() thread; nothing is persisted to state (AMQP has
    // no seekable offset, recovery relies on broker redelivery of unacked messages).
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->pending[ckpt_id.value()] = impl_->highest_tag;
}

void RabbitMqSource::notify_checkpoint_complete(CheckpointId ckpt_id) {
    // A checkpoint committed: everything captured up to it is now safe to ack. Advance the
    // watermark to the highest tag of any pending checkpoint <= the committed id, dropping those
    // records. The produce() thread issues the basic.ack(multiple=true) on its next turn.
    std::lock_guard<std::mutex> lk(impl_->mu);
    std::uint64_t safe = impl_->commit_watermark.load(std::memory_order_relaxed);
    for (auto it = impl_->pending.begin();
         it != impl_->pending.end() && it->first <= ckpt_id.value();) {
        safe = std::max(safe, it->second);
        it = impl_->pending.erase(it);
    }
    impl_->commit_watermark.store(safe, std::memory_order_release);
}

void RabbitMqSource::notify_checkpoint_aborted(CheckpointId ckpt_id) {
    // The checkpoint aborted: drop its pending record without acking. Those messages stay unacked
    // and the broker redelivers them on reconnect (or a later committed checkpoint that captured a
    // higher tag acks past them via multiple=true).
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->pending.erase(ckpt_id.value());
}

bool RabbitMqSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // nothing to restore; the broker redelivers whatever was left unacked
}

void RabbitMqSource::close() {
    detail::close_and_destroy(impl_->conn, kChannel);
    impl_->conn = nullptr;
}

}  // namespace clink::rabbitmq
