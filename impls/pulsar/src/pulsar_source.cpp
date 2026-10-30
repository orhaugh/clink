#include "clink/pulsar/pulsar_source.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <pulsar/MessageId.h>
#include <pulsar/c/client.h>
#include <pulsar/c/consumer.h>
#include <pulsar/c/consumer_configuration.h>
#include <pulsar/c/message.h>
#include <pulsar/c/message_id.h>
#include <pulsar/c/string_list.h>

#include "clink/metrics/connector_metrics.hpp"

#include "pulsar_conn.hpp"

namespace clink::pulsar {

namespace {
constexpr const char* kLabel = "pulsar";
// Operator state key of an exclusive subscription's position: the serialized id of the last
// message emitted before the barrier.
constexpr const char* kPositionKey = "pulsar.position";

// The serialized id of a message, the form the position is persisted in.
std::string serialized_id(pulsar_message_t* msg) {
    pulsar_message_id_t* id = pulsar_message_get_message_id(msg);
    int len = 0;
    void* buf = pulsar_message_id_serialize(id, &len);
    std::string out(static_cast<const char*>(buf), static_cast<std::size_t>(len));
    std::free(buf);
    pulsar_message_id_free(id);
    return out;
}
}  // namespace

struct PulsarSource::Impl {
    Options opts;
    pulsar_client_t* client{nullptr};
    pulsar_consumer_t* consumer{nullptr};
    std::vector<pulsar_message_t*> held;  // emitted since the last barrier (produce thread only)
    // Cross-thread ack bookkeeping (guarded by mu): snapshot_offset (produce thread) buckets
    // `held` under a checkpoint id; notify_checkpoint_* (commit-dispatch thread) move buckets into
    // ack_ready / abort_drop; produce() (produce thread) drains those, keeping every
    // pulsar_consumer_acknowledge / pulsar_message_free on the produce thread (one consumer).
    std::mutex mu;
    std::map<std::uint64_t, std::vector<pulsar_message_t*>> pending;  // ckpt id -> captured msgs
    std::vector<pulsar_message_t*> ack_ready;                         // committed: ack then free
    std::vector<pulsar_message_t*> abort_drop;  // aborted: free without ack (-> Pulsar redelivers)
    // Exclusive subscription only (produce thread). The serialized id of the last message emitted
    // before the latest barrier, and of the position the run was restored to, which a barrier
    // with nothing new to record carries forward.
    bool exclusive{false};
    std::string position;
    // Set by the seek on restore: drop (and ack) anything at or before `position` that still
    // arrives, which the restored checkpoint already covers. Cleared by the first later message.
    bool skipping_replayed{false};
    ::pulsar::MessageId replayed_up_to;
    explicit Impl(Options o)
        : opts(std::move(o)), exclusive(opts.subscription_type == "exclusive") {}

    // Free every not-yet-acked message (held + bucketed + queued) WITHOUT acking, so Pulsar
    // redelivers them (at-least-once). Called from close()/dtor after the runner thread stops.
    void free_unacked() {
        auto wipe = [](std::vector<pulsar_message_t*>& v) {
            for (pulsar_message_t* m : v) {
                pulsar_message_free(m);
            }
            v.clear();
        };
        wipe(held);
        for (auto& [ckpt, msgs] : pending) {
            wipe(msgs);
        }
        pending.clear();
        wipe(ack_ready);
        wipe(abort_drop);
    }
};

PulsarSource::PulsarSource(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.topic.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'topic' is required");
    }
    if (impl_->opts.subscription_type != "shared" && impl_->opts.subscription_type != "exclusive") {
        throw std::runtime_error(impl_->opts.name +
                                 ": subscription_type must be 'shared' or 'exclusive', not '" +
                                 impl_->opts.subscription_type + "'");
    }
}

PulsarSource::~PulsarSource() {
    impl_->free_unacked();
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
    if (impl_->exclusive) {
        // A seek by message id positions one topic's cursor; a partitioned topic has one per
        // partition, which this consumer cannot address. Refused before anything is consumed.
        pulsar_string_list_t* parts = nullptr;
        const pulsar_result pr =
            pulsar_client_get_topic_partitions(impl_->client, impl_->opts.topic.c_str(), &parts);
        detail::check_first_contact(pr, impl_->opts.name + ": topic partitions", impl_->opts.conn);
        const int n = parts != nullptr ? pulsar_string_list_size(parts) : 0;
        if (parts != nullptr) {
            pulsar_string_list_free(parts);
        }
        if (n > 1) {
            throw std::runtime_error(impl_->opts.name + ": subscription_type 'exclusive' replays " +
                                     "by seeking one cursor, and '" + impl_->opts.topic +
                                     "' is partitioned (" + std::to_string(n) +
                                     " partitions); use a non-partitioned topic or 'shared'");
        }
    }
    pulsar_consumer_configuration_t* conf = pulsar_consumer_configuration_create();
    // Shared: parallel subtasks distribute the topic and each acks only its own messages
    // (cumulative ack is not allowed on Shared; we ack individually at checkpoint completion).
    // Exclusive: this subtask is the only consumer, which is what lets a restore seek the cursor.
    pulsar_consumer_configuration_set_consumer_type(
        conf, impl_->exclusive ? pulsar_ConsumerExclusive : pulsar_ConsumerShared);
    pulsar_consumer_configuration_set_receiver_queue_size(conf, impl_->opts.receiver_queue_size);
    // An exclusive subscription can still be held by the consumer of the incarnation this one
    // replaces, while that one is torn down: retried until it is released or the timeout passes.
    const auto deadline = std::chrono::steady_clock::now() + impl_->opts.subscribe_timeout;
    pulsar_result r = pulsar_result_Ok;
    for (;;) {
        r = pulsar_client_subscribe(impl_->client,
                                    impl_->opts.topic.c_str(),
                                    impl_->opts.subscription.c_str(),
                                    conf,
                                    &impl_->consumer);
        if (!impl_->exclusive || r != pulsar_result_ConsumerBusy ||
            std::chrono::steady_clock::now() >= deadline || this->cancelled()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{250});
    }
    pulsar_consumer_configuration_free(conf);
    detail::check_first_contact(r, impl_->opts.name + ": subscribe", impl_->opts.conn);
    if (impl_->exclusive && !impl_->position.empty()) {
        // Back to the last message the restored checkpoint covers. The seek excludes the message
        // itself (libpulsar's start_message_id_inclusive defaults to false), so everything after
        // it is delivered again, in topic order, acknowledged or not.
        pulsar_message_id_t* at = pulsar_message_id_deserialize(
            impl_->position.data(), static_cast<std::uint32_t>(impl_->position.size()));
        if (at == nullptr) {
            throw std::runtime_error(impl_->opts.name +
                                     ": the checkpointed position does not deserialize");
        }
        const pulsar_result sr = pulsar_consumer_seek(impl_->consumer, at);
        pulsar_message_id_free(at);
        if (sr != pulsar_result_Ok) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(
                impl_->opts.name + ": seek to the checkpointed position: " + pulsar_result_str(sr));
        }
        impl_->replayed_up_to = ::pulsar::MessageId::deserialize(impl_->position);
        impl_->skipping_replayed = true;
    }
}

bool PulsarSource::produce(Emitter<std::string>& out) {
    // Ack/free messages whose checkpoint resolved since the last turn. Done here (the produce
    // thread) so every pulsar C call stays on the single consumer.
    {
        std::vector<pulsar_message_t*> to_ack;
        std::vector<pulsar_message_t*> to_drop;
        {
            std::lock_guard<std::mutex> lk(impl_->mu);
            to_ack.swap(impl_->ack_ready);
            to_drop.swap(impl_->abort_drop);
        }
        pulsar_result first_err = pulsar_result_Ok;
        for (pulsar_message_t* m : to_ack) {
            const pulsar_result r = pulsar_consumer_acknowledge(impl_->consumer, m);
            if (r != pulsar_result_Ok && first_err == pulsar_result_Ok) {
                first_err = r;
            }
            pulsar_message_free(m);
        }
        for (pulsar_message_t* m : to_drop) {
            pulsar_message_free(m);  // no ack -> Pulsar redelivers
        }
        if (first_err != pulsar_result_Ok) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(impl_->opts.name +
                                     ": acknowledge: " + pulsar_result_str(first_err));
        }
    }
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
        if (impl_->skipping_replayed) {
            // The seek resumes after the recorded message, so nothing at or before it should
            // arrive; anything that does is in the restored checkpoint already: acknowledged here,
            // not emitted twice.
            if (::pulsar::MessageId::deserialize(serialized_id(msg)) <= impl_->replayed_up_to) {
                (void)pulsar_consumer_acknowledge(impl_->consumer, msg);
                pulsar_message_free(msg);
                continue;
            }
            impl_->skipping_replayed = false;
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

void PulsarSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) {
    // Exclusive: the position this checkpoint covers is the last message emitted before the
    // barrier, or, with nothing emitted since, the one the previous barrier or the restore
    // recorded.
    if (impl_->exclusive) {
        if (!impl_->held.empty()) {
            impl_->position = serialized_id(impl_->held.back());
        }
        if (impl_->position.empty()) {
            backend.erase_operator_state(op_id, kPositionKey);
        } else {
            backend.put_operator_state(op_id, kPositionKey, impl_->position);
        }
    }
    // Bucket the messages emitted before this barrier under the checkpoint id (no ack yet). The
    // ack is deferred to notify_checkpoint_complete so a message is confirmed only after the
    // checkpoint that captured it is globally durable. Runs on the produce() thread.
    if (impl_->held.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto& bucket = impl_->pending[ckpt_id.value()];
    bucket.insert(bucket.end(), impl_->held.begin(), impl_->held.end());
    impl_->held.clear();
}

void PulsarSource::notify_checkpoint_complete(CheckpointId ckpt_id) {
    // Every checkpoint up to the committed id is durable: queue its messages for ack on the
    // produce() thread.
    std::lock_guard<std::mutex> lk(impl_->mu);
    for (auto it = impl_->pending.begin();
         it != impl_->pending.end() && it->first <= ckpt_id.value();) {
        impl_->ack_ready.insert(impl_->ack_ready.end(), it->second.begin(), it->second.end());
        it = impl_->pending.erase(it);
    }
}

void PulsarSource::notify_checkpoint_aborted(CheckpointId ckpt_id) {
    // The checkpoint aborted: release its messages WITHOUT acking so Pulsar redelivers them.
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto it = impl_->pending.find(ckpt_id.value());
    if (it != impl_->pending.end()) {
        impl_->abort_drop.insert(impl_->abort_drop.end(), it->second.begin(), it->second.end());
        impl_->pending.erase(it);
    }
}

bool PulsarSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    if (!impl_->exclusive) {
        return false;  // the subscription cursor is durable server-side; unacked are redelivered
    }
    // Runs before open(), which seeks the subscription to what is read here.
    const auto v = backend.get_operator_state(op_id, kPositionKey);
    if (!v.has_value() || v->empty()) {
        return false;
    }
    impl_->position.assign(reinterpret_cast<const char*>(v->data()), v->size());
    return true;
}

void PulsarSource::close() {
    impl_->free_unacked();  // unacked on close -> Pulsar redelivers them (at-least-once)
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
