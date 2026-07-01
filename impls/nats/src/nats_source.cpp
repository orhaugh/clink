#include "clink/nats/nats_source.hpp"

#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"

#include "nats_conn.hpp"

namespace clink::nats {

namespace {
constexpr const char* kLabel = "nats";
}  // namespace

struct NatsSource::Impl {
    Options opts;
    natsConnection* conn{nullptr};
    jsCtx* js{nullptr};
    natsSubscription* sub{nullptr};
    std::vector<natsMsg*> held;  // emitted since the last barrier (produce thread only)
    // Cross-thread ack bookkeeping (guarded by mu): snapshot_offset (produce thread) buckets
    // `held` under a checkpoint id; notify_checkpoint_* (commit-dispatch thread) move buckets into
    // ack_ready / abort_drop; produce() (produce thread) drains those, keeping every natsMsg_Ack /
    // natsMsg_Destroy on the produce thread (the subscription is not thread-safe).
    std::mutex mu;
    std::map<std::uint64_t, std::vector<natsMsg*>> pending;  // ckpt id -> captured messages
    std::vector<natsMsg*> ack_ready;                         // committed: ack then destroy
    std::vector<natsMsg*> abort_drop;  // aborted: destroy without ack (-> JetStream redelivers)
    explicit Impl(Options o) : opts(std::move(o)) {}

    // Destroy every not-yet-acked message (held + bucketed + queued) WITHOUT acking, so JetStream
    // redelivers them after AckWait (at-least-once). Called from close()/dtor after the runner
    // thread has stopped, so no locking is needed.
    void destroy_unacked() {
        auto wipe = [](std::vector<natsMsg*>& v) {
            for (natsMsg* m : v) {
                natsMsg_Destroy(m);
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

NatsSource::NatsSource(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.subject.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'subject' is required");
    }
}

NatsSource::~NatsSource() {
    impl_->destroy_unacked();
    natsSubscription_Destroy(impl_->sub);
    jsCtx_Destroy(impl_->js);  // destroy JetStream ctx BEFORE the connection (nats.c requirement)
    natsConnection_Destroy(impl_->conn);
}

void NatsSource::open() {
    impl_->conn = detail::connect(impl_->opts.conn, impl_->opts.name);

    jsOptions jsOpts;
    detail::check(jsOptions_Init(&jsOpts), impl_->opts.name + ": jsOptions_Init");
    detail::check(natsConnection_JetStream(&impl_->js, impl_->conn, &jsOpts),
                  impl_->opts.name + ": JetStream ctx");

    jsSubOptions subOpts;
    detail::check(jsSubOptions_Init(&subOpts), impl_->opts.name + ": jsSubOptions_Init");
    if (!impl_->opts.stream.empty()) {
        subOpts.Stream = impl_->opts.stream.c_str();
    }
    // Explicit per-message ack so parallel subtasks sharing the durable each ack only their own
    // messages (AckAll would over-ack across a shared consumer). AckWait/MaxAckPending bound and
    // pace redelivery + the held-unacked buffer.
    subOpts.Config.AckPolicy = js_AckExplicit;
    subOpts.Config.AckWait =
        static_cast<int64_t>(impl_->opts.ack_wait.count()) * 1000000000LL;  // seconds -> ns
    subOpts.Config.MaxAckPending = impl_->opts.max_ack_pending;

    jsErrCode jerr = static_cast<jsErrCode>(0);
    const natsStatus s = js_PullSubscribe(&impl_->sub,
                                          impl_->js,
                                          impl_->opts.subject.c_str(),
                                          impl_->opts.durable.c_str(),
                                          &jsOpts,
                                          &subOpts,
                                          &jerr);
    detail::check(s, impl_->opts.name + ": js_PullSubscribe");
}

bool NatsSource::produce(Emitter<std::string>& out) {
    // Ack/release messages whose checkpoint resolved since the last turn. Done here (the produce
    // thread) so every natsMsg_Ack stays on the single non-thread-safe subscription.
    {
        std::vector<natsMsg*> to_ack;
        std::vector<natsMsg*> to_drop;
        {
            std::lock_guard<std::mutex> lk(impl_->mu);
            to_ack.swap(impl_->ack_ready);
            to_drop.swap(impl_->abort_drop);
        }
        natsStatus first_err = NATS_OK;
        for (natsMsg* m : to_ack) {
            const natsStatus s = natsMsg_Ack(m, nullptr);
            if (s != NATS_OK && first_err == NATS_OK) {
                first_err = s;
            }
            natsMsg_Destroy(m);
        }
        for (natsMsg* m : to_drop) {
            natsMsg_Destroy(m);  // no ack -> JetStream redelivers after AckWait
        }
        if (first_err != NATS_OK) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(impl_->opts.name + ": ack: " + natsStatus_GetText(first_err));
        }
    }
    natsMsgList list;
    list.Msgs = nullptr;
    list.Count = 0;
    jsErrCode jerr = static_cast<jsErrCode>(0);
    const natsStatus s = natsSubscription_Fetch(
        &list, impl_->sub, impl_->opts.batch, impl_->opts.fetch_timeout.count(), &jerr);
    if (s == NATS_TIMEOUT) {
        return !this->cancelled();  // no messages within the fetch window
    }
    if (s != NATS_OK) {
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(impl_->opts.name + ": fetch: " + natsStatus_GetText(s));
    }

    Batch<std::string> batch;
    std::size_t bytes = 0;
    for (int i = 0; i < list.Count; ++i) {
        natsMsg* m = list.Msgs[i];
        const int len = natsMsg_GetDataLength(m);
        std::string body(natsMsg_GetData(m), static_cast<std::size_t>(len < 0 ? 0 : len));
        bytes += body.size();
        batch.emplace(std::move(body));
        impl_->held.push_back(m);  // keep the message alive until the barrier ack
        list.Msgs[i] = nullptr;    // transfer ownership out of the list
    }
    natsMsgList_Destroy(&list);  // frees the array; the nulled entries are skipped

    if (!batch.empty()) {
        const std::size_t n = batch.size();
        out.emit_data(std::move(batch));
        clink::metrics::connector::records_in_inc(kLabel, n);
        clink::metrics::connector::bytes_in_inc(kLabel, bytes);
    }
    return !this->cancelled();
}

void NatsSource::snapshot_offset(StateBackend& /*backend*/,
                                 OperatorId /*op_id*/,
                                 CheckpointId ckpt_id) {
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

void NatsSource::notify_checkpoint_complete(CheckpointId ckpt_id) {
    // Every checkpoint up to the committed id is durable: queue its messages for ack on the
    // produce() thread.
    std::lock_guard<std::mutex> lk(impl_->mu);
    for (auto it = impl_->pending.begin();
         it != impl_->pending.end() && it->first <= ckpt_id.value();) {
        impl_->ack_ready.insert(impl_->ack_ready.end(), it->second.begin(), it->second.end());
        it = impl_->pending.erase(it);
    }
}

void NatsSource::notify_checkpoint_aborted(CheckpointId ckpt_id) {
    // The checkpoint aborted: release its messages WITHOUT acking so JetStream redelivers them.
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto it = impl_->pending.find(ckpt_id.value());
    if (it != impl_->pending.end()) {
        impl_->abort_drop.insert(impl_->abort_drop.end(), it->second.begin(), it->second.end());
        impl_->pending.erase(it);
    }
}

bool NatsSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // JetStream tracks the durable consumer's ack floor; unacked are redelivered
}

void NatsSource::close() {
    impl_->destroy_unacked();  // unacked on close -> JetStream redelivers them (at-least-once)
    natsSubscription_Destroy(impl_->sub);
    impl_->sub = nullptr;
    jsCtx_Destroy(impl_->js);
    impl_->js = nullptr;
    natsConnection_Destroy(impl_->conn);
    impl_->conn = nullptr;
}

}  // namespace clink::nats
