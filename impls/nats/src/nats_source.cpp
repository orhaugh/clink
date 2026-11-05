#include "clink/nats/nats_source.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "nats_conn.hpp"

namespace clink::nats {

namespace {
constexpr const char* kLabel = "nats";
}  // namespace

struct NatsSource::Impl {
    using Clock = std::chrono::steady_clock;
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
    std::vector<natsMsg*> abort_drop;  // aborted: never acked (-> JetStream redelivers)
    // Produce thread only. Every message held or bucketed and not yet resolved, and when its
    // AckWait is next due to be reset with natsMsg_InProgress (at half AckWait, for at most
    // max_lease from receipt).
    struct Lease {
        Clock::time_point received;
        Clock::time_point extend_at;
    };
    std::unordered_map<natsMsg*, Lease> leases;
    // An aborted checkpoint's messages, kept until their AckWait has run out, so close() can hand
    // them back at once rather than leave them to the timer (produce thread only).
    std::vector<std::pair<natsMsg*, Clock::time_point>> released;
    explicit Impl(Options o) : opts(std::move(o)) {}

    // Destroy every message still held, WITHOUT acking or handing it back, so JetStream redelivers
    // it after AckWait: a crash. Runs from the destructor, after the runner thread has stopped.
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
        for (auto& [m, _] : released) {
            natsMsg_Destroy(m);
        }
        released.clear();
        leases.clear();
    }

    // Reset the AckWait of every held message that is due, up to max_lease from receipt; forget
    // released messages whose AckWait has run out. Produce thread. A failed reset is not an
    // error: its cost is a redelivery, a duplicate rather than a loss.
    void extend_leases() {
        const auto now = Clock::now();
        const auto half = std::chrono::duration_cast<Clock::duration>(opts.ack_wait) / 2;
        for (auto it = leases.begin(); it != leases.end();) {
            if (now < it->second.extend_at) {
                ++it;
                continue;
            }
            if (now - it->second.received >= opts.max_lease) {
                it = leases.erase(it);  // no longer extended: JetStream redelivers it
                continue;
            }
            (void)natsMsg_InProgress(it->first, nullptr);
            it->second.extend_at = now + half;
            ++it;
        }
        for (auto it = released.begin(); it != released.end();) {
            if (now >= it->second) {
                natsMsg_Destroy(it->first);
                it = released.erase(it);
            } else {
                ++it;
            }
        }
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
            impl_->leases.erase(m);
            natsMsg_Destroy(m);
        }
        // Never acked: JetStream redelivers each once its AckWait, last reset by the lease, runs
        // out. Kept until then for close() to hand back.
        const auto now = Impl::Clock::now();
        const auto half =
            std::chrono::duration_cast<Impl::Clock::duration>(impl_->opts.ack_wait) / 2;
        for (natsMsg* m : to_drop) {
            auto lease = impl_->leases.find(m);
            const auto expires =
                lease != impl_->leases.end() ? lease->second.extend_at + half : now + 2 * half;
            if (lease != impl_->leases.end()) {
                impl_->leases.erase(lease);
            }
            impl_->released.emplace_back(m, expires);
        }
        if (first_err != NATS_OK) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(impl_->opts.name + ": ack: " + natsStatus_GetText(first_err));
        }
    }
    impl_->extend_leases();
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
        impl_->held.push_back(m);  // keep the message alive until its checkpoint resolves
        const auto received = Impl::Clock::now();
        impl_->leases[m] = Impl::Lease{
            received,
            received + std::chrono::duration_cast<Impl::Clock::duration>(impl_->opts.ack_wait) / 2};
        list.Msgs[i] = nullptr;  // transfer ownership out of the list
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
    // Acknowledge what completed checkpoints cover, and hand every other message back (Nak) so
    // JetStream redelivers it at once rather than after AckWait: held, bucketed under a checkpoint
    // that has not completed, or released by an abort. Whether this is a cancel, a failure or the
    // end of a stop, no message a completed checkpoint does not cover is acknowledged. Best
    // effort: what fails here is redelivered once its AckWait runs out.
    std::vector<natsMsg*> to_ack;
    std::vector<natsMsg*> to_return;
    {
        std::lock_guard<std::mutex> lk(impl_->mu);
        to_ack.swap(impl_->ack_ready);
        to_return.swap(impl_->abort_drop);
        for (auto& [ckpt, msgs] : impl_->pending) {
            to_return.insert(to_return.end(), msgs.begin(), msgs.end());
        }
        impl_->pending.clear();
    }
    to_return.insert(to_return.end(), impl_->held.begin(), impl_->held.end());
    impl_->held.clear();
    for (auto& [m, _] : impl_->released) {
        to_return.push_back(m);
    }
    impl_->released.clear();
    impl_->leases.clear();
    std::size_t failed = 0;
    for (natsMsg* m : to_ack) {
        failed += natsMsg_Ack(m, nullptr) != NATS_OK ? 1 : 0;
        natsMsg_Destroy(m);
    }
    if (failed > 0) {
        clink::log::warn("nats.source",
                         impl_->opts.name + ": " + std::to_string(failed) +
                             " acks at close failed; those messages are redelivered");
    }
    for (natsMsg* m : to_return) {
        (void)natsMsg_Nak(m, nullptr);
        natsMsg_Destroy(m);
    }
    natsSubscription_Destroy(impl_->sub);
    impl_->sub = nullptr;
    jsCtx_Destroy(impl_->js);
    impl_->js = nullptr;
    natsConnection_Destroy(impl_->conn);
    impl_->conn = nullptr;
}

}  // namespace clink::nats
