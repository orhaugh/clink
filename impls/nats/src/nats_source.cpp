#include "clink/nats/nats_source.hpp"

#include <cstdint>
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
    std::vector<natsMsg*> held;  // emitted-but-unacked messages, ack'd at the next barrier
    explicit Impl(Options o) : opts(std::move(o)) {}
};

NatsSource::NatsSource(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.subject.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'subject' is required");
    }
}

NatsSource::~NatsSource() {
    for (natsMsg* m : impl_->held) {
        natsMsg_Destroy(m);
    }
    impl_->held.clear();
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
                                 CheckpointId /*ckpt_id*/) {
    // Ack + release every message emitted since the last barrier (at-least-once). Runs on the
    // produce() thread, so it is serialised with Fetch on the single subscription.
    natsStatus first_err = NATS_OK;
    for (natsMsg* m : impl_->held) {
        const natsStatus s = natsMsg_Ack(m, nullptr);
        if (s != NATS_OK && first_err == NATS_OK) {
            first_err = s;
        }
        natsMsg_Destroy(m);
    }
    impl_->held.clear();
    if (first_err != NATS_OK) {
        clink::metrics::connector::error_inc(kLabel, "source");
        throw std::runtime_error(impl_->opts.name + ": ack: " + natsStatus_GetText(first_err));
    }
}

bool NatsSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // JetStream tracks the durable consumer's ack floor; unacked are redelivered
}

void NatsSource::close() {
    for (natsMsg* m : impl_->held) {
        natsMsg_Destroy(m);  // unacked on close -> JetStream redelivers them (at-least-once)
    }
    impl_->held.clear();
    natsSubscription_Destroy(impl_->sub);
    impl_->sub = nullptr;
    jsCtx_Destroy(impl_->js);
    impl_->js = nullptr;
    natsConnection_Destroy(impl_->conn);
    impl_->conn = nullptr;
}

}  // namespace clink::nats
