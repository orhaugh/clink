#include "clink/connectors/kafka_sink.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

#ifdef CLINK_HAS_KAFKA
#include <librdkafka/rdkafkacpp.h>
#endif

namespace clink {

#ifdef CLINK_HAS_KAFKA

namespace {

// Delivery report callback wired into the producer at config time.
// Counts successes and failures; remembers the last failure message.
class DeliveryReportImpl final : public RdKafka::DeliveryReportCb {
public:
    DeliveryReportImpl(std::atomic<std::uint64_t>& delivered,
                       std::atomic<std::uint64_t>& errors,
                       std::mutex& last_err_mu,
                       std::string& last_err,
                       Counter* delivered_metric,
                       Counter* error_metric)
        : delivered_(delivered),
          errors_(errors),
          last_err_mu_(last_err_mu),
          last_err_(last_err),
          delivered_metric_(delivered_metric),
          error_metric_(error_metric) {}

    void dr_cb(RdKafka::Message& message) override {
        if (message.err() == RdKafka::ERR_NO_ERROR) {
            delivered_.fetch_add(1, std::memory_order_relaxed);
            if (delivered_metric_ != nullptr) {
                delivered_metric_->increment();
            }
        } else {
            errors_.fetch_add(1, std::memory_order_relaxed);
            if (error_metric_ != nullptr) {
                error_metric_->increment();
            }
            std::lock_guard lock(last_err_mu_);
            last_err_ = message.errstr();
        }
    }

private:
    std::atomic<std::uint64_t>& delivered_;
    std::atomic<std::uint64_t>& errors_;
    std::mutex& last_err_mu_;
    std::string& last_err_;
    Counter* delivered_metric_;
    Counter* error_metric_;
};

}  // namespace

// Member declaration order matters here. C++ destroys members in
// reverse declaration order, so the LAST-declared member destructs
// first. We need `producer` to destruct before `dr` because
// librdkafka runs a final poll loop in the producer destructor that
// can fire dr_cb against `dr` - if `dr` is already gone we crash
// (originally observed as a SIGSEGV in librdkafka's rdk:main thread
// on Debian 13; Linux's scheduling exposes the race that macOS
// happened to hide). The counter pointers are non-owning so their
// position is irrelevant; the atomics, mutex, and string are
// referenced by the dr_cb closure, so they must outlive `dr` and
// `producer` - keep them above both.
struct KafkaSink::Impl {
    Options opts;

    Counter* delivered_metric{nullptr};
    Counter* delivery_error_metric{nullptr};
    Counter* queue_full_metric{nullptr};

    std::atomic<std::uint64_t> delivered{0};
    std::atomic<std::uint64_t> delivery_errors{0};
    mutable std::mutex last_err_mu;
    std::string last_err;

    // Declared in destruction-order-safe order: `producer` last so it
    // runs its shutdown poll while `dr` is still live.
    std::unique_ptr<DeliveryReportImpl> dr;
    std::unique_ptr<RdKafka::Producer> producer;
};

bool KafkaSink::is_real_implementation() {
    return true;
}

KafkaSink::KafkaSink(Options opts) : impl_(std::make_unique<Impl>()) {
    if (opts.brokers.empty() || opts.topic.empty()) {
        throw std::invalid_argument("KafkaSink: Options.brokers and Options.topic are required");
    }
    impl_->opts = std::move(opts);
}

KafkaSink::~KafkaSink() {
    if (impl_ && impl_->producer) {
        // Final drain + explicit teardown of the producer BEFORE its
        // delivery callback (`dr`). librdkafka's destructor runs one
        // last poll loop on shutdown that can fire dr_cb; if `dr` has
        // already destructed by then we crash on Linux. Resetting
        // producer first keeps the order safe regardless of member
        // declaration ordering.
        impl_->producer->flush(static_cast<int>(impl_->opts.flush_timeout.count()));
        impl_->producer.reset();
    }
}

void KafkaSink::open() {
    std::string err;
    auto cfg = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    auto set_or_throw = [&](const std::string& k, const std::string& v) {
        if (cfg->set(k, v, err) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("KafkaSink: config '" + k + "': " + err);
        }
    };

    set_or_throw("bootstrap.servers", impl_->opts.brokers);
    set_or_throw("client.id", impl_->opts.client_id);
    set_or_throw("acks", impl_->opts.acks);
    set_or_throw("compression.type", impl_->opts.compression_type);
    set_or_throw("linger.ms", std::to_string(impl_->opts.linger_ms.count()));
    // Transactional producer mode. Setting transactional.id
    // automatically enables idempotence and requires acks=all. We set
    // enable.idempotence explicitly so the config matches librdkafka's
    // implicit requirements regardless of broker version.
    if (!impl_->opts.transactional_id.empty()) {
        set_or_throw("transactional.id", impl_->opts.transactional_id);
        set_or_throw("enable.idempotence", "true");
        set_or_throw("acks", "all");
    }
    // Tighten message timeout so we don't leak memory waiting forever for
    // a downed broker. Capped at the user's produce_timeout (we'll fail
    // fast on backpressure rather than letting librdkafka retry forever).
    set_or_throw("message.timeout.ms", std::to_string(impl_->opts.produce_timeout.count()));

    // Wire metrics first so the delivery callback can hit them on the
    // very first poll.
    if (auto* ctx = this->runtime();
        ctx != nullptr && ctx->metrics() != nullptr && !impl_->opts.metric_prefix.empty()) {
        const std::string prefix = "kafka_sink." + impl_->opts.metric_prefix + ".";
        impl_->delivered_metric = &ctx->metrics()->counter(prefix + "delivered");
        impl_->delivery_error_metric = &ctx->metrics()->counter(prefix + "delivery_errors");
        impl_->queue_full_metric = &ctx->metrics()->counter(prefix + "queue_full_retries");
    }

    impl_->dr = std::make_unique<DeliveryReportImpl>(impl_->delivered,
                                                     impl_->delivery_errors,
                                                     impl_->last_err_mu,
                                                     impl_->last_err,
                                                     impl_->delivered_metric,
                                                     impl_->delivery_error_metric);

    if (cfg->set("dr_cb", impl_->dr.get(), err) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("KafkaSink: dr_cb: " + err);
    }

    auto* producer = RdKafka::Producer::create(cfg.get(), err);
    if (producer == nullptr) {
        throw std::runtime_error("KafkaSink: create producer failed: " + err);
    }
    impl_->producer.reset(producer);

    // Kick off transactions if configured. init_transactions
    // talks to the transaction coordinator and fences any prior
    // producer instance with the same transactional.id (the recovery-
    // safety property). begin_transaction opens the first transaction
    // so subsequent produce() calls land inside it.
    if (!impl_->opts.transactional_id.empty()) {
        const int timeout_ms = static_cast<int>(impl_->opts.produce_timeout.count());
        auto err_init = impl_->producer->init_transactions(timeout_ms);
        if (err_init) {
            throw std::runtime_error("KafkaSink: init_transactions failed: " + err_init->str());
        }
        auto err_begin = impl_->producer->begin_transaction();
        if (err_begin) {
            throw std::runtime_error("KafkaSink: begin_transaction failed: " + err_begin->str());
        }
    }
}

void KafkaSink::commit_transaction() {
    if (impl_->opts.transactional_id.empty() || !impl_->producer) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    const int timeout_ms = static_cast<int>(impl_->opts.flush_timeout.count());
    auto err = impl_->producer->commit_transaction(timeout_ms);
    if (err) {
        clink::metrics::connector::error_inc("kafka");
        throw std::runtime_error("KafkaSink: commit_transaction failed: " + err->str());
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe("kafka", static_cast<std::uint64_t>(dt));
    auto err_begin = impl_->producer->begin_transaction();
    if (err_begin) {
        throw std::runtime_error("KafkaSink: begin_transaction (post-commit) failed: " +
                                 err_begin->str());
    }
}

void KafkaSink::abort_transaction() {
    if (impl_->opts.transactional_id.empty() || !impl_->producer) {
        return;
    }
    const int timeout_ms = static_cast<int>(impl_->opts.flush_timeout.count());
    auto err = impl_->producer->abort_transaction(timeout_ms);
    if (err) {
        // Abort failure is logged but not propagated - we don't want
        // close() to throw and skip the producer teardown.
        clink::metrics::connector::error_inc("kafka");
    }
    // Re-open a fresh transaction so the sink is ready for the next
    // checkpoint, mirroring commit_transaction's post-commit begin.
    // Without this, the next on_data() (which produces inside a
    // transaction) fails because no transaction is open after an abort.
    // Best-effort, unlike commit's begin which throws: abort_transaction
    // is also the close() teardown path, which must not throw. If the
    // re-begin fails the producer is already fatally broken and the next
    // on_data will surface it; at close() the empty transaction is
    // simply abandoned when the producer is destroyed.
    auto err_begin = impl_->producer->begin_transaction();
    if (err_begin) {
        clink::metrics::connector::error_inc("kafka");
    }
}

void KafkaSink::on_data(const Batch<KafkaMessage>& batch) {
    using namespace std::chrono;
    std::uint64_t bytes_written = 0;
    for (const auto& record : batch) {
        const auto& m = record.value();
        bytes_written += m.payload.size() + (m.key.has_value() ? m.key->size() : 0);

        // Per-record partition wins; otherwise the configured fixed partition;
        // otherwise let librdkafka pick.
        std::int32_t partition = RdKafka::Topic::PARTITION_UA;
        if (m.partition >= 0) {
            partition = m.partition;
        } else if (impl_->opts.fixed_partition.has_value()) {
            partition = *impl_->opts.fixed_partition;
        }

        // Build headers via librdkafka's owning Headers class. produce()
        // takes ownership.
        RdKafka::Headers* hdrs = nullptr;
        if (!m.headers.empty()) {
            hdrs = RdKafka::Headers::create();
            for (const auto& h : m.headers) {
                hdrs->add(h.key, h.value);
            }
        }

        const void* key_ptr = nullptr;
        std::size_t key_len = 0;
        if (m.key.has_value()) {
            key_ptr = m.key->data();
            key_len = m.key->size();
        }

        // RK_MSG_COPY makes librdkafka copy the value bytes; safe to
        // const_cast the user buffer because librdkafka does not retain
        // the pointer past produce().
        const auto deadline = steady_clock::now() + impl_->opts.produce_timeout;
        while (true) {
            const auto rc = impl_->producer->produce(impl_->opts.topic,
                                                     partition,
                                                     RdKafka::Producer::RK_MSG_COPY,
                                                     const_cast<char*>(m.payload.data()),
                                                     m.payload.size(),
                                                     key_ptr,
                                                     key_len,
                                                     /*timestamp*/ 0,
                                                     hdrs,
                                                     /*opaque*/ nullptr);
            if (rc == RdKafka::ERR_NO_ERROR) {
                break;
            }
            if (rc == RdKafka::ERR__QUEUE_FULL) {
                if (impl_->queue_full_metric != nullptr) {
                    impl_->queue_full_metric->increment();
                }
                if (steady_clock::now() >= deadline) {
                    if (hdrs != nullptr) {
                        delete hdrs;
                    }
                    throw std::runtime_error(
                        "KafkaSink: producer queue full beyond produce_timeout");
                }
                impl_->producer->poll(50);
                continue;
            }
            // Anything else: produce() did not take ownership of hdrs,
            // so we must delete to avoid a leak before we throw.
            if (hdrs != nullptr) {
                delete hdrs;
            }
            throw std::runtime_error("KafkaSink: produce failed: " + RdKafka::err2str(rc));
        }
        // produce() succeeded - librdkafka now owns hdrs.
    }
    impl_->producer->poll(0);
    clink::metrics::connector::records_out_inc("kafka", batch.size());
    clink::metrics::connector::bytes_out_inc("kafka", bytes_written);
}

void KafkaSink::flush() {
    if (impl_ && impl_->producer) {
        impl_->producer->flush(static_cast<int>(impl_->opts.flush_timeout.count()));
    }
}

void KafkaSink::close() {
    if (impl_ && impl_->producer) {
        impl_->producer->flush(static_cast<int>(impl_->opts.flush_timeout.count()));
        impl_->producer.reset();
    }
}

std::uint64_t KafkaSink::delivered_count() const noexcept {
    return impl_ ? impl_->delivered.load(std::memory_order_relaxed) : 0;
}

std::uint64_t KafkaSink::delivery_error_count() const noexcept {
    return impl_ ? impl_->delivery_errors.load(std::memory_order_relaxed) : 0;
}

std::string KafkaSink::last_error() const {
    if (!impl_) {
        return {};
    }
    std::lock_guard lock(impl_->last_err_mu);
    return impl_->last_err;
}

#else

struct KafkaSink::Impl {};

bool KafkaSink::is_real_implementation() {
    return false;
}

KafkaSink::KafkaSink(Options /*opts*/) {
    throw std::runtime_error(
        "KafkaSink: built without librdkafka. Install it (e.g. "
        "`brew install librdkafka`) and reconfigure cmake.");
}

KafkaSink::~KafkaSink() = default;
void KafkaSink::open() {}
void KafkaSink::on_data(const Batch<KafkaMessage>& /*batch*/) {}
void KafkaSink::flush() {}
void KafkaSink::close() {}
void KafkaSink::commit_transaction() {}
void KafkaSink::abort_transaction() {}
std::uint64_t KafkaSink::delivered_count() const noexcept {
    return 0;
}
std::uint64_t KafkaSink::delivery_error_count() const noexcept {
    return 0;
}
std::string KafkaSink::last_error() const {
    return {};
}

#endif

}  // namespace clink
