// Two-stream interval-join bench (v1): the clink side.
//
// Pipeline:
//   orders     -> assign_ts -> key_by(user_id) --+
//                                                 | connect -> KeyedCoProcessFn -> sink
//   payments   -> assign_ts -> key_by(user_id) --+
//
// KeyedCoProcessFn keeps a single latest-order-per-key in ValueState
// (KeyedState<long, Order>). On Payment arrival, look up the latest
// Order for the key; if present, emit a Joined record. This is a
// stripped-down "interval" join (no time window enforcement in v1)
// that still exercises the production-critical paths: two-input
// keyed operator wiring, cross-stream state lookup, fan-in routing,
// and downstream emit.
//
// Future v2: add event-time window via two ListStates + timer-driven
// emit + broadcast UserProfile enrichment.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/api/pipeline.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/rocksdb/install.hpp"
#include "clink/time/event_time.hpp"

namespace bench {

using namespace std::chrono_literals;

struct Order {
    std::int64_t ts_ms{0};
    std::int64_t user_id{0};
    std::int64_t order_id{0};
    std::int64_t amount_cents{0};
};

struct Payment {
    std::int64_t ts_ms{0};
    std::int64_t user_id{0};
    std::int64_t payment_id{0};
    std::int64_t paid_cents{0};
};

struct Joined {
    std::int64_t user_id{0};
    std::int64_t order_id{0};
    std::int64_t payment_id{0};
    std::int64_t amount_cents{0};
    std::int64_t paid_cents{0};
};

inline void put_i64_le(std::byte*& p, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
    }
}
inline std::int64_t read_i64_le(std::span<const std::byte> b, std::size_t& pos) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(b[pos + i])) << (i * 8);
    }
    pos += 8;
    return static_cast<std::int64_t>(u);
}

inline clink::Codec<Order> order_codec() {
    return clink::Codec<Order>{.encode =
                                   [](const Order& o) {
                                       std::vector<std::byte> out(32);
                                       std::byte* p = out.data();
                                       put_i64_le(p, o.ts_ms);
                                       put_i64_le(p, o.user_id);
                                       put_i64_le(p, o.order_id);
                                       put_i64_le(p, o.amount_cents);
                                       return out;
                                   },
                               .decode = [](std::span<const std::byte> b) -> std::optional<Order> {
                                   std::size_t pos = 0;
                                   Order o;
                                   o.ts_ms = read_i64_le(b, pos);
                                   o.user_id = read_i64_le(b, pos);
                                   o.order_id = read_i64_le(b, pos);
                                   o.amount_cents = read_i64_le(b, pos);
                                   return o;
                               }};
}

inline clink::Codec<Payment> payment_codec() {
    return clink::Codec<Payment>{
        .encode =
            [](const Payment& p) {
                std::vector<std::byte> out(32);
                std::byte* q = out.data();
                put_i64_le(q, p.ts_ms);
                put_i64_le(q, p.user_id);
                put_i64_le(q, p.payment_id);
                put_i64_le(q, p.paid_cents);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<Payment> {
            std::size_t pos = 0;
            Payment p;
            p.ts_ms = read_i64_le(b, pos);
            p.user_id = read_i64_le(b, pos);
            p.payment_id = read_i64_le(b, pos);
            p.paid_cents = read_i64_le(b, pos);
            return p;
        }};
}

inline clink::Codec<Joined> joined_codec() {
    return clink::Codec<Joined>{
        .encode =
            [](const Joined& j) {
                std::vector<std::byte> out(40);
                std::byte* p = out.data();
                put_i64_le(p, j.user_id);
                put_i64_le(p, j.order_id);
                put_i64_le(p, j.payment_id);
                put_i64_le(p, j.amount_cents);
                put_i64_le(p, j.paid_cents);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<Joined> {
            std::size_t pos = 0;
            Joined j;
            j.user_id = read_i64_le(b, pos);
            j.order_id = read_i64_le(b, pos);
            j.payment_id = read_i64_le(b, pos);
            j.amount_cents = read_i64_le(b, pos);
            j.paid_cents = read_i64_le(b, pos);
            return j;
        }};
}

// Synthetic sources. Orders + Payments are interleaved with the same
// key distribution and a small phase offset so Payment.ts_ms is
// always slightly after the matching Order.ts_ms.
constexpr std::size_t kBatchSize = 256;

class OrderSource final : public clink::Source<Order> {
public:
    OrderSource(std::int64_t total, std::int64_t keys, std::int64_t windows)
        : total_(total), keys_(keys), windows_(windows) {}

    bool produce(clink::Emitter<Order>& out) override {
        if (this->cancelled() || cursor_ >= total_) {
            if (!eos_sent_) {
                out.emit_watermark(clink::Watermark::max());
                eos_sent_ = true;
            }
            return false;
        }
        const std::int64_t end =
            std::min<std::int64_t>(cursor_ + static_cast<std::int64_t>(kBatchSize), total_);
        clink::Batch<Order> batch;
        batch.reserve(static_cast<std::size_t>(end - cursor_));
        const double step_ms = static_cast<double>(windows_ * 1000) / static_cast<double>(total_);
        for (std::int64_t i = cursor_; i < end; ++i) {
            Order o;
            o.ts_ms = static_cast<std::int64_t>(static_cast<double>(i) * step_ms);
            o.user_id = i % keys_;
            o.order_id = i;
            o.amount_cents = ((i % 1000) + 1) * 100;
            clink::Record<Order> r{std::move(o), clink::EventTime{o.ts_ms}};
            batch.push(std::move(r));
        }
        cursor_ = end;
        out.emit_data(std::move(batch));
        return true;
    }

    std::string name() const override { return "orders_source"; }

private:
    std::int64_t total_;
    std::int64_t keys_;
    std::int64_t windows_;
    std::int64_t cursor_{0};
    bool eos_sent_{false};
};

class PaymentSource final : public clink::Source<Payment> {
public:
    PaymentSource(std::int64_t total, std::int64_t keys, std::int64_t windows)
        : total_(total), keys_(keys), windows_(windows) {}

    bool produce(clink::Emitter<Payment>& out) override {
        if (this->cancelled() || cursor_ >= total_) {
            if (!eos_sent_) {
                out.emit_watermark(clink::Watermark::max());
                eos_sent_ = true;
            }
            return false;
        }
        const std::int64_t end =
            std::min<std::int64_t>(cursor_ + static_cast<std::int64_t>(kBatchSize), total_);
        clink::Batch<Payment> batch;
        batch.reserve(static_cast<std::size_t>(end - cursor_));
        // Payments lag orders by 50ms so the latest-order-state has been
        // populated by the time a Payment arrives for the same key.
        const double step_ms = static_cast<double>(windows_ * 1000) / static_cast<double>(total_);
        for (std::int64_t i = cursor_; i < end; ++i) {
            Payment p;
            p.ts_ms = static_cast<std::int64_t>(static_cast<double>(i) * step_ms) + 50;
            p.user_id = i % keys_;
            p.payment_id = i;
            p.paid_cents = ((i % 1000) + 1) * 100;
            clink::Record<Payment> r{std::move(p), clink::EventTime{p.ts_ms}};
            batch.push(std::move(r));
        }
        cursor_ = end;
        out.emit_data(std::move(batch));
        return true;
    }

    std::string name() const override { return "payments_source"; }

private:
    std::int64_t total_;
    std::int64_t keys_;
    std::int64_t windows_;
    std::int64_t cursor_{0};
    bool eos_sent_{false};
};

// KeyedCoProcessFunction: latest-order-state per key; on Payment,
// look up latest Order and emit Joined.
class JoinFn final : public clink::KeyedCoProcessFunction<std::int64_t, Order, Payment, Joined> {
public:
    void open(clink::RuntimeContext& ctx) override {
        latest_order_ = std::make_unique<clink::KeyedState<std::int64_t, Order>>(
            ctx.keyed_state<std::int64_t, Order>(
                "latest_order", clink::int64_codec(), order_codec()));
    }

    void process_element1(const Order& o,
                          clink::ProcessFunctionContext<Joined>& /*ctx*/,
                          clink::Collector<Joined>& /*out*/) override {
        latest_order_->put(current_key(), o);
    }

    void process_element2(const Payment& p,
                          clink::ProcessFunctionContext<Joined>& /*ctx*/,
                          clink::Collector<Joined>& out) override {
        auto o = latest_order_->get(current_key());
        if (!o.has_value()) {
            return;
        }
        Joined j;
        j.user_id = current_key();
        j.order_id = o->order_id;
        j.payment_id = p.payment_id;
        j.amount_cents = o->amount_cents;
        j.paid_cents = p.paid_cents;
        out.collect(j);
    }

    std::string name() const override { return "join_fn"; }

private:
    std::unique_ptr<clink::KeyedState<std::int64_t, Order>> latest_order_;
};

class CountingSink final : public clink::Sink<Joined> {
public:
    explicit CountingSink(std::string label) : label_(std::move(label)) {}

    void on_data(const clink::Batch<Joined>& b) override {
        count_ += static_cast<std::int64_t>(b.size());
    }

    void close() override {
        std::fprintf(stderr,
                     "[%s] sink final count: %lld\n",
                     label_.c_str(),
                     static_cast<long long>(count_));
    }

    std::string name() const override { return "join_sink"; }

private:
    std::string label_;
    std::int64_t count_{0};
};

void define_job(clink::api::Pipeline& env) {
    clink::rocksdb::install();
    const auto envv_int64 = [](const char* k, std::int64_t def) -> std::int64_t {
        const char* v = std::getenv(k);
        return v ? std::atoll(v) : def;
    };

    const std::int64_t orders = envv_int64("BENCH_ORDERS", 5'000'000);
    const std::int64_t payments = envv_int64("BENCH_PAYMENTS", 5'000'000);
    const std::int64_t keys = envv_int64("BENCH_KEYS", 1000);
    const std::int64_t windows = envv_int64("BENCH_WINDOWS", 100);

    env.registry().register_type<Order>("bench.join.order", order_codec());
    env.registry().register_type<Payment>("bench.join.payment", payment_codec());
    env.registry().register_type<Joined>("bench.join.joined", joined_codec());

    // Register the user-id extractor under one shared name for both
    // sides. KeyedDataStream::connect_process enforces that both
    // upstreams use the same key-extractor name (so subtask routing
    // is consistent across the join's two inputs); inline lambdas via
    // .key_by(fn) mint distinct names and break connect_process.
    env.registry().register_key_extractor<Order>("user_id",
                                                 [](const Order& o) { return o.user_id; });
    env.registry().register_key_extractor<Payment>("user_id",
                                                   [](const Payment& p) { return p.user_id; });

    env.registry().register_source<Order>(
        "bench.join.order_source", [orders, keys, windows](const clink::plugin::BuildContext&) {
            return std::make_shared<OrderSource>(orders, keys, windows);
        });
    env.registry().register_source<Payment>(
        "bench.join.payment_source", [payments, keys, windows](const clink::plugin::BuildContext&) {
            return std::make_shared<PaymentSource>(payments, keys, windows);
        });
    env.registry().register_sink<Joined>(
        "bench.join.sink", [](const clink::plugin::BuildContext& bctx) {
            return std::make_shared<CountingSink>("clink-subtask-" +
                                                  std::to_string(bctx.subtask_idx));
        });

    clink::api::SourceDescriptor order_src;
    order_src.op_type = "bench.join.order_source";
    order_src.channel_type = "bench.join.order";
    order_src.parallelism = 1;

    clink::api::SourceDescriptor payment_src;
    payment_src.op_type = "bench.join.payment_source";
    payment_src.channel_type = "bench.join.payment";
    payment_src.parallelism = 1;

    clink::api::SinkDescriptor sink_desc;
    sink_desc.op_type = "bench.join.sink";
    sink_desc.channel_type = "bench.join.joined";
    sink_desc.parallelism = 1;

    auto fn = std::make_shared<JoinFn>();

    auto orders_keyed =
        env.source<Order>(order_src)
            .assign_timestamps_monotonic([](const Order& o) { return clink::EventTime{o.ts_ms}; })
            .key_by(std::string{"user_id"});
    auto payments_keyed =
        env.source<Payment>(payment_src)
            .assign_timestamps_monotonic([](const Payment& p) { return clink::EventTime{p.ts_ms}; })
            .key_by(std::string{"user_id"});

    orders_keyed
        .connect_process<Payment, std::int64_t, Joined>(
            payments_keyed,
            fn,
            [](const Order& o) { return o.user_id; },
            [](const Payment& p) { return p.user_id; })
        .sink(sink_desc);
}

}  // namespace bench

CLINK_REGISTER_JOB("interval-join-bench-pipeline",
                   "1.0",
                   "orders+payments -> connect_process(KeyedCoProcessFunction) -> sink",
                   bench::define_job);
