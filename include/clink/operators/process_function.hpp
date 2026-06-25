#pragma once

// ProcessFunction APIs - KeyedProcessFunction /
// ProcessFunction analogues. These are the high-level, "do anything"
// user APIs that bundle:
//
//   * Per-element processing with optional event time
//   * Direct emit-many via a Collector
//   * Processing-time AND event-time timer registration + callback
//   * Side output emission
//   * (Keyed variant) current-key access for state lookups
//
// They sit on top of the lower-level Operator<I, O> base and the
// existing TimerService / RuntimeContext infrastructure - there's
// no new runtime machinery, only a sugar layer that matches the
// shape users expect.
//
// Use via:
//
//   class MyFn : public KeyedProcessFunction<int64_t, Click, Output> {
//   public:
//     void process_element(const Click& v,
//                          ProcessFunctionContext<Output>& ctx,
//                          Collector<Output>& out) override {
//       if (something) {
//         out.collect(Output{...});
//         ctx.timer_service()->register_event_time_timer(
//             ctx.timestamp().value_or(EventTime{0}).millis() + 60'000);
//       }
//     }
//     void on_timer(int64_t ts, OnTimerContext<Output>& ctx,
//                   Collector<Output>& out) override {
//       out.collect(Output{...});
//     }
//   };
//   keyed_stream.process<Output>(std::make_shared<MyFn>());

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/hash_map.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/timer_service.hpp"
#include "clink/state/broadcast_state.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// ProcessFunction time domain (for OnTimerContext::time_domain).
// We call this TimeDomain; the names match so user code reads
// idiomatically.
enum class TimeDomain : std::uint8_t {
    ProcessingTime,
    EventTime,
};

// Collector<O> is the user-facing emit handle inside process_element
// and on_timer. It wraps the operator runner's Emitter<O> so the user
// never sees the StreamElement / Batch layer directly.
template <typename O>
class Collector {
public:
    explicit Collector(Emitter<O>* emitter) : emitter_(emitter) {}

    void collect(O value) {
        Batch<O> b;
        b.push(Record<O>{std::move(value)});
        emitter_->emit_data(std::move(b));
    }

    void collect_with_timestamp(O value, EventTime t) {
        Batch<O> b;
        b.push(Record<O>{std::move(value), t});
        emitter_->emit_data(std::move(b));
    }

private:
    Emitter<O>* emitter_;
};

// Context handed to ProcessFunction::process_element. Bundles the
// current record's timestamp, the operator's timer service, and a
// way to grab side-output emitters. The ProcessFunctionAdapter wires
// these up freshly for each process call.
template <typename O>
class ProcessFunctionContext {
public:
    ProcessFunctionContext(std::optional<EventTime> ts,
                           TimerService* timers,
                           RuntimeContext* rt) noexcept
        : ts_(ts), timers_(timers), rt_(rt) {}

    // The current record's event-time, or nullopt for records the
    // source didn't tag. Available to both process_element (the
    // current record's stamp) and on_timer (the timer's firing time).
    std::optional<EventTime> timestamp() const noexcept { return ts_; }

    // Register processing-time / event-time timers. Timers are
    // keyed by an opaque string; KeyedProcessFunction subclasses
    // typically pass the current key so on_timer can route by key.
    TimerService* timer_service() noexcept { return timers_; }

    // Emit to a named side output. The tag must have been registered
    // on this operator's stage via Dag::side_output() (or via the
    // fluent API's equivalent).
    template <typename U>
    Emitter<U> side_output(const OutputTag<U>& tag) {
        return rt_->template side_output<U>(tag);
    }

protected:
    std::optional<EventTime> ts_;
    TimerService* timers_{nullptr};
    RuntimeContext* rt_{nullptr};
};

// Same as ProcessFunctionContext but adds the firing timer's time
// domain (event-time vs processing-time) so user code can branch on
// it without parsing keys.
template <typename O>
class OnTimerContext : public ProcessFunctionContext<O> {
public:
    OnTimerContext(EventTime fired_at,
                   TimeDomain domain,
                   TimerService* timers,
                   RuntimeContext* rt) noexcept
        : ProcessFunctionContext<O>(fired_at, timers, rt), domain_(domain) {}

    TimeDomain time_domain() const noexcept { return domain_; }

private:
    TimeDomain domain_;
};

// Non-keyed ProcessFunction. Users override process_element (and
// optionally on_timer). Open/close hooks mirror the other operator
// templates. Side outputs, timer registration, and the collector
// are all reachable through ctx / out.
template <typename I, typename O>
class ProcessFunction {
public:
    virtual ~ProcessFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    virtual void process_element(const I& value,
                                 ProcessFunctionContext<O>& ctx,
                                 Collector<O>& out) = 0;

    virtual void on_timer(std::int64_t /*timestamp_ms*/,
                          OnTimerContext<O>& /*ctx*/,
                          Collector<O>& /*out*/) {}

    // Called once after the input is drained, before close(). Same
    // hook Operator<>::flush exposes; lets the user emit any buffered
    // tail records at end-of-stream.
    virtual void flush(Collector<O>& /*out*/) {}

    virtual std::string name() const { return "process_function"; }
};

// Keyed variant. Same surface plus current_key() which the adapter
// maintains via the user-supplied key extractor. State access
// (RuntimeContext::keyed_state) lives on ctx via inheritance -
// users keep state slots as member fields of the function class
// and consult them inside process_element / on_timer.
template <typename K, typename I, typename O>
class KeyedProcessFunction : public ProcessFunction<I, O> {
public:
    // The key of the record currently being processed. Inside
    // on_timer, the key of the timer that fired. Adapter sets this
    // immediately before each call.
    const K& current_key() const noexcept { return current_key_; }

    void set_current_key_(K k) { current_key_ = std::move(k); }

    std::string name() const override { return "keyed_process_function"; }

private:
    K current_key_{};
};

namespace detail {

// Operator<I, O> adapter that drives a ProcessFunction. The runner
// hands us each input record; we call user's process_element with a
// fresh Context + Collector. Watermarks fire event-time timers via
// the default Operator path (we override on_watermark just to
// preserve forwarding semantics). Processing-time timers route
// through on_processing_time_timer.
template <typename I, typename O>
class ProcessFunctionAdapter : public Operator<I, O> {
public:
    explicit ProcessFunctionAdapter(std::shared_ptr<ProcessFunction<I, O>> fn,
                                    std::string name = "process_function")
        : fn_(std::move(fn)), name_(std::move(name)) {}

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
        }
    }

    void close() override { fn_->close(); }

    void process(const StreamElement<I>& element, Emitter<O>& out) override {
        if (element.is_data()) {
            Collector<O> col(&out);
            for (const auto& rec : element.as_data()) {
                ProcessFunctionContext<O> ctx(
                    rec.event_time(), this->runtime()->timer_service(), this->runtime());
                pre_dispatch_(rec);
                fn_->process_element(rec.value(), ctx, col);
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_processing_time_timer(std::int64_t ts,
                                  const std::string& key,
                                  Emitter<O>& out) override {
        Collector<O> col(&out);
        OnTimerContext<O> ctx(EventTime{ts},
                              TimeDomain::ProcessingTime,
                              this->runtime()->timer_service(),
                              this->runtime());
        pre_dispatch_timer_(key);
        fn_->on_timer(ts, ctx, col);
    }

    void on_event_time_timer(std::int64_t ts, const std::string& key, Emitter<O>& out) override {
        Collector<O> col(&out);
        OnTimerContext<O> ctx(EventTime{ts},
                              TimeDomain::EventTime,
                              this->runtime()->timer_service(),
                              this->runtime());
        pre_dispatch_timer_(key);
        fn_->on_timer(ts, ctx, col);
    }

    void flush(Emitter<O>& out) override {
        Collector<O> col(&out);
        fn_->flush(col);
    }

    std::string name() const override { return name_; }

    // The user's on_timer freely reads/writes keyed state, so this operator
    // fires state-touching timers. Conservatively true even if a given function
    // registers no timers: it gates the (currently unsupported) async +
    // state-touching-timer combination at runner start. See
    // Operator::fires_state_touching_timers.
    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }

private:
    // Hooks for the keyed adapter to set the current-key on the
    // function. Non-keyed adapter leaves them no-ops.
    virtual void pre_dispatch_(const Record<I>& /*rec*/) {}
    virtual void pre_dispatch_timer_(const std::string& /*key*/) {}

    std::shared_ptr<ProcessFunction<I, O>> fn_;
    std::string name_;
};

// Keyed adapter: same shape but also maintains the function's
// current_key via the user-supplied extractor. The extractor returns
// the same key type the user's KeyedProcessFunction was templated on.
template <typename K, typename I, typename O>
class KeyedProcessFunctionAdapter : public ProcessFunctionAdapter<I, O> {
public:
    using KeyFn = std::function<K(const I&)>;
    using KeyFromTimerKey = std::function<K(const std::string&)>;

    KeyedProcessFunctionAdapter(std::shared_ptr<KeyedProcessFunction<K, I, O>> fn,
                                KeyFn key_fn,
                                KeyFromTimerKey timer_key_fn = nullptr,
                                std::string name = "keyed_process_function")
        : ProcessFunctionAdapter<I, O>(fn, std::move(name)),
          keyed_fn_(std::move(fn)),
          key_fn_(std::move(key_fn)),
          timer_key_fn_(std::move(timer_key_fn)) {}

private:
    void pre_dispatch_(const Record<I>& rec) override {
        keyed_fn_->set_current_key_(key_fn_(rec.value()));
    }

    void pre_dispatch_timer_(const std::string& key) override {
        // If the user supplied a timer-key-to-K decoder, recover the
        // typed key; otherwise leave whatever the function's
        // current_key was last set to. The typical pattern is to
        // register timers with the same string-encoded key the
        // KeyedState system uses, so a decoder is easy to wire.
        if (timer_key_fn_) {
            keyed_fn_->set_current_key_(timer_key_fn_(key));
        }
    }

    std::shared_ptr<KeyedProcessFunction<K, I, O>> keyed_fn_;
    KeyFn key_fn_;
    KeyFromTimerKey timer_key_fn_;
};

}  // namespace detail

// Two-input ProcessFunction. CoProcessFunction analogue. The
// user implements process_element1 / process_element2 for the two
// input streams; both share the same Collector<O> output. Timers,
// side outputs, and the (optional) keyed-current-key all flow
// through the same Context types.
template <typename I1, typename I2, typename O>
class CoProcessFunction {
public:
    virtual ~CoProcessFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    virtual void process_element1(const I1& value,
                                  ProcessFunctionContext<O>& ctx,
                                  Collector<O>& out) = 0;
    virtual void process_element2(const I2& value,
                                  ProcessFunctionContext<O>& ctx,
                                  Collector<O>& out) = 0;

    virtual void on_timer(std::int64_t /*timestamp_ms*/,
                          OnTimerContext<O>& /*ctx*/,
                          Collector<O>& /*out*/) {}

    virtual void flush(Collector<O>& /*out*/) {}

    virtual std::string name() const { return "co_process_function"; }
};

// Keyed CoProcessFunction. Provides current_key() during both
// process_element1 / process_element2 / on_timer. The adapter
// updates the key from each side's record before dispatch using a
// per-side extractor (keyed connect: both streams are
// keyed by the same K).
template <typename K, typename I1, typename I2, typename O>
class KeyedCoProcessFunction : public CoProcessFunction<I1, I2, O> {
public:
    const K& current_key() const noexcept { return current_key_; }
    void set_current_key_(K k) { current_key_ = std::move(k); }

    std::string name() const override { return "keyed_co_process_function"; }

private:
    K current_key_{};
};

namespace detail {

// CoOperator<I1, I2, O> adapter. The runtime hands us StreamElements
// from each side; we extract the typed value and call user's
// process_element{1,2}.
template <typename I1, typename I2, typename O>
class CoProcessFunctionAdapter : public CoOperator<I1, I2, O> {
public:
    explicit CoProcessFunctionAdapter(std::shared_ptr<CoProcessFunction<I1, I2, O>> fn,
                                      std::string name = "co_process_function")
        : fn_(std::move(fn)), name_(std::move(name)) {}

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
        }
    }

    void close() override { fn_->close(); }

    void process_element1(const StreamElement<I1>& element, Emitter<O>& out) override {
        if (!element.is_data()) {
            return;
        }
        Collector<O> col(&out);
        for (const auto& rec : element.as_data()) {
            ProcessFunctionContext<O> ctx(
                rec.event_time(), this->runtime()->timer_service(), this->runtime());
            pre_dispatch_left_(rec);
            fn_->process_element1(rec.value(), ctx, col);
        }
    }

    void process_element2(const StreamElement<I2>& element, Emitter<O>& out) override {
        if (!element.is_data()) {
            return;
        }
        Collector<O> col(&out);
        for (const auto& rec : element.as_data()) {
            ProcessFunctionContext<O> ctx(
                rec.event_time(), this->runtime()->timer_service(), this->runtime());
            pre_dispatch_right_(rec);
            fn_->process_element2(rec.value(), ctx, col);
        }
    }

    void on_processing_time_timer(std::int64_t ts,
                                  const std::string& key,
                                  Emitter<O>& out) override {
        Collector<O> col(&out);
        OnTimerContext<O> ctx(EventTime{ts},
                              TimeDomain::ProcessingTime,
                              this->runtime()->timer_service(),
                              this->runtime());
        pre_dispatch_timer_(key);
        fn_->on_timer(ts, ctx, col);
    }

    void on_event_time_timer(std::int64_t ts, const std::string& key, Emitter<O>& out) override {
        Collector<O> col(&out);
        OnTimerContext<O> ctx(EventTime{ts},
                              TimeDomain::EventTime,
                              this->runtime()->timer_service(),
                              this->runtime());
        pre_dispatch_timer_(key);
        fn_->on_timer(ts, ctx, col);
    }

    void flush(Emitter<O>& out) override {
        Collector<O> col(&out);
        fn_->flush(col);
    }

    std::string name() const override { return name_; }

private:
    virtual void pre_dispatch_left_(const Record<I1>& /*rec*/) {}
    virtual void pre_dispatch_right_(const Record<I2>& /*rec*/) {}
    virtual void pre_dispatch_timer_(const std::string& /*key*/) {}

    std::shared_ptr<CoProcessFunction<I1, I2, O>> fn_;
    std::string name_;
};

// Keyed co-adapter. Both sides supply a key extractor; the adapter
// keeps current_key in sync with whichever side is being dispatched.
template <typename K, typename I1, typename I2, typename O>
class KeyedCoProcessFunctionAdapter : public CoProcessFunctionAdapter<I1, I2, O> {
public:
    using KeyFn1 = std::function<K(const I1&)>;
    using KeyFn2 = std::function<K(const I2&)>;
    using KeyFromTimerKey = std::function<K(const std::string&)>;

    KeyedCoProcessFunctionAdapter(std::shared_ptr<KeyedCoProcessFunction<K, I1, I2, O>> fn,
                                  KeyFn1 key1,
                                  KeyFn2 key2,
                                  KeyFromTimerKey timer_key_fn = nullptr,
                                  std::string name = "keyed_co_process_function")
        : CoProcessFunctionAdapter<I1, I2, O>(fn, std::move(name)),
          keyed_fn_(std::move(fn)),
          key1_(std::move(key1)),
          key2_(std::move(key2)),
          timer_key_fn_(std::move(timer_key_fn)) {}

private:
    void pre_dispatch_left_(const Record<I1>& rec) override {
        keyed_fn_->set_current_key_(key1_(rec.value()));
    }
    void pre_dispatch_right_(const Record<I2>& rec) override {
        keyed_fn_->set_current_key_(key2_(rec.value()));
    }
    void pre_dispatch_timer_(const std::string& key) override {
        if (timer_key_fn_) {
            keyed_fn_->set_current_key_(timer_key_fn_(key));
        }
    }

    std::shared_ptr<KeyedCoProcessFunction<K, I1, I2, O>> keyed_fn_;
    KeyFn1 key1_;
    KeyFn2 key2_;
    KeyFromTimerKey timer_key_fn_;
};

}  // namespace detail

// BroadcastProcessFunction - two-stream operator pattern for
// "main stream + slowly-changing control stream". The control stream
// updates a BroadcastState<State>; every main-stream record sees the
// current state and can emit zero or more records via the collector.
//
// process_broadcast_element(Brod, BroadcastState<State>&, Collector)
//   - read AND write the broadcast state, emit downstream
// process_element(Main, const BroadcastState<State>&, Collector)
//   - read-only view of the state, emit downstream
//
// Both callbacks receive a Collector so they can fan out to many
// records per input (vs. the older broadcast_connect which limited
// each main-stream input to one optional output). State must be
// codec-serializable so the runtime can persist it through
// checkpointing.
template <typename Main, typename Brod, typename Out, typename State>
class BroadcastProcessFunction {
public:
    virtual ~BroadcastProcessFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    virtual void process_element(const Main& value,
                                 const BroadcastState<State>& state,
                                 Collector<Out>& out) = 0;
    virtual void process_broadcast_element(const Brod& value,
                                           BroadcastState<State>& state,
                                           Collector<Out>& out) = 0;

    virtual std::string name() const { return "broadcast_process_function"; }
};

namespace detail {

// Convert a BroadcastProcessFunction into the std::function callback
// shape Dag::broadcast_process expects. Returns a pair {process_brod,
// process_main} ready to pass through.
template <typename Main, typename Brod, typename Out, typename State>
inline std::pair<std::function<void(const Brod&, BroadcastState<State>&, std::vector<Out>&)>,
                 std::function<void(const Main&, BroadcastState<State>&, std::vector<Out>&)>>
build_broadcast_process_callbacks(
    std::shared_ptr<BroadcastProcessFunction<Main, Brod, Out, State>> fn) {
    // The vector<Out>& the runtime hands us is the "drain": user code
    // collects into it via Collector wrapper, runtime emits at end.
    // We adapt by writing a thin Collector<Out> that pushes into the
    // drain (no Emitter - we don't have one at this layer; the runner
    // emits the batch at the end).
    //
    // We can't reuse Collector<Out> directly because it wraps an
    // Emitter<Out>*; the runner exposes the drain as vector<Out>&.
    // A small DrainCollector type bridges.
    struct DrainCollector {
        std::vector<Out>* drain;
        void collect(Out v) { drain->push_back(std::move(v)); }
    };

    auto process_brod = [fn](const Brod& v, BroadcastState<State>& state, std::vector<Out>& drain) {
        // We need a real Collector<Out>; build a stub that wraps a
        // temporary Emitter<Out> over a tiny inline channel - but
        // that allocates per call. Cheaper: build a Collector that
        // wraps an Emitter constructed via the forwarding form.
        Emitter<Out> emitter([&drain](StreamElement<Out> el) -> bool {
            if (el.is_data()) {
                for (auto& rec : el.as_data()) {
                    drain.push_back(rec.value());
                }
            }
            return true;
        });
        Collector<Out> col(&emitter);
        fn->process_broadcast_element(v, state, col);
    };

    auto process_main = [fn](const Main& v, BroadcastState<State>& state, std::vector<Out>& drain) {
        Emitter<Out> emitter([&drain](StreamElement<Out> el) -> bool {
            if (el.is_data()) {
                for (auto& rec : el.as_data()) {
                    drain.push_back(rec.value());
                }
            }
            return true;
        });
        Collector<Out> col(&emitter);
        fn->process_element(v, state, col);
    };

    return {std::move(process_brod), std::move(process_main)};
}

}  // namespace detail

// ProcessWindowFunction - full-iterable window function. The
// user receives the entire buffered batch of records that fell into
// one (key, window) tuple, plus the window's bounds via WindowContext.
// Use this when incremental aggregation isn't expressive enough (e.g.
// you need percentiles, sorting, or anything that wants random access
// to the records).
//
// process(K key, WindowContext& ctx, Iterable<In> elements, Collector<Out>& out)
struct WindowContext {
    std::int64_t window_start{0};
    std::int64_t window_end{0};
    std::int64_t window_max_event_time{0};
};

template <typename K, typename In, typename Out>
class ProcessWindowFunction {
public:
    virtual ~ProcessWindowFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    // Iterable is std::vector<In> - the buffered window contents in
    // arrival order.  uses java.lang.Iterable; std::vector is the
    // natural C++23 analogue and gives the user random access (
    // users often reach for that even though the Java Iterable
    // doesn't formally promise it).
    virtual void process(const K& key,
                         const WindowContext& ctx,
                         const std::vector<In>& elements,
                         Collector<Out>& out) = 0;

    virtual std::string name() const { return "process_window_function"; }
};

namespace detail {

// Codec for the per-(key, window_start) bucket stored in keyed state
// by Tumbling/Sliding ProcessWindow adapters. Layout:
//   [i64 start][i64 end][i64 max_event_time][u8 fired][vector<In> elements]
template <typename In>
struct ProcessBucket {
    std::int64_t start{0};
    std::int64_t end{0};
    std::int64_t max_event_time{0};
    bool fired{false};
    std::vector<In> elements;
};

template <typename In>
inline Codec<ProcessBucket<In>> process_bucket_codec(Codec<In> elem) {
    auto vec_codec = vector_codec<In>(elem);
    return Codec<ProcessBucket<In>>{
        .encode =
            [vec_codec](const ProcessBucket<In>& b) {
                typename Codec<ProcessBucket<In>>::Bytes out;
                out.reserve(8 * 3 + 1 + 4 + b.elements.size() * 8);
                auto put_i64 = [&out](std::int64_t v) {
                    for (int i = 0; i < 8; ++i) {
                        out.push_back(static_cast<std::byte>(
                            (static_cast<std::uint64_t>(v) >> (i * 8)) & 0xFF));
                    }
                };
                put_i64(b.start);
                put_i64(b.end);
                put_i64(b.max_event_time);
                out.push_back(static_cast<std::byte>(b.fired ? 1 : 0));
                auto elem_bytes = vec_codec.encode(b.elements);
                out.insert(out.end(), elem_bytes.begin(), elem_bytes.end());
                return out;
            },
        .decode = [vec_codec](typename Codec<ProcessBucket<In>>::BytesView buf)
            -> std::optional<ProcessBucket<In>> {
            if (buf.size() < 8 * 3 + 1) {
                return std::nullopt;
            }
            auto read_i64 = [&](std::size_t off) -> std::int64_t {
                std::uint64_t v = 0;
                for (int i = 0; i < 8; ++i) {
                    v |= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[off + i]))
                         << (i * 8);
                }
                return static_cast<std::int64_t>(v);
            };
            ProcessBucket<In> b;
            b.start = read_i64(0);
            b.end = read_i64(8);
            b.max_event_time = read_i64(16);
            b.fired = static_cast<unsigned char>(buf[24]) != 0;
            auto rest = buf.subspan(25);
            auto vec = vec_codec.decode(rest);
            if (!vec.has_value()) {
                return std::nullopt;
            }
            b.elements = std::move(*vec);
            return b;
        }};
}

// Per-key Session buffer stored in keyed state by the session
// ProcessWindow adapter. Each key maps to a vector<ProcessSession>;
// merge logic happens in the adapter, not the codec. Layout per
// session: [i64 start][i64 end][u8 fired][vector<In> elements].
template <typename In>
struct ProcessSession {
    std::int64_t start{0};
    std::int64_t end{0};
    bool fired{false};
    std::vector<In> elements;
};

template <typename In>
inline Codec<ProcessSession<In>> process_session_codec(Codec<In> elem) {
    auto vec_codec = vector_codec<In>(elem);
    return Codec<ProcessSession<In>>{
        .encode =
            [vec_codec](const ProcessSession<In>& s) {
                typename Codec<ProcessSession<In>>::Bytes out;
                out.reserve(8 * 2 + 1 + 4 + s.elements.size() * 8);
                auto put_i64 = [&out](std::int64_t v) {
                    for (int i = 0; i < 8; ++i) {
                        out.push_back(static_cast<std::byte>(
                            (static_cast<std::uint64_t>(v) >> (i * 8)) & 0xFF));
                    }
                };
                put_i64(s.start);
                put_i64(s.end);
                out.push_back(static_cast<std::byte>(s.fired ? 1 : 0));
                auto elem_bytes = vec_codec.encode(s.elements);
                out.insert(out.end(), elem_bytes.begin(), elem_bytes.end());
                return out;
            },
        .decode = [vec_codec](typename Codec<ProcessSession<In>>::BytesView buf)
            -> std::optional<ProcessSession<In>> {
            if (buf.size() < 8 * 2 + 1) {
                return std::nullopt;
            }
            auto read_i64 = [&](std::size_t off) -> std::int64_t {
                std::uint64_t v = 0;
                for (int i = 0; i < 8; ++i) {
                    v |= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[off + i]))
                         << (i * 8);
                }
                return static_cast<std::int64_t>(v);
            };
            ProcessSession<In> s;
            s.start = read_i64(0);
            s.end = read_i64(8);
            s.fired = static_cast<unsigned char>(buf[16]) != 0;
            auto rest = buf.subspan(17);
            auto vec = vec_codec.decode(rest);
            if (!vec.has_value()) {
                return std::nullopt;
            }
            s.elements = std::move(*vec);
            return s;
        }};
}

// Per-key per-window buffering operator that drives a
// ProcessWindowFunction at trigger time (event-time end of window).
// Input is std::pair<Key, In>; output Out. Tumbling event-time
// windows of fixed `size`.
//
// Two execution modes (mirrors TumblingWindowOperator):
//   * In-memory (default ctor): private std::unordered_map. Used by
//     callers that don't need restart durability or that don't have
//     a state backend wired (most existing tests).
//   * Persistent (codec-bearing ctor): KeyedState<pair<Key, i64>,
//     ProcessBucket<In>>. Buckets survive checkpoint/restore.
template <typename Key, typename In, typename Out>
class TumblingProcessWindowAdapter final : public Operator<std::pair<Key, In>, Out> {
public:
    TumblingProcessWindowAdapter(std::chrono::milliseconds size,
                                 std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                 std::string name = "tumbling_process_window")
        : size_(size), fn_(std::move(fn)), name_(std::move(name)) {}

    TumblingProcessWindowAdapter(std::chrono::milliseconds size,
                                 std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                 Codec<Key> key_codec,
                                 Codec<In> in_codec,
                                 std::string name = "tumbling_process_window")
        : size_(size),
          fn_(std::move(fn)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          in_codec_(std::move(in_codec)) {}

    // Configure how long after window_end this operator retains the
    // bucket for late records. Mirrors the basic operators'
    // allowed_lateness contract. Default 0 = single fire then purge.
    TumblingProcessWindowAdapter& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag for records arriving past
    // (window_end + allowed_lateness). Forwarded as-is, preserving
    // event_time. Without a tag, the historic "create fresh bucket"
    // behavior is preserved. Records within the lateness band still
    // flow to handle_record_ where the re-fire path picks them up
    // (the operator emits a fresh process() call on the bucket's
    // updated contents).
    TumblingProcessWindowAdapter& late_output_tag(OutputTag<In> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
            if (key_codec_.has_value() && in_codec_.has_value() && rt->has_state_backend()) {
                keyed_ = std::make_unique<KeyedState<StateKey, Bucket>>(
                    rt->template keyed_state<StateKey, Bucket>(
                        "process_window_buf",
                        pair_codec<Key, std::int64_t>(*key_codec_, int64_codec()),
                        process_bucket_codec<In>(*in_codec_)));
            }
        }
    }

    void close() override {
        keyed_.reset();
        fn_->close();
    }

    void process(const StreamElement<std::pair<Key, In>>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            Collector<Out> col(&out);
            for (const auto& rec : element.as_data()) {
                const std::int64_t ts = rec.event_time().value_or(EventTime{0}).millis();
                const std::int64_t start = (ts / size_.count()) * size_.count();
                const std::int64_t end = start + size_.count();
                const std::int64_t purge_at = end + allowed_lateness_.count();
                // Late-late: watermark has crossed (end + lateness),
                // so the bucket is gone. Route to side output if a
                // tag is set; otherwise fall through to create a
                // fresh bucket (historic behavior).
                if (late_tag_.has_value() && last_watermark_ms_ >= purge_at &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<In>(*late_tag_);
                    Batch<In> b;
                    if (rec.event_time().has_value()) {
                        b.emplace(rec.value().second, *rec.event_time());
                    } else {
                        b.emplace(rec.value().second);
                    }
                    side.emit_data(std::move(b));
                    continue;
                }
                const StateKey sk{rec.value().first, start};
                Bucket bucket = load_(sk);
                bucket.start = start;
                bucket.end = end;
                bucket.elements.push_back(rec.value().second);
                if (ts > bucket.max_event_time) {
                    bucket.max_event_time = ts;
                }
                // If the bucket already fired (a late record within
                // the lateness band) re-fire with the updated
                // contents - same semantics as the basic operators'
                // re-fire path.
                if (bucket.fired) {
                    WindowContext ctx{.window_start = bucket.start,
                                      .window_end = bucket.end,
                                      .window_max_event_time = bucket.max_event_time};
                    fn_->process(rec.value().first, ctx, bucket.elements, col);
                }
                store_(sk, std::move(bucket));
            }
        } else if (element.is_watermark()) {
            last_watermark_ms_ = element.as_watermark().timestamp().millis();
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_watermark(Watermark wm, Emitter<Out>& out) override {
        // Two passes:
        //   1. Fire every not-yet-fired window with end <= wm.
        //   2. Purge buckets past (end + allowed_lateness).
        // When persistent: scan into a local action list first
        // (KeyedState::scan forbids mutation), then fire/erase.
        const std::int64_t wm_ms = wm.timestamp().millis();
        Collector<Out> col(&out);

        if (keyed_) {
            struct Action {
                StateKey sk;
                Bucket bucket;
                bool fire{false};
                bool purge{false};
            };
            std::vector<Action> actions;
            keyed_->scan([&](const StateKey& sk, const Bucket& bucket) {
                Action a;
                a.sk = sk;
                a.bucket = bucket;
                if (!bucket.fired && bucket.end <= wm_ms) {
                    a.fire = true;
                }
                if (wm_ms >= bucket.end + allowed_lateness_.count()) {
                    a.purge = true;
                }
                if (a.fire || a.purge) {
                    actions.push_back(std::move(a));
                }
            });
            for (auto& a : actions) {
                if (a.fire) {
                    WindowContext ctx{.window_start = a.bucket.start,
                                      .window_end = a.bucket.end,
                                      .window_max_event_time = a.bucket.max_event_time};
                    fn_->process(a.sk.first, ctx, a.bucket.elements, col);
                    a.bucket.fired = true;
                }
                if (a.purge) {
                    keyed_->erase(a.sk);
                } else if (a.fire) {
                    keyed_->put(a.sk, a.bucket);
                }
            }
        } else {
            for (auto it = mem_.begin(); it != mem_.end();) {
                auto& bucket = it->second;
                if (!bucket.fired && bucket.end <= wm_ms) {
                    WindowContext ctx{.window_start = bucket.start,
                                      .window_end = bucket.end,
                                      .window_max_event_time = bucket.max_event_time};
                    fn_->process(it->first.first, ctx, bucket.elements, col);
                    bucket.fired = true;
                }
                const std::int64_t purge_at = bucket.end + allowed_lateness_.count();
                if (wm_ms >= purge_at) {
                    it = mem_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        Operator<std::pair<Key, In>, Out>::on_watermark(wm, out);
    }

    void flush(Emitter<Out>& out) override {
        // End-of-stream: fire every remaining window so no buffered
        // records are silently dropped. Matches
        // closeOnFlush=true semantic for bounded sources. Skip
        // already-fired buckets - they were emitted via the on-time
        // pane and any late re-fires; don't re-emit at EOS.
        Collector<Out> col(&out);
        if (keyed_) {
            std::vector<std::pair<StateKey, Bucket>> all;
            keyed_->scan(
                [&](const StateKey& sk, const Bucket& bucket) { all.emplace_back(sk, bucket); });
            for (auto& [sk, bucket] : all) {
                if (!bucket.fired) {
                    WindowContext ctx{.window_start = bucket.start,
                                      .window_end = bucket.end,
                                      .window_max_event_time = bucket.max_event_time};
                    fn_->process(sk.first, ctx, bucket.elements, col);
                }
                keyed_->erase(sk);
            }
        } else {
            for (auto& [key, bucket] : mem_) {
                if (bucket.fired) {
                    continue;
                }
                WindowContext ctx{.window_start = bucket.start,
                                  .window_end = bucket.end,
                                  .window_max_event_time = bucket.max_event_time};
                fn_->process(key.first, ctx, bucket.elements, col);
            }
            mem_.clear();
        }
    }

    std::string name() const override { return name_; }

private:
    using Bucket = ProcessBucket<In>;
    using StateKey = std::pair<Key, std::int64_t>;
    struct PairHash {
        std::size_t operator()(const StateKey& p) const noexcept {
            return std::hash<Key>{}(p.first) ^ (std::hash<std::int64_t>{}(p.second) << 1);
        }
    };

    Bucket load_(const StateKey& sk) {
        if (keyed_) {
            auto v = keyed_->get(sk);
            if (v.has_value()) {
                return std::move(*v);
            }
            return Bucket{};
        }
        auto it = mem_.find(sk);
        if (it != mem_.end()) {
            return it->second;
        }
        return Bucket{};
    }
    void store_(const StateKey& sk, Bucket b) {
        if (keyed_) {
            keyed_->put(sk, b);
        } else {
            mem_[sk] = std::move(b);
        }
    }

    std::chrono::milliseconds size_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn_;
    std::string name_;
    std::optional<OutputTag<In>> late_tag_;
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};

    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<In>> in_codec_;
    std::unique_ptr<KeyedState<StateKey, Bucket>> keyed_;
    clink::FlatMap<StateKey, Bucket, PairHash> mem_;
};

// Sliding-window variant of the same adapter. Each input record
// belongs to multiple overlapping windows (size / slide of them);
// the operator pushes the record into every covering bucket. Same
// trigger-at-watermark + flush-at-eos semantics as the tumbling
// adapter - the only difference is the per-record window enumeration.
//
// size MUST be a positive multiple of slide; non-aligned sizes
// give surprising boundary behaviour for the same reason
// recommends a multiple.
template <typename Key, typename In, typename Out>
class SlidingProcessWindowAdapter final : public Operator<std::pair<Key, In>, Out> {
public:
    SlidingProcessWindowAdapter(std::chrono::milliseconds size,
                                std::chrono::milliseconds slide,
                                std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                std::string name = "sliding_process_window")
        : size_(size), slide_(slide), fn_(std::move(fn)), name_(std::move(name)) {}

    SlidingProcessWindowAdapter(std::chrono::milliseconds size,
                                std::chrono::milliseconds slide,
                                std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                Codec<Key> key_codec,
                                Codec<In> in_codec,
                                std::string name = "sliding_process_window")
        : size_(size),
          slide_(slide),
          fn_(std::move(fn)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          in_codec_(std::move(in_codec)) {}

    // Configure how long after each covering window's end this
    // operator retains the bucket for late records. Each overlapping
    // window has its own (end + allowed_lateness) deadline.
    SlidingProcessWindowAdapter& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag for records arriving past every covering
    // window's (end + allowed_lateness). See
    // TumblingProcessWindowAdapter::late_output_tag.
    SlidingProcessWindowAdapter& late_output_tag(OutputTag<In> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
            if (key_codec_.has_value() && in_codec_.has_value() && rt->has_state_backend()) {
                keyed_ = std::make_unique<KeyedState<StateKey, Bucket>>(
                    rt->template keyed_state<StateKey, Bucket>(
                        "process_window_buf",
                        pair_codec<Key, std::int64_t>(*key_codec_, int64_codec()),
                        process_bucket_codec<In>(*in_codec_)));
            }
        }
    }

    void close() override {
        keyed_.reset();
        fn_->close();
    }

    void process(const StreamElement<std::pair<Key, In>>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            Collector<Out> col(&out);
            for (const auto& rec : element.as_data()) {
                const std::int64_t ts = rec.event_time().value_or(EventTime{0}).millis();
                const std::int64_t slide_ms = slide_.count();
                const std::int64_t size_ms = size_.count();
                const std::int64_t first_start = ((ts / slide_ms) * slide_ms);
                const std::int64_t latest_purge_at =
                    first_start + size_ms + allowed_lateness_.count();
                if (late_tag_.has_value() && last_watermark_ms_ >= latest_purge_at &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<In>(*late_tag_);
                    Batch<In> b;
                    if (rec.event_time().has_value()) {
                        b.emplace(rec.value().second, *rec.event_time());
                    } else {
                        b.emplace(rec.value().second);
                    }
                    side.emit_data(std::move(b));
                    continue;
                }
                for (std::int64_t start = first_start; start > ts - size_ms; start -= slide_ms) {
                    const StateKey sk{rec.value().first, start};
                    Bucket bucket = load_(sk);
                    bucket.start = start;
                    bucket.end = start + size_ms;
                    bucket.elements.push_back(rec.value().second);
                    if (ts > bucket.max_event_time) {
                        bucket.max_event_time = ts;
                    }
                    if (bucket.fired) {
                        WindowContext ctx{.window_start = bucket.start,
                                          .window_end = bucket.end,
                                          .window_max_event_time = bucket.max_event_time};
                        fn_->process(rec.value().first, ctx, bucket.elements, col);
                    }
                    store_(sk, std::move(bucket));
                }
            }
        } else if (element.is_watermark()) {
            last_watermark_ms_ = element.as_watermark().timestamp().millis();
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_watermark(Watermark wm, Emitter<Out>& out) override {
        const std::int64_t wm_ms = wm.timestamp().millis();
        Collector<Out> col(&out);
        if (keyed_) {
            struct Action {
                StateKey sk;
                Bucket bucket;
                bool fire{false};
                bool purge{false};
            };
            std::vector<Action> actions;
            keyed_->scan([&](const StateKey& sk, const Bucket& bucket) {
                Action a;
                a.sk = sk;
                a.bucket = bucket;
                if (!bucket.fired && bucket.end <= wm_ms) {
                    a.fire = true;
                }
                if (wm_ms >= bucket.end + allowed_lateness_.count()) {
                    a.purge = true;
                }
                if (a.fire || a.purge) {
                    actions.push_back(std::move(a));
                }
            });
            for (auto& a : actions) {
                if (a.fire) {
                    WindowContext ctx{.window_start = a.bucket.start,
                                      .window_end = a.bucket.end,
                                      .window_max_event_time = a.bucket.max_event_time};
                    fn_->process(a.sk.first, ctx, a.bucket.elements, col);
                    a.bucket.fired = true;
                }
                if (a.purge) {
                    keyed_->erase(a.sk);
                } else if (a.fire) {
                    keyed_->put(a.sk, a.bucket);
                }
            }
        } else {
            for (auto it = mem_.begin(); it != mem_.end();) {
                auto& bucket = it->second;
                if (!bucket.fired && bucket.end <= wm_ms) {
                    WindowContext ctx{.window_start = bucket.start,
                                      .window_end = bucket.end,
                                      .window_max_event_time = bucket.max_event_time};
                    fn_->process(it->first.first, ctx, bucket.elements, col);
                    bucket.fired = true;
                }
                const std::int64_t purge_at = bucket.end + allowed_lateness_.count();
                if (wm_ms >= purge_at) {
                    it = mem_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        Operator<std::pair<Key, In>, Out>::on_watermark(wm, out);
    }

    void flush(Emitter<Out>& out) override {
        Collector<Out> col(&out);
        if (keyed_) {
            std::vector<std::pair<StateKey, Bucket>> all;
            keyed_->scan(
                [&](const StateKey& sk, const Bucket& bucket) { all.emplace_back(sk, bucket); });
            for (auto& [sk, bucket] : all) {
                if (!bucket.fired) {
                    WindowContext ctx{.window_start = bucket.start,
                                      .window_end = bucket.end,
                                      .window_max_event_time = bucket.max_event_time};
                    fn_->process(sk.first, ctx, bucket.elements, col);
                }
                keyed_->erase(sk);
            }
        } else {
            for (auto& [key, bucket] : mem_) {
                if (bucket.fired) {
                    continue;
                }
                WindowContext ctx{.window_start = bucket.start,
                                  .window_end = bucket.end,
                                  .window_max_event_time = bucket.max_event_time};
                fn_->process(key.first, ctx, bucket.elements, col);
            }
            mem_.clear();
        }
    }

    std::string name() const override { return name_; }

private:
    using Bucket = ProcessBucket<In>;
    using StateKey = std::pair<Key, std::int64_t>;
    struct PairHash {
        std::size_t operator()(const StateKey& p) const noexcept {
            return std::hash<Key>{}(p.first) ^ (std::hash<std::int64_t>{}(p.second) << 1);
        }
    };

    Bucket load_(const StateKey& sk) {
        if (keyed_) {
            auto v = keyed_->get(sk);
            if (v.has_value()) {
                return std::move(*v);
            }
            return Bucket{};
        }
        auto it = mem_.find(sk);
        if (it != mem_.end()) {
            return it->second;
        }
        return Bucket{};
    }
    void store_(const StateKey& sk, Bucket b) {
        if (keyed_) {
            keyed_->put(sk, b);
        } else {
            mem_[sk] = std::move(b);
        }
    }

    std::chrono::milliseconds size_;
    std::chrono::milliseconds slide_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn_;
    std::string name_;
    std::optional<OutputTag<In>> late_tag_;
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};

    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<In>> in_codec_;
    std::unique_ptr<KeyedState<StateKey, Bucket>> keyed_;
    clink::FlatMap<StateKey, Bucket, PairHash> mem_;
};

// Session window variant. A session window groups records that are
// within `gap` of each other; a new session starts after a gap of
// inactivity. Sessions are per-key. The operator merges overlapping
// sessions on the fly: a new record that lands within `gap` of an
// existing session extends it; a new record bridging two sessions
// merges them. Trigger semantics match the other variants: a session
// fires when the watermark crosses its (current) end + gap (the
// session needs the gap of quiet AFTER its last record before we can
// be sure no late arrival will extend it).
template <typename Key, typename In, typename Out>
class SessionProcessWindowAdapter final : public Operator<std::pair<Key, In>, Out> {
public:
    SessionProcessWindowAdapter(std::chrono::milliseconds gap,
                                std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                std::string name = "session_process_window")
        : gap_(gap), fn_(std::move(fn)), name_(std::move(name)) {}

    SessionProcessWindowAdapter(std::chrono::milliseconds gap,
                                std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn,
                                Codec<Key> key_codec,
                                Codec<In> in_codec,
                                std::string name = "session_process_window")
        : gap_(gap),
          fn_(std::move(fn)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          in_codec_(std::move(in_codec)) {}

    // Configure how long after a session's fire time (end + gap) this
    // operator retains the session for late records. Late records
    // arriving within the band can extend / merge the session and
    // trigger a re-fire.
    SessionProcessWindowAdapter& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag for records arriving past
    // (ts + 2*gap + allowed_lateness) when no session containing ts
    // can still be alive. See TumblingProcessWindowAdapter::late_output_tag.
    SessionProcessWindowAdapter& late_output_tag(OutputTag<In> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
            if (key_codec_.has_value() && in_codec_.has_value() && rt->has_state_backend()) {
                keyed_ = std::make_unique<KeyedState<Key, std::vector<Session>>>(
                    rt->template keyed_state<Key, std::vector<Session>>(
                        "process_session_buf",
                        *key_codec_,
                        vector_codec<Session>(process_session_codec<In>(*in_codec_))));
            }
        }
    }

    void close() override {
        keyed_.reset();
        fn_->close();
    }

    void process(const StreamElement<std::pair<Key, In>>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            Collector<Out> col(&out);
            for (const auto& rec : element.as_data()) {
                const std::int64_t ts = rec.event_time().value_or(EventTime{0}).millis();
                if (late_tag_.has_value() &&
                    last_watermark_ms_ >= ts + 2 * gap_.count() + allowed_lateness_.count() &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<In>(*late_tag_);
                    Batch<In> b;
                    if (rec.event_time().has_value()) {
                        b.emplace(rec.value().second, *rec.event_time());
                    } else {
                        b.emplace(rec.value().second);
                    }
                    side.emit_data(std::move(b));
                    continue;
                }
                add_record_(rec.value().first, ts, rec.value().second, col);
            }
        } else if (element.is_watermark()) {
            last_watermark_ms_ = element.as_watermark().timestamp().millis();
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_watermark(Watermark wm, Emitter<Out>& out) override {
        const std::int64_t wm_ms = wm.timestamp().millis();
        const std::int64_t gap_ms = gap_.count();
        const std::int64_t lateness_ms = allowed_lateness_.count();
        Collector<Out> col(&out);
        // Two passes per key:
        //   1. Fire every not-yet-fired session whose end + gap <= wm.
        //   2. Purge sessions whose end + gap + lateness <= wm.
        auto process_key = [&](const Key& key, std::vector<Session>& sessions) {
            std::vector<Session> remaining;
            remaining.reserve(sessions.size());
            for (auto& s : sessions) {
                if (!s.fired && s.end + gap_ms <= wm_ms) {
                    WindowContext ctx{.window_start = s.start,
                                      .window_end = s.end,
                                      .window_max_event_time = s.end};
                    fn_->process(key, ctx, s.elements, col);
                    s.fired = true;
                }
                if (s.end + gap_ms + lateness_ms <= wm_ms) {
                    continue;  // purge
                }
                remaining.push_back(std::move(s));
            }
            sessions = std::move(remaining);
        };
        if (keyed_) {
            std::vector<std::pair<Key, std::vector<Session>>> all;
            keyed_->scan(
                [&](const Key& key, const std::vector<Session>& v) { all.emplace_back(key, v); });
            for (auto& [key, sessions] : all) {
                process_key(key, sessions);
                if (sessions.empty()) {
                    keyed_->erase(key);
                } else {
                    keyed_->put(key, sessions);
                }
            }
        } else {
            for (auto it = mem_.begin(); it != mem_.end();) {
                process_key(it->first, it->second);
                if (it->second.empty()) {
                    it = mem_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        Operator<std::pair<Key, In>, Out>::on_watermark(wm, out);
    }

    void flush(Emitter<Out>& out) override {
        Collector<Out> col(&out);
        auto fire_unfired = [&](const Key& key, std::vector<Session>& sessions) {
            for (auto& s : sessions) {
                if (s.fired) {
                    continue;
                }
                WindowContext ctx{
                    .window_start = s.start, .window_end = s.end, .window_max_event_time = s.end};
                fn_->process(key, ctx, s.elements, col);
            }
        };
        if (keyed_) {
            std::vector<std::pair<Key, std::vector<Session>>> all;
            keyed_->scan(
                [&](const Key& key, const std::vector<Session>& v) { all.emplace_back(key, v); });
            for (auto& [key, sessions] : all) {
                fire_unfired(key, sessions);
                keyed_->erase(key);
            }
        } else {
            for (auto& [key, sessions] : mem_) {
                fire_unfired(key, sessions);
            }
            mem_.clear();
        }
    }

    std::string name() const override { return name_; }

private:
    using Session = ProcessSession<In>;

    std::vector<Session> load_sessions_(const Key& key) {
        if (keyed_) {
            auto v = keyed_->get(key);
            if (v.has_value()) {
                return std::move(*v);
            }
            return {};
        }
        auto it = mem_.find(key);
        if (it != mem_.end()) {
            return it->second;
        }
        return {};
    }
    void store_sessions_(const Key& key, std::vector<Session> sessions) {
        if (keyed_) {
            if (sessions.empty()) {
                keyed_->erase(key);
            } else {
                keyed_->put(key, sessions);
            }
        } else {
            if (sessions.empty()) {
                mem_.erase(key);
            } else {
                mem_[key] = std::move(sessions);
            }
        }
    }

    void add_record_(const Key& key, std::int64_t ts, In value, Collector<Out>& col) {
        const std::int64_t gap_ms = gap_.count();
        auto sessions = load_sessions_(key);
        // Find existing sessions that overlap [ts - gap_ms, ts + 1).
        // Merge them with the new record's footprint. If any of the
        // merged sessions had already fired, the merged result is
        // fired too - and we re-fire with the updated contents.
        Session merged;
        merged.start = ts;
        merged.end = ts + 1;
        merged.elements.push_back(std::move(value));
        std::vector<Session> kept;
        kept.reserve(sessions.size());
        for (auto& s : sessions) {
            const bool overlaps =
                !(s.end + gap_ms <= merged.start || merged.end + gap_ms <= s.start);
            if (overlaps) {
                if (s.start < merged.start)
                    merged.start = s.start;
                if (s.end > merged.end)
                    merged.end = s.end;
                if (s.fired) {
                    merged.fired = true;
                }
                for (auto& e : s.elements) {
                    merged.elements.push_back(std::move(e));
                }
            } else {
                kept.push_back(std::move(s));
            }
        }
        if (merged.fired) {
            // Re-fire: the merged session previously emitted an
            // on-time pane; this is a late re-emission with the
            // expanded contents.
            WindowContext ctx{.window_start = merged.start,
                              .window_end = merged.end,
                              .window_max_event_time = merged.end};
            fn_->process(key, ctx, merged.elements, col);
        }
        kept.push_back(std::move(merged));
        store_sessions_(key, std::move(kept));
    }

    std::chrono::milliseconds gap_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::shared_ptr<ProcessWindowFunction<Key, In, Out>> fn_;
    std::string name_;
    std::optional<OutputTag<In>> late_tag_;
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};

    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<In>> in_codec_;
    std::unique_ptr<KeyedState<Key, std::vector<Session>>> keyed_;
    clink::FlatMap<Key, std::vector<Session>> mem_;
};

}  // namespace detail

}  // namespace clink
