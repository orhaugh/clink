#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/api/channel_name.hpp"
#include "clink/api/descriptors.hpp"
#include "clink/application/job_submitter.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/operators/async_co_process_function.hpp"
#include "clink/operators/async_process_function.hpp"
#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/flat_map_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace clink::api {

template <typename T>
class DataStream;

template <typename T>
class KeyedDataStream;

template <typename T>
class SlidingWindowedDataStream;

template <typename T>
class TumblingWindowedDataStream;

template <typename T>
class SessionWindowedDataStream;

template <typename T>
class EvictingTumblingWindowedDataStream;

// StreamExecutionEnvironment is the entry point for building a pipeline
// programmatically. Mirrors
// `StreamExecutionEnvironment.getExecutionEnvironment()`.
//
//   auto env = StreamExecutionEnvironment::create();
//   DataStream<std::int64_t> src = env.source<std::int64_t>(
//       IntRangeSource::builder().count(10).build());
//   src.transform<std::int64_t>("multiply_int64", {{"factor", "3"}})
//      .sink(FileInt64Sink::builder().path("/tmp/out.txt").build());
//   env.execute("my-job", submitter);
//
// All wiring goes through this object: each DataStream<T> holds a
// reference back and uses it to append `OperatorSpec` entries to the
// pending graph. .execute() serialises the graph and ships it via
// JobSubmitter.
//
// The env is single-use after execute() - calling fluent methods on
// stale DataStream<T> handles afterwards is a programmer error.
class StreamExecutionEnvironment {
public:
    static StreamExecutionEnvironment create() { return StreamExecutionEnvironment{}; }

    // Construct an env that routes inline-lambda registrations through
    // an externally-provided PluginRegistry instead of a default-
    // constructed one. Used by CLINK_REGISTER_JOB to forward the
    // host-supplied registry (whose internal references point at the
    // HOST's TypeRegistry / RunnerRegistry / SelectorRegistry singletons)
    // through to the build_fn, so registrations land where the host's
    // planner / dispatcher will find them.
    //
    // Without this, the registrations land in the .so's own copy of
    // those singletons (since clink_core is statically linked into
    // each plugin .so under RTLD_LOCAL), and the host can't see them.
    static StreamExecutionEnvironment create_with_registry(
        ::clink::plugin::PluginRegistry* host_registry) {
        StreamExecutionEnvironment env;
        env.host_registry_ = host_registry;
        return env;
    }

    StreamExecutionEnvironment() = default;

    // Returns the PluginRegistry the inline-lambda fluent shortcuts
    // should write into. If `create_with_registry()` set a host registry,
    // we forward to it (so registrations cross the dlopen boundary
    // correctly). Otherwise we fall back to a default-constructed
    // registry, which references the default-singletons in THIS process.
    ::clink::plugin::PluginRegistry& registry() {
        if (host_registry_ != nullptr) {
            return *host_registry_;
        }
        if (!fallback_registry_.has_value()) {
            fallback_registry_.emplace();
        }
        return *fallback_registry_;
    }

    // Anchor a stream on a source descriptor. Returns a typed DataStream
    // whose channel_type is taken from the descriptor (if set) or from
    // ChannelName<T> as a fallback.
    template <typename T>
    DataStream<T> source(SourceDescriptor desc, std::string id = {});

    // Anchor a stream on a fixed sequence of values, mirroring // env.fromElements(v1, v2, ...). T
    // must be registered as a channel type - the built-ins (int64_t, std::string) are covered by
    // ensure_built_ins_registered(); custom types need a
    // PluginRegistry::register_type<T>(name, codec) call at process
    // startup. Same in-process-only contract as the other inline-lambda
    // fluent shortcuts: the elements live in this process's
    // RunnerRegistry singleton and a remote TM won't see them.
    //
    // At parallelism > 1 the elements are striped across subtasks
    // round-robin (subtask i emits indices i, i+N, i+2N, ...), matching
    // the IntRangeSource behaviour.
    template <typename T>
    DataStream<T> from_elements(std::vector<T> elements, std::string id = {});

    template <typename T>
    DataStream<T> from_elements(std::initializer_list<T> elements, std::string id = {}) {
        return from_elements<T>(std::vector<T>(elements.begin(), elements.end()), std::move(id));
    }

    // Track a plugin .so that should be shipped to the cluster with this
    // job. Repeated calls add additional plugins. The JM ships every
    // listed plugin in every Deploy message.
    StreamExecutionEnvironment& add_plugin(std::string path) {
        plugin_paths_.push_back(std::move(path));
        return *this;
    }

    // Set the default parallelism for every operator subsequently
    // created via the fluent API. Per-operator overrides
    // (e.g. SourceDescriptor.parallelism explicitly > 1, or
    // TumblingWindowedDataStream::parallelism(n)) take precedence; this
    // is the floor applied when no explicit value is given.
    //
    // Inline-lambda ops (map / flat_map / filter / assign_timestamps /
    // reduce / process / aggregate / etc.) all read this value when
    // appended to the graph. Source/sink read it as a floor on
    // descriptor.parallelism. Window classes already have their own
    // .parallelism(n) setter that overrides this default.
    StreamExecutionEnvironment& set_parallelism(std::uint32_t n) {
        default_parallelism_ = (n == 0 ? 1u : n);
        return *this;
    }

    // Read-only accessor; default 1.
    [[nodiscard]] std::uint32_t default_parallelism() const noexcept {
        return default_parallelism_;
    }

    // Compile the accumulated graph and submit via the supplied
    // JobSubmitter. job_name is informational (logged by the JM and
    // surfaced in ListJobs once we attach job-name metadata to JobInfo).
    application::SubmitResult execute(const std::string& job_name,
                                      const application::JobSubmitter& submitter,
                                      application::SubmitOptions opts = {}) {
        (void)job_name;  // reserved for when JobInfo carries names
        if (graph_.ops.empty()) {
            throw std::runtime_error(
                "StreamExecutionEnvironment::execute: no sources / sinks have been added");
        }
        return submitter.submit(graph_.to_json(), plugin_paths_, opts);
    }

    // Internal: append an OperatorSpec built by a DataStream<T> method
    // and return the assigned op id. Public so DataStream<T> can call
    // it; not intended for direct user use.
    std::string append_op(cluster::OperatorSpec op) {
        if (op.id.empty()) {
            op.id = "op_" + std::to_string(graph_.ops.size());
        } else if (used_ids_.count(op.id) != 0) {
            throw std::runtime_error("StreamExecutionEnvironment: duplicate operator id '" + op.id +
                                     "'");
        }
        used_ids_.insert(op.id);
        std::string id = op.id;
        graph_.ops.push_back(std::move(op));
        return id;
    }

    // Read-only access to the IR for tests / inspection. The fluent API
    // is the supported way to build it; this exists for diagnostics.
    [[nodiscard]] const cluster::JobGraphSpec& graph() const noexcept { return graph_; }

    // State schema evolution: declare the version this job expects for a
    // keyed-state slot, identified by the operator's `.uid("...")` and a
    // free-form state_type tag. At restore the engine compares this to
    // the snapshot's stamped version and migrates via the
    // StateMigrationRegistry (or fails loudly if no path exists). Keyed
    // by the same operator_id_from_uid the runtime stamps state under,
    // so the tag here must match the one used at register_migration and
    // at the snapshot stamp. Carried in the submitted JobGraphSpec so it
    // reaches the JM and TMs.
    // `slot` (optional) is the keyed-state slot name (the name passed to
    // RuntimeContext::keyed_state) this version applies to. Set it when an
    // operator has more than one keyed-state slot so restore migrates only
    // that slot; leave empty (the default) for single-slot operators, where
    // every value under the operator is migrated.
    StreamExecutionEnvironment& expect_state_version(const std::string& uid,
                                                     std::string state_type,
                                                     std::uint32_t version,
                                                     std::string slot = {}) {
        graph_.expected_state_versions.set(
            clink::operator_id_from_uid(uid), std::move(state_type), version, std::move(slot));
        return *this;
    }

    // Mutate the display_name / uid of an already-appended op. Used by
    // DataStream<T>::name() / .uid() to attach metadata to the most-
    // recent operator without rewiring the graph. Throws if `op_id` is
    // unknown OR if `new_uid` collides with another op's uid (uid
    // uniqueness is a hard invariant - two ops sharing a uid would
    // share keyed state, which is almost certainly not what the user
    // wants).
    void set_op_display_name(const std::string& op_id, std::string display_name) {
        auto* op = find_op_(op_id);
        if (op == nullptr) {
            throw std::runtime_error(
                "StreamExecutionEnvironment::set_op_display_name: "
                "unknown op id '" +
                op_id + "'");
        }
        op->display_name = std::move(display_name);
    }

    void set_op_uid(const std::string& op_id, std::string new_uid) {
        auto* op = find_op_(op_id);
        if (op == nullptr) {
            throw std::runtime_error(
                "StreamExecutionEnvironment::set_op_uid: "
                "unknown op id '" +
                op_id + "'");
        }
        if (!new_uid.empty()) {
            for (const auto& other : graph_.ops) {
                if (other.id != op_id && other.uid == new_uid) {
                    throw std::runtime_error(
                        "StreamExecutionEnvironment::set_op_uid: duplicate uid '" + new_uid +
                        "' (already on op '" + other.id + "')");
                }
            }
        }
        op->uid = std::move(new_uid);
    }

    // Declare a named side output on an already-appended op. The op's
    // ProcessFunction (or windowed-aggregate operator) emits records of
    // type U to OutputTag<U>(tag) via ctx.side_output<U>(tag); the
    // planner uses SideOutputDecl to wire a typed network bridge for
    // that channel so downstream consumers reachable via "<op_id>::<tag>"
    // can pick it up.
    //
    // Idempotent on (tag, channel_type) match - a second declaration with
    // the same pair is a no-op. Throws on:
    //   * unknown op_id
    //   * tag collision with a different channel_type (almost certainly
    //     a user bug: two side outputs sharing a name but disagreeing on
    //     element type)
    void declare_side_output_on(const std::string& op_id,
                                std::string tag,
                                std::string channel_type) {
        auto* op = find_op_(op_id);
        if (op == nullptr) {
            throw std::runtime_error(
                "StreamExecutionEnvironment::declare_side_output_on: "
                "unknown op id '" +
                op_id + "'");
        }
        for (const auto& existing : op->side_outputs) {
            if (existing.tag == tag) {
                if (existing.channel_type != channel_type) {
                    throw std::runtime_error(
                        "StreamExecutionEnvironment::declare_side_output_on: "
                        "tag '" +
                        tag + "' on op '" + op_id + "' is already declared as channel '" +
                        existing.channel_type + "', cannot re-declare as '" + channel_type + "'");
                }
                return;  // idempotent
            }
        }
        cluster::SideOutputDecl decl;
        decl.tag = std::move(tag);
        decl.channel_type = std::move(channel_type);
        op->side_outputs.push_back(std::move(decl));
    }

    // Mint a unique op-type name for an inline-lambda operator. Used
    // internally by DataStream<T>::map() / .flat_map() / etc; exposed as
    // part of the public API so plugin authors writing fluent helpers
    // can mint conflict-free names too.
    //
    // The counter is per-env (not process-wide): two distinct envs in
    // the same process mint independent _inline_<kind>_0, _<kind>_1, ...
    // sequences. Cross-process determinism still holds because each
    // process (submitter, JM, TM) re-runs the same build_fn against a
    // fresh env, which mints names in the same order - the contract is
    // that build_fn's registration order is deterministic.
    //
    // The previous (process-wide) counter trampled itself when two
    // jobs were loaded into the same TM. Per-env makes per-job
    // hosting possible.
    std::string mint_inline_op_type(const std::string& kind) {
        return "_inline_" + kind + "_" + std::to_string(inline_op_counter_++);
    }

private:
    cluster::OperatorSpec* find_op_(const std::string& id) noexcept {
        for (auto& op : graph_.ops) {
            if (op.id == id) {
                return &op;
            }
        }
        return nullptr;
    }

    cluster::JobGraphSpec graph_;
    std::vector<std::string> plugin_paths_;
    std::unordered_set<std::string> used_ids_;
    ::clink::plugin::PluginRegistry* host_registry_{nullptr};
    std::optional<::clink::plugin::PluginRegistry> fallback_registry_{};
    std::uint64_t inline_op_counter_{0};
    std::uint32_t default_parallelism_{1};
};

namespace detail {

// Helper operator used by KeyedDataStream<T>::reduce(). Maintains a
// per-key running aggregate in T-shape (no pair<K, T> conversion) and
// emits the latest accumulator on every input, matching // keyedStream.reduce(ReduceFunction).
// State is in-memory per subtask; since the upstream OperatorSpec carries key_by, hash routing
// guarantees a given key always lands on the same subtask, so the
// per-subtask map is the complete state for that key.
template <typename T>
class KeyedReduceOperator final : public Operator<T, T> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using Reducer = std::function<T(const T&, const T&)>;

    KeyedReduceOperator(KeyFn key_fn, Reducer reducer, std::string name)
        : key_fn_(std::move(key_fn)), reducer_(std::move(reducer)), name_(std::move(name)) {}

    void process(const StreamElement<T>& element, Emitter<T>& out) override {
        if (element.is_data()) {
            Batch<T> batch_out;
            for (const auto& rec : element.as_data()) {
                const auto k = key_fn_(rec.value());
                auto it = state_.find(k);
                if (it == state_.end()) {
                    it = state_.emplace(k, rec.value()).first;
                } else {
                    it->second = reducer_(it->second, rec.value());
                }
                if (rec.event_time().has_value()) {
                    batch_out.emplace(it->second, *rec.event_time());
                } else {
                    batch_out.emplace(it->second);
                }
            }
            if (!batch_out.empty()) {
                out.emit_data(std::move(batch_out));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    Reducer reducer_;
    std::string name_;
    std::unordered_map<std::int64_t, T> state_;
};

// Keyed event-time sliding-window aggregator used by
// SlidingWindowedDataStream<T>::aggregate<Agg>(). T records carry
// event-time (Record::event_time()); a record at time t is folded into
// every window whose half-open span [start, start + size) contains t,
// where window starts are multiples of `slide`. On each forwarded
// watermark, windows with `start + size <= watermark` fire (one Agg
// record per matured (key, window)) and are dropped from state.
//
// Records without event-time are silently skipped - matches the
// existing SlidingWindowOperator's behaviour. Future revision can plug
// in processing-time fallback / configurable trigger / allowed-lateness.
template <typename T, typename Agg>
class KeyedSlidingWindowAggregateOperator final : public Operator<T, Agg> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;

    KeyedSlidingWindowAggregateOperator(KeyFn key_fn,
                                        InitialFn initial,
                                        CombinerFn combiner,
                                        std::chrono::milliseconds size,
                                        std::chrono::milliseconds slide,
                                        std::string name)
        : key_fn_(std::move(key_fn)),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          size_ms_(size.count()),
          slide_ms_(slide.count()),
          name_(std::move(name)) {
        if (size_ms_ <= 0 || slide_ms_ <= 0) {
            throw std::invalid_argument(
                "KeyedSlidingWindowAggregateOperator: size and slide must be positive");
        }
    }

    void set_allowed_lateness(std::chrono::milliseconds v) { allowed_lateness_ms_ = v.count(); }
    void set_late_output_tag(OutputTag<T> tag) { late_tag_ = std::move(tag); }

    void process(const StreamElement<T>& element, Emitter<Agg>& out) override {
        if (element.is_data()) {
            Batch<Agg> refires;
            for (const auto& rec : element.as_data()) {
                if (!rec.event_time().has_value()) {
                    continue;  // event-time required; skip records without it
                }
                const std::int64_t t = rec.event_time()->millis();
                const std::int64_t k = key_fn_(rec.value());
                // Late-late: the latest-starting covering window has
                // end = floor(t/slide)*slide + size. If wm has crossed
                // (latest_end + lateness), every covering window is
                // gone. Route to side output if a tag is set;
                // otherwise fall through (fresh-bucket behavior).
                const std::int64_t latest_start = (t / slide_ms_) * slide_ms_;
                const std::int64_t latest_purge_at = latest_start + size_ms_ + allowed_lateness_ms_;
                if (late_tag_.has_value() && last_watermark_ms_ >= latest_purge_at &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<T>(*late_tag_);
                    Batch<T> b;
                    b.emplace(rec.value(), *rec.event_time());
                    side.emit_data(std::move(b));
                    continue;
                }
                // Enumerate windows whose [start, start+size) contains t.
                std::int64_t start = latest_start;
                while (start + size_ms_ > t) {
                    auto& cell = state_[StateKey{k, start}];
                    if (!cell.agg.has_value()) {
                        cell.agg = initial_();
                    }
                    cell.agg = combiner_(*cell.agg, rec.value());
                    // Re-fire if this is a late record landing on an
                    // already-fired (within-lateness) bucket.
                    if (cell.fired) {
                        refires.emplace(*cell.agg, EventTime{start + size_ms_ - 1});
                    }
                    if (start < slide_ms_) {
                        break;  // next iteration would go negative
                    }
                    start -= slide_ms_;
                }
            }
            if (!refires.empty()) {
                out.emit_data(std::move(refires));
            }
        } else if (element.is_watermark()) {
            const auto wm = element.as_watermark();
            last_watermark_ms_ = wm.timestamp().millis();
            Batch<Agg> fired;
            for (auto it = state_.begin(); it != state_.end();) {
                const auto window_end = it->first.window_start + size_ms_;
                // Fire on-time pane on first watermark crossing end.
                if (!it->second.fired && window_end <= last_watermark_ms_ &&
                    it->second.agg.has_value()) {
                    fired.emplace(*it->second.agg, EventTime{window_end - 1});
                    it->second.fired = true;
                }
                // Purge past lateness deadline.
                const auto purge_at = window_end + allowed_lateness_ms_;
                if (last_watermark_ms_ >= purge_at) {
                    it = state_.erase(it);
                } else {
                    ++it;
                }
            }
            if (!fired.empty()) {
                out.emit_data(std::move(fired));
            }
            out.emit_watermark(wm);
        } else {
            out.emit_barrier(element.as_barrier());
        }
    }

    void flush(Emitter<Agg>& out) override {
        // EOS: fire every still-unfired window (skip already-fired
        // buckets that we kept around within the lateness band).
        Batch<Agg> fired;
        for (auto& [key, cell] : state_) {
            if (!cell.fired && cell.agg.has_value()) {
                fired.emplace(*cell.agg, EventTime{key.window_start + size_ms_ - 1});
            }
        }
        state_.clear();
        if (!fired.empty()) {
            out.emit_data(std::move(fired));
        }
    }

    std::string name() const override { return name_; }

private:
    struct StateKey {
        std::int64_t key;
        std::int64_t window_start;
        bool operator==(const StateKey& o) const noexcept {
            return key == o.key && window_start == o.window_start;
        }
    };
    struct StateKeyHash {
        std::size_t operator()(const StateKey& k) const noexcept {
            // mix using a 64-bit splitmix-style hash on both halves
            auto h = std::hash<std::int64_t>{}(k.key);
            h ^= std::hash<std::int64_t>{}(k.window_start) + 0x9e3779b97f4a7c15ULL + (h << 6) +
                 (h >> 2);
            return h;
        }
    };
    struct Cell {
        std::optional<Agg> agg;
        bool fired{false};
    };

    KeyFn key_fn_;
    InitialFn initial_;
    CombinerFn combiner_;
    std::int64_t size_ms_;
    std::int64_t slide_ms_;
    std::int64_t allowed_lateness_ms_{0};
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};
    std::optional<OutputTag<T>> late_tag_;
    std::string name_;
    std::unordered_map<StateKey, Cell, StateKeyHash> state_;
};

// Strip-and-forward emitter helper used by the wrapper operators
// below - converts inner's pair<int64_t, Agg> output back to Agg.
template <typename Agg>
inline clink::Emitter<std::pair<std::int64_t, Agg>> make_key_stripping_emitter_(
    clink::Emitter<Agg>& out) {
    return clink::Emitter<std::pair<std::int64_t, Agg>>(
        typename clink::Emitter<std::pair<std::int64_t, Agg>>::Forward(
            [&out](clink::StreamElement<std::pair<std::int64_t, Agg>> e) -> bool {
                if (e.is_data()) {
                    Batch<Agg> stripped;
                    for (auto& r : e.as_data()) {
                        if (r.event_time().has_value()) {
                            stripped.emplace(r.value().second, *r.event_time());
                        } else {
                            stripped.emplace(r.value().second);
                        }
                    }
                    if (!stripped.empty()) {
                        out.emit_data(std::move(stripped));
                    }
                } else if (e.is_watermark()) {
                    out.emit_watermark(e.as_watermark());
                } else {
                    out.emit_barrier(e.as_barrier());
                }
                return true;
            }));
}

// Forwarding emitter for emit-form windowed aggregates. The inner
// operator emits pair<int64_t, Agg> with event_time = window_end - 1.
// We expose (key, TimeWindow{start, end}, agg) to the user's emit_fn
// and forward its Out result downstream with the same event_time.
//
// Tumbling: window size is fixed; size_ms is the operator's window size.
// Sliding:  the inner emits one record per (key, window) pair, each
//           with event_time = that window's end - 1. The window size
//           (not slide) is what reconstructs start from end.
template <typename Agg, typename Out>
inline clink::Emitter<std::pair<std::int64_t, Agg>> make_window_emit_forwarding_emitter_(
    clink::Emitter<Out>& out,
    std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)> emit_fn,
    std::int64_t size_ms) {
    return clink::Emitter<std::pair<std::int64_t, Agg>>(
        typename clink::Emitter<std::pair<std::int64_t, Agg>>::Forward(
            [&out, emit_fn = std::move(emit_fn), size_ms](
                clink::StreamElement<std::pair<std::int64_t, Agg>> e) -> bool {
                if (e.is_data()) {
                    Batch<Out> projected;
                    for (auto& r : e.as_data()) {
                        if (!r.event_time().has_value()) {
                            continue;
                        }
                        const std::int64_t end_ms = r.event_time()->millis() + 1;
                        const std::int64_t start_ms = end_ms - size_ms;
                        clink::TimeWindow win{start_ms, end_ms};
                        Out o = emit_fn(r.value().first, win, r.value().second);
                        projected.emplace(std::move(o), *r.event_time());
                    }
                    if (!projected.empty()) {
                        out.emit_data(std::move(projected));
                    }
                } else if (e.is_watermark()) {
                    out.emit_watermark(e.as_watermark());
                } else {
                    out.emit_barrier(e.as_barrier());
                }
                return true;
            }));
}

// Wrapper around the full SlidingWindowOperator<int64_t, T, Agg>.
// Same shape and rationale as KeyedTumblingWindowFullOperator below.
template <typename T, typename Agg>
class KeyedSlidingWindowFullOperator final : public Operator<T, Agg> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using TriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;

    KeyedSlidingWindowFullOperator(KeyFn key_fn,
                                   InitialFn initial,
                                   CombinerFn combiner,
                                   std::chrono::milliseconds size,
                                   std::chrono::milliseconds slide,
                                   std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size, slide, std::move(initial), std::move(combiner), name + "_inner"),
          name_(std::move(name)) {}

    void set_allowed_lateness(std::chrono::milliseconds v) { inner_.allowed_lateness(v); }
    void set_late_output_tag(OutputTag<T> tag) { inner_.late_output_tag(std::move(tag)); }
    void set_trigger(TriggerPtr t) { inner_.with_trigger(std::move(t)); }

    void open() override {
        inner_.attach_runtime(this->runtime());
        inner_.open();
    }

    void close() override {
        inner_.close();
        inner_.attach_runtime(nullptr);
    }

    void process(const StreamElement<T>& element, Emitter<Agg>& out) override {
        auto fwd = make_key_stripping_emitter_<Agg>(out);
        if (element.is_data()) {
            Batch<std::pair<std::int64_t, T>> projected;
            for (const auto& r : element.as_data()) {
                if (r.event_time().has_value()) {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()),
                                      *r.event_time());
                } else {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()));
                }
            }
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::data(std::move(projected)), fwd);
        } else if (element.is_watermark()) {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::watermark(element.as_watermark()),
                fwd);
        } else {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::barrier(element.as_barrier()),
                fwd);
        }
    }

    void flush(Emitter<Agg>& out) override {
        auto fwd = make_key_stripping_emitter_<Agg>(out);
        inner_.flush(fwd);
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    clink::SlidingWindowOperator<std::int64_t, T, Agg> inner_;
    std::string name_;
};

// Emit-form sliding-window aggregator: like KeyedSlidingWindowFull, but
// at emit time the user's emit_fn(key, TimeWindow, agg) -> Out is
// called per (key, window) pair so the downstream can carry the key
// and window-end alongside the accumulator. Same inner operator
// underneath; only the Emitter is different.
template <typename T, typename Agg, typename Out>
class KeyedSlidingWindowEmitOperator final : public Operator<T, Out> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using EmitFn = std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)>;
    using TriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;

    KeyedSlidingWindowEmitOperator(KeyFn key_fn,
                                   InitialFn initial,
                                   CombinerFn combiner,
                                   EmitFn emit_fn,
                                   std::chrono::milliseconds size,
                                   std::chrono::milliseconds slide,
                                   std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size, slide, std::move(initial), std::move(combiner), name + "_inner"),
          emit_fn_(std::move(emit_fn)),
          size_ms_(size.count()),
          name_(std::move(name)) {}

    void set_allowed_lateness(std::chrono::milliseconds v) { inner_.allowed_lateness(v); }
    void set_late_output_tag(OutputTag<T> tag) { inner_.late_output_tag(std::move(tag)); }
    void set_trigger(TriggerPtr t) { inner_.with_trigger(std::move(t)); }

    void open() override {
        inner_.attach_runtime(this->runtime());
        inner_.open();
    }
    void close() override {
        inner_.close();
        inner_.attach_runtime(nullptr);
    }

    void process(const StreamElement<T>& element, Emitter<Out>& out) override {
        auto fwd = make_window_emit_forwarding_emitter_<Agg, Out>(out, emit_fn_, size_ms_);
        if (element.is_data()) {
            Batch<std::pair<std::int64_t, T>> projected;
            for (const auto& r : element.as_data()) {
                if (r.event_time().has_value()) {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()),
                                      *r.event_time());
                } else {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()));
                }
            }
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::data(std::move(projected)), fwd);
        } else if (element.is_watermark()) {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::watermark(element.as_watermark()),
                fwd);
        } else {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::barrier(element.as_barrier()),
                fwd);
        }
    }

    void flush(Emitter<Out>& out) override {
        auto fwd = make_window_emit_forwarding_emitter_<Agg, Out>(out, emit_fn_, size_ms_);
        inner_.flush(fwd);
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    clink::SlidingWindowOperator<std::int64_t, T, Agg> inner_;
    EmitFn emit_fn_;
    std::int64_t size_ms_;
    std::string name_;
};

// Wrapper around the full TumblingWindowOperator<int64_t, T, Agg>
// that lets the fluent API expose trigger / evictor / lateness /
// late-tag in one place. T-typed records are projected to
// pair<int64_t, T> via the key extractor before delegating; the
// inner's pair<int64_t, Agg> output is stripped back to Agg via a
// forwarding Emitter. Single inner; same RuntimeContext is shared.
//
// Why this wrapper instead of duplicating trigger logic on the
// simpler keyed-aggregate operator: the full operator is the
// canonical implementation (it's the one the lower-level Dag tests
// already cover). The wrapper keeps the fluent path on one code
// path, eliminating the parallel-implementation anti-pattern.
template <typename T, typename Agg>
class KeyedTumblingWindowFullOperator final : public Operator<T, Agg> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using TriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;
    using InnerTriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;

    KeyedTumblingWindowFullOperator(KeyFn key_fn,
                                    InitialFn initial,
                                    CombinerFn combiner,
                                    std::chrono::milliseconds size,
                                    std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size, std::move(initial), std::move(combiner), name + "_inner"),
          name_(std::move(name)) {}

    void set_allowed_lateness(std::chrono::milliseconds v) { inner_.allowed_lateness(v); }
    void set_late_output_tag(OutputTag<T> tag) { inner_.late_output_tag(std::move(tag)); }
    void set_trigger(InnerTriggerPtr t) { inner_.with_trigger(std::move(t)); }

    void open() override {
        // Share the wrapper's runtime context with the inner. The
        // inner expects its runtime() to have a state backend etc;
        // attach_runtime threads it through.
        inner_.attach_runtime(this->runtime());
        inner_.open();
    }

    void close() override {
        inner_.close();
        inner_.attach_runtime(nullptr);
    }

    void process(const StreamElement<T>& element, Emitter<Agg>& out) override {
        auto fwd = make_key_stripping_emitter_<Agg>(out);
        if (element.is_data()) {
            Batch<std::pair<std::int64_t, T>> projected;
            for (const auto& r : element.as_data()) {
                if (r.event_time().has_value()) {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()),
                                      *r.event_time());
                } else {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()));
                }
            }
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::data(std::move(projected)), fwd);
        } else if (element.is_watermark()) {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::watermark(element.as_watermark()),
                fwd);
        } else {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::barrier(element.as_barrier()),
                fwd);
        }
    }

    void flush(Emitter<Agg>& out) override {
        auto fwd = make_key_stripping_emitter_<Agg>(out);
        inner_.flush(fwd);
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    clink::TumblingWindowOperator<std::int64_t, T, Agg> inner_;
    std::string name_;
};

// Emit-form tumbling-window aggregator: like KeyedTumblingWindowFull,
// but the user's emit_fn(key, TimeWindow, agg) -> Out runs at emit
// time so downstream can see the key and window-end. Same inner
// operator underneath; only the Emitter is different.
template <typename T, typename Agg, typename Out>
class KeyedTumblingWindowEmitOperator final : public Operator<T, Out> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using EmitFn = std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)>;
    using TriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;

    // In-memory ctor: working state lives in inner_.mem_ only, no
    // codec-driven persistent backing. Fluent API uses this when no
    // codecs are wired through.
    KeyedTumblingWindowEmitOperator(KeyFn key_fn,
                                    InitialFn initial,
                                    CombinerFn combiner,
                                    EmitFn emit_fn,
                                    std::chrono::milliseconds size,
                                    std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size, std::move(initial), std::move(combiner), name + "_inner"),
          emit_fn_(std::move(emit_fn)),
          size_ms_(size.count()),
          name_(std::move(name)) {}

    // Persistent ctor: working state still lives in mem_ for the hot
    // path, but inner_ also owns a KeyedState<StateKey, Entry> that
    // store_ writes through to (unless CLINK_WB_STATE_CACHE=1
    // suppresses it). Pair this with a job whose JobConfig.state_backend
    // is RocksDB to get per-record durable state backed by the embedded
    // RocksDB state backend.
    KeyedTumblingWindowEmitOperator(KeyFn key_fn,
                                    InitialFn initial,
                                    CombinerFn combiner,
                                    EmitFn emit_fn,
                                    std::chrono::milliseconds size,
                                    clink::Codec<Agg> agg_codec,
                                    std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size,
                 std::move(initial),
                 std::move(combiner),
                 int64_codec(),
                 std::move(agg_codec),
                 name + "_inner"),
          emit_fn_(std::move(emit_fn)),
          size_ms_(size.count()),
          name_(std::move(name)) {}

    void set_allowed_lateness(std::chrono::milliseconds v) { inner_.allowed_lateness(v); }
    void set_late_output_tag(OutputTag<T> tag) { inner_.late_output_tag(std::move(tag)); }
    void set_trigger(TriggerPtr t) { inner_.with_trigger(std::move(t)); }

    void open() override {
        inner_.attach_runtime(this->runtime());
        inner_.open();
    }
    void close() override {
        inner_.close();
        inner_.attach_runtime(nullptr);
    }

    void process(const StreamElement<T>& element, Emitter<Out>& out) override {
        auto fwd = make_window_emit_forwarding_emitter_<Agg, Out>(out, emit_fn_, size_ms_);
        if (element.is_data()) {
            // Drive the inner state machine per record without
            // building a Batch<pair<key, T>> projection - that step
            // copied every Event (its shared_ptr fields each took an
            // atomic refcount bump). process_record splits key
            // extraction from state handling so the value is touched
            // by reference all the way through to combiner_.
            inner_.begin_batch();
            for (const auto& r : element.as_data()) {
                const auto ts =
                    r.event_time().has_value() ? r.event_time()->millis() : std::int64_t{0};
                inner_.process_record(key_fn_(r.value()), r.value(), ts, fwd);
            }
        } else if (element.is_watermark()) {
            inner_.process_watermark(element.as_watermark(), fwd);
        } else {
            inner_.process_barrier(element.as_barrier(), fwd);
        }
    }

    void flush(Emitter<Out>& out) override {
        auto fwd = make_window_emit_forwarding_emitter_<Agg, Out>(out, emit_fn_, size_ms_);
        inner_.flush(fwd);
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    clink::TumblingWindowOperator<std::int64_t, T, Agg> inner_;
    EmitFn emit_fn_;
    std::int64_t size_ms_;
    std::string name_;
};

// Keyed event-time tumbling aggregator - same shape as the sliding
// operator but with a single non-overlapping window per record.
template <typename T, typename Agg>
class KeyedTumblingWindowAggregateOperator final : public Operator<T, Agg> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;

    KeyedTumblingWindowAggregateOperator(KeyFn key_fn,
                                         InitialFn initial,
                                         CombinerFn combiner,
                                         std::chrono::milliseconds size,
                                         std::string name)
        : key_fn_(std::move(key_fn)),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          size_ms_(size.count()),
          name_(std::move(name)) {
        if (size_ms_ <= 0) {
            throw std::invalid_argument(
                "KeyedTumblingWindowAggregateOperator: size must be positive");
        }
    }

    void set_allowed_lateness(std::chrono::milliseconds v) { allowed_lateness_ms_ = v.count(); }
    void set_late_output_tag(OutputTag<T> tag) { late_tag_ = std::move(tag); }

    void process(const StreamElement<T>& element, Emitter<Agg>& out) override {
        if (element.is_data()) {
            Batch<Agg> refires;
            for (const auto& rec : element.as_data()) {
                if (!rec.event_time().has_value()) {
                    continue;
                }
                const std::int64_t t = rec.event_time()->millis();
                const std::int64_t k = key_fn_(rec.value());
                const std::int64_t start = (t / size_ms_) * size_ms_;
                const std::int64_t end = start + size_ms_;
                const std::int64_t purge_at = end + allowed_lateness_ms_;
                if (late_tag_.has_value() && last_watermark_ms_ >= purge_at &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<T>(*late_tag_);
                    Batch<T> b;
                    b.emplace(rec.value(), *rec.event_time());
                    side.emit_data(std::move(b));
                    continue;
                }
                auto& cell = state_[StateKey{k, start}];
                if (!cell.agg.has_value()) {
                    cell.agg = initial_();
                }
                cell.agg = combiner_(*cell.agg, rec.value());
                if (cell.fired) {
                    refires.emplace(*cell.agg, EventTime{end - 1});
                }
            }
            if (!refires.empty()) {
                out.emit_data(std::move(refires));
            }
        } else if (element.is_watermark()) {
            const auto wm = element.as_watermark();
            last_watermark_ms_ = wm.timestamp().millis();
            Batch<Agg> fired;
            for (auto it = state_.begin(); it != state_.end();) {
                const auto window_end = it->first.window_start + size_ms_;
                if (!it->second.fired && window_end <= last_watermark_ms_ &&
                    it->second.agg.has_value()) {
                    fired.emplace(*it->second.agg, EventTime{window_end - 1});
                    it->second.fired = true;
                }
                const auto purge_at = window_end + allowed_lateness_ms_;
                if (last_watermark_ms_ >= purge_at) {
                    it = state_.erase(it);
                } else {
                    ++it;
                }
            }
            if (!fired.empty()) {
                out.emit_data(std::move(fired));
            }
            out.emit_watermark(wm);
        } else {
            out.emit_barrier(element.as_barrier());
        }
    }

    void flush(Emitter<Agg>& out) override {
        Batch<Agg> fired;
        for (auto& [key, cell] : state_) {
            if (!cell.fired && cell.agg.has_value()) {
                fired.emplace(*cell.agg, EventTime{key.window_start + size_ms_ - 1});
            }
        }
        state_.clear();
        if (!fired.empty()) {
            out.emit_data(std::move(fired));
        }
    }

    std::string name() const override { return name_; }

private:
    struct StateKey {
        std::int64_t key;
        std::int64_t window_start;
        bool operator==(const StateKey& o) const noexcept {
            return key == o.key && window_start == o.window_start;
        }
    };
    struct StateKeyHash {
        std::size_t operator()(const StateKey& k) const noexcept {
            auto h = std::hash<std::int64_t>{}(k.key);
            h ^= std::hash<std::int64_t>{}(k.window_start) + 0x9e3779b97f4a7c15ULL + (h << 6) +
                 (h >> 2);
            return h;
        }
    };
    struct Cell {
        std::optional<Agg> agg;
        bool fired{false};
    };

    KeyFn key_fn_;
    InitialFn initial_;
    CombinerFn combiner_;
    std::int64_t size_ms_;
    std::int64_t allowed_lateness_ms_{0};
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};
    std::optional<OutputTag<T>> late_tag_;
    std::string name_;
    std::unordered_map<StateKey, Cell, StateKeyHash> state_;
};

// Keyed event-time session aggregator. Sessions per key are dynamic
// (records within `gap` extend / merge); fires when wm crosses
// session_end + gap; retains until wm crosses end + gap + lateness.
template <typename T, typename Agg>
class KeyedSessionWindowAggregateOperator final : public Operator<T, Agg> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using AggMerger = std::function<Agg(const Agg&, const Agg&)>;

    KeyedSessionWindowAggregateOperator(KeyFn key_fn,
                                        InitialFn initial,
                                        CombinerFn combiner,
                                        AggMerger merger,
                                        std::chrono::milliseconds gap,
                                        std::string name)
        : key_fn_(std::move(key_fn)),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          merger_(std::move(merger)),
          gap_ms_(gap.count()),
          name_(std::move(name)) {
        if (gap_ms_ <= 0) {
            throw std::invalid_argument(
                "KeyedSessionWindowAggregateOperator: gap must be positive");
        }
    }

    void set_allowed_lateness(std::chrono::milliseconds v) { allowed_lateness_ms_ = v.count(); }
    void set_late_output_tag(OutputTag<T> tag) { late_tag_ = std::move(tag); }

    void process(const StreamElement<T>& element, Emitter<Agg>& out) override {
        if (element.is_data()) {
            Batch<Agg> refires;
            for (const auto& rec : element.as_data()) {
                if (!rec.event_time().has_value()) {
                    continue;
                }
                const std::int64_t t = rec.event_time()->millis();
                const std::int64_t k = key_fn_(rec.value());
                // Late-late: wm >= ts + 2*gap + lateness → no session
                // containing ts can still be alive.
                if (late_tag_.has_value() &&
                    last_watermark_ms_ >= t + 2 * gap_ms_ + allowed_lateness_ms_ &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<T>(*late_tag_);
                    Batch<T> b;
                    b.emplace(rec.value(), *rec.event_time());
                    side.emit_data(std::move(b));
                    continue;
                }
                add_record_(k, t, rec.value(), refires);
            }
            if (!refires.empty()) {
                out.emit_data(std::move(refires));
            }
        } else if (element.is_watermark()) {
            const auto wm = element.as_watermark();
            last_watermark_ms_ = wm.timestamp().millis();
            Batch<Agg> fired;
            for (auto& [key, sessions] : sessions_by_key_) {
                std::vector<Session> remaining;
                remaining.reserve(sessions.size());
                for (auto& s : sessions) {
                    if (!s.fired && s.end + gap_ms_ <= last_watermark_ms_) {
                        fired.emplace(s.agg, EventTime{s.end - 1});
                        s.fired = true;
                    }
                    if (s.end + gap_ms_ + allowed_lateness_ms_ <= last_watermark_ms_) {
                        continue;  // purge
                    }
                    remaining.push_back(std::move(s));
                }
                sessions = std::move(remaining);
            }
            if (!fired.empty()) {
                out.emit_data(std::move(fired));
            }
            out.emit_watermark(wm);
        } else {
            out.emit_barrier(element.as_barrier());
        }
    }

    void flush(Emitter<Agg>& out) override {
        Batch<Agg> fired;
        for (auto& [key, sessions] : sessions_by_key_) {
            for (auto& s : sessions) {
                if (!s.fired) {
                    fired.emplace(s.agg, EventTime{s.end - 1});
                }
            }
        }
        sessions_by_key_.clear();
        if (!fired.empty()) {
            out.emit_data(std::move(fired));
        }
    }

    std::string name() const override { return name_; }

private:
    struct Session {
        std::int64_t start{0};
        std::int64_t end{0};
        Agg agg{};
        bool fired{false};
    };

    void add_record_(std::int64_t key, std::int64_t t, const T& v, Batch<Agg>& refires) {
        auto& sessions = sessions_by_key_[key];
        Session merged;
        merged.start = t;
        merged.end = t + 1;
        merged.agg = combiner_(initial_(), v);
        bool any_existing = false;
        std::vector<Session> kept;
        kept.reserve(sessions.size());
        for (auto& s : sessions) {
            const bool overlaps =
                !(s.end + gap_ms_ <= merged.start || merged.end + gap_ms_ <= s.start);
            if (overlaps) {
                if (any_existing) {
                    merged.agg = merger_(merged.agg, s.agg);
                } else {
                    // First overlapping session: merger combines its
                    // accumulated agg with our seeded (initial + v).
                    merged.agg = merger_(s.agg, merged.agg);
                    any_existing = true;
                }
                if (s.start < merged.start) {
                    merged.start = s.start;
                }
                if (s.end > merged.end) {
                    merged.end = s.end;
                }
                if (s.fired) {
                    merged.fired = true;
                }
            } else {
                kept.push_back(std::move(s));
            }
        }
        if (merged.fired) {
            refires.emplace(merged.agg, EventTime{merged.end - 1});
        }
        kept.push_back(std::move(merged));
        sessions = std::move(kept);
    }

    KeyFn key_fn_;
    InitialFn initial_;
    CombinerFn combiner_;
    AggMerger merger_;
    std::int64_t gap_ms_;
    std::int64_t allowed_lateness_ms_{0};
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};
    std::optional<OutputTag<T>> late_tag_;
    std::string name_;
    std::unordered_map<std::int64_t, std::vector<Session>> sessions_by_key_;
};

// Emit-form session-window aggregator. Mirrors
// KeyedSessionWindowAggregateOperator but, at every emit point (refire,
// watermark fire, EOS flush), calls the user's
//   emit_fn(key, TimeWindow{session_start, session_end}, agg) -> Out
// instead of stripping the accumulator down to Agg. We mirror rather
// than wrap the original because the original drops session
// boundaries before emit, leaving the wrapper no way to recover start.
template <typename T, typename Agg, typename Out>
class KeyedSessionWindowEmitOperator final : public Operator<T, Out> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using InitialFn = std::function<Agg()>;
    using CombinerFn = std::function<Agg(const Agg&, const T&)>;
    using AggMerger = std::function<Agg(const Agg&, const Agg&)>;
    using EmitFn = std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)>;

    KeyedSessionWindowEmitOperator(KeyFn key_fn,
                                   InitialFn initial,
                                   CombinerFn combiner,
                                   AggMerger merger,
                                   EmitFn emit_fn,
                                   std::chrono::milliseconds gap,
                                   std::string name)
        : key_fn_(std::move(key_fn)),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          merger_(std::move(merger)),
          emit_fn_(std::move(emit_fn)),
          gap_ms_(gap.count()),
          name_(std::move(name)) {
        if (gap_ms_ <= 0) {
            throw std::invalid_argument("KeyedSessionWindowEmitOperator: gap must be positive");
        }
    }

    void set_allowed_lateness(std::chrono::milliseconds v) { allowed_lateness_ms_ = v.count(); }
    void set_late_output_tag(OutputTag<T> tag) { late_tag_ = std::move(tag); }

    void process(const StreamElement<T>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            Batch<Out> refires;
            for (const auto& rec : element.as_data()) {
                if (!rec.event_time().has_value()) {
                    continue;
                }
                const std::int64_t t = rec.event_time()->millis();
                const std::int64_t k = key_fn_(rec.value());
                if (late_tag_.has_value() &&
                    last_watermark_ms_ >= t + 2 * gap_ms_ + allowed_lateness_ms_ &&
                    this->runtime() != nullptr) {
                    auto side = this->runtime()->template side_output<T>(*late_tag_);
                    Batch<T> b;
                    b.emplace(rec.value(), *rec.event_time());
                    side.emit_data(std::move(b));
                    continue;
                }
                add_record_(k, t, rec.value(), refires);
            }
            if (!refires.empty()) {
                out.emit_data(std::move(refires));
            }
        } else if (element.is_watermark()) {
            const auto wm = element.as_watermark();
            last_watermark_ms_ = wm.timestamp().millis();
            Batch<Out> fired;
            for (auto& [key, sessions] : sessions_by_key_) {
                std::vector<Session> remaining;
                remaining.reserve(sessions.size());
                for (auto& s : sessions) {
                    if (!s.fired && s.end + gap_ms_ <= last_watermark_ms_) {
                        fired.emplace(emit_fn_(key, clink::TimeWindow{s.start, s.end}, s.agg),
                                      EventTime{s.end - 1});
                        s.fired = true;
                    }
                    if (s.end + gap_ms_ + allowed_lateness_ms_ <= last_watermark_ms_) {
                        continue;  // purge
                    }
                    remaining.push_back(std::move(s));
                }
                sessions = std::move(remaining);
            }
            if (!fired.empty()) {
                out.emit_data(std::move(fired));
            }
            out.emit_watermark(wm);
        } else {
            out.emit_barrier(element.as_barrier());
        }
    }

    void flush(Emitter<Out>& out) override {
        Batch<Out> fired;
        for (auto& [key, sessions] : sessions_by_key_) {
            for (auto& s : sessions) {
                if (!s.fired) {
                    fired.emplace(emit_fn_(key, clink::TimeWindow{s.start, s.end}, s.agg),
                                  EventTime{s.end - 1});
                }
            }
        }
        sessions_by_key_.clear();
        if (!fired.empty()) {
            out.emit_data(std::move(fired));
        }
    }

    std::string name() const override { return name_; }

private:
    struct Session {
        std::int64_t start{0};
        std::int64_t end{0};
        Agg agg{};
        bool fired{false};
    };

    void add_record_(std::int64_t key, std::int64_t t, const T& v, Batch<Out>& refires) {
        auto& sessions = sessions_by_key_[key];
        Session merged;
        merged.start = t;
        merged.end = t + 1;
        merged.agg = combiner_(initial_(), v);
        bool any_existing = false;
        std::vector<Session> kept;
        kept.reserve(sessions.size());
        for (auto& s : sessions) {
            const bool overlaps =
                !(s.end + gap_ms_ <= merged.start || merged.end + gap_ms_ <= s.start);
            if (overlaps) {
                if (any_existing) {
                    merged.agg = merger_(merged.agg, s.agg);
                } else {
                    merged.agg = merger_(s.agg, merged.agg);
                    any_existing = true;
                }
                if (s.start < merged.start) {
                    merged.start = s.start;
                }
                if (s.end > merged.end) {
                    merged.end = s.end;
                }
                if (s.fired) {
                    merged.fired = true;
                }
            } else {
                kept.push_back(std::move(s));
            }
        }
        if (merged.fired) {
            refires.emplace(emit_fn_(key, clink::TimeWindow{merged.start, merged.end}, merged.agg),
                            EventTime{merged.end - 1});
        }
        kept.push_back(std::move(merged));
        sessions = std::move(kept);
    }

    KeyFn key_fn_;
    InitialFn initial_;
    CombinerFn combiner_;
    AggMerger merger_;
    EmitFn emit_fn_;
    std::int64_t gap_ms_;
    std::int64_t allowed_lateness_ms_{0};
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};
    std::optional<OutputTag<T>> late_tag_;
    std::string name_;
    std::unordered_map<std::int64_t, std::vector<Session>> sessions_by_key_;
};

// Wrapper around EvictingTumblingWindowOperator<int64_t, T, Out>.
// Output type is Out (the ProcessFn's result type) - not Agg, since
// the evicting path is a process-the-bucket model, not an aggregate.
// Same key-projection / strip-Out emitter pattern as the
// aggregate-path wrappers above.
template <typename T, typename Out>
class KeyedEvictingTumblingWindowFullOperator final : public Operator<T, Out> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using ProcessFn =
        std::function<Out(const std::vector<Record<T>>& records, const clink::TimeWindow& window)>;
    using EvictorPtr = std::unique_ptr<clink::Evictor<T, clink::TimeWindow>>;
    using TriggerPtr = std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>;

    KeyedEvictingTumblingWindowFullOperator(KeyFn key_fn,
                                            std::chrono::milliseconds size,
                                            ProcessFn process_fn,
                                            EvictorPtr evictor,
                                            std::string name)
        : key_fn_(std::move(key_fn)),
          inner_(size, std::move(process_fn), std::move(evictor), name + "_inner"),
          name_(std::move(name)) {}

    void set_allowed_lateness(std::chrono::milliseconds v) { inner_.allowed_lateness(v); }
    void set_late_output_tag(OutputTag<T> tag) { inner_.late_output_tag(std::move(tag)); }
    void set_trigger(TriggerPtr t) { inner_.with_trigger(std::move(t)); }

    void open() override {
        inner_.attach_runtime(this->runtime());
        inner_.open();
    }

    void close() override {
        inner_.close();
        inner_.attach_runtime(nullptr);
    }

    void process(const StreamElement<T>& element, Emitter<Out>& out) override {
        // Strip-and-forward: inner emits pair<int64_t, Out>; we
        // forward Out.
        clink::Emitter<std::pair<std::int64_t, Out>> fwd(
            typename clink::Emitter<std::pair<std::int64_t, Out>>::Forward(
                [&out](clink::StreamElement<std::pair<std::int64_t, Out>> e) -> bool {
                    if (e.is_data()) {
                        Batch<Out> stripped;
                        for (auto& r : e.as_data()) {
                            if (r.event_time().has_value()) {
                                stripped.emplace(r.value().second, *r.event_time());
                            } else {
                                stripped.emplace(r.value().second);
                            }
                        }
                        if (!stripped.empty()) {
                            out.emit_data(std::move(stripped));
                        }
                    } else if (e.is_watermark()) {
                        out.emit_watermark(e.as_watermark());
                    } else {
                        out.emit_barrier(e.as_barrier());
                    }
                    return true;
                }));
        if (element.is_data()) {
            Batch<std::pair<std::int64_t, T>> projected;
            for (const auto& r : element.as_data()) {
                if (r.event_time().has_value()) {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()),
                                      *r.event_time());
                } else {
                    projected.emplace(std::make_pair(key_fn_(r.value()), r.value()));
                }
            }
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::data(std::move(projected)), fwd);
        } else if (element.is_watermark()) {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::watermark(element.as_watermark()),
                fwd);
        } else {
            inner_.process(
                clink::StreamElement<std::pair<std::int64_t, T>>::barrier(element.as_barrier()),
                fwd);
        }
    }

    std::string name() const override { return name_; }

private:
    KeyFn key_fn_;
    clink::EvictingTumblingWindowOperator<std::int64_t, T, Out> inner_;
    std::string name_;
};

}  // namespace detail

// DataStream<T> is the user-facing typed handle to one logical stage of
// the pipeline. T is the channel's element type; the handle carries the
// owning env pointer and the upstream op id so subsequent fluent calls
// know what to wire as their input.
//
// Methods append a new OperatorSpec to the env and return a new typed
// handle (the next stage). Handles are cheap value types; lose them at
// will, the env owns the graph.
template <typename T>
class DataStream {
public:
    DataStream(StreamExecutionEnvironment* env, std::string upstream_id, std::string channel_type)
        : env_(env), upstream_id_(std::move(upstream_id)), channel_type_(std::move(channel_type)) {}

    // Append a registered operator that converts T -> U. The named op
    // must be findable in the OperatorRegistry / RunnerRegistry on the
    // cluster (built-in or plugin-supplied).
    template <typename U>
    DataStream<U> transform(std::string op_type,
                            std::map<std::string, std::string> params = {},
                            std::string id = {}) {
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = std::move(op_type);
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<U>::get();
        op.params = std::move(params);
        op.parallelism = env_->default_parallelism();
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Inline 1:1 map. Convenience over .transform<U>(op_type) - mints a
    // unique op-type name, registers a MapOperator<T, U> factory holding
    // `fn` into the process-wide RunnerRegistry, and chains. T and U
    // must already be registered as channel types (the built-ins
    // std::int64_t / std::string are covered by
    // ensure_built_ins_registered(); for custom types call
    // PluginRegistry::register_type<X>(name, codec) first).
    //
    // The lambda is captured by value into the registered runner closure,
    // which lives for the process lifetime. Submitting a job that
    // references this op-type to a remote cluster will fail there
    // because the remote TM has no such registration - for cross-process
    // jobs, package your operator into a real plugin instead.
    template <typename U>
    DataStream<U> map(std::function<U(const T&)> fn, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("map");
        auto& reg = env_->registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn = std::move(fn),
             op_type](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<MapOperator<T, U>>(fn, op_type);
            });
        return transform<U>(op_type, {}, std::move(id));
    }

    // ProcessFunction-style transform. Mirrors
    // dataStream.process(new ProcessFunction<>...). Wraps the supplied
    // ProcessFunction<T, U> in an adapter operator that gives the user
    // a Collector (for emit-many), per-record timestamp, side-output
    // emitter access, and registration of processing-time / event-time
    // timers. Same in-process-only contract as .map() / .flat_map() -
    // for cross-cluster jobs the function must live in a real plugin.
    template <typename U>
    DataStream<U> process(std::shared_ptr<ProcessFunction<T, U>> fn, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("process");
        auto& reg = env_->registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn, op_type](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<clink::detail::ProcessFunctionAdapter<T, U>>(fn, op_type);
            });
        return transform<U>(op_type, {}, std::move(id));
    }

    // Inline 1:N map. Each input record produces a vector<U>; an empty
    // vector drops the record (filter-with-transform). Same constraints
    // as .map() - T and U must already be registered as channel types.
    template <typename U>
    DataStream<U> flat_map(std::function<std::vector<U>(const T&)> fn, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("flat_map");
        auto& reg = env_->registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn = std::move(fn),
             op_type](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<FlatMapOperator<T, U>>(fn, op_type);
            });
        return transform<U>(op_type, {}, std::move(id));
    }

    // Inline predicate filter. Records for which `pred` returns true pass
    // through unchanged; the rest are dropped. Watermarks and barriers
    // always forward. Same in-process-only contract as .map() - the
    // predicate captures live in this process's RunnerRegistry singleton.
    DataStream<T> filter(std::function<bool(const T&)> pred, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("filter");
        auto& reg = env_->registry();
        reg.template register_operator<T, T>(
            op_type,
            [pred = std::move(pred),
             op_type](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, T>> {
                return std::make_shared<FilterOperator<T>>(pred, op_type);
            });
        return transform<T>(op_type, {}, std::move(id));
    }

    // Assign event-time + emit monotonic watermarks. Each record's
    // event_time is set to `extractor(record.value())` (if not already
    // present), and watermarks track max-seen event-time. Mirrors
    // WatermarkStrategy.forMonotonousTimestamps()
    // .withTimestampAssigner(...).
    //
    // Returns DataStream<T> - type unchanged, but downstream operators
    // can now rely on records carrying event_time + receiving
    // watermarks. Required upstream of any windowed operator.
    DataStream<T> assign_timestamps_monotonic(std::function<EventTime(const T&)> extractor,
                                              std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("ts_monotonic");
        auto& reg = env_->registry();
        reg.template register_operator<T, T>(
            op_type,
            [extractor = std::move(extractor),
             op_type](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, T>> {
                return std::make_shared<WatermarkAssignerOperator<T>>(
                    extractor, std::make_unique<MonotonicWatermarkStrategy<T>>(), op_type);
            });
        return transform<T>(op_type, {}, std::move(id));
    }

    // Same as above but tolerates events arriving up to `bound` ms
    // late: watermark = max-seen event-time - bound. Mirrors
    // WatermarkStrategy.forBoundedOutOfOrderness(bound)
    // .withTimestampAssigner(...).
    DataStream<T> assign_timestamps_bounded(std::function<EventTime(const T&)> extractor,
                                            std::chrono::milliseconds bound,
                                            std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("ts_bounded");
        auto& reg = env_->registry();
        reg.template register_operator<T, T>(
            op_type,
            [extractor = std::move(extractor), bound, op_type](
                const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, T>> {
                return std::make_shared<WatermarkAssignerOperator<T>>(
                    extractor, std::make_unique<BoundedOutOfOrdernessStrategy<T>>(bound), op_type);
            });
        return transform<T>(op_type, {}, std::move(id));
    }

    // Mark this stream as keyed by the named extractor (registered via
    // PluginRegistry::register_key_extractor<T>). Returns a
    // KeyedDataStream<T> handle whose .process<U>() / .sink() calls
    // emit ops whose `key_by` field is set, forcing the planner to use
    // Hash routing on the incoming edges.
    //
    // Same key always lands on the same downstream subtask -> keyed
    // state with K=hash(T) is correct at parallelism > 1.
    KeyedDataStream<T> key_by(std::string extractor_name) const;

    // Inline key_by. Mints a unique extractor name, registers `fn` into
    // the process-wide KeyExtractorRegistry, returns a KeyedDataStream<T>.
    // Same in-process-only contract as the other inline-lambda fluent
    // shortcuts - the lambda lives in this process's registry.
    KeyedDataStream<T> key_by(std::function<std::int64_t(const T&)> fn) const;

    // Terminate the stream into a sink. Returns void - sinks don't have
    // downstream handles.
    void sink(SinkDescriptor desc, std::string id = {}) {
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = std::move(desc.op_type);
        op.inputs = {upstream_id_};
        op.out_channel = desc.channel_type.empty() ? ChannelName<T>::get() : desc.channel_type;
        op.params = std::move(desc.params);
        op.parallelism = (desc.parallelism > 1) ? desc.parallelism : env_->default_parallelism();
        env_->append_op(std::move(op));
    }

    // Attach a human-readable display name to the most-recent op
    // (the one this DataStream<T> handle points at). Returns *this
    // for chaining: stream.map<int>(fn).name("multiply").uid("...");
    DataStream<T>& name(std::string display_name) {
        env_->set_op_display_name(upstream_id_, std::move(display_name));
        return *this;
    }

    // Attach a stable identifier (`.uid("...")`) to the most-
    // recent op. The runtime derives the OperatorId from this string
    // so keyed state survives topology edits. Strongly recommended
    // for any stateful operator the user expects to outlive a
    // savepoint. Throws on uid collision within the same env.
    DataStream<T>& uid(std::string stable_id) {
        env_->set_op_uid(upstream_id_, std::move(stable_id));
        return *this;
    }

    // Declare a named side output on the most-recent op and return a
    // DataStream<U> for the side channel. Mirrors
    // SingleOutputStreamOperator.getSideOutput(OutputTag<U>).
    //
    // The op pointed at by this DataStream - typically the result of
    // .process(my_process_function) - emits records of type U to the
    // tag via `ctx.side_output<U>(tag)` inside its ProcessFunction. The
    // returned DataStream<U> reads from the channel "<op_id>::<tag.id>",
    // which the planner already understands; chain .map(), .sink(),
    // etc. on it normally.
    //
    // Constraints (same as .map<U>(...) for inline ops):
    //   * U must be a registered channel type. ensure_built_ins_registered()
    //     covers std::int64_t / std::string; custom types need
    //     PluginRegistry::register_type<U>(name, codec[, batcher]).
    //   * Idempotent: re-calling with the same OutputTag<U> on the
    //     same upstream just returns another handle, no duplicate
    //     SideOutputDecl appears.
    template <typename U>
    DataStream<U> side_output(const OutputTag<U>& tag) {
        cluster::ensure_built_ins_registered();
        env_->declare_side_output_on(upstream_id_, tag.id, ChannelName<U>::get());
        return DataStream<U>(env_, upstream_id_ + "::" + tag.id, ChannelName<U>::get());
    }

    [[nodiscard]] const std::string& id() const noexcept { return upstream_id_; }
    [[nodiscard]] const std::string& channel_type() const noexcept { return channel_type_; }
    [[nodiscard]] StreamExecutionEnvironment* env() const noexcept { return env_; }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
};

// KeyedDataStream<T> is the partitioning marker the fluent API hands to
// the user after a .key_by() call. Any subsequent .process<U>() /
// .sink() / .key_by() emits an op whose `key_by` field is set, which
// signals the planner to use RoutingMode::Hash on the incoming edges.
// Once you key, the partitioning sticks until you start a new lineage.
template <typename T>
class KeyedDataStream {
public:
    KeyedDataStream(StreamExecutionEnvironment* env,
                    std::string upstream_id,
                    std::string channel_type,
                    std::string key_by)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)) {}

    // Append a registered keyed operator T -> U. Emits an op with
    // `key_by` set, forcing Hash routing from the upstream.
    template <typename U>
    DataStream<U> process(std::string op_type,
                          std::map<std::string, std::string> params = {},
                          std::string id = {}) {
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = std::move(op_type);
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<U>::get();
        op.params = std::move(params);
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Typed-K overload. The user supplies a typed extractor
    // `key_fn` that turns a T into the K the KeyedProcessFunction was
    // templated on. Routing across subtasks still uses the int64
    // extractor attached to this KeyedDataStream<T> via .key_by(), so
    // the partitioning is identical to the int64 form - typing only
    // affects what current_key() returns inside the function. This is
    // the right shape for string keys, struct keys, etc. without
    // having to add a typed key-extractor registry.
    template <typename K, typename U>
    DataStream<U> process(std::shared_ptr<KeyedProcessFunction<K, T, U>> fn,
                          std::function<K(const T&)> key_fn,
                          std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("keyed_process_typed");
        auto& reg = env_->registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn, key_fn, op_type](
                const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<clink::detail::KeyedProcessFunctionAdapter<K, T, U>>(
                    fn, key_fn, /*timer_key_fn=*/nullptr, op_type);
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Async-state KeyedProcessFunction transform. Registers the
    // AsyncKeyedProcessFunctionAdapter, which runs the user's coroutine
    // process_element under the per-key gate (co_await async state reads
    // overlap across keys) and routes its synchronous on_timer through the
    // gated processing-time / epoch-gated event-time paths. Needs a Codec<K>
    // for the gate-key/timer-key encode+decode (the one argument the sync
    // typed process() does not carry). Partitioning still uses the int64
    // extractor from .key_by(); the throughput win materialises only on a
    // deferring backend.
    template <typename K, typename U>
    DataStream<U> process_async(std::shared_ptr<AsyncKeyedProcessFunction<K, T, U>> fn,
                                std::function<K(const T&)> key_fn,
                                Codec<K> key_codec,
                                std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("async_keyed_process_typed");
        auto& reg = env_->registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn, key_fn, key_codec, op_type](
                const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<clink::detail::AsyncKeyedProcessFunctionAdapter<K, T, U>>(
                    fn, key_fn, key_codec, op_type);
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Inline KeyedProcessFunction transform. Mirrors
    // keyedStream.process(new KeyedProcessFunction<>...). This overload
    // uses the int64 extractor already registered via .key_by(...) -
    // current_key() inside process_element / on_timer returns int64.
    // For typed (non-int64) keys, use the overload above that takes
    // an explicit K-returning extractor.
    template <typename U>
    DataStream<U> process(std::shared_ptr<KeyedProcessFunction<std::int64_t, T, U>> fn,
                          std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("keyed_process");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, U>(
            op_type,
            [fn, op_type, channel, key_name, key_reg](
                const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline keyed process: extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                return std::make_shared<
                    clink::detail::KeyedProcessFunctionAdapter<std::int64_t, T, U>>(
                    fn,
                    [extractor](const T& v) { return extractor(v); },
                    /*timer_key_fn=*/nullptr,
                    op_type);
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Open a keyed event-time sliding window with span `size` and step
    // `slide` (both in milliseconds). The handle returned carries the
    // window spec; the operator only materialises on the subsequent
    // .aggregate<Agg>() call, mirroring // keyedStream.window(SlidingEventTimeWindows.of(...))
    // shape.
    SlidingWindowedDataStream<T> sliding_window(std::chrono::milliseconds size,
                                                std::chrono::milliseconds slide) const {
        return SlidingWindowedDataStream<T>(
            env_, upstream_id_, channel_type_, key_by_, size, slide);
    }

    // Open a keyed event-time tumbling window of `size`. Mirrors
    // keyedStream.window(TumblingEventTimeWindows.of(size)).
    TumblingWindowedDataStream<T> tumbling_window(std::chrono::milliseconds size) const {
        return TumblingWindowedDataStream<T>(env_, upstream_id_, channel_type_, key_by_, size);
    }

    // Open a keyed event-time session window with idle gap `gap`.
    // Mirrors keyedStream.window(EventTimeSessionWindows.withGap(gap)).
    SessionWindowedDataStream<T> session_window(std::chrono::milliseconds gap) const {
        return SessionWindowedDataStream<T>(env_, upstream_id_, channel_type_, key_by_, gap);
    }

    // Inline reduce. For each input record, look up the key (via the
    // extractor name already attached to this KeyedDataStream<T>),
    // combine with the running per-key state using `fn`, emit the new
    // accumulated value. First record for a key seeds the state with
    // that record's value. Output: DataStream<T> (no longer keyed).
    //
    // The key extractor must be findable in the
    // KeyExtractorRegistry at the time the operator runs - that means
    // either it was registered inline via .key_by(lambda) earlier or
    // the user registered it by name via PluginRegistry::
    // register_key_extractor<T>. Same in-process-only contract as the
    // other inline-lambda shortcuts.
    DataStream<T> reduce(std::function<T(const T&, const T&)> fn, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("reduce");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        auto& reg = env_->registry();
        // Capture the KeyExtractorRegistry reference held by the env's
        // PluginRegistry, NOT a process-wide default_instance(). Inside
        // a plugin .so (RTLD_LOCAL + static clink_core),
        // KeyExtractorRegistry::default_instance() returns the .so's
        // private static - which won't see registrations the host made
        // through us. Capturing the host's ref avoids that trap.
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, T>(
            op_type,
            [fn = std::move(fn), op_type, channel, key_name, key_reg](
                const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, T>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error(
                        "inline reduce: key extractor '" + key_name + "' for channel '" + channel +
                        "' is not registered (call .key_by(...) before .reduce(...))");
                }
                return std::make_shared<detail::KeyedReduceOperator<T>>(
                    std::move(extractor), fn, op_type);
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<T>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<T>(env_, std::move(new_id), ChannelName<T>::get());
    }

    // Connect with another KeyedDataStream<T2> sharing the same key
    // extractor name -> co-process function. Both upstreams must already
    // be keyed by the same extractor name (one per upstream channel
    // type) for routing to land matching keys on the same subtask.
    template <typename T2, typename U>
    DataStream<U> connect(const KeyedDataStream<T2>& other,
                          std::string op_type,
                          std::map<std::string, std::string> params = {},
                          std::string id = {}) {
        if (key_by_ != other.key_by()) {
            throw std::runtime_error(
                "KeyedDataStream::connect: key extractor names must match (got '" + key_by_ +
                "' vs '" + other.key_by() + "')");
        }
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = std::move(op_type);
        op.inputs = {upstream_id_, other.id()};
        op.out_channel = ChannelName<U>::get();
        op.params = std::move(params);
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Inline KeyedCoProcessFunction connect. Mirrors
    // keyedA.connect(keyedB).process(new KeyedCoProcessFunction<>...)
    // and the typed-K KeyedProcessFunction overload above. Each side
    // supplies its own per-record key extractor for typed-K access
    // inside process_element1 / process_element2 / on_timer. The
    // optional `timer_key_fn` decodes the timer-string back into K
    // so on_timer sees the typed key (mirrors register_keyed_co_operator).
    //
    // Both upstreams must already be keyed by the same int64 extractor
    // name (set via .key_by(name)) for routing - typing only affects
    // what `current_key()` returns inside the function body.
    template <typename T2, typename K, typename U>
    DataStream<U> connect_process(const KeyedDataStream<T2>& other,
                                  std::shared_ptr<KeyedCoProcessFunction<K, T, T2, U>> fn,
                                  std::function<K(const T&)> key1,
                                  std::function<K(const T2&)> key2,
                                  std::function<K(const std::string&)> timer_key_fn = nullptr,
                                  std::string id = {}) {
        if (key_by_ != other.key_by()) {
            throw std::runtime_error(
                "KeyedDataStream::connect_process: key extractor names must match (got '" +
                key_by_ + "' vs '" + other.key_by() + "')");
        }
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("keyed_co_process");
        auto& reg = env_->registry();
        reg.template register_keyed_co_operator<K, T, T2, U>(
            op_type,
            [fn](const clink::plugin::BuildContext&) { return fn; },
            std::move(key1),
            std::move(key2),
            std::move(timer_key_fn));
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_, other.id()};
        op.out_channel = ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Async-state inline KeyedCoProcessFunction connect. Like connect_process
    // but registers the AsyncKeyedCoProcessFunctionAdapter, whose
    // process_element{1,2} co_await keyed-state reads under the shared per-key
    // gate. Needs a Codec<K> for the gate-key/timer-key encoding (the one extra
    // argument vs the sync connect_process); the throughput win materialises
    // only on a deferring backend.
    template <typename T2, typename K, typename U>
    DataStream<U> connect_process_async(
        const KeyedDataStream<T2>& other,
        std::shared_ptr<AsyncKeyedCoProcessFunction<K, T, T2, U>> fn,
        std::function<K(const T&)> key1,
        std::function<K(const T2&)> key2,
        Codec<K> key_codec,
        std::string id = {}) {
        if (key_by_ != other.key_by()) {
            throw std::runtime_error(
                "KeyedDataStream::connect_process_async: key extractor names must match (got '" +
                key_by_ + "' vs '" + other.key_by() + "')");
        }
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("async_keyed_co_process");
        auto& reg = env_->registry();
        reg.template register_async_keyed_co_operator<K, T, T2, U>(
            op_type,
            [fn](const clink::plugin::BuildContext&) { return fn; },
            std::move(key1),
            std::move(key2),
            std::move(key_codec));
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_, other.id()};
        op.out_channel = ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return DataStream<U>(env_, std::move(new_id), ChannelName<U>::get());
    }

    // Terminal sink. The sink op carries `key_by` so its inputs hash-
    // partition and the sink's per-subtask state stays consistent.
    void sink(SinkDescriptor desc, std::string id = {}) {
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = std::move(desc.op_type);
        op.inputs = {upstream_id_};
        op.out_channel = desc.channel_type.empty() ? ChannelName<T>::get() : desc.channel_type;
        op.params = std::move(desc.params);
        op.parallelism = (desc.parallelism > 1) ? desc.parallelism : env_->default_parallelism();
        op.key_by = key_by_;
        env_->append_op(std::move(op));
    }

    // Same .name() / .uid() shape as DataStream<T> - see that class
    // for semantics. Attaches metadata to the most-recent op (which
    // is what this handle points at).
    KeyedDataStream<T>& name(std::string display_name) {
        env_->set_op_display_name(upstream_id_, std::move(display_name));
        return *this;
    }

    KeyedDataStream<T>& uid(std::string stable_id) {
        env_->set_op_uid(upstream_id_, std::move(stable_id));
        return *this;
    }

    // Declare a side output on the keyed op and return a DataStream<U>
    // for the side channel. Same shape and constraints as
    // DataStream<T>::side_output<U>(); see that overload for details.
    // The returned stream is NOT keyed - the side channel is its own
    // independent lineage.
    template <typename U>
    DataStream<U> side_output(const OutputTag<U>& tag) {
        cluster::ensure_built_ins_registered();
        env_->declare_side_output_on(upstream_id_, tag.id, ChannelName<U>::get());
        return DataStream<U>(env_, upstream_id_ + "::" + tag.id, ChannelName<U>::get());
    }

    [[nodiscard]] const std::string& id() const noexcept { return upstream_id_; }
    [[nodiscard]] const std::string& channel_type() const noexcept { return channel_type_; }
    [[nodiscard]] const std::string& key_by() const noexcept { return key_by_; }
    [[nodiscard]] StreamExecutionEnvironment* env() const noexcept { return env_; }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
};

// Transient handle returned by KeyedDataStream<T>::sliding_window().
// Captures the window spec; the actual op only materialises on the
// subsequent .aggregate<Agg>() call. Modeled after // WindowedStream<T, K, W>; the W type parameter
// is collapsed since v1 only supports event-time sliding windows.
template <typename T>
class SlidingWindowedDataStream {
public:
    SlidingWindowedDataStream(StreamExecutionEnvironment* env,
                              std::string upstream_id,
                              std::string channel_type,
                              std::string key_by,
                              std::chrono::milliseconds size,
                              std::chrono::milliseconds slide)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)),
          size_(size),
          slide_(slide) {}

    // Override the parallelism of the aggregate operator. Defaults to 1.
    // Setting > 1 enables true hash partitioning across subtasks for
    // the key extractor attached upstream.
    SlidingWindowedDataStream& parallelism(std::uint32_t p) {
        parallelism_ = p;
        return *this;
    }

    // Retain each window's bucket for `v` after window_end so late
    // records within the band re-fire the window with the updated
    // aggregate. Mirrors
    // .allowedLateness(Time.milliseconds(...)). Default 0 = single
    // fire on watermark crossing.
    SlidingWindowedDataStream& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Route records arriving past every covering window's
    // (end + allowed_lateness) to the named side output. Mirrors
    // .sideOutputLateData(OutputTag). Without a tag,
    // late-late records fall through to the historic create-fresh-
    // bucket path.
    SlidingWindowedDataStream& late_output_tag(OutputTag<T> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // Replace the default EventTimeTrigger with a user-supplied
    // trigger. Takes a FACTORY (function returning a fresh
    // unique_ptr<Trigger<T, TimeWindow>>) because each operator
    // instantiation - one per subtask at parallelism > 1 - needs
    // its own trigger instance. Mirrors
    // `.trigger(Trigger.of(...))`.
    using TriggerFactory = std::function<std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>()>;
    SlidingWindowedDataStream& with_trigger(TriggerFactory factory) {
        trigger_factory_ = std::move(factory);
        return *this;
    }

    // Fold each record into a per-(key, window) accumulator. `initial`
    // produces the seed accumulator; `combiner` folds a T into it. On
    // watermark, windows with end <= watermark fire one Agg record
    // each; the operator also flushes any remaining windows on EOS.
    // Returns DataStream<Agg> - the keyed marker doesn't propagate.
    template <typename Agg>
    DataStream<Agg> aggregate(std::function<Agg()> initial,
                              std::function<Agg(const Agg&, const T&)> combiner,
                              std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("sliding_aggregate");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto size = size_;
        const auto slide = slide_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        const auto trigger_factory = trigger_factory_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Agg>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             op_type,
             channel,
             key_name,
             size,
             slide,
             lateness,
             late_tag,
             trigger_factory,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Agg>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline sliding_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                auto op = std::make_shared<detail::KeyedSlidingWindowFullOperator<T, Agg>>(
                    std::move(extractor), initial, combiner, size, slide, op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                if (trigger_factory) {
                    op->set_trigger(trigger_factory());
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Agg>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Agg>(env_, std::move(new_id), ChannelName<Agg>::get());
    }

    // Emit-form: like aggregate<Agg> but invokes emit_fn(key, window, agg)
    // per (key, window) emit. Out must be a registered channel type.
    template <typename Agg, typename Out>
    DataStream<Out> aggregate(
        std::function<Agg()> initial,
        std::function<Agg(const Agg&, const T&)> combiner,
        std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)> emit_fn,
        std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("sliding_aggregate_emit");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto size = size_;
        const auto slide = slide_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        const auto trigger_factory = trigger_factory_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Out>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             emit_fn = std::move(emit_fn),
             op_type,
             channel,
             key_name,
             size,
             slide,
             lateness,
             late_tag,
             trigger_factory,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Out>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline sliding_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                auto op = std::make_shared<detail::KeyedSlidingWindowEmitOperator<T, Agg, Out>>(
                    std::move(extractor), initial, combiner, emit_fn, size, slide, op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                if (trigger_factory) {
                    op->set_trigger(trigger_factory());
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Out>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Out>(env_, std::move(new_id), ChannelName<Out>::get());
    }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
    std::chrono::milliseconds size_;
    std::chrono::milliseconds slide_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<T>> late_tag_;
    TriggerFactory trigger_factory_;
    std::uint32_t parallelism_{1};
};

// Tumbling window handle - same shape as SlidingWindowedDataStream
// but with a single non-overlapping window.
template <typename T>
class TumblingWindowedDataStream {
public:
    TumblingWindowedDataStream(StreamExecutionEnvironment* env,
                               std::string upstream_id,
                               std::string channel_type,
                               std::string key_by,
                               std::chrono::milliseconds size)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)),
          size_(size) {}

    TumblingWindowedDataStream& parallelism(std::uint32_t p) {
        parallelism_ = p;
        return *this;
    }

    TumblingWindowedDataStream& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    TumblingWindowedDataStream& late_output_tag(OutputTag<T> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // Replace the default EventTimeTrigger with a user-supplied
    // trigger via a factory. See SlidingWindowedDataStream::with_trigger
    // for rationale.
    using TriggerFactory = std::function<std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>()>;
    TumblingWindowedDataStream& with_trigger(TriggerFactory factory) {
        trigger_factory_ = std::move(factory);
        return *this;
    }

    // Switch to the evicting-window path: per-window records are
    // buffered, the evictor filters them at trigger fire time, and
    // the user's process function runs on the surviving set to emit
    // Out. Returns a new handle (EvictingTumblingWindowedDataStream)
    // whose .process<Out>(fn) terminates the build. The factory
    // pattern matches with_trigger - each operator instance gets a
    // fresh evictor.
    using EvictorFactory = std::function<std::unique_ptr<clink::Evictor<T, clink::TimeWindow>>()>;
    EvictingTumblingWindowedDataStream<T> evicting(EvictorFactory factory) const;

    template <typename Agg>
    DataStream<Agg> aggregate(std::function<Agg()> initial,
                              std::function<Agg(const Agg&, const T&)> combiner,
                              std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("tumbling_aggregate");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto size = size_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        const auto trigger_factory = trigger_factory_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Agg>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             op_type,
             channel,
             key_name,
             size,
             lateness,
             late_tag,
             trigger_factory,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Agg>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline tumbling_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                auto op = std::make_shared<detail::KeyedTumblingWindowFullOperator<T, Agg>>(
                    std::move(extractor), initial, combiner, size, op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                if (trigger_factory) {
                    op->set_trigger(trigger_factory());
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Agg>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Agg>(env_, std::move(new_id), ChannelName<Agg>::get());
    }

    // Emit-form: like aggregate<Agg> but invokes emit_fn(key, window, agg)
    // at every fire so downstream sees the key and window-end alongside
    // the accumulator. Out must be a registered channel type
    // (PluginRegistry::register_type<Out>(...)).
    template <typename Agg, typename Out>
    DataStream<Out> aggregate(
        std::function<Agg()> initial,
        std::function<Agg(const Agg&, const T&)> combiner,
        std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)> emit_fn,
        std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("tumbling_aggregate_emit");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto size = size_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        const auto trigger_factory = trigger_factory_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        // Resolve the Agg codec at registration time. If present
        // (the user registered Agg via env.registry().register_type
        // before this aggregate call), the operator's persistent
        // ctor wires through a KeyedState<...> backed by the job's
        // state_backend, giving per-record durability via the embedded
        // RocksDB state backend. If absent, fall back to the in-memory ctor.
        std::shared_ptr<const Codec<Agg>> agg_codec = reg.template codec_for<Agg>();
        reg.template register_operator<T, Out>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             emit_fn = std::move(emit_fn),
             agg_codec,
             op_type,
             channel,
             key_name,
             size,
             lateness,
             late_tag,
             trigger_factory,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Out>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline tumbling_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                std::shared_ptr<detail::KeyedTumblingWindowEmitOperator<T, Agg, Out>> op;
                if (agg_codec) {
                    op = std::make_shared<detail::KeyedTumblingWindowEmitOperator<T, Agg, Out>>(
                        std::move(extractor),
                        initial,
                        combiner,
                        emit_fn,
                        size,
                        *agg_codec,
                        op_type);
                } else {
                    op = std::make_shared<detail::KeyedTumblingWindowEmitOperator<T, Agg, Out>>(
                        std::move(extractor), initial, combiner, emit_fn, size, op_type);
                }
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                if (trigger_factory) {
                    op->set_trigger(trigger_factory());
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Out>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Out>(env_, std::move(new_id), ChannelName<Out>::get());
    }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
    std::chrono::milliseconds size_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<T>> late_tag_;
    TriggerFactory trigger_factory_;
    std::uint32_t parallelism_{1};
};

// EvictingTumbling window handle - distinct from
// TumblingWindowedDataStream because the underlying
// EvictingTumblingWindowOperator uses a ProcessFn(records, window)
// → Out model instead of accumulator-style aggregation. Reached via
// TumblingWindowedDataStream::evicting(factory).
template <typename T>
class EvictingTumblingWindowedDataStream {
public:
    using EvictorFactory = std::function<std::unique_ptr<clink::Evictor<T, clink::TimeWindow>>()>;
    using TriggerFactory = std::function<std::unique_ptr<clink::Trigger<T, clink::TimeWindow>>()>;

    EvictingTumblingWindowedDataStream(StreamExecutionEnvironment* env,
                                       std::string upstream_id,
                                       std::string channel_type,
                                       std::string key_by,
                                       std::chrono::milliseconds size,
                                       EvictorFactory evictor_factory)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)),
          size_(size),
          evictor_factory_(std::move(evictor_factory)) {}

    EvictingTumblingWindowedDataStream& parallelism(std::uint32_t p) {
        parallelism_ = p;
        return *this;
    }

    EvictingTumblingWindowedDataStream& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    EvictingTumblingWindowedDataStream& late_output_tag(OutputTag<T> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    EvictingTumblingWindowedDataStream& with_trigger(TriggerFactory factory) {
        trigger_factory_ = std::move(factory);
        return *this;
    }

    // Terminate the build with a process function that consumes the
    // evictor-filtered record list and emits a single Out per window.
    template <typename Out>
    DataStream<Out> process(
        std::function<Out(const std::vector<Record<T>>&, const clink::TimeWindow&)> process_fn,
        std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("evicting_tumbling_process");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto size = size_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        const auto trigger_factory = trigger_factory_;
        const auto evictor_factory = evictor_factory_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Out>(
            op_type,
            [process_fn = std::move(process_fn),
             op_type,
             channel,
             key_name,
             size,
             lateness,
             late_tag,
             trigger_factory,
             evictor_factory,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Out>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline evicting_tumbling: key extractor '" +
                                             key_name + "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                if (!evictor_factory) {
                    throw std::runtime_error(
                        "inline evicting_tumbling: evictor factory not set "
                        "(call .evicting(factory) before .process(fn))");
                }
                auto op = std::make_shared<detail::KeyedEvictingTumblingWindowFullOperator<T, Out>>(
                    std::move(extractor), size, process_fn, evictor_factory(), op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                if (trigger_factory) {
                    op->set_trigger(trigger_factory());
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Out>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Out>(env_, std::move(new_id), ChannelName<Out>::get());
    }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
    std::chrono::milliseconds size_;
    EvictorFactory evictor_factory_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<T>> late_tag_;
    TriggerFactory trigger_factory_;
    std::uint32_t parallelism_{1};
};

// Out-of-line definition of TumblingWindowedDataStream::evicting.
// Forward-declared above the EvictingTumbling handle to break the
// dependency cycle.
template <typename T>
EvictingTumblingWindowedDataStream<T> TumblingWindowedDataStream<T>::evicting(
    typename TumblingWindowedDataStream<T>::EvictorFactory factory) const {
    return EvictingTumblingWindowedDataStream<T>(
        env_, upstream_id_, channel_type_, key_by_, size_, std::move(factory));
}

// Session window handle - sessions per key form on records within
// `gap` of each other.
template <typename T>
class SessionWindowedDataStream {
public:
    SessionWindowedDataStream(StreamExecutionEnvironment* env,
                              std::string upstream_id,
                              std::string channel_type,
                              std::string key_by,
                              std::chrono::milliseconds gap)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)),
          gap_(gap) {}

    SessionWindowedDataStream& parallelism(std::uint32_t p) {
        parallelism_ = p;
        return *this;
    }

    SessionWindowedDataStream& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    SessionWindowedDataStream& late_output_tag(OutputTag<T> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // Session aggregation needs a merger to combine two accumulators
    // when sessions merge (no Value available at that moment).
    template <typename Agg>
    DataStream<Agg> aggregate(std::function<Agg()> initial,
                              std::function<Agg(const Agg&, const T&)> combiner,
                              std::function<Agg(const Agg&, const Agg&)> merger,
                              std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("session_aggregate");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto gap = gap_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Agg>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             merger = std::move(merger),
             op_type,
             channel,
             key_name,
             gap,
             lateness,
             late_tag,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Agg>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline session_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                auto op = std::make_shared<detail::KeyedSessionWindowAggregateOperator<T, Agg>>(
                    std::move(extractor), initial, combiner, merger, gap, op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Agg>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Agg>(env_, std::move(new_id), ChannelName<Agg>::get());
    }

    // Emit-form: like aggregate<Agg> but invokes emit_fn(key, session_window, agg)
    // at every fire (refire, watermark fire, EOS). The TimeWindow carries
    // {start, end} for the dynamic session bounds. Out must be a
    // registered channel type.
    template <typename Agg, typename Out>
    DataStream<Out> aggregate(
        std::function<Agg()> initial,
        std::function<Agg(const Agg&, const T&)> combiner,
        std::function<Agg(const Agg&, const Agg&)> merger,
        std::function<Out(std::int64_t, const clink::TimeWindow&, const Agg&)> emit_fn,
        std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("session_aggregate_emit");
        const std::string channel = channel_type_;
        const std::string key_name = key_by_;
        const auto gap = gap_;
        const auto lateness = allowed_lateness_;
        const auto late_tag = late_tag_;
        auto& reg = env_->registry();
        auto* key_reg = &reg.key_extractor_registry();
        reg.template register_operator<T, Out>(
            op_type,
            [initial = std::move(initial),
             combiner = std::move(combiner),
             merger = std::move(merger),
             emit_fn = std::move(emit_fn),
             op_type,
             channel,
             key_name,
             gap,
             lateness,
             late_tag,
             key_reg](const clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, Out>> {
                auto extractor = key_reg->template find<T>(channel, key_name);
                if (!extractor) {
                    throw std::runtime_error("inline session_window: key extractor '" + key_name +
                                             "' for channel '" + channel +
                                             "' is not registered (call .key_by(...) first)");
                }
                auto op = std::make_shared<detail::KeyedSessionWindowEmitOperator<T, Agg, Out>>(
                    std::move(extractor), initial, combiner, merger, emit_fn, gap, op_type);
                op->set_allowed_lateness(lateness);
                if (late_tag.has_value()) {
                    op->set_late_output_tag(*late_tag);
                }
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = ChannelName<Out>::get();
        op.key_by = key_by_;
        op.parallelism = (parallelism_ > 1) ? parallelism_ : env_->default_parallelism();
        if (late_tag_.has_value()) {
            cluster::SideOutputDecl decl;
            decl.tag = late_tag_->id;
            decl.channel_type = ChannelName<T>::get();
            op.side_outputs.push_back(std::move(decl));
        }
        auto new_id = env_->append_op(std::move(op));
        return DataStream<Out>(env_, std::move(new_id), ChannelName<Out>::get());
    }

private:
    StreamExecutionEnvironment* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
    std::chrono::milliseconds gap_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<T>> late_tag_;
    std::uint32_t parallelism_{1};
};

template <typename T>
KeyedDataStream<T> DataStream<T>::key_by(std::string extractor_name) const {
    return KeyedDataStream<T>(env_, upstream_id_, channel_type_, std::move(extractor_name));
}

template <typename T>
KeyedDataStream<T> DataStream<T>::key_by(std::function<std::int64_t(const T&)> fn) const {
    cluster::ensure_built_ins_registered();
    const std::string name = env_->mint_inline_op_type("key");
    auto& reg = env_->registry();
    reg.template register_key_extractor<T>(name, std::move(fn));
    return KeyedDataStream<T>(env_, upstream_id_, channel_type_, name);
}

template <typename T>
DataStream<T> StreamExecutionEnvironment::from_elements(std::vector<T> elements, std::string id) {
    cluster::ensure_built_ins_registered();
    const std::string op_type = mint_inline_op_type("from_elements");
    auto& reg = registry();
    reg.template register_source<T>(
        op_type,
        [elements = std::move(elements),
         op_type](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Source<T>> {
            const auto par = static_cast<std::size_t>(ctx.parallelism == 0 ? 1 : ctx.parallelism);
            const auto idx = static_cast<std::size_t>(ctx.subtask_idx);
            std::vector<Record<T>> records;
            for (std::size_t i = idx; i < elements.size(); i += par) {
                records.emplace_back(Record<T>{elements[i]});
            }
            return std::make_shared<VectorSource<T>>(std::move(records), op_type);
        });
    cluster::OperatorSpec op;
    op.id = std::move(id);
    op.type = op_type;
    op.out_channel = ChannelName<T>::get();
    std::string out_channel = op.out_channel;
    auto new_id = append_op(std::move(op));
    return DataStream<T>(this, std::move(new_id), std::move(out_channel));
}

template <typename T>
DataStream<T> StreamExecutionEnvironment::source(SourceDescriptor desc, std::string id) {
    cluster::OperatorSpec op;
    op.id = std::move(id);
    op.type = std::move(desc.op_type);
    op.out_channel = desc.channel_type.empty() ? ChannelName<T>::get() : desc.channel_type;
    op.params = std::move(desc.params);
    // Descriptor parallelism wins if it's been set above the default
    // (1); otherwise inherit env's default_parallelism.
    op.parallelism = (desc.parallelism > 1) ? desc.parallelism : default_parallelism_;
    // Capture out_channel BEFORE the move - append_op(std::move(op))
    // moves op into the graph, after which op.out_channel is in
    // moved-from state (empty). The returned DataStream<T> needs the
    // channel string for later .key_by() / .reduce() lookups that go
    // through KeyExtractorRegistry by (channel, name).
    std::string out_channel = op.out_channel;
    auto new_id = append_op(std::move(op));
    return DataStream<T>(this, std::move(new_id), std::move(out_channel));
}

}  // namespace clink::api
