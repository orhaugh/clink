#pragma once

// PatternStream<T> is the handle returned by clink::cep::pattern(...).
// It captures everything needed to materialise the CEP operator on
// the subsequent .select<U>() call:
//
//   * env_         - the Pipeline to append to
//   * upstream_id_ - the producing op's id
//   * channel_type - the upstream channel type name
//   * key_by_      - empty for non-keyed CEP; otherwise the key
//                    extractor name registered on this op's edge
//                    (so the planner uses Hash routing into us)
//   * pattern_     - the compiled Pattern<T>
//   * t_codec_     - codec for T (used to persist partial matches)
//   * key_fn_      - int64 extractor over T. For keyed: the extractor
//                    looked up from KeyExtractorRegistry by key_by_.
//                    For non-keyed: a constant-0 extractor.
//   * order_by_event_time_ - set by order_by_event_time(); the operator
//                    then matches each key's records in event-time order.
//
// The .select<U>() call mints an inline op type, registers a runner
// factory that builds the CepOperator at runtime, and appends an
// OperatorSpec carrying key_by_ so the planner routes correctly.

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "clink/api/pipeline.hpp"
#include "clink/cep/cep_operator.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/core/codec.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/output_tag.hpp"

namespace clink::cep {

template <typename T>
class PatternStream {
public:
    PatternStream(api::Pipeline* env,
                  std::string upstream_id,
                  std::string channel_type,
                  std::string key_by,
                  Pattern<T> pattern,
                  Codec<T> t_codec,
                  std::function<std::int64_t(const T&)> key_fn)
        : env_(env),
          upstream_id_(std::move(upstream_id)),
          channel_type_(std::move(channel_type)),
          key_by_(std::move(key_by)),
          pattern_(std::move(pattern)),
          t_codec_(std::move(t_codec)),
          key_fn_(std::move(key_fn)) {}

    // Match each key's records in event-time order rather than the order they
    // arrive: each record is held until the watermark passes it, then fed to
    // the pattern with the key's other held records by event time, records at
    // the same time in the order they arrived. A record at or behind the
    // watermark is late and dropped; one without an event time is matched
    // where it arrives. Held records are checkpointed with the partial
    // matches, and the end of the input releases them. The stream needs event
    // times and watermarks, from assign_timestamps_bounded() upstream for
    // example. Applies to the select(), select_with_timed_out() or
    // flat_select() that follows.
    PatternStream& order_by_event_time() {
        order_by_event_time_ = true;
        return *this;
    }

    // Materialise the CEP operator and emit one U per completed
    // pattern match. The selector receives the full PatternMatch<T>
    // (step-name -> matched events) and returns the user's payload.
    //
    // Same in-process-only contract as the other inline-lambda fluent
    // shortcuts: the operator factory captures `fn` by value into the
    // process-wide RunnerRegistry, so submitting this job to a remote
    // worker that has no equivalent registration will fail there. For
    // cross-process CEP jobs, package the operator as a real plugin.
    template <typename U>
    api::DataStream<U> select(std::function<U(const PatternMatch<T>&)> fn, std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("cep");
        auto& reg = env_->registry();
        auto pattern = pattern_;
        auto t_codec = t_codec_;
        auto key_fn = key_fn_;
        const bool event_time_order = order_by_event_time_;
        reg.template register_operator<T, U>(
            op_type,
            [pattern, t_codec, key_fn, fn = std::move(fn), op_type, event_time_order](
                const ::clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                auto op =
                    std::make_shared<CepOperator<T, U>>(pattern, t_codec, key_fn, fn, op_type);
                op->order_by_event_time(event_time_order);
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = api::ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return api::DataStream<U>(env_, std::move(new_id), api::ChannelName<U>::get());
    }

    // Like select<U>(), but additionally routes TIMED-OUT partials -
    // those whose within() window closed before the pattern completed -
    // to `timed_out_tag` as a side output, each mapped through
    // `timed_out_fn`. Obtain the side stream with the standard fluent
    // idiom on the returned stream:
    //
    //   auto matches = cep::pattern(keyed, p, codec)
    //                      .select_with_timed_out<Alert>(fn, tag, timed_out_fn);
    //   auto expired = matches.side_output<Alert>(tag);
    //
    // The returned (main) stream carries the completed matches; the
    // side stream carries the expirations, whether discovered by
    // watermark pruning or by a record arriving past the partial's
    // within() deadline. Requires .within() on the pattern - without a
    // bound, partials never time out and the tag never fires. Same
    // in-process-only lambda contract as select(): package as a plugin
    // for cross-process jobs.
    template <typename U>
    api::DataStream<U> select_with_timed_out(std::function<U(const PatternMatch<T>&)> fn,
                                             OutputTag<U> timed_out_tag,
                                             std::function<U(const PatternMatch<T>&)> timed_out_fn,
                                             std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("cep_timed_out");
        auto& reg = env_->registry();
        auto pattern = pattern_;
        auto t_codec = t_codec_;
        auto key_fn = key_fn_;
        const bool event_time_order = order_by_event_time_;
        reg.template register_operator<T, U>(
            op_type,
            [pattern,
             t_codec,
             key_fn,
             fn = std::move(fn),
             timed_out_tag,
             timed_out_fn = std::move(timed_out_fn),
             op_type,
             event_time_order](
                const ::clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                auto op =
                    std::make_shared<CepOperator<T, U>>(pattern, t_codec, key_fn, fn, op_type);
                op->template with_timed_out_output<U>(timed_out_tag, timed_out_fn);
                op->order_by_event_time(event_time_order);
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = api::ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return api::DataStream<U>(env_, std::move(new_id), api::ChannelName<U>::get());
    }

    // Flat-select: like select<U>(), but the user fn returns a
    // std::vector<U>. Each completed pattern match emits zero or
    // more U records depending on the vector size. Mirrors
    // PatternFlatSelectFunction. Mutually exclusive with select<U>()
    // - the underlying CepOperator is built with the flat variant
    // and routes through its FlatSelect constructor.
    template <typename U>
    api::DataStream<U> flat_select(std::function<std::vector<U>(const PatternMatch<T>&)> fn,
                                   std::string id = {}) {
        cluster::ensure_built_ins_registered();
        const std::string op_type = env_->mint_inline_op_type("cep_flat");
        auto& reg = env_->registry();
        auto pattern = pattern_;
        auto t_codec = t_codec_;
        auto key_fn = key_fn_;
        const bool event_time_order = order_by_event_time_;
        reg.template register_operator<T, U>(
            op_type,
            [pattern, t_codec, key_fn, fn = std::move(fn), op_type, event_time_order](
                const ::clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                typename CepOperator<T, U>::FlatSelectFn flat_fn = fn;
                auto op =
                    std::make_shared<CepOperator<T, U>>(pattern, t_codec, key_fn, flat_fn, op_type);
                op->order_by_event_time(event_time_order);
                return op;
            });
        cluster::OperatorSpec op;
        op.id = std::move(id);
        op.type = op_type;
        op.inputs = {upstream_id_};
        op.out_channel = api::ChannelName<U>::get();
        op.key_by = key_by_;
        auto new_id = env_->append_op(std::move(op));
        return api::DataStream<U>(env_, std::move(new_id), api::ChannelName<U>::get());
    }

private:
    api::Pipeline* env_;
    std::string upstream_id_;
    std::string channel_type_;
    std::string key_by_;
    Pattern<T> pattern_;
    Codec<T> t_codec_;
    std::function<std::int64_t(const T&)> key_fn_;
    bool order_by_event_time_{false};
};

}  // namespace clink::cep
