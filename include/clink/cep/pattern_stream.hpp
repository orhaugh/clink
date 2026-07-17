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
        reg.template register_operator<T, U>(
            op_type,
            [pattern, t_codec, key_fn, fn = std::move(fn), op_type](
                const ::clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                return std::make_shared<CepOperator<T, U>>(pattern, t_codec, key_fn, fn, op_type);
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
        reg.template register_operator<T, U>(
            op_type,
            [pattern, t_codec, key_fn, fn = std::move(fn), op_type](
                const ::clink::plugin::BuildContext&) -> std::shared_ptr<Operator<T, U>> {
                typename CepOperator<T, U>::FlatSelectFn flat_fn = fn;
                return std::make_shared<CepOperator<T, U>>(
                    pattern, t_codec, key_fn, flat_fn, op_type);
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
};

}  // namespace clink::cep
