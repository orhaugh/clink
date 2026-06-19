#pragma once

#include <cstddef>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "clink/operators/operator_base.hpp"
#include "clink/runtime/output_tag.hpp"

namespace clink {

// SplitByVariantOperator routes a `std::variant<Tags...>` input to one
// of N typed side outputs, picking the side based on which alternative
// the variant currently holds. The main output passes the variant
// through unchanged so consumers can attach diagnostic taps if they
// like; the typed side outputs carry the unwrapped alternative value.
//
// Constructor takes a `std::tuple<OutputTag<Tags>...>` whose tag types
// must match the variant alternatives in order. Compile-time `static_assert`s
// guard the order and length so a mismatch is caught at the call site
// rather than at runtime.
//
// Wiring (typical):
//   OutputTag<A> tag_a("a"); OutputTag<B> tag_b("b");
//   using V = std::variant<A, B>;
//   auto op = std::make_shared<SplitByVariantOperator<V, A, B>>(
//       std::make_tuple(tag_a, tag_b));
//   auto h_op = dag.add_operator<V, V>(h_upstream, op);
//   auto h_a = dag.side_output<A>(h_op, tag_a);
//   auto h_b = dag.side_output<B>(h_op, tag_b);
//
// Watermarks and barriers are forwarded unchanged on the main channel.
// Side channels don't carry their own watermarks for v1 (matches the
// existing side-output convention in clink).
template <typename Variant, typename... Tags>
class SplitByVariantOperator final : public Operator<Variant, Variant> {
public:
    static_assert(std::variant_size_v<Variant> == sizeof...(Tags),
                  "SplitByVariantOperator: number of OutputTag types must match number of "
                  "alternatives in the variant");

    using TagTuple = std::tuple<OutputTag<Tags>...>;

    explicit SplitByVariantOperator(TagTuple tags, std::string name = "split_by_variant")
        : tags_(std::move(tags)), name_(std::move(name)) {}

    void process(const StreamElement<Variant>& element, Emitter<Variant>& out) override {
        if (element.is_data()) {
            const Batch<Variant>& in_batch = element.as_data();
            for (const auto& record : in_batch) {
                // Side-emit one record at a time so we can pick the
                // correct typed channel per alternative. Caller paths
                // that need higher per-side throughput can wrap this
                // with a batching downstream operator.
                std::visit(
                    [&](const auto& payload) {
                        using P = std::decay_t<decltype(payload)>;
                        emit_to_side<P>(payload);
                    },
                    record.value());
            }
            // Passthrough on main output so consumers can tap.
            out.emit_data(in_batch);
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    template <typename P>
    void emit_to_side(const P& payload) {
        // Locate the OutputTag<P> in the tuple by compile-time index
        // search. The variant alternative -> tuple index mapping is
        // 1:1 since we static_assert sizes match.
        emit_to_side_impl<P>(payload, std::make_index_sequence<sizeof...(Tags)>{});
    }

    template <typename P, std::size_t... Is>
    void emit_to_side_impl(const P& payload, std::index_sequence<Is...>) {
        bool emitted = false;
        ((!emitted && try_emit_at<Is, P>(payload, emitted)), ...);
    }

    template <std::size_t I, typename P>
    bool try_emit_at(const P& payload, bool& emitted) {
        using TagT = typename std::tuple_element_t<I, TagTuple>::element_type;
        if constexpr (std::is_same_v<TagT, P>) {
            const auto& tag = std::get<I>(tags_);
            Batch<P> b;
            b.emplace(payload);
            auto side = this->runtime()->template side_output<P>(tag);
            side.emit_data(std::move(b));
            emitted = true;
            return true;
        }
        return false;
    }

    TagTuple tags_;
    std::string name_;
};

}  // namespace clink
