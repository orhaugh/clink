#pragma once

#include <functional>
#include <string>
#include <utility>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Stateless 1:1 mapping operator. The user supplies a function In -> Out.
//
// MapOperator forwards watermarks and barriers unchanged via the default
// Operator<> implementations.
template <typename In, typename Out>
class MapOperator final : public Operator<In, Out> {
public:
    using Func = std::function<Out(const In&)>;

    explicit MapOperator(Func fn, std::string name = "map")
        : fn_(std::move(fn)), name_(std::move(name)) {}

    void process(const StreamElement<In>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            const Batch<In>& in_batch = element.as_data();
            Batch<Out> out_batch;
            for (const auto& record : in_batch) {
                if (record.event_time().has_value()) {
                    out_batch.emplace(fn_(record.value()), *record.event_time());
                } else {
                    out_batch.emplace(fn_(record.value()));
                }
            }
            out.emit_data(std::move(out_batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    Func fn_;
    std::string name_;
};

}  // namespace clink
