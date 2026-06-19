#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Stateless 1:N mapping operator. The user supplies a function returning a
// vector<Out> per input - useful for tokenization, expansion, filter-with-
// transform, etc. Watermarks and barriers are forwarded unchanged.
template <typename In, typename Out>
class FlatMapOperator final : public Operator<In, Out> {
public:
    using Func = std::function<std::vector<Out>(const In&)>;

    explicit FlatMapOperator(Func fn, std::string name = "flat_map")
        : fn_(std::move(fn)), name_(std::move(name)) {}

    void process(const StreamElement<In>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            const Batch<In>& in_batch = element.as_data();
            Batch<Out> out_batch;
            for (const auto& record : in_batch) {
                auto produced = fn_(record.value());
                for (auto& v : produced) {
                    if (record.event_time().has_value()) {
                        out_batch.emplace(std::move(v), *record.event_time());
                    } else {
                        out_batch.emplace(std::move(v));
                    }
                }
            }
            if (!out_batch.empty()) {
                out.emit_data(std::move(out_batch));
            }
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
