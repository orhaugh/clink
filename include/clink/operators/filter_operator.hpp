#pragma once

#include <functional>
#include <string>
#include <utility>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Stateless predicate operator. Passes through records where pred(record) is
// true, drops the rest. Watermarks and barriers are always forwarded.
template <typename T>
class FilterOperator final : public Operator<T, T> {
public:
    using Predicate = std::function<bool(const T&)>;

    explicit FilterOperator(Predicate pred, std::string name = "filter")
        : pred_(std::move(pred)), name_(std::move(name)) {}

    void process(const StreamElement<T>& element, Emitter<T>& out) override {
        if (element.is_data()) {
            const Batch<T>& in_batch = element.as_data();
            Batch<T> out_batch;
            for (const auto& record : in_batch) {
                if (pred_(record.value())) {
                    out_batch.push(record);
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
    Predicate pred_;
    std::string name_;
};

}  // namespace clink
