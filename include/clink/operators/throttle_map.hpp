#pragma once

#include <chrono>
#include <string>
#include <thread>
#include <utility>

#include "clink/operators/operator_base.hpp"

namespace clink {

// ThrottleMap forwards records 1:1, sleeping `sleep_per_record` between
// each on the current thread. When `sleep_per_record` is zero the
// operator degrades to a pure passthrough - its purpose is to keep the
// topology hash stable across config toggles (a rate-limit setting can
// move between zero and non-zero without changing the graph shape).
// Watermarks and barriers are forwarded unchanged.
template <typename T>
class ThrottleMap final : public Operator<T, T> {
public:
    explicit ThrottleMap(std::chrono::milliseconds sleep_per_record,
                         std::string name = "throttle_map")
        : sleep_per_record_(sleep_per_record), name_(std::move(name)) {}

    void process(const StreamElement<T>& element, Emitter<T>& out) override {
        if (element.is_data()) {
            const Batch<T>& in_batch = element.as_data();
            Batch<T> out_batch;
            for (const auto& record : in_batch) {
                if (sleep_per_record_.count() > 0) {
                    std::this_thread::sleep_for(sleep_per_record_);
                }
                out_batch.push(record);
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
    std::chrono::milliseconds sleep_per_record_;
    std::string name_;
};

}  // namespace clink
