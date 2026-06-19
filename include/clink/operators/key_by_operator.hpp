#pragma once

#include <functional>
#include <string>
#include <utility>

#include "clink/operators/operator_base.hpp"

namespace clink {

// KeyByOperator is a placeholder. In the eventual distributed engine, KeyBy is
// the boundary at which records are reshuffled across partitions according to a
// hash of the key. In the local in-process MVP we don't reshuffle - but we do
// preserve the API and stamp every record with its derived key, so downstream
// keyed operators (e.g. windows, keyed state) have a stable contract.
//
// The output type is std::pair<Key, Value> so the next operator can pick up
// keyed semantics without needing to know the original record shape.
template <typename Value, typename Key>
class KeyByOperator final : public Operator<Value, std::pair<Key, Value>> {
public:
    using KeyExtractor = std::function<Key(const Value&)>;

    explicit KeyByOperator(KeyExtractor extractor, std::string name = "key_by")
        : extractor_(std::move(extractor)), name_(std::move(name)) {}

    void process(const StreamElement<Value>& element,
                 Emitter<std::pair<Key, Value>>& out) override {
        if (element.is_data()) {
            const Batch<Value>& in_batch = element.as_data();
            Batch<std::pair<Key, Value>> out_batch;
            for (const auto& record : in_batch) {
                Key k = extractor_(record.value());
                auto keyed_v = std::make_pair(std::move(k), record.value());
                if (record.event_time().has_value()) {
                    out_batch.emplace(std::move(keyed_v), *record.event_time());
                } else {
                    out_batch.emplace(std::move(keyed_v));
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
    KeyExtractor extractor_;
    std::string name_;
};

}  // namespace clink
