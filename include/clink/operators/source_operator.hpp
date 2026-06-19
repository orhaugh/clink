#pragma once

#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Bounded vector source: emits the supplied records as a single Batch then
// signals end-of-stream. Useful for tests and bounded examples.
//
// Checkpoint behaviour: VectorSource snapshots the index of the next
// record to emit when a barrier flows through. On restore it advances
// past records the previous run had already emitted, so a restart
// after a checkpoint resumes from where the source left off rather
// than replaying from offset 0. Combined with the operator-side keyed
// state restore that's already in place, that gives pipeline-wide
// exactly-once for bounded sources.
template <typename T>
class VectorSource final : public Source<T> {
public:
    explicit VectorSource(std::vector<Record<T>> records, std::string name = "vector_source")
        : records_(std::move(records)), name_(std::move(name)) {}

    // A fixed vector of records is a finite stream (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    bool produce(Emitter<T>& out) override {
        if (emitted_ || this->cancelled()) {
            return false;
        }
        // Emit records from `next_index_` onward - on restore that
        // index advances to skip what the previous run already shipped.
        if (next_index_ < records_.size()) {
            Batch<T> batch;
            for (std::size_t i = next_index_; i < records_.size(); ++i) {
                batch.push(records_[i]);
            }
            next_index_ = records_.size();
            out.emit_data(std::move(batch));
        }
        // Emit a max watermark to signal end of event time on this source.
        out.emit_watermark(Watermark::max());
        emitted_ = true;
        return false;  // exhausted after this call
    }

    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        // Persist next_index_ as a single u64 LE under a fixed slot, via the
        // operator-state path: source state is operator-state, not keyed
        // state, so it must NOT be narrowed by the rescale restore filter
        // (every subtask restores it whole). put_operator_state stores it
        // under the reserved prefix that makes the filter exempt it.
        std::array<std::byte, 8> bytes{};
        const auto v = static_cast<std::uint64_t>(next_index_);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((v >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(
            op_id,
            StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value() || v->size() < 8) {
            return false;
        }
        std::uint64_t restored = 0;
        for (int i = 0; i < 8; ++i) {
            restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
        }
        next_index_ = static_cast<std::size_t>(restored);
        return true;
    }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kOffsetKey_ = "__vector_source_offset__";
    std::vector<Record<T>> records_;
    std::string name_;
    std::size_t next_index_{0};
    bool emitted_{false};
};

// Generator source: callable produces optional<Record<T>>. Returning nullopt
// signals end of stream.
template <typename T>
class GeneratorSource final : public Source<T> {
public:
    using Gen = std::function<std::optional<Record<T>>()>;

    // `bounded` declares whether the generator is guaranteed to terminate
    // (eventually return nullopt). A generator can model either a finite
    // dataset or an endless stream, so boundedness can't be inferred and is
    // declared at construction. Default false (unbounded) is the safe choice
    // for the common endless-generator case; pass true for a finite generator
    // to opt into the end-of-input drain + batch execution path (BATCH-1).
    explicit GeneratorSource(Gen gen, std::string name = "generator_source", bool bounded = false)
        : gen_(std::move(gen)), name_(std::move(name)), bounded_(bounded) {}

    [[nodiscard]] bool is_bounded() const noexcept override { return bounded_; }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        auto maybe = gen_();
        if (!maybe.has_value()) {
            out.emit_watermark(Watermark::max());
            return false;
        }
        Batch<T> batch;
        batch.push(std::move(*maybe));
        out.emit_data(std::move(batch));
        return true;
    }

    std::string name() const override { return name_; }

private:
    Gen gen_;
    std::string name_;
    bool bounded_{false};
};

}  // namespace clink
