#pragma once

// BoundedSlowStringSource - the shared source for the exactly-once job
// plugins (file 2PC, Postgres 2PC, ...). Emits exactly N payloads derived
// from "record-0" through "record-(N-1)", one every `tick`, then returns
// false (natural completion). Bounded so "did it finish" is a real question,
// slow enough that several checkpoints land mid-run.
//
// The payload formatter lets each job shape the record for its sink (plain
// string for the file sink, a JSON object for the Postgres JSON sink) while
// the offset-checkpoint logic - which the exactly-once claim depends on -
// stays in one place: the next-record index is snapshotted as operator
// state, so a restart resumes where the source left off instead of
// replaying from 0. Combined with a 2PC sink's commit-on-checkpoint, that
// gives exactly-once across a recovery.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <utility>

#include "clink/operators/source_operator.hpp"
#include "clink/state/state_backend.hpp"

namespace clink_examples {

class BoundedSlowStringSource final : public clink::Source<std::string> {
public:
    using Format = std::function<std::string(std::int64_t)>;

    BoundedSlowStringSource(std::int64_t total, std::chrono::milliseconds tick, Format format = {})
        : total_(total), tick_(tick), format_(std::move(format)) {
        if (!format_) {
            format_ = [](std::int64_t i) { return "record-" + std::to_string(i); };
        }
    }

    bool produce(clink::Emitter<std::string>& out) override {
        if (this->cancelled() || counter_ >= total_) {
            return false;
        }
        clink::Batch<std::string> b;
        b.emplace(format_(counter_));
        ++counter_;
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        std::this_thread::sleep_for(tick_);
        return counter_ < total_;
    }

    void snapshot_offset(clink::StateBackend& backend,
                         clink::OperatorId op_id,
                         clink::CheckpointId /*ckpt*/) override {
        std::array<std::byte, 8> bytes{};
        const auto u = static_cast<std::uint64_t>(counter_);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(op_id,
                                   clink::StateBackend::KeyView{kKey_, std::strlen(kKey_)},
                                   clink::StateBackend::ValueView{
                                       reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(clink::StateBackend& backend, clink::OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, clink::StateBackend::KeyView{kKey_, std::strlen(kKey_)});
        if (!v.has_value() || v->size() < 8) {
            return false;
        }
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(
                     static_cast<std::uint8_t>((*v)[static_cast<std::size_t>(i)]))
                 << (i * 8);
        }
        counter_ = static_cast<std::int64_t>(u);
        return true;
    }

    std::string name() const override { return "bounded_slow_string_source"; }

private:
    static constexpr const char* kKey_ = "__twopc_source_offset__";
    std::int64_t total_;
    std::chrono::milliseconds tick_;
    Format format_;
    std::int64_t counter_{0};
};

// Shared environment knobs, so every exactly-once job reads the same
// contract the tests set: CLINK_2PC_TOTAL records at CLINK_2PC_TICK_MS.
inline std::int64_t total_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_TOTAL"); p != nullptr && *p != '\0') {
        try {
            return std::stoll(p);
        } catch (...) {
        }
    }
    return 30;
}

inline std::chrono::milliseconds tick_from_env() {
    if (const char* p = std::getenv("CLINK_2PC_TICK_MS"); p != nullptr && *p != '\0') {
        try {
            return std::chrono::milliseconds{std::stoll(p)};
        } catch (...) {
        }
    }
    return std::chrono::milliseconds{50};
}

}  // namespace clink_examples
