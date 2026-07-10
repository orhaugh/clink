#pragma once

// clink::test sources and sinks - deterministic, scripted stream
// endpoints for testing pipelines and sink/source protocols:
//
//   TestSource<T>            scripted bounded source; checkpointable offset
//   CollectSink<T>           thread-safe collecting sink
//   FailingSink<T>           collects, then throws after N records
//   TransactionalTestSink<T> records the full 2PC lifecycle (stage at
//                            barrier, commit/abort per checkpoint)
//
// All of them implement the engine's real Source<T>/Sink<T> contracts,
// so they compose with harnesses, the local runtime and the cluster
// alike. Sinks are internally locked: values are observable through a
// shared_ptr'd sink while a pipeline drives it from its task threads.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/test/failure_injection.hpp"

namespace clink::test {

// ---- TestSource -----------------------------------------------------

// A scripted, bounded source. Script it fluently, then hand it to a
// pipeline (or drive produce() directly with an OutputCapture):
//
//   auto src = clink::test::TestSource<std::int64_t>({1, 2, 3});   // plain values
//   clink::test::TestSource<Event> src2;
//   src2.emit(e1, 1000).emit(e2, 2000).watermark(2500).emit(e3, 3000);
//
// produce() emits exactly ONE script entry per call - the finest
// granularity, so checkpoint barriers can land between any two
// entries. The offset is checkpointable through the production
// snapshot_offset/restore_offset hooks: a restored TestSource with the
// same script resumes after the last checkpointed entry, which is what
// makes it usable in exactly-once recovery tests.
template <typename T>
class TestSource final : public Source<T> {
public:
    TestSource() = default;
    explicit TestSource(std::vector<T> values) {
        for (auto& v : values) {
            emit(std::move(v));
        }
    }

    // ---- Scripting (chainable; script before the pipeline starts) ----

    TestSource& emit(T value) {
        entries_.push_back(Record{std::move(value), std::nullopt});
        return *this;
    }
    TestSource& emit(T value, std::int64_t event_time_ms) {
        entries_.push_back(Record{std::move(value), event_time_ms});
        return *this;
    }
    TestSource& watermark(std::int64_t ts_ms) {
        entries_.push_back(WatermarkEntry{ts_ms});
        return *this;
    }

    // ---- Source contract ----

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || cursor_ >= entries_.size()) {
            return false;
        }
        const auto& entry = entries_[cursor_++];
        if (const auto* rec = std::get_if<Record>(&entry)) {
            Batch<T> b;
            if (rec->event_time_ms) {
                b.emplace(rec->value, EventTime{*rec->event_time_ms});
            } else {
                b.emplace(rec->value);
            }
            out.emit_data(std::move(b));
        } else {
            out.emit_watermark(Watermark{EventTime{std::get<WatermarkEntry>(entry).ts_ms}});
        }
        return true;
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    // Exactly-once offset checkpointing (the production source protocol):
    // the cursor is persisted at a barrier and restored before the first
    // produce() after recovery.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        char bytes[8];
        const auto c = cursor_;
        for (int i = 0; i < 8; ++i) {
            bytes[i] = static_cast<char>((c >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(op_id,
                                   StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
                                   StateBackend::ValueView{bytes, sizeof bytes});
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
        cursor_ = restored;
        return true;
    }

    std::string name() const override { return "test-source"; }

    // ---- Inspection ----

    std::uint64_t cursor() const noexcept { return cursor_; }
    std::size_t script_size() const noexcept { return entries_.size(); }

private:
    struct Record {
        T value;
        std::optional<std::int64_t> event_time_ms;
    };
    struct WatermarkEntry {
        std::int64_t ts_ms;
    };
    using Entry = std::variant<Record, WatermarkEntry>;

    static constexpr const char* kOffsetKey_ = "test_source.cursor";

    std::vector<Entry> entries_;
    std::uint64_t cursor_{0};
};

// ---- CollectSink ----------------------------------------------------

// Collects everything it is given, thread-safely. Keep a shared_ptr to
// the sink and read values()/records() after (or while) the pipeline
// runs.
template <typename T>
class CollectSink : public Sink<T> {
public:
    struct TimestampedValue {
        T value;
        std::optional<std::int64_t> event_time_ms;

        friend bool operator==(const TimestampedValue& a, const TimestampedValue& b) {
            return a.value == b.value && a.event_time_ms == b.event_time_ms;
        }
    };

    void on_data(const Batch<T>& batch) override {
        const std::lock_guard<std::mutex> lock(mu_);
        for (const auto& rec : batch) {
            const auto ts = rec.event_time();
            records_.push_back(TimestampedValue{
                rec.value(), ts ? std::optional<std::int64_t>{ts->millis()} : std::nullopt});
        }
    }
    void on_watermark(Watermark wm) override {
        const std::lock_guard<std::mutex> lock(mu_);
        if (!wm.is_idle()) {
            watermarks_.push_back(wm.timestamp().millis());
        }
    }

    std::string name() const override { return "collect-sink"; }

    // ---- Inspection (locked copies) ----

    std::vector<T> values() const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<T> out;
        out.reserve(records_.size());
        for (const auto& r : records_) {
            out.push_back(r.value);
        }
        return out;
    }
    std::vector<TimestampedValue> records() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return records_;
    }
    std::vector<std::int64_t> watermarks() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return watermarks_;
    }
    std::size_t value_count() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return records_.size();
    }

private:
    mutable std::mutex mu_;
    std::vector<TimestampedValue> records_;
    std::vector<std::int64_t> watermarks_;
};

// ---- FailingSink ----------------------------------------------------

// Accepts `pass_first` records, then throws InjectedFailure exactly
// once on the next record - the sink-side crash for recovery tests.
// Records accepted before (and after) the failure are observable via
// values().
template <typename T>
class FailingSink final : public Sink<T> {
public:
    explicit FailingSink(std::uint64_t pass_first) : pass_first_(pass_first) {}

    void on_data(const Batch<T>& batch) override {
        const std::lock_guard<std::mutex> lock(mu_);
        for (const auto& rec : batch) {
            if (!failed_ && values_.size() >= pass_first_) {
                failed_ = true;
                throw InjectedFailure("FailingSink: injected failure after " +
                                      std::to_string(values_.size()) + " records");
            }
            values_.push_back(rec.value());
        }
    }

    std::string name() const override { return "failing-sink"; }

    std::vector<T> values() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return values_;
    }
    bool failed() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return failed_;
    }

private:
    mutable std::mutex mu_;
    std::uint64_t pass_first_;
    std::vector<T> values_;
    bool failed_{false};
};

// ---- TransactionalTestSink -------------------------------------------

// A sink that records the engine's full two-phase-commit lifecycle so
// tests can assert transactional semantics end to end:
//
//   on_data     appends to the CURRENT (uncommitted) epoch
//   on_barrier  stages the current epoch under the barrier's checkpoint
//               id (pre-commit); a TERMINAL barrier also commits it
//               immediately (the engine's bounded-stream contract)
//   on_commit   promotes that checkpoint's staged records to committed
//               (idempotent, as the engine requires)
//   on_abort    discards that checkpoint's staged records
//
// committed_values() then contains exactly the records of committed
// epochs, in order - what an external system would durably hold.
template <typename T>
class TransactionalTestSink final : public Sink<T> {
public:
    void on_data(const Batch<T>& batch) override {
        const std::lock_guard<std::mutex> lock(mu_);
        for (const auto& rec : batch) {
            current_.push_back(rec.value());
        }
    }

    void on_barrier(CheckpointBarrier b) override {
        const std::lock_guard<std::mutex> lock(mu_);
        auto& staged = pending_[b.id().value()];
        staged.insert(staged.end(),
                      std::make_move_iterator(current_.begin()),
                      std::make_move_iterator(current_.end()));
        current_.clear();
        if (b.is_terminal()) {
            commit_locked_(b.id().value());
        }
    }

    void on_commit(std::uint64_t checkpoint_id) override {
        const std::lock_guard<std::mutex> lock(mu_);
        commit_locked_(checkpoint_id);
    }

    void on_abort(std::uint64_t checkpoint_id) override {
        const std::lock_guard<std::mutex> lock(mu_);
        pending_.erase(checkpoint_id);
        aborts_.push_back(checkpoint_id);
    }

    std::string name() const override { return "transactional-test-sink"; }

    // ---- Inspection (locked copies) ----

    // Records of committed epochs only, in commit order.
    std::vector<T> committed_values() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return committed_;
    }
    // Records staged at a barrier but not yet committed/aborted.
    std::vector<std::uint64_t> pending_checkpoints() const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::uint64_t> out;
        out.reserve(pending_.size());
        for (const auto& [id, _] : pending_) {
            out.push_back(id);
        }
        return out;
    }
    // Records received since the last barrier (no epoch yet).
    std::vector<T> uncommitted_values() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return current_;
    }
    std::vector<std::uint64_t> commits() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return commits_;
    }
    std::vector<std::uint64_t> aborts() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return aborts_;
    }

private:
    void commit_locked_(std::uint64_t checkpoint_id) {
        const auto it = pending_.find(checkpoint_id);
        if (it == pending_.end()) {
            return;  // idempotent: already committed, aborted, or empty
        }
        committed_.insert(committed_.end(),
                          std::make_move_iterator(it->second.begin()),
                          std::make_move_iterator(it->second.end()));
        pending_.erase(it);
        commits_.push_back(checkpoint_id);
    }

    mutable std::mutex mu_;
    std::vector<T> current_;
    std::map<std::uint64_t, std::vector<T>> pending_;
    std::vector<T> committed_;
    std::vector<std::uint64_t> commits_;
    std::vector<std::uint64_t> aborts_;
};

}  // namespace clink::test
