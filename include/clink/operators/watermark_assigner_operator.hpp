#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "clink/operators/operator_base.hpp"
#include "clink/time/alignment_group.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace clink {

// Pass-through operator that assigns event-time to each record (via a user-
// supplied extractor) and emits a watermark on a cadence determined by the
// supplied strategy.
//
// In the MVP the cadence is "after every batch" - the operator processes a
// batch, asks the strategy for the current watermark, and emits one if the
// strategy says it advanced. This keeps the operator deterministic and
// trivially testable.
//
// Idleness (WatermarkStrategy.withIdleness):
//
// Call `.with_idleness(duration)` to mark this stream as "potentially
// quiet". If no records arrive for `duration`, the operator emits an
// idle watermark via Watermark::idle(...). Downstream multi-input
// alignment will skip this input from its min-watermark computation
// so a quiet partition doesn't stall global time progress at joins.
//
// The idleness check runs on a processing-time timer (registered at
// open) at half the idleness interval - this guarantees an idle
// detection fires even when zero records / watermarks / barriers
// flow through the operator (which would otherwise never wake up
// the process() path). When a record arrives, the operator
// transitions back to active and the next watermark goes out non-idle.
template <typename T>
class WatermarkAssignerOperator final : public Operator<T, T> {
public:
    using TimeExtractor = std::function<EventTime(const T&)>;
    // Optional columnar event-time reader: fills `out[i]` with row i's event
    // time read straight from the Arrow sidecar (no row materialisation),
    // returning false if it cannot (e.g. the column is absent), so the row
    // process() path takes over. When set, process_columnar can forward a
    // columnar batch through unchanged while still advancing the watermark.
    using ColumnarEventTimes =
        std::function<bool(const arrow::RecordBatch&, std::vector<EventTime>&)>;

    // Optional companion to ColumnarEventTimes: reads each row's source
    // partition straight from the Arrow sidecar so the columnar fast path can
    // feed a PARTITION-AWARE watermark strategy without materialising rows. Per
    // row it yields the partition or nullopt (no partition / null cell). When
    // the reader is unset, or yields a size mismatch, the columnar path feeds
    // partition-unset records - i.e. a single global watermark, which is the
    // correct behaviour for a non-partitioned source. This is what lets a
    // partitioned (Kafka) columnar source keep per-partition watermarking
    // through the assigner instead of racing the watermark to the fastest
    // partition (see json_string_to_row_columnar).
    using ColumnarPartitions =
        std::function<bool(const arrow::RecordBatch&, std::vector<std::optional<std::int32_t>>&)>;

    WatermarkAssignerOperator(TimeExtractor extractor,
                              std::unique_ptr<WatermarkStrategy<T>> strategy,
                              std::string name = "watermark_assigner")
        : extractor_(std::move(extractor)),
          strategy_(std::move(strategy)),
          name_(std::move(name)) {}

    // Configure idleness detection. When non-zero, an idle watermark is
    // emitted whenever no record has arrived in `duration`. 0 disables
    // (the historic shape).
    WatermarkAssignerOperator& with_idleness(std::chrono::milliseconds duration) {
        idleness_ = duration;
        return *this;
    }

    // Enroll this assigner in a watermark-alignment group. While the
    // assigner's watermark is more than `max_drift` ahead of the
    // slowest member of the group, process() blocks at the top of
    // each call - which back-pressures the upstream source via the
    // channel. When the slowest member catches up, the assigner
    // resumes.
    //
    // The group is looked up in AlignmentGroupRegistry::default_instance()
    // by name; multiple operators with the same group_name share state.
    // Setting an empty group_name disables alignment.
    //
    // `max_wait` is the per-process()-call cap on how long the
    // assigner will block; on timeout it proceeds anyway (the next
    // call will re-check). Keeping this bounded avoids wedging the
    // pipeline if the slow side has hung. Default 1s.
    WatermarkAssignerOperator& with_watermark_alignment(
        std::string group_name,
        std::chrono::milliseconds max_drift,
        std::chrono::milliseconds max_wait = std::chrono::seconds{1}) {
        if (group_name.empty()) {
            alignment_group_.reset();
            return *this;
        }
        alignment_group_ = AlignmentGroupRegistry::default_instance().get_or_create(group_name);
        alignment_member_id_ = alignment_group_->join();
        alignment_max_drift_ms_ = max_drift.count();
        alignment_max_wait_ = max_wait;
        return *this;
    }

    // Test seam: enroll in a caller-provided group (bypasses the
    // process-wide registry). Used to keep tests isolated from
    // global state.
    WatermarkAssignerOperator& with_watermark_alignment(
        std::shared_ptr<AlignmentGroup> group,
        std::chrono::milliseconds max_drift,
        std::chrono::milliseconds max_wait = std::chrono::seconds{1}) {
        if (!group) {
            alignment_group_.reset();
            return *this;
        }
        alignment_group_ = std::move(group);
        alignment_member_id_ = alignment_group_->join();
        alignment_max_drift_ms_ = max_drift.count();
        alignment_max_wait_ = max_wait;
        return *this;
    }

    // Enable the columnar fast path: when a columnar batch arrives, read its
    // event times via `reader` and forward the batch unchanged instead of
    // materialising rows. Only safe for non-partitioned sources (the Arrow
    // sidecar carries no source_partition, so per-partition watermarking
    // degrades to a single global watermark - which is exactly what a
    // non-partitioned source's records produce on the row path too, so it is
    // byte-identical for them). Columnar batches only originate from
    // non-partitioned columnar sources (Parquet/file), so this never triggers
    // for a partitioned (Kafka) source, whose batches are row-form.
    WatermarkAssignerOperator& with_columnar_event_times(ColumnarEventTimes reader) {
        columnar_event_times_ = std::move(reader);
        return *this;
    }

    // Enable per-partition watermarking on the columnar fast path. Only useful
    // alongside with_columnar_event_times and a partition-aware strategy.
    WatermarkAssignerOperator& with_columnar_partitions(ColumnarPartitions reader) {
        columnar_partitions_ = std::move(reader);
        return *this;
    }

    [[nodiscard]] bool supports_columnar() const noexcept override {
        return static_cast<bool>(columnar_event_times_);
    }

    // Columnar fast path: advance the watermark from the event-time column read
    // straight from the Arrow sidecar, then forward the SAME batch downstream
    // unchanged (sidecar preserved, zero row materialisation). Mirrors process()'s
    // data-element side effects exactly (alignment wait, strategy feed,
    // last_record_time_/is_idle_, watermark emission); the idleness timer is
    // self-driving and untouched. Returns false BEFORE any emit on a surprise so
    // process() takes over (no double-emit, no strategy double-count).
    bool process_columnar(const StreamElement<T>& element, Emitter<T>& out) override {
        if (!columnar_event_times_ || !element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& batch = element.as_data();
        const auto& rb = batch.arrow();
        if (!rb) {
            return false;
        }
        std::vector<EventTime> times;
        if (!columnar_event_times_(*rb, times)) {
            return false;  // cannot read columnar (e.g. column absent) -> row path
        }
        // Alignment wait, identical to process() (does not depend on rows).
        if (alignment_group_ && last_emitted_wm_.timestamp() > EventTime::min()) {
            alignment_group_->wait_until_within_drift(alignment_member_id_,
                                                      last_emitted_wm_,
                                                      alignment_max_drift_ms_,
                                                      alignment_max_wait_);
        }
        // Per-partition watermarking on the columnar path: if a partition reader
        // is set, read each row's source partition from the sidecar and stamp it
        // on the probe so a partition-aware strategy tracks per partition (min
        // across partitions). Without it (or on a size mismatch), the probe is
        // partition-unset = a single global watermark, matching a non-partitioned
        // source's row path. read_partitions returns false when the column is
        // absent (e.g. a Parquet source), which is the global-watermark case.
        std::vector<std::optional<std::int32_t>> parts;
        const bool have_parts = columnar_partitions_ && columnar_partitions_(*rb, parts) &&
                                parts.size() == times.size();
        // Feed the strategy once per record. The value is irrelevant (the
        // strategy reads only event_time + source_partition).
        Record<T> probe;
        for (std::size_t i = 0; i < times.size(); ++i) {
            probe.set_event_time(times[i]);
            if (have_parts && parts[i].has_value()) {
                probe.set_source_partition(*parts[i]);
            } else {
                probe.clear_source_partition();
            }
            strategy_->on_record(probe);
        }
        last_record_time_ms_ = now_ms_();
        is_idle_ = false;
        // Forward the SAME batch unchanged - the sidecar (a shared_ptr member)
        // moves with it, so the stream stays columnar with zero row decode.
        out.emit_data(std::move(const_cast<Batch<T>&>(batch)));
        if (auto wm = strategy_->current_watermark(); wm.has_value()) {
            last_emitted_wm_ = *wm;
            if (alignment_group_) {
                alignment_group_->update_watermark(alignment_member_id_, *wm);
            }
            out.emit_watermark(*wm);
        }
        return true;
    }

    void open() override {
        if (idleness_.count() > 0 && this->runtime() != nullptr) {
            // Seed the last-record time and schedule the first idleness
            // probe. Subsequent probes re-register from
            // on_processing_time_timer to form a loop.
            last_record_time_ms_ = now_ms_();
            schedule_idle_probe_();
        }
    }

    void process(const StreamElement<T>& element, Emitter<T>& out) override {
        // Watermark alignment: before processing any data element,
        // check whether we're too far ahead of the group's slowest
        // member. If so, block until they catch up or we hit
        // max_wait. Use the most-recently-emitted watermark we
        // tracked locally - the strategy's current_watermark() is
        // one-shot (clears its dirty flag), so we can't rely on it
        // for repeated checks.
        if (alignment_group_ && element.is_data() &&
            last_emitted_wm_.timestamp() > EventTime::min()) {
            alignment_group_->wait_until_within_drift(alignment_member_id_,
                                                      last_emitted_wm_,
                                                      alignment_max_drift_ms_,
                                                      alignment_max_wait_);
        }

        if (element.is_data()) {
            const Batch<T>& in_batch = element.as_data();
            // Fast path: if every record already carries an
            // event_time (the common case for sources that set
            // timestamps inline - e.g. the bench's
            // SyntheticEventSource, or any source that derived ts
            // from the payload), we don't need to mutate any record.
            // Update the strategy in-place from the incoming records
            // and forward the batch by move - the dag runner passes
            // the element once and doesn't observe it afterwards, so
            // moving out of it is safe even though the type signature
            // is const&. Avoids constructing a new Batch<T>, which
            // would copy every Record (and Record carries shared_ptr
            // fields whose copy is an atomic refcount bump).
            bool all_timestamped = true;
            for (const auto& record : in_batch) {
                if (!record.event_time().has_value()) {
                    all_timestamped = false;
                    break;
                }
            }
            if (all_timestamped) {
                for (const auto& record : in_batch) {
                    strategy_->on_record(record);
                }
                last_record_time_ms_ = now_ms_();
                is_idle_ = false;
                out.emit_data(std::move(const_cast<Batch<T>&>(in_batch)));
                if (auto wm = strategy_->current_watermark(); wm.has_value()) {
                    last_emitted_wm_ = *wm;
                    if (alignment_group_) {
                        alignment_group_->update_watermark(alignment_member_id_, *wm);
                    }
                    out.emit_watermark(*wm);
                }
                return;
            }

            Batch<T> out_batch;
            out_batch.reserve(in_batch.size());
            for (auto record : in_batch) {
                if (!record.event_time().has_value()) {
                    record.set_event_time(extractor_(record.value()));
                }
                strategy_->on_record(record);
                out_batch.push(std::move(record));
            }
            last_record_time_ms_ = now_ms_();
            is_idle_ = false;
            if (!out_batch.empty()) {
                out.emit_data(std::move(out_batch));
            }
            if (auto wm = strategy_->current_watermark(); wm.has_value()) {
                last_emitted_wm_ = *wm;
                if (alignment_group_) {
                    alignment_group_->update_watermark(alignment_member_id_, *wm);
                }
                out.emit_watermark(*wm);
            }
        } else if (element.is_watermark()) {
            // Upstream watermarks are forwarded but do not feed the strategy:
            // event-time progress is the assigner's responsibility on this
            // boundary.
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& key,
                                  Emitter<T>& out) override {
        if (key != kIdleProbeKey || idleness_.count() <= 0) {
            return;
        }
        const auto now = now_ms_();
        if (!is_idle_ && (now - last_record_time_ms_) >= idleness_.count()) {
            // Determine the timestamp to attach to the idle marker: the
            // strategy's latest observed watermark if any (preserves
            // diagnostics); otherwise EventTime::min().
            EventTime t = EventTime::min();
            if (auto wm = strategy_->current_watermark(); wm.has_value()) {
                t = wm->timestamp();
            }
            out.emit_watermark(Watermark::idle(t));
            is_idle_ = true;
        }
        schedule_idle_probe_();
    }

    void flush(Emitter<T>& out) override {
        if (auto wm = strategy_->current_watermark(); wm.has_value()) {
            out.emit_watermark(*wm);
        }
    }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kIdleProbeKey = "__idle_probe__";

    void schedule_idle_probe_() {
        auto* rt = this->runtime();
        if (rt == nullptr || idleness_.count() <= 0) {
            return;
        }
        // Probe at half the idleness interval - guarantees a detection
        // fires within idleness ms of going quiet. Caps at >= 1ms to
        // avoid a busy loop for absurdly tight settings.
        const std::int64_t probe_ms = std::max<std::int64_t>(idleness_.count() / 2, 1);
        rt->timer_service()->register_processing_time_timer(now_ms_() + probe_ms, kIdleProbeKey);
    }

    static std::int64_t now_ms_() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    TimeExtractor extractor_;
    ColumnarEventTimes columnar_event_times_;  // optional; enables process_columnar
    ColumnarPartitions columnar_partitions_;   // optional; per-partition columnar watermarking
    std::unique_ptr<WatermarkStrategy<T>> strategy_;
    std::string name_;
    std::chrono::milliseconds idleness_{0};
    std::int64_t last_record_time_ms_{0};
    bool is_idle_{false};

    // Watermark alignment state (withWatermarkAlignment).
    std::shared_ptr<AlignmentGroup> alignment_group_;
    AlignmentGroup::MemberId alignment_member_id_{0};
    std::int64_t alignment_max_drift_ms_{0};
    std::chrono::milliseconds alignment_max_wait_{std::chrono::seconds{1}};
    Watermark last_emitted_wm_{Watermark::min()};
};

}  // namespace clink
