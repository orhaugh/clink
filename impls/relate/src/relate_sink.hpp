#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "store.hpp"
#include "types.hpp"

namespace clink::relate {

// Writes each batch of rows into a Relate store as one transaction of the
// configured statement, with the rows as the $rows parameter. The store is
// opened in this process, through Relate's C library: there is no server and
// no request to lose. At least once: a checkpoint barrier returns only after
// every row received before it is in a transaction Relate committed, and a
// restore replays what came after the last completed checkpoint.
class RelateSink final : public Sink<clink::sql::Row> {
public:
    explicit RelateSink(SinkOptions options);
    ~RelateSink() override;

    void open() override;
    void on_data(const Batch<clink::sql::Row>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    void flush() override;
    void close() override;
    void close_cancelled() override;
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "relate_sink"; }

    // What the sink has done, for its summary line and for tests.
    struct Counts {
        std::uint64_t batches_committed{0};
        std::uint64_t rows_committed{0};
        std::uint64_t attempts_failed{0};
        std::uint64_t retries{0};
        // The bookmark of the last commit that wrote something: "relate:<n>",
        // n being the store's commit sequence.
        std::string last_bookmark;
    };
    [[nodiscard]] const Counts& counts() const noexcept { return counts_; }
    [[nodiscard]] std::uint64_t buffered_rows() const noexcept { return batch_.rows(); }

private:
    void add_row_(const clink::sql::Row& row);
    void commit_pending_(const char* why);
    void apply_schema_();
    void check_statement_();
    void release_();
    [[nodiscard]] std::string where_() const;
    void publish_buffer_gauges_();
    void summary_(const char* how) const;
    [[noreturn]] void fail_(const char* code, const std::string& message);

    SinkOptions opts_;
    std::unique_ptr<Store> store_;
    std::unique_ptr<Connection> conn_;
    RowsParam batch_;
    CancelSignal cancel_;
    std::chrono::steady_clock::time_point first_pending_{};
    std::uint64_t batch_seq_{0};
    Counts counts_;
    bool opened_{false};
    bool closed_{false};
};

}  // namespace clink::relate
