#pragma once

// BlackholeRowSink: the discard sink for the Row channel, counting without ever
// materialising a batch into rows.
//
// It lives in a header rather than inside install.cpp because it has to be
// registered from two places and MUST be the same class in both. It previously was
// not, and the consequence was invisible: clink::nexmark::register_nexmark_factories
// also registered "blackhole_sink_row", as a FunctionSink<Row> with an empty
// callback, and factory registration is latest-wins. clink_node installs the SQL
// factories and then the nexmark ones, so every build of the node that linked SQL
// silently ran the per-record sink - including the binary the nexmark benchmark
// measures. A comment at the call site asserted the re-registration was harmless.
//
// The cost was not small. A FunctionSink iterates the batch, and the FIRST row
// accessor decodes the whole Arrow sidecar into name-keyed rows - an allocation and
// a string-keyed map per record - purely to call a callback whose body is empty. On
// nexmark q0 through a real coordinator/worker pair that was one materialisation for
// every batch in the run (14,377 of 14,377 at 256 rows each), so the pipeline paid
// for the columnar representation AND the row representation. Measured at +0.54s on
// a 1.02s q0 decode.
//
// Answering the columnar hook lets the runner hand the batch over untouched and take
// the row count from the sidecar, and (via enable_columnar_output) lets the operator
// UPSTREAM emit born-columnar too, which the planner otherwise forbids in front of a
// sink.

#include <cstdint>
#include <string>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace clink::sql {

class BlackholeRowSink final : public Sink<Row> {
public:
    void on_data(const Batch<Row>& batch) override {
        // Row form: count without touching a row accessor. size() answers from the
        // sidecar when there is one and from the vector otherwise, so this never
        // materialises either.
        count_ += batch.size();
    }
    [[nodiscard]] bool supports_columnar() const noexcept override { return true; }
    bool on_data_columnar(const Batch<Row>& batch) override {
        count_ += batch.size();
        return true;
    }
    [[nodiscard]] std::string name() const override { return "blackhole_sink_row"; }
    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }

private:
    std::uint64_t count_{0};
};

}  // namespace clink::sql
