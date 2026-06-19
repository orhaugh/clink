#pragma once

// Batch scheduler / planner (BATCH-3).
//
// A small, side-effect-free planner over a Dag in batch mode. It computes two
// things the batch execution path keys off:
//
//   1. Logical stages, cut at the blocking edges introduced by BATCH-2
//      (Dag::blocking_edge_runner_indices). A blocking exchange is the boundary
//      between a producing stage and a consuming stage; the planner groups the
//      DAG's runners into contiguous stages so a scheduler can reason about
//      producer-before-consumer ordering.
//
//   2. A recommended per-stage parallelism derived from the bounded sources'
//      split counts (Source::split_count), clamped to operator bounds and the
//      key-group count.
//
// Pragmatic-scheduler scope: the planner is analysis only. The in-process
// LocalExecutor still launches every operator thread eagerly - the BATCH-2
// blocking exchange already enforces "no data crosses until the producer
// completes", so consumer threads simply block on their empty input until the
// exchange's flush() replays. Physical stage-by-stage thread launch (which would
// require the exchange to hand off via disk so the consumer reads the
// materialised file directly, avoiding the bounded-channel replay deadlock) is a
// documented follow-on, not part of this increment.

#include <algorithm>
#include <cstddef>
#include <vector>

#include "clink/runtime/dag.hpp"
#include "clink/runtime/key_groups.hpp"

namespace clink::batch {

// Recommended parallelism for a stage: the desired split count, clamped down to
// the operator's max parallelism and the key-group count, floored at 1. A zero
// bound is treated as "no bound" so callers can opt a clamp out.
[[nodiscard]] inline std::size_t recommended_parallelism(std::size_t split_count,
                                                         std::size_t operator_max_parallelism,
                                                         std::size_t num_key_groups) noexcept {
    std::size_t p = split_count;
    if (operator_max_parallelism > 0) {
        p = std::min(p, operator_max_parallelism);
    }
    if (num_key_groups > 0) {
        p = std::min(p, num_key_groups);
    }
    return p == 0 ? std::size_t{1} : p;
}

// One logical stage: a contiguous range of runner indices [first_runner,
// last_runner] that execute without crossing a blocking edge.
struct BatchStage {
    std::size_t index{0};
    std::size_t first_runner{0};  // inclusive
    std::size_t last_runner{0};   // inclusive

    [[nodiscard]] std::size_t runner_count() const noexcept {
        return last_runner - first_runner + 1;
    }
};

struct BatchPlan {
    std::vector<BatchStage> stages;
    std::size_t max_source_split_count{1};
    std::size_t num_key_groups{0};

    [[nodiscard]] std::size_t stage_count() const noexcept { return stages.size(); }

    // Recommended parallelism for a keyed stage given the operator's max bound
    // (0 = unbounded). Derived from the source split count, clamped to key groups.
    [[nodiscard]] std::size_t recommended_keyed_parallelism(
        std::size_t operator_max = 0) const noexcept {
        return recommended_parallelism(max_source_split_count, operator_max, num_key_groups);
    }
};

// Compute the logical batch plan for `dag`.
//
// Stages: runner indices are appended in topological order (an operator can only
// be added once its upstream handle exists, and a multi-input operator's inputs
// both precede it), so a blocking-exchange runner at index b separates the
// producing side (runner index <= b, the exchange buffers during the producer
// phase) from the consuming side (index > b). stage(i) = number of blocking-edge
// cuts strictly less than i, which is monotonic non-decreasing in i, so stages
// come out as contiguous index ranges.
[[nodiscard]] inline BatchPlan compute_batch_plan(const Dag& dag,
                                                  std::size_t num_key_groups = kNumKeyGroups) {
    BatchPlan plan;
    plan.num_key_groups = num_key_groups;

    std::size_t max_split = 1;
    for (auto s : dag.source_split_counts()) {
        max_split = std::max(max_split, s);
    }
    plan.max_source_split_count = max_split;

    const std::size_t n = dag.operator_count();
    if (n == 0) {
        return plan;
    }

    std::vector<std::size_t> cuts(dag.blocking_edge_runner_indices().begin(),
                                  dag.blocking_edge_runner_indices().end());
    std::sort(cuts.begin(), cuts.end());

    // stage_of(i) = count of cut indices strictly < i.
    auto stage_of = [&cuts](std::size_t i) {
        return static_cast<std::size_t>(std::lower_bound(cuts.begin(), cuts.end(), i) -
                                        cuts.begin());
    };

    std::size_t cur_stage = stage_of(0);
    std::size_t first = 0;
    for (std::size_t i = 1; i < n; ++i) {
        const auto s = stage_of(i);
        if (s != cur_stage) {
            plan.stages.push_back(BatchStage{plan.stages.size(), first, i - 1});
            cur_stage = s;
            first = i;
        }
    }
    plan.stages.push_back(BatchStage{plan.stages.size(), first, n - 1});
    return plan;
}

}  // namespace clink::batch
