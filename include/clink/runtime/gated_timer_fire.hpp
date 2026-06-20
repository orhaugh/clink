#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/timer_service.hpp"

namespace clink::detail {

// Fire an async operator's due PROCESSING-TIME timers through the per-key gate.
//
// On the synchronous path the runner fires processing-time timers by calling
// op.fire_due_timers() directly, which invokes on_processing_time_timer() inline
// between input pops. Under async-state execution that is unsafe: a timer
// callback that touches keyed state for key K could run while an async read for
// K is still in flight (a record suspended mid get_async on a worker thread),
// producing a torn read / lost update. The event-time path is already safe (it
// fires inside the AsyncExecutionController's epoch-gated on_watermark release,
// after the epoch has drained); only processing-time timers fire ungated.
//
// This routes each due processing-time timer through aec.submit() under the
// per-key gate, so a timer for K parks behind any in-flight record for K (and
// any record for K submitted later parks behind the timer) - exactly the
// serialisation the gate gives records. Distinct keys still overlap.
//
// OPERATOR CONTRACT (runner-unverifiable): an async operator MUST register each
// processing-time timer with the SAME key bytes it uses as the record gate key
// in process_async (e.g. key_codec.encode(k) as a std::string). The runner
// passes the timer's own key straight through as the submit gate key, so they
// serialise against the right state only when they match by construction.
//
// Algorithm: COLLECT-THEN-SUBMIT. poll_due() consumes (erases) due timers as it
// snapshots them, so we first collect the due (ts, key) set into a local vector
// (firing nothing), then submit each under its key with the standard
// backpressure spin. A timer stays owned by the local vector until its submit
// succeeds, so nothing is dropped if a submit is refused at the in-flight cap.
//
// on_processing_time_timer is invoked from INSIDE the submitted coroutine, on
// the runner thread, under the gate - so its synchronous keyed-state access is
// safe (no same-key async read can be outstanding when it runs). A timer the
// callback re-registers lands in the service and fires on a later pass.
//
// Op is any operator with runtime() and on_processing_time_timer(ts, key, out):
// both Operator<In, Out> (single-input runner) and CoOperator<In1, In2, Out>
// (the co-operator runner) qualify. Out is deduced from the Emitter argument.
template <typename Op, typename Out>
void gated_fire_processing_time_timers(Op& op,
                                       Emitter<Out>& out,
                                       std::int64_t now_ms,
                                       AsyncExecutionController& aec) {
    auto* rt = op.runtime();
    if (rt == nullptr) {
        return;
    }
    // Step 1: collect (poll_due erases as it goes; collect, fire nothing yet).
    std::vector<std::pair<std::int64_t, std::string>> to_fire;
    rt->timer_service()->poll_due(now_ms, [&to_fire](std::int64_t ts, const std::string& key) {
        to_fire.emplace_back(ts, key);
    });
    if (to_fire.empty()) {
        return;  // common case (no processing-time timers): a pure no-op
    }
    // Step 2: submit each under the per-key gate. The gate key IS the timer's
    // registered key (the operator's contract makes it the record gate key).
    for (const auto& entry : to_fire) {
        const std::int64_t ts = entry.first;
        const std::string key = entry.second;
        AsyncExecutionController::CoroFactory factory =
            [&op, &out, ts, key]() -> async::Task<void> {
            op.on_processing_time_timer(ts, key, out);
            co_return;
        };
        while (!aec.submit(key, factory)) {
            aec.poll();  // free capacity, then retry (the timer is still owned here)
        }
    }
    aec.poll();  // settle any timer that ran synchronously (free key) + surface throws
}

}  // namespace clink::detail
