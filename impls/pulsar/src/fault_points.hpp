#pragma once

// Named CLINK_FAULT_POINT sites for the Pulsar sink's recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION, and a reached point prints
// the fault framework's witness line, so a test can prove the fault landed
// where it meant.

namespace clink::pulsar::points {

// A barrier's flush is about to wait for the broker's answers: its messages are
// sent and the sink has not waited for any of them.
inline constexpr char kSinkBeforeAck[] = "pulsar.sink_before_ack";
// A barrier's messages are all persisted and the barrier has not been passed
// on, so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterAck[] = "pulsar.sink_after_ack";

}  // namespace clink::pulsar::points
