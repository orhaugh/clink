#pragma once

// Named CLINK_FAULT_POINT sites for the NATS JetStream sink's recovery tests.
// They compile to nothing without CLINK_FAULT_INJECTION, and a reached point
// prints the fault framework's witness line, so a test can prove the fault
// landed where it meant.

namespace clink::nats::points {

// A barrier's flush is about to wait for the server's answers: its messages are
// published and the sink has not waited for any of them.
inline constexpr char kSinkBeforeAck[] = "nats.sink_before_ack";
// A barrier's messages are all stored and the barrier has not been passed on,
// so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterAck[] = "nats.sink_after_ack";

}  // namespace clink::nats::points
