#pragma once

// Named CLINK_FAULT_POINT sites for the RabbitMQ sink's recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION, and a reached point prints
// the fault framework's witness line, so a test can prove the fault landed
// where it meant.

namespace clink::rabbitmq::points {

// A barrier's flush is about to wait for the broker's confirms: its messages
// are published and none of their confirms has been read.
inline constexpr char kSinkBeforeConfirm[] = "rabbitmq.sink_before_confirm";
// A barrier's messages are all confirmed and the barrier has not been passed
// on, so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterConfirm[] = "rabbitmq.sink_after_confirm";

}  // namespace clink::rabbitmq::points
