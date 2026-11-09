#pragma once

// Named CLINK_FAULT_POINT sites for the Redis sinks' recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION, and a reached point prints
// the fault framework's witness line, so a test can prove the fault landed
// where it meant.

namespace clink::redis::points {

// A flush's commands are built and nothing of them has been sent: a kill here
// leaves nothing of the flush in Redis.
inline constexpr char kSinkBeforeFlush[] = "redis.sink_before_flush";
// A checkpoint barrier's flush has been applied and the barrier has not been
// passed on, so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterFlush[] = "redis.sink_after_flush";

}  // namespace clink::redis::points
