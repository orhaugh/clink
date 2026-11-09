#pragma once

// Named CLINK_FAULT_POINT sites for the MQTT sink's recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION, and a reached point prints
// the fault framework's witness line, so a test can prove the fault landed
// where it meant.

namespace clink::mqtt::points {

// A flush's messages are handed to the connection and the sink has not waited
// for any of the broker's acknowledgements.
inline constexpr char kSinkBeforeAck[] = "mqtt.sink_before_ack";
// A barrier's messages are all acknowledged and the barrier has not been passed
// on, so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterAck[] = "mqtt.sink_after_ack";

}  // namespace clink::mqtt::points
