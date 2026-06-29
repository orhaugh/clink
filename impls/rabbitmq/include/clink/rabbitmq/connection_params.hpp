#pragma once

#include <cstdint>
#include <string>

namespace clink::rabbitmq {

// AMQP 0-9-1 connection parameters shared by the source and sink. Plain TCP only in v1
// (no TLS); the broker login uses SASL PLAIN. These are plain fields - no librabbitmq types
// leak into the public headers (amqp.h is confined to the .cpp implementation).
struct RabbitMqConnParams {
    std::string host{"localhost"};
    int port{5672};
    std::string vhost{"/"};
    std::string user{"guest"};
    std::string password{"guest"};
    int heartbeat_s{60};  // AMQP heartbeat interval (0 disables)
};

}  // namespace clink::rabbitmq
