#pragma once

#include <string>

namespace clink::nats {

// NATS connection parameters shared by the source and sink. Plain fields - no nats.c types
// leak into the public headers (nats.h is confined to the .cpp implementation). Auth is via
// user/password or a token (set whichever the server expects; leave blank for no-auth).
struct NatsConnParams {
    std::string url{"nats://localhost:4222"};  // comma-separated URLs allowed by nats.c
    std::string user;
    std::string password;
    std::string token;
    std::string name{"clink"};  // client connection name (shown in NATS monitoring)
};

}  // namespace clink::nats
