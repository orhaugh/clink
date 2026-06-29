#pragma once

// PRIVATE implementation helper - lives under src/, NOT include/, so nats.c's nats.h is confined
// to the .cpp side (the public connector headers stay free of it). Included only by
// nats_source.cpp / nats_sink.cpp.

#include <stdexcept>
#include <string>

#include <nats/nats.h>

#include "clink/nats/connection_params.hpp"

namespace clink::nats::detail {

inline void check(natsStatus s, const std::string& ctx) {
    if (s != NATS_OK) {
        throw std::runtime_error(ctx + ": " + natsStatus_GetText(s));
    }
}

// Connect to NATS using URL + optional credentials. Throws std::runtime_error on failure. The
// caller owns the returned connection and must natsConnection_Destroy it (after destroying any
// JetStream context / subscription derived from it).
inline natsConnection* connect(const NatsConnParams& c, const std::string& ctx) {
    natsOptions* opts = nullptr;
    check(natsOptions_Create(&opts), ctx + ": natsOptions_Create");
    natsStatus s = natsOptions_SetURL(opts, c.url.c_str());
    if (s == NATS_OK && !c.name.empty()) {
        s = natsOptions_SetName(opts, c.name.c_str());
    }
    if (s == NATS_OK && !c.user.empty()) {
        s = natsOptions_SetUserInfo(opts, c.user.c_str(), c.password.c_str());
    }
    if (s == NATS_OK && !c.token.empty()) {
        s = natsOptions_SetToken(opts, c.token.c_str());
    }
    if (s != NATS_OK) {
        natsOptions_Destroy(opts);
        throw std::runtime_error(ctx + ": set options: " + natsStatus_GetText(s));
    }
    natsConnection* conn = nullptr;
    s = natsConnection_Connect(&conn, opts);
    natsOptions_Destroy(opts);  // options are copied into the connection; safe to free now
    if (s != NATS_OK) {
        throw std::runtime_error(ctx + ": connect to " + c.url + ": " + natsStatus_GetText(s));
    }
    return conn;
}

}  // namespace clink::nats::detail
