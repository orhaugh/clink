#pragma once

// PRIVATE implementation helper - lives under src/, NOT include/, so librabbitmq's amqp.h is
// confined to the .cpp side (the public connector headers stay free of it, mirroring how
// clink::kafka keeps librdkafka private). Included only by rabbitmq_source.cpp / _sink.cpp.

#include <stdexcept>
#include <string>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include "clink/rabbitmq/connection_params.hpp"

namespace clink::rabbitmq::detail {

// Human-readable message for a non-normal amqp_rpc_reply_t.
inline std::string reply_error(const amqp_rpc_reply_t& r, const std::string& ctx) {
    switch (r.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            return {};
        case AMQP_RESPONSE_NONE:
            return ctx + ": missing RPC reply (connection closed?)";
        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            return ctx + ": " + amqp_error_string2(r.library_error);
        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (r.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    const auto* m = static_cast<amqp_connection_close_t*>(r.reply.decoded);
                    return ctx + ": server connection error " + std::to_string(m->reply_code);
                }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    const auto* m = static_cast<amqp_channel_close_t*>(r.reply.decoded);
                    return ctx + ": server channel error " + std::to_string(m->reply_code);
                }
                default:
                    return ctx + ": server exception, method id " + std::to_string(r.reply.id);
            }
    }
    return ctx + ": unknown error";
}

inline void check_reply(const amqp_rpc_reply_t& r, const std::string& ctx) {
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        throw std::runtime_error(reply_error(r, ctx));
    }
}

// Open a TCP connection, log in (SASL PLAIN), and open the given channel. Throws
// std::runtime_error on any failure (leaving nothing to clean up). The caller owns the returned
// connection and must close_and_destroy it.
inline amqp_connection_state_t connect_and_open(const RabbitMqConnParams& c,
                                                amqp_channel_t channel,
                                                const std::string& ctx) {
    amqp_connection_state_t conn = amqp_new_connection();
    if (conn == nullptr) {
        throw std::runtime_error(ctx + ": amqp_new_connection failed");
    }
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (socket == nullptr) {
        amqp_destroy_connection(conn);
        throw std::runtime_error(ctx + ": amqp_tcp_socket_new failed");
    }
    const int s = amqp_socket_open(socket, c.host.c_str(), c.port);
    if (s != AMQP_STATUS_OK) {
        amqp_destroy_connection(conn);
        throw std::runtime_error(ctx + ": connect to " + c.host + ":" + std::to_string(c.port) +
                                 " failed: " + amqp_error_string2(s));
    }
    const amqp_rpc_reply_t login = amqp_login(conn,
                                              c.vhost.c_str(),
                                              0 /*channel_max=default*/,
                                              AMQP_DEFAULT_FRAME_SIZE,
                                              c.heartbeat_s,
                                              AMQP_SASL_METHOD_PLAIN,
                                              c.user.c_str(),
                                              c.password.c_str());
    if (login.reply_type != AMQP_RESPONSE_NORMAL) {
        const std::string e = reply_error(login, ctx + ": login");
        amqp_destroy_connection(conn);
        throw std::runtime_error(e);
    }
    amqp_channel_open(conn, channel);
    const amqp_rpc_reply_t chr = amqp_get_rpc_reply(conn);
    if (chr.reply_type != AMQP_RESPONSE_NORMAL) {
        const std::string e = reply_error(chr, ctx + ": channel_open");
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        throw std::runtime_error(e);
    }
    return conn;
}

inline void close_and_destroy(amqp_connection_state_t conn, amqp_channel_t channel) {
    if (conn == nullptr) {
        return;
    }
    // Best-effort orderly shutdown; ignore errors (we are tearing down anyway).
    amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
}

}  // namespace clink::rabbitmq::detail
