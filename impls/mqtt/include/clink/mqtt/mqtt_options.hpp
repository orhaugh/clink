#pragma once

// Connection options shared by the sink's libmosquitto client (mqtt_client.hpp)
// and the source's subscriber (mqtt_subscriber.hpp): the broker address, the
// client identity and session, keepalive, credentials and TLS.

#include <chrono>
#include <cstdint>
#include <string>

namespace clink::mqtt {

struct ConnectOptions {
    std::string host{"localhost"};
    std::uint16_t port{1883};
    std::string client_id;     // empty -> library-generated random id (forces clean_session)
    bool clean_session{true};  // false + stable client_id = persistent session (broker queues)
    int keepalive{60};         // seconds; clamped to >= 5 (MQTT minimum)
    std::string username;      // empty = no auth
    std::string password;
    std::chrono::milliseconds connack_timeout{5000};  // bound the CONNACK wait in the ctor
    // TLS. tls=true encrypts the connection. tls_ca/tls_capath give the CA (PEM
    // file / dir) to verify the server; tls_cert+tls_key are an optional client
    // cert for mutual TLS. tls_verify=false skips server-cert + hostname
    // verification (self-signed dev).
    bool tls{false};
    std::string tls_ca;
    std::string tls_capath;
    std::string tls_cert;
    std::string tls_key;
    bool tls_verify{true};
};

}  // namespace clink::mqtt
