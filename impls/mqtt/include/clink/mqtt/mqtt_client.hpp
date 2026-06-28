#pragma once

// Minimal RAII wrapper over libmosquitto's synchronous client API, shared by the
// MQTT source and sink. Three concerns it centralises:
//   1. library lifecycle - mosquitto_lib_init() is called exactly once per
//      process (call_once); mosquitto_lib_cleanup() is deliberately NOT called
//      (running it at static-destruction time races with live connection
//      destructors - same teardown-race reasoning as the S3 FinalizeS3 note).
//   2. connection lifecycle - mosquitto_new + auth + TLS + connect run in the
//      ctor, and the CONNACK is awaited (the loop is driven until on_connect
//      fires) so a constructed MqttConnection is always usable or throws.
//   3. callback plumbing - the C callbacks carry a void* userdata (this), and a
//      static trampoline forwards to std::function members the source/sink set.
//      The library uses the SYNCHRONOUS mosquitto_loop(), so callbacks fire
//      inline on the caller's thread - no cross-thread locking is needed for the
//      buffers the source/sink fill from them.
//
// TLS is built into libmosquitto (OpenSSL); there is no separate SSL library to
// find (unlike hiredis). If the installed libmosquitto was compiled without TLS,
// tls=true fails at connect with the library's own clear error.
//
// Compiled only where libmosquitto is found (CLINK_HAS_MQTT); the whole
// impls/mqtt module is dep-gated on it.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mosquitto.h>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

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
    // TLS (libmosquitto built-in OpenSSL; no separate lib). tls=true encrypts the
    // connection. tls_ca/tls_capath give the CA (PEM file / dir) to verify the
    // server; tls_cert+tls_key are an optional client cert for mutual TLS.
    // tls_verify=false skips server-cert + hostname verification (self-signed dev).
    bool tls{false};
    std::string tls_ca;
    std::string tls_capath;
    std::string tls_cert;
    std::string tls_key;
    bool tls_verify{true};
};

// Owns a struct mosquitto*. Move-only. Connects (+ auth/TLS + CONNACK wait) in
// the ctor and throws on any failure.
class MqttConnection {
public:
    using MessageCb = std::function<void(const char* topic, const void* payload, std::size_t len)>;
    using PublishCb = std::function<void(int mid)>;

    explicit MqttConnection(const ConnectOptions& o) {
        global_init_();
        op_timeout_ = o.connack_timeout;  // reused to bound the SUBACK wait
        int keepalive = o.keepalive < 5 ? 5 : o.keepalive;
        const char* id = o.client_id.empty() ? nullptr : o.client_id.c_str();
        // A NULL id requires clean_session=true (the broker cannot persist a
        // session for an anonymous client); enforce it rather than let
        // mosquitto_new reject the combination.
        const bool clean = (id == nullptr) ? true : o.clean_session;
        mosq_ = mosquitto_new(id, clean, this);
        if (mosq_ == nullptr) {
            throw std::runtime_error("mqtt: mosquitto_new failed (out of memory or invalid id)");
        }
        mosquitto_connect_callback_set(mosq_, &MqttConnection::on_connect_trampoline);
        mosquitto_disconnect_callback_set(mosq_, &MqttConnection::on_disconnect_trampoline);
        mosquitto_message_callback_set(mosq_, &MqttConnection::on_message_trampoline);
        mosquitto_publish_callback_set(mosq_, &MqttConnection::on_publish_trampoline);
        mosquitto_subscribe_callback_set(mosq_, &MqttConnection::on_subscribe_trampoline);

        if (!o.username.empty() || !o.password.empty()) {
            check_(mosquitto_username_pw_set(mosq_,
                                             o.username.empty() ? nullptr : o.username.c_str(),
                                             o.password.empty() ? nullptr : o.password.c_str()),
                   "username_pw_set");
        }
        if (o.tls) {
            configure_tls_(o);
        }

        check_(mosquitto_connect(mosq_, o.host.c_str(), o.port, keepalive), "connect");
        // mosquitto_connect did the TCP connect + sent CONNECT; the broker's
        // CONNACK (accept / reject) arrives on the next loop iterations. Drive the
        // loop until on_connect fires so credential/TLS rejections surface here.
        const auto deadline = std::chrono::steady_clock::now() + o.connack_timeout;
        while (!got_connack_ && std::chrono::steady_clock::now() < deadline) {
            int rc = mosquitto_loop(mosq_, 100, 1);
            if (rc != MOSQ_ERR_SUCCESS) {
                destroy_();
                throw std::runtime_error(std::string("mqtt: loop during connect failed: ") +
                                         mosquitto_strerror(rc));
            }
        }
        if (!got_connack_) {
            destroy_();
            throw std::runtime_error("mqtt: timed out waiting for CONNACK from " + o.host + ":" +
                                     std::to_string(o.port));
        }
        if (connack_rc_ != 0) {
            const std::string reason = mosquitto_connack_string(connack_rc_);
            destroy_();
            throw std::runtime_error("mqtt: broker refused connection: " + reason);
        }
        connected_ = true;
    }

    MqttConnection(const MqttConnection&) = delete;
    MqttConnection& operator=(const MqttConnection&) = delete;
    MqttConnection(MqttConnection&& o) noexcept { move_from_(o); }
    MqttConnection& operator=(MqttConnection&& o) noexcept {
        if (this != &o) {
            destroy_();
            move_from_(o);
        }
        return *this;
    }
    ~MqttConnection() { destroy_(); }

    // The source fills its receive buffer from this; the sink clears acked mids.
    void set_message_cb(MessageCb cb) { on_message_ = std::move(cb); }
    void set_publish_cb(PublishCb cb) { on_publish_ = std::move(cb); }

    // One synchronous network iteration: reads/writes the socket and dispatches
    // any callbacks inline on this thread. timeout_ms bounds the select() wait.
    // Returns the mosquitto rc (MOSQ_ERR_SUCCESS on success).
    int loop(int timeout_ms, int max_packets = 1) {
        return mosquitto_loop(mosq_, timeout_ms, max_packets);
    }

    // Subscribe and AWAIT the SUBACK: drive the loop until the broker confirms the
    // subscription (so it is genuinely registered before the caller's first read,
    // not merely written to the socket). Bounded by the connect/op timeout. Any
    // messages that arrive during the wait are dispatched to the message callback
    // (the caller sets it before subscribing), so they are not lost.
    void subscribe(const std::string& topic, int qos) {
        got_suback_ = false;
        check_(mosquitto_subscribe(mosq_, nullptr, topic.c_str(), qos), "subscribe");
        const auto deadline = std::chrono::steady_clock::now() + op_timeout_;
        while (!got_suback_ && std::chrono::steady_clock::now() < deadline) {
            int rc = mosquitto_loop(mosq_, 100, 1);
            if (rc != MOSQ_ERR_SUCCESS) {
                throw std::runtime_error(std::string("mqtt: loop during subscribe failed: ") +
                                         mosquitto_strerror(rc));
            }
        }
        if (!got_suback_) {
            throw std::runtime_error("mqtt: timed out waiting for SUBACK for topic " + topic);
        }
    }

    // If a user callback (message/publish) threw during a loop() dispatch, the
    // exception was caught at the C boundary and stashed here; the caller drains it
    // after loop() and surfaces it as a normal error (-> checkpoint replay).
    // Returns true and moves the message out if an error was pending.
    bool consume_callback_error(std::string& out) {
        if (!cb_error_) {
            return false;
        }
        out = std::move(cb_error_msg_);
        cb_error_ = false;
        cb_error_msg_.clear();
        return true;
    }

    // Returns the assigned message id (mid). For QoS > 0 the caller pairs the mid
    // with a PUBLISH-complete via the publish callback.
    int publish(
        const std::string& topic, const void* payload, std::size_t len, int qos, bool retain) {
        int mid = 0;
        check_(mosquitto_publish(
                   mosq_, &mid, topic.c_str(), static_cast<int>(len), payload, qos, retain),
               "publish");
        return mid;
    }

    bool want_write() const { return mosquitto_want_write(mosq_); }

    // Reconnect using the parameters from the original connect. Used by the source
    // when loop() reports the connection was lost.
    int reconnect() { return mosquitto_reconnect(mosq_); }

    [[nodiscard]] bool connected() const noexcept { return connected_; }
    struct mosquitto* native() const noexcept { return mosq_; }

private:
    // OpenSSL SSL_VERIFY_* values, passed to mosquitto_tls_opts_set as ints so we
    // do not pull OpenSSL headers into this client.
    static constexpr int kSslVerifyNone = 0;  // SSL_VERIFY_NONE
    static constexpr int kSslVerifyPeer = 1;  // SSL_VERIFY_PEER

    static void global_init_() {
        static std::once_flag once;
        std::call_once(once, [] { mosquitto_lib_init(); });
    }

    void configure_tls_(const ConnectOptions& o) {
        const char* cafile = o.tls_ca.empty() ? nullptr : o.tls_ca.c_str();
        const char* capath = o.tls_capath.empty() ? nullptr : o.tls_capath.c_str();
        const char* certfile = o.tls_cert.empty() ? nullptr : o.tls_cert.c_str();
        const char* keyfile = o.tls_key.empty() ? nullptr : o.tls_key.c_str();
        check_(mosquitto_tls_set(mosq_, cafile, capath, certfile, keyfile, nullptr), "tls_set");
        if (!o.tls_verify) {
            // Skip hostname matching AND require no peer cert: the self-signed dev
            // path (encrypt without authenticating the server).
            check_(mosquitto_tls_insecure_set(mosq_, true), "tls_insecure_set");
        }
        check_(mosquitto_tls_opts_set(
                   mosq_, o.tls_verify ? kSslVerifyPeer : kSslVerifyNone, nullptr, nullptr),
               "tls_opts_set");
    }

    void check_(int rc, const char* what) {
        if (rc != MOSQ_ERR_SUCCESS) {
            std::string msg = std::string("mqtt: ") + what + " failed: " + mosquitto_strerror(rc);
            destroy_();
            throw std::runtime_error(msg);
        }
    }

    void move_from_(MqttConnection& o) noexcept {
        mosq_ = o.mosq_;
        on_message_ = std::move(o.on_message_);
        on_publish_ = std::move(o.on_publish_);
        op_timeout_ = o.op_timeout_;
        connected_ = o.connected_;
        got_connack_ = o.got_connack_;
        got_suback_ = o.got_suback_;
        connack_rc_ = o.connack_rc_;
        cb_error_ = o.cb_error_;
        cb_error_msg_ = std::move(o.cb_error_msg_);
        o.mosq_ = nullptr;
        o.connected_ = false;
        // Re-point the library's userdata at the new owner so callbacks dispatch
        // to *this, not the moved-from shell.
        if (mosq_ != nullptr) {
            mosquitto_user_data_set(mosq_, this);
        }
    }

    void destroy_() noexcept {
        if (mosq_ != nullptr) {
            mosquitto_destroy(mosq_);
            mosq_ = nullptr;
        }
        connected_ = false;
    }

    static MqttConnection* self_(void* obj) { return static_cast<MqttConnection*>(obj); }

    // First-error-wins: stash a callback exception so the caller can surface it.
    void record_cb_error_(std::string msg) {
        if (!cb_error_) {
            cb_error_ = true;
            cb_error_msg_ = std::move(msg);
        }
    }

    // The trampolines are the C/C++ seam: libmosquitto calls them from inside its C
    // implementation of mosquitto_loop(), which has no exception support, so an
    // exception escaping here would unwind through C frames (undefined behaviour).
    // All four are noexcept, and the ones that invoke a user std::function wrap it
    // so a throwing callback (e.g. std::bad_alloc growing the receive buffer)
    // becomes a recorded error the caller drains, not a crash.
    static void on_connect_trampoline(struct mosquitto*, void* obj, int rc) noexcept {
        MqttConnection* c = self_(obj);
        c->got_connack_ = true;
        c->connack_rc_ = rc;
        c->connected_ = (rc == 0);
    }
    static void on_disconnect_trampoline(struct mosquitto*, void* obj, int /*rc*/) noexcept {
        self_(obj)->connected_ = false;
    }
    static void on_subscribe_trampoline(struct mosquitto*,
                                        void* obj,
                                        int /*mid*/,
                                        int /*qos_count*/,
                                        const int* /*granted_qos*/) noexcept {
        self_(obj)->got_suback_ = true;
    }
    static void on_message_trampoline(struct mosquitto*,
                                      void* obj,
                                      const struct mosquitto_message* m) noexcept {
        MqttConnection* c = self_(obj);
        if (!c->on_message_ || m == nullptr) {
            return;
        }
        // A zero-length MQTT message can carry a NULL payload; never form a
        // std::string from a null pointer (UB even with count 0).
        const char* payload = (m->payload != nullptr) ? static_cast<const char*>(m->payload) : "";
        const std::size_t len = m->payloadlen < 0 ? 0 : static_cast<std::size_t>(m->payloadlen);
        try {
            c->on_message_(m->topic, payload, len);
        } catch (const std::exception& e) {
            c->record_cb_error_(e.what());
        } catch (...) {
            c->record_cb_error_("unknown exception in message callback");
        }
    }
    static void on_publish_trampoline(struct mosquitto*, void* obj, int mid) noexcept {
        MqttConnection* c = self_(obj);
        if (!c->on_publish_) {
            return;
        }
        try {
            c->on_publish_(mid);
        } catch (const std::exception& e) {
            c->record_cb_error_(e.what());
        } catch (...) {
            c->record_cb_error_("unknown exception in publish callback");
        }
    }

    struct mosquitto* mosq_{nullptr};
    MessageCb on_message_;
    PublishCb on_publish_;
    std::chrono::milliseconds op_timeout_{5000};
    bool connected_{false};
    bool got_connack_{false};
    bool got_suback_{false};
    int connack_rc_{0};
    bool cb_error_{false};
    std::string cb_error_msg_;
};

}  // namespace clink::mqtt
