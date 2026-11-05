#include "clink/mqtt/mqtt_subscriber.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <optional>
#include <poll.h>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#ifdef CLINK_MQTT_TLS
#include <arpa/inet.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#endif

namespace clink::mqtt {

namespace {

using Clock = std::chrono::steady_clock;
namespace proto = protocol;

#ifdef CLINK_MQTT_TLS
// The address `host` spells when it is an IPv4 or IPv6 literal (without
// brackets or an IPv6 zone); empty when it is a name. The WebSocket client and
// TlsSocket read a host the same way.
std::string ip_literal(std::string host) {
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    in_addr v4{};
    if (::inet_pton(AF_INET, host.c_str(), &v4) == 1) {
        return host;
    }
    const std::string unzoned = host.substr(0, host.find('%'));
    in6_addr v6{};
    if (::inet_pton(AF_INET6, unzoned.c_str(), &v6) == 1) {
        return unzoned;
    }
    return {};
}

std::string ssl_reason() {
    const auto* reason = ERR_reason_error_string(ERR_peek_last_error());
    return reason != nullptr ? std::string{": "} + reason : std::string{};
}
#endif

std::chrono::milliseconds remaining(Clock::time_point deadline) {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    return left.count() > 0 ? left : std::chrono::milliseconds{0};
}

}  // namespace

struct MqttSubscriber::Impl {
    SubscriberOptions opts;
    std::uint8_t version;
    int fd{-1};
#ifdef CLINK_MQTT_TLS
    SSL_CTX* ssl_ctx{nullptr};
    SSL* ssl{nullptr};
#endif
    proto::PacketReader reader;
    std::vector<char> rbuf = std::vector<char>(64 * 1024);
    bool open{false};
    std::string lost;
    std::chrono::seconds keepalive{60};
    Clock::time_point last_send{};
    std::optional<Clock::time_point> ping_sent;  // a PINGREQ awaiting its PINGRESP
    std::uint16_t next_id{0};
    // Delivered on this connection and not yet acknowledged: a PUBLISH with one of
    // these identifiers is the same message sent again.
    std::set<std::uint16_t> unacked;
    // QoS 2 messages whose PUBREC went out and whose PUBREL has not come.
    std::set<std::uint16_t> awaiting_pubrel;
    std::vector<Delivery> early;  // arrived while subscribe() waited for its SUBACK

    explicit Impl(SubscriberOptions o)
        : opts(std::move(o)), version(opts.protocol_version), reader(opts.max_packet_bytes) {
        if (version != proto::kMqtt311 && version != proto::kMqtt5) {
            throw std::runtime_error(opts.name + ": protocol_version must be MQTT 5 or MQTT 3.1.1");
        }
        if (opts.conn.client_id.empty()) {
            throw std::runtime_error(opts.name + ": a persistent session needs a client_id");
        }
        if (opts.receive_maximum == 0) {
            throw std::runtime_error(opts.name + ": receive_maximum must be at least 1");
        }
        if (opts.session_expiry_s == 0) {
            throw std::runtime_error(opts.name +
                                     ": session_expiry_s must be at least 1: a session that ends "
                                     "with its connection redelivers nothing");
        }
    }

    ~Impl() { teardown(); }

    void teardown() noexcept {
#ifdef CLINK_MQTT_TLS
        if (ssl != nullptr) {
            SSL_free(ssl);
            ssl = nullptr;
        }
        if (ssl_ctx != nullptr) {
            SSL_CTX_free(ssl_ctx);
            ssl_ctx = nullptr;
        }
#endif
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
        open = false;
    }

    void mark_lost(std::string why) {
        if (open) {
            lost = std::move(why);
        }
        teardown();
    }

    std::string where() const { return opts.conn.host + ":" + std::to_string(opts.conn.port); }

    // ---- raw I/O -------------------------------------------------------------

    // Wait for readiness until the deadline; one non-blocking look once it has
    // passed. Returns false when nothing is ready.
    bool wait_io(short events, Clock::time_point deadline) {
        while (true) {
            struct pollfd pfd = {};
            pfd.fd = fd;
            pfd.events = events;
            const int rc = ::poll(&pfd, 1, static_cast<int>(remaining(deadline).count()));
            if (rc > 0) {
                return true;
            }
            if (rc == 0) {
                return false;
            }
            if (errno != EINTR) {
                throw std::runtime_error(opts.name + ": poll: " + std::strerror(errno));
            }
        }
    }

    void send_all(const std::string& bytes, Clock::time_point deadline) {
        const char* data = bytes.data();
        const std::size_t len = bytes.size();
        std::size_t off = 0;
        while (off < len) {
#ifdef CLINK_MQTT_TLS
            if (ssl != nullptr) {
                const int rc = SSL_write(ssl, data + off, static_cast<int>(len - off));
                if (rc > 0) {
                    off += static_cast<std::size_t>(rc);
                    continue;
                }
                const int err = SSL_get_error(ssl, rc);
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                    if (!wait_io(err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline)) {
                        throw std::runtime_error(opts.name + ": send timed out");
                    }
                    continue;
                }
                throw std::runtime_error(opts.name + ": TLS send failed");
            }
#endif
#ifdef MSG_NOSIGNAL
            const auto rc = ::send(fd, data + off, len - off, MSG_NOSIGNAL);
#else
            const auto rc = ::send(fd, data + off, len - off, 0);
#endif
            if (rc > 0) {
                off += static_cast<std::size_t>(rc);
                continue;
            }
            if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!wait_io(POLLOUT, deadline)) {
                    throw std::runtime_error(opts.name + ": send timed out");
                }
                continue;
            }
            if (rc < 0 && errno == EINTR) {
                continue;
            }
            throw std::runtime_error(opts.name + ": send: " + std::strerror(errno));
        }
    }

    // One read of what is available, waiting until the deadline for the first
    // byte. Returns 0 when nothing came, -1 when the broker closed the connection.
    long recv_some(Clock::time_point deadline) {
        char* buf = rbuf.data();
        const std::size_t cap = rbuf.size();
#ifdef CLINK_MQTT_TLS
        if (ssl != nullptr) {
            while (true) {
                if (SSL_pending(ssl) == 0 && !wait_io(POLLIN, deadline)) {
                    return 0;
                }
                const int rc = SSL_read(ssl, buf, static_cast<int>(cap));
                if (rc > 0) {
                    return rc;
                }
                const int err = SSL_get_error(ssl, rc);
                if (err == SSL_ERROR_ZERO_RETURN) {
                    return -1;
                }
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                    if (remaining(deadline).count() == 0) {
                        return 0;
                    }
                    continue;
                }
                // A peer that drops TCP without a TLS close-notify: EOF.
                if (err == SSL_ERROR_SYSCALL &&
                    (errno == 0 || errno == ECONNRESET || errno == EPIPE)) {
                    return -1;
                }
                throw std::runtime_error(opts.name + ": TLS receive failed" + ssl_reason());
            }
        }
#endif
        while (true) {
            if (!wait_io(POLLIN, deadline)) {
                return 0;
            }
            const auto rc = ::recv(fd, buf, cap, 0);
            if (rc > 0) {
                return static_cast<long>(rc);
            }
            if (rc == 0) {
                return -1;
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                if (remaining(deadline).count() == 0) {
                    return 0;
                }
                continue;
            }
            if (errno == ECONNRESET) {
                return -1;
            }
            throw std::runtime_error(opts.name + ": recv: " + std::strerror(errno));
        }
    }

    // Send, or mark the connection lost. Returns whether it went.
    bool send(const std::string& bytes) {
        if (!open) {
            return false;
        }
        try {
            send_all(bytes, Clock::now() + opts.conn.connack_timeout);
        } catch (const std::exception& e) {
            mark_lost(e.what());
            return false;
        }
        last_send = Clock::now();
        return true;
    }

    // The next whole packet by the deadline; nullopt when none came. Throws when
    // the broker closes the connection.
    std::optional<proto::Packet> read_packet(Clock::time_point deadline, const char* waiting_for) {
        while (true) {
            if (auto p = reader.next()) {
                return p;
            }
            const long n = recv_some(deadline);
            if (n == 0) {
                return std::nullopt;
            }
            if (n < 0) {
                std::string why = opts.name + ": the broker at " + where() +
                                  " closed the connection before its " + waiting_for;
                if (version == proto::kMqtt5 && std::string_view{waiting_for} == "CONNACK") {
                    why +=
                        " (a broker that does not speak MQTT 5 may do that; "
                        "protocol_version='3.1.1' speaks MQTT 3.1.1)";
                }
                mark_lost(why);
                throw std::runtime_error(why);
            }
            reader.append(rbuf.data(), static_cast<std::size_t>(n));
        }
    }

    // ---- connection establishment ----------------------------------------------

    void tcp_connect(Clock::time_point deadline) {
        struct addrinfo hints = {};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo* res = nullptr;
        const std::string port_str = std::to_string(opts.conn.port);
        const int gai = ::getaddrinfo(opts.conn.host.c_str(), port_str.c_str(), &hints, &res);
        if (gai != 0 || res == nullptr) {
            throw std::runtime_error(opts.name + ": could not resolve '" + opts.conn.host +
                                     "': " + ::gai_strerror(gai));
        }
        std::string last_error = "no addresses";
        for (struct addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
            fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) {
                last_error = std::strerror(errno);
                continue;
            }
            // Non-blocking, so the deadline holds against a host that never answers.
            const int flags = ::fcntl(fd, F_GETFL, 0);
            ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#ifdef SO_NOSIGPIPE
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0 ||
                (errno == EINPROGRESS && wait_io(POLLOUT, deadline) && sock_error() == 0)) {
                int nd = 1;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
                ::freeaddrinfo(res);
                return;
            }
            last_error = std::strerror(sock_error() != 0 ? sock_error() : errno);
            ::close(fd);
            fd = -1;
        }
        ::freeaddrinfo(res);
        throw std::runtime_error(opts.name + ": could not connect to " + where() + ": " +
                                 last_error);
    }

    int sock_error() const {
        int err = 0;
        socklen_t len = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        return err;
    }

#ifdef CLINK_MQTT_TLS
    void tls_connect(Clock::time_point deadline) {
        const auto& c = opts.conn;
        ERR_clear_error();
        ssl_ctx = SSL_CTX_new(TLS_client_method());
        if (ssl_ctx == nullptr) {
            throw std::runtime_error(opts.name + ": SSL_CTX_new failed");
        }
        if (!c.tls_ca.empty() || !c.tls_capath.empty()) {
            if (SSL_CTX_load_verify_locations(
                    ssl_ctx,
                    c.tls_ca.empty() ? nullptr : c.tls_ca.c_str(),
                    c.tls_capath.empty() ? nullptr : c.tls_capath.c_str()) != 1) {
                throw std::runtime_error(opts.name + ": cannot load the CA from '" +
                                         (c.tls_ca.empty() ? c.tls_capath : c.tls_ca) + "'" +
                                         ssl_reason());
            }
        } else {
            SSL_CTX_set_default_verify_paths(ssl_ctx);
        }
        if (!c.tls_cert.empty()) {
            const std::string& key = c.tls_key.empty() ? c.tls_cert : c.tls_key;
            if (SSL_CTX_use_certificate_chain_file(ssl_ctx, c.tls_cert.c_str()) != 1 ||
                SSL_CTX_use_PrivateKey_file(ssl_ctx, key.c_str(), SSL_FILETYPE_PEM) != 1 ||
                SSL_CTX_check_private_key(ssl_ctx) != 1) {
                throw std::runtime_error(opts.name + ": cannot load the client certificate '" +
                                         c.tls_cert + "'" + ssl_reason());
            }
        }
        ssl = SSL_new(ssl_ctx);
        if (ssl == nullptr) {
            throw std::runtime_error(opts.name + ": SSL_new failed");
        }
        SSL_set_fd(ssl, fd);
        // SNI carries host names only (RFC 6066, section 3), so an address is not
        // sent. tls_verify=false, for a self-signed broker in development, checks
        // neither the chain nor the name.
        const std::string ip = ip_literal(c.host);
        if (ip.empty()) {
            SSL_set_tlsext_host_name(ssl, c.host.c_str());
        }
        if (c.tls_verify) {
            SSL_set_verify(ssl, SSL_VERIFY_PEER, nullptr);
            X509_VERIFY_PARAM* param = SSL_get0_param(ssl);
            int ok = 0;
            if (ip.empty()) {
                X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                ok = X509_VERIFY_PARAM_set1_host(param, c.host.c_str(), c.host.size());
            } else {
                ok = X509_VERIFY_PARAM_set1_ip_asc(param, ip.c_str());
            }
            if (ok != 1) {
                throw std::runtime_error(opts.name + ": cannot check the certificate against '" +
                                         c.host + "'");
            }
        } else {
            SSL_set_verify(ssl, SSL_VERIFY_NONE, nullptr);
        }
        while (true) {
            const int rc = SSL_connect(ssl);
            if (rc == 1) {
                return;
            }
            const int err = SSL_get_error(ssl, rc);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                if (!wait_io(err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline)) {
                    throw std::runtime_error(opts.name + ": TLS handshake with " + where() +
                                             " timed out");
                }
                continue;
            }
            throw std::runtime_error(opts.name + ": TLS handshake with " + where() + " failed" +
                                     ssl_reason());
        }
    }
#endif

    std::uint16_t take_packet_id() {
        next_id = static_cast<std::uint16_t>(next_id == 0xFFFF ? 1 : next_id + 1);
        return next_id;
    }

    // ---- inbound packets ---------------------------------------------------------

    void handle(const proto::Packet& p, std::vector<Delivery>& out) {
        switch (p.type) {
            case proto::PacketType::Publish: {
                auto m = proto::decode_publish(version, p.flags, p.body);
                if (m.qos == 2 && awaiting_pubrel.count(m.packet_id) != 0) {
                    // Already taken over by its PUBREC: answer again, deliver nothing.
                    send(proto::encode_ack(proto::PacketType::Pubrec, m.packet_id));
                    return;
                }
                if (m.qos > 0 && !unacked.insert(m.packet_id).second) {
                    return;  // sent again while still unacknowledged: the same message
                }
                out.push_back(Delivery{
                    std::move(m.topic), std::move(m.payload), m.packet_id, m.qos, m.dup, m.retain});
                return;
            }
            case proto::PacketType::Pubrel: {
                if (p.flags != 0x02) {
                    throw proto::ProtocolError(opts.name + ": PUBREL with reserved flags");
                }
                const auto id = proto::decode_packet_id(p.body);
                awaiting_pubrel.erase(id);
                send(proto::encode_ack(proto::PacketType::Pubcomp, id));
                return;
            }
            case proto::PacketType::Pingresp:
                ping_sent.reset();
                return;
            case proto::PacketType::Disconnect: {
                std::string why = "the broker disconnected";
                if (version == proto::kMqtt5) {
                    const auto d = proto::decode_disconnect(p.body);
                    why += ": " + proto::reason_text(version, d.reason);
                    if (!d.reason_string.empty()) {
                        why += " (" + d.reason_string + ")";
                    }
                }
                mark_lost(why);
                return;
            }
            case proto::PacketType::Suback:
            case proto::PacketType::Unsuback:
                return;  // the late answer to a subscribe that gave up waiting
            default:
                throw proto::ProtocolError(opts.name + ": unexpected packet type " +
                                           std::to_string(static_cast<int>(p.type)) +
                                           " from the broker");
        }
    }

    // Send the keepalive's PINGREQ once half the keepalive has passed with
    // nothing sent, and give the connection up when its PINGRESP has not come
    // within a whole keepalive. Returns false when the connection is gone.
    bool keep_alive() {
        if (keepalive.count() == 0) {
            return true;
        }
        const auto now = Clock::now();
        if (ping_sent) {
            if (now - *ping_sent > keepalive) {
                mark_lost("no PINGRESP within the keepalive of " +
                          std::to_string(keepalive.count()) + "s");
                return false;
            }
            return true;
        }
        if (now - last_send >= keepalive / 2) {
            if (!send(proto::encode_pingreq())) {
                return false;
            }
            ping_sent = now;
        }
        return true;
    }
};

MqttSubscriber::MqttSubscriber(SubscriberOptions opts)
    : impl_(std::make_unique<Impl>(std::move(opts))) {}

MqttSubscriber::~MqttSubscriber() = default;

bool MqttSubscriber::connect() {
    auto& im = *impl_;
    im.teardown();
    im.reader = proto::PacketReader(im.opts.max_packet_bytes);
    im.lost.clear();
    im.unacked.clear();
    im.awaiting_pubrel.clear();
    im.ping_sent.reset();
    im.early.clear();

    const auto& c = im.opts.conn;
    const auto deadline = Clock::now() + c.connack_timeout;
    try {
        im.tcp_connect(deadline);
        if (c.tls) {
#ifdef CLINK_MQTT_TLS
            im.tls_connect(deadline);
#else
            throw std::runtime_error(im.opts.name +
                                     ": tls=true but this build has no TLS support (the mqtt "
                                     "impl was built without OpenSSL)");
#endif
        }
        proto::ConnectFields f;
        f.version = im.version;
        f.client_id = c.client_id;
        f.clean_start = false;
        f.keepalive_s = static_cast<std::uint16_t>(std::clamp(c.keepalive, 5, 65535));
        if (!c.username.empty()) {
            f.username = c.username;
        }
        if (!c.password.empty()) {
            f.password = c.password;
        }
        f.session_expiry_s = im.opts.session_expiry_s;
        f.receive_maximum = im.opts.receive_maximum;
        im.send_all(proto::encode_connect(f), deadline);
        im.open = true;
        im.last_send = Clock::now();

        const auto p = im.read_packet(deadline, "CONNACK");
        if (!p) {
            throw std::runtime_error(im.opts.name + ": timed out waiting for the CONNACK from " +
                                     im.where());
        }
        if (p->type != proto::PacketType::Connack) {
            throw proto::ProtocolError(im.opts.name +
                                       ": the broker answered CONNECT with packet type " +
                                       std::to_string(static_cast<int>(p->type)));
        }
        const auto ack = proto::decode_connack(im.version, p->body);
        if (ack.code != 0) {
            std::string why = proto::reason_text(im.version, ack.code);
            if (im.version == proto::kMqtt5 && (ack.code == 0x01 || ack.code == 0x84)) {
                why +=
                    " (the broker does not speak MQTT 5; protocol_version='3.1.1' speaks MQTT "
                    "3.1.1)";
            }
            if (!ack.props.reason_string.empty()) {
                why += " (" + ack.props.reason_string + ")";
            }
            throw std::runtime_error(im.opts.name + ": the broker at " + im.where() +
                                     " refused the connection: " + why);
        }
        im.keepalive = ack.props.server_keep_alive
                           ? std::chrono::seconds{*ack.props.server_keep_alive}
                           : std::chrono::seconds{f.keepalive_s};
        return ack.session_present;
    } catch (...) {
        im.teardown();
        throw;
    }
}

int MqttSubscriber::subscribe(const std::string& filter, int qos) {
    auto& im = *impl_;
    if (!im.open) {
        throw std::runtime_error(im.opts.name + ": subscribe with no connection");
    }
    const auto id = im.take_packet_id();
    // Under MQTT 5, retained messages only with a subscription that is new.
    if (!im.send(proto::encode_subscribe(im.version,
                                         id,
                                         filter,
                                         static_cast<std::uint8_t>(qos),
                                         /*retain_handling=*/1))) {
        throw std::runtime_error(im.opts.name + ": SUBSCRIBE failed: " + im.lost);
    }
    const auto deadline = Clock::now() + im.opts.conn.connack_timeout;
    while (true) {
        const auto p = im.read_packet(deadline, "SUBACK");
        if (!p) {
            throw std::runtime_error(im.opts.name + ": timed out waiting for the SUBACK for '" +
                                     filter + "'");
        }
        if (p->type == proto::PacketType::Suback) {
            const auto s = proto::decode_suback(im.version, p->body);
            if (s.packet_id != id) {
                continue;
            }
            const auto code = s.codes.front();
            if (code >= 0x80) {
                throw std::runtime_error(
                    im.opts.name + ": the broker refused the subscription to '" + filter +
                    "': " + proto::reason_text(im.version, code) +
                    (s.reason_string.empty() ? "" : " (" + s.reason_string + ")"));
            }
            return code;
        }
        im.handle(*p, im.early);
        if (!im.open) {
            throw std::runtime_error(im.opts.name +
                                     ": connection lost while subscribing: " + im.lost);
        }
    }
}

bool MqttSubscriber::poll(std::chrono::milliseconds block, std::vector<Delivery>& out) {
    auto& im = *impl_;
    if (!im.open) {
        return false;
    }
    if (!im.early.empty()) {
        for (auto& d : im.early) {
            out.push_back(std::move(d));
        }
        im.early.clear();
    }
    if (!im.keep_alive()) {
        return false;
    }
    const auto deadline = out.empty() ? Clock::now() + block : Clock::now();
    while (true) {
        while (auto p = im.reader.next()) {
            im.handle(*p, out);
            if (!im.open) {
                return false;
            }
        }
        if (!out.empty()) {
            return true;
        }
        long n = 0;
        try {
            n = im.recv_some(deadline);
        } catch (const std::exception& e) {
            im.mark_lost(e.what());
            return false;
        }
        if (n == 0) {
            return true;
        }
        if (n < 0) {
            im.mark_lost("the broker closed the connection");
            return false;
        }
        im.reader.append(im.rbuf.data(), static_cast<std::size_t>(n));
    }
}

bool MqttSubscriber::acknowledge(std::uint16_t packet_id, std::uint8_t qos) {
    auto& im = *impl_;
    if (qos == 0) {
        return true;
    }
    const auto type = qos == 1 ? proto::PacketType::Puback : proto::PacketType::Pubrec;
    if (!im.send(proto::encode_ack(type, packet_id))) {
        return false;
    }
    im.unacked.erase(packet_id);
    if (qos == 2) {
        im.awaiting_pubrel.insert(packet_id);
    }
    return true;
}

void MqttSubscriber::disconnect() noexcept {
    auto& im = *impl_;
    if (im.open) {
        try {
            im.send_all(proto::encode_disconnect(), Clock::now() + std::chrono::milliseconds{1000});
        } catch (...) {
            // Best effort: the broker treats a vanished client the same way.
        }
    }
    im.teardown();
}

void MqttSubscriber::drop() noexcept {
    impl_->teardown();
}

bool MqttSubscriber::connected() const noexcept {
    return impl_->open;
}

std::string MqttSubscriber::lost_reason() const {
    return impl_->lost;
}

std::size_t MqttSubscriber::unacknowledged() const noexcept {
    return impl_->unacked.size();
}

std::chrono::seconds MqttSubscriber::keepalive() const noexcept {
    return impl_->keepalive;
}

}  // namespace clink::mqtt
