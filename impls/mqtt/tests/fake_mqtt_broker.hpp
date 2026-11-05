#pragma once

// A broker in a thread on loopback that speaks enough MQTT to drive the
// source's subscriber offline: one client at a time, a persistent session per
// client id that delivers again on reconnect what was not acknowledged, and a
// record of every acknowledgement in the order it came. Tests publish into it
// and read back what the client acknowledged. Not a general broker: one
// subscription per session, and no routing.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <poll.h>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/mqtt/mqtt_protocol.hpp"

namespace clink::mqtt::testing {

class FakeMqttBroker {
public:
    struct Connect {
        std::uint8_t version{0};
        std::string client_id;
        bool clean_start{false};
        std::uint16_t keepalive{0};
        std::optional<std::uint32_t> session_expiry;
        std::optional<std::uint16_t> receive_maximum;
    };

    FakeMqttBroker() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            throw std::runtime_error("fake broker: socket");
        }
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(listen_fd_, 4) != 0) {
            ::close(listen_fd_);
            throw std::runtime_error("fake broker: bind/listen");
        }
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve(); });
    }

    ~FakeMqttBroker() {
        {
            std::lock_guard lk(mu_);
            stop_ = true;
        }
        thread_.join();
        if (client_fd_ >= 0) {
            ::close(client_fd_);
        }
        ::close(listen_fd_);
    }

    FakeMqttBroker(const FakeMqttBroker&) = delete;
    FakeMqttBroker& operator=(const FakeMqttBroker&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

    // Answer the next CONNECTs with this CONNACK code (0 accepts again).
    void refuse_with(std::uint8_t code) {
        std::lock_guard lk(mu_);
        refuse_code_ = code;
    }

    // A message for the last client's session, sent now when it is connected
    // and otherwise when it reconnects. QoS is capped at what it subscribed
    // with. Returns its packet identifier (0 for QoS 0, which is not held).
    std::uint16_t publish(const std::string& topic, const std::string& payload, std::uint8_t qos) {
        std::lock_guard lk(mu_);
        auto& s = sessions_[last_client_];
        qos = std::min(qos, s.granted);
        if (qos == 0) {
            if (client_fd_ >= 0) {
                send_locked(publish_packet(topic, payload, 0, 0, false));
            }
            return 0;
        }
        std::uint16_t id = s.next_id;
        do {
            id = static_cast<std::uint16_t>(id == 0xFFFF ? 1 : id + 1);
        } while (s.holds(id));
        s.next_id = id;
        s.inflight.push_back(Message{id, topic, payload, qos, false});
        if (client_fd_ >= 0 && current_ == last_client_) {
            send_locked(publish_packet(topic, payload, qos, id, false));
            s.inflight.back().sent = true;
        }
        return id;
    }

    // Send a held message again on the open connection, as an MQTT 3.1.1 broker may.
    void resend(std::uint16_t id) {
        std::lock_guard lk(mu_);
        for (const auto& m : sessions_[current_].inflight) {
            if (m.id == id) {
                send_locked(publish_packet(m.topic, m.payload, m.qos, m.id, true));
            }
        }
    }

    // Close the client's connection, as a broker or network failure does.
    void drop_connection() {
        {
            std::lock_guard lk(mu_);
            if (client_fd_ >= 0) {
                ::shutdown(client_fd_, SHUT_RDWR);
            }
        }
        wait_for([this] { return client_fd_ < 0; }, std::chrono::seconds{5});
    }

    // Payloads whose PUBACK (QoS 1) or PUBREC (QoS 2) came, in the order they came.
    std::vector<std::string> acknowledged() {
        std::lock_guard lk(mu_);
        return acknowledged_;
    }
    // Payloads the last client's session holds unacknowledged, in send order.
    std::vector<std::string> unacknowledged() {
        std::lock_guard lk(mu_);
        std::vector<std::string> out;
        for (const auto& m : sessions_[last_client_].inflight) {
            out.push_back(m.payload);
        }
        return out;
    }
    // Acknowledgements for a packet identifier the session did not hold.
    std::size_t stray_acknowledgements() {
        std::lock_guard lk(mu_);
        return stray_;
    }
    std::size_t connects() {
        std::lock_guard lk(mu_);
        return connects_;
    }
    std::optional<Connect> last_connect() {
        std::lock_guard lk(mu_);
        return last_connect_;
    }
    std::size_t subscribes() {
        std::lock_guard lk(mu_);
        return subscribes_;
    }
    std::size_t pubcomps() {
        std::lock_guard lk(mu_);
        return pubcomps_;
    }
    std::size_t pingreqs() {
        std::lock_guard lk(mu_);
        return pingreqs_;
    }
    std::size_t disconnects() {
        std::lock_guard lk(mu_);
        return disconnects_;
    }
    bool connected() {
        std::lock_guard lk(mu_);
        return client_fd_ >= 0;
    }

    // Poll `pred` under the broker's lock until it holds or the timeout passes.
    bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
        const auto until = std::chrono::steady_clock::now() + timeout;
        while (true) {
            {
                std::lock_guard lk(mu_);
                if (pred()) {
                    return true;
                }
            }
            if (std::chrono::steady_clock::now() >= until) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
        }
    }

private:
    struct Message {
        std::uint16_t id;
        std::string topic;
        std::string payload;
        std::uint8_t qos;
        bool sent;
    };
    struct Session {
        std::vector<Message> inflight;
        std::set<std::uint16_t> awaiting_pubcomp;
        std::uint16_t next_id{0};
        std::uint8_t granted{2};
        bool holds(std::uint16_t id) const {
            if (awaiting_pubcomp.count(id) != 0) {
                return true;
            }
            for (const auto& m : inflight) {
                if (m.id == id) {
                    return true;
                }
            }
            return false;
        }
    };

    std::string publish_packet(const std::string& topic,
                               const std::string& payload,
                               std::uint8_t qos,
                               std::uint16_t id,
                               bool dup) const {
        std::string body;
        protocol::put_string(body, topic);
        if (qos > 0) {
            protocol::put_u16(body, id);
        }
        if (version_ == protocol::kMqtt5) {
            protocol::put_varint(body, 0);
        }
        body += payload;
        auto flags = static_cast<std::uint8_t>(qos << 1);
        if (dup) {
            flags |= 0x08;
        }
        return protocol::packet(protocol::PacketType::Publish, flags, body);
    }

    void send_locked(const std::string& bytes) {
        std::size_t off = 0;
        while (off < bytes.size() && client_fd_ >= 0) {
#ifdef MSG_NOSIGNAL
            const auto n = ::send(client_fd_, bytes.data() + off, bytes.size() - off, MSG_NOSIGNAL);
#else
            const auto n = ::send(client_fd_, bytes.data() + off, bytes.size() - off, 0);
#endif
            if (n <= 0) {
                return;  // the client went; the read side notices
            }
            off += static_cast<std::size_t>(n);
        }
    }

    void close_client_locked() {
        if (client_fd_ >= 0) {
            ::close(client_fd_);
            client_fd_ = -1;
        }
        current_.clear();
    }

    void on_connect(const std::string& body) {
        protocol::Cursor c(body);
        c.str();  // "MQTT"
        Connect info;
        info.version = c.u8();
        const auto flags = c.u8();
        info.clean_start = (flags & 0x02) != 0;
        info.keepalive = c.u16();
        if (info.version == protocol::kMqtt5) {
            const auto props = protocol::read_properties(c);
            info.session_expiry = props.session_expiry;
            info.receive_maximum = props.receive_maximum;
        }
        info.client_id = std::string(c.str());
        version_ = info.version;
        ++connects_;
        last_connect_ = info;
        std::string ack;
        if (refuse_code_ != 0) {
            protocol::put_u8(ack, 0);
            protocol::put_u8(ack, refuse_code_);
            if (version_ == protocol::kMqtt5) {
                protocol::put_varint(ack, 0);
            }
            send_locked(protocol::packet(protocol::PacketType::Connack, 0, ack));
            close_client_locked();
            return;
        }
        if (info.clean_start) {
            sessions_.erase(info.client_id);
        }
        const bool present = sessions_.count(info.client_id) != 0;
        auto& s = sessions_[info.client_id];
        current_ = info.client_id;
        last_client_ = info.client_id;
        protocol::put_u8(ack, present ? 1 : 0);
        protocol::put_u8(ack, 0);
        if (version_ == protocol::kMqtt5) {
            protocol::put_varint(ack, 0);
        }
        send_locked(protocol::packet(protocol::PacketType::Connack, 0, ack));
        // What the session holds unacknowledged goes again, in order; what was
        // sent before goes as a duplicate.
        for (auto& m : s.inflight) {
            send_locked(publish_packet(m.topic, m.payload, m.qos, m.id, m.sent));
            m.sent = true;
        }
        for (const auto id : s.awaiting_pubcomp) {
            send_locked(protocol::encode_ack(protocol::PacketType::Pubrel, id));
        }
    }

    void on_packet(const protocol::Packet& p) {
        using protocol::PacketType;
        auto& s = sessions_[current_];
        switch (p.type) {
            case PacketType::Connect:
                on_connect(p.body);
                return;
            case PacketType::Subscribe: {
                protocol::Cursor c(p.body);
                const auto id = c.u16();
                if (version_ == protocol::kMqtt5) {
                    protocol::read_properties(c);
                }
                c.str();
                s.granted = static_cast<std::uint8_t>(c.u8() & 0x03);
                ++subscribes_;
                std::string ack;
                protocol::put_u16(ack, id);
                if (version_ == protocol::kMqtt5) {
                    protocol::put_varint(ack, 0);
                }
                protocol::put_u8(ack, s.granted);
                send_locked(protocol::packet(PacketType::Suback, 0, ack));
                return;
            }
            case PacketType::Puback:
            case PacketType::Pubrec: {
                const auto id = protocol::decode_packet_id(p.body);
                const auto want = p.type == PacketType::Puback ? 1 : 2;
                for (auto it = s.inflight.begin(); it != s.inflight.end(); ++it) {
                    if (it->id == id && it->qos == want) {
                        acknowledged_.push_back(it->payload);
                        s.inflight.erase(it);
                        if (want == 2) {
                            s.awaiting_pubcomp.insert(id);
                            send_locked(protocol::encode_ack(PacketType::Pubrel, id));
                        }
                        return;
                    }
                }
                ++stray_;
                return;
            }
            case PacketType::Pubcomp:
                s.awaiting_pubcomp.erase(protocol::decode_packet_id(p.body));
                ++pubcomps_;
                return;
            case PacketType::Pingreq:
                ++pingreqs_;
                send_locked(protocol::packet(PacketType::Pingresp, 0, {}));
                return;
            case PacketType::Disconnect:
                ++disconnects_;
                close_client_locked();
                return;
            default:
                return;
        }
    }

    void serve() {
        protocol::PacketReader reader;
        std::vector<char> buf(65536);
        while (true) {
            int fd = -1;
            {
                std::lock_guard lk(mu_);
                if (stop_) {
                    return;
                }
                fd = client_fd_;
            }
            pollfd pfd{};
            pfd.fd = fd >= 0 ? fd : listen_fd_;
            pfd.events = POLLIN;
            if (::poll(&pfd, 1, 20) <= 0) {
                continue;
            }
            if (fd < 0) {
                const int c = ::accept(listen_fd_, nullptr, nullptr);
                if (c >= 0) {
                    std::lock_guard lk(mu_);
                    client_fd_ = c;
                    reader = protocol::PacketReader{};
                }
                continue;
            }
            const auto n = ::recv(fd, buf.data(), buf.size(), 0);
            std::lock_guard lk(mu_);
            if (n <= 0) {
                close_client_locked();
                continue;
            }
            reader.append(buf.data(), static_cast<std::size_t>(n));
            while (client_fd_ >= 0) {
                auto p = reader.next();
                if (!p) {
                    break;
                }
                on_packet(*p);
            }
        }
    }

    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread thread_;
    std::mutex mu_;
    bool stop_{false};
    int client_fd_{-1};
    std::uint8_t version_{protocol::kMqtt5};
    std::uint8_t refuse_code_{0};
    std::string current_;      // the connected client's id
    std::string last_client_;  // the most recent client's id
    std::map<std::string, Session> sessions_;
    std::vector<std::string> acknowledged_;
    std::size_t stray_{0};
    std::size_t connects_{0};
    std::size_t subscribes_{0};
    std::size_t pubcomps_{0};
    std::size_t pingreqs_{0};
    std::size_t disconnects_{0};
    std::optional<Connect> last_connect_;
};

}  // namespace clink::mqtt::testing
