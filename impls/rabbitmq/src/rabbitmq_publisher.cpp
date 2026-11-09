// The RabbitMQ sink's publisher over librabbitmq. See rabbitmq_publisher.hpp.

#include "clink/rabbitmq/rabbitmq_publisher.hpp"

#include <stdexcept>
#include <string>
#include <utility>

#include <sys/socket.h>

#include "amqp_conn.hpp"

namespace clink::rabbitmq {

namespace {

constexpr amqp_channel_t kChannel = 1;
// The longest each step of an orderly close waits for the broker.
constexpr std::chrono::milliseconds kCloseWait{2000};

class AmqpPublisher final : public RabbitMqPublisher {
public:
    explicit AmqpPublisher(PublisherSettings s) : s_(std::move(s)) {
        conn_ = detail::connect_and_open(s_.conn, kChannel, "rabbitmq_sink");
        amqp_confirm_select(conn_, kChannel);
        const amqp_rpc_reply_t r = amqp_get_rpc_reply(conn_);
        if (r.reply_type != AMQP_RESPONSE_NORMAL) {
            const std::string why = detail::reply_error(r, "rabbitmq_sink: confirm.select");
            close(false);
            throw std::runtime_error(why);
        }
    }
    ~AmqpPublisher() override { close(false); }
    AmqpPublisher(const AmqpPublisher&) = delete;
    AmqpPublisher& operator=(const AmqpPublisher&) = delete;

    void publish(std::string_view body) override {
        if (lost_) {
            throw std::runtime_error(lost_why_);
        }
        amqp_basic_properties_t props{};
        props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.delivery_mode = s_.persistent ? 2 : 1;  // 2 = persistent
        if (!s_.content_type.empty()) {
            props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
            props.content_type = amqp_cstring_bytes(s_.content_type.c_str());
        }
        amqp_bytes_t b;
        b.len = body.size();
        b.bytes = const_cast<char*>(body.data());
        const int rc = amqp_basic_publish(conn_,
                                          kChannel,
                                          amqp_cstring_bytes(s_.exchange.c_str()),
                                          amqp_cstring_bytes(s_.routing_key.c_str()),
                                          s_.mandatory ? 1 : 0,
                                          0 /*immediate*/,
                                          &props,
                                          b);
        if (rc != AMQP_STATUS_OK) {
            lose_(std::string{"basic.publish failed: "} + amqp_error_string2(rc));
            throw std::runtime_error(lost_why_);
        }
    }

    PublisherEvent next(std::chrono::milliseconds timeout) override {
        if (lost_) {
            return PublisherEvent{PublisherEvent::Kind::Lost, 0, false, lost_why_};
        }
        struct timeval tv = detail::to_timeval(timeout);
        amqp_publisher_confirm_t result{};
        const amqp_rpc_reply_t r = amqp_publisher_confirm_wait(conn_, &tv, &result);
        if (r.reply_type == AMQP_RESPONSE_NORMAL) {
            PublisherEvent e;
            switch (result.method) {
                case AMQP_BASIC_ACK_METHOD:
                    e.kind = PublisherEvent::Kind::Ack;
                    e.delivery_tag = result.payload.ack.delivery_tag;
                    e.multiple = result.payload.ack.multiple != 0;
                    break;
                case AMQP_BASIC_NACK_METHOD:
                    e.kind = PublisherEvent::Kind::Nack;
                    e.delivery_tag = result.payload.nack.delivery_tag;
                    e.multiple = result.payload.nack.multiple != 0;
                    break;
                case AMQP_BASIC_REJECT_METHOD:
                    e.kind = PublisherEvent::Kind::Nack;
                    e.delivery_tag = result.payload.reject.delivery_tag;
                    break;
                default:
                    return lost_event_("the broker sent a confirm of an unknown kind");
            }
            amqp_maybe_release_buffers(conn_);
            return e;
        }
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            r.library_error == AMQP_STATUS_TIMEOUT) {
            return PublisherEvent{PublisherEvent::Kind::Timeout, 0, false, {}};
        }
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            r.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
            return other_frame_();
        }
        return lost_event_(detail::reply_error(r, "the confirm wait failed"));
    }

    void cancel() noexcept override {
        if (conn_ != nullptr) {
            const int fd = amqp_get_sockfd(conn_);
            if (fd >= 0) {
                (void)::shutdown(fd, SHUT_RDWR);
            }
        }
    }

    void close(bool orderly) noexcept override {
        if (conn_ == nullptr) {
            return;
        }
        if (orderly && !lost_) {
            struct timeval tv = detail::to_timeval(kCloseWait);
            (void)amqp_set_rpc_timeout(conn_, &tv);
            if (!channel_closed_) {
                (void)amqp_channel_close(conn_, kChannel, AMQP_REPLY_SUCCESS);
            }
            (void)amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
        }
        amqp_destroy_connection(conn_);
        conn_ = nullptr;
    }

private:
    void lose_(std::string why) {
        lost_ = true;
        lost_why_ = std::move(why);
    }
    PublisherEvent lost_event_(std::string why) {
        lose_(std::move(why));
        return PublisherEvent{PublisherEvent::Kind::Lost, 0, false, lost_why_};
    }

    // The frame amqp_publisher_confirm_wait put back because it was not a
    // confirm: a basic.return ahead of the ack of a mandatory message no queue
    // receives, or the broker closing the channel or the connection.
    PublisherEvent other_frame_() {
        amqp_frame_t frame{};
        struct timeval none{};
        const int rc = amqp_simple_wait_frame_noblock(conn_, &frame, &none);
        if (rc != AMQP_STATUS_OK) {
            return lost_event_(std::string{"reading the broker's frame failed: "} +
                               amqp_error_string2(rc));
        }
        if (frame.frame_type != AMQP_FRAME_METHOD) {
            return lost_event_("the broker sent a frame that is neither a confirm nor a method");
        }
        switch (frame.payload.method.id) {
            case AMQP_BASIC_RETURN_METHOD: {
                const auto* m = static_cast<amqp_basic_return_t*>(frame.payload.method.decoded);
                std::string text =
                    std::to_string(m->reply_code) + " " + detail::text_of(m->reply_text);
                // The returned message's header and body follow on the channel.
                amqp_message_t message{};
                const amqp_rpc_reply_t body = amqp_read_message(conn_, frame.channel, &message, 0);
                if (body.reply_type != AMQP_RESPONSE_NORMAL) {
                    return lost_event_(detail::reply_error(body, "reading a returned message"));
                }
                amqp_destroy_message(&message);
                amqp_maybe_release_buffers(conn_);
                return PublisherEvent{PublisherEvent::Kind::Return, 0, false, std::move(text)};
            }
            case AMQP_CHANNEL_CLOSE_METHOD: {
                const auto* m = static_cast<amqp_channel_close_t*>(frame.payload.method.decoded);
                channel_closed_ = true;
                lose_("the broker closed the channel: " + std::to_string(m->reply_code) + " " +
                      detail::text_of(m->reply_text));
                return PublisherEvent{PublisherEvent::Kind::Closed, 0, false, lost_why_};
            }
            case AMQP_CONNECTION_CLOSE_METHOD: {
                const auto* m = static_cast<amqp_connection_close_t*>(frame.payload.method.decoded);
                lose_("the broker closed the connection: " + std::to_string(m->reply_code) + " " +
                      detail::text_of(m->reply_text));
                return PublisherEvent{PublisherEvent::Kind::Closed, 0, false, lost_why_};
            }
            default:
                return lost_event_("the broker sent method " +
                                   std::to_string(frame.payload.method.id) +
                                   ", which is not a confirm");
        }
    }

    PublisherSettings s_;
    amqp_connection_state_t conn_{nullptr};
    bool lost_{false};
    bool channel_closed_{false};
    std::string lost_why_;
};

}  // namespace

std::unique_ptr<RabbitMqPublisher> connect_rabbitmq_publisher(const PublisherSettings& s) {
    return std::make_unique<AmqpPublisher>(s);
}

}  // namespace clink::rabbitmq
