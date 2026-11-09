// The NATS JetStream sink's publisher over nats.c. See nats_publisher.hpp.

#include "clink/nats/nats_publisher.hpp"

#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include "nats_conn.hpp"

namespace clink::nats {

namespace {

class JsPublisher final : public NatsPublisher {
public:
    explicit JsPublisher(NatsPublisherSettings s) : s_(std::move(s)) {
        conn_ = detail::connect(s_.conn, "nats_sink");
        jsOptions o;
        natsStatus st = jsOptions_Init(&o);
        if (st == NATS_OK) {
            o.PublishAsync.MaxPending = s_.max_pending;
            // Every publish is answered here, positive or negative, before the
            // library counts it complete; ErrHandler is then not called.
            o.PublishAsync.AckHandler = &JsPublisher::on_answer;
            o.PublishAsync.AckHandlerClosure = this;
            st = natsConnection_JetStream(&js_, conn_, &o);
        }
        if (st != NATS_OK) {
            const std::string why =
                std::string{"nats_sink: JetStream context: "} + natsStatus_GetText(st);
            close();
            throw std::runtime_error(why);
        }
    }
    ~JsPublisher() override { close(); }
    JsPublisher(const JsPublisher&) = delete;
    JsPublisher& operator=(const JsPublisher&) = delete;

    void publish(std::string_view body) override {
        const natsStatus st = js_PublishAsync(
            js_, s_.subject.c_str(), body.data(), static_cast<int>(body.size()), nullptr);
        if (st != NATS_OK) {
            throw std::runtime_error(std::string{"js_PublishAsync: "} + natsStatus_GetText(st));
        }
    }

    NatsAnswers wait(std::uint64_t seen, std::chrono::milliseconds timeout) override {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, timeout, [&] { return answers_.acked + answers_.refused > seen; });
        if (answers_.acked + answers_.refused <= seen &&
            natsConnection_Status(conn_) == NATS_CONN_STATUS_CLOSED) {
            throw std::runtime_error("the connection to the server is closed");
        }
        return answers_;
    }

    [[nodiscard]] NatsAnswers answers() const override {
        const std::lock_guard<std::mutex> lock(mu_);
        return answers_;
    }

    void close() noexcept override {
        // The context before the connection (a nats.c requirement); it frees
        // the publishes still awaiting answers.
        jsCtx_Destroy(js_);
        js_ = nullptr;
        natsConnection_Destroy(conn_);
        conn_ = nullptr;
    }

private:
    // Called on the library's thread for every asynchronous publish. The
    // handler owns the message.
    static void on_answer(
        jsCtx* /*js*/, natsMsg* msg, jsPubAck* pa, jsPubAckErr* pae, void* closure) {
        auto* self = static_cast<JsPublisher*>(closure);
        {
            const std::lock_guard<std::mutex> lock(self->mu_);
            self->answers_.answered_bytes +=
                static_cast<std::uint64_t>(msg != nullptr ? natsMsg_GetDataLength(msg) : 0);
            if (pa != nullptr && pae == nullptr) {
                ++self->answers_.acked;
            } else {
                if (self->answers_.refused++ == 0) {
                    self->answers_.first_refusal =
                        pae != nullptr && pae->ErrText != nullptr && pae->ErrText[0] != '\0'
                            ? std::string{pae->ErrText}
                            : std::string{natsStatus_GetText(pae != nullptr ? pae->Err : NATS_ERR)};
                }
            }
        }
        self->cv_.notify_all();
        natsMsg_Destroy(msg);
    }

    NatsPublisherSettings s_;
    natsConnection* conn_{nullptr};
    jsCtx* js_{nullptr};
    mutable std::mutex mu_;
    std::condition_variable cv_;
    NatsAnswers answers_;
};

}  // namespace

std::unique_ptr<NatsPublisher> connect_nats_publisher(const NatsPublisherSettings& s) {
    return std::make_unique<JsPublisher>(s);
}

}  // namespace clink::nats
