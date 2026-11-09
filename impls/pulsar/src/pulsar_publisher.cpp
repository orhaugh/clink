// The Pulsar sink's publisher over the Pulsar C client. See pulsar_publisher.hpp.

#include "clink/pulsar/pulsar_publisher.hpp"

#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include <pulsar/c/message.h>
#include <pulsar/c/message_id.h>
#include <pulsar/c/producer.h>
#include <pulsar/c/producer_configuration.h>

#include "pulsar_conn.hpp"

namespace clink::pulsar {

namespace {

// What the send callbacks report into. Shared with each callback's context, so
// a callback that runs after the publisher has gone touches nothing freed.
struct AnswerState {
    std::mutex mu;
    std::condition_variable cv;
    PulsarAnswers answers;
};

struct SendContext {
    std::shared_ptr<AnswerState> state;
    std::size_t bytes{0};
};

// Runs on a client thread once the broker persisted the message or the send
// failed. On success the C client allocates the message id and hands it over.
void on_sent(pulsar_result result, pulsar_message_id_t* id, void* ctx) {
    const std::unique_ptr<SendContext> c(static_cast<SendContext*>(ctx));
    if (id != nullptr) {
        pulsar_message_id_free(id);
    }
    {
        const std::lock_guard<std::mutex> lock(c->state->mu);
        auto& a = c->state->answers;
        a.answered_bytes += c->bytes;
        if (result == pulsar_result_Ok) {
            ++a.stored;
        } else if (a.failed++ == 0) {
            a.first_failure = pulsar_result_str(result);
        }
    }
    c->state->cv.notify_all();
}

void ignore_flush(pulsar_result /*result*/, void* /*ctx*/) {}

class CPublisher final : public PulsarPublisher {
public:
    explicit CPublisher(PulsarPublisherSettings s)
        : s_(std::move(s)), state_(std::make_shared<AnswerState>()) {
        client_ = detail::connect(s_.conn, "pulsar_sink");
        pulsar_producer_configuration_t* conf = pulsar_producer_configuration_create();
        pulsar_producer_configuration_set_batching_enabled(conf, s_.batching ? 1 : 0);
        pulsar_producer_configuration_set_send_timeout(conf,
                                                       static_cast<int>(s_.send_timeout.count()));
        // The sink keeps its own window below this, so the queue never refuses
        // a send for being full.
        pulsar_producer_configuration_set_max_pending_messages(conf, s_.max_pending);
        pulsar_producer_configuration_set_block_if_queue_full(conf, 0);
        const pulsar_result r =
            pulsar_client_create_producer(client_, s_.topic.c_str(), conf, &producer_);
        pulsar_producer_configuration_free(conf);
        if (r != pulsar_result_Ok) {
            producer_ = nullptr;
            close();
            detail::check_first_contact(r, "pulsar_sink: create_producer", s_.conn);
        }
    }
    ~CPublisher() override { close(); }
    CPublisher(const CPublisher&) = delete;
    CPublisher& operator=(const CPublisher&) = delete;

    void publish(std::string_view body) override {
        pulsar_message_t* msg = pulsar_message_create();
        if (msg == nullptr) {
            throw std::runtime_error("pulsar_message_create failed");
        }
        pulsar_message_set_content(msg, body.data(), body.size());
        auto* ctx = new SendContext{state_, body.size()};
        pulsar_producer_send_async(producer_, msg, &on_sent, ctx);
        // The producer holds its own reference to the message from here.
        pulsar_message_free(msg);
    }

    void push() override { pulsar_producer_flush_async(producer_, &ignore_flush, nullptr); }

    PulsarAnswers wait(std::uint64_t seen, std::chrono::milliseconds timeout) override {
        std::unique_lock<std::mutex> lock(state_->mu);
        state_->cv.wait_for(
            lock, timeout, [&] { return state_->answers.stored + state_->answers.failed > seen; });
        return state_->answers;
    }

    [[nodiscard]] PulsarAnswers answers() const override {
        const std::lock_guard<std::mutex> lock(state_->mu);
        return state_->answers;
    }

    void close() noexcept override {
        if (producer_ != nullptr) {
            (void)pulsar_producer_close(producer_);
            pulsar_producer_free(producer_);
            producer_ = nullptr;
        }
        if (client_ != nullptr) {
            (void)pulsar_client_close(client_);
            pulsar_client_free(client_);
            client_ = nullptr;
        }
    }

private:
    PulsarPublisherSettings s_;
    std::shared_ptr<AnswerState> state_;
    pulsar_client_t* client_{nullptr};
    pulsar_producer_t* producer_{nullptr};
};

}  // namespace

std::unique_ptr<PulsarPublisher> connect_pulsar_publisher(const PulsarPublisherSettings& s) {
    return std::make_unique<CPublisher>(s);
}

}  // namespace clink::pulsar
