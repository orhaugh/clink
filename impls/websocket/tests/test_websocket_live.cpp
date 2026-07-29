// Live WebSocket test, env-gated and skipped by default. Point it at any
// real feed:
//
//   CLINK_WS_LIVE_URL='wss://stream.example.test/ws' \
//   CLINK_WS_LIVE_SUBSCRIBE='{"op":"subscribe","channel":"trades"}' \
//   ./clink_websocket_tests --gtest_filter='WebSocketLive.*'
//
// The assertion is deliberately minimal - one non-empty text message inside
// the deadline - because the feed's content is not ours to pin.

#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/websocket/websocket_source.hpp"

namespace {

using namespace std::chrono_literals;

TEST(WebSocketLive, ReceivesOneMessage) {
    const char* url = std::getenv("CLINK_WS_LIVE_URL");
    if (url == nullptr || *url == '\0') {
        GTEST_SKIP() << "set CLINK_WS_LIVE_URL to run the live WebSocket test";
    }
    const char* subscribe = std::getenv("CLINK_WS_LIVE_SUBSCRIBE");

    clink::websocket::WebSocketSourceOptions opts;
    opts.url = url;
    if (subscribe != nullptr) {
        opts.subscribe = subscribe;
    }
    opts.max_messages = 1;
    opts.block = 1000ms;
    opts.open_timeout = 15s;
    clink::websocket::WebSocketSource src(std::move(opts));
    src.open();

    std::vector<std::string> got;
    clink::Emitter<std::string> em([&](clink::StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (src.produce(em) && std::chrono::steady_clock::now() < deadline) {
    }
    src.close();

    ASSERT_EQ(got.size(), 1u) << "no message arrived from " << url << " within 30s";
    EXPECT_FALSE(got.front().empty());
}

}  // namespace
