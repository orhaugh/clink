#pragma once

// clink::test side-output capture - register a typed OutputTag channel
// on a harness's RuntimeContext (what the executor's Dag wiring does in
// production) so an operator's runtime()->side_output<T>(tag) emits are
// captured and drainable:
//
//   OutputTag<std::string> late{"late"};
//   h.register_side_output(late);   // BEFORE open()
//   ...
//   EXPECT_EQ(h.side_output_values(late), (std::vector<std::string>{"x"}));
//
// The free functions below are the implementation; harnesses expose
// them as members.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::test {

// Wire a typed side-output channel for `tag` into `ctx`, exactly as the
// executor does from Dag::side_output registrations. `capacity` bounds
// buffered elements between drains (emits beyond it would block - drain
// in long loops).
template <typename T>
void register_side_output_channel(RuntimeContext& ctx,
                                  const OutputTag<T>& tag,
                                  std::size_t capacity = 4096) {
    auto channel =
        std::make_shared<BoundedChannel<StreamElement<T>>>(capacity, "test-side:" + tag.id);
    auto map = ctx.side_output_channels();  // copy, extend, set back
    if (map.count(tag.id) != 0) {
        throw std::logic_error("side output tag '" + tag.id + "' is already registered");
    }
    map[tag.id] = SideOutputChannelEntry{channel, [channel] { channel->close(); }};
    ctx.set_side_output_channels(std::move(map));
}

// Drain every element currently buffered on `tag`'s channel, flattening
// data batches to values (non-blocking; safe to call repeatedly).
template <typename T>
std::vector<T> drain_side_output(RuntimeContext& ctx, const OutputTag<T>& tag) {
    const auto& map = ctx.side_output_channels();
    const auto it = map.find(tag.id);
    if (it == map.end()) {
        throw std::logic_error("side output tag '" + tag.id +
                               "' was not registered on this harness; call "
                               "register_side_output() before open()");
    }
    auto* channel = static_cast<BoundedChannel<StreamElement<T>>*>(it->second.channel.get());
    std::vector<T> out;
    while (auto element = channel->try_pop()) {
        if (!element->is_data()) {
            continue;
        }
        for (const auto& rec : element->as_data()) {
            out.push_back(rec.value());
        }
    }
    return out;
}

}  // namespace clink::test
