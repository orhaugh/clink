// Stable tier: core types, codecs, declared types and time primitives.
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/derived_codec.hpp"
#include "clink/core/fields.hpp"
#include "clink/core/pane_info.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

struct Position {
    std::int64_t account{};
    std::string symbol;
    std::int64_t qty{};
    double avg_price{};
    std::optional<std::string> note;
    std::vector<std::int64_t> fills;
    std::map<std::string, std::int64_t> tags;
};
CLINK_FIELDS(Position, account, symbol, qty, avg_price, note, fills, tags);
static_assert(clink::HasArrowFields<Position>);

namespace {

[[maybe_unused]] void identifiers() {
    const clink::OperatorId op{1};
    const clink::CheckpointId ckpt{2};
    (void)op;
    (void)ckpt;
    const clink::OperatorId from_uid = clink::operator_id_from_uid("my-operator");
    (void)from_uid;
}

[[maybe_unused]] void time_primitives() {
    constexpr clink::EventTime t = clink::EventTime::from_millis(1'000);
    (void)clink::EventTime::from_system_clock(std::chrono::system_clock::now());
    static_assert(clink::EventTime::min() < clink::EventTime::max());
    (void)t.millis();

    constexpr clink::Watermark wm{t};
    (void)wm.timestamp();
    (void)wm.is_idle();
    (void)clink::Watermark::idle();
    (void)clink::Watermark::min();
    (void)clink::Watermark::max();
    static_assert(clink::Watermark::min() < clink::Watermark::max());

    constexpr clink::CheckpointBarrier b1{clink::CheckpointId{7}};
    constexpr clink::CheckpointBarrier b2{clink::CheckpointId{7}, /*terminal=*/true};
    constexpr clink::CheckpointBarrier b3{clink::CheckpointId{7},
                                          clink::CheckpointBarrier::Mode::Unaligned};
    (void)b1.id();
    (void)b2.is_terminal();
    (void)b3.mode();
}

[[maybe_unused]] void records_and_elements() {
    clink::Record<std::int64_t> plain{42};
    clink::Record<std::int64_t> stamped{42, clink::EventTime{5}};
    (void)plain.value();
    (void)stamped.event_time();
    plain.set_event_time(clink::EventTime{6});
    (void)plain.pane();
    plain.set_pane(clink::PaneInfo{});
    (void)plain.source_partition();

    clink::Batch<std::int64_t> batch{std::vector<clink::Record<std::int64_t>>{plain, stamped}};
    batch.push(plain);
    (void)batch.size();
    (void)batch.empty();
    for (const auto& r : batch) {
        (void)r.value();
    }
    (void)batch[0];
    (void)batch.records();

    auto data = clink::StreamElement<std::int64_t>::data(std::move(batch));
    auto wm = clink::StreamElement<std::int64_t>::watermark(clink::Watermark::max());
    auto bar = clink::StreamElement<std::int64_t>::barrier(
        clink::CheckpointBarrier{clink::CheckpointId{1}});
    (void)data.is_data();
    (void)data.as_data();
    (void)wm.is_watermark();
    (void)wm.as_watermark();
    (void)bar.is_barrier();
    (void)bar.as_barrier();
    (void)data.kind();

    const clink::PaneInfo pane{.timing = clink::PaneInfo::Timing::Late,
                               .pane_index = 2,
                               .is_first = false,
                               .is_last = true};
    (void)pane;

    const clink::OutputTag<std::string> tag{"late"};
    (void)tag.id;
    (void)tag.name();
}

[[maybe_unused]] void codecs() {
    clink::Codec<std::int64_t> i64 = clink::int64_codec();
    clink::Codec<std::uint64_t> u64 = clink::uint64_codec();
    clink::Codec<std::uint32_t> u32 = clink::uint32_codec();
    clink::Codec<std::string> str = clink::string_codec();
    clink::Codec<double> dbl = clink::trivial_codec<double>();
    auto vec = clink::vector_codec<std::int64_t>(i64);
    auto pr = clink::pair_codec<std::string, std::int64_t>(str, i64);
    auto opt = clink::optional_codec<std::int64_t>(i64);
    auto set = clink::set_codec<std::int64_t>(i64);
    auto uset = clink::unordered_set_codec<std::int64_t>(i64);
    auto map = clink::map_codec<std::string, std::int64_t>(str, i64);
    auto umap = clink::unordered_map_codec<std::string, std::int64_t>(str, i64);
    (void)u64;
    (void)u32;
    (void)dbl;
    (void)vec;
    (void)pr;
    (void)opt;
    (void)set;
    (void)uset;
    (void)map;
    (void)umap;

    const clink::Codec<std::int64_t>::Bytes bytes = i64.encode(1);
    const std::optional<std::int64_t> back = i64.decode(bytes);
    (void)back;
    clink::Codec<std::int64_t>::Bytes scratch;
    if (i64.encode_into) {
        i64.encode_into(2, scratch);
    }

    // A hand-written codec is a pair of callables; encode_into is optional.
    clink::Codec<Position> by_hand{
        .encode = [](const Position&) { return clink::Codec<Position>::Bytes{}; },
        .decode = [](clink::Codec<Position>::BytesView) { return std::optional<Position>{}; },
        .encode_into = nullptr,
    };
    (void)by_hand;

    // Declared types: one CLINK_FIELDS declaration derives the byte codec.
    const clink::Codec<Position> derived = clink::derived_codec<Position>();
    (void)derived.encode(Position{});
}

[[maybe_unused]] void arrow_batchers() {
    clink::ArrowBatcher<std::int64_t> i64 = clink::int64_arrow_batcher();
    (void)i64.schema;
    (void)i64.build;
    (void)i64.parse;
    (void)clink::int32_arrow_batcher();
    (void)clink::uint32_arrow_batcher();
    (void)clink::uint64_arrow_batcher();
    (void)clink::string_arrow_batcher();
    (void)clink::int64_keyed_arrow_batcher();
    (void)clink::string_keyed_arrow_batcher();
    (void)clink::make_default_arrow_batcher<Position>(clink::derived_codec<Position>());
}

}  // namespace
