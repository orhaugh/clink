// clink replay for PLUGIN-TYPED operators (not the SQL Row channel).
//
// register_operator<In, Out> hangs a type-erased ReplayDriver on the op's
// OperatorFactory, capturing In's codec (to read the capture) and Out's
// codec (to serialise emissions). EpochReplay picks it up for any op whose
// channels are not row->row, so `clink replay --verify` / `--emit-test`
// work on custom C++ types - the capability card-sentry's CEP detectors
// (cs_event->cs_alert) need. This pins it in-process, no .so, no CLI.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/sql/replay.hpp"

namespace fs = std::filesystem;

namespace {

using namespace clink;

// A trivially-copyable custom type on its own channel. In == Out here for
// brevity, but the operator triples the value so the emissions are a
// non-identity function of the input (a real behaviour to replay).
struct Ev {
    std::int32_t v{0};
};

Codec<Ev> ev_codec() {
    return Codec<Ev>{.encode =
                         [](const Ev& e) {
                             Codec<Ev>::Bytes out(4);
                             const auto u = static_cast<std::uint32_t>(e.v);
                             for (int i = 0; i < 4; ++i) {
                                 out[i] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
                             }
                             return out;
                         },
                     .decode = [](Codec<Ev>::BytesView b) -> std::optional<Ev> {
                         if (b.size() != 4) {
                             return std::nullopt;
                         }
                         std::uint32_t u = 0;
                         for (int i = 0; i < 4; ++i) {
                             u |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[i]))
                                  << (i * 8);
                         }
                         return Ev{static_cast<std::int32_t>(u)};
                     }};
}

class TripleOp final : public Operator<Ev, Ev> {
public:
    void process(const StreamElement<Ev>& element, Emitter<Ev>& out) override {
        if (element.is_data()) {
            Batch<Ev> b;
            for (const auto& rec : element.as_data()) {
                b.emplace(Ev{rec.value().v * 3});
            }
            out.emit_data(std::move(b));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }
    std::string name() const override { return "triple_ev"; }
};

}  // namespace

TEST(ReplayPluginTypes, CustomTypedOperatorReplaysDeterministically) {
    // Register the custom type + operator so its OperatorFactory (carrying
    // the ReplayDriver) lands in the default OperatorRegistry, exactly as a
    // dlopen'd plugin's registrations would via PluginLoader.
    plugin::PluginRegistry reg;
    reg.register_type<Ev>("ev_replay_test", ev_codec());
    reg.register_operator<Ev, Ev>(
        "triple_ev", [](const plugin::BuildContext&) { return std::make_shared<TripleOp>(); });

    // Write a capture epoch for the op, exactly as the operator runner's
    // record-capture tee would: op-<id>/subtask-0/epoch-1.cap + op.json.
    const auto dir = fs::temp_directory_path() / "clink_replay_plugin_types";
    fs::remove_all(dir);
    fs::create_directories(dir);
    const OperatorId op_id{0xABCD1234};
    {
        auto codec = std::make_shared<const Codec<Ev>>(ev_codec());
        capture::EpochCaptureBuffer<Ev> rec(dir,
                                            op_id,
                                            /*subtask_idx=*/0,
                                            /*max_records=*/0,
                                            codec);
        Batch<Ev> b;
        b.emplace(Ev{1});
        b.emplace(Ev{2});
        b.emplace(Ev{7});
        rec.on_data(b);
        rec.on_barrier(1);  // closes epoch-1.cap
        capture::write_op_spec(dir,
                               op_id,
                               /*subtask_idx=*/0,
                               capture::OpSpecSidecar{.op_type = "triple_ev",
                                                      .in_channel = "ev_replay_test",
                                                      .out_channel = "ev_replay_test",
                                                      .uid = {},
                                                      .params = {}});
    }

    clink::sql::ReplayRequest request;
    request.capture_dir = dir.string();
    request.epoch = "1";
    request.op_id = op_id.value();

    const auto replay = clink::sql::EpochReplay::load(request);
    const auto first = replay.run();
    const auto second = replay.run();

    // Three inputs -> three emissions; two runs byte-identical (--verify).
    EXPECT_EQ(first.size(), 3u);
    EXPECT_EQ(first, second);
    // The emissions are the tripled values, encoded as the Ev codec's hex:
    // 3, 6, 21 little-endian int32.
    EXPECT_EQ(first[0], "03000000");
    EXPECT_EQ(first[1], "06000000");
    EXPECT_EQ(first[2], "15000000");

    fs::remove_all(dir);
}
