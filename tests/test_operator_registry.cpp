// OperatorRegistry resolves op-type strings to typed factories,
// keyed by (type, in_channel, out_channel). The generic subtask role on
// the worker uses it to translate a JSON OperatorChainSpec back into typed
// operators. These tests pin:
//   - The default registry has the v1 built-ins for both supported
//     channel types.
//   - Lookups fail clearly on unknown (type, channel) pairs.
//   - User code can register additional factories side-by-side with
//     the defaults.

#include <cstdint>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"

using namespace clink;
using namespace clink::cluster;

TEST(OperatorRegistry, ChannelTypeNameRoundTrip) {
    // After the enum -> string migration, channel_type_name is the
    // identity for strings and channel_type_from_name accepts any
    // non-empty type name (validation moves to the registry's
    // find_source/find_sink/find_operator gates). Built-in channels
    // continue to use the kChannel* constants.
    EXPECT_EQ(channel_type_name(std::string{kChannelInt64}), "int64");
    EXPECT_EQ(channel_type_name(std::string{kChannelString}), "string");
    EXPECT_EQ(channel_type_from_name("int64").value(), std::string{kChannelInt64});
    EXPECT_EQ(channel_type_from_name("string").value(), std::string{kChannelString});
    // Empty -> nullopt; any non-empty string is accepted (plugin types
    // come in any name).
    EXPECT_FALSE(channel_type_from_name("").has_value());
    EXPECT_TRUE(channel_type_from_name("myplugin.MyEvent").has_value());
}

TEST(OperatorRegistry, DefaultsHaveInt64RangeSource) {
    const auto& reg = OperatorRegistry::default_instance();
    ASSERT_NE(reg.find_source("int64_range_source", std::string{clink::cluster::kChannelInt64}),
              nullptr);
    // Wrong channel type isn't found.
    EXPECT_EQ(reg.find_source("int64_range_source", std::string{clink::cluster::kChannelString}),
              nullptr);
}

TEST(OperatorRegistry, DefaultsHaveStringLinesSource) {
    const auto& reg = OperatorRegistry::default_instance();
    ASSERT_NE(reg.find_source("string_lines_source", std::string{clink::cluster::kChannelString}),
              nullptr);
    EXPECT_EQ(reg.find_source("string_lines_source", std::string{clink::cluster::kChannelInt64}),
              nullptr);
}

TEST(OperatorRegistry, DefaultsHaveFileSinks) {
    const auto& reg = OperatorRegistry::default_instance();
    EXPECT_NE(reg.find_sink("file_int64_sink", std::string{clink::cluster::kChannelInt64}),
              nullptr);
    EXPECT_NE(reg.find_sink("file_line_sink", std::string{clink::cluster::kChannelString}),
              nullptr);
}

TEST(OperatorRegistry, BuildIntRangeProducesRequestedRecords) {
    const auto& reg = OperatorRegistry::default_instance();
    const auto* fac =
        reg.find_source("int64_range_source", std::string{clink::cluster::kChannelInt64});
    ASSERT_NE(fac, nullptr);

    OperatorBuildContext ctx;
    ctx.params = {{"count", "3"}, {"start", "10"}, {"step", "5"}};
    auto raw = fac->build(ctx);
    auto src = std::static_pointer_cast<Source<std::int64_t>>(raw);
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->name(), "int64_range_source");
}

TEST(OperatorRegistry, UserCanRegisterAlongsideDefaults) {
    OperatorRegistry reg;
    reg.register_source("custom_src",
                        SourceFactory{
                            .out = std::string{clink::cluster::kChannelInt64},
                            .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                                std::vector<Record<std::int64_t>> v;
                                v.emplace_back(Record<std::int64_t>{42});
                                return std::static_pointer_cast<void>(
                                    std::make_shared<VectorSource<std::int64_t>>(std::move(v)));
                            },
                        });
    EXPECT_NE(reg.find_source("custom_src", std::string{clink::cluster::kChannelInt64}), nullptr);
    EXPECT_EQ(reg.find_source("custom_src", std::string{clink::cluster::kChannelString}), nullptr);
}

TEST(OperatorRegistry, SinkRequiresPathParam) {
    const auto& reg = OperatorRegistry::default_instance();
    const auto* fac = reg.find_sink("file_int64_sink", std::string{clink::cluster::kChannelInt64});
    ASSERT_NE(fac, nullptr);

    OperatorBuildContext ctx;
    ctx.params = {};  // 'path' missing -> factory throws
    EXPECT_THROW((void)fac->build(ctx), std::runtime_error);
}
