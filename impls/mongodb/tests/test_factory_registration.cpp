// Verifies clink::mongodb::install() makes the mongo_cdc_source, mongo_sink and
// mongo_upsert_sink factories reachable through the registry on the "string"
// channel, that each refuses the options it cannot honour by name, and that the
// three capability records say what each mode does.

#include <map>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

namespace {

using clink::cluster::RunnerRegistry;
using Params = std::map<std::string, std::string>;

// The build error for `params` on a source or sink factory, or empty when it
// built. Building connects to nothing: open() does.
std::string build_error(const std::string& factory, bool source, const Params& params) {
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = params;
    try {
        if (source) {
            const auto* f =
                clink::cluster::OperatorRegistry::default_instance().find_source(factory, "string");
            if (f == nullptr) {
                return "not registered";
            }
            return f->build(ctx) ? std::string{} : std::string{"built nothing"};
        }
        const auto* f =
            clink::cluster::OperatorRegistry::default_instance().find_sink(factory, "string");
        if (f == nullptr) {
            return "not registered";
        }
        return f->build(ctx) ? std::string{} : std::string{"built nothing"};
    } catch (const std::exception& e) {
        return e.what();
    }
}

bool mentions(const std::string& text, const std::string& what) {
    return text.find(what) != std::string::npos;
}

Params sink_params(Params extra = {}) {
    Params p{{"database", "d"}, {"collection", "c"}};
    for (auto& [k, v] : extra) {
        p[k] = v;
    }
    return p;
}

TEST(MongoFactoryRegistration, TheFactoriesAreRegistered) {
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("mongo_sink", "string"), nullptr);
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("mongo_upsert_sink", "string"), nullptr);
    EXPECT_NE(RunnerRegistry::default_instance().find_source("mongo_cdc_source", "string"),
              nullptr);
}

TEST(MongoFactoryRegistration, ValidOptionsBuild) {
    EXPECT_EQ(build_error("mongo_sink", false, sink_params()), "");
    EXPECT_EQ(
        build_error(
            "mongo_sink", false, sink_params({{"on_duplicate", "replace"}, {"key_field", "k"}})),
        "");
    EXPECT_EQ(build_error("mongo_upsert_sink", false, sink_params({{"key_field", "k"}})), "");
    EXPECT_EQ(build_error("mongo_cdc_source",
                          true,
                          {{"database", "d"}, {"collection", "c"}, {"full_document", "required"}}),
              "");
}

// A value outside an option's domain is refused when the job is built, naming
// the option, rather than read as the default.
TEST(MongoFactoryRegistration, OptionsOutsideTheirDomainAreRefusedByName) {
    struct Case {
        std::string factory;
        bool source;
        Params params;
        std::string names;
    };
    const Case cases[] = {
        {"mongo_sink", false, sink_params({{"on_duplicate", "upsert"}}), "'on_duplicate'"},
        {"mongo_sink", false, sink_params({{"key_field", "k"}}), "'key_field' applies only"},
        {"mongo_sink", false, sink_params({{"batch_records", "0"}}), "'batch_records'"},
        {"mongo_sink", false, sink_params({{"max_bytes", "0"}}), "'max_bytes'"},
        {"mongo_sink", false, sink_params({{"linger_ms", "-1"}}), "'linger_ms'"},
        {"mongo_sink", false, sink_params({{"write_timeout_ms", "0"}}), "'write_timeout_ms'"},
        {"mongo_sink", false, {{"collection", "c"}}, "'database' and 'collection'"},
        {"mongo_upsert_sink", false, sink_params({{"key_field", "a.b"}}), "dotted path"},
        {"mongo_upsert_sink", false, sink_params({{"key_field", "$k"}}), "operator"},
        {"mongo_cdc_source", true, {{"full_document", "lookupish"}}, "'full_document'"},
        {"mongo_cdc_source", true, {{"collection", "c"}}, "'collection' requires 'database'"},
        {"mongo_cdc_source", true, {{"max_await_ms", "0"}}, "'max_await_ms'"},
    };
    for (const auto& c : cases) {
        const auto err = build_error(c.factory, c.source, c.params);
        EXPECT_TRUE(mentions(err, c.names)) << c.factory << ": " << err;
    }
}

// Each mode has its own record, so the analyser reads what the job's op does:
// the source replays from its resume token; an insert is at least once; only
// the sink that replaces by key claims that duplicates collapse, on its key.
TEST(MongoCapabilityRecords, EachModeDeclaresWhatItDoes) {
    const auto& reg = clink::connectors::CapabilityRegistry::instance();
    const auto* source = reg.find("mongo_cdc");
    ASSERT_NE(source, nullptr);
    EXPECT_TRUE(source->is_source);
    EXPECT_FALSE(source->is_sink);
    EXPECT_EQ(source->offset_model, clink::connectors::OffsetModel::OpaqueToken);
    EXPECT_EQ(source->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_TRUE(source->self_check().empty());
    const auto* insert = reg.find("mongo");
    ASSERT_NE(insert, nullptr);
    EXPECT_FALSE(insert->is_source);
    EXPECT_EQ(insert->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_TRUE(insert->idempotency_key_option.empty());
    EXPECT_TRUE(insert->self_check().empty());
    const auto* upsert = reg.find("mongo_upsert");
    ASSERT_NE(upsert, nullptr);
    EXPECT_EQ(upsert->delivery, clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent);
    EXPECT_EQ(upsert->idempotency_key_option, "key_field");
    EXPECT_TRUE(upsert->self_check().empty());
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mongo_cdc_source"), "mongo_cdc");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mongo_sink"), "mongo");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mongo_upsert_sink"), "mongo_upsert");
}

}  // namespace
