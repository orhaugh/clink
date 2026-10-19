// The CDC factories' options (register_factories.cpp cdc_config): every value
// outside an option's domain is refused with the option named, the SQL CDC
// contract's requirements are enforced at build time, and nothing connects to
// a server until open().

#include <map>
#include <memory>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/postgres/cdc_event_codec.hpp"

namespace {

using Params = std::map<std::string, std::string>;

Params contract(Params extra = {}) {
    Params p = {{"conninfo", "host=127.0.0.1 port=1 dbname=x"},
                {"slot_name", "s"},
                {"publication_names", "p"},
                {"changelog_mode", "upsert"},
                {"table", "public.orders"},
                {"primary_key", "id"},
                {"declared_types", "id:BIGINT;status:VARCHAR"}};
    for (auto& [k, v] : extra) {
        p[k] = v;
    }
    return p;
}

// The build error for `params` on `factory`, or empty when it built.
std::string build_error(const std::string& factory, const Params& params) {
    const std::string channel = factory == "postgres_cdc_event_source"
                                    ? std::string{clink::kChannelPostgresCdcEvent}
                                    : "string";
    const auto* sf =
        clink::cluster::OperatorRegistry::default_instance().find_source(factory, channel);
    if (sf == nullptr) {
        return "factory not registered";
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = params;
    try {
        auto built = sf->build(ctx);
        return built ? std::string{} : std::string{"built nothing"};
    } catch (const std::exception& e) {
        return e.what();
    }
}

bool mentions(const std::string& text, const std::string& what) {
    return text.find(what) != std::string::npos;
}

}  // namespace

TEST(PostgresCdcFactoryOptions, TheSqlContractBuildsWithItsRequiredOptions) {
    EXPECT_EQ(build_error("postgres_cdc_source", contract()), "");
    EXPECT_EQ(build_error("postgres_cdc_source",
                          contract({{"changelog_mode", "append"}, {"snapshot_mode", "never"}})),
              "");
}

TEST(PostgresCdcFactoryOptions, ValuesOutsideTheirDomainAreRefusedByName) {
    struct Case {
        std::string key;
        std::string value;
    };
    for (const Case& c : {Case{"changelog_mode", "retract"},
                          Case{"snapshot_mode", "always"},
                          Case{"on_decode_error", "skip"},
                          Case{"create_slot", "yes"},
                          Case{"drop_slot_on_close", "1"},
                          Case{"plugin", "wal2json"},
                          Case{"snapshot_batch_rows", "0"},
                          Case{"reconnect_timeout_ms", "-1"},
                          Case{"status_interval_ms", "10s"},
                          Case{"poll_interval_ms", "0"},
                          Case{"on_sub_millisecond", "round"}}) {
        const std::string err = build_error("postgres_cdc_source", contract({{c.key, c.value}}));
        EXPECT_TRUE(mentions(err, "'" + c.key + "'")) << c.key << "=" << c.value << ": " << err;
    }
}

TEST(PostgresCdcFactoryOptions, TheContractNeedsATableAKeyAndPgoutput) {
    Params no_table = contract();
    no_table.erase("table");
    EXPECT_TRUE(mentions(build_error("postgres_cdc_source", no_table), "'table'"));

    Params no_key = contract();
    no_key.erase("primary_key");
    EXPECT_TRUE(mentions(build_error("postgres_cdc_source", no_key), "primary key"));

    EXPECT_TRUE(mentions(
        build_error("postgres_cdc_source", contract({{"plugin", "test_decoding"}})), "pgoutput"));

    Params no_pub = contract();
    no_pub.erase("publication_names");
    EXPECT_TRUE(mentions(build_error("postgres_cdc_source", no_pub), "publication_names"));

    EXPECT_TRUE(mentions(
        build_error("postgres_cdc_source", contract({{"primary_key", "order_id"}})), "order_id"));
}

TEST(PostgresCdcFactoryOptions, ADeclaredTypeTheContractCannotCarryIsRefused) {
    const std::string err = build_error("postgres_cdc_source",
                                        contract({{"declared_types", "id:BIGINT;payload:BYTEA"}}));
    EXPECT_TRUE(mentions(err, "payload")) << err;
    EXPECT_TRUE(mentions(err, "BYTEA")) << err;
}

TEST(PostgresCdcFactoryOptions, AnInitialSnapshotNeedsTheTableToReadFrom) {
    const Params p = {
        {"conninfo", "host=127.0.0.1 port=1"}, {"slot_name", "s"}, {"snapshot_mode", "initial"}};
    EXPECT_TRUE(mentions(build_error("postgres_cdc_event_source", p), "'table'"));
    EXPECT_TRUE(mentions(build_error("postgres_cdc_text_source", p), "'table'"));
}

TEST(PostgresCdcFactoryOptions, ChangelogModeBelongsToTheSqlSourceOnly) {
    const Params p = {
        {"conninfo", "host=127.0.0.1 port=1"}, {"slot_name", "s"}, {"changelog_mode", "upsert"}};
    EXPECT_TRUE(mentions(build_error("postgres_cdc_event_source", p), "postgres_cdc_source"));
    EXPECT_TRUE(mentions(build_error("postgres_cdc_text_source", p), "postgres_cdc_source"));
}

TEST(PostgresCdcFactoryOptions, SubMillisecondTruncationIsPartOfTheContractOnly) {
    EXPECT_EQ(build_error("postgres_cdc_source",
                          contract({{"on_sub_millisecond", "truncate"},
                                    {"declared_types", "id:BIGINT;at:TIMESTAMP(3)"}})),
              "");
    const Params p = {{"conninfo", "host=127.0.0.1 port=1"},
                      {"slot_name", "s"},
                      {"on_sub_millisecond", "truncate"}};
    EXPECT_TRUE(mentions(build_error("postgres_cdc_source", p), "changelog_mode"));
}

TEST(PostgresCdcFactoryOptions, WithoutChangelogModeTheEarlierDefaultsHold) {
    // No contract: test_decoding by default, no snapshot, no table needed.
    const Params p = {{"conninfo", "host=127.0.0.1 port=1"}, {"slot_name", "s"}};
    EXPECT_EQ(build_error("postgres_cdc_source", p), "");
    EXPECT_EQ(build_error("postgres_cdc_text_source", p), "");
    EXPECT_EQ(build_error("postgres_cdc_event_source", p), "");
}
