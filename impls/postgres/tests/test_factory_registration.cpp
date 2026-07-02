// Verifies clink::postgres::install() registers both the legacy
// postgres_text_* / postgres_cdc_text_* string ops and the typed
// postgres_row_source / postgres_cdc_event_source on their respective
// channel types. Also smoke-tests the PostgresRow / CdcEvent codecs
// for round-trip fidelity.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/postgres/cdc_event_codec.hpp"
#include "clink/postgres/postgres_row_codec.hpp"

namespace {

using clink::cluster::RunnerRegistry;
using clink::cluster::TypeRegistry;

TEST(PostgresFactoryRegistration, PostgresTextSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("postgres_text_source", "string"), nullptr);
    EXPECT_NE(rr.find_source("postgres_cdc_text_source", "string"), nullptr);
}

TEST(PostgresFactoryRegistration, PostgresJsonSinkIsRegistered) {
    // M4: the JSON sink on the string channel (the newer Row-bridge pattern).
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("postgres_sink", "string"), nullptr);
}

TEST(PostgresFactoryRegistration, PostgresExactlyOnceSinkIsRegistered) {
    // The two-phase-commit sink selected by delivery_guarantee='exactly_once'.
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("postgres_2pc_sink", "string"), nullptr);
}

TEST(PostgresFactoryRegistration, PostgresJsonSourceIsRegistered) {
    // M3: the JSON source on the string channel (delimited postgres_text_source
    // kept for back-compat).
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("postgres_source", "string"), nullptr);
    EXPECT_NE(rr.find_source("postgres_text_source", "string"), nullptr);  // back-compat preserved
}

TEST(PostgresFactoryRegistration, PostgresCdcJsonSourceIsRegistered) {
    // M5: the flat-JSON CDC source for the Row path (nested-format
    // postgres_cdc_text_source kept for back-compat).
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("postgres_cdc_source", "string"), nullptr);
    EXPECT_NE(rr.find_source("postgres_cdc_text_source", "string"), nullptr);  // back-compat
}

TEST(PostgresFactoryRegistration, PostgresRowAndCdcEventTypedChannelsAndOpsAreRegistered) {
    const auto& tr = TypeRegistry::default_instance();
    ASSERT_NE(tr.find(clink::kChannelPostgresRow), nullptr);
    ASSERT_NE(tr.find(clink::kChannelPostgresCdcEvent), nullptr);

    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("postgres_row_source", clink::kChannelPostgresRow), nullptr);
    EXPECT_NE(rr.find_source("postgres_cdc_event_source", clink::kChannelPostgresCdcEvent),
              nullptr);
}

TEST(PostgresRowCodec, RoundTripsColumnNamesAndValues) {
    auto names =
        std::make_shared<std::vector<std::string>>(std::vector<std::string>{"id", "name", "email"});
    clink::PostgresRow in{names, std::vector<std::string>{"42", "alice", "alice@example.com"}};

    const auto codec = clink::postgres_row_codec();
    auto bytes = codec.encode(in);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    ASSERT_NE(round->column_names(), nullptr);
    EXPECT_EQ(*round->column_names(), (std::vector<std::string>{"id", "name", "email"}));
    EXPECT_EQ(round->values(), (std::vector<std::string>{"42", "alice", "alice@example.com"}));
    // .at(name) still resolves after a round-trip - column names made it
    // through the wire.
    EXPECT_EQ(round->at("email"), "alice@example.com");
}

TEST(PostgresRowCodec, RoundTripsNullMask) {
    // M5: the per-cell null mask must survive the codec (else a typed-channel
    // round-trip would silently lose NULLs - the construction-path-symmetry trap).
    auto names =
        std::make_shared<std::vector<std::string>>(std::vector<std::string>{"id", "val", "note"});
    clink::PostgresRow in{names,
                          std::vector<std::string>{"1", "", ""},
                          std::vector<char>{0, 1, 0}};  // val IS NULL; note is empty-string

    const auto codec = clink::postgres_row_codec();
    auto round = codec.decode(codec.encode(in));
    ASSERT_TRUE(round.has_value());
    EXPECT_FALSE(round->is_null(0));
    EXPECT_TRUE(round->is_null(1)) << "NULL must survive the codec";
    EXPECT_FALSE(round->is_null(2)) << "empty-string is NOT null";
    EXPECT_EQ(round->values(), (std::vector<std::string>{"1", "", ""}));
}

TEST(PostgresRowCodec, LegacyTwoArgRowHasNoNulls) {
    // The 2-arg ctor (no mask) round-trips as all-non-null.
    clink::PostgresRow in{nullptr, std::vector<std::string>{"a", "b"}};
    const auto codec = clink::postgres_row_codec();
    auto round = codec.decode(codec.encode(in));
    ASSERT_TRUE(round.has_value());
    EXPECT_FALSE(round->is_null(0));
    EXPECT_FALSE(round->is_null(1));
}

TEST(PostgresRowCodec, RoundTripsRowWithoutColumnNames) {
    // A default-constructed PostgresRow has names_ == nullptr; some
    // sources (e.g. simple SELECT without column metadata) may produce
    // these. The codec must preserve that state.
    clink::PostgresRow in{nullptr, std::vector<std::string>{"a", "b"}};

    const auto codec = clink::postgres_row_codec();
    auto bytes = codec.encode(in);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->column_names(), nullptr);
    EXPECT_EQ(round->values(), (std::vector<std::string>{"a", "b"}));
}

TEST(CdcEventCodec, RoundTripsRowChangeWithFullMetadata) {
    clink::CdcEvent in;
    in.op = clink::CdcEvent::Op::Update;
    in.table = "public.users";
    in.lsn = "0/16E2A38";
    in.xid = 1234567;
    in.values.push_back({.name = "id", .value = "1", .type = "int4", .is_null = false});
    in.values.push_back(
        {.name = "email", .value = "alice@example.com", .type = "text", .is_null = false});
    in.values.push_back(
        {.name = "deleted_at", .value = "", .type = "timestamptz", .is_null = true});

    const auto codec = clink::cdc_event_codec();
    auto bytes = codec.encode(in);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->op, clink::CdcEvent::Op::Update);
    EXPECT_EQ(round->table, "public.users");
    EXPECT_EQ(round->lsn, "0/16E2A38");
    EXPECT_EQ(round->xid, 1234567);
    ASSERT_EQ(round->values.size(), 3u);
    EXPECT_EQ(round->values[0].name, "id");
    EXPECT_EQ(round->values[0].type, "int4");
    EXPECT_FALSE(round->values[0].is_null);
    EXPECT_EQ(round->values[2].name, "deleted_at");
    EXPECT_TRUE(round->values[2].is_null);
    EXPECT_EQ(round->values[2].value, "");
}

TEST(CdcEventCodec, UnknownOpByteDecodesToOpUnknown) {
    // Forward-compat: if a future producer adds a new Op value, older
    // decoders shouldn't crash - they get CdcEvent::Op::Unknown.
    clink::CdcEvent in;
    in.op = clink::CdcEvent::Op::Insert;
    in.table = "t";
    const auto codec = clink::cdc_event_codec();
    auto bytes = codec.encode(in);
    // Stomp the first byte (the op tag) with a value past Unknown.
    bytes[0] = static_cast<std::byte>(99);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->op, clink::CdcEvent::Op::Unknown);
    EXPECT_EQ(round->table, "t");
}

}  // namespace
