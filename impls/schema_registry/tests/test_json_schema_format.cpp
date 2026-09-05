// The JSON Schema format: header strip and prefix, type check against the
// registry, and the encode side's column filtering with exact decimals.
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/schema_registry/formats.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "fake_registry.hpp"

namespace clink::schema_registry {
namespace {

TEST(JsonSchemaFormat, DecoderStripsTheHeaderAfterCheckingTheType) {
    test::FakeRegistry reg;
    const auto id = reg.add("t-value", "JSON", R"({"type":"object"})");
    auto client = std::make_shared<Client>(ClientOptions{.url = reg.url()});
    FormatOptions o;
    o.format = Format::JsonSchema;
    auto dec = make_decoder(o, client);
    EXPECT_EQ(dec->to_json(frame(id, R"({"a":1})")), R"({"a":1})");
    EXPECT_EQ(dec->to_json(frame(id, R"({"a":2})")), R"({"a":2})");
    EXPECT_EQ(reg.requests().size(), 1u) << "one type check per id";
    EXPECT_THROW(dec->to_json("not framed"), std::runtime_error);
}

TEST(JsonSchemaFormat, DecoderRefusesAnAvroIdByName) {
    test::FakeRegistry reg;
    const auto id = reg.add("t-value", "AVRO", R"({"type":"record","name":"R","fields":[]})");
    auto client = std::make_shared<Client>(ClientOptions{.url = reg.url()});
    FormatOptions o;
    o.format = Format::JsonSchema;
    auto dec = make_decoder(o, client);
    try {
        dec->to_json(frame(id, "{}"));
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("AVRO"), std::string::npos) << e.what();
        EXPECT_NE(std::string(e.what()).find("format='avro'"), std::string::npos) << e.what();
    }
}

TEST(JsonSchemaFormat, EncoderRegistersADerivedSchemaAndKeepsDeclaredColumnsOnly) {
    test::FakeRegistry reg;
    auto client = std::make_shared<Client>(ClientOptions{.url = reg.url()});
    FormatOptions o;
    o.format = Format::JsonSchema;
    o.subject = "orders-value";
    o.columns = "id:i64;amount:dec_18_2;name:str";
    auto enc = make_encoder(o, client);
    EXPECT_GT(enc->schema_id(), 0);
    const auto registered = client->schema_by_id(enc->schema_id());
    EXPECT_EQ(registered.type, SchemaType::Json);
    EXPECT_NE(registered.schema.find("\"title\":\"orders\""), std::string::npos)
        << registered.schema;

    // __row_kind is dropped, the decimal keeps its exact digits, column order is the schema's.
    const auto out = enc->from_json(
        R"({"name":"x","__row_kind":"insert","amount":12345678901234567.89,"id":7})");
    const auto f = parse_frame(out, false);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->schema_id, enc->schema_id());
    EXPECT_EQ(f->payload, R"({"id":7,"amount":12345678901234567.89,"name":"x"})");
    // Registering again is idempotent on the registry side.
    auto enc2 = make_encoder(o, client);
    EXPECT_EQ(enc2->schema_id(), enc->schema_id());
    EXPECT_EQ(reg.schema_count(), 1u);
}

TEST(JsonSchemaFormat, EncoderWithoutAutoRegisterUsesTheSubjectAsIs) {
    test::FakeRegistry reg;
    const auto id = reg.add("orders-value", "JSON", R"({"type":"object"})");
    auto client = std::make_shared<Client>(ClientOptions{.url = reg.url()});
    FormatOptions o;
    o.format = Format::JsonSchema;
    o.subject = "orders-value";
    o.auto_register = false;
    auto enc = make_encoder(o, client);
    EXPECT_EQ(enc->schema_id(), id);
    const auto out = enc->from_json(R"({"anything":true})");
    EXPECT_EQ(parse_frame(out, false)->payload, R"({"anything":true})");

    FormatOptions missing = o;
    missing.subject = "absent-value";
    EXPECT_THROW(make_encoder(missing, client), RegistryError);
}

}  // namespace
}  // namespace clink::schema_registry
