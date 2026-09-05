// The Avro format. Reference payloads are produced by avro-cpp itself
// (GenericDatum + binaryEncoder), so the decoder is checked against the
// reference implementation; the encoder is checked by decoding its output
// with avro-cpp and by round-tripping through the decoder.
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/schema_registry/formats.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "fake_registry.hpp"

#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>

namespace clink::schema_registry {
namespace {

using clink::config::JsonValue;

std::string encode_datum(const avro::GenericDatum& d) {
    auto out = avro::memoryOutputStream();
    auto enc = avro::binaryEncoder();
    enc->init(*out);
    avro::encode(*enc, d);
    enc->flush();
    const auto snap = avro::snapshot(*out);
    return std::string(snap->begin(), snap->end());
}

avro::GenericDatum decode_bytes(const avro::ValidSchema& schema, std::string_view bytes) {
    avro::GenericDatum d(schema);
    auto in =
        avro::memoryInputStream(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
    auto dec = avro::binaryDecoder();
    dec->init(*in);
    avro::decode(*dec, d);
    return d;
}

constexpr const char* kWideSchema = R"({
  "type":"record","name":"Wide","namespace":"t","fields":[
    {"name":"id","type":"long"},
    {"name":"n","type":"int"},
    {"name":"ok","type":"boolean"},
    {"name":"px","type":"double"},
    {"name":"ratio","type":"float"},
    {"name":"name","type":["null","string"],"default":null},
    {"name":"blob","type":"bytes"},
    {"name":"colour","type":{"type":"enum","name":"Colour","symbols":["RED","GREEN"]}},
    {"name":"tags","type":{"type":"array","items":"string"}},
    {"name":"attrs","type":{"type":"map","values":"long"}},
    {"name":"addr","type":{"type":"record","name":"Addr","fields":[{"name":"city","type":"string"}]}},
    {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":18,"scale":2}},
    {"name":"fixed_amount","type":{"type":"fixed","name":"Dec8","size":8,"logicalType":"decimal","precision":16,"scale":3}},
    {"name":"day","type":{"type":"int","logicalType":"date"}},
    {"name":"t_ms","type":{"type":"int","logicalType":"time-millis"}},
    {"name":"ts_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"ts_us","type":{"type":"long","logicalType":"timestamp-micros"}},
    {"name":"uid","type":{"type":"string","logicalType":"uuid"}}
  ]})";

struct Fixture {
    test::FakeRegistry reg;
    std::shared_ptr<Client> client{std::make_shared<Client>(ClientOptions{.url = reg.url()})};
    std::unique_ptr<ValueDecoder> decoder() {
        FormatOptions o;
        o.format = Format::Avro;
        return make_decoder(o, client);
    }
};

TEST(AvroFormat, DecodesEveryTypeAndLogicalTypeToTheDocumentedJson) {
    Fixture fx;
    const auto id = fx.reg.add("wide-value", "AVRO", kWideSchema);
    const auto schema = avro::compileJsonSchemaFromString(kWideSchema);
    avro::GenericDatum d(schema);
    auto& r = d.value<avro::GenericRecord>();
    r.field("id").value<std::int64_t>() = 42;
    r.field("n").value<std::int32_t>() = -7;
    r.field("ok").value<bool>() = true;
    r.field("px").value<double>() = 1.5;
    r.field("ratio").value<float>() = 0.25f;
    r.field("name").selectBranch(1);
    r.field("name").value<std::string>() = "dana";
    r.field("blob").value<std::vector<std::uint8_t>>() = {0x00, 0xff, 0x10};
    r.field("colour").value<avro::GenericEnum>().set("GREEN");
    r.field("tags").value<avro::GenericArray>().value() = {avro::GenericDatum(std::string("a")),
                                                           avro::GenericDatum(std::string("b"))};
    r.field("attrs").value<avro::GenericMap>().value() = {
        {"k", avro::GenericDatum(std::int64_t{9})}};
    r.field("addr").value<avro::GenericRecord>().field("city").value<std::string>() = "Oslo";
    // 1234.56 at scale 2 = unscaled 123456 = 0x01E240
    r.field("amount").value<std::vector<std::uint8_t>>() = {0x01, 0xE2, 0x40};
    // -1.5 at scale 3 = unscaled -1500 = 0xFFFFFFFFFFFFFA24 as 8 bytes
    r.field("fixed_amount").value<avro::GenericFixed>().value() = {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfa, 0x24};
    r.field("day").value<std::int32_t>() = 19723;                // 2024-01-01
    r.field("t_ms").value<std::int32_t>() = 3'723'004;           // 01:02:03.004
    r.field("ts_ms").value<std::int64_t>() = 1'704'067'200'123;  // 2024-01-01T00:00:00.123Z
    r.field("ts_us").value<std::int64_t>() = 1'704'067'200'123'456;
    r.field("uid").value<std::string>() = "123e4567-e89b-12d3-a456-426614174000";

    auto dec = fx.decoder();
    const auto json = dec->to_json(frame(id, encode_datum(d)));
    const auto v = clink::config::parse(json);
    ASSERT_TRUE(v.is_object()) << json;
    EXPECT_EQ(v.at("id").as_int(), 42);
    EXPECT_EQ(v.at("n").as_int(), -7);
    EXPECT_TRUE(v.at("ok").as_bool());
    EXPECT_DOUBLE_EQ(v.at("px").as_number(), 1.5);
    EXPECT_DOUBLE_EQ(v.at("ratio").as_number(), 0.25);
    EXPECT_EQ(v.at("name").as_string(), "dana");
    EXPECT_EQ(v.at("blob").as_string(), "AP8Q");
    EXPECT_EQ(v.at("colour").as_string(), "GREEN");
    EXPECT_EQ(v.at("tags").serialize(), R"(["a","b"])");
    EXPECT_EQ(v.at("attrs").serialize(), R"({"k":9})");
    EXPECT_EQ(v.at("addr").serialize(), R"({"city":"Oslo"})");
    EXPECT_EQ(v.at("amount").as_string(), "1234.56");
    EXPECT_EQ(v.at("fixed_amount").as_string(), "-1.500");
    EXPECT_EQ(v.at("day").as_string(), "2024-01-01");
    EXPECT_EQ(v.at("t_ms").as_string(), "01:02:03.004");
    EXPECT_EQ(v.at("ts_ms").as_int(), 1'704'067'200'123);
    EXPECT_EQ(v.at("ts_us").as_int(), 1'704'067'200'123) << "micros scale down to millis";
    EXPECT_EQ(v.at("uid").as_string(), "123e4567-e89b-12d3-a456-426614174000");
    EXPECT_EQ(fx.reg.requests().size(), 1u) << "the schema is fetched once per id";
}

TEST(AvroFormat, NullBranchAndNonRecordTopLevel) {
    Fixture fx;
    const auto id = fx.reg.add(
        "n-value",
        "AVRO",
        R"({"type":"record","name":"N","fields":[{"name":"name","type":["null","string"]}]})");
    const auto schema = avro::compileJsonSchemaFromString(
        R"({"type":"record","name":"N","fields":[{"name":"name","type":["null","string"]}]})");
    avro::GenericDatum d(schema);
    d.value<avro::GenericRecord>().field("name").selectBranch(0);
    auto dec = fx.decoder();
    EXPECT_EQ(dec->to_json(frame(id, encode_datum(d))), R"({"name":null})");

    const auto sid = fx.reg.add("s-value", "AVRO", R"("string")");
    avro::GenericDatum s(std::string("just a string"));
    EXPECT_EQ(dec->to_json(frame(sid, encode_datum(s))), R"({"value":"just a string"})");
}

TEST(AvroFormat, DecoderRefusesUnknownIdWrongTypeAndBadPayload) {
    Fixture fx;
    auto dec = fx.decoder();
    EXPECT_THROW(dec->to_json(frame(99, "x")), RegistryError);
    const auto pid = fx.reg.add("p-value", "PROTOBUF", "syntax = \"proto3\"; message P {}");
    try {
        dec->to_json(frame(pid, "x"));
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("PROTOBUF"), std::string::npos) << e.what();
        EXPECT_NE(std::string(e.what()).find("format='protobuf'"), std::string::npos) << e.what();
    }
    const auto id = fx.reg.add("w-value", "AVRO", kWideSchema);
    EXPECT_THROW(dec->to_json(frame(id, "\x01")), std::runtime_error) << "truncated payload";
}

TEST(AvroFormat, EncoderDerivesRegistersAndEncodesRowsTheReferenceDecoderReads) {
    Fixture fx;
    FormatOptions o;
    o.format = Format::Avro;
    o.subject = "orders-value";
    o.columns = "id:i64;qty:i32;px:f64;ok:bool;name:str;amount:dec_18_2;emb:list_f32";
    auto enc = make_encoder(o, fx.client);
    const auto registered = fx.client->schema_by_id(enc->schema_id());
    EXPECT_EQ(registered.type, SchemaType::Avro);
    const auto schema = avro::compileJsonSchemaFromString(registered.schema);
    EXPECT_EQ(schema.root()->name().fullname(), "clink.orders");

    const auto framed = enc->from_json(
        R"({"id":7,"qty":3,"px":2.5,"ok":false,"name":"x","amount":12345678901234567.89,"emb":[1.0,2.5],"__row_kind":"insert"})");
    const auto f = parse_frame(framed, false);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->schema_id, enc->schema_id());
    const auto d = decode_bytes(schema, f->payload);
    const auto& r = d.value<avro::GenericRecord>();
    EXPECT_EQ(r.field("id").value<std::int64_t>(), 7);
    EXPECT_EQ(r.field("qty").value<std::int32_t>(), 3);
    EXPECT_DOUBLE_EQ(r.field("px").value<double>(), 2.5);
    EXPECT_FALSE(r.field("ok").value<bool>());
    EXPECT_EQ(r.field("name").value<std::string>(), "x");
    // 12345678901234567.89 at scale 2 -> unscaled 1234567890123456789 (exact, past double
    // precision)
    const auto& amount = r.field("amount").value<std::vector<std::uint8_t>>();
    const std::vector<std::uint8_t> expected{0x11, 0x22, 0x10, 0xF4, 0x7D, 0xE9, 0x81, 0x15};
    EXPECT_EQ(amount, expected);
    EXPECT_EQ(r.field("emb").value<avro::GenericArray>().value().size(), 2u);

    // A missing column encodes as null (every derived field is nullable); the
    // decoder reads our own output back.
    auto dec = fx.decoder();
    const auto back = dec->to_json(enc->from_json(R"({"id":8})"));
    const auto v = clink::config::parse(back);
    EXPECT_EQ(v.at("id").as_int(), 8);
    EXPECT_TRUE(v.at("name").is_null());
    EXPECT_TRUE(v.at("amount").is_null());
    EXPECT_EQ(fx.reg.schema_count(), 1u);
}

TEST(AvroFormat, EncoderAgainstARegistryHeldSchemaHonoursLogicalTypesAndRefusesBadRows) {
    Fixture fx;
    const auto id = fx.reg.add("wide-value", "AVRO", kWideSchema);
    FormatOptions o;
    o.format = Format::Avro;
    o.subject = "wide-value";
    o.auto_register = false;
    auto enc = make_encoder(o, fx.client);
    EXPECT_EQ(enc->schema_id(), id);
    const auto framed = enc->from_json(R"({
        "id":1,"n":2,"ok":true,"px":0.5,"ratio":0.5,"name":null,"blob":"AP8Q","colour":"RED",
        "tags":["t"],"attrs":{"a":1},"addr":{"city":"Bergen"},"amount":"1234.56","fixed_amount":-1.5,
        "day":"2024-01-01","t_ms":"01:02:03.004","ts_ms":"2024-01-01T00:00:00.123Z","ts_us":1704067200123,
        "uid":"u"})");
    const auto schema = avro::compileJsonSchemaFromString(kWideSchema);
    const auto d = decode_bytes(schema, parse_frame(framed, false)->payload);
    const auto& r = d.value<avro::GenericRecord>();
    EXPECT_EQ(r.field("name").unionBranch(), 0u);
    EXPECT_EQ(r.field("blob").value<std::vector<std::uint8_t>>(),
              (std::vector<std::uint8_t>{0x00, 0xff, 0x10}));
    EXPECT_EQ(r.field("colour").value<avro::GenericEnum>().symbol(), "RED");
    EXPECT_EQ(r.field("amount").value<std::vector<std::uint8_t>>(),
              (std::vector<std::uint8_t>{0x01, 0xE2, 0x40}));
    EXPECT_EQ(r.field("fixed_amount").value<avro::GenericFixed>().value(),
              (std::vector<std::uint8_t>{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfa, 0x24}));
    EXPECT_EQ(r.field("day").value<std::int32_t>(), 19723);
    EXPECT_EQ(r.field("t_ms").value<std::int32_t>(), 3'723'004);
    EXPECT_EQ(r.field("ts_ms").value<std::int64_t>(), 1'704'067'200'123);
    EXPECT_EQ(r.field("ts_us").value<std::int64_t>(), 1'704'067'200'123'000)
        << "millis scale up to the schema's micros";

    // A required (non-nullable) field missing is a refusal that names the field.
    try {
        enc->from_json(R"({"n":2})");
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("'id'"), std::string::npos) << e.what();
    }
    // A decimal with more fractional digits than the scale is refused, not rounded.
    EXPECT_THROW(
        enc->from_json(R"({"id":1,"n":2,"ok":true,"px":0.5,"ratio":0.5,"blob":"","colour":"RED",
        "tags":[],"attrs":{},"addr":{"city":""},"amount":"1.234","fixed_amount":0,"day":0,"t_ms":0,"ts_ms":0,"ts_us":0,"uid":""})"),
        std::runtime_error);
    // An unknown enum symbol is refused.
    EXPECT_THROW(
        enc->from_json(R"({"id":1,"n":2,"ok":true,"px":0.5,"ratio":0.5,"blob":"","colour":"BLUE",
        "tags":[],"attrs":{},"addr":{"city":""},"amount":"1.23","fixed_amount":0,"day":0,"t_ms":0,"ts_ms":0,"ts_us":0,"uid":""})"),
        std::runtime_error);
}

}  // namespace
}  // namespace clink::schema_registry

#else
TEST(AvroFormat, NotCompiledIn) {
    GTEST_SKIP() << "avro-cpp not available in this build";
}
#endif
