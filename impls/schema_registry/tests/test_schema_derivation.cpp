// Schemas derived from the planner's serialised column list, and the option
// parsing every connector shares.
#include <map>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/schema_registry/formats.hpp"
#include "clink/schema_registry/schema_derivation.hpp"

namespace clink::schema_registry {
namespace {

constexpr const char* kColumns =
    "id:i64;qty:i32;px:f64;ratio:f32;ok:bool;name:str;amount:dec_18_2;emb:list_f32";

TEST(SchemaDerivation, AvroRecordHasNullableFieldsWithNullDefaults) {
    const auto text = derive_avro_schema(kColumns, "Orders", "clink");
    const auto s = clink::config::parse(text);
    EXPECT_EQ(s.string_or("type", ""), "record");
    EXPECT_EQ(s.string_or("name", ""), "Orders");
    EXPECT_EQ(s.string_or("namespace", ""), "clink");
    const auto& fields = s.at("fields").as_array();
    ASSERT_EQ(fields.size(), 8u);
    std::map<std::string, std::string> types;
    for (const auto& f : fields) {
        const auto& u = f.at("type").as_array();
        ASSERT_EQ(u.size(), 2u);
        EXPECT_EQ(u[0].as_string(), "null");
        EXPECT_TRUE(f.contains("default") && f.at("default").is_null());
        types[f.string_or("name", "")] = u[1].is_string() ? u[1].as_string() : u[1].serialize();
    }
    EXPECT_EQ(types["id"], "long");
    EXPECT_EQ(types["qty"], "int");
    EXPECT_EQ(types["px"], "double");
    EXPECT_EQ(types["ratio"], "float");
    EXPECT_EQ(types["ok"], "boolean");
    EXPECT_EQ(types["name"], "string");
    EXPECT_NE(types["amount"].find("\"logicalType\":\"decimal\""), std::string::npos)
        << types["amount"];
    EXPECT_NE(types["amount"].find("\"precision\":18"), std::string::npos);
    EXPECT_NE(types["amount"].find("\"scale\":2"), std::string::npos);
    EXPECT_NE(types["emb"].find("\"items\":\"float\""), std::string::npos) << types["emb"];
}

TEST(SchemaDerivation, ProtobufMessageNumbersFieldsInColumnOrder) {
    const auto text = derive_protobuf_schema(kColumns, "Orders");
    EXPECT_NE(text.find("syntax = \"proto3\";"), std::string::npos);
    EXPECT_NE(text.find("message Orders {"), std::string::npos);
    EXPECT_NE(text.find("  int64 id = 1;"), std::string::npos);
    EXPECT_NE(text.find("  int32 qty = 2;"), std::string::npos);
    EXPECT_NE(text.find("  double px = 3;"), std::string::npos);
    EXPECT_NE(text.find("  float ratio = 4;"), std::string::npos);
    EXPECT_NE(text.find("  bool ok = 5;"), std::string::npos);
    EXPECT_NE(text.find("  string name = 6;"), std::string::npos);
    EXPECT_NE(text.find("  string amount = 7;"), std::string::npos) << "decimals travel as strings";
    EXPECT_NE(text.find("  repeated float emb = 8;"), std::string::npos);
}

TEST(SchemaDerivation, JsonSchemaTypesEveryPropertyAsNullable) {
    const auto s = clink::config::parse(derive_json_schema(kColumns, "Orders"));
    EXPECT_EQ(s.string_or("title", ""), "Orders");
    EXPECT_EQ(s.string_or("type", ""), "object");
    const auto& props = s.at("properties").as_object();
    ASSERT_EQ(props.size(), 8u);
    auto type_of = [&](const char* name) {
        const auto& t = props.find(name)->second.at("type").as_array();
        EXPECT_EQ(t.back().as_string(), "null");
        return t.front().as_string();
    };
    EXPECT_EQ(type_of("id"), "integer");
    EXPECT_EQ(type_of("qty"), "integer");
    EXPECT_EQ(type_of("px"), "number");
    EXPECT_EQ(type_of("amount"), "number");
    EXPECT_EQ(type_of("ok"), "boolean");
    EXPECT_EQ(type_of("name"), "string");
    EXPECT_EQ(type_of("emb"), "array");
}

TEST(SchemaDerivation, SanitiseNameMakesValidIdentifiers) {
    EXPECT_EQ(sanitise_name("orders-value"), "orders");
    EXPECT_EQ(sanitise_name("orders-key"), "orders");
    EXPECT_EQ(sanitise_name("prod.orders.v2-value"), "prod_orders_v2");
    EXPECT_EQ(sanitise_name("2fast"), "_2fast");
    EXPECT_EQ(sanitise_name(""), "record");
    EXPECT_EQ(sanitise_name("-value"), "record");
}

TEST(FormatOptions, ParsesEveryConnectorOptionAndDefaultsTheSubject) {
    std::map<std::string, std::string> params{
        {"format", "avro"},
        {"schema_registry_url", "https://sr.example.test"},
        {"schema_registry_auth", "k:s"},
        {"schema_registry_verify_tls", "false"},
        {"schema_registry_auto_register", "false"},
        {"schema_registry_timeout_ms", "2500"},
        {"schema_columns", "a:i64"},
    };
    auto lookup = [&](const std::string& k, const std::string& fb) {
        const auto it = params.find(k);
        return it == params.end() ? fb : it->second;
    };
    const auto parsed = parse_format_options(lookup, "orders", "kafka");
    ASSERT_TRUE(parsed.options.has_value()) << parsed.error;
    const auto& o = *parsed.options;
    EXPECT_EQ(o.format, Format::Avro);
    EXPECT_EQ(o.client.url, "https://sr.example.test");
    EXPECT_EQ(o.client.basic_auth, "k:s");
    EXPECT_FALSE(o.client.verify_tls);
    EXPECT_EQ(o.client.rw_timeout_ms, 2500);
    EXPECT_EQ(o.client.connect_timeout_ms, 2500);
    EXPECT_EQ(o.subject, "orders-value");
    EXPECT_FALSE(o.auto_register);
    EXPECT_EQ(o.columns, "a:i64");
    EXPECT_EQ(o.record_namespace, "clink");
}

TEST(FormatOptions, PlainFormatsAreNotOursAndRegistryFormatsNeedAUrl) {
    auto none = [](const std::string&, const std::string& fb) { return fb; };
    EXPECT_FALSE(parse_format_options(none, "t", "kafka").options.has_value());
    EXPECT_TRUE(parse_format_options(none, "t", "kafka").error.empty());
    auto json = [](const std::string& k, const std::string& fb) {
        return k == "format" ? "json" : fb;
    };
    EXPECT_FALSE(parse_format_options(json, "t", "kafka").options.has_value());
    EXPECT_TRUE(parse_format_options(json, "t", "kafka").error.empty());
    auto no_url = [](const std::string& k, const std::string& fb) {
        return k == "format" ? "json-schema" : fb;
    };
    const auto p = parse_format_options(no_url, "t", "kafka");
    EXPECT_FALSE(p.options.has_value());
    EXPECT_NE(p.error.find("schema_registry_url"), std::string::npos) << p.error;
    EXPECT_NE(p.error.find("kafka"), std::string::npos) << p.error;
}

TEST(FormatOptions, FormatNamesParseAndReportCompiledIn) {
    EXPECT_EQ(parse_format("AVRO"), Format::Avro);
    EXPECT_EQ(parse_format("protobuf"), Format::Protobuf);
    EXPECT_EQ(parse_format("json-schema"), Format::JsonSchema);
    EXPECT_EQ(parse_format("json_schema"), Format::JsonSchema);
    EXPECT_FALSE(parse_format("json").has_value());
    EXPECT_TRUE(format_compiled_in(Format::JsonSchema));
    const auto names = supported_format_names();
    EXPECT_EQ(names.back(), "json-schema");
#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
    EXPECT_TRUE(format_compiled_in(Format::Avro));
    EXPECT_EQ(names.front(), "avro");
#endif
}

}  // namespace
}  // namespace clink::schema_registry
