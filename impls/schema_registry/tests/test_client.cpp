// The registry client against the in-process double: response shapes,
// caching, credentials, path prefixes and the error mapping.
#include <gtest/gtest.h>

#include "clink/schema_registry/client.hpp"

#include "fake_registry.hpp"

namespace clink::schema_registry {
namespace {

constexpr const char* kAvro =
    R"({"type":"record","name":"R","fields":[{"name":"a","type":"long"}]})";

TEST(RegistryClient, FetchesByIdOnceAndCaches) {
    test::FakeRegistry reg;
    const auto id = reg.add("t-value", "AVRO", kAvro);
    Client c(ClientOptions{.url = reg.url()});
    const auto s1 = c.schema_by_id(id);
    EXPECT_EQ(s1.id, id);
    EXPECT_EQ(s1.type, SchemaType::Avro);
    EXPECT_EQ(s1.schema, kAvro);
    const auto s2 = c.schema_by_id(id);
    EXPECT_EQ(s2.schema, kAvro);
    EXPECT_EQ(reg.requests().size(), 1u) << "the second lookup must be served from the cache";
    EXPECT_EQ(reg.requests()[0], "GET /schemas/ids/" + std::to_string(id));
}

TEST(RegistryClient, LatestCarriesSubjectVersionAndType) {
    test::FakeRegistry reg;
    reg.add("orders-value", "PROTOBUF", "syntax = \"proto3\"; message O { int64 id = 1; }");
    reg.add("orders-value",
            "PROTOBUF",
            "syntax = \"proto3\"; message O { int64 id = 1; string s = 2; }");
    Client c(ClientOptions{.url = reg.url()});
    const auto s = c.latest("orders-value");
    EXPECT_EQ(s.subject, "orders-value");
    EXPECT_EQ(s.version, 2);
    EXPECT_EQ(s.type, SchemaType::Protobuf);
    EXPECT_NE(s.schema.find("string s"), std::string::npos);
    const auto v1 = c.version("orders-value", 1);
    EXPECT_EQ(v1.version, 1);
    EXPECT_EQ(v1.schema.find("string s"), std::string::npos);
}

TEST(RegistryClient, RegisterIsIdempotentAndTypesTheSchema) {
    test::FakeRegistry reg;
    Client c(ClientOptions{.url = reg.url()});
    const auto id1 = c.register_schema("s-value", SchemaType::Json, R"({"type":"object"})");
    const auto id2 = c.register_schema("s-value", SchemaType::Json, R"({"type":"object"})");
    EXPECT_EQ(id1, id2);
    EXPECT_EQ(reg.schema_count(), 1u);
    // Registered ids are cached like fetched ones.
    const auto back = c.schema_by_id(id1);
    EXPECT_EQ(back.type, SchemaType::Json);
    EXPECT_EQ(reg.requests().size(), 2u);
    // And a lookup finds it, while an unregistered schema is nullopt.
    const auto found = c.lookup("s-value", SchemaType::Json, R"({"type":"object"})");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->id, id1);
    EXPECT_FALSE(c.lookup("s-value", SchemaType::Json, R"({"type":"string"})").has_value());
}

TEST(RegistryClient, SendsBasicAuthFromOptionOrUrlUserinfo) {
    test::FakeRegistry reg;
    const auto id = reg.add("t-value", "AVRO", kAvro);
    {
        Client c(ClientOptions{.url = reg.url(), .basic_auth = "key:secret"});
        c.schema_by_id(id);
        EXPECT_EQ(reg.last_authorization(), "Basic a2V5OnNlY3JldA==");
    }
    {
        // http://key2:secret2@127.0.0.1:port
        const auto url = "http://key2:secret2@" + reg.url().substr(std::string("http://").size());
        Client c(ClientOptions{.url = url});
        c.schema_by_id(id);
        EXPECT_EQ(reg.last_authorization(), "Basic a2V5MjpzZWNyZXQy");
    }
    {
        Client c(ClientOptions{.url = reg.url(), .bearer_token = "tok"});
        c.schema_by_id(id);
        EXPECT_EQ(reg.last_authorization(), "Bearer tok");
    }
}

TEST(RegistryClient, ErrorsCarryTheRegistryCodeAndAreSpecific) {
    test::FakeRegistry reg;
    Client c(ClientOptions{.url = reg.url()});
    try {
        c.schema_by_id(42);
        FAIL() << "expected a RegistryError";
    } catch (const RegistryError& e) {
        EXPECT_EQ(e.http_status(), 404);
        EXPECT_EQ(e.registry_code(), 40403);
        EXPECT_NE(std::string(e.what()).find("schema id 42"), std::string::npos) << e.what();
    }
    try {
        c.latest("nope-value");
        FAIL() << "expected a RegistryError";
    } catch (const RegistryError& e) {
        EXPECT_EQ(e.registry_code(), 40401);
        EXPECT_NE(std::string(e.what()).find("nope-value"), std::string::npos);
    }
    reg.fail_with(500);
    try {
        c.register_schema("x-value", SchemaType::Avro, kAvro);
        FAIL() << "expected a RegistryError";
    } catch (const RegistryError& e) {
        EXPECT_EQ(e.http_status(), 500);
    }
}

TEST(RegistryClient, UnreachableRegistryIsATransportError) {
    Client c(ClientOptions{
        .url = "http://127.0.0.1:1", .connect_timeout_ms = 500, .rw_timeout_ms = 500});
    try {
        c.schema_by_id(1);
        FAIL() << "expected a RegistryError";
    } catch (const RegistryError& e) {
        EXPECT_EQ(e.http_status(), 0);
        EXPECT_NE(std::string(e.what()).find("127.0.0.1:1"), std::string::npos);
    }
}

TEST(RegistryClient, RefusesAUrlWithoutAScheme) {
    EXPECT_THROW(Client(ClientOptions{.url = "localhost:8081"}), RegistryError);
}

}  // namespace
}  // namespace clink::schema_registry
