// The Protobuf format: .proto text parsed at runtime, message selection by
// index path, references resolved through the registry, and the reflection
// walk in both directions (checked against libprotobuf's own dynamic
// messages).
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/schema_registry/formats.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "fake_registry.hpp"

#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace clink::schema_registry {
namespace {

namespace pb = google::protobuf;

constexpr const char* kOrders = R"(syntax = "proto3";
import "google/protobuf/timestamp.proto";
package shop;

enum Status { NEW = 0; PAID = 1; }

message Order {
  int64 id = 1;
  string customer = 2;
  double total = 3;
  bool rush = 4;
  Status status = 5;
  repeated string tags = 6;
  map<string, int32> counts = 7;
  bytes token = 8;
  google.protobuf.Timestamp placed_at = 9;
  optional int32 note_count = 10;
  Line first = 11;
  message Line {
    string sku = 1;
    int32 qty = 2;
  }
}

message Ping {
  int32 seq = 1;
}
)";

// A reference-implementation harness: the same .proto compiled by the test
// with libprotobuf, so payloads are produced and checked independently of the
// code under test.
struct Compiled {
    pb::DescriptorPool pool{pb::DescriptorPool::generated_pool()};
    pb::DynamicMessageFactory factory{&pool};
    const pb::FileDescriptor* file{nullptr};

    explicit Compiled(const std::string& text) {
        pb::io::ArrayInputStream in(text.data(), static_cast<int>(text.size()));
        pb::io::Tokenizer tok(&in, nullptr);
        pb::compiler::Parser parser;
        pb::FileDescriptorProto fdp;
        if (!parser.Parse(&tok, &fdp)) {
            throw std::runtime_error("test proto does not parse");
        }
        fdp.set_name("orders.proto");
        file = pool.BuildFile(fdp);
        if (file == nullptr) {
            throw std::runtime_error("test proto does not build");
        }
    }
    std::unique_ptr<pb::Message> make(const std::string& name) {
        const auto* d = file->FindMessageTypeByName(name);
        if (d == nullptr) {
            d = pool.FindMessageTypeByName("shop." + name);
        }
        return std::unique_ptr<pb::Message>(factory.GetPrototype(d)->New());
    }
};

struct Fixture {
    test::FakeRegistry reg;
    std::shared_ptr<Client> client{std::make_shared<Client>(ClientOptions{.url = reg.url()})};
    std::unique_ptr<ValueDecoder> decoder() {
        FormatOptions o;
        o.format = Format::Protobuf;
        return make_decoder(o, client);
    }
};

TEST(ProtobufFormat, DecodesAMessageWithEveryFieldShape) {
    Fixture fx;
    const auto id = fx.reg.add("orders-value", "PROTOBUF", kOrders);
    Compiled ref(kOrders);
    auto msg = ref.make("Order");
    const auto* r = msg->GetReflection();
    const auto* d = msg->GetDescriptor();
    r->SetInt64(msg.get(), d->FindFieldByName("id"), 42);
    r->SetString(msg.get(), d->FindFieldByName("customer"), "dana");
    r->SetDouble(msg.get(), d->FindFieldByName("total"), 12.5);
    r->SetBool(msg.get(), d->FindFieldByName("rush"), true);
    r->SetEnum(msg.get(),
               d->FindFieldByName("status"),
               d->FindFieldByName("status")->enum_type()->FindValueByName("PAID"));
    r->AddString(msg.get(), d->FindFieldByName("tags"), "a");
    r->AddString(msg.get(), d->FindFieldByName("tags"), "b");
    {
        auto* entry = r->AddMessage(msg.get(), d->FindFieldByName("counts"));
        const auto* ed = entry->GetDescriptor();
        entry->GetReflection()->SetString(entry, ed->map_key(), "x");
        entry->GetReflection()->SetInt32(entry, ed->map_value(), 3);
    }
    r->SetString(msg.get(), d->FindFieldByName("token"), std::string("\x00\xff\x10", 3));
    {
        auto* ts = r->MutableMessage(msg.get(), d->FindFieldByName("placed_at"));
        ts->GetReflection()->SetInt64(
            ts, ts->GetDescriptor()->FindFieldByName("seconds"), 1'704'067'200);
        ts->GetReflection()->SetInt32(
            ts, ts->GetDescriptor()->FindFieldByName("nanos"), 123'000'000);
    }
    {
        auto* line = r->MutableMessage(msg.get(), d->FindFieldByName("first"));
        line->GetReflection()->SetString(
            line, line->GetDescriptor()->FindFieldByName("sku"), "SKU-1");
        line->GetReflection()->SetInt32(line, line->GetDescriptor()->FindFieldByName("qty"), 2);
    }
    std::string bytes;
    ASSERT_TRUE(msg->SerializeToString(&bytes));

    auto dec = fx.decoder();
    const auto json = dec->to_json(frame(id, std::vector<std::int32_t>{0}, bytes));
    const auto v = clink::config::parse(json);
    ASSERT_TRUE(v.is_object()) << json;
    EXPECT_EQ(v.at("id").as_int(), 42);
    EXPECT_EQ(v.at("customer").as_string(), "dana");
    EXPECT_DOUBLE_EQ(v.at("total").as_number(), 12.5);
    EXPECT_TRUE(v.at("rush").as_bool());
    EXPECT_EQ(v.at("status").as_string(), "PAID");
    EXPECT_EQ(v.at("tags").serialize(), R"(["a","b"])");
    EXPECT_EQ(v.at("counts").serialize(), R"({"x":3})");
    EXPECT_EQ(v.at("token").as_string(), "AP8Q");
    EXPECT_EQ(v.at("placed_at").as_int(), 1'704'067'200'123) << "Timestamp -> epoch millis";
    EXPECT_FALSE(v.contains("note_count")) << "an unset optional field is omitted";
    EXPECT_EQ(v.at("first").serialize(),
              R"({"qty":2,"sku":"SKU-1"})");  // objects serialise key-sorted
    EXPECT_EQ(fx.reg.requests().size(), 1u);
}

TEST(ProtobufFormat, MessageIndexesSelectNestedAndSiblingMessages) {
    Fixture fx;
    const auto id = fx.reg.add("orders-value", "PROTOBUF", kOrders);
    Compiled ref(kOrders);
    auto dec = fx.decoder();
    {
        auto ping = ref.make("Ping");
        ping->GetReflection()->SetInt32(
            ping.get(), ping->GetDescriptor()->FindFieldByName("seq"), 5);
        std::string bytes;
        ping->SerializeToString(&bytes);
        EXPECT_EQ(dec->to_json(frame(id, std::vector<std::int32_t>{1}, bytes)), R"({"seq":5})");
    }
    {
        // Nested types are counted as the descriptor lists them, so the
        // synthesised CountsEntry (map<string,int32> counts) is Order's nested
        // type 0 and Line is nested type 1: the path is {0, 1}.
        auto line = ref.make("Order.Line");
        line->GetReflection()->SetString(
            line.get(), line->GetDescriptor()->FindFieldByName("sku"), "S");
        std::string bytes;
        line->SerializeToString(&bytes);
        EXPECT_EQ(dec->to_json(frame(id, std::vector<std::int32_t>{0, 1}, bytes)),
                  R"({"qty":0,"sku":"S"})");
    }
    EXPECT_THROW(dec->to_json(frame(id, std::vector<std::int32_t>{7}, "")), std::runtime_error);
}

TEST(ProtobufFormat, ResolvesReferencesThroughTheRegistry) {
    Fixture fx;
    const auto common_id = fx.reg.add("common.proto",
                                      "PROTOBUF",
                                      "syntax = \"proto3\"; package common; message Money { int64 "
                                      "units = 1; string currency = 2; }");
    (void)common_id;
    clink::config::JsonArray refs;
    {
        clink::config::JsonObject r;
        r.emplace("name", clink::config::JsonValue{"common.proto"});
        r.emplace("subject", clink::config::JsonValue{"common.proto"});
        r.emplace("version", clink::config::JsonValue{static_cast<std::int64_t>(1)});
        refs.emplace_back(std::move(r));
    }
    const char* invoice = R"(syntax = "proto3";
import "common.proto";
message Invoice { int64 id = 1; common.Money amount = 2; })";
    const auto id =
        fx.reg.add("invoice-value", "PROTOBUF", invoice, clink::config::JsonValue{refs});

    // Build the reference payload with both files in one pool.
    pb::DescriptorPool pool(pb::DescriptorPool::generated_pool());
    auto build = [&](const char* text, const char* name) {
        const std::string s(text);
        pb::io::ArrayInputStream in(s.data(), static_cast<int>(s.size()));
        pb::io::Tokenizer tok(&in, nullptr);
        pb::compiler::Parser parser;
        pb::FileDescriptorProto fdp;
        ASSERT_TRUE(parser.Parse(&tok, &fdp));
        fdp.set_name(name);
        ASSERT_NE(pool.BuildFile(fdp), nullptr);
    };
    build(
        "syntax = \"proto3\"; package common; message Money { int64 units = 1; string currency = "
        "2; }",
        "common.proto");
    build(invoice, "invoice.proto");
    pb::DynamicMessageFactory factory(&pool);
    std::unique_ptr<pb::Message> msg(
        factory.GetPrototype(pool.FindMessageTypeByName("Invoice"))->New());
    msg->GetReflection()->SetInt64(msg.get(), msg->GetDescriptor()->FindFieldByName("id"), 1);
    auto* money = msg->GetReflection()->MutableMessage(
        msg.get(), msg->GetDescriptor()->FindFieldByName("amount"));
    money->GetReflection()->SetInt64(money, money->GetDescriptor()->FindFieldByName("units"), 250);
    money->GetReflection()->SetString(
        money, money->GetDescriptor()->FindFieldByName("currency"), "NOK");
    std::string bytes;
    msg->SerializeToString(&bytes);

    auto dec = fx.decoder();
    EXPECT_EQ(dec->to_json(frame(id, std::vector<std::int32_t>{0}, bytes)),
              R"({"amount":{"currency":"NOK","units":250},"id":1})");
    const auto reqs = fx.reg.requests();
    ASSERT_EQ(reqs.size(), 2u);
    EXPECT_EQ(reqs[1], "GET /subjects/common.proto/versions/1");
}

TEST(ProtobufFormat, EncoderDerivesRegistersAndTheReferenceParserReadsItsOutput) {
    Fixture fx;
    FormatOptions o;
    o.format = Format::Protobuf;
    o.subject = "orders-value";
    o.columns = "id:i64;qty:i32;px:f64;ok:bool;name:str;amount:dec_18_2;emb:list_f32";
    auto enc = make_encoder(o, fx.client);
    const auto registered = fx.client->schema_by_id(enc->schema_id());
    EXPECT_EQ(registered.type, SchemaType::Protobuf);
    EXPECT_NE(registered.schema.find("message orders {"), std::string::npos) << registered.schema;

    const auto framed = enc->from_json(
        R"({"id":7,"qty":3,"px":2.5,"ok":true,"name":"x","amount":12345678901234567.89,"emb":[1.0,2.5],"__row_kind":"insert"})");
    const auto f = parse_frame(framed, true);
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->message_indexes, (std::vector<std::int32_t>{0}));
    Compiled ref(registered.schema);
    auto msg = ref.make("orders");
    ASSERT_TRUE(msg->ParseFromArray(f->payload.data(), static_cast<int>(f->payload.size())));
    const auto* r = msg->GetReflection();
    const auto* d = msg->GetDescriptor();
    EXPECT_EQ(r->GetInt64(*msg, d->FindFieldByName("id")), 7);
    EXPECT_EQ(r->GetInt32(*msg, d->FindFieldByName("qty")), 3);
    EXPECT_DOUBLE_EQ(r->GetDouble(*msg, d->FindFieldByName("px")), 2.5);
    EXPECT_TRUE(r->GetBool(*msg, d->FindFieldByName("ok")));
    EXPECT_EQ(r->GetString(*msg, d->FindFieldByName("name")), "x");
    EXPECT_EQ(r->GetString(*msg, d->FindFieldByName("amount")), "12345678901234567.89")
        << "digits kept as a string";
    EXPECT_EQ(r->FieldSize(*msg, d->FindFieldByName("emb")), 2);

    // Round trip through our decoder; a missing column is the proto3 default.
    auto dec = fx.decoder();
    const auto back = clink::config::parse(dec->to_json(enc->from_json(R"({"id":8})")));
    EXPECT_EQ(back.at("id").as_int(), 8);
    EXPECT_EQ(back.at("name").as_string(), "");
    EXPECT_EQ(back.at("qty").as_int(), 0);
}

TEST(ProtobufFormat, EncoderAgainstARegistryHeldSchemaHandlesEveryShapeAndNamedMessages) {
    Fixture fx;
    const auto id = fx.reg.add("orders-value", "PROTOBUF", kOrders);
    FormatOptions o;
    o.format = Format::Protobuf;
    o.subject = "orders-value";
    o.auto_register = false;
    auto enc = make_encoder(o, fx.client);
    EXPECT_EQ(enc->schema_id(), id);
    const auto framed =
        enc->from_json(R"({"id":1,"customer":"c","total":1.25,"rush":false,"status":"PAID",
        "tags":["t1"],"counts":{"k":2},"token":"AP8Q","placed_at":"2024-01-01T00:00:00.123Z","note_count":4,
        "first":{"sku":"S","qty":1}})");
    Compiled ref(kOrders);
    auto msg = ref.make("Order");
    const auto f = parse_frame(framed, true);
    ASSERT_TRUE(msg->ParseFromArray(f->payload.data(), static_cast<int>(f->payload.size())));
    const auto* r = msg->GetReflection();
    const auto* d = msg->GetDescriptor();
    EXPECT_EQ(r->GetEnum(*msg, d->FindFieldByName("status"))->name(), "PAID");
    EXPECT_EQ(r->GetString(*msg, d->FindFieldByName("token")), std::string("\x00\xff\x10", 3));
    const auto& ts = r->GetMessage(*msg, d->FindFieldByName("placed_at"));
    EXPECT_EQ(ts.GetReflection()->GetInt64(ts, ts.GetDescriptor()->FindFieldByName("seconds")),
              1'704'067'200);
    EXPECT_EQ(ts.GetReflection()->GetInt32(ts, ts.GetDescriptor()->FindFieldByName("nanos")),
              123'000'000);
    EXPECT_TRUE(r->HasField(*msg, d->FindFieldByName("note_count")));
    EXPECT_EQ(r->GetInt32(*msg, d->FindFieldByName("note_count")), 4);
    EXPECT_EQ(r->FieldSize(*msg, d->FindFieldByName("counts")), 1);

    // Writing a named (sibling / nested) message carries its index path.
    FormatOptions ping = o;
    ping.message_name = "Ping";
    auto penc = make_encoder(ping, fx.client);
    EXPECT_EQ(parse_frame(penc->from_json(R"({"seq":9})"), true)->message_indexes,
              (std::vector<std::int32_t>{1}));
    FormatOptions line = o;
    line.message_name = "Order.Line";
    auto lenc = make_encoder(line, fx.client);
    EXPECT_EQ(parse_frame(lenc->from_json(R"({"sku":"s"})"), true)->message_indexes,
              (std::vector<std::int32_t>{0, 1}));
    FormatOptions missing = o;
    missing.message_name = "Nope";
    EXPECT_THROW(make_encoder(missing, fx.client), std::runtime_error);

    // Type errors name the field.
    try {
        enc->from_json(R"({"id":"not a number"})");
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("'id'"), std::string::npos) << e.what();
    }
}

}  // namespace
}  // namespace clink::schema_registry

#else
TEST(ProtobufFormat, NotCompiledIn) {
    GTEST_SKIP() << "libprotobuf + libprotoc not available in this build";
}
#endif
