// The DynamoDB sink against an emulator. Every case is skipped unless
// CLINK_DYNAMODB_TEST_ENDPOINT is set (scripts/dynamodb-live.sh starts LocalStack,
// whose DynamoDB is DynamoDB Local, and sets it with the AWS variables the SDK
// signs with). Each case creates its own table, writes through the production
// sink and its SDK service, and reads the table back with a separate SDK client,
// comparing complete items: every attribute, its type and its value, numbers by
// the model's own normalisation rather than the sink's.
//
// This is emulator evidence, not a qualification against AWS: the emulator
// applies DynamoDB's documented validation, but not its throttling, its
// partitions or its permissions.

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/CreateTableRequest.h>
#include <aws/dynamodb/model/DeleteTableRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/KeySchemaElement.h>
#include <aws/dynamodb/model/ScanRequest.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/dynamodb_sink.hpp"
#include "clink/cluster/operator_registry.hpp"

#include "dynamodb_model.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::DynamoDbSink;
using clink::aws::DynamoDbSinkOptions;
using AV = Aws::DynamoDB::Model::AttributeValue;

namespace {

const char* endpoint() {
    return std::getenv("CLINK_DYNAMODB_TEST_ENDPOINT");
}

clink::aws::AwsClientOptions client_options() {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = endpoint();
    return o;
}

Aws::DynamoDB::DynamoDBClient& admin() {
    static auto client = [] {
        clink::aws::ensure_aws_initialized();
        auto o = client_options();
        o.request_timeout_ms = 60000;  // the emulator starts its DynamoDB on first use
        return std::make_unique<Aws::DynamoDB::DynamoDBClient>(clink::aws::make_client_config(o));
    }();
    return *client;
}

std::string unique_table(const std::string& stem) {
    static int n = 0;
    return "clink-" + stem + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n++);
}

// Creates a table with the given key and waits for it to be ACTIVE.
// sort_type empty: no sort key.
void create_table(const std::string& name,
                  const std::string& pk,
                  const char* pk_type,
                  const std::string& sk = {},
                  const char* sk_type = "") {
    using namespace Aws::DynamoDB::Model;
    const auto type_of = [](const char* t) {
        return std::string(t) == "N"   ? ScalarAttributeType::N
               : std::string(t) == "B" ? ScalarAttributeType::B
                                       : ScalarAttributeType::S;
    };
    CreateTableRequest req;
    req.SetTableName(name);
    req.SetBillingMode(BillingMode::PAY_PER_REQUEST);
    req.AddAttributeDefinitions(
        AttributeDefinition().WithAttributeName(pk).WithAttributeType(type_of(pk_type)));
    req.AddKeySchema(KeySchemaElement().WithAttributeName(pk).WithKeyType(KeyType::HASH));
    if (!sk.empty()) {
        req.AddAttributeDefinitions(
            AttributeDefinition().WithAttributeName(sk).WithAttributeType(type_of(sk_type)));
        req.AddKeySchema(KeySchemaElement().WithAttributeName(sk).WithKeyType(KeyType::RANGE));
    }
    const auto created = admin().CreateTable(req);
    ASSERT_TRUE(created.IsSuccess()) << created.GetError().GetMessage();
    for (int i = 0; i < 100; ++i) {
        DescribeTableRequest d;
        d.SetTableName(name);
        const auto r = admin().DescribeTable(d);
        if (r.IsSuccess() && r.GetResult().GetTable().GetTableStatus() == TableStatus::ACTIVE) {
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
    FAIL() << "table " << name << " never became ACTIVE";
}

void delete_table(const std::string& name) {
    Aws::DynamoDB::Model::DeleteTableRequest req;
    req.SetTableName(name);
    (void)admin().DeleteTable(req);
}

// A value as the test compares it: type and text, a number normalised by the
// model's own rule, a map's members and a list's elements in order.
std::string shape(const AV& v) {
    using Aws::DynamoDB::Model::ValueType;
    switch (v.GetType()) {
        case ValueType::STRING: {
            std::string out = "S:";
            for (const char ch : v.GetS()) {
                const auto c = static_cast<unsigned char>(ch);
                if (c < 0x20) {
                    out += "\\x" + std::to_string(c);
                } else {
                    out.push_back(ch);
                }
            }
            return out;
        }
        case ValueType::NUMBER: {
            const auto n = clink::aws::test::DynamoDbModel::normal_number(v.GetN());
            return "N:" + (n ? *n : "<invalid " + std::string(v.GetN()) + ">");
        }
        case ValueType::BOOL:
            return v.GetBool() ? "BOOL:true" : "BOOL:false";
        case ValueType::NULLVALUE:
            return "NULL";
        case ValueType::ATTRIBUTE_MAP: {
            std::map<std::string, std::string> members;
            for (const auto& [k, m] : v.GetM()) {
                members[k] = shape(*m);
            }
            std::string out = "M{";
            for (const auto& [k, s] : members) {
                out += k + "=" + s + ";";
            }
            return out + "}";
        }
        case ValueType::ATTRIBUTE_LIST: {
            std::string out = "L[";
            for (const auto& m : v.GetL()) {
                out += shape(*m) + ";";
            }
            return out + "]";
        }
        default:
            return "<other>";
    }
}

using Item = std::map<std::string, std::string>;  // attribute -> shape

// Every item in the table, by a paginated Scan.
std::vector<Item> scan(const std::string& table) {
    std::vector<Item> out;
    Aws::Map<Aws::String, AV> start;
    do {
        Aws::DynamoDB::Model::ScanRequest req;
        req.SetTableName(table);
        req.SetConsistentRead(true);
        if (!start.empty()) {
            req.SetExclusiveStartKey(start);
        }
        const auto r = admin().Scan(req);
        EXPECT_TRUE(r.IsSuccess()) << r.GetError().GetMessage();
        if (!r.IsSuccess()) {
            break;
        }
        for (const auto& row : r.GetResult().GetItems()) {
            Item item;
            for (const auto& [k, v] : row) {
                item[k] = shape(v);
            }
            out.push_back(std::move(item));
        }
        start = r.GetResult().GetLastEvaluatedKey();
    } while (!start.empty());
    return out;
}

std::string N(const std::string& numeral) {
    return "N:" + *clink::aws::test::DynamoDbModel::normal_number(numeral);
}

// The table's items as a sorted list of their printed forms, for comparison.
std::vector<std::string> printed(const std::vector<Item>& items) {
    std::vector<std::string> out;
    for (const auto& item : items) {
        std::string s;
        for (const auto& [k, v] : item) {
            s += k + "=" + v + " ";
        }
        out.push_back(s);
    }
    std::sort(out.begin(), out.end());
    return out;
}

Batch<std::string> batch_of(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

DynamoDbSinkOptions options(const std::string& table, std::string pk, std::string sk) {
    DynamoDbSinkOptions o;
    o.table = table;
    o.partition_key = std::move(pk);
    o.sort_key = std::move(sk);
    o.client = client_options();
    return o;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// A loopback listener that accepts connections and never answers, so a request
// sent to it stays in flight until the client gives up or is aborted.
class SilentServer {
public:
    SilentServer() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(fd_, 8);
        socklen_t len = sizeof(addr);
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] {
            for (;;) {
                const int c = ::accept(fd_, nullptr, nullptr);
                if (c < 0) {
                    return;
                }
                accepted_.fetch_add(1);
                held_.push_back(c);
            }
        });
    }
    ~SilentServer() {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        thread_.join();
        for (const int c : held_) {
            ::close(c);
        }
    }
    SilentServer(const SilentServer&) = delete;
    SilentServer& operator=(const SilentServer&) = delete;
    [[nodiscard]] int port() const { return port_; }
    [[nodiscard]] int accepted() const { return accepted_.load(); }

private:
    int fd_{-1};
    int port_{0};
    std::atomic<int> accepted_{0};
    std::vector<int> held_;
    std::thread thread_;
};

class DynamoDbLive : public ::testing::Test {
protected:
    void SetUp() override {
        if (endpoint() == nullptr) {
            GTEST_SKIP() << "set CLINK_DYNAMODB_TEST_ENDPOINT (scripts/dynamodb-live.sh does)";
        }
    }
    void TearDown() override {
        for (const auto& t : tables_) {
            delete_table(t);
        }
    }
    std::string table(const std::string& stem) {
        tables_.push_back(unique_table(stem));
        return tables_.back();
    }
    std::vector<std::string> tables_;
};

}  // namespace

// Through the registered factory, as a job builds it: every identity case
// lands as the item the service keeps, and every attribute as written.
TEST_F(DynamoDbLive, EveryRecordLandsAsTheItemTheServiceKeeps) {
    const auto t = table("identity");
    create_table(t, "pk", "S", "sk", "N");
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "dynamodb_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"table", t},
                  {"partition_key", "pk"},
                  {"sort_key", "sk"},
                  {"region", "us-east-1"},
                  {"endpoint_override", endpoint()},
                  {"connect_timeout_ms", "2000"},
                  {"request_timeout_ms", "5000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch_of({
        // A separator inside a key: two items, where a joined key made one.
        R"({"pk":"a\u0001b","sk":1,"v":"first"})",
        R"({"pk":"a","sk":1,"v":"second"})",
        // Precomposed and decomposed e-acute: two items.
        "{\"pk\":\"\xC3\xA9\",\"sk\":1,\"v\":\"precomposed\"}",
        "{\"pk\":\"e\xCC\x81\",\"sk\":1,\"v\":\"decomposed\"}",
        // Equivalent numerals in one batch: one item, the last record's. Sent
        // apart, the service refuses the batch as naming one item twice.
        R"({"pk":"num","sk":7,"v":"a"})",
        R"({"pk":"num","sk":7.0,"v":"b"})",
        R"({"pk":"num","sk":70E-1,"v":"c"})",
        R"({"pk":"zero","sk":-0,"v":"neg"})",
        R"({"pk":"zero","sk":0.0,"v":"pos"})",
        // Neighbouring integers past int64, and 38 significant digits.
        R"({"pk":"big","sk":9223372036854775808,"v":"808"})",
        R"({"pk":"big","sk":9223372036854775809,"v":"809"})",
        R"({"pk":"big","sk":12345678901234567890123456789012345678,"v":"38"})",
        // Every type, nested.
        R"({"pk":"doc","sk":1,"s":"x","e":"","n":-12.50,"t":true,"z":null,)"
        R"("m":{"b":[1,"two",{"c":false}],"a":{}},"l":[]})",
    }));
    sink->on_barrier(barrier(1));
    // A later record for a written item replaces it.
    sink->on_data(batch_of({R"({"pk":"a","sk":1.0,"v":"third"})"}));
    sink->on_barrier(barrier(2));
    sink->close();
    // U+0000 is refused before anything is sent: the SDK would store "a".
    auto nul = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    nul->open();
    const auto why =
        thrown([&] { nul->on_data(batch_of({R"({"pk":"a\u0000b","sk":1,"v":"nul"})"})); });
    EXPECT_NE(why.find("U+0000"), std::string::npos) << why;

    const std::vector<Item> expected = {
        {{"pk", std::string("S:a\\x1b")}, {"sk", N("1")}, {"v", "S:first"}},
        {{"pk", "S:a"}, {"sk", N("1")}, {"v", "S:third"}},
        {{"pk", "S:\xC3\xA9"}, {"sk", N("1")}, {"v", "S:precomposed"}},
        {{"pk", "S:e\xCC\x81"}, {"sk", N("1")}, {"v", "S:decomposed"}},
        {{"pk", "S:num"}, {"sk", N("7")}, {"v", "S:c"}},
        {{"pk", "S:zero"}, {"sk", N("0")}, {"v", "S:pos"}},
        {{"pk", "S:big"}, {"sk", N("9223372036854775808")}, {"v", "S:808"}},
        {{"pk", "S:big"}, {"sk", N("9223372036854775809")}, {"v", "S:809"}},
        {{"pk", "S:big"}, {"sk", N("12345678901234567890123456789012345678")}, {"v", "S:38"}},
        {{"pk", "S:doc"},
         {"sk", N("1")},
         {"s", "S:x"},
         {"e", "S:"},
         {"n", N("-12.5")},
         {"t", "BOOL:true"},
         {"z", "NULL"},
         {"m", "M{a=M{};b=L[" + N("1") + ";S:two;M{c=BOOL:false;};];}"},
         {"l", "L[]"}},
    };
    EXPECT_EQ(printed(scan(t)), printed(expected));
}

TEST_F(DynamoDbLive, OpenChecksTheTablesKeySchema) {
    const auto t = table("schema");
    create_table(t, "pk", "S", "sk", "N");
    const auto open_with = [&](std::string pk, std::string sk) {
        DynamoDbSink s(options(t, std::move(pk), std::move(sk)));
        return thrown([&] { s.open(); });
    };
    EXPECT_EQ(open_with("pk", "sk"), "");
    EXPECT_NE(open_with("id", "sk").find("has partition key 'pk' (S)"), std::string::npos);
    EXPECT_NE(open_with("pk", "").find("set sort_key='sk'"), std::string::npos);
    EXPECT_NE(open_with("pk", "ts").find("the sink was given sort_key='ts'"), std::string::npos);
    const auto b = table("binary");
    create_table(b, "id", "B");
    DynamoDbSink binary(options(b, "id", ""));
    EXPECT_NE(thrown([&] { binary.open(); }).find("is binary (B)"), std::string::npos);
    DynamoDbSink missing(options(unique_table("missing"), "pk", "sk"));
    const auto why = thrown([&] { missing.open(); });
    EXPECT_NE(why.find("ResourceNotFoundException"), std::string::npos) << why;
    EXPECT_NE(why.find("Check that the table exists"), std::string::npos) << why;
}

// The sink's limits agree with the service's: what the sink sends at its
// limits the service stores, and what it refuses never reaches the service.
TEST_F(DynamoDbLive, ItemsAtTheLimitsAreStoredAndPastThemNeverSent) {
    const auto t = table("limits");
    create_table(t, "pk", "S", "sk", "N");
    DynamoDbSink s(options(t, "pk", "sk"));
    s.open();
    std::string two_byte;
    for (int i = 0; i < 1024; ++i) {
        two_byte += "\xC3\xA9";
    }
    s.on_data(batch_of({
        // 409,600 bytes by the sink's rules: pk "s" (3) + sk 1 (4) + "v" (1 + L).
        R"({"pk":"s","sk":1,"v":")" + std::string(409592, 'x') + R"("})",
        // The same with numbers the documented approximation undercounts (1.5
        // takes 3 bytes, not 2): the sink's count is the service's.
        R"({"pk":"s","sk":2,"n":1.5,"v":")" + std::string(409588, 'x') + R"("})",
        // A 2,048-byte partition key of two-byte characters.
        "{\"pk\":\"" + two_byte + "\",\"sk\":1}",
        // A map nested 31 levels below its attribute.
        [] {
            std::string v = "\"leaf\"";
            for (int i = 0; i < 31; ++i) {
                v = "{\"a\":" + v + "}";
            }
            return R"({"pk":"deep","sk":1,"v":)" + v + "}";
        }(),
    }));
    s.on_barrier(barrier(1));
    EXPECT_EQ(scan(t).size(), 4u);
    // One byte more is refused before anything is sent.
    DynamoDbSink over(options(t, "pk", "sk"));
    over.open();
    const auto why = thrown([&] {
        over.on_data(batch_of({R"({"pk":"s","sk":3,"v":")" + std::string(409593, 'x') + R"("})"}));
    });
    EXPECT_NE(why.find("409601 bytes"), std::string::npos) << why;
    EXPECT_EQ(scan(t).size(), 4u);
}

// Items whose indented JSON is about 2.4 MB each: a batch of them goes out in
// several requests, each within 16 MiB, and every item lands.
TEST_F(DynamoDbLive, ABatchPastSixteenMiBGoesOutInSeveralRequests) {
    const auto t = table("wide");
    create_table(t, "pk", "S", "sk", "N");
    DynamoDbSink s(options(t, "pk", "sk"));
    s.open();
    std::string controls;
    for (int i = 0; i < 400000; ++i) {
        controls += "\\u0001";
    }
    Batch<std::string> b;
    for (int i = 0; i < 12; ++i) {
        b.emplace(R"({"pk":"wide","sk":)" + std::to_string(i) + R"(,"v":")" + controls + R"("})");
    }
    s.on_data(b);
    s.on_barrier(barrier(1));
    const auto items = scan(t);
    ASSERT_EQ(items.size(), 12u);
    for (const auto& item : items) {
        EXPECT_EQ(item.at("v").size(), 2 + 400000u * 3);  // "S:", then "\x1" per byte
    }
}

// A table deleted under the sink: the write is refused for good, the barrier
// is not acknowledged, and a restart once the table is back converges.
TEST_F(DynamoDbLive, APermanentRefusalFailsTheBarrierAndARestartConverges) {
    const auto t = table("gone");
    create_table(t, "pk", "S", "sk", "N");
    DynamoDbSink first(options(t, "pk", "sk"));
    first.open();
    first.on_data(batch_of({R"({"pk":"k","sk":1,"v":1})"}));
    first.on_barrier(barrier(1));
    first.on_data(batch_of({R"({"pk":"k","sk":1,"v":2})", R"({"pk":"k","sk":2,"v":2})"}));
    delete_table(t);
    const auto why = thrown([&] { first.on_barrier(barrier(2)); });
    EXPECT_NE(why.find("BatchWriteItem was refused: ResourceNotFoundException"), std::string::npos)
        << why;
    EXPECT_TRUE(first.failed());
    EXPECT_NE(thrown([&] { first.on_barrier(barrier(3)); }).find("failed earlier"),
              std::string::npos);
    // The table returns (empty) and the job restarts from checkpoint 1: the
    // records after it are replayed.
    create_table(t, "pk", "S", "sk", "N");
    DynamoDbSink restarted(options(t, "pk", "sk"));
    restarted.open();
    restarted.on_data(batch_of({R"({"pk":"k","sk":1,"v":2})", R"({"pk":"k","sk":2,"v":2})"}));
    restarted.on_barrier(barrier(4));
    const std::vector<Item> expected = {
        {{"pk", "S:k"}, {"sk", N("1")}, {"v", N("2")}},
        {{"pk", "S:k"}, {"sk", N("2")}, {"v", N("2")}},
    };
    EXPECT_EQ(printed(scan(t)), printed(expected));
}

// The production SDK service, with a request in flight to a server that never
// answers: abort() ends it at once, well inside the request timeout, and the
// call reports a cancellation. The sink's watch calls abort() when its task is
// cancelled (DynamoDbSinkCancel covers that path against the model).
TEST_F(DynamoDbLive, AbortEndsAnSdkCallInFlight) {
    SilentServer server;
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = "http://127.0.0.1:" + std::to_string(server.port());
    o.connect_timeout_ms = 2000;
    o.request_timeout_ms = 30000;
    auto service = clink::aws::make_dynamodb_sdk_service(o);
    clink::aws::DynamoDbValue pk;
    pk.kind = clink::aws::DynamoDbValue::Kind::S;
    pk.text = "k";
    const clink::aws::DynamoDbItem item{{"pk", pk}};
    auto call = std::async(std::launch::async, [&] { return service->batch_write("t", {&item}); });
    for (int i = 0; i < 200 && server.accepted() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_GT(server.accepted(), 0) << "the request never reached the server";
    EXPECT_EQ(call.wait_for(300ms), std::future_status::timeout) << "the call ended by itself";
    const auto t0 = std::chrono::steady_clock::now();
    service->abort();
    ASSERT_EQ(call.wait_for(5s), std::future_status::ready) << "abort() did not end the call";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
    const auto r = call.get();
    EXPECT_EQ(r.error.fault, clink::aws::DynamoDbFault::Cancelled)
        << r.error.code << ": " << r.error.message;
}
