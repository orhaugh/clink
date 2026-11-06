// The DynamoDB sink's write path, deterministically: the production DynamoDbSink
// against a model of DescribeTable and BatchWriteItem (dynamodb_model.hpp). The
// model keeps items by its own key equality and refuses what the service
// refuses for a request's shape, so the cases check what reaches the table, not
// what the sink believes it sent. Nothing here talks to a service; the
// emulator's cases are in test_dynamodb_live.cpp.

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/aws/dynamodb_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "dynamodb_model.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::DynamoDbCallError;
using clink::aws::DynamoDbFault;
using clink::aws::DynamoDbItem;
using clink::aws::DynamoDbKeyAttribute;
using clink::aws::DynamoDbKeySchema;
using clink::aws::DynamoDbKeyType;
using clink::aws::DynamoDbSink;
using clink::aws::DynamoDbSinkOptions;
using clink::aws::DynamoDbUnprocessed;
using clink::aws::DynamoDbValue;
using clink::aws::test::DynamoDbModel;

namespace {

constexpr char kTable[] = "orders";
const DynamoDbKeySchema kNumberSort{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::S},
                                    DynamoDbKeyAttribute{"sk", DynamoDbKeyType::N}};
const DynamoDbKeySchema kStringPair{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::S},
                                    DynamoDbKeyAttribute{"sk", DynamoDbKeyType::S}};
const DynamoDbKeySchema kNumberKey{DynamoDbKeyAttribute{"id", DynamoDbKeyType::N}, std::nullopt};

DynamoDbValue S(std::string s) {
    DynamoDbValue v;
    v.kind = DynamoDbValue::Kind::S;
    v.text = std::move(s);
    return v;
}
DynamoDbValue N(std::string s) {
    DynamoDbValue v;
    v.kind = DynamoDbValue::Kind::N;
    v.text = std::move(s);
    return v;
}

CheckpointBarrier barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

std::shared_ptr<DynamoDbModel> model_of(const DynamoDbKeySchema& key = kNumberSort) {
    return std::make_shared<DynamoDbModel>(kTable, key);
}

DynamoDbSinkOptions options_for(const std::shared_ptr<DynamoDbModel>& m,
                                const DynamoDbKeySchema& key = kNumberSort) {
    DynamoDbSinkOptions o;
    o.table = kTable;
    o.partition_key = key.partition.name;
    if (key.sort) {
        o.sort_key = key.sort->name;
    }
    o.service = m;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 8ms;
    o.retry_timeout = 30s;
    return o;
}

// {"pk":<pk>,"sk":<sk>,"v":<v>}, with pk a JSON string body and sk a numeral.
std::string rec(const std::string& pk, const std::string& sk, const std::string& v) {
    return R"({"pk":")" + pk + R"(","sk":)" + sk + R"(,"v":)" + v + "}";
}

Batch<std::string> batch_of(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
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

// The value of attribute `name` in an item the model holds, or "<none>".
std::string attr(const std::optional<DynamoDbItem>& item, const std::string& name) {
    if (!item) {
        return "<no item>";
    }
    for (const auto& [n, v] : *item) {
        if (n == name) {
            return v.text;
        }
    }
    return "<none>";
}

// A sink attached to a runtime context whose cancel signal the case controls.
struct Attached {
    explicit Attached(DynamoDbSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4545}, "dynamodb_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    DynamoDbSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

// ---- options and the table ----------------------------------------------------------

TEST(DynamoDbSinkConfig, RefusesOptionsOutsideTheirRanges) {
    auto m = model_of();
    const auto refused = [&](auto edit) {
        auto o = options_for(m);
        edit(o);
        return thrown([&] { DynamoDbSink s(std::move(o)); });
    };
    EXPECT_NE(refused([](auto& o) { o.table.clear(); }).find("'table' is required"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.table = "a b"; }).find("table name"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.partition_key.clear(); }).find("'partition_key' is required"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.sort_key = "pk"; }).find("same attribute"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 0; }).find("1 to 25"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 26; }).find("1 to 25"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_retries = -1; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_retries = 21; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.retry_timeout = 0ms; }).find("retry_timeout_ms"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_age = -1ms; }).find("linger_ms"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.client.region = ""; }).find("'region' is empty"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.client.request_timeout_ms = 0; }).find("request_timeout_ms"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) {
                  o.client.endpoint_override = "ftp://host";
              }).find("endpoint_override"),
              std::string::npos);
    // An endpoint with user information is refused without repeating it.
    const auto why = refused(
        [](auto& o) { o.client.endpoint_override = "http://AKIAEXAMPLE:hunter2@host:4566"; });
    EXPECT_NE(why.find("without user information"), std::string::npos) << why;
    EXPECT_EQ(why.find("hunter2"), std::string::npos) << why;
    EXPECT_EQ(why.find("AKIAEXAMPLE"), std::string::npos) << why;
    // Valid options touch no service.
    EXPECT_EQ(refused([](auto& o) { o.client.endpoint_override = "http://127.0.0.1:4566"; }), "");
    EXPECT_EQ(
        refused([](auto& o) { o.table = "arn:aws:dynamodb:eu-west-2:123456789012:table/orders"; }),
        "");
    EXPECT_EQ(m->describe_calls(), 0u);
}

TEST(DynamoDbSinkConfig, TheFactoryRangeChecksItsNumbers) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "dynamodb_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"table", "orders"}, {"partition_key", "pk"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"batch_records", "26", "'batch_records' must be 1 to 25"},
             {"batch_records", "0", "'batch_records' must be 1 to 25"},
             {"max_retries", "21", "'max_retries' must be 0 to 20"},
             {"connect_timeout_ms", "4294967396", "'connect_timeout_ms' must be 1 to 600000"},
             {"request_timeout_ms", "-5", "'request_timeout_ms' must be 1 to 600000"},
             {"retry_timeout_ms", "0", "'retry_timeout_ms' must be 1 to 3600000"},
             {"linger_ms", "-1", "'linger_ms' must be 0 to 3600000"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(thrown([&] { (void)f->build(c); }).find(rule), std::string::npos)
            << key << "=" << value;
    }
}

TEST(DynamoDbSinkConfig, OpenChecksTheTablesKeySchema) {
    const auto open_with = [](const DynamoDbKeySchema& table_key, auto edit) {
        auto m = model_of(table_key);
        auto o = options_for(m, kNumberSort);
        edit(o);
        DynamoDbSink s(std::move(o));
        return thrown([&] { s.open(); });
    };
    const auto none = [](auto&) {};
    EXPECT_EQ(open_with(kNumberSort, none), "");
    EXPECT_NE(open_with(kNumberSort, [](auto& o) { o.partition_key = "id"; })
                  .find("has partition key 'pk' (S); the sink was given partition_key='id'"),
              std::string::npos);
    EXPECT_NE(open_with(kNumberSort, [](auto& o) { o.sort_key.clear(); })
                  .find("has sort key 'sk' (N); set sort_key='sk'"),
              std::string::npos);
    EXPECT_NE(open_with(kNumberSort, [](auto& o) { o.sort_key = "ts"; })
                  .find("the sink was given sort_key='ts'"),
              std::string::npos);
    const DynamoDbKeySchema no_sort{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::S}, std::nullopt};
    EXPECT_NE(open_with(no_sort, none).find("has no sort key"), std::string::npos);
    const DynamoDbKeySchema binary{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::B},
                                   DynamoDbKeyAttribute{"sk", DynamoDbKeyType::N}};
    EXPECT_NE(open_with(binary, none).find("is binary (B)"), std::string::npos);
    // A missing table, and one not in a state the sink writes to.
    EXPECT_NE(open_with(kNumberSort, [](auto& o) { o.table = "missing"; })
                  .find("Check that the table exists"),
              std::string::npos);
    auto m = model_of();
    m->set_status("DELETING");
    DynamoDbSink deleting(options_for(m));
    EXPECT_NE(thrown([&] { deleting.open(); }).find("is DELETING"), std::string::npos);
}

TEST(DynamoDbSinkConfig, OpenRetriesWhatTheServiceSaysMayPass) {
    auto m = model_of();
    m->fail_describe({{DynamoDbFault::Throttled, "ThrottlingException", "slow down", 400},
                      {DynamoDbFault::Transient, "InternalServerError", "oops", 500}});
    DynamoDbSink s(options_for(m));
    EXPECT_NO_THROW(s.open());
    EXPECT_EQ(m->describe_calls(), 3u);
    // A table still CREATING is waited for, within the retry budget.
    auto creating = model_of();
    creating->set_status("CREATING");
    auto o = options_for(creating);
    o.retry_timeout = 40ms;
    o.max_retries = 20;
    DynamoDbSink waiting(std::move(o));
    EXPECT_NE(thrown([&] { waiting.open(); }).find("the table is CREATING"), std::string::npos);
    EXPECT_GE(creating->describe_calls(), 2u);
}

TEST(DynamoDbSinkConfig, ErrorsNameTheFixAndNoCredentials) {
    auto m = model_of();
    m->fail_describe(
        {{DynamoDbFault::Permanent,
          "AccessDeniedException",
          "User: arn:aws:iam::123456789012:user/u is not authorized; key AKIAABCDEFGHIJKLMNOP; "
          "Credential=AKIAABCDEFGHIJKLMNOP/20261106/eu-west-2/dynamodb/aws4_request, "
          "Signature=0123456789abcdef X-Amz-Security-Token=FQoGZXIvYXdzEJr",
          400}});
    DynamoDbSink s(options_for(m));
    const auto why = thrown([&] { s.open(); });
    EXPECT_NE(why.find("AccessDeniedException (HTTP 400)"), std::string::npos) << why;
    EXPECT_NE(why.find("dynamodb:DescribeTable and dynamodb:BatchWriteItem"), std::string::npos)
        << why;
    EXPECT_EQ(why.find("AKIAABCDEFGHIJKLMNOP"), std::string::npos) << why;
    EXPECT_EQ(why.find("0123456789abcdef"), std::string::npos) << why;
    EXPECT_EQ(why.find("FQoGZXIvYXdzEJr"), std::string::npos) << why;
}

// ---- identity through the sink --------------------------------------------------------

// The pairs a joined text key could not tell apart are two items. With U+0000
// as the separator the records are refused: the AWS SDK for C++ would cut the
// strings at it and store ("a", "c") and ("a", "b").
TEST(DynamoDbSinkIdentity, KeysWithAnEmbeddedSeparatorAreTwoItems) {
    auto m = model_of(kStringPair);
    DynamoDbSink s(options_for(m, kStringPair));
    s.open();
    s.on_data(
        batch_of({R"({"pk":"a\u0001b","sk":"c","v":1})", R"({"pk":"a","sk":"b\u0001c","v":2})"}));
    s.on_barrier(barrier(1));
    ASSERT_EQ(m->calls().size(), 1u);
    EXPECT_EQ(m->calls()[0].size(), 2u);
    EXPECT_EQ(attr(m->item(S(std::string("a\x01") + "b"), S("c")), "v"), "1");
    EXPECT_EQ(attr(m->item(S("a"), S(std::string("b\x01") + "c")), "v"), "2");
    EXPECT_EQ(m->items().size(), 2u);

    DynamoDbSink nul(options_for(m, kStringPair));
    nul.open();
    const auto why = thrown([&] {
        nul.on_data(batch_of(
            {R"({"pk":"a\u0000b","sk":"c","v":3})", R"({"pk":"a","sk":"b\u0000c","v":4})"}));
    });
    EXPECT_NE(why.find("record 1 of a batch of 2 is refused: a string holds U+0000"),
              std::string::npos)
        << why;
    EXPECT_EQ(m->calls().size(), 1u);
    EXPECT_EQ(m->items().size(), 2u);
}

TEST(DynamoDbSinkIdentity, EquivalentNumbersInOneBatchAreOneItem) {
    auto m = model_of();
    DynamoDbSink s(options_for(m));
    s.open();
    // Sent apart, the model would refuse the batch as naming one item twice.
    s.on_data(batch_of({rec("p", "1", "1"),
                        rec("p", "1.0", "2"),
                        rec("p", "10E-1", "3"),
                        rec("p", "-0", "4"),
                        rec("p", "0.0", "5")}));
    s.on_barrier(barrier(1));
    ASSERT_EQ(m->calls().size(), 1u);
    EXPECT_EQ(m->calls()[0].size(), 2u);
    EXPECT_EQ(attr(m->item(S("p"), N("1")), "v"), "3");
    EXPECT_EQ(attr(m->item(S("p"), N("0")), "v"), "5");
}

TEST(DynamoDbSinkIdentity, RepeatedKeysEndAtTheLastRecordWithinAndAcrossBatches) {
    auto m = model_of();
    DynamoDbSink s(options_for(m));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "1"), rec("k1", "1", "2")}));
    s.on_barrier(barrier(1));
    s.on_data(batch_of({rec("k1", "1", "3")}));
    s.on_data(batch_of({rec("k2", "1", "4"), rec("k1", "1.00", "5")}));
    s.on_barrier(barrier(2));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[0].size(), 2u);
    EXPECT_EQ(calls[1].size(), 2u);
    EXPECT_EQ(attr(m->item(S("k1"), N("1")), "v"), "5");
    EXPECT_EQ(attr(m->item(S("k2"), N("1")), "v"), "4");
}

TEST(DynamoDbSinkIdentity, NeighbouringLargeIntegerKeysAreDistinctItems) {
    auto m = model_of(kNumberKey);
    DynamoDbSink s(options_for(m, kNumberKey));
    s.open();
    const std::vector<std::string> ids = {"9223372036854775807",
                                          "9223372036854775808",
                                          "9223372036854775809",
                                          "99999999999999999999999999999999999998",
                                          "99999999999999999999999999999999999999"};
    Batch<std::string> b;
    for (const auto& id : ids) {
        b.emplace(R"({"id":)" + id + R"(,"v":")" + id + R"("})");
    }
    s.on_data(b);
    s.on_barrier(barrier(1));
    ASSERT_EQ(m->items().size(), ids.size());
    for (const auto& id : ids) {
        EXPECT_EQ(attr(m->item(N(id)), "v"), id);
        EXPECT_EQ(attr(m->item(N(id)), "id"), id);  // stored exactly, not rounded
    }
}

// ---- refused records ------------------------------------------------------------------

TEST(DynamoDbSinkValidation, ARefusedRecordAddsNothingOfItsBatch) {
    auto m = model_of();
    DynamoDbSink s(options_for(m));
    s.open();
    const auto why = thrown([&] {
        s.on_data(batch_of({rec("k1", "1", "1"), R"({"pk":"k2","v":1})", rec("k3", "1", "1")}));
    });
    EXPECT_NE(
        why.find("record 2 of a batch of 3 is refused: the sort key attribute \"sk\" is missing"),
        std::string::npos)
        << why;
    EXPECT_NE(why.find("Nothing of that batch was buffered or written"), std::string::npos) << why;
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(s.pending_items(), 0u);
    EXPECT_TRUE(m->calls().empty());
}

TEST(DynamoDbSinkValidation, ARefusedRecordNeverReplacesABufferedOne) {
    auto m = model_of();
    {
        DynamoDbSink s(options_for(m));
        s.open();
        s.on_data(batch_of({rec("k1", "1", "1")}));
        EXPECT_EQ(s.pending_items(), 1u);
        // The same item with a number DynamoDB cannot hold.
        const auto why = thrown([&] { s.on_data(batch_of({rec("k1", "1", "1E+126")})); });
        EXPECT_NE(why.find("outside DynamoDB's range"), std::string::npos) << why;
        EXPECT_NE(why.find("1 buffered item(s) not yet written are dropped"), std::string::npos)
            << why;
        EXPECT_TRUE(m->calls().empty());
    }
    // The restart replays the valid record; the refused one would fail again.
    DynamoDbSink restarted(options_for(m));
    restarted.open();
    restarted.on_data(batch_of({rec("k1", "1", "1")}));
    restarted.on_barrier(barrier(1));
    EXPECT_EQ(attr(m->item(S("k1"), N("1")), "v"), "1");
}

TEST(DynamoDbSinkValidation, EarlierWritesStandWhenALaterBatchIsRefused) {
    auto m = model_of();
    auto o = options_for(m);
    o.batch_records = 2;
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "1"), rec("k3", "1", "1")}));
    ASSERT_EQ(m->items().size(), 2u);  // the first two were written when the buffer filled
    const auto why =
        thrown([&] { s.on_data(batch_of({rec("k4", "1", "1"), R"({"pk":"k5","sk":"x"})"})); });
    EXPECT_NE(why.find("record 2 of a batch of 2"), std::string::npos) << why;
    EXPECT_NE(why.find("1 buffered item(s) not yet written are dropped"), std::string::npos) << why;
    EXPECT_EQ(m->items().size(), 2u);
    EXPECT_FALSE(m->item(S("k3"), N("1")).has_value());
    EXPECT_FALSE(m->item(S("k4"), N("1")).has_value());
}

TEST(DynamoDbSinkValidation, ItemsAtDynamoDbsLimitsAreWrittenAndPastThemRefused) {
    auto m = model_of();
    auto o = options_for(m);
    DynamoDbSink s(std::move(o));
    s.open();
    // pk "s" (3) + sk 1 (4) + attribute "v" (1 + L) = 8 + L bytes.
    s.on_data(batch_of({R"({"pk":"s","sk":1,"v":")" + std::string(409592, 'x') + R"("})"}));
    s.on_barrier(barrier(1));
    EXPECT_TRUE(m->item(S("s"), N("1")).has_value());
    const auto why = thrown([&] {
        s.on_data(batch_of({R"({"pk":"s","sk":2,"v":")" + std::string(409593, 'x') + R"("})"}));
    });
    EXPECT_NE(why.find("409601 bytes"), std::string::npos) << why;
}

TEST(DynamoDbSinkValidation, ABatchStaysWithin16MiBOnTheWire) {
    auto m = model_of();
    DynamoDbSink s(options_for(m));
    s.open();
    // 25 items of 400,000 control bytes: within 400 KB stored, about 2.4 MB each
    // as JSON, so no request may carry all 25.
    std::string controls;
    for (int i = 0; i < 400000; ++i) {
        controls += "\\u0001";
    }
    Batch<std::string> b;
    for (int i = 0; i < 25; ++i) {
        b.emplace(R"({"pk":"big","sk":)" + std::to_string(i) + R"(,"v":")" + controls + R"("})");
    }
    s.on_data(b);
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->items().size(), 25u);
    const auto calls = m->calls();
    EXPECT_GE(calls.size(), 4u);
    for (const auto& call : calls) {
        std::size_t bytes = 0;
        for (const auto& item : call) {
            bytes += clink::aws::dynamodb_request_bytes(item);
        }
        EXPECT_LE(bytes, clink::aws::kDynamoDbMaxRequestBytes);
    }
}

// ---- partial writes and retries -------------------------------------------------------

TEST(DynamoDbSinkWrites, OnlyTheUnprocessedItemsAreSentAgain) {
    auto m = model_of();
    m->set_planner([](std::size_t call, const auto& items) {
        return call == 0 ? DynamoDbModel::unprocessed(items.size(), {1, 3}) : DynamoDbModel::Plan{};
    });
    DynamoDbSink s(options_for(m));
    s.open();
    s.on_data(batch_of({rec("k0", "1", "0"),
                        rec("k1", "1", "1"),
                        rec("k2", "1", "2"),
                        rec("k3", "1", "3"),
                        rec("k4", "1", "4")}));
    s.on_barrier(barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    ASSERT_EQ(calls[1].size(), 2u);
    std::set<std::string> resent;
    for (const auto& item : calls[1]) {
        resent.insert(attr(item, "pk"));
    }
    EXPECT_EQ(resent, (std::set<std::string>{"k1", "k3"}));
    EXPECT_EQ(m->items().size(), 5u);
}

TEST(DynamoDbSinkWrites, UnprocessedItemsAreRetriedWhileTheyShrink) {
    auto m = model_of();
    // 10 items: nothing, then 2, 3 and 5 written. Only the first attempt wrote
    // nothing, so one retry in a row without progress is enough.
    m->set_planner([](std::size_t call, const auto& items) {
        switch (call) {
            case 0:
                return DynamoDbModel::unprocessed(items.size(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
            case 1:
                return DynamoDbModel::unprocessed(items.size(), {0, 1, 2, 3, 4, 5, 6, 7});
            case 2:
                return DynamoDbModel::unprocessed(items.size(), {0, 1, 2, 3, 4});
            default:
                return DynamoDbModel::Plan{};
        }
    });
    auto o = options_for(m);
    o.max_retries = 1;
    DynamoDbSink s(std::move(o));
    s.open();
    Batch<std::string> b;
    for (int i = 0; i < 10; ++i) {
        b.emplace(rec("k" + std::to_string(i), "1", std::to_string(i)));
    }
    s.on_data(b);
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->calls().size(), 4u);
    EXPECT_EQ(m->items().size(), 10u);
}

TEST(DynamoDbSinkWrites, AttemptsThatWriteNothingAreBounded) {
    auto m = model_of();
    m->set_planner([](std::size_t, const auto& items) {
        std::set<std::size_t> all;
        for (std::size_t i = 0; i < items.size(); ++i) {
            all.insert(i);
        }
        return DynamoDbModel::unprocessed(items.size(), all);
    });
    auto o = options_for(m);
    o.max_retries = 3;
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")}));
    const auto why = thrown([&] { s.on_barrier(barrier(1)); });
    EXPECT_NE(why.find("2 of 2 item(s) of a BatchWriteItem were still unwritten after 4 attempt(s) "
                       "in a row that wrote nothing (max_retries=3"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("2 returned as UnprocessedItems"), std::string::npos) << why;
    EXPECT_EQ(m->calls().size(), 4u);
    EXPECT_TRUE(s.failed());
    EXPECT_TRUE(m->items().empty());
}

TEST(DynamoDbSinkWrites, TheRetryBudgetBoundsTheWaiting) {
    auto m = model_of();
    m->set_planner([](std::size_t, const auto&) {
        return DynamoDbModel::error(
            "ProvisionedThroughputExceededException", 400, DynamoDbFault::Throttled);
    });
    auto o = options_for(m);
    o.max_retries = 20;
    o.retry_base_backoff = 20ms;
    o.max_backoff = 40ms;
    o.retry_timeout = 150ms;
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1")}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { s.on_barrier(barrier(1)); });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);
    EXPECT_NE(why.find("when the retry budget of 150 ms ran out"), std::string::npos) << why;
    EXPECT_NE(why.find("ProvisionedThroughputExceededException"), std::string::npos) << why;
}

TEST(DynamoDbSinkWrites, ThrottledAndFailedRequestsAreSentAgain) {
    auto m = model_of();
    m->set_planner([](std::size_t call, const auto&) {
        if (call == 0) {
            return DynamoDbModel::error("ThrottlingException", 400, DynamoDbFault::Throttled);
        }
        if (call == 1) {
            return DynamoDbModel::error("InternalServerError", 500, DynamoDbFault::Transient);
        }
        return DynamoDbModel::Plan{};
    });
    DynamoDbSink s(options_for(m));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")}));
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->calls().size(), 3u);
    EXPECT_EQ(m->items().size(), 2u);
}

// ---- responses that do not match their request --------------------------------------------

TEST(DynamoDbSinkResponses, AnInconsistentResponseFailsTheFlush) {
    const auto put = [](const std::string& table, std::string pk, std::string sk) {
        DynamoDbItem item{{"pk", S(std::move(pk))}, {"sk", N(std::move(sk))}};
        return DynamoDbUnprocessed{table, DynamoDbUnprocessed::Kind::Put, std::move(item), true};
    };
    struct Case {
        std::string name;
        std::vector<DynamoDbUnprocessed> unprocessed;
        std::string expect;
    };
    auto unrecognised = put(kTable, "k1", "1");
    unrecognised.recognised = false;
    auto keyless = put(kTable, "k1", "1");
    keyless.item.pop_back();
    auto del = put(kTable, "k1", "1");
    del.kind = DynamoDbUnprocessed::Kind::Delete;
    auto neither = put(kTable, "k1", "1");
    neither.kind = DynamoDbUnprocessed::Kind::Neither;
    const std::vector<Case> cases = {
        {"another table", {put("invoices", "k1", "1")}, "for table 'invoices'"},
        {"a delete", {del}, "returns a DeleteRequest"},
        {"neither", {neither}, "neither a put nor a delete"},
        {"an item not sent",
         {put(kTable, "k9", "1")},
         "returns item (\"k9\", 1), which the request did not carry"},
        {"one item twice",
         {put(kTable, "k1", "1"), put(kTable, "k1", "1.0")},
         "returns item (\"k1\", 1) twice"},
        {"a foreign type", {unrecognised}, "a type the sink never writes"},
        {"no key", {keyless}, "without a valid key"},
    };
    for (const auto& c : cases) {
        auto m = model_of();
        m->set_planner([&c](std::size_t, const auto&) {
            DynamoDbModel::Plan p;
            p.unprocessed = c.unprocessed;
            return p;
        });
        DynamoDbSink s(options_for(m));
        s.open();
        s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")}));
        const auto why = thrown([&] { s.on_barrier(barrier(1)); });
        EXPECT_NE(why.find("does not match its request"), std::string::npos)
            << c.name << ": " << why;
        EXPECT_NE(why.find(c.expect), std::string::npos) << c.name << ": " << why;
        EXPECT_NE(why.find("Whether the request's 2 item(s) were written is unknown"),
                  std::string::npos)
            << c.name;
        EXPECT_TRUE(s.failed()) << c.name;
        EXPECT_EQ(m->calls().size(), 1u) << c.name << ": an ambiguous response is not retried";
    }
}

// A sink given the table's ARN accepts UnprocessedItems listed under the table's
// name, as DescribeTable reported it.
TEST(DynamoDbSinkResponses, TheTableMayBeNamedByNameOrArn) {
    auto m = model_of();
    m->set_planner([](std::size_t call, const auto& items) {
        if (call != 0) {
            return DynamoDbModel::Plan{};
        }
        auto p = DynamoDbModel::unprocessed(items.size(), {0});
        p.unprocessed = std::vector<DynamoDbUnprocessed>{
            DynamoDbUnprocessed{kTable, DynamoDbUnprocessed::Kind::Put, *items[0], true}};
        return p;
    });
    auto o = options_for(m);
    o.table = m->arn();
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")}));
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->calls().size(), 2u);
    EXPECT_EQ(m->items().size(), 2u);
}

// ---- permanent errors and lost replies --------------------------------------------------

TEST(DynamoDbSinkFailures, PermanentErrorsFailAtOnce) {
    for (const auto& [code, status, hint] : std::vector<std::tuple<std::string, int, std::string>>{
             {"ValidationException", 400, "may be a defect in the sink"},
             {"ResourceNotFoundException", 400, "Check that the table exists"},
             {"AccessDeniedException", 400, "dynamodb:BatchWriteItem"},
             {"UnrecognizedClientException", 400, "default chain"}}) {
        auto m = model_of();
        m->set_planner([&](std::size_t, const auto&) {
            return DynamoDbModel::error(code, status, DynamoDbFault::Permanent);
        });
        DynamoDbSink s(options_for(m));
        s.open();
        s.on_data(batch_of({rec("k1", "1", "1")}));
        const auto why = thrown([&] { s.on_barrier(barrier(1)); });
        EXPECT_NE(why.find("BatchWriteItem was refused: " + code), std::string::npos) << why;
        EXPECT_NE(why.find(hint), std::string::npos) << code << ": " << why;
        EXPECT_EQ(m->calls().size(), 1u) << code;
        EXPECT_TRUE(s.failed());
    }
}

TEST(DynamoDbSinkFailures, AReplyLostAfterTheWritesIsSentAgain) {
    auto m = model_of();
    // The service applies the request and the reply never arrives.
    m->set_planner([](std::size_t call, const auto&) {
        return call == 0 ? DynamoDbModel::error(
                               "NoResponse", 0, DynamoDbFault::Transient, /*applied=*/true)
                         : DynamoDbModel::Plan{};
    });
    DynamoDbSink s(options_for(m));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")}));
    s.on_barrier(barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[0], calls[1]);  // the same puts, the same values
    EXPECT_EQ(m->items().size(), 2u);
    EXPECT_EQ(attr(m->item(S("k2"), N("1")), "v"), "2");
    EXPECT_FALSE(s.failed());
}

// ---- cancellation -----------------------------------------------------------------------

TEST(DynamoDbSinkCancel, ACancelDuringARetryWaitStopsAtOnce) {
    auto m = model_of();
    m->set_planner([](std::size_t, const auto&) {
        return DynamoDbModel::error("ThrottlingException", 400, DynamoDbFault::Throttled);
    });
    auto o = options_for(m);
    o.retry_base_backoff = 5000ms;
    o.max_backoff = 5000ms;
    DynamoDbSink s(std::move(o));
    Attached a(s);
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1")}));
    auto done =
        std::async(std::launch::async, [&] { return thrown([&] { s.on_barrier(barrier(1)); }); });
    while (m->calls().empty()) {
        std::this_thread::sleep_for(5ms);
    }
    const auto t0 = std::chrono::steady_clock::now();
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 1s);
    const auto why = done.get();
    EXPECT_NE(why.find("cancelled while waiting to retry a BatchWriteItem"), std::string::npos)
        << why;
    EXPECT_TRUE(s.failed());
}

TEST(DynamoDbSinkCancel, ACancelDuringACallEndsIt) {
    auto m = model_of();
    m->set_planner([](std::size_t, const auto&) {
        DynamoDbModel::Plan p;
        p.hang = true;
        return p;
    });
    DynamoDbSink s(options_for(m));
    Attached a(s);
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1")}));
    auto done =
        std::async(std::launch::async, [&] { return thrown([&] { s.on_barrier(barrier(1)); }); });
    while (!m->hanging()) {
        std::this_thread::sleep_for(5ms);
    }
    // The barrier has not returned while its write is unresolved.
    EXPECT_EQ(done.wait_for(200ms), std::future_status::timeout);
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    const auto why = done.get();
    EXPECT_NE(why.find("cancelled while a BatchWriteItem of 1 item(s) was in flight"),
              std::string::npos)
        << why;
    EXPECT_TRUE(m->aborted());
    EXPECT_TRUE(s.failed());
}

// ---- the lifecycle ----------------------------------------------------------------------

TEST(DynamoDbSinkLifecycle, ABarrierReturnsOnlyOnceEverythingBeforeItIsWritten) {
    auto m = model_of();
    m->set_planner([](std::size_t call, const auto& items) {
        return call < 2 ? DynamoDbModel::unprocessed(items.size(), {0}) : DynamoDbModel::Plan{};
    });
    auto o = options_for(m);
    o.batch_records = 25;
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2"), rec("k3", "1", "3")}));
    EXPECT_TRUE(m->calls().empty());  // buffered, below batch_records
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->items().size(), 3u);
    EXPECT_EQ(s.pending_items(), 0u);
}

TEST(DynamoDbSinkLifecycle, AFailedSinkRefusesEveryLaterBarrier) {
    auto m = model_of();
    m->set_planner([](std::size_t, const auto&) {
        return DynamoDbModel::error("ValidationException", 400, DynamoDbFault::Permanent);
    });
    auto o = options_for(m);
    o.batch_records = 1;
    DynamoDbSink s(std::move(o));
    s.open();
    EXPECT_NE(thrown([&] { s.on_data(batch_of({rec("k1", "1", "1"), rec("k2", "1", "2")})); }), "");
    EXPECT_TRUE(s.failed());
    // Nothing that would acknowledge a checkpoint over the dropped writes passes.
    EXPECT_NE(thrown([&] { s.on_barrier(barrier(1)); }).find("failed earlier and dropped writes"),
              std::string::npos);
    EXPECT_NE(thrown([&] { s.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { s.on_data(batch_of({rec("k3", "1", "3")})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NO_THROW(s.close());
    EXPECT_EQ(m->calls().size(), 1u);
}

TEST(DynamoDbSinkLifecycle, ARestartAfterAFailedBarrierConverges) {
    auto m = model_of();
    std::vector<std::string> records;
    for (int i = 0; i < 40; ++i) {
        records.push_back(
            rec("k" + std::to_string(i % 7), std::to_string(i % 3), std::to_string(i)));
    }
    const auto slice = [&](std::size_t from, std::size_t to) {
        return batch_of(
            std::vector<std::string>(records.begin() + static_cast<std::ptrdiff_t>(from),
                                     records.begin() + static_cast<std::ptrdiff_t>(to)));
    };
    {
        auto o = options_for(m);
        o.batch_records = 5;
        DynamoDbSink first(std::move(o));
        first.open();
        first.on_data(slice(0, 20));
        first.on_barrier(barrier(1));  // checkpoint 1 covers records 0..19
        // From here every write fails, after some of records 20..29 reached the table.
        std::atomic<int> calls{0};
        m->set_planner([&calls](std::size_t, const auto&) {
            return ++calls > 1
                       ? DynamoDbModel::error("InternalServerError", 500, DynamoDbFault::Transient)
                       : DynamoDbModel::Plan{};
        });
        EXPECT_NE(thrown([&] {
                      first.on_data(slice(20, 30));
                      first.on_barrier(barrier(2));
                  }),
                  "");
        EXPECT_TRUE(first.failed());
        m->set_planner({});
    }
    // The restart from checkpoint 1 replays records 20..39 in order.
    auto o = options_for(m);
    o.batch_records = 5;
    DynamoDbSink restarted(std::move(o));
    restarted.open();
    restarted.on_data(slice(20, 40));
    restarted.on_barrier(barrier(3));
    // Every item holds the last record written for it.
    std::map<std::pair<std::string, std::string>, std::string> expected;
    for (int i = 0; i < 40; ++i) {
        expected[{"k" + std::to_string(i % 7), std::to_string(i % 3)}] = std::to_string(i);
    }
    ASSERT_EQ(m->items().size(), expected.size());
    for (const auto& [key, v] : expected) {
        EXPECT_EQ(attr(m->item(S(key.first), N(key.second)), "v"), v)
            << key.first << "/" << key.second;
    }
}

TEST(DynamoDbSinkLifecycle, CloseWritesNothing) {
    auto m = model_of();
    DynamoDbSink s(options_for(m));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1")}));
    s.close();
    EXPECT_TRUE(m->calls().empty());
}

TEST(DynamoDbSinkLifecycle, LingerFlushesAnOldPartialBatch) {
    auto m = model_of();
    auto o = options_for(m);
    o.max_age = 30ms;
    DynamoDbSink s(std::move(o));
    s.open();
    s.on_data(batch_of({rec("k1", "1", "1")}));
    EXPECT_TRUE(m->calls().empty());
    std::this_thread::sleep_for(40ms);
    s.on_data(batch_of({rec("k2", "1", "2")}));
    EXPECT_EQ(m->items().size(), 2u);
}

// ---- classification -------------------------------------------------------------------

TEST(DynamoDbErrorClassification, FollowsTheServicesRetryTable) {
    using clink::aws::classify_dynamodb_error;
    for (const char* c :
         {"ProvisionedThroughputExceededException",
          "ThrottlingException",
          "RequestLimitExceeded",
          "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"}) {
        EXPECT_EQ(classify_dynamodb_error(c, 400, true), DynamoDbFault::Throttled) << c;
    }
    EXPECT_EQ(classify_dynamodb_error("", 429, true), DynamoDbFault::Throttled);
    for (const char* c : {"InternalServerError",
                          "ServiceUnavailable",
                          "ReplicatedWriteConflictException",
                          "ItemCollectionSizeLimitExceededException"}) {
        EXPECT_EQ(classify_dynamodb_error(c, 400, false), DynamoDbFault::Transient) << c;
    }
    EXPECT_EQ(classify_dynamodb_error("NoResponse", 0, false), DynamoDbFault::Transient);
    EXPECT_EQ(classify_dynamodb_error("SomethingNew", 502, false), DynamoDbFault::Transient);
    for (const char* c : {"ValidationException",
                          "ResourceNotFoundException",
                          "AccessDeniedException",
                          "UnrecognizedClientException",
                          "MissingAuthenticationTokenException",
                          "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"}) {
        EXPECT_EQ(classify_dynamodb_error(c, 400, true), DynamoDbFault::Permanent) << c;
    }
    EXPECT_EQ(classify_dynamodb_error("SomethingNew", 400, false), DynamoDbFault::Permanent);
    EXPECT_EQ(classify_dynamodb_error("SomethingNew", 400, true), DynamoDbFault::Transient);
}
