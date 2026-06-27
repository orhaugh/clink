// DynamoDB sink mapping: JSON value -> AttributeValue (S/N/BOOL/NULL/M/L), the
// number-text formatting (incl. the int64 range guard), and the sink's
// construction-time validation. These need the AWS SDK linked (for the model
// types) but NO live endpoint.

#include <memory>
#include <string>

#include <aws/dynamodb/model/AttributeValue.h>
#include <gtest/gtest.h>

#include "clink/aws/dynamodb_sink.hpp"
#include "clink/config/json.hpp"

using clink::aws::ddb_number_text;
using clink::aws::DynamoDbSink;
using clink::aws::DynamoDbSinkOptions;
using clink::aws::json_to_attribute_value;
using clink::config::parse;
using AV = Aws::DynamoDB::Model::AttributeValue;
using VT = Aws::DynamoDB::Model::ValueType;

namespace {

AV av_of(const std::string& json) {
    return json_to_attribute_value(parse(json));
}

}  // namespace

TEST(DynamoDbItem, ScalarTypeMapping) {
    EXPECT_EQ(av_of(R"("hello")").GetType(), VT::STRING);
    EXPECT_EQ(av_of(R"("hello")").GetS(), "hello");

    EXPECT_EQ(av_of("42").GetType(), VT::NUMBER);
    EXPECT_EQ(av_of("42").GetN(), "42");  // integral -> no ".0"

    EXPECT_EQ(av_of("true").GetType(), VT::BOOL);
    EXPECT_TRUE(av_of("true").GetBool());

    EXPECT_EQ(av_of("null").GetType(), VT::NULLVALUE);
    EXPECT_TRUE(av_of("null").GetNull());

    // Empty string is allowed for non-key attributes (modern DynamoDB).
    EXPECT_EQ(av_of(R"("")").GetType(), VT::STRING);
    EXPECT_EQ(av_of(R"("")").GetS(), "");
}

TEST(DynamoDbItem, NestedObjectMapsToM) {
    AV m = av_of(R"({"a":1,"b":"x"})");
    ASSERT_EQ(m.GetType(), VT::ATTRIBUTE_MAP);
    auto inner = m.GetM();
    ASSERT_TRUE(inner.count("a"));
    ASSERT_TRUE(inner.count("b"));
    EXPECT_EQ(inner.at("a")->GetN(), "1");
    EXPECT_EQ(inner.at("b")->GetS(), "x");
}

TEST(DynamoDbItem, ArrayMapsToL) {
    AV l = av_of(R"([1,"two",true])");
    ASSERT_EQ(l.GetType(), VT::ATTRIBUTE_LIST);
    auto items = l.GetL();
    ASSERT_EQ(items.size(), 3u);
    EXPECT_EQ(items[0]->GetN(), "1");
    EXPECT_EQ(items[1]->GetS(), "two");
    EXPECT_TRUE(items[2]->GetBool());
}

TEST(DynamoDbItem, NumberTextIntegerAndFractionalAndOutOfRange) {
    EXPECT_EQ(ddb_number_text(42.0), "42");
    EXPECT_EQ(ddb_number_text(-7.0), "-7");
    EXPECT_EQ(ddb_number_text(42.5), "42.5");
    // Out of int64 range must NOT trap (UB) nor collapse onto INT64_MAX.
    const std::string big = ddb_number_text(1e19);
    EXPECT_NE(big, "9223372036854775807");
    EXPECT_FALSE(big.empty());
}

TEST(DynamoDbSinkCtor, RequiresTableAndPartitionKey) {
    DynamoDbSinkOptions o;
    o.partition_key = "id";  // no table
    EXPECT_THROW(DynamoDbSink{std::move(o)}, std::runtime_error);

    DynamoDbSinkOptions o2;
    o2.table = "t";  // no partition_key
    EXPECT_THROW(DynamoDbSink{std::move(o2)}, std::runtime_error);

    DynamoDbSinkOptions o3;
    o3.table = "t";
    o3.partition_key = "id";
    EXPECT_NO_THROW(DynamoDbSink{std::move(o3)});  // ctor does not touch AWS
}

TEST(DynamoDbSinkCtor, BatchRecordsClampedToDynamoMax) {
    DynamoDbSinkOptions o;
    o.table = "t";
    o.partition_key = "id";
    o.batch_records = 1000;  // clamps to 25 (the BatchWriteItem ceiling)
    EXPECT_NO_THROW(DynamoDbSink{std::move(o)});
    DynamoDbSinkOptions z;
    z.table = "t";
    z.partition_key = "id";
    z.batch_records = 0;  // clamps to 25
    EXPECT_NO_THROW(DynamoDbSink{std::move(z)});
}
