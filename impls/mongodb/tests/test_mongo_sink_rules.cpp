// The MongoDB sink's rules (sink_rules.hpp), without a server: which keys a
// replacement matches by, which write concerns the sink's guarantee survives,
// and which index makes a key match at most one document.

#include <chrono>
#include <string>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_MONGODB
#include <bsoncxx/json.hpp>
#include <mongocxx/write_concern.hpp>

#include "sink_rules.hpp"
#endif

#ifdef CLINK_HAS_MONGODB

namespace rules = clink::mongodb::rules;
using namespace std::chrono_literals;

namespace {

std::optional<std::string> key_problem_of(const std::string& json) {
    const auto doc = bsoncxx::from_json(json);
    return rules::key_problem(doc.view(), "_id");
}

bool contains(const std::optional<std::string>& s, const std::string& needle) {
    return s.has_value() && s->find(needle) != std::string::npos;
}

}  // namespace

TEST(MongoSinkRules, AKeyIsAStringAnIntegerOrAnObjectId) {
    EXPECT_FALSE(key_problem_of(R"({"_id":"k"})"));
    EXPECT_FALSE(key_problem_of(R"({"_id":7})"));
    EXPECT_FALSE(key_problem_of(R"({"_id":{"$numberLong":"9000000000"}})"));
    EXPECT_FALSE(key_problem_of(R"({"_id":{"$oid":"65a1b2c3d4e5f60718293a4b"}})"));
}

// Each refused type would let a replacement match a document that is not the
// record's, or none it should.
TEST(MongoSinkRules, AnyOtherKeyIsRefusedByName) {
    EXPECT_TRUE(contains(key_problem_of(R"({"v":1})"), "has no '_id' field"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":null})"), "is null"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":1.5})"), "is a double"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":[1,2]})"), "is an array"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":{"a":1}})"), "is an embedded document"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":true})"), "is a boolean"));
    EXPECT_TRUE(
        contains(key_problem_of(R"({"_id":{"$date":"2026-01-01T00:00:00Z"}})"), "is a date"));
    EXPECT_TRUE(contains(key_problem_of(R"({"_id":{"$numberDecimal":"1.5"}})"), "is a decimal"));
}

TEST(MongoSinkRules, TheKeyFieldIsATopLevelFieldName) {
    EXPECT_FALSE(rules::key_field_problem("order_id"));
    EXPECT_TRUE(contains(rules::key_field_problem("a.b"), "dotted path"));
    EXPECT_TRUE(contains(rules::key_field_problem("$k"), "operator"));
    EXPECT_TRUE(contains(rules::key_field_problem(""), "empty"));
}

// Only a majority-acknowledged write survives a failover; the default is taken
// as majority, anything weaker is refused.
TEST(MongoSinkRules, OnlyAMajorityWriteConcernCarriesTheGuarantee) {
    using level = mongocxx::write_concern::level;
    EXPECT_FALSE(rules::write_concern_problem(mongocxx::write_concern{}));
    mongocxx::write_concern majority;
    majority.majority(0ms);
    EXPECT_FALSE(rules::write_concern_problem(majority));

    mongocxx::write_concern w0;
    w0.acknowledge_level(level::k_unacknowledged);
    EXPECT_TRUE(contains(rules::write_concern_problem(w0), "w=0"));
    mongocxx::write_concern w1;
    w1.acknowledge_level(level::k_acknowledged);
    EXPECT_TRUE(contains(rules::write_concern_problem(w1), "w=1"));
    mongocxx::write_concern w2;
    w2.nodes(2);
    EXPECT_TRUE(contains(rules::write_concern_problem(w2), "w=2"));
    mongocxx::write_concern tagged;
    tagged.tag("dc");
    EXPECT_TRUE(contains(rules::write_concern_problem(tagged), "tagged"));
}

TEST(MongoSinkRules, TheSinkWritesMajorityWithABoundedTimeout) {
    const auto from_default = rules::effective_write_concern(mongocxx::write_concern{}, 30s);
    EXPECT_TRUE(from_default.majority());
    EXPECT_EQ(from_default.timeout(), 30s);
    mongocxx::write_concern uri_majority;
    uri_majority.majority(5s);
    const auto kept = rules::effective_write_concern(uri_majority, 30s);
    EXPECT_TRUE(kept.majority());
    EXPECT_EQ(kept.timeout(), 5s) << "the URI's own wtimeout stands";
}

TEST(MongoSinkRules, OnlyAUniqueIndexOnTheKeyAloneMakesItMatchOneDocument) {
    const auto yes = bsoncxx::from_json(R"({"v":2,"key":{"k":1},"name":"k_1","unique":true})");
    EXPECT_TRUE(rules::unique_single_field_index(yes.view(), "k"));
    const auto descending =
        bsoncxx::from_json(R"({"v":2,"key":{"k":-1},"name":"k_-1","unique":true})");
    EXPECT_TRUE(rules::unique_single_field_index(descending.view(), "k"));

    const auto not_unique = bsoncxx::from_json(R"({"v":2,"key":{"k":1},"name":"k_1"})");
    EXPECT_FALSE(rules::unique_single_field_index(not_unique.view(), "k"));
    const auto compound =
        bsoncxx::from_json(R"({"v":2,"key":{"k":1,"j":1},"name":"k_1_j_1","unique":true})");
    EXPECT_FALSE(rules::unique_single_field_index(compound.view(), "k"));
    const auto partial = bsoncxx::from_json(
        R"({"v":2,"key":{"k":1},"name":"k_1","unique":true,"partialFilterExpression":{"a":1}})");
    EXPECT_FALSE(rules::unique_single_field_index(partial.view(), "k"));
    EXPECT_FALSE(rules::unique_single_field_index(yes.view(), "other"));
}

#endif  // CLINK_HAS_MONGODB
