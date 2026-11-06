// The DynamoDB sink's records, without a service: numbers and their equality,
// item identity, the record checks and DynamoDB's size rules. The limits under
// test are DynamoDB's documented ones; the number-size rule (digit pairs aligned
// on the decimal point) is the one DynamoDB Local applies, never smaller than
// the documented approximation.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/PutRequest.h>
#include <aws/dynamodb/model/WriteRequest.h>
#include <gtest/gtest.h>

#include "clink/aws/dynamodb_sink.hpp"

using clink::aws::DynamoDbItemKey;
using clink::aws::DynamoDbKeyAttribute;
using clink::aws::DynamoDbKeySchema;
using clink::aws::DynamoDbKeyType;
using clink::aws::DynamoDbKeyValue;
using clink::aws::DynamoDbRecordError;
using clink::aws::DynamoDbValue;
using clink::aws::parse_dynamodb_number;
using clink::aws::prepare_dynamodb_item;

namespace {

const DynamoDbKeySchema kStringKey{DynamoDbKeyAttribute{"id", DynamoDbKeyType::S}, std::nullopt};
const DynamoDbKeySchema kComposite{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::S},
                                   DynamoDbKeyAttribute{"sk", DynamoDbKeyType::S}};
const DynamoDbKeySchema kNumberSort{DynamoDbKeyAttribute{"pk", DynamoDbKeyType::S},
                                    DynamoDbKeyAttribute{"sk", DynamoDbKeyType::N}};

std::string canonical(const std::string& numeral, bool json = true) {
    const auto n = parse_dynamodb_number(numeral, json);
    return n ? n->text() : "<refused>";
}

std::size_t number_size(const std::string& numeral) {
    return parse_dynamodb_number(numeral, false)->size_bytes();
}

// The reason prepare_dynamodb_item refuses `record`, or "" when it accepts it.
std::string refusal(const std::string& record, const DynamoDbKeySchema& schema = kStringKey) {
    try {
        (void)prepare_dynamodb_item(record, schema);
        return "";
    } catch (const DynamoDbRecordError& e) {
        return e.what();
    }
}

DynamoDbItemKey key_of(const std::string& record, const DynamoDbKeySchema& schema) {
    return prepare_dynamodb_item(record, schema).key;
}

std::string nested_maps(int depth, const std::string& leaf) {
    std::string s = leaf;
    for (int i = 0; i < depth; ++i) {
        s = R"({"a":)" + s + "}";
    }
    return s;
}

std::string nested_lists(int depth, const std::string& leaf) {
    std::string s = leaf;
    for (int i = 0; i < depth; ++i) {
        s = "[" + s + "]";
    }
    return s;
}

}  // namespace

// ---- numbers ------------------------------------------------------------------------

TEST(DynamoDbNumber, EquivalentNumeralsAreOneNumber) {
    for (const char* n : {"1", "1.0", "1.00", "1e0", "1E0", "10E-1", "0.1E1", "0.01e2"}) {
        EXPECT_EQ(canonical(n), "1") << n;
    }
    for (const char* n : {"100", "1e2", "1E+2", "0.01e4", "100.000"}) {
        EXPECT_EQ(canonical(n), "100") << n;
    }
    // Zero has no sign: -0 and 0 are one number, as in DynamoDB.
    for (const char* n : {"0", "-0", "0.0", "-0.0E5", "0e-7"}) {
        EXPECT_EQ(canonical(n), "0") << n;
    }
    EXPECT_EQ(*parse_dynamodb_number("-0", true), *parse_dynamodb_number("0", true));
    EXPECT_EQ(canonical("1.50"), "1.5");
    EXPECT_EQ(canonical("-42.500e1"), "-425");
}

TEST(DynamoDbNumber, TextKeepsEveryDigit) {
    // Integers keep their decimal text, as the sink wrote them before.
    EXPECT_EQ(canonical("42"), "42");
    EXPECT_EQ(canonical("-7"), "-7");
    EXPECT_EQ(canonical("9007199254740993"), "9007199254740993");
    EXPECT_EQ(canonical("-9223372036854775808"), "-9223372036854775808");
    // Past int64 and past a double's 17 digits: exact, where a double rounded.
    EXPECT_EQ(canonical("9223372036854775809"), "9223372036854775809");
    EXPECT_EQ(canonical("12345678901234567890123456789012345678"),
              "12345678901234567890123456789012345678");
    EXPECT_EQ(canonical("0.1000000000000000055511151231257827"),
              "0.1000000000000000055511151231257827");
    EXPECT_EQ(canonical("42.5"), "42.5");
    EXPECT_EQ(canonical("0.001"), "0.001");
    EXPECT_EQ(canonical("1e20"), "100000000000000000000");
    // Past 40 characters the text turns scientific.
    EXPECT_EQ(canonical("1E+125"), "1E+125");
    EXPECT_EQ(canonical("1E-130"), "1E-130");
    EXPECT_EQ(canonical("-9.9999999999999999999999999999999999999E+125"),
              "-9.9999999999999999999999999999999999999E+125");
    EXPECT_EQ(canonical("12345678901234567890123456789012345678e5"),
              "1.2345678901234567890123456789012345678E+42");
    // Every text reads back as the same number.
    for (const char* n : {"1E+125", "1E-130", "0.001", "123.45", "1e20", "-0.5"}) {
        const auto first = parse_dynamodb_number(n, true);
        ASSERT_TRUE(first) << n;
        EXPECT_EQ(*parse_dynamodb_number(first->text(), false), *first) << n;
    }
}

TEST(DynamoDbNumber, NeighbouringLargeIntegersStayDistinct) {
    const std::vector<std::string> neighbours = {
        "9007199254740992",
        "9007199254740993",
        "9223372036854775807",
        "9223372036854775808",
        "9223372036854775809",
        "18446744073709551615",
        "18446744073709551616",
        "99999999999999999999999999999999999998",
        "99999999999999999999999999999999999999",
    };
    for (std::size_t i = 0; i < neighbours.size(); ++i) {
        for (std::size_t j = i + 1; j < neighbours.size(); ++j) {
            EXPECT_NE(canonical(neighbours[i]), canonical(neighbours[j]))
                << neighbours[i] << " and " << neighbours[j];
        }
    }
}

TEST(DynamoDbNumber, PrecisionAndRangeAreDynamoDbs) {
    std::string why;
    EXPECT_TRUE(parse_dynamodb_number("12345678901234567890123456789012345678", true));
    EXPECT_FALSE(parse_dynamodb_number("123456789012345678901234567890123456789", true, &why));
    EXPECT_NE(why.find("39 significant digits"), std::string::npos) << why;
    // Trailing zeros are not significant.
    EXPECT_TRUE(parse_dynamodb_number("1234567890123456789012345678901234567800000", true));
    EXPECT_TRUE(parse_dynamodb_number("1E-130", true));
    EXPECT_TRUE(parse_dynamodb_number("-1E-130", true));
    EXPECT_TRUE(parse_dynamodb_number("9.9999999999999999999999999999999999999E+125", true));
    for (const char* n :
         {"1E-131", "-1E-131", "1E+126", "1e999999999999999999999", "1e-99999999999"}) {
        why.clear();
        EXPECT_FALSE(parse_dynamodb_number(n, true, &why)) << n;
        EXPECT_NE(why.find("range"), std::string::npos) << n << ": " << why;
    }
}

TEST(DynamoDbNumber, RecordsFollowJsonGrammar) {
    for (const char* n :
         {"01", "+1", ".5", "5.", "1e", "--1", "1.2.3", "NaN", "Infinity", "-", ""}) {
        EXPECT_FALSE(parse_dynamodb_number(n, true)) << n;
    }
    // The service's own numerals are read leniently.
    EXPECT_EQ(canonical("+1", false), "1");
    EXPECT_EQ(canonical("01", false), "1");
    EXPECT_EQ(canonical(".5", false), "0.5");
    EXPECT_EQ(canonical("5.", false), "5");
}

TEST(DynamoDbNumber, SizeCountsDigitPairsAlignedOnThePoint) {
    EXPECT_EQ(number_size("1"), 2u);
    EXPECT_EQ(number_size("15"), 2u);
    EXPECT_EQ(number_size("100000"), 2u);
    EXPECT_EQ(number_size("123"), 3u);
    EXPECT_EQ(number_size("0.0012"), 2u);
    // Two significant digits split by the point take two pairs: 01.50.
    EXPECT_EQ(number_size("1.5"), 3u);
    EXPECT_EQ(number_size("1.23456789"), 6u);
    EXPECT_EQ(number_size(std::string(38, '1')), 20u);
    EXPECT_EQ(number_size(std::string(37, '1')), 20u);
    EXPECT_EQ(number_size("0"), 2u);
}

// ---- identity -----------------------------------------------------------------------

// The pairs the old NUL-joined text key could not tell apart: "a\0b" + "c" and
// "a" + "b\0c" both joined to "a\0b\0c". Compared structurally they differ.
TEST(DynamoDbIdentity, KeysWithAnEmbeddedSeparatorStayApart) {
    const auto key = [](std::string pk, std::string sk) {
        return DynamoDbItemKey{DynamoDbKeyValue{DynamoDbKeyType::S, std::move(pk)},
                               DynamoDbKeyValue{DynamoDbKeyType::S, std::move(sk)}};
    };
    EXPECT_NE(key(std::string("a\0b", 3), "c"), key("a", std::string("b\0c", 3)));
    EXPECT_NE(key("a|b", "c"), key("a", "b|c"));
    // Separators a record can carry are ordinary bytes of its keys.
    EXPECT_NE(key_of(R"({"pk":"a|b","sk":"c"})", kComposite),
              key_of(R"({"pk":"a","sk":"b|c"})", kComposite));
    EXPECT_NE(key_of(R"({"pk":"a\u0001","sk":"b"})", kComposite),
              key_of(R"({"pk":"a","sk":"\u0001b"})", kComposite));
    EXPECT_EQ(key_of(R"({"pk":"a\u0001","sk":"b"})", kComposite).partition.bytes,
              std::string("a\x01"));
}

// The AWS SDK for C++ writes a string only up to its first NUL, so a record
// holding U+0000 anywhere is refused rather than stored shortened.
TEST(DynamoDbRecord, AStringHoldingNulIsRefused) {
    for (const char* record : {R"({"pk":"a\u0000b","sk":"c"})",
                               R"({"pk":"a","sk":"b\u0000c"})",
                               R"({"pk":"a","sk":"c","v":"x\u0000"})",
                               R"({"pk":"a","sk":"c","m":{"k":["\u0000"]}})",
                               R"({"pk":"a","sk":"c","n\u0000m":1})",
                               R"({"pk":"a","sk":"c","m":{"k\u0000":1}})"}) {
        EXPECT_NE(refusal(record, kComposite).find("U+0000"), std::string::npos) << record;
    }
}

// What the refusal above rests on: the SDK's request body stops each string at
// its NUL, a value's and an attribute name's alike. Should a later SDK keep the
// whole string, this fails, and the refusal can be lifted.
TEST(DynamoDbRecord, TheSdkCutsStringsAtNul) {
    Aws::DynamoDB::Model::PutRequest put;
    put.AddItem("id", Aws::DynamoDB::Model::AttributeValue().SetS(Aws::String("a\0b", 3)));
    put.AddItem(Aws::String("n\0m", 3), Aws::DynamoDB::Model::AttributeValue().SetS("v"));
    Aws::DynamoDB::Model::WriteRequest w;
    w.SetPutRequest(put);
    Aws::DynamoDB::Model::BatchWriteItemRequest r;
    r.AddRequestItems("t", Aws::Vector<Aws::DynamoDB::Model::WriteRequest>{w});
    std::string body;
    for (const char c : r.SerializePayload()) {
        if (c != '\t' && c != '\n') {
            body.push_back(c);
        }
    }
    EXPECT_NE(body.find(R"("id":{"S":"a"})"), std::string::npos) << body;
    EXPECT_NE(body.find(R"("n":{"S":"v"})"), std::string::npos) << body;
    EXPECT_EQ(body.find("u0000"), std::string::npos) << body;
}

TEST(DynamoDbIdentity, TypeIsPartOfIdentity) {
    const DynamoDbKeyValue s{DynamoDbKeyType::S, "1"};
    const DynamoDbKeyValue n{DynamoDbKeyType::N, "1"};
    EXPECT_NE(s, n);
    EXPECT_NE((DynamoDbItemKey{s, std::nullopt}), (DynamoDbItemKey{n, std::nullopt}));
    // A record can only produce the table's type: a string for an N key is
    // refused, never coalesced with the number.
    EXPECT_NE(refusal(R"({"pk":"p","sk":"1"})", kNumberSort), "");
    EXPECT_EQ(refusal(R"({"pk":"p","sk":1})", kNumberSort), "");
}

TEST(DynamoDbIdentity, StringsCompareByTheirUtf8Bytes) {
    // Precomposed and decomposed e-acute are different keys, as in DynamoDB.
    EXPECT_NE(key_of("{\"id\":\"\xC3\xA9\"}", kStringKey),
              key_of("{\"id\":\"e\xCC\x81\"}", kStringKey));
    // An escape and the raw character are the same string.
    EXPECT_EQ(key_of(R"({"id":"é"})", kStringKey), key_of("{\"id\":\"\xC3\xA9\"}", kStringKey));
    EXPECT_EQ(key_of(R"({"id":"😀"})", kStringKey),
              key_of("{\"id\":\"\xF0\x9F\x98\x80\"}", kStringKey));
    EXPECT_NE(key_of(R"({"id":"A"})", kStringKey), key_of(R"({"id":"a"})", kStringKey));
}

TEST(DynamoDbIdentity, EquivalentNumericKeysAreOneItem) {
    const auto one = key_of(R"({"pk":"p","sk":1})", kNumberSort);
    for (const char* sk : {"1.0", "1e0", "10E-1", "0.1E1"}) {
        EXPECT_EQ(key_of(std::string(R"({"pk":"p","sk":)") + sk + "}", kNumberSort), one) << sk;
    }
    EXPECT_EQ(key_of(R"({"pk":"p","sk":-0})", kNumberSort),
              key_of(R"({"pk":"p","sk":0})", kNumberSort));
    EXPECT_NE(key_of(R"({"pk":"p","sk":9223372036854775808})", kNumberSort),
              key_of(R"({"pk":"p","sk":9223372036854775809})", kNumberSort));
    EXPECT_EQ(clink::aws::describe_dynamodb_key(one), "(\"p\", 1)");
}

// ---- records ------------------------------------------------------------------------

TEST(DynamoDbRecord, MapsEveryJsonType) {
    const auto p =
        prepare_dynamodb_item(R"({"id":"k","s":"x","e":"","n":42.50,"t":true,"f":false,"z":null,)"
                              R"("m":{"b":1,"a":[1,"two",{}],"c":{}},"l":[]})",
                              kStringKey);
    ASSERT_EQ(p.item.size(), 9u);
    // Attributes in name order.
    EXPECT_EQ(p.item.front().first, "e");
    const auto at = [&](const std::string& n) -> const DynamoDbValue& {
        for (const auto& [name, v] : p.item) {
            if (name == n) {
                return v;
            }
        }
        ADD_FAILURE() << "no attribute " << n;
        static DynamoDbValue none;
        return none;
    };
    EXPECT_EQ(at("s").kind, DynamoDbValue::Kind::S);
    EXPECT_EQ(at("e").text, "");  // an empty string outside the key is kept
    EXPECT_EQ(at("n").kind, DynamoDbValue::Kind::N);
    EXPECT_EQ(at("n").text, "42.5");
    EXPECT_TRUE(at("t").flag);
    EXPECT_FALSE(at("f").flag);
    EXPECT_EQ(at("z").kind, DynamoDbValue::Kind::Null);
    const auto& m = at("m");
    ASSERT_EQ(m.kind, DynamoDbValue::Kind::M);
    ASSERT_EQ(m.members.size(), 3u);
    EXPECT_EQ(m.members[0].first, "a");
    ASSERT_EQ(m.members[0].second.kind, DynamoDbValue::Kind::L);
    ASSERT_EQ(m.members[0].second.members.size(), 3u);
    EXPECT_EQ(m.members[0].second.members[2].second.kind, DynamoDbValue::Kind::M);
    EXPECT_EQ(at("l").kind, DynamoDbValue::Kind::L);
    EXPECT_TRUE(at("l").members.empty());
}

TEST(DynamoDbRecord, RefusesWhatIsNotAJsonObject) {
    EXPECT_NE(refusal("not json"), "");
    EXPECT_NE(refusal(R"(["id"])"), "");
    EXPECT_NE(refusal(R"("id")"), "");
    EXPECT_NE(refusal(R"({"id":"k"} x)"), "");
    EXPECT_NE(refusal(R"({"id":"k",})"), "");
    EXPECT_NE(refusal(R"({"id":"k")"), "");
    EXPECT_NE(refusal(""), "");
    // Strings: valid UTF-8, control characters escaped, surrogates paired.
    EXPECT_NE(refusal("{\"id\":\"k\",\"v\":\"\xC3\x28\"}"), "");          // bad continuation
    EXPECT_NE(refusal("{\"id\":\"k\",\"v\":\"\xC0\xAF\"}"), "");          // overlong
    EXPECT_NE(refusal("{\"id\":\"k\",\"v\":\"\xED\xA0\x80\"}"), "");      // a surrogate in UTF-8
    EXPECT_NE(refusal("{\"id\":\"k\",\"v\":\"\xF4\x90\x80\x80\"}"), "");  // past U+10FFFF
    EXPECT_NE(refusal("{\"id\":\"k\",\"v\":\"a\tb\"}"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":"\ud800"})"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":"\udc00x"})"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":"\q"})"), "");
    EXPECT_EQ(refusal(R"({"id":"k","v":"a\tb\u0001\"\\\/"})"), "");
}

TEST(DynamoDbRecord, RefusesNamesDynamoDbRefuses) {
    EXPECT_NE(refusal(R"({"id":"k","":1})").find("attribute name is empty"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":"k","m":{"":1}})").find("map key is empty"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":"k","v":1,"v":2})").find("appears twice"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":"k","m":{"a":1,"a":1}})").find("appears twice"), std::string::npos);
    const std::string long_name(64 * 1024 + 1, 'n');
    EXPECT_NE(refusal(R"({"id":"k",")" + long_name + R"(":1})").find("at most 65536"),
              std::string::npos);
    EXPECT_EQ(refusal(R"({"id":"k",")" + std::string(64 * 1024, 'n') + R"(":1})"), "");
}

TEST(DynamoDbRecord, KeyAttributesFollowTheTable) {
    EXPECT_NE(refusal(R"({"v":1})").find("partition key attribute \"id\" is missing"),
              std::string::npos);
    EXPECT_NE(refusal(R"({"id":null})").find("is null"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":""})").find("empty string"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":true})").find("a boolean"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":{"a":1}})").find("an object"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":["a"]})").find("an array"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":1})").find("must be a string (the table declares it S)"),
              std::string::npos);
    EXPECT_NE(refusal(R"({"pk":"p","sk":"1"})", kNumberSort)
                  .find("must be a number (the table declares it N)"),
              std::string::npos);
    EXPECT_NE(refusal(R"({"pk":"p"})", kNumberSort).find("sort key attribute \"sk\" is missing"),
              std::string::npos);
    // Key sizes are bytes, not characters.
    EXPECT_EQ(refusal(R"({"id":")" + std::string(2048, 'k') + R"("})"), "");
    EXPECT_NE(refusal(R"({"id":")" + std::string(2049, 'k') + R"("})").find("at most 2048"),
              std::string::npos);
    std::string two_byte;
    for (int i = 0; i < 1024; ++i) {
        two_byte += "\xC3\xA9";
    }
    EXPECT_EQ(refusal("{\"id\":\"" + two_byte + "\"}"), "");
    EXPECT_NE(refusal("{\"id\":\"" + two_byte + "\xC3\xA9\"}"), "");
    EXPECT_EQ(refusal(R"({"pk":"p","sk":")" + std::string(1024, 's') + R"("})", kComposite), "");
    EXPECT_NE(refusal(R"({"pk":"p","sk":")" + std::string(1025, 's') + R"("})", kComposite)
                  .find("at most 1024"),
              std::string::npos);
    // A binary key cannot come from JSON.
    const DynamoDbKeySchema binary{DynamoDbKeyAttribute{"id", DynamoDbKeyType::B}, std::nullopt};
    EXPECT_NE(refusal(R"({"id":"aGk="})", binary).find("binary"), std::string::npos);
}

TEST(DynamoDbRecord, NumbersOutsideDynamoDbAreRefusedAnywhere) {
    EXPECT_NE(refusal(R"({"id":"k","v":123456789012345678901234567890123456789})").find("38"),
              std::string::npos);
    EXPECT_NE(refusal(R"({"id":"k","m":{"a":[1E+126]}})").find("range"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":"k","v":1e-131})").find("range"), std::string::npos);
    EXPECT_EQ(refusal(R"({"id":"k","v":9.9999999999999999999999999999999999999E+125})"), "");
}

TEST(DynamoDbRecord, NestingStopsAt31LevelsBelowTheAttribute) {
    EXPECT_EQ(refusal(R"({"id":"k","v":)" + nested_maps(31, R"("leaf")") + "}"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":)" + nested_maps(32, R"("leaf")") + "}")
                  .find("nested more than 31"),
              std::string::npos);
    EXPECT_EQ(refusal(R"({"id":"k","v":)" + nested_lists(31, "1") + "}"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":)" + nested_lists(32, "1") + "}"), "");
    // An empty container at level 32 is refused too, as DynamoDB Local does.
    EXPECT_NE(refusal(R"({"id":"k","v":)" + nested_maps(31, "{}") + "}"), "");
    EXPECT_NE(refusal(R"({"id":"k","v":)" + nested_maps(16, nested_lists(16, "1")) + "}"), "");
}

TEST(DynamoDbItemSize, FollowsTheDocumentedRules) {
    // The documentation's example: "shirt-color":"R" and "shirt-size":"M" is 23 bytes.
    const DynamoDbKeySchema shirt{DynamoDbKeyAttribute{"shirt-color", DynamoDbKeyType::S},
                                  std::nullopt};
    EXPECT_EQ(prepare_dynamodb_item(R"({"shirt-color":"R","shirt-size":"M"})", shirt).item_bytes,
              23u);
    // pk "s" (2 + 1), sk N 1 (2 + 2), attribute "v" (1 + L): 8 + L.
    const auto with_string = [](std::size_t len) {
        return R"({"pk":"s","sk":1,"v":")" + std::string(len, 'x') + R"("})";
    };
    EXPECT_EQ(prepare_dynamodb_item(with_string(409592), kNumberSort).item_bytes, 409600u);
    EXPECT_NE(refusal(with_string(409593), kNumberSort).find("409601 bytes"), std::string::npos);
    // A number split by its point costs a byte more than the approximation.
    const auto with_number = [](const std::string& n, std::size_t len) {
        return R"({"pk":"s","sk":1,"n":)" + n + R"(,"v":")" + std::string(len, 'x') + R"("})";
    };
    EXPECT_EQ(prepare_dynamodb_item(with_number("1.5", 409588), kNumberSort).item_bytes, 409600u);
    EXPECT_NE(refusal(with_number("1.5", 409589), kNumberSort), "");
    // A map: 3 bytes, and each member 1 byte plus its name; a list: 3, and 1 per element.
    EXPECT_EQ(prepare_dynamodb_item(R"({"id":"k","m":{"a":"xy"}})", kStringKey).item_bytes,
              (2u + 1u) + (1u + 3u + (1u + 1u + 2u)));
    EXPECT_EQ(prepare_dynamodb_item(R"({"id":"k","l":[true,null,"z"]})", kStringKey).item_bytes,
              (2u + 1u) + (1u + 3u + (1u + 1u) + (1u + 1u) + (1u + 1u)));
}

// The request bound is never below the JSON the SDK writes for the item, which
// it indents with a tab per level.
TEST(DynamoDbItemSize, RequestBytesBoundTheSdksJson) {
    using AV = Aws::DynamoDB::Model::AttributeValue;
    std::function<AV(const DynamoDbValue&)> to_sdk = [&](const DynamoDbValue& v) {
        AV av;
        switch (v.kind) {
            case DynamoDbValue::Kind::S:
                av.SetS(v.text);
                break;
            case DynamoDbValue::Kind::N:
                av.SetN(v.text);
                break;
            case DynamoDbValue::Kind::Bool:
                av.SetBool(v.flag);
                break;
            case DynamoDbValue::Kind::Null:
                av.SetNull(true);
                break;
            case DynamoDbValue::Kind::M:
                for (const auto& [name, m] : v.members) {
                    av.AddMEntry(name, Aws::MakeShared<AV>("t", to_sdk(m)));
                }
                if (v.members.empty()) {
                    av.SetM(Aws::Map<Aws::String, const std::shared_ptr<AV>>{});
                }
                break;
            case DynamoDbValue::Kind::L:
                for (const auto& [name, m] : v.members) {
                    av.AddLItem(Aws::MakeShared<AV>("t", to_sdk(m)));
                }
                if (v.members.empty()) {
                    av.SetL(Aws::Vector<std::shared_ptr<AV>>{});
                }
                break;
        }
        return av;
    };
    const auto sdk_bytes = [&](const clink::aws::DynamoDbItem& item) {
        Aws::DynamoDB::Model::PutRequest put;
        for (const auto& [name, v] : item) {
            put.AddItem(name, to_sdk(v));
        }
        Aws::DynamoDB::Model::WriteRequest w;
        w.SetPutRequest(put);
        Aws::DynamoDB::Model::BatchWriteItemRequest two;
        two.AddRequestItems("t", Aws::Vector<Aws::DynamoDB::Model::WriteRequest>{w, w});
        Aws::DynamoDB::Model::BatchWriteItemRequest one;
        one.AddRequestItems("t", Aws::Vector<Aws::DynamoDB::Model::WriteRequest>{w});
        return two.SerializePayload().size() - one.SerializePayload().size();
    };
    std::string escaped;
    for (int i = 0; i < 1000; ++i) {
        escaped += "\\u0001";
    }
    for (
        const std::string& record : {
            std::string(
                R"({"id":"k\"\\\u0001\u001f","n":-12.5,"t":true,"z":null,"m":{"q\u0002":["\u00e9",{}]}})"),
            std::string(R"({"id":"k","v":")") + escaped + "\"}",
            std::string(R"({"id":"k","l":[1,2,3,[4,[5,[6]]]],"e":[],"o":{}})"),
            std::string(R"({"id":"k","v":)") + nested_maps(31, R"("leaf")") + "}",
            std::string(R"({"id":"k","v":)") + nested_lists(30, "[]") + "}",
        }) {
        const auto p = prepare_dynamodb_item(record, kStringKey);
        EXPECT_LE(sdk_bytes(p.item), p.request_bytes) << record.substr(0, 80);
    }
}
