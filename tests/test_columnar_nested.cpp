// Contract tests for the recursive columnar batcher: nested structs and
// containers (std::vector -> list, std::map -> map, std::optional -> nullable,
// nested CLINK_ARROW_FIELDS struct -> struct), round-tripped over the real
// Arrow IPC wire path.

#ifndef CLINK_HAS_ARROW
#error "test_columnar_nested requires Arrow"
#endif

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"

using namespace clink;

// Leaf nested struct - must ALSO be described.
struct NestAddr {
    std::string city;
    std::int32_t zip;
};
CLINK_ARROW_FIELDS(NestAddr, city, zip);

// Carries a nested struct field.
struct NestPerson {
    std::int64_t id;
    std::string name;
    NestAddr addr;
};
CLINK_ARROW_FIELDS(NestPerson, id, name, addr);

// Carries every container shape, including a list-of-struct.
struct NestBag {
    std::int64_t id;
    std::vector<std::int64_t> nums;
    std::optional<std::string> note;
    std::map<std::string, std::int64_t> counts;
    std::vector<NestAddr> addrs;
};
CLINK_ARROW_FIELDS(NestBag, id, nums, note, counts, addrs);

namespace {

template <typename T>
std::optional<Batch<T>> wire_round_trip(const ArrowBatcher<T>& batcher, const Batch<T>& in) {
    auto rb = batcher.build(in);
    if (rb == nullptr) {
        return std::nullopt;
    }
    auto ipc = arrow_batch_to_ipc(*rb);
    auto rb2 = arrow_batch_from_ipc(ipc.data(), ipc.size());
    if (rb2 == nullptr) {
        return std::nullopt;
    }
    return batcher.parse(*rb2);
}

bool addr_eq(const NestAddr& a, const NestAddr& b) {
    return a.city == b.city && a.zip == b.zip;
}

}  // namespace

TEST(ColumnarNested, NestedStructSchemaAndRoundTrip) {
    auto batcher = make_columnar_arrow_batcher<NestPerson>();
    auto schema = batcher.schema();
    // event_time, id, name, addr
    ASSERT_EQ(schema->num_fields(), 4);
    EXPECT_EQ(schema->field(3)->name(), "addr");
    ASSERT_EQ(schema->field(3)->type()->id(), arrow::Type::STRUCT);
    EXPECT_EQ(schema->field(3)->type()->num_fields(), 2);  // city, zip
    EXPECT_EQ(schema->field(3)->type()->field(0)->name(), "city");
    EXPECT_EQ(schema->field(3)->type()->field(1)->name(), "zip");

    Batch<NestPerson> in;
    in.emplace(NestPerson{1, "Ada", NestAddr{"London", 1}}, EventTime{1000});
    in.emplace(NestPerson{2, "Bo", NestAddr{"Paris", 2}});

    auto out = wire_round_trip(batcher, in);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 2u);
    EXPECT_EQ((*out)[0].value().id, 1);
    EXPECT_EQ((*out)[0].value().name, "Ada");
    EXPECT_TRUE(addr_eq((*out)[0].value().addr, NestAddr{"London", 1}));
    EXPECT_TRUE(addr_eq((*out)[1].value().addr, NestAddr{"Paris", 2}));
    EXPECT_EQ((*out)[0].event_time().value().millis(), 1000);
    EXPECT_FALSE((*out)[1].event_time().has_value());
}

TEST(ColumnarNested, ContainersSchema) {
    auto schema = make_columnar_arrow_batcher<NestBag>().schema();
    // event_time, id, nums, note, counts, addrs
    ASSERT_EQ(schema->num_fields(), 6);
    EXPECT_EQ(schema->field(2)->name(), "nums");
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::LIST);
    EXPECT_EQ(schema->field(3)->name(), "note");
    EXPECT_EQ(schema->field(3)->type()->id(), arrow::Type::STRING);
    EXPECT_TRUE(schema->field(3)->nullable());   // optional -> nullable
    EXPECT_FALSE(schema->field(2)->nullable());  // vector is not nullable
    EXPECT_EQ(schema->field(4)->name(), "counts");
    EXPECT_EQ(schema->field(4)->type()->id(), arrow::Type::MAP);
    EXPECT_EQ(schema->field(5)->name(), "addrs");
    ASSERT_EQ(schema->field(5)->type()->id(), arrow::Type::LIST);
    // list<struct>
    EXPECT_EQ(schema->field(5)->type()->field(0)->type()->id(), arrow::Type::STRUCT);
}

TEST(ColumnarNested, ContainersRoundTrip) {
    auto batcher = make_columnar_arrow_batcher<NestBag>();

    Batch<NestBag> in;
    in.emplace(NestBag{1,
                       {10, 20, 30},
                       std::optional<std::string>{"hi"},
                       {{"a", 1}, {"b", 2}},
                       {NestAddr{"London", 1}, NestAddr{"Paris", 2}}},
               EventTime{500});
    // Second row exercises empty containers and a null optional.
    in.emplace(NestBag{2, {}, std::nullopt, {}, {}});

    auto out = wire_round_trip(batcher, in);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 2u);

    const auto& r0 = (*out)[0].value();
    EXPECT_EQ(r0.id, 1);
    EXPECT_EQ(r0.nums, (std::vector<std::int64_t>{10, 20, 30}));
    ASSERT_TRUE(r0.note.has_value());
    EXPECT_EQ(*r0.note, "hi");
    EXPECT_EQ(r0.counts, (std::map<std::string, std::int64_t>{{"a", 1}, {"b", 2}}));
    ASSERT_EQ(r0.addrs.size(), 2u);
    EXPECT_TRUE(addr_eq(r0.addrs[0], NestAddr{"London", 1}));
    EXPECT_TRUE(addr_eq(r0.addrs[1], NestAddr{"Paris", 2}));

    const auto& r1 = (*out)[1].value();
    EXPECT_EQ(r1.id, 2);
    EXPECT_TRUE(r1.nums.empty());
    EXPECT_FALSE(r1.note.has_value());
    EXPECT_TRUE(r1.counts.empty());
    EXPECT_TRUE(r1.addrs.empty());
}
