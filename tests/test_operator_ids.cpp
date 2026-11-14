// Operator ids: the stable hash every id is derived from, and the two
// standard-library hashes ids were derived from before it, which a restore
// needs to recognise state written by either kind of build.

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/legacy_operator_ids.hpp"
#include "clink/core/types.hpp"

namespace {

// The inputs the golden vectors below were produced from, by each standard
// library's own std::hash<std::string>: one of every length class each
// implementation branches on (0, 1-3, 4-8, 9-16, 17-32, 33-64 and over 64
// bytes), the shapes of the old id strings, and non-ASCII bytes.
std::vector<std::string> inputs() {
    std::vector<std::string> v = {"",
                                  "a",
                                  "ab",
                                  "abc",
                                  "abcd",
                                  "uid/x",
                                  "uid/abc",
                                  "uid/abcd",
                                  "stage0/x",
                                  "uid/uc-windows",
                                  "stage3/aggregate_row",
                                  "uid/source-orders",
                                  "stage12/equi_join_row",
                                  "uid/an-operator-uid-of-32-bytes!",
                                  "uid/a-uid-that-is-thirty-three-b",
                                  "stage7/s3_agg_14/aggregate_row_with_a_long_suffix_x",
                                  "uid/\xc3\xa9t\xc3\xa9-\xe2\x82\xac-\xf0\x9f\x98\x80"};
    v.emplace_back(64, 'q');
    v.emplace_back(65, 'r');
    std::string s100;
    for (int i = 0; i < 100; ++i) {
        s100.push_back(static_cast<char>('a' + i % 26));
    }
    v.push_back(s100);
    std::string s200;
    for (int i = 0; i < 200; ++i) {
        s200.push_back(static_cast<char>(i * 37 % 256));
    }
    v.push_back(s200);
    return v;
}

// {libstdc++, libc++} std::hash<std::string> of each input above: libstdc++
// from g++ 14 in the Linux runtime image, libc++ from Apple clang 17 on macOS.
const std::vector<std::pair<std::uint64_t, std::uint64_t>> kStdHash = {
    {6142509188972423790ULL, 11160318154034397263ULL},
    {4993892634952068459ULL, 2603192927274642682ULL},
    {5498234120169091963ULL, 1725057946192985918ULL},
    {3663726644998027833ULL, 4220206313085259313ULL},
    {16030370620903852840ULL, 8979402121050354847ULL},
    {5815550502990584418ULL, 11024357795811570155ULL},
    {1173558631032745668ULL, 6402130747351992358ULL},
    {11201035602844846360ULL, 8317671914049403575ULL},
    {824454677933409510ULL, 10339340543094255317ULL},
    {17940908319813556766ULL, 14756459749213163393ULL},
    {2501231760621152227ULL, 17683742569720382036ULL},
    {6584326599597017206ULL, 381930014523995938ULL},
    {439018794306718698ULL, 5752125128810584219ULL},
    {11299837173174775295ULL, 18084790256878909349ULL},
    {5911154994930403268ULL, 15815991606831597252ULL},
    {4724570170796283333ULL, 17191201873519843855ULL},
    {10396876605128759165ULL, 5716355276213934475ULL},
    {5152969690474882659ULL, 18143039714411685758ULL},
    {12504868937904711646ULL, 3415067544536799094ULL},
    {15558739239578664168ULL, 3974768800134596195ULL},
    {2014520745282045048ULL, 2971531832397742856ULL},
};

}  // namespace

TEST(OperatorIds, TheStableHashIsFrozen) {
    // Values from an independent FNV-1a 64; a change here orphans every
    // checkpoint taken since the stable ids, so it must never pass review.
    EXPECT_EQ(clink::stable_operator_id_hash("uid/uc-windows"), 1175189170853880542ULL);
    EXPECT_EQ(clink::stable_operator_id_hash("node/agg_3"), 17741236947412870517ULL);
    EXPECT_EQ(clink::stable_operator_id_hash("stage0/kafka_source"), 10442350363036713214ULL);
    EXPECT_EQ(clink::stable_operator_id_hash("uid/"), 4138451697354885222ULL);
    EXPECT_EQ(clink::stable_operator_id_hash(""), 14695981039346656037ULL);
    EXPECT_EQ(clink::stable_operator_id("node/agg_3").value(), 17741236947412870517ULL);
    static_assert(clink::stable_operator_id_hash("uid/") == 4138451697354885222ULL);
}

TEST(OperatorIds, BothLegacyHashesMatchEachLibrarysStdHash) {
    const auto in = inputs();
    ASSERT_EQ(in.size(), kStdHash.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        EXPECT_EQ(clink::legacy_ids::libstdcxx_string_hash(in[i]), kStdHash[i].first)
            << "libstdc++, input of " << in[i].size() << " bytes";
        EXPECT_EQ(clink::legacy_ids::libcxx_string_hash(in[i]), kStdHash[i].second)
            << "libc++, input of " << in[i].size() << " bytes";
    }
}

// The pinned vectors come from two toolchains; this checks the reimplementation
// of the library this test is built with against its std::hash directly, on
// every input, so a library that changed its hash would show here first.
TEST(OperatorIds, TheNativeLegacyHashIsThisBuildsStdHash) {
    for (const auto& s : inputs()) {
#if defined(_LIBCPP_VERSION)
        EXPECT_EQ(clink::legacy_ids::libcxx_string_hash(s), std::hash<std::string>{}(s))
            << s.size();
#elif defined(__GLIBCXX__)
        EXPECT_EQ(clink::legacy_ids::libstdcxx_string_hash(s), std::hash<std::string>{}(s))
            << s.size();
#else
        GTEST_SKIP() << "neither libc++ nor libstdc++";
#endif
    }
}

TEST(OperatorIds, LegacyIdsAreBothLibrariesOfTheOldString) {
    // "uid/uc-windows", the upgrade example's window operator, as each kind of
    // build stored it.
    const auto ids = clink::legacy_ids::legacy_operator_ids("uid/uc-windows");
    EXPECT_EQ(ids[0].value(), 17940908319813556766ULL);
    EXPECT_EQ(ids[1].value(), 14756459749213163393ULL);
}
