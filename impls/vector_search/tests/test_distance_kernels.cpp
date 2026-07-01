#include <vector>

#include <gtest/gtest.h>

#include "clink/vector_search/distance_kernels.hpp"
#include "clink/vector_search/knn_index.hpp"

namespace clink::vector_search {
namespace {

TEST(VectorDistance, DotProduct) {
    const std::vector<float> a{1, 2, 3};
    const std::vector<float> b{4, 5, 6};
    EXPECT_FLOAT_EQ(distance(Metric::Dot, a.data(), b.data(), 3), 32.0F);  // 4 + 10 + 18
}

TEST(VectorDistance, L2Squared) {
    const std::vector<float> a{1, 2, 3};
    const std::vector<float> b{1, 2, 5};
    EXPECT_FLOAT_EQ(distance(Metric::L2, a.data(), b.data(), 3), 4.0F);  // 0 + 0 + 2^2
}

TEST(VectorDistance, CosineOfParallelVectorsIsOne) {
    const std::vector<float> a{1, 2, 3};
    const std::vector<float> b{2, 4, 6};  // same direction
    EXPECT_NEAR(distance(Metric::Cosine, a.data(), b.data(), 3), 1.0F, 1e-5);
}

TEST(VectorDistance, MetricNearerDirection) {
    EXPECT_TRUE(metric_nearer(Metric::Cosine, 0.9F, 0.5F));  // higher similarity = nearer
    EXPECT_TRUE(metric_nearer(Metric::Dot, 10.0F, 5.0F));
    EXPECT_TRUE(metric_nearer(Metric::L2, 1.0F, 5.0F));  // lower distance = nearer
}

TEST(KnnIndex, FlatExactTopK) {
    IndexParams p;
    p.metric = Metric::L2;
    p.dim = 2;
    p.kind = "flat";
    auto idx = make_knn_index(p, 4);
    idx->build({{0, 0}, {1, 1}, {5, 5}, {10, 10}});
    const std::vector<float> q{0.9F, 0.9F};
    const auto hits = idx->search(q.data(), 2, 2);
    ASSERT_EQ(hits.size(), 2U);
    EXPECT_EQ(hits[0].row_index, 1U);  // (1,1) nearest
    EXPECT_EQ(hits[1].row_index, 0U);  // (0,0) next
    EXPECT_FALSE(idx->is_approximate());
}

TEST(KnnIndex, CosineRanksBySimilarity) {
    IndexParams p;
    p.metric = Metric::Cosine;
    p.dim = 2;
    p.kind = "flat";
    auto idx = make_knn_index(p, 3);
    idx->build({{1, 0}, {0, 1}, {1, 1}});
    const std::vector<float> q{1, 0};
    const auto hits = idx->search(q.data(), 2, 1);
    ASSERT_EQ(hits.size(), 1U);
    EXPECT_EQ(hits[0].row_index, 0U);  // identical direction
    EXPECT_NEAR(hits[0].score, 1.0F, 1e-5);
}

TEST(KnnIndex, DimMismatchReturnsEmpty) {
    IndexParams p;
    p.metric = Metric::L2;
    p.dim = 3;
    p.kind = "flat";
    auto idx = make_knn_index(p, 2);
    idx->build({{1, 2, 3}, {4, 5, 6}});
    const std::vector<float> q{1, 2};  // wrong dim
    EXPECT_TRUE(idx->search(q.data(), 2, 1).empty());
}

// kind='hnsw' exercises the usearch path when the build has it, else the documented
// flat fallback. The nearest neighbour is correct either way (usearch re-ranks by the
// canonical score), so this test is portable across builds with/without usearch.
TEST(KnnIndex, HnswRequestFindsNearest) {
    IndexParams p;
    p.metric = Metric::L2;
    p.dim = 2;
    p.kind = "hnsw";
    auto idx = make_knn_index(p, 4);
    idx->build({{0, 0}, {1, 1}, {5, 5}, {10, 10}});
    const std::vector<float> q{0.9F, 0.9F};
    const auto hits = idx->search(q.data(), 2, 2);
    ASSERT_GE(hits.size(), 1U);
    EXPECT_EQ(hits[0].row_index, 1U);  // (1,1) nearest
}

}  // namespace
}  // namespace clink::vector_search
