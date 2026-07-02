#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/sql/row.hpp"
#include "clink/vector_search/distance_kernels.hpp"
#include "clink/vector_search/knn_index.hpp"
#include "clink/vector_search/vector_search_operator.hpp"

namespace clink::vector_search {
namespace {

// Process-global corpus the vs_test_corpus source factory reads, so the test can change
// the corpus between builds without re-registering the factory (repeat-safe).
struct CorpusState {
    std::string doc = "a";
    std::vector<double> vec{1.0, 0.0};
};
CorpusState& corpus_state() {
    static CorpusState s;
    return s;
}

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

// The corpus_refresh_ms knob rebuilds the in-memory index inline when the interval has
// elapsed, so a changed reference table is picked up without a job restart.
TEST(VectorSearchOperator, CorpusRefreshPicksUpChangedCorpus) {
    using clink::sql::Row;

    // Register (once) a source that yields the current process-global corpus.
    auto& reg = clink::cluster::OperatorRegistry::default_instance();
    if (reg.find_source("vs_test_corpus", std::string{"row"}) == nullptr) {
        reg.register_source(
            "vs_test_corpus",
            clink::cluster::SourceFactory{
                std::string{"row"},
                [](const clink::cluster::OperatorBuildContext&) -> std::shared_ptr<void> {
                    Row r;
                    clink::config::JsonArray vec;
                    for (double v : corpus_state().vec) {
                        vec.push_back(clink::config::JsonValue{v});
                    }
                    r.values["vec"] = clink::config::JsonValue{std::move(vec)};
                    r.values["doc"] = clink::config::JsonValue{corpus_state().doc};
                    std::vector<clink::Record<Row>> rows;
                    rows.emplace_back(std::move(r));
                    std::shared_ptr<clink::Source<Row>> src =
                        std::make_shared<clink::VectorSource<Row>>(std::move(rows),
                                                                   "vs_test_corpus");
                    return src;
                }});
    }

    corpus_state().doc = "a";
    corpus_state().vec = {1.0, 0.0};

    VectorSearchOperator::Config cfg;
    cfg.source_factory = "vs_test_corpus";
    cfg.query_column = "q";
    cfg.index_column = "vec";
    cfg.vector_columns = {"doc"};
    cfg.top_k = 1;
    cfg.corpus_refresh_ms = 1;
    cfg.index.kind = "flat";
    cfg.index.metric = Metric::Cosine;
    cfg.index.dim = 2;
    VectorSearchOperator op(std::move(cfg));
    op.open();  // builds corpus A (doc "a")

    auto query_element = []() {
        Row r;
        clink::config::JsonArray q;
        q.push_back(clink::config::JsonValue{1.0});
        q.push_back(clink::config::JsonValue{0.1});
        r.values["q"] = clink::config::JsonValue{std::move(q)};
        clink::Batch<Row> b;
        b.push(clink::Record<Row>{std::move(r)});
        return clink::StreamElement<Row>::data(std::move(b));
    };
    auto drain_docs = [](clink::BoundedChannel<clink::StreamElement<Row>>& ch) {
        std::vector<std::string> docs;
        while (auto e = ch.try_pop()) {
            if (e->is_data()) {
                for (const auto& rec : e->as_data()) {
                    docs.push_back(rec.value().values.at("doc").as_string());
                }
            }
        }
        return docs;
    };

    // First query: corpus A -> doc "a".
    clink::BoundedChannel<clink::StreamElement<Row>> ch1(16);
    clink::Emitter<Row> em1(&ch1);
    auto q1 = query_element();
    op.process(q1, em1);
    auto d1 = drain_docs(ch1);
    ASSERT_EQ(d1.size(), 1U);
    EXPECT_EQ(d1[0], "a");

    // Change the corpus, wait past the refresh interval, query again: the operator
    // rebuilds the index inline and now searches corpus B -> doc "b".
    corpus_state().doc = "b";
    corpus_state().vec = {0.0, 1.0};
    std::this_thread::sleep_for(std::chrono::milliseconds{5});

    clink::BoundedChannel<clink::StreamElement<Row>> ch2(16);
    clink::Emitter<Row> em2(&ch2);
    auto q2 = query_element();
    op.process(q2, em2);
    auto d2 = drain_docs(ch2);
    ASSERT_EQ(d2.size(), 1U);
    EXPECT_EQ(d2[0], "b");
}

}  // namespace
}  // namespace clink::vector_search
