#include "clink/vector_search/knn_index.hpp"

#include <algorithm>
#include <cstdio>
#include <utility>

#if defined(CLINK_VECTOR_HNSW)
#include <usearch/index_dense.hpp>
#endif

namespace clink::vector_search {

namespace {

// A resident row-major corpus matrix + the exact brute-force scan. Also used by the
// HNSW index to (a) recompute canonical scores for its candidates and (b) back a
// fallback when a query dim mismatches.
class CorpusMatrix {
public:
    void set(std::vector<std::vector<float>> vectors, std::size_t dim) {
        dim_ = dim;
        rows_ = vectors.size();
        data_.clear();
        data_.reserve(rows_ * dim_);
        for (auto& v : vectors) {
            // Rows that are not exactly dim were filtered by the operator; guard anyway.
            if (v.size() == dim_) {
                data_.insert(data_.end(), v.begin(), v.end());
            } else {
                data_.resize(data_.size() + dim_, 0.0F);
            }
        }
    }
    [[nodiscard]] const float* row(std::size_t i) const { return data_.data() + (i * dim_); }
    [[nodiscard]] std::size_t rows() const { return rows_; }
    [[nodiscard]] std::size_t dim() const { return dim_; }

private:
    std::vector<float> data_;
    std::size_t dim_ = 0;
    std::size_t rows_ = 0;
};

std::vector<Neighbour> topk_from_scores(std::vector<Neighbour> all, Metric metric, std::size_t k) {
    const std::size_t kk = std::min(k, all.size());
    std::partial_sort(all.begin(),
                      all.begin() + static_cast<std::ptrdiff_t>(kk),
                      all.end(),
                      [metric](const Neighbour& a, const Neighbour& b) {
                          return metric_nearer(metric, a.score, b.score);
                      });
    all.resize(kk);
    return all;
}

class FlatIndex final : public KnnIndex {
public:
    explicit FlatIndex(Metric metric) : metric_(metric) {}

    void build(std::vector<std::vector<float>> vectors) override {
        std::size_t dim = 0;
        for (const auto& v : vectors) {
            if (!v.empty()) {
                dim = v.size();
                break;
            }
        }
        corpus_.set(std::move(vectors), dim);
    }

    [[nodiscard]] std::vector<Neighbour> search(const float* query,
                                                std::size_t dim,
                                                std::size_t k) const override {
        if (dim != corpus_.dim() || corpus_.rows() == 0) {
            return {};
        }
        std::vector<Neighbour> all;
        all.reserve(corpus_.rows());
        for (std::size_t i = 0; i < corpus_.rows(); ++i) {
            all.push_back(Neighbour{i, distance(metric_, query, corpus_.row(i), dim)});
        }
        return topk_from_scores(std::move(all), metric_, k);
    }

    [[nodiscard]] bool is_approximate() const noexcept override { return false; }

private:
    Metric metric_;
    CorpusMatrix corpus_;
};

#if defined(CLINK_VECTOR_HNSW)

// usearch HNSW graph over the corpus. usearch finds candidate row indices
// approximately; we recompute the canonical score via distance() on the resident
// corpus so the emitted `score` is identical to the flat path's.
class HnswIndex final : public KnnIndex {
public:
    explicit HnswIndex(const IndexParams& params) : params_(params), metric_(params.metric) {}

    void build(std::vector<std::vector<float>> vectors) override {
        std::size_t dim = params_.dim;
        if (dim == 0) {
            for (const auto& v : vectors) {
                if (!v.empty()) {
                    dim = v.size();
                    break;
                }
            }
        }
        const std::size_t rows = vectors.size();
        corpus_.set(std::move(vectors), dim);

        using namespace unum::usearch;
        const metric_kind_t mk = metric_ == Metric::L2    ? metric_kind_t::l2sq_k
                                 : metric_ == Metric::Dot ? metric_kind_t::ip_k
                                                          : metric_kind_t::cos_k;
        metric_punned_t metric(dim, mk, scalar_kind_t::f32_k);
        index_dense_config_t config(params_.m, params_.ef_construction, params_.ef_search);
        index_ = index_dense_t::make(metric, config);
        index_.reserve(rows);
        for (std::size_t i = 0; i < corpus_.rows(); ++i) {
            index_.add(static_cast<default_key_t>(i), corpus_.row(i));
        }
    }

    [[nodiscard]] std::vector<Neighbour> search(const float* query,
                                                std::size_t dim,
                                                std::size_t k) const override {
        if (dim != corpus_.dim() || corpus_.rows() == 0) {
            return {};
        }
        auto results = index_.search(query, k);
        std::vector<Neighbour> hits;
        hits.reserve(results.size());
        for (std::size_t i = 0; i < results.size(); ++i) {
            const auto row = static_cast<std::size_t>(results[i].member.key);
            hits.push_back(Neighbour{row, distance(metric_, query, corpus_.row(row), dim)});
        }
        // usearch orders by its own distance; re-rank by the canonical score so ties
        // and the emitted order match the flat path exactly.
        return topk_from_scores(std::move(hits), metric_, k);
    }

    [[nodiscard]] bool is_approximate() const noexcept override { return true; }

private:
    IndexParams params_;
    Metric metric_;
    CorpusMatrix corpus_;
    unum::usearch::index_dense_t index_;
};

#endif  // CLINK_VECTOR_HNSW

}  // namespace

std::unique_ptr<KnnIndex> make_knn_index(const IndexParams& params, std::size_t corpus_rows) {
    const bool want_hnsw = params.kind == "hnsw" ||
                           (params.kind == "auto" && corpus_rows >= params.hnsw_auto_threshold);
#if defined(CLINK_VECTOR_HNSW)
    if (want_hnsw) {
        return std::make_unique<HnswIndex>(params);
    }
#else
    if (params.kind == "hnsw") {
        std::fprintf(stderr,
                     "vector_search: index='hnsw' requested but this build has no usearch; "
                     "using exact brute-force flat search\n");
    }
#endif
    return std::make_unique<FlatIndex>(params.metric);
}

}  // namespace clink::vector_search
