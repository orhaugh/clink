#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/vector_search/distance_kernels.hpp"

// The KNN index behind VECTOR_SEARCH: one interface, two implementations chosen at
// build/runtime. FlatIndex is an exact brute-force scan (the SimSIMD distance kernel
// per query x corpus). HnswIndex (compiled when CLINK_VECTOR_HNSW is defined) wraps
// usearch's approximate HNSW graph for large corpora. Both keep the corpus matrix
// resident and report the SAME canonical metric value as `score`, so the emitted
// column is identical whichever index served the query. usearch headers stay PRIVATE
// to the impl .cpp (no third-party include here, no plugin-ABI impact).

namespace clink::vector_search {

struct IndexParams {
    Metric metric = Metric::Cosine;
    std::size_t dim = 0;
    std::string kind = "auto";  // 'flat' | 'hnsw' | 'auto'
    // HNSW build/search knobs (ignored by flat).
    std::size_t m = 16;
    std::size_t ef_construction = 128;
    std::size_t ef_search = 64;
    // 'auto' flips flat -> hnsw at/above this corpus size (when usearch is present).
    std::size_t hnsw_auto_threshold = 50000;
};

struct Neighbour {
    std::size_t row_index = 0;  // index into the corpus, for payload lookup
    float score = 0.0F;         // the canonical metric value (see distance())
};

class KnnIndex {
public:
    virtual ~KnnIndex() = default;
    // Build over the corpus (vectors[i] is the i-th corpus embedding, all of dim
    // params.dim; ragged / wrong-dim rows are the caller's responsibility to filter).
    virtual void build(std::vector<std::vector<float>> vectors) = 0;
    // Up to k nearest neighbours of `query`, best first. Empty if dim mismatches.
    [[nodiscard]] virtual std::vector<Neighbour> search(const float* query,
                                                        std::size_t dim,
                                                        std::size_t k) const = 0;
    [[nodiscard]] virtual bool is_approximate() const noexcept = 0;
};

// Pick the implementation from params.kind + corpus size. 'hnsw' requested on a build
// without usearch logs a warning and falls back to exact flat.
std::unique_ptr<KnnIndex> make_knn_index(const IndexParams& params, std::size_t corpus_rows);

}  // namespace clink::vector_search
