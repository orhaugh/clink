#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/vector_search/knn_index.hpp"

// The vector_search_row operator: a SYNCHRONOUS flatmap (in-memory KNN is CPU-bound,
// not I/O-bound, so it is not on the async operator path). At open() it bounded-drains
// the vector table into an in-memory index (the ANALYZE-style local scan); at process
// it emits, per input row, its top_k nearest corpus rows plus a `score`. The corpus
// index is derived state - rebuilt on restart from the bounded corpus, not checkpointed.
// By default the corpus is fixed at open(); set corpus_refresh_ms > 0 to periodically
// re-scan and rebuild it inline (on the operator thread, when the next row arrives after
// the interval elapses) so a slowly-changing reference table is picked up without a
// restart. A remote / incrementally-mutable vector index remains an async follow-on.

namespace clink::vector_search {

class VectorSearchOperator final : public clink::Operator<clink::sql::Row, clink::sql::Row> {
public:
    struct Config {
        std::string source_factory;                        // e.g. "file_json_source"
        std::map<std::string, std::string> source_params;  // corpus source build params
        std::string query_column;                          // embedding column in the input row
        std::string index_column;                          // embedding column in the corpus
        std::vector<std::string> vector_columns;           // corpus columns to attach to output
        std::string score_column = "score";
        std::size_t top_k = 10;
        std::int64_t corpus_refresh_ms = 0;  // 0 = never refresh (corpus fixed at open)
        IndexParams index;                   // metric / kind / dim / hnsw knobs
        // Optional metadata equality pre-filter: each pair is (query_column,
        // corpus_column). A corpus row is a candidate for a query row only if, for
        // every pair, the query column is null/absent OR equals the corpus column.
        // When set, the operator scores the FILTERED corpus subset EXACTLY (a true
        // pre-filter over the distance kernel), so scoping retrieval by metadata does
        // not lose recall the way post-filtering a top-k would.
        std::vector<std::pair<std::string, std::string>> filter_eq;
    };

    explicit VectorSearchOperator(Config cfg) : cfg_(std::move(cfg)) {}

    void open() override;
    void process(const clink::StreamElement<clink::sql::Row>& element,
                 clink::Emitter<clink::sql::Row>& out) override;
    [[nodiscard]] std::string name() const override { return "vector_search_row"; }

private:
    // Scan the bounded corpus and (re)build the in-memory index + payloads. Runs at
    // open() and, when corpus_refresh_ms > 0, again inline when a row arrives after the
    // interval elapses.
    void rebuild_corpus_();

    // Exact top-k over the corpus rows that satisfy filter_eq for this query row (a
    // query filter value that is null/absent/empty imposes no constraint). Used only
    // when filter_eq is set; ties break by corpus row index so the result is stable.
    std::vector<Neighbour> filtered_search_(const float* q,
                                            std::size_t dim,
                                            const clink::sql::Row& in) const;

    Config cfg_;
    std::unique_ptr<KnnIndex> index_;
    std::vector<clink::sql::Row> corpus_payloads_;    // parallel to the index's row order
    std::vector<std::vector<float>> corpus_vectors_;  // retained only when filter_eq is set
    std::chrono::steady_clock::time_point last_build_{};
};

}  // namespace clink::vector_search
