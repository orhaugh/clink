#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/vector_search/knn_index.hpp"

// The vector_search_row operator: a SYNCHRONOUS flatmap (in-memory KNN is CPU-bound,
// not I/O-bound, so it is not on the async operator path). At open() it bounded-drains
// the vector table into an in-memory index (the ANALYZE-style local scan); at process
// it emits, per input row, its top_k nearest corpus rows plus a `score`. The corpus
// index is derived state - rebuilt on restart from the bounded corpus, not
// checkpointed - so a corpus that changes mid-job is not reflected until restart (a
// stated v1 limitation; a remote / mutable vector index is an async follow-on).

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
        IndexParams index;  // metric / kind / dim / hnsw knobs
    };

    explicit VectorSearchOperator(Config cfg) : cfg_(std::move(cfg)) {}

    void open() override;
    void process(const clink::StreamElement<clink::sql::Row>& element,
                 clink::Emitter<clink::sql::Row>& out) override;
    [[nodiscard]] std::string name() const override { return "vector_search_row"; }

private:
    Config cfg_;
    std::unique_ptr<KnnIndex> index_;
    std::vector<clink::sql::Row> corpus_payloads_;  // parallel to the index's row order
};

}  // namespace clink::vector_search
