#include "clink/vector_search/vector_search_operator.hpp"

#include <stdexcept>
#include <utility>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/vector_value.hpp"

namespace clink::vector_search {

using clink::sql::Row;
using clink::sql::vector_from_row;
using clink::sql::VectorCell;

void VectorSearchOperator::open() {
    auto& reg = clink::cluster::OperatorRegistry::default_instance();
    const auto* sf = reg.find_source(cfg_.source_factory, std::string{"row"});
    if (sf == nullptr) {
        throw std::runtime_error("vector_search: corpus source factory '" + cfg_.source_factory +
                                 "' is not registered (is the connector implementation linked?)");
    }
    clink::cluster::OperatorBuildContext bctx;
    bctx.params = cfg_.source_params;
    auto source = std::static_pointer_cast<clink::Source<Row>>(sf->build(bctx));
    if (!source) {
        throw std::runtime_error("vector_search: '" + cfg_.source_factory +
                                 "' did not build a Row source");
    }
    if (!source->is_bounded()) {
        throw std::runtime_error(
            "vector_search: the vector table must be a bounded source (file / parquet); "
            "an unbounded stream cannot back a searchable corpus");
    }

    // A one-shot in-process scan of the bounded corpus (the analyze_table pattern):
    // each corpus row contributes its index vector + the projected emit columns.
    std::vector<std::vector<float>> corpus_vectors;
    clink::Dag dag;
    auto handle = dag.add_source<Row>(source);
    auto sink = std::make_shared<clink::FunctionSink<Row>>(
        [&](const Row& r) {
            VectorCell cell = vector_from_row(r, cfg_.index_column, cfg_.index.dim);
            if (!cell.present || !cell.dim_ok || cell.data.empty()) {
                return;  // skip corpus rows without a usable index vector
            }
            Row payload;
            for (const auto& col : cfg_.vector_columns) {
                const auto it = r.values.find(col);
                if (it != r.values.end()) {
                    payload.values[col] = it->second;
                }
            }
            corpus_vectors.push_back(std::move(cell.data));
            corpus_payloads_.push_back(std::move(payload));
        },
        "vector_search_corpus_sink");
    dag.add_sink<Row>(handle, sink);

    clink::JobConfig cfg;
    clink::LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    for (const auto& [op, msg] : exec.operator_errors()) {
        throw std::runtime_error("vector_search: corpus scan failed in " + op + ": " + msg);
    }

    if (cfg_.index.dim == 0 && !corpus_vectors.empty()) {
        cfg_.index.dim = corpus_vectors.front().size();
    }
    index_ = make_knn_index(cfg_.index, corpus_vectors.size());
    index_->build(std::move(corpus_vectors));
}

void VectorSearchOperator::process(const clink::StreamElement<Row>& element,
                                   clink::Emitter<Row>& out) {
    if (element.is_data()) {
        clink::Batch<Row> emit_batch;
        for (const auto& rec : element.as_data()) {
            const Row& in = rec.value();
            const VectorCell q = vector_from_row(in, cfg_.query_column, cfg_.index.dim);
            if (!q.present || !q.dim_ok || q.data.empty()) {
                continue;  // no usable query vector: emit nothing for this row
            }
            const auto hits = index_->search(q.data.data(), q.data.size(), cfg_.top_k);
            for (const auto& h : hits) {
                Row out_row = in;  // carry every input column (and __row_kind) through
                if (h.row_index < corpus_payloads_.size()) {
                    for (const auto& [col, val] : corpus_payloads_[h.row_index].values) {
                        out_row.values[col] = val;
                    }
                }
                out_row.values[cfg_.score_column] =
                    clink::config::JsonValue{static_cast<double>(h.score)};
                if (rec.event_time().has_value()) {
                    emit_batch.push(clink::Record<Row>{std::move(out_row), *rec.event_time()});
                } else {
                    emit_batch.push(clink::Record<Row>{std::move(out_row)});
                }
            }
        }
        if (!emit_batch.empty()) {
            out.emit_data(std::move(emit_batch));
        }
    } else if (element.is_watermark()) {
        this->on_watermark(element.as_watermark(), out);
    } else if (element.is_barrier()) {
        this->on_barrier(element.as_barrier(), out);
    } else if (element.is_drain()) {
        out.emit_drain(element.as_drain());
    }
}

}  // namespace clink::vector_search
