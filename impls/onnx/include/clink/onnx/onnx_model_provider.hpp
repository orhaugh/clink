#pragma once

#include <map>
#include <memory>
#include <string>

#include "clink/sql/model_provider.hpp"

// SQL-native AI: a ModelProvider that runs local inference with ONNX Runtime. A model
// declared WITH (provider='onnx', model_path='/path/to/model.onnx') loads that ONNX
// graph once at operator-open time; predict() marshals the DESCRIPTOR feature columns
// into the model's first float input tensor, runs the session, and reads the output
// tensors' float elements back into the model's OUTPUT columns positionally.
//
// Synchronous (v1): one blocking inference per row on the operator thread. ORT runs on
// CPU single-thread by default; the ORT shared library must be present at runtime. The
// ORT headers are PRIVATE to the impl TU, so ONNX Runtime never reaches a public clink
// header or the plugin ABI.
//
// Recognised WITH-options (from CREATE MODEL, plus feature/output columns injected by
// the ml_predict_row operator):
//   model_path      (required) filesystem path to the .onnx model
//   feature_columns (injected) csv of input column names, in tensor order
//   output_columns  (injected) csv of OUTPUT column names to fill from the outputs
//   intra_op_threads optional ORT intra-op thread count (default 1)
//
// v1 shape contract: the model takes ONE float32 input tensor of shape [1, K] (K = the
// number of feature columns) and its output float elements are read in order across
// all output tensors, then assigned to output_columns in order. Models outside that
// shape are a documented follow-on (multi-input / integer / string tensors).

namespace clink::onnx {

std::shared_ptr<clink::sql::ModelProvider> make_onnx_model_provider(
    const std::map<std::string, std::string>& opts);

}  // namespace clink::onnx
