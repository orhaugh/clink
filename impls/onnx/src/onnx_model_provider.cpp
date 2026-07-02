#include "clink/onnx/onnx_model_provider.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <onnxruntime_cxx_api.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

namespace clink::onnx {

namespace {

std::string opt_str(const std::map<std::string, std::string>& o,
                    const std::string& k,
                    const std::string& fallback = "") {
    const auto it = o.find(k);
    return it == o.end() ? fallback : it->second;
}

int opt_int(const std::map<std::string, std::string>& o, const std::string& k, int fallback) {
    const auto it = o.find(k);
    if (it == o.end() || it->second.empty()) {
        return fallback;
    }
    try {
        return std::stoi(it->second);
    } catch (...) {
        return fallback;
    }
}

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= s.size()) {
        const auto comma = s.find(',', start);
        const auto end = comma == std::string::npos ? s.size() : comma;
        if (end > start) {
            out.push_back(s.substr(start, end - start));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

// Coerce a JSON cell to a double (for a numeric input tensor). Numbers pass through;
// numeric strings are parsed; anything else is a model/query mismatch.
double to_double(const clink::config::JsonValue& v, const std::string& col) {
    if (v.is_number()) {
        return v.as_number();
    }
    if (v.is_string()) {
        try {
            return std::stod(v.as_string());
        } catch (...) {
            // fall through to the error
        }
    }
    throw std::runtime_error("ML_PREDICT onnx provider: feature column '" + col +
                             "' is not numeric (required by the model's input tensor)");
}

// Render a JSON cell as a string (for a string input tensor).
std::string to_string_cell(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        return v.as_string();
    }
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (d == static_cast<double>(static_cast<std::int64_t>(d))) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        return std::to_string(d);
    }
    return v.serialize(0);
}

// Owns the backing buffers for the input tensors across a single Run() call: CreateTensor
// for numeric types references (does not copy) the data, so the buffer must outlive Run.
// A deque never relocates existing elements, so pointers into earlier buffers stay valid
// as later inputs are appended. String tensors are allocator-owned (FillStringTensor
// copies), but their const char* view must also outlive the call.
struct InputBuffers {
    std::deque<std::vector<float>> f32;
    std::deque<std::vector<double>> f64;
    std::deque<std::vector<std::int64_t>> i64;
    std::deque<std::vector<std::int32_t>> i32;
    std::deque<std::vector<std::string>> str;
    std::deque<std::vector<const char*>> strptr;
};

// Synchronous ONNX Runtime provider. v2 I/O: each model INPUT tensor is fed by matching
// its name to a feature column (a JSON array column -> a vector tensor, a scalar -> a
// [1,1] tensor); an input with no matching column falls back to the whole DESCRIPTOR
// feature vector (the common single-unnamed-input case). Each is coerced to the input
// tensor's declared dtype (float32/float64/int64/int32/string). Outputs: if any OUTPUT
// column name matches a model output tensor name, every OUTPUT column is read by name
// (typed - float/double -> number, int -> integer, string -> string); otherwise the
// legacy positional path flattens the float outputs into the OUTPUT columns in order.
class OnnxModelProvider final : public clink::sql::ModelProvider {
public:
    OnnxModelProvider(const std::string& model_path,
                      std::vector<std::string> feature_columns,
                      std::vector<std::string> output_columns,
                      int intra_op_threads)
        : env_(ORT_LOGGING_LEVEL_WARNING, "clink-onnx"),
          session_(nullptr),
          mem_(Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault)),
          feature_columns_(std::move(feature_columns)),
          output_columns_(std::move(output_columns)) {
        Ort::SessionOptions so;
        so.SetIntraOpNumThreads(intra_op_threads < 1 ? 1 : intra_op_threads);
        so.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);
        session_ = Ort::Session(env_, model_path.c_str(), so);

        Ort::AllocatorWithDefaultOptions alloc;
        const std::size_t n_in = session_.GetInputCount();
        for (std::size_t i = 0; i < n_in; ++i) {
            input_names_owned_.emplace_back(session_.GetInputNameAllocated(i, alloc).get());
            // Keep the TypeInfo alive: GetTensorTypeAndShapeInfo() returns a view into it,
            // so reading through a destroyed temporary would dangle.
            Ort::TypeInfo type_info = session_.GetInputTypeInfo(i);
            const auto shape_info = type_info.GetTensorTypeAndShapeInfo();
            input_types_.push_back(shape_info.GetElementType());
            // Declared rank: a rank-1 input takes a [N] tensor; anything else (the common
            // [batch, features]) takes [1, N]. Honouring it avoids an ORT rank mismatch.
            input_ranks_.push_back(shape_info.GetShape().size());
        }
        const std::size_t n_out = session_.GetOutputCount();
        for (std::size_t i = 0; i < n_out; ++i) {
            output_names_owned_.emplace_back(session_.GetOutputNameAllocated(i, alloc).get());
        }
        for (const auto& s : input_names_owned_) {
            input_name_ptrs_.push_back(s.c_str());
        }
        for (const auto& s : output_names_owned_) {
            output_name_ptrs_.push_back(s.c_str());
        }
    }

    clink::sql::Row predict(const clink::sql::Row& features) override {
        InputBuffers bufs;
        std::vector<Ort::Value> inputs;
        inputs.reserve(input_names_owned_.size());
        for (std::size_t i = 0; i < input_names_owned_.size(); ++i) {
            const auto cells = source_cells_(input_names_owned_[i], features);
            inputs.push_back(
                build_input_(input_types_[i], input_ranks_[i], input_names_owned_[i], cells, bufs));
        }

        auto outputs = session_.Run(Ort::RunOptions{nullptr},
                                    input_name_ptrs_.data(),
                                    inputs.data(),
                                    inputs.size(),
                                    output_name_ptrs_.data(),
                                    output_name_ptrs_.size());

        clink::sql::Row out;
        if (outputs_matched_by_name_()) {
            for (const auto& oc : output_columns_) {
                const auto idx = output_index_(oc);
                out.values[oc] = idx.has_value() ? read_output_scalar_(outputs[*idx])
                                                 : clink::config::JsonValue{};
            }
        } else {
            // Legacy positional path: flatten every float output tensor's elements in
            // order and assign to the OUTPUT columns positionally.
            std::vector<float> flat;
            for (auto& v : outputs) {
                if (!v.IsTensor()) {
                    continue;
                }
                auto info = v.GetTensorTypeAndShapeInfo();
                if (info.GetElementType() != ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT) {
                    continue;
                }
                const std::size_t count = info.GetElementCount();
                const float* p = v.GetTensorData<float>();
                for (std::size_t j = 0; j < count; ++j) {
                    flat.push_back(p[j]);
                }
            }
            for (std::size_t i = 0; i < output_columns_.size() && i < flat.size(); ++i) {
                out.values[output_columns_[i]] =
                    clink::config::JsonValue{static_cast<double>(flat[i])};
            }
        }
        return out;
    }

    [[nodiscard]] std::string name() const override { return "onnx"; }

private:
    // The JSON cells that feed input tensor `input_name`: the same-named feature column
    // (its array elements, or the scalar itself), or - when no column matches - the whole
    // DESCRIPTOR feature vector (the single-unnamed-input case).
    std::vector<const clink::config::JsonValue*> source_cells_(const std::string& input_name,
                                                               const clink::sql::Row& features) {
        std::vector<const clink::config::JsonValue*> cells;
        const auto it = features.values.find(input_name);
        if (it != features.values.end()) {
            if (it->second.is_array()) {
                for (const auto& e : it->second.as_array()) {
                    cells.push_back(&e);
                }
            } else {
                cells.push_back(&it->second);
            }
            return cells;
        }
        for (const auto& col : feature_columns_) {
            const auto fit = features.values.find(col);
            if (fit == features.values.end()) {
                throw std::runtime_error("ML_PREDICT onnx provider: missing feature column '" +
                                         col + "'");
            }
            cells.push_back(&fit->second);
        }
        return cells;
    }

    Ort::Value build_input_(ONNXTensorElementDataType type,
                            std::size_t rank,
                            const std::string& input_name,
                            const std::vector<const clink::config::JsonValue*>& cells,
                            InputBuffers& bufs) {
        // Rank-1 input -> [N]; otherwise a batch-of-one [1, N].
        const std::vector<std::int64_t> shape =
            rank <= 1 ? std::vector<std::int64_t>{static_cast<std::int64_t>(cells.size())}
                      : std::vector<std::int64_t>{1, static_cast<std::int64_t>(cells.size())};
        switch (type) {
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT: {
                bufs.f32.emplace_back();
                auto& b = bufs.f32.back();
                for (const auto* v : cells) {
                    b.push_back(static_cast<float>(to_double(*v, input_name)));
                }
                return Ort::Value::CreateTensor<float>(
                    mem_, b.data(), b.size(), shape.data(), shape.size());
            }
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_DOUBLE: {
                bufs.f64.emplace_back();
                auto& b = bufs.f64.back();
                for (const auto* v : cells) {
                    b.push_back(to_double(*v, input_name));
                }
                return Ort::Value::CreateTensor<double>(
                    mem_, b.data(), b.size(), shape.data(), shape.size());
            }
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64: {
                bufs.i64.emplace_back();
                auto& b = bufs.i64.back();
                for (const auto* v : cells) {
                    b.push_back(static_cast<std::int64_t>(to_double(*v, input_name)));
                }
                return Ort::Value::CreateTensor<std::int64_t>(
                    mem_, b.data(), b.size(), shape.data(), shape.size());
            }
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32: {
                bufs.i32.emplace_back();
                auto& b = bufs.i32.back();
                for (const auto* v : cells) {
                    b.push_back(static_cast<std::int32_t>(to_double(*v, input_name)));
                }
                return Ort::Value::CreateTensor<std::int32_t>(
                    mem_, b.data(), b.size(), shape.data(), shape.size());
            }
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_STRING: {
                bufs.str.emplace_back();
                auto& s = bufs.str.back();
                for (const auto* v : cells) {
                    s.push_back(to_string_cell(*v));
                }
                bufs.strptr.emplace_back();
                auto& p = bufs.strptr.back();
                for (const auto& x : s) {
                    p.push_back(x.c_str());
                }
                Ort::AllocatorWithDefaultOptions alloc;
                Ort::Value t = Ort::Value::CreateTensor(
                    alloc, shape.data(), shape.size(), ONNX_TENSOR_ELEMENT_DATA_TYPE_STRING);
                t.FillStringTensor(p.data(), p.size());
                return t;
            }
            default:
                throw std::runtime_error(
                    "ML_PREDICT onnx provider: input '" + input_name +
                    "' has an unsupported element type (float32/float64/int64/int32/string only)");
        }
    }

    [[nodiscard]] bool outputs_matched_by_name_() const {
        for (const auto& oc : output_columns_) {
            if (output_index_(oc).has_value()) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::optional<std::size_t> output_index_(const std::string& name) const {
        for (std::size_t i = 0; i < output_names_owned_.size(); ++i) {
            if (output_names_owned_[i] == name) {
                return i;
            }
        }
        return std::nullopt;
    }

    // Read the first element of an output tensor as a typed JSON value.
    static clink::config::JsonValue read_output_scalar_(const Ort::Value& v) {
        if (!v.IsTensor()) {
            return clink::config::JsonValue{};
        }
        auto info = v.GetTensorTypeAndShapeInfo();
        if (info.GetElementCount() == 0) {
            return clink::config::JsonValue{};
        }
        switch (info.GetElementType()) {
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT:
                return clink::config::JsonValue{static_cast<double>(v.GetTensorData<float>()[0])};
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_DOUBLE:
                return clink::config::JsonValue{v.GetTensorData<double>()[0]};
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64:
                return clink::config::JsonValue{
                    static_cast<std::int64_t>(v.GetTensorData<std::int64_t>()[0])};
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32:
                return clink::config::JsonValue{
                    static_cast<std::int64_t>(v.GetTensorData<std::int32_t>()[0])};
            case ONNX_TENSOR_ELEMENT_DATA_TYPE_STRING:
                return clink::config::JsonValue{v.GetStringTensorElement(0)};
            default:
                return clink::config::JsonValue{};
        }
    }

    Ort::Env env_;
    Ort::Session session_;
    Ort::MemoryInfo mem_;
    std::vector<std::string> input_names_owned_;
    std::vector<const char*> input_name_ptrs_;
    std::vector<ONNXTensorElementDataType> input_types_;
    std::vector<std::size_t> input_ranks_;
    std::vector<std::string> output_names_owned_;
    std::vector<const char*> output_name_ptrs_;
    std::vector<std::string> feature_columns_;
    std::vector<std::string> output_columns_;
};

}  // namespace

std::shared_ptr<clink::sql::ModelProvider> make_onnx_model_provider(
    const std::map<std::string, std::string>& opts) {
    const std::string model_path = opt_str(opts, "model_path");
    if (model_path.empty()) {
        throw std::runtime_error("ML_PREDICT onnx provider: 'model_path' option is required");
    }
    return std::make_shared<OnnxModelProvider>(model_path,
                                               split_csv(opt_str(opts, "feature_columns")),
                                               split_csv(opt_str(opts, "output_columns")),
                                               opt_int(opts, "intra_op_threads", 1));
}

}  // namespace clink::onnx
