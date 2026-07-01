#include "clink/onnx/onnx_model_provider.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <onnxruntime_cxx_api.h>
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

// Coerce a JSON cell to a float feature. Numbers pass through; numeric strings are
// parsed; anything else is a hard error (a non-numeric feature into a float tensor is
// a model/query mismatch, not something to silently zero-fill).
float to_feature(const clink::config::JsonValue& v, const std::string& col) {
    if (v.is_number()) {
        return static_cast<float>(v.as_number());
    }
    if (v.is_string()) {
        try {
            return std::stof(v.as_string());
        } catch (...) {
            // fall through to the error below
        }
    }
    throw std::runtime_error("ML_PREDICT onnx provider: feature column '" + col +
                             "' is not a number");
}

// Synchronous ONNX Runtime provider: one loaded session, one blocking inference per
// row. The session, its owned input/output name strings, and the derived const char*
// name vectors are all members so the pointers handed to Session::Run stay valid.
class OnnxModelProvider final : public clink::sql::ModelProvider {
public:
    OnnxModelProvider(const std::string& model_path,
                      std::vector<std::string> feature_columns,
                      std::vector<std::string> output_columns,
                      int intra_op_threads)
        : env_(ORT_LOGGING_LEVEL_WARNING, "clink-onnx"),
          session_(nullptr),
          feature_columns_(std::move(feature_columns)),
          output_columns_(std::move(output_columns)) {
        Ort::SessionOptions so;
        so.SetIntraOpNumThreads(intra_op_threads < 1 ? 1 : intra_op_threads);
        so.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);
        session_ = Ort::Session(env_, model_path.c_str(), so);

        if (session_.GetInputCount() != 1) {
            throw std::runtime_error(
                "ML_PREDICT onnx provider: v1 supports a single input tensor, model '" +
                model_path + "' has " + std::to_string(session_.GetInputCount()));
        }
        Ort::AllocatorWithDefaultOptions alloc;
        auto in = session_.GetInputNameAllocated(0, alloc);
        input_name_ = in.get();
        input_names_ = {input_name_.c_str()};
        const std::size_t n_out = session_.GetOutputCount();
        output_name_storage_.reserve(n_out);
        output_names_.reserve(n_out);
        for (std::size_t i = 0; i < n_out; ++i) {
            auto on = session_.GetOutputNameAllocated(i, alloc);
            output_name_storage_.emplace_back(on.get());
        }
        for (const auto& s : output_name_storage_) {
            output_names_.push_back(s.c_str());
        }
    }

    clink::sql::Row predict(const clink::sql::Row& features) override {
        std::vector<float> in;
        in.reserve(feature_columns_.size());
        for (const auto& col : feature_columns_) {
            const auto it = features.values.find(col);
            if (it == features.values.end()) {
                throw std::runtime_error("ML_PREDICT onnx provider: missing feature column '" +
                                         col + "'");
            }
            in.push_back(to_feature(it->second, col));
        }

        const std::array<std::int64_t, 2> shape{1, static_cast<std::int64_t>(in.size())};
        Ort::MemoryInfo mem = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
        Ort::Value input =
            Ort::Value::CreateTensor<float>(mem, in.data(), in.size(), shape.data(), shape.size());

        auto outputs = session_.Run(Ort::RunOptions{nullptr},
                                    input_names_.data(),
                                    &input,
                                    1,
                                    output_names_.data(),
                                    output_names_.size());

        // Flatten every output tensor's float elements in output order, then assign to
        // the OUTPUT columns positionally.
        std::vector<float> flat;
        for (auto& v : outputs) {
            if (!v.IsTensor()) {
                continue;
            }
            auto info = v.GetTensorTypeAndShapeInfo();
            if (info.GetElementType() != ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT) {
                throw std::runtime_error("ML_PREDICT onnx provider: v1 reads float32 outputs only");
            }
            const std::size_t count = info.GetElementCount();
            const float* p = v.GetTensorData<float>();
            for (std::size_t i = 0; i < count; ++i) {
                flat.push_back(p[i]);
            }
        }

        clink::sql::Row out;
        for (std::size_t i = 0; i < output_columns_.size() && i < flat.size(); ++i) {
            out.values[output_columns_[i]] = clink::config::JsonValue{static_cast<double>(flat[i])};
        }
        return out;
    }

    [[nodiscard]] std::string name() const override { return "onnx"; }

private:
    Ort::Env env_;
    Ort::Session session_;
    std::string input_name_;
    std::vector<const char*> input_names_;
    std::vector<std::string> output_name_storage_;
    std::vector<const char*> output_names_;
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
