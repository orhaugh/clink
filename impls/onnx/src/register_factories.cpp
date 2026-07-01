#include <map>
#include <memory>
#include <string>

#include "clink/onnx/install.hpp"
#include "clink/onnx/onnx_model_provider.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/model_provider.hpp"

namespace clink::onnx {

void install(clink::plugin::PluginRegistry& /*reg*/) {
    // SQL-native AI: register the local ONNX Runtime model-inference provider so
    // ML_PREDICT with a model declared WITH (provider='onnx', model_path='...') runs
    // inference in-process. Registered into the process-wide ModelProviderRegistry (not
    // the PluginRegistry) - the ml_predict_row operator looks it up by name, the same
    // as the HTTP provider.
    clink::sql::ModelProviderRegistry::global().register_provider(
        "onnx", [](const std::map<std::string, std::string>& opts) {
            return make_onnx_model_provider(opts);
        });
}

}  // namespace clink::onnx
