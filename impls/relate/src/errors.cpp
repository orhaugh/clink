#include "errors.hpp"

#include <exception>

#include "clink/config/json.hpp"

namespace clink::relate {

std::string EngineError::describe() const {
    if (code.empty()) {
        return message;
    }
    return message + " (" + code + ")";
}

EngineError parse_engine_error(std::string_view json) {
    const auto whole = [json] {
        EngineError e;
        e.message = json.empty() ? std::string("Relate gave no detail") : std::string(json);
        return e;
    };
    if (json.empty()) {
        return whole();
    }
    try {
        const auto doc = clink::config::parse(json);
        if (!doc.is_object()) {
            return whole();
        }
        const auto& obj = doc.as_object();
        const auto text = [&obj](const char* key) -> std::string {
            const auto it = obj.find(key);
            return it != obj.end() && it->second.is_string() ? it->second.as_string()
                                                             : std::string{};
        };
        EngineError e;
        e.code = text("code");
        e.stage = text("stage");
        e.message = text("message");
        if (const auto it = obj.find("retryable"); it != obj.end() && it->second.is_bool()) {
            e.retryable = it->second.as_bool();
        }
        return e.message.empty() && e.code.empty() ? whole() : e;
    } catch (const std::exception&) {
        return whole();
    }
}

}  // namespace clink::relate
