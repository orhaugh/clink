#pragma once

#include <map>
#include <memory>
#include <string>

#include "clink/sql/model_provider.hpp"

// SQL-native AI: a ModelProvider that runs inference by POSTing feature columns as
// JSON to a configured HTTP endpoint and mapping the JSON response into the model's
// OUTPUT columns. Reuses the connector's HttpRequest client (cpp-httplib). Registered
// under provider='http' in the http_connector install. Synchronous (v1): one blocking
// POST per row.
//
// Recognised WITH-options (from CREATE MODEL, plus feature/output columns injected by
// the ml_predict_row operator):
//   endpoint        (required) full inference URL, scheme://host[:port]/path
//   output_columns  (injected) csv of OUTPUT column names to read from the response
//   auth_token      static bearer token -> "Authorization: Bearer <token>"
//   auth_token_file refreshing bearer token file (takes precedence over auth_token)
//   headers         extra request headers, "K: V\nK2: V2"
//   timeout_ms      request timeout (default 5000)
//   verify_tls      https cert verification (default true)
//   content_type    request content type (default application/json)
//   response_path   optional key in the response object holding the predictions object

namespace clink::http_connector {

std::shared_ptr<clink::sql::ModelProvider> make_http_model_provider(
    const std::map<std::string, std::string>& opts);

}  // namespace clink::http_connector
