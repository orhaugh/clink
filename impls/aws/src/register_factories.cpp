// AWS-family connector factory registration (Kinesis, Firehose, DynamoDB).

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/dynamodb_sink.hpp"
#include "clink/aws/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::aws {

namespace {

// Read the shared AWS client options (region / endpoint_override for LocalStack)
// off a BuildContext.
AwsClientOptions client_options_from(const clink::plugin::BuildContext& ctx) {
    AwsClientOptions o;
    const auto region = ctx.param_or("region", "");
    if (!region.empty()) {
        o.region = region;
    }
    const auto endpoint = ctx.param_or("endpoint_override", "");
    if (!endpoint.empty()) {
        o.endpoint_override = endpoint;
    }
    return o;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // dynamodb_sink: upsert JSON-object records into a DynamoDB table via
    // BatchWriteItem. At-least-once; effectively-once when the primary key is
    // stable (PutItem overwrites). Params:
    //   table (required)          - target table
    //   partition_key (required)  - the table's partition-key attribute name
    //   sort_key                  - the sort-key attribute name (composite tables)
    //   region, endpoint_override - endpoint_override targets LocalStack/custom
    //   batch_records (default 25; clamped to [1, 25])
    //   max_retries (default 8)   - UnprocessedItems resubmit attempts
    reg.register_sink<std::string>(
        "dynamodb_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            DynamoDbSinkOptions o;
            o.table = ctx.param_or("table");
            o.partition_key = ctx.param_or("partition_key");
            o.sort_key = ctx.param_or("sort_key", "");
            o.client = client_options_from(ctx);
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 25));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 8));
            o.name = "dynamodb_sink";
            return std::make_shared<DynamoDbSink>(std::move(o));
        });
}

}  // namespace clink::aws
