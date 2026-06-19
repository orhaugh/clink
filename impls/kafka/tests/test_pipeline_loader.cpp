// Unit tests for PipelineLoader. The loader's main job is parsing a
// JSON document into a Dag of operators. These tests focus on the
// parsing/validation layer; end-to-end (with real Kafka) lives in
// test_pipeline_config.cpp under the integration-tests build flag.

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/kafka/pipeline_loader.hpp"
#include "clink/runtime/dag.hpp"

using namespace clink;
using namespace clink::config;
using namespace clink::kafka;

TEST(PipelineLoader, RejectsMissingPipelineRoot) {
    Dag dag;
    KafkaPipelineLoader loader;
    EXPECT_THROW(loader.load(parse(R"({"x":1})"), dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsMissingStagesArray) {
    Dag dag;
    KafkaPipelineLoader loader;
    EXPECT_THROW(loader.load(parse(R"({"pipeline":{}})"), dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsUnknownStageType) {
    Dag dag;
    KafkaPipelineLoader loader;
    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "x", "type": "telepath", "params": {}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsMissingInputForOpStage) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_map_fn("identity", [](const KafkaMessage& m) { return m; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "m", "type": "map", "params": {"fn": "identity"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsUnknownMapFn) {
    Dag dag;
    KafkaPipelineLoader loader;
    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "x", "topic": "y"}},
                {"name": "m", "type": "map", "input": "src",
                 "params": {"fn": "missing_fn"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsDuplicateStageName) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_map_fn("identity", [](const KafkaMessage& m) { return m; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "x", "topic": "y"}},
                {"name": "src", "type": "map", "input": "src",
                 "params": {"fn": "identity"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsBranchReferenceOnNonSplitStage) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_map_fn("identity", [](const KafkaMessage& m) { return m; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "x", "topic": "y"}},
                {"name": "m", "type": "map", "input": "src.0",
                 "params": {"fn": "identity"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsBranchIndexOutOfRange) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_selector_fn("always_zero", [](const KafkaMessage&) { return 0; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "x", "topic": "y"}},
                {"name": "spl", "type": "split", "input": "src",
                 "params": {"fn": "always_zero", "branches": 2}},
                {"name": "kafka_sink_ghost", "type": "kafka_sink",
                 "input": "spl.7",
                 "params": {"brokers": "x", "topic": "out"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, RejectsUnnamedBranchReferenceToSplit) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_selector_fn("zero", [](const KafkaMessage&) { return 0; });
    loader.register_map_fn("identity", [](const KafkaMessage& m) { return m; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "x", "topic": "y"}},
                {"name": "spl", "type": "split", "input": "src",
                 "params": {"fn": "zero", "branches": 2}},
                {"name": "m", "type": "map", "input": "spl",
                 "params": {"fn": "identity"}}
            ]
        }
    })");
    EXPECT_THROW(loader.load(cfg, dag), std::runtime_error);
}

TEST(PipelineLoader, AcceptsLinearKafkaPipeline) {
    // We don't run the dag (would require a live Kafka cluster), but a
    // successful load() proves the schema and registry bindings line up.
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_map_fn("uppercase", [](const KafkaMessage& m) { return m; });
    loader.register_filter_fn("non_empty", [](const KafkaMessage&) { return true; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "in", "type": "kafka_source",
                 "params": {"brokers": "host:9092", "topic": "in"}},
                {"name": "u",  "type": "map", "input": "in",
                 "params": {"fn": "uppercase"}},
                {"name": "f",  "type": "filter", "input": "u",
                 "params": {"fn": "non_empty"}},
                {"name": "out","type": "kafka_sink", "input": "f",
                 "params": {"brokers": "host:9092", "topic": "out"}}
            ]
        }
    })");
    EXPECT_NO_THROW(loader.load(cfg, dag));
}

TEST(PipelineLoader, AcceptsBranchingPipelineWithSplit) {
    Dag dag;
    KafkaPipelineLoader loader;
    loader.register_selector_fn("by_key",
                                [](const KafkaMessage& m) { return m.key.has_value() ? 0 : 1; });

    const auto cfg = parse(R"({
        "pipeline": {
            "stages": [
                {"name": "src", "type": "kafka_source",
                 "params": {"brokers": "h:9092", "topic": "in"}},
                {"name": "router", "type": "split", "input": "src",
                 "params": {"fn": "by_key", "branches": 2}},
                {"name": "main", "type": "kafka_sink", "input": "router.0",
                 "params": {"brokers": "h:9092", "topic": "main"}},
                {"name": "errs", "type": "kafka_sink", "input": "router.1",
                 "params": {"brokers": "h:9092", "topic": "errs"}}
            ]
        }
    })");
    EXPECT_NO_THROW(loader.load(cfg, dag));
}
