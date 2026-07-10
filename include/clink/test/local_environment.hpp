#pragma once

// clink::test::LocalTestEnvironment - run a complete pipeline through
// the real local runtime, in-process, to completion:
//
//   clink::test::LocalTestEnvironment env;
//   auto src  = std::make_shared<clink::test::TestSource<std::int64_t>>(
//       std::vector<std::int64_t>{1, 2, 3});
//   auto sink = std::make_shared<clink::test::CollectSink<std::int64_t>>();
//   auto h0 = env.dag().add_source<std::int64_t>(src);
//   env.dag().add_sink<std::int64_t>(h0, sink);
//   env.execute();  // runs to completion; throws PipelineFailure on errors
//   EXPECT_EQ(sink->values(), (std::vector<std::int64_t>{1, 2, 3}));
//
// This is the integration tier above the operator harnesses: the same
// Dag, channels, operator runners, watermark propagation and terminal
// barriers production uses, driven over bounded test sources so the
// run terminates deterministically. State lives in the environment's
// backend (a fresh in-memory one by default), and Options::restore_from
// resumes from a Snapshot for pipeline-level recovery tests.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink::test {

// Thrown by execute() when any operator thread failed; the message
// lists every (operator, error) pair.
class PipelineFailure : public std::runtime_error {
public:
    PipelineFailure(std::string message, std::vector<LocalExecutor::OperatorError> errors)
        : std::runtime_error(std::move(message)), errors_(std::move(errors)) {}

    const std::vector<LocalExecutor::OperatorError>& errors() const noexcept { return errors_; }

private:
    std::vector<LocalExecutor::OperatorError> errors_;
};

class LocalTestEnvironment {
public:
    struct Options {
        // The pipeline's state backend (defaults to a fresh in-memory one).
        std::shared_ptr<StateBackend> state_backend;
        // Execution mode; Auto derives bounded/streaming from the sources.
        JobConfig::ExecutionMode execution_mode{JobConfig::ExecutionMode::Auto};
        // Resume from a previous checkpoint before any operator starts.
        std::optional<Snapshot> restore_from;
    };

    LocalTestEnvironment() : LocalTestEnvironment(Options{}) {}
    explicit LocalTestEnvironment(Options options)
        : backend_(options.state_backend ? std::move(options.state_backend)
                                         : std::make_shared<InMemoryStateBackend>()),
          options_(std::move(options)) {}

    // The Dag to build the pipeline on (add_source / add_operator /
    // add_sink and everything else the production Dag offers).
    Dag& dag() noexcept { return dag_; }

    std::shared_ptr<StateBackend> state_backend() const noexcept { return backend_; }

    // Run the pipeline to completion on the real local runtime.
    // One-shot: the Dag moves into the executor. Throws PipelineFailure
    // if any operator thread failed; use execute_collecting_errors()
    // when a failure is the expected outcome.
    void execute() {
        auto errors = execute_collecting_errors();
        if (!errors.empty()) {
            std::string msg{"pipeline failed: "};
            for (const auto& [op, what] : errors) {
                msg += "[" + op + "] " + what + "; ";
            }
            throw PipelineFailure(std::move(msg), std::move(errors));
        }
    }

    // Run to completion and RETURN operator-thread failures instead of
    // throwing - the form for tests where the crash is the point.
    std::vector<LocalExecutor::OperatorError> execute_collecting_errors() {
        if (executed_) {
            throw std::logic_error(
                "LocalTestEnvironment: execute() may only run once per environment");
        }
        executed_ = true;
        JobConfig cfg;
        cfg.state_backend = backend_;
        cfg.execution_mode = options_.execution_mode;
        cfg.restore_from = std::move(options_.restore_from);
        LocalExecutor exec(std::move(dag_), std::move(cfg));
        exec.run();
        errors_ = exec.operator_errors();
        return errors_;
    }

    // The last run's operator errors (empty when it ran cleanly).
    const std::vector<LocalExecutor::OperatorError>& errors() const noexcept { return errors_; }

private:
    std::shared_ptr<StateBackend> backend_;
    Options options_;
    Dag dag_;
    bool executed_{false};
    std::vector<LocalExecutor::OperatorError> errors_;
};

}  // namespace clink::test
