#pragma once

// clink::test::TestCluster - the smallest true distributed deployment,
// in one process: a real Coordinator and N real Workers wired over
// loopback RPC, with registration awaited before the constructor
// returns. Submit JobGraphSpecs exactly as a client would; everything
// (planning, deployment, slots, checkpoint coordination, failover) is
// the production cluster code.
//
//   clink::test::TestCluster cluster;
//   cluster::JobGraphSpec spec = ...;   // fluent env capture, SQL capture, or hand-built
//   cluster.execute(spec);              // submit + await completion; throws on job errors
//
// Use this tier for behaviour only a cluster exhibits (deployment,
// distributed checkpointing, network shuffles); operator logic belongs
// on the harnesses and pipeline wiring on LocalTestEnvironment.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <chrono>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/worker.hpp"

namespace clink::test {

class TestCluster {
public:
    struct Options {
        std::size_t workers{1};
        std::size_t slots_per_worker{4};
        std::chrono::milliseconds registration_timeout{std::chrono::seconds{10}};
        // Distributed checkpointing for submitted jobs (default: none).
        cluster::CheckpointConfig checkpoint{};
    };

    TestCluster() : TestCluster(Options{}) {}
    explicit TestCluster(Options options) : options_(std::move(options)) {
        cluster::ensure_built_ins_registered();
        const auto coordinator_port = coordinator_.start();
        std::vector<std::string> worker_ids;
        worker_ids.reserve(options_.workers);
        for (std::size_t i = 0; i < options_.workers; ++i) {
            worker_ids.push_back("mini-worker-" + std::to_string(i));
        }
        coordinator_.expect_workers(worker_ids);
        for (auto& id : worker_ids) {
            cluster::Worker::Config cfg;
            cfg.slot_count = options_.slots_per_worker;
            auto worker = std::make_unique<cluster::Worker>(id, "127.0.0.1", cfg);
            worker->connect_to_coordinator("127.0.0.1", coordinator_port);
            workers_.push_back(std::move(worker));
        }
        if (!coordinator_.await_registrations(options_.registration_timeout)) {
            throw std::runtime_error("TestCluster: Workers did not register within " +
                                     std::to_string(options_.registration_timeout.count()) + "ms");
        }
    }

    ~TestCluster() {
        for (auto& worker : workers_) {
            worker->stop();
        }
        coordinator_.stop();
    }

    TestCluster(const TestCluster&) = delete;
    TestCluster& operator=(const TestCluster&) = delete;

    // Submit against the default operator registry (built-ins + anything
    // the test registered), with the fixture's checkpoint config.
    cluster::JobId submit(const cluster::JobGraphSpec& spec) {
        return coordinator_.submit_job(
            spec, cluster::OperatorRegistry::default_instance(), {}, options_.checkpoint, nullptr);
    }

    bool await_completion(cluster::JobId job_id,
                          std::chrono::milliseconds timeout = std::chrono::seconds{60}) {
        return coordinator_.await_job_completion(job_id, timeout);
    }

    std::vector<std::string> errors(cluster::JobId job_id) const {
        return coordinator_.job_errors(job_id);
    }

    // Submit + await + assert clean: throws on timeout or job errors.
    cluster::JobId execute(const cluster::JobGraphSpec& spec,
                           std::chrono::milliseconds timeout = std::chrono::seconds{60}) {
        const auto id = submit(spec);
        if (!await_completion(id, timeout)) {
            throw std::runtime_error("TestCluster: job did not complete within " +
                                     std::to_string(timeout.count()) + "ms");
        }
        auto errs = errors(id);
        if (!errs.empty()) {
            std::string msg{"TestCluster: job failed: "};
            for (const auto& e : errs) {
                msg += e + "; ";
            }
            throw std::runtime_error(std::move(msg));
        }
        return id;
    }

    // Escape hatches to the real cluster pieces.
    cluster::Coordinator& coordinator() noexcept { return coordinator_; }
    cluster::Worker& worker(std::size_t i) { return *workers_.at(i); }
    std::size_t worker_count() const noexcept { return workers_.size(); }

private:
    Options options_;
    cluster::Coordinator coordinator_;
    std::vector<std::unique_ptr<cluster::Worker>> workers_;
};

}  // namespace clink::test
