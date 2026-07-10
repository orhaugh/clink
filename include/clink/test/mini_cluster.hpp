#pragma once

// clink::test::MiniCluster - the smallest true distributed deployment,
// in one process: a real JobManager and N real TaskManagers wired over
// loopback RPC, with registration awaited before the constructor
// returns. Submit JobGraphSpecs exactly as a client would; everything
// (planning, deployment, slots, checkpoint coordination, failover) is
// the production cluster code.
//
//   clink::test::MiniCluster cluster;
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
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"

namespace clink::test {

class MiniCluster {
public:
    struct Options {
        std::size_t task_managers{1};
        std::size_t slots_per_task_manager{4};
        std::chrono::milliseconds registration_timeout{std::chrono::seconds{10}};
        // Distributed checkpointing for submitted jobs (default: none).
        cluster::CheckpointConfig checkpoint{};
    };

    MiniCluster() : MiniCluster(Options{}) {}
    explicit MiniCluster(Options options) : options_(std::move(options)) {
        cluster::ensure_built_ins_registered();
        const auto jm_port = jm_.start();
        std::vector<std::string> tm_ids;
        tm_ids.reserve(options_.task_managers);
        for (std::size_t i = 0; i < options_.task_managers; ++i) {
            tm_ids.push_back("mini-tm-" + std::to_string(i));
        }
        jm_.expect_tms(tm_ids);
        for (auto& id : tm_ids) {
            cluster::TaskManager::Config cfg;
            cfg.slot_count = options_.slots_per_task_manager;
            auto tm = std::make_unique<cluster::TaskManager>(id, "127.0.0.1", cfg);
            tm->connect_to_jm("127.0.0.1", jm_port);
            tms_.push_back(std::move(tm));
        }
        if (!jm_.await_registrations(options_.registration_timeout)) {
            throw std::runtime_error("MiniCluster: TaskManagers did not register within " +
                                     std::to_string(options_.registration_timeout.count()) + "ms");
        }
    }

    ~MiniCluster() {
        for (auto& tm : tms_) {
            tm->stop();
        }
        jm_.stop();
    }

    MiniCluster(const MiniCluster&) = delete;
    MiniCluster& operator=(const MiniCluster&) = delete;

    // Submit against the default operator registry (built-ins + anything
    // the test registered), with the fixture's checkpoint config.
    cluster::JobId submit(const cluster::JobGraphSpec& spec) {
        return jm_.submit_job(
            spec, cluster::OperatorRegistry::default_instance(), {}, options_.checkpoint, nullptr);
    }

    bool await_completion(cluster::JobId job_id,
                          std::chrono::milliseconds timeout = std::chrono::seconds{60}) {
        return jm_.await_job_completion(job_id, timeout);
    }

    std::vector<std::string> errors(cluster::JobId job_id) const { return jm_.job_errors(job_id); }

    // Submit + await + assert clean: throws on timeout or job errors.
    cluster::JobId execute(const cluster::JobGraphSpec& spec,
                           std::chrono::milliseconds timeout = std::chrono::seconds{60}) {
        const auto id = submit(spec);
        if (!await_completion(id, timeout)) {
            throw std::runtime_error("MiniCluster: job did not complete within " +
                                     std::to_string(timeout.count()) + "ms");
        }
        auto errs = errors(id);
        if (!errs.empty()) {
            std::string msg{"MiniCluster: job failed: "};
            for (const auto& e : errs) {
                msg += e + "; ";
            }
            throw std::runtime_error(std::move(msg));
        }
        return id;
    }

    // Escape hatches to the real cluster pieces.
    cluster::JobManager& job_manager() noexcept { return jm_; }
    cluster::TaskManager& task_manager(std::size_t i) { return *tms_.at(i); }
    std::size_t task_manager_count() const noexcept { return tms_.size(); }

private:
    Options options_;
    cluster::JobManager jm_;
    std::vector<std::unique_ptr<cluster::TaskManager>> tms_;
};

}  // namespace clink::test
