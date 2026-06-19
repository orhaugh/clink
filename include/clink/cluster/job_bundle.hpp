#pragma once

#include <memory>
#include <string>

#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_helpers.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cluster {

// JobBundle owns one of each registry type, all parented to the
// corresponding default-instance. It's how the JM and TM scope a
// submitted job's plugin/inline-lambda registrations: registrations
// (PluginLoader writes, build_fn writes) land in the bundle's
// instances, while lookups (planner, runner dispatch) fall through to
// the default singletons on miss for built-ins (int64/string types,
// file_text_sink etc).
//
// Lifetime: one bundle per submitted job. JM creates it in
// handle_submit_; TM creates a mirror on first Deploy of that job.
// Bundles do NOT cross processes - each side maintains its own,
// driven by the same .so dlopened in both places.
//
// Registries are heap-allocated through unique_ptr so the bundle is
// movable. The registries themselves hold a std::mutex (non-movable),
// so we can't put them inline as value members and still let the
// bundle be returned by value / stored in containers.
class JobBundle {
public:
    JobBundle()
        : type_registry_(std::make_unique<TypeRegistry>(&TypeRegistry::default_instance())),
          runner_registry_(std::make_unique<RunnerRegistry>(&RunnerRegistry::default_instance())),
          selector_registry_(
              std::make_unique<SelectorRegistry>(&SelectorRegistry::default_instance())),
          key_extractor_registry_(
              std::make_unique<KeyExtractorRegistry>(&KeyExtractorRegistry::default_instance())),
          side_output_attacher_registry_(std::make_unique<SideOutputAttacherRegistry>(
              &SideOutputAttacherRegistry::default_instance())),
          operator_registry_(
              std::make_unique<OperatorRegistry>(&OperatorRegistry::default_instance())),
          dag_builder_registry_(
              std::make_unique<DagBuilderRegistry>(&DagBuilderRegistry::default_instance())) {}

    JobBundle(JobBundle&&) noexcept = default;
    JobBundle& operator=(JobBundle&&) noexcept = default;
    JobBundle(const JobBundle&) = delete;
    JobBundle& operator=(const JobBundle&) = delete;

    TypeRegistry& type_registry() noexcept { return *type_registry_; }
    RunnerRegistry& runner_registry() noexcept { return *runner_registry_; }
    SelectorRegistry& selector_registry() noexcept { return *selector_registry_; }
    KeyExtractorRegistry& key_extractor_registry() noexcept { return *key_extractor_registry_; }
    SideOutputAttacherRegistry& side_output_attacher_registry() noexcept {
        return *side_output_attacher_registry_;
    }
    OperatorRegistry& operator_registry() noexcept { return *operator_registry_; }
    DagBuilderRegistry& dag_builder_registry() noexcept { return *dag_builder_registry_; }
    const RunnerRegistry& runner_registry() const noexcept { return *runner_registry_; }
    const OperatorRegistry& operator_registry() const noexcept { return *operator_registry_; }
    const DagBuilderRegistry& dag_builder_registry() const noexcept {
        return *dag_builder_registry_;
    }

    // Returns a PluginRegistry view bound to this bundle's registries.
    // The view is non-owning; the bundle must outlive every use of
    // the returned PluginRegistry. PluginLoader::load_into and the
    // CLINK_REGISTER_JOB macro forward this view to the .so's
    // register entry point so registrations land in the bundle.
    plugin::PluginRegistry as_plugin_registry() {
        return plugin::PluginRegistry{*type_registry_,
                                      *runner_registry_,
                                      *selector_registry_,
                                      *key_extractor_registry_,
                                      *side_output_attacher_registry_,
                                      *operator_registry_,
                                      *dag_builder_registry_};
    }

private:
    std::unique_ptr<TypeRegistry> type_registry_;
    std::unique_ptr<RunnerRegistry> runner_registry_;
    std::unique_ptr<SelectorRegistry> selector_registry_;
    std::unique_ptr<KeyExtractorRegistry> key_extractor_registry_;
    std::unique_ptr<SideOutputAttacherRegistry> side_output_attacher_registry_;
    std::unique_ptr<OperatorRegistry> operator_registry_;
    std::unique_ptr<DagBuilderRegistry> dag_builder_registry_;
};

}  // namespace clink::cluster
