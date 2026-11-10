// `clink incident-reconcile` in a build without the ClickHouse connector: every
// run is refused, saying so.

#include "clink_incident_reconcile.hpp"

bool clink_incident_reconcile_available() {
    return false;
}

clink::incident::reconcile::Report clink_incident_reconcile(
    const std::string& bundle,
    const ClinkReconcileTarget& /*target*/,
    const clink::incident::reconcile::Options& options) {
    const auto open =
        [](const std::string&,
           const std::string&) -> std::unique_ptr<clink::incident::reconcile::Destination> {
        throw std::runtime_error(
            "this build has no ClickHouse connector, so it cannot read the destination");
    };
    return clink::incident::reconcile::run(bundle, open, options);
}
