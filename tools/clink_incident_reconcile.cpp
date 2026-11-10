// `clink incident-reconcile` against ClickHouse, through the connector's
// read-only reader (clink/clickhouse/reconcile_destination.hpp).

#include "clink_incident_reconcile.hpp"

#include <memory>

#include "clink/clickhouse/reconcile_destination.hpp"

bool clink_incident_reconcile_available() {
    return clink::clickhouse::reconcile_destination_available();
}

clink::incident::reconcile::Report clink_incident_reconcile(
    const std::string& bundle,
    const ClinkReconcileTarget& target,
    const clink::incident::reconcile::Options& options) {
    const auto open = [&](const std::string& database, const std::string& table) {
        clink::clickhouse::ReconcileConnection c;
        c.host = target.host;
        c.port = target.port != 0 ? target.port
                                  : static_cast<std::uint16_t>(target.secure ? 9440 : 9000);
        c.user = target.user;
        c.password = target.password;
        c.database = database;
        c.table = table;
        c.secure = target.secure;
        c.tls_ca_file = target.tls_ca_file;
        c.tls_verify = target.tls_verify;
        return clink::clickhouse::make_reconcile_destination(std::move(c), options.bounds);
    };
    return clink::incident::reconcile::run(bundle, open, options);
}
