#pragma once

// The destination half of `clink incident-reconcile`: tools/clink_incident_reconcile.cpp
// reads ClickHouse; tools/clink_incident_reconcile_stub.cpp, built without the ClickHouse
// connector, refuses.

#include <cstdint>
#include <string>

#include "clink/incident/reconcile.hpp"

// The connection the operator gives on the command line. The password comes
// from the environment variable --password-env names, never from an argument
// or a bundle.
struct ClinkReconcileTarget {
    std::string host;
    std::uint16_t port{0};  // 0: 9000, or 9440 with secure
    std::string user{"default"};
    std::string password;
    bool secure{false};
    std::string tls_ca_file;
    bool tls_verify{true};
};

// Whether this build can reconcile ClickHouse at all.
bool clink_incident_reconcile_available();

clink::incident::reconcile::Report clink_incident_reconcile(
    const std::string& bundle,
    const ClinkReconcileTarget& target,
    const clink::incident::reconcile::Options& options);
