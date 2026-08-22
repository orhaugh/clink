// clink state-sweep - reclaim unreferenced state objects from a
// disaggregated backend's store.
//
// The reclaimer already existed and could not be run. S3RemotePool::sweep
// and S3CasSnapshotStore::sweep delete every value object no live
// manifest references, and both are documented as "intended for an admin
// / periodic trigger" - but nothing in the engine called either one: no
// CLI, no endpoint, no periodic task. So the store grew with UPDATE
// VOLUME rather than with live state, for ever.
//
// QUAL-04 measured what that costs. Live keyed state sat flat while the
// store went 0.13 -> 2.69 GiB in ten minutes on a small local run, and a
// 30 GiB rig run finished holding 84 GiB of objects - 2.7x live, still
// climbing, with nothing that could ever reclaim the difference. It is
// not a correctness problem: purge() bounds the live manifest set and
// restores read only referenced objects. It is unbounded storage, and on
// a metered object store unbounded cost.
//
//   clink state-sweep --backend=<uri> [--min-age-s=N] [--dry-run]
//
// SAFETY. A sweep races an in-flight checkpoint whose objects are
// uploaded but whose manifest is not yet written: those look exactly like
// orphans. --min-age-s is the guard, and it must exceed the longest
// checkpoint persist the job performs, so an in-flight checkpoint's
// objects survive until its manifest lands. The default is deliberately
// generous rather than convenient. The underlying sweep is also
// conservative on its own account: if any manifest is unreadable it
// deletes nothing, because it cannot then prove an object is
// unreferenced.
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

#ifdef CLINK_LINKED_S3
#include "clink/s3/s3_remote_pool.hpp"
#endif

namespace {

std::string arg_value(int argc, char** argv, const char* name, const std::string& fallback) {
    const std::string prefix = std::string("--") + name + "=";
    for (int i = 0; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.rfind(prefix, 0) == 0) {
            return a.substr(prefix.size());
        }
    }
    return fallback;
}

bool has_flag(int argc, char** argv, const char* name) {
    const std::string want = std::string("--") + name;
    for (int i = 0; i < argc; ++i) {
        if (want == argv[i]) {
            return true;
        }
    }
    return false;
}

void usage() {
    std::cerr << "Usage: clink state-sweep --backend=<uri> [--min-age-s=N] [--dry-run]\n"
              << "\n"
              << "  Deletes state objects that no live checkpoint manifest references.\n"
              << "  Supported backend: remote-read://<bucket>/<prefix>"
                 "[?endpoint=<url>&region=<r>]\n"
              << "\n"
              << "  --min-age-s=N  never delete an object younger than N seconds\n"
              << "                 (default 3600). MUST exceed the longest checkpoint\n"
              << "                 persist the job performs: an in-flight checkpoint's\n"
              << "                 objects are indistinguishable from orphans until its\n"
              << "                 manifest is written.\n"
              << "  --dry-run      report what a sweep would reclaim; delete nothing.\n";
}

}  // namespace

int clink_cmd_state_sweep(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return 2;
    }
    const std::string backend = arg_value(argc, argv, "backend", "");
    if (backend.empty()) {
        std::cerr << "state-sweep: --backend is required\n";
        usage();
        return 2;
    }
    const auto min_age =
        std::chrono::seconds{std::stoll(arg_value(argc, argv, "min-age-s", "3600"))};
    const bool dry_run = has_flag(argc, argv, "dry-run");

#ifndef CLINK_LINKED_S3
    (void)min_age;
    (void)dry_run;
    std::cerr << "state-sweep: this build has no S3 impl linked, so no disaggregated\n"
                 "  store can be swept. Rebuild with the AWS SDK available.\n";
    return 2;
#else
    constexpr const char* kScheme = "remote-read://";
    if (backend.rfind(kScheme, 0) != 0) {
        std::cerr << "state-sweep: unsupported backend '" << backend
                  << "'. Only remote-read:// stores have a reclaimable object tier.\n";
        return 2;
    }
    std::string rest = backend.substr(std::strlen(kScheme));
    std::string query;
    if (const auto q = rest.find('?'); q != std::string::npos) {
        query = rest.substr(q + 1);
        rest = rest.substr(0, q);
    }
    const auto slash = rest.find('/');
    if (slash == std::string::npos || slash == 0) {
        std::cerr << "state-sweep: backend must be remote-read://<bucket>/<prefix>\n";
        return 2;
    }

    clink::s3::S3RemotePool::Options opts;
    opts.bucket = rest.substr(0, slash);
    opts.prefix = rest.substr(slash + 1);
    // Same query keys the backend factory accepts, so an operator can paste
    // the job's own --state-backend URI verbatim.
    std::size_t pos = 0;
    while (pos < query.size()) {
        const auto amp = query.find('&', pos);
        const std::string kv = query.substr(pos, amp == std::string::npos ? amp : amp - pos);
        const auto eq = kv.find('=');
        if (eq != std::string::npos) {
            const std::string k = kv.substr(0, eq);
            const std::string v = kv.substr(eq + 1);
            if (k == "endpoint") {
                opts.endpoint_override = v;
            } else if (k == "region") {
                opts.region = v;
            } else if (k == "anonymous") {
                opts.allow_anonymous = (v == "1" || v == "true");
            }
        }
        if (amp == std::string::npos) {
            break;
        }
        pos = amp + 1;
    }

    clink::s3::S3RemotePool pool{opts};
    if (dry_run) {
        // Nothing destructive: report the live/total split so an operator
        // can see the reclaimable volume before running for real.
        const auto reclaimable = pool.reclaimable_bytes(min_age);
        std::cout << "state-sweep: " << reclaimable.objects << " object(s), " << reclaimable.bytes
                  << " bytes are unreferenced and older than " << min_age.count()
                  << "s (dry run, nothing deleted)\n";
        return 0;
    }
    const auto reclaimed = pool.sweep(min_age);
    std::cout << "state-sweep: reclaimed " << reclaimed << " unreferenced object(s) older than "
              << min_age.count() << "s from " << opts.bucket << "/" << opts.prefix << "\n";
    return 0;
#endif
}
