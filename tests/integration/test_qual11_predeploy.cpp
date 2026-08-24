// QUAL-11's pre-deploy gate, exercised against the campaign's own three
// job variants (gate 2 of the campaign is the same assertion made on a
// live rig; this is the fast half that runs in every suite).
//
// The claim under test is the .so-side compatibility check on the REAL
// campaign artefacts, not a fixture: given a savepoint stamped
// account_state@1 (what the v1 job writes),
//
//   * the v1 .so accepts it (expected version 1, nothing to migrate);
//   * the v2 .so accepts it (expected 2, its registered 1->2 migration
//     reaches it);
//   * the v2-BROKEN .so - identical to v2 except the migration is not
//     registered - must REFUSE it, naming the account-agg operator and
//     the account_state type. This is the campaign's negative control:
//     a check that approves the broken variant approves anything, and
//     the rig campaign would then "prove" fail-loudly with a gate that
//     cannot fail.
//
// The check runs .so-side by design (the migration registry is
// .so-local under RTLD_LOCAL with clink statically linked), so these
// tests dlopen exactly like the coordinator's pre-deploy path does.

#include <cstddef>
#include <cstring>
#include <dlfcn.h>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace {

const char* job_path(const char* which) {
#ifdef CLINK_QUAL11_V1_PATH
    if (std::strcmp(which, "v1") == 0) {
        return CLINK_QUAL11_V1_PATH;
    }
#endif
#ifdef CLINK_QUAL11_V2_PATH
    if (std::strcmp(which, "v2") == 0) {
        return CLINK_QUAL11_V2_PATH;
    }
#endif
#ifdef CLINK_QUAL11_V2_BROKEN_PATH
    if (std::strcmp(which, "v2_broken") == 0) {
        return CLINK_QUAL11_V2_BROKEN_PATH;
    }
#endif
    (void)which;
    return nullptr;
}

std::vector<clink::StateIncompatibility> check(const char* so_path,
                                               const std::string& stored_packed,
                                               int& rc_out) {
    void* handle = ::dlopen(so_path, RTLD_NOW | RTLD_LOCAL);
    EXPECT_NE(handle, nullptr) << ::dlerror();
    if (handle == nullptr) {
        rc_out = -1;
        return {};
    }
    using CheckFn = int (*)(const char*, const char**, std::size_t*);
    auto sym = ::dlsym(handle, "clink_job_check_restore_compatibility");
    EXPECT_NE(sym, nullptr) << ".so missing clink_job_check_restore_compatibility";
    if (sym == nullptr) {
        ::dlclose(handle);
        rc_out = -1;
        return {};
    }
    CheckFn fn = nullptr;
    std::memcpy(&fn, &sym, sizeof(fn));
    const char* out_packed = nullptr;
    std::size_t out_size = 0;
    rc_out = fn(stored_packed.c_str(), &out_packed, &out_size);
    std::string packed{out_packed != nullptr ? out_packed : "", out_size};
    ::dlclose(handle);
    if (rc_out != 0) {
        return {};
    }
    return clink::unpack_incompatibilities(packed);
}

clink::OperatorId account_op() {
    return clink::operator_id_from_uid("account-agg");
}

std::string stored_v1() {
    clink::StateVersionMap stored;
    stored.set(account_op(), "account_state", 1);
    return stored.pack();
}

}  // namespace

TEST(Qual11Predeploy, V1JobAcceptsItsOwnSavepoint) {
    const char* path = job_path("v1");
    if (path == nullptr) {
        GTEST_SKIP() << "qual11 job plugins not built (CLINK_BUILD_QUAL11_JOBS=OFF)";
    }
    int rc = 0;
    const auto incompat = check(path, stored_v1(), rc);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(incompat.empty()) << "v1 -> v1 needs no migration and must be compatible";
}

TEST(Qual11Predeploy, V2JobAcceptsAV1SavepointThroughItsMigration) {
    const char* path = job_path("v2");
    if (path == nullptr) {
        GTEST_SKIP() << "qual11 job plugins not built (CLINK_BUILD_QUAL11_JOBS=OFF)";
    }
    int rc = 0;
    const auto incompat = check(path, stored_v1(), rc);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(incompat.empty())
        << "the registered account_state 1->2 migration must make a v1 savepoint restorable";
}

TEST(Qual11Predeploy, BrokenV2JobIsRefusedAndNamesTheGap) {
    const char* path = job_path("v2_broken");
    if (path == nullptr) {
        GTEST_SKIP() << "qual11 job plugins not built (CLINK_BUILD_QUAL11_JOBS=OFF)";
    }
    int rc = 0;
    const auto incompat = check(path, stored_v1(), rc);
    EXPECT_EQ(rc, 0);
    ASSERT_EQ(incompat.size(), 1U)
        << "v2 without the migration must be refused - a check that approves it approves "
           "anything (the campaign's negative control would be inert)";
    EXPECT_EQ(incompat[0].op_id, account_op());
    EXPECT_EQ(incompat[0].state_type, "account_state");
    EXPECT_EQ(incompat[0].from_version, 1U);
    EXPECT_EQ(incompat[0].to_version, 2U);
}

TEST(Qual11Predeploy, V2JobAcceptsACurrentV2Savepoint) {
    const char* path = job_path("v2");
    if (path == nullptr) {
        GTEST_SKIP() << "qual11 job plugins not built (CLINK_BUILD_QUAL11_JOBS=OFF)";
    }
    clink::StateVersionMap stored;
    stored.set(account_op(), "account_state", 2);
    int rc = 0;
    const auto incompat = check(path, stored.pack(), rc);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(incompat.empty());
}

TEST(Qual11Predeploy, V1JobRefusesAV2SavepointNoDowngradePath) {
    const char* path = job_path("v1");
    if (path == nullptr) {
        GTEST_SKIP() << "qual11 job plugins not built (CLINK_BUILD_QUAL11_JOBS=OFF)";
    }
    // Rollback direction: a v2-era savepoint offered to the v1 job. No
    // 2->1 migration exists, so the check must refuse rather than let the
    // v1 codec misread 32-byte values.
    clink::StateVersionMap stored;
    stored.set(account_op(), "account_state", 2);
    int rc = 0;
    const auto incompat = check(path, stored.pack(), rc);
    EXPECT_EQ(rc, 0);
    ASSERT_EQ(incompat.size(), 1U);
    EXPECT_EQ(incompat[0].from_version, 2U);
    EXPECT_EQ(incompat[0].to_version, 1U);
}
