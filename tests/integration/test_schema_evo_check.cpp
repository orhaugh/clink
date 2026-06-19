// End-to-end test for the .so-side pre-deploy compatibility check (D).
//
// Loads the schema_evo_test_job fixture .so (build_fn registers a
// counter:1->2->3 migration chain into the .so-local
// StateMigrationRegistry and declares expect_state_version("counter-op",
// "counter", 3)) and calls its clink_job_check_restore_compatibility
// export with crafted "stored" version maps.
//
// The crux this proves: the migrations build_fn registered are visible
// to the export but NOT to this host process - clink_core is statically
// linked and the .so is RTLD_LOCAL, so each side has its own
// StateMigrationRegistry::global(). A host-side check would see no
// migrations and wrongly flag the v1->v3 case incompatible; the .so-side
// check gets it right. That is the whole reason D runs inside the .so.

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

const char* schema_evo_job_path() {
#ifdef CLINK_SCHEMA_EVO_JOB_PATH
    return CLINK_SCHEMA_EVO_JOB_PATH;
#else
    return nullptr;
#endif
}

// dlopen the fixture, call the export with `stored_packed`, return the
// unpacked incompatibility list. `rc_out` receives the raw export return
// code (0 = ran, 1 = build_fn failed, 2 = decode error).
std::vector<clink::StateIncompatibility> check(const std::string& stored_packed, int& rc_out) {
    const char* path = schema_evo_job_path();
    void* handle = ::dlopen(path, RTLD_NOW | RTLD_LOCAL);
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

// The op_id the fixture's expect_state_version("counter-op", ...) keys on.
clink::OperatorId counter_op() {
    return clink::operator_id_from_uid("counter-op");
}

TEST(SchemaEvoCheck, AbsentStoredVersionIsCompatibleViaDefaultV1) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Empty stored map -> the "counter" entry defaults to v1 -> chain
    // 1->2->3 reaches the expected v3 -> compatible.
    int rc = 0;
    const auto incompat = check("", rc);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(incompat.empty());
}

TEST(SchemaEvoCheck, StoredV1IsCompatibleThroughTheMigrationChain) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    clink::StateVersionMap stored;
    stored.set(counter_op(), "counter", 1);
    int rc = 0;
    const auto incompat = check(stored.pack(), rc);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(incompat.empty());
}

TEST(SchemaEvoCheck, StoredV5IsIncompatibleNoDowngradePath) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Stored at v5, expected v3, only forward 1->2->3 registered -> no
    // 5->3 path -> incompatible.
    clink::StateVersionMap stored;
    stored.set(counter_op(), "counter", 5);
    int rc = 0;
    const auto incompat = check(stored.pack(), rc);
    EXPECT_EQ(rc, 0);
    ASSERT_EQ(incompat.size(), 1u);
    EXPECT_EQ(incompat[0].op_id, counter_op());
    EXPECT_EQ(incompat[0].state_type, "counter");
    EXPECT_EQ(incompat[0].from_version, 5u);
    EXPECT_EQ(incompat[0].to_version, 3u);
}

TEST(SchemaEvoCheck, MalformedStoredMapReportsDecodeError) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Not a valid packed StateVersionMap (op_id not numeric) -> the
    // export's unpack throws -> return code 2, distinct from compatible.
    int rc = 0;
    const auto incompat = check("not-a-number|counter|1", rc);
    EXPECT_EQ(rc, 2);
    EXPECT_TRUE(incompat.empty());
}

}  // namespace
