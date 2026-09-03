# QUAL-11's plugin-job variants: one source, three .so's, one engine
# revision. The plugin ABI fingerprint hashes the declared extension surface
# (scripts/plugin-abi-surface.txt), so the pair must be built against the
# same surface as the cluster image that runs them - the qualification image
# build turns this on (-DCLINK_BUILD_QUAL11_JOBS=ON) and bakes the artefacts
# in; a host build may also enable it for the migration unit test.
#
# Requires clink::kafka (the job's source and sink are Kafka text).

if(NOT TARGET clink::kafka)
    message(FATAL_ERROR "CLINK_BUILD_QUAL11_JOBS requires the kafka impl (clink::kafka)")
endif()

clink_add_job_module(clink_qual11_job_v1
    SOURCES account_job.cpp
    OUTPUT_NAME qual11_job_v1
    LINK clink::kafka)
clink_add_job_module(clink_qual11_job_v2
    SOURCES account_job.cpp
    OUTPUT_NAME qual11_job_v2
    LINK clink::kafka
    DEFINES QUAL11_SCHEMA_V2)
clink_add_job_module(clink_qual11_job_v2_broken
    SOURCES account_job.cpp
    OUTPUT_NAME qual11_job_v2_broken
    LINK clink::kafka
    DEFINES QUAL11_SCHEMA_V2 QUAL11_BROKEN)
