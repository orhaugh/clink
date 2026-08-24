# QUAL-11's plugin-job variants: one source, three .so's, one engine
# revision. The plugin ABI fingerprint is the engine's git SHA, so the
# pair MUST be built at the same commit as the cluster image that runs
# them - the qualification image build turns this on
# (-DCLINK_BUILD_QUAL11_JOBS=ON) and bakes the artefacts in; a host
# build may also enable it for the migration unit test.
#
# Requires clink::kafka (the job's source and sink are Kafka text).

if(NOT TARGET clink::kafka)
    message(FATAL_ERROR "CLINK_BUILD_QUAL11_JOBS requires the kafka impl (clink::kafka)")
endif()

function(qual11_job target defines output)
    add_library(${target} MODULE account_job.cpp)
    target_link_libraries(${target} PRIVATE clink::core clink::kafka)
    if(NOT "${defines}" STREQUAL "")
        target_compile_definitions(${target} PRIVATE ${defines})
    endif()
    set_target_properties(${target} PROPERTIES PREFIX "" OUTPUT_NAME "${output}")
    clink_set_warnings(${target})
endfunction()

qual11_job(clink_qual11_job_v1 "" "qual11_job_v1")
qual11_job(clink_qual11_job_v2 "QUAL11_SCHEMA_V2" "qual11_job_v2")
qual11_job(clink_qual11_job_v2_broken "QUAL11_SCHEMA_V2;QUAL11_BROKEN" "qual11_job_v2_broken")

# Item 30 (ship small modules): same debug-split-and-strip pass the
# example modules get.
clink_split_debug_and_strip_modules_in_dir()
