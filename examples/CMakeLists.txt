add_executable(clink_word_count word_count.cpp)
target_link_libraries(clink_word_count PRIVATE clink::clink)
clink_set_warnings(clink_word_count)

add_executable(clink_event_time_tumbling event_time_tumbling.cpp)
target_link_libraries(clink_event_time_tumbling PRIVATE clink::clink)
clink_set_warnings(clink_event_time_tumbling)

add_executable(clink_file_word_count file_word_count.cpp)
target_link_libraries(clink_file_word_count PRIVATE clink::clink)
clink_set_warnings(clink_file_word_count)

add_executable(clink_clickstream_join clickstream_join.cpp)
target_link_libraries(clink_clickstream_join PRIVATE clink::clink)
clink_set_warnings(clink_clickstream_join)

# k8s_smoke_job: a tiny bounded, stateless CLINK_REGISTER_JOB pipeline submitted
# by deploy/helm/clink/kind-smoke.sh to prove job submission works on a
# Kubernetes-deployed cluster. Baked into the runtime image at /opt/clink/jobs.
add_library(clink_k8s_smoke_job MODULE k8s_smoke_job.cpp)
target_link_libraries(clink_k8s_smoke_job PRIVATE clink::core)
set_target_properties(clink_k8s_smoke_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "k8s_smoke_job"
)
clink_set_warnings(clink_k8s_smoke_job)

# canonical_pipeline_job: a CLINK_REGISTER_JOB-wrapped pipeline built as a
# shared library. The clink_submit_job CLI dlopens this and ships it
# to the cluster. The .so links against clink::core so the inline
# Pipeline templates / RunnerRegistry singletons it
# touches resolve symbols in this process; on the worker side dlopen
# resolves the same symbols against the host clink_node binary.
add_library(clink_canonical_pipeline_job MODULE canonical_pipeline_job.cpp)
target_link_libraries(clink_canonical_pipeline_job PRIVATE clink::core)
set_target_properties(clink_canonical_pipeline_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "canonical_pipeline_job"
)
clink_set_warnings(clink_canonical_pipeline_job)

# Bundle-isolation pair: two CLINK_REGISTER_JOB .so's whose inline
# lambdas mint overlapping _inline_<kind>_<n> names. Used by
# JobBundleIsolation.ConcurrentJobsWithCollidingInlineNamesDontTrample
# to verify per-job bundles keep their registrations separate.
foreach(_letter a b)
    add_library(clink_collision_job_${_letter} MODULE collision_job_${_letter}.cpp)
    target_link_libraries(clink_collision_job_${_letter} PRIVATE clink::core)
    set_target_properties(clink_collision_job_${_letter} PROPERTIES
        PREFIX ""
        OUTPUT_NAME "collision_job_${_letter}"
    )
    clink_set_warnings(clink_collision_job_${_letter})
endforeach()

# heavy_pipeline_job: 99-customer pipeline (Customer -> Order key_by/reduce,
# sink at parallelism=3). Exercises typed channels, custom codecs, and
# cross-worker wire traffic. Drives test_heavy_pipeline_e2e.cpp.
add_library(clink_heavy_pipeline_job MODULE heavy_pipeline_job.cpp)
target_link_libraries(clink_heavy_pipeline_job PRIVATE clink::core)
set_target_properties(clink_heavy_pipeline_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "heavy_pipeline_job"
)
clink_set_warnings(clink_heavy_pipeline_job)

# bench_pipeline_job: minimal throughput target for the cross-process
# benchmark (test_cross_process_bench.cpp).
add_library(clink_bench_pipeline_job MODULE bench_pipeline_job.cpp)
target_link_libraries(clink_bench_pipeline_job PRIVATE clink::core)
set_target_properties(clink_bench_pipeline_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "bench_pipeline_job"
)
clink_set_warnings(clink_bench_pipeline_job)

# cancel_test_job: long-running source that only stops on CancelJob.
# Drives test_cancel_job_e2e.cpp.
add_library(clink_cancel_test_job MODULE cancel_test_job.cpp)
target_link_libraries(clink_cancel_test_job PRIVATE clink::core)
set_target_properties(clink_cancel_test_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "cancel_test_job"
)
clink_set_warnings(clink_cancel_test_job)

# two_phase_commit_job: bounded slow source -> file_2pc_sink_string.
# Drives test_two_phase_commit.cpp.
add_library(clink_two_phase_commit_job MODULE two_phase_commit_job.cpp)
target_link_libraries(clink_two_phase_commit_job PRIVATE clink::core)

# Follow-up 14: a job whose SIDE OUTPUT is held to the same exactly-once
# standard as its main branch across a worker failure.
add_library(clink_side_output_recovery_job MODULE side_output_recovery_job.cpp)
target_link_libraries(clink_side_output_recovery_job PRIVATE clink::core)
set_target_properties(clink_two_phase_commit_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "two_phase_commit_job"
)
clink_set_warnings(clink_two_phase_commit_job)

# postgres_2pc_job: the Postgres arm of the multi-connector exactly-once
# coverage - the same bounded source, committed via PREPARE TRANSACTION /
# COMMIT PREPARED. The job CARRIES the connector (clink_node links no
# impls), so it links clink::postgres and installs its factories into the
# job bundle registry. Only built when the Postgres impl is.
if(TARGET clink::postgres)
    add_library(clink_postgres_2pc_job MODULE postgres_2pc_job.cpp)
    target_link_libraries(clink_postgres_2pc_job PRIVATE clink::core clink::postgres)
    set_target_properties(clink_postgres_2pc_job PROPERTIES
        PREFIX ""
        OUTPUT_NAME "postgres_2pc_job"
    )
    clink_set_warnings(clink_postgres_2pc_job)
endif()

# kafka_2pc_job: the Kafka arm of the same coverage - the shared bounded
# source into a broker transaction committed at CommitCheckpoint. Carries
# clink::kafka the same way.
if(TARGET clink::kafka)
    add_library(clink_kafka_2pc_job MODULE kafka_2pc_job.cpp)
    target_link_libraries(clink_kafka_2pc_job PRIVATE clink::core clink::kafka)
    set_target_properties(clink_kafka_2pc_job PROPERTIES
        PREFIX ""
        OUTPUT_NAME "kafka_2pc_job"
    )
    clink_set_warnings(clink_kafka_2pc_job)
endif()

# rescale_exactly_once_job: replayable source -> keyed self-checking counter
# (rescale bounds declared) -> 2PC sink. Written to demonstrate exactly-once
# across a rescale; it instead established that a running job cannot be
# rescaled, and now drives the tests that pin the refusals.
add_library(clink_rescale_exactly_once_job MODULE rescale_exactly_once_job.cpp)
target_link_libraries(clink_rescale_exactly_once_job PRIVATE clink::core)
set_target_properties(clink_rescale_exactly_once_job PROPERTIES
    PREFIX "" SUFFIX ".so")
clink_set_warnings(clink_rescale_exactly_once_job)

# two_sink_commit_group_job: one source fanned out to TWO 2PC sinks sharing a
# commit_group. Drives the cross-sink atomicity tests - sinks in a group are
# supposed to commit as a unit, and nothing exercised that under failure.
add_library(clink_two_sink_commit_group_job MODULE two_sink_commit_group_job.cpp)
target_link_libraries(clink_two_sink_commit_group_job PRIVATE clink::core)
set_target_properties(clink_two_sink_commit_group_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "two_sink_commit_group_job"
)
clink_set_warnings(clink_two_sink_commit_group_job)

# rescale_test_job: slow generator source -> keyBy -> reduce(sum) ->
# file sink. Drives test_coordinator_rescale.cpp; the source's per-record
# sleep keeps the pipeline alive long enough to checkpoint and then
# rescale via clink_rescale_job.
add_library(clink_rescale_test_job MODULE rescale_test_job.cpp)
target_link_libraries(clink_rescale_test_job PRIVATE clink::core)
set_target_properties(clink_rescale_test_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "rescale_test_job"
)
clink_set_warnings(clink_rescale_test_job)

# schema_evo_test_job: trivial pipeline whose build_fn registers a
# counter:1->2->3 migration chain and declares expect_state_version v3.
# Fixture for the .so-side pre-deploy compatibility check (D).
add_library(clink_schema_evo_test_job MODULE schema_evo_test_job.cpp)
target_link_libraries(clink_schema_evo_test_job PRIVATE clink::core)
set_target_properties(clink_schema_evo_test_job PROPERTIES
    PREFIX ""
    OUTPUT_NAME "schema_evo_test_job"
)
clink_set_warnings(clink_schema_evo_test_job)

# Item 30: ship ~5 MB job modules, not ~104 MB ones. Applies to every MODULE
# target defined above; the DWARF moves whole into <module>.so.debug beside
# the artefact (see cmake/ClinkPluginDebugSplit.cmake).
clink_split_debug_and_strip_modules_in_dir()
if(CLINK_BUILD_TESTS AND UNIX AND NOT APPLE)
    add_test(NAME job_plugin_ships_stripped
        COMMAND "${CMAKE_COMMAND}" -DBINARY=$<TARGET_FILE:clink_heavy_pipeline_job>
                -P "${CMAKE_SOURCE_DIR}/cmake/CheckPluginStripped.cmake")
    set_tests_properties(job_plugin_ships_stripped PROPERTIES LABELS "core")
endif()
