# Core test suite. Links clink::core only - no impl deps. If any
# core test pulls in a vendor header (clink/connectors/kafka_*.hpp,
# clink/state/rocksdb_state_backend.hpp, etc.) the build fails here,
# which is the point: it proves clink_core stands on its own.
#
# Per-impl test suites live under impls/<x>/tests/ and link only
# clink::core + clink::<x>.

add_executable(clink_core_tests
    test_sha256.cpp
    test_remote_read_backend.cpp
    test_state_parquet_export.cpp
    test_window_state_codecs.cpp
    test_typed_state.cpp
    test_session_window_persistence.cpp
    test_evicting_window_persistence.cpp
    test_sliding_window_persistence.cpp
    test_trigger_persistence.cpp
    test_dag_construction.cpp
    test_batch_bounded_execution.cpp
    test_blocking_exchange.cpp
    test_batch_scheduler.cpp
    test_batch_bootstrap.cpp
    test_map_filter.cpp
    test_watermark_propagation.cpp
    test_watermark_strategy.cpp
    test_dag_fork.cpp
    test_periodic_checkpoint.cpp
    test_file_connector.cpp
    test_arrow_batcher.cpp
    test_columnar_batcher.cpp
    test_row_columnar_batcher.cpp
    test_columnar_registration.cpp
    test_columnar_parquet.cpp
    test_parquet_2pc_sink.cpp
    test_columnar_parquet_factories.cpp
    test_parquet_connector.cpp
    test_file_2pc_sink.cpp
    test_state_recovery.cpp
    test_keyed_state.cpp
    test_window_recovery.cpp
    test_dag_union.cpp
    test_dag_join.cpp
    test_join_recovery.cpp
    test_co_operator_unaligned.cpp
    test_dag_self_join.cpp
    test_broadcast_state.cpp
    test_parallel_reduce.cpp
    test_keyed_aggregate_operator.cpp
    test_deadline_keyed_operator.cpp
    test_network_channel.cpp
    test_network_channel_credit.cpp
    test_state_schema_version.cpp
    test_check_savepoint_cli.cpp
    test_restore_compat_gate.cpp
    test_async_task.cpp
    test_async_lookup_operator.cpp
    test_async_pool_primitives.cpp
    test_io_uring_reactor.cpp
    test_io_uring_completion_executor.cpp
    test_http_pool_io_uring.cpp
    test_pid_controller.cpp
    test_rescale_coordinator.cpp
    test_rescale_dispatch.cpp
    test_autoscaler.cpp
    test_operator_metrics.cpp
    test_state_metrics.cpp
    test_checkpoint_metrics.cpp
    test_connector_metrics.cpp
    test_network_metrics.cpp
    test_orchestration_metrics.cpp
    test_async_metrics.cpp
    test_distributed_bridge.cpp
    test_cluster.cpp
    test_ha_coordinator.cpp
    test_key_groups.cpp
    test_service_discovery.cpp
    test_codec.cpp
    test_stream_element.cpp
    test_wire_protocol.cpp
    test_job_graph.cpp
    test_job_planner.cpp
    test_operator_registry.cpp
    test_plugin_registry.cpp
    test_plugin_loader.cpp
    test_flat_map_operator.cpp
    test_key_by_operator.cpp
    test_tumbling_window_operator.cpp
    test_async_tumbling_window_operator.cpp
    test_async_sliding_window_operator.cpp
    test_async_session_window_operator.cpp
    test_gated_timer_fire.cpp
    test_async_keyed_process_function.cpp
    test_async_co_process_function.cpp
    test_watermark_assigner_operator.cpp
    test_metrics_registry.cpp
    test_histogram.cpp
    test_disagg_metrics.cpp
    test_json.cpp
    test_dag_split.cpp
    test_sliding_window_operator.cpp
    test_sliding_window_scenario.cpp
    test_session_window_operator.cpp
    test_window_allowed_lateness.cpp
    test_window_triggers.cpp
    test_window_pane_info.cpp
    test_window_evictors.cpp
    test_subtask_emitter.cpp
    test_keyed_state_multitype.cpp
    test_runtime_context.cpp
    test_operator_lifecycle.cpp
    test_checkpoint_barrier.cpp
    test_in_memory_state.cpp
    test_async_state_get.cpp
    test_async_execution_controller.cpp
    test_completion_executor.cpp
    test_get_many_async.cpp
    test_coalescing_backend.cpp
    test_async_operator_runner.cpp
    test_bounded_channel.cpp
    test_timer_service.cpp
    test_checkpoint_retention.cpp
    test_side_output.cpp
    test_real_connectors_factory.cpp
    test_file_backed_state.cpp
    test_snapshot_worker.cpp
    test_durable_file_write.cpp
    test_columnar_filter.cpp
    test_columnar_string_filter.cpp
    test_columnar_keyed_aggregate.cpp
    test_columnar_keyed_string_aggregate.cpp
    test_columnar_window_agg.cpp
    test_sharded_in_memory_state.cpp
    test_cpu_affinity.cpp
    test_key_group_partitioner.cpp
    test_sharded_keyed_stage.cpp
    test_state_backend_factory.cpp
    test_stream_env.cpp
    test_local_submitter.cpp
    test_inline_ops_e2e.cpp
    test_pipe_throughput_floor.cpp
    test_source_replay.cpp
    test_keyed_state_ttl.cpp
    test_async_map_operator.cpp
    test_dag_iteration.cpp
    test_process_function.cpp
    test_cep.cpp
    test_state_processor.cpp
    test_queryable_state.cpp
    test_changelog_state_backend.cpp
    test_operator_uid.cpp
    test_operator_uid_cluster.cpp
    test_queryable_state_cluster.cpp
    test_queryable_state_routing.cpp
    test_window_late_side_output.cpp
    test_watermark_idleness.cpp
    test_process_window_lateness.cpp
    test_process_window_persistence.cpp
    test_fluent_window_config.cpp
    test_fluent_side_outputs.cpp
    test_watermark_alignment.cpp
    test_throttle_map.cpp
    test_split_by_variant_operator.cpp
    test_json_predicate.cpp
    test_json_value_expr.cpp
    test_decimal.cpp
    test_logging.cpp
)

target_link_libraries(clink_core_tests PRIVATE
    clink::core
    GTest::gtest_main
)
# test_logging.cpp constructs the spdlog-backed sinks directly. spdlog is
# linked PRIVATE to clink_core (header-only) so its include path does not
# reach this test exe; add it here. Harmless for the test binary (it is not a
# plugin .so, so the per-.so registry hazard does not apply).
if(TARGET spdlog::spdlog_header_only)
    target_link_libraries(clink_core_tests PRIVATE spdlog::spdlog_header_only)
endif()
clink_set_warnings(clink_core_tests)

# ---------------------------------------------------------------------------
# Example plugin shared libraries for the plugin-loader tests.
#
# Built as real .dylib / .so against the clink public headers; the
# loader test dlopens them and verifies the registration path. The
# library has NO link to libclink - it only consumes the header-only
# parts and inline templates. The clink symbols it references (codec
# helpers, NetworkBridgeSource/Sink templates, registry classes)
# resolve to whatever's loaded in the host process at dlopen time, so
# we don't get duplicate-symbol diagnostics or ABI drift.
# ---------------------------------------------------------------------------
add_library(clink_hello_plugin MODULE
    plugin_examples/hello_plugin.cpp
)
target_link_libraries(clink_hello_plugin PRIVATE clink::core)
set_target_properties(clink_hello_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "hello_plugin"
)
clink_set_warnings(clink_hello_plugin)

add_library(clink_gateway_plugin MODULE
    plugin_examples/gateway_plugin.cpp
)
target_link_libraries(clink_gateway_plugin PRIVATE clink::core)
set_target_properties(clink_gateway_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "gateway_plugin"
)
clink_set_warnings(clink_gateway_plugin)

# Make the plugin's on-disk path available to the test as a
# preprocessor define. $<TARGET_FILE:...> resolves to the per-config
# build artifact location.
target_compile_definitions(clink_core_tests PRIVATE
    CLINK_HELLO_PLUGIN_PATH=\"$<TARGET_FILE:clink_hello_plugin>\"
    CLINK_CLI_BINARY=\"$<TARGET_FILE:clink>\"
    CLINK_SCHEMA_EVO_JOB_PATH=\"$<TARGET_FILE:clink_schema_evo_test_job>\")
add_dependencies(clink_core_tests clink_hello_plugin clink clink_schema_evo_test_job)

gtest_discover_tests(clink_core_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS core
)

# ---------------------------------------------------------------------------
# Integration tests. Cross-cutting tests that exercise full pipelines
# end-to-end across multiple impls. Need external services (Docker for
# now); off by default, opt in with -DCLINK_INTEGRATION_TESTS=ON.
# Each test self-skips when its required services aren't reachable.
#
# These tests link clink::clink (the kitchen-sink aggregator)
# because by their nature they need every available impl: Postgres
# CDC + Kafka topics + plugin submission etc. The core-isolation
# invariant doesn't apply here - these are deliberately cross-cutting.
# ---------------------------------------------------------------------------
if(CLINK_INTEGRATION_TESTS)
    add_executable(clink_integration_tests
        integration/test_postgres_pipeline.cpp
        integration/test_postgres_sink.cpp
        integration/test_postgres_cdc.cpp
        integration/test_postgres_cdc_pgoutput.cpp
        integration/test_multiprocess_cluster.cpp
        integration/test_plugin_submission.cpp
        integration/test_application_mode.cpp
        integration/test_list_jobs.cpp
        integration/test_stream_env_end_to_end.cpp
        integration/test_gateway_parity.cpp
        integration/test_job_plugin_e2e.cpp
        integration/test_job_bundle_isolation.cpp
        integration/test_application_mode_e2e.cpp
        integration/test_heavy_pipeline_e2e.cpp
        integration/test_cross_process_bench.cpp
        integration/test_cancel_job_e2e.cpp
        integration/test_sigterm_shutdown.cpp
        integration/test_http_health.cpp
        integration/test_http_read_api.cpp
        integration/test_http_federation.cpp
        integration/test_http_observability.cpp
        integration/test_http_sse.cpp
        integration/test_http_dashboard.cpp
        integration/test_http_submit.cpp
        integration/test_cluster_tls.cpp
        integration/test_two_phase_commit.cpp
        integration/test_tm_crash_recovery.cpp
        integration/test_jm_ha_failover.cpp
        integration/test_jm_rescale.cpp
        integration/test_schema_evo_check.cpp
        test_helpers/register_linked_impls.cpp
    )
    target_link_libraries(clink_integration_tests PRIVATE
        clink::clink
        GTest::gtest_main
    )
    target_include_directories(clink_integration_tests PRIVATE
        ${CMAKE_SOURCE_DIR})
    foreach(_impl kafka postgres clickhouse s3 rocksdb tls)
        if(TARGET clink::${_impl})
            string(TOUPPER ${_impl} _IMPL_UC)
            target_compile_definitions(clink_integration_tests PRIVATE
                CLINK_LINKED_${_IMPL_UC}
                CLINK_HAS_${_IMPL_UC}
            )
        endif()
    endforeach()
    if(TARGET clink::s3)
        target_compile_definitions(clink_integration_tests PRIVATE CLINK_HAS_AWS_S3)
    endif()
    if(TARGET clink::tls)
        target_compile_definitions(clink_integration_tests PRIVATE CLINK_HAS_OPENSSL)
    endif()
    if(TARGET clink::postgres)
        # Integration tests drive a real Postgres instance via libpq
        # directly. Re-run find_package here so PostgreSQL::PostgreSQL
        # is reliably visible at this scope.
        find_package(PostgreSQL REQUIRED)
        target_link_libraries(clink_integration_tests PRIVATE PostgreSQL::PostgreSQL)
    else()
        message(WARNING "CLINK_INTEGRATION_TESTS=ON but clink::postgres not built; "
                        "the integration tests will fail to build")
    endif()
    target_compile_definitions(clink_integration_tests PRIVATE
        CLINK_NODE_BINARY=\"$<TARGET_FILE:clink_node>\"
        CLINK_SUBMIT_BINARY=\"$<TARGET_FILE:clink_submit_job>\"
        CLINK_APP_BINARY=\"$<TARGET_FILE:clink_app>\"
        CLINK_RESCALE_BINARY=\"$<TARGET_FILE:clink_rescale_job>\"
        CLINK_RESCALE_TEST_JOB_PATH=\"$<TARGET_FILE:clink_rescale_test_job>\"
        CLINK_HELLO_PLUGIN_PATH=\"$<TARGET_FILE:clink_hello_plugin>\"
        CLINK_GATEWAY_PLUGIN_PATH=\"$<TARGET_FILE:clink_gateway_plugin>\"
        CLINK_CANONICAL_JOB_PATH=\"$<TARGET_FILE:clink_canonical_pipeline_job>\"
        CLINK_COLLISION_JOB_A_PATH=\"$<TARGET_FILE:clink_collision_job_a>\"
        CLINK_COLLISION_JOB_B_PATH=\"$<TARGET_FILE:clink_collision_job_b>\"
        CLINK_HEAVY_JOB_PATH=\"$<TARGET_FILE:clink_heavy_pipeline_job>\"
        CLINK_BENCH_JOB_PATH=\"$<TARGET_FILE:clink_bench_pipeline_job>\"
        CLINK_CANCEL_BINARY=\"$<TARGET_FILE:clink_cancel_job>\"
        CLINK_CANCEL_TEST_JOB_PATH=\"$<TARGET_FILE:clink_cancel_test_job>\"
        CLINK_TWO_PHASE_COMMIT_JOB_PATH=\"$<TARGET_FILE:clink_two_phase_commit_job>\"
        CLINK_SCHEMA_EVO_JOB_PATH=\"$<TARGET_FILE:clink_schema_evo_test_job>\")
    add_dependencies(clink_integration_tests
        clink_node
        clink_submit_job
        clink_app
        clink_cancel_job
        clink_rescale_job
        clink_hello_plugin
        clink_gateway_plugin
        clink_canonical_pipeline_job
        clink_collision_job_a
        clink_collision_job_b
        clink_heavy_pipeline_job
        clink_bench_pipeline_job
        clink_cancel_test_job
        clink_two_phase_commit_job
        clink_rescale_test_job
        clink_schema_evo_test_job)
    clink_set_warnings(clink_integration_tests)
    gtest_discover_tests(clink_integration_tests
        DISCOVERY_MODE PRE_TEST
        PROPERTIES TIMEOUT 120 LABELS integration)

    # Failover + cold-start benchmark. A standalone (non-gtest) driver that
    # spawns real clink_node JM/TM processes, times the cold-start path and
    # a TM-crash recovery, and prints a clink-only scoreboard. It reuses the
    # same cluster binaries + 2PC job .so as the crash-recovery test, so it
    # lives under the integration gate. Labelled 'benchmark' so it is opt-in
    # (ctest -L benchmark) and not part of the normal integration run.
    add_executable(clink_failover_coldstart_bench
        integration/bench_failover_coldstart.cpp)
    target_link_libraries(clink_failover_coldstart_bench PRIVATE clink::core)
    target_include_directories(clink_failover_coldstart_bench PRIVATE ${CMAKE_SOURCE_DIR})
    target_compile_definitions(clink_failover_coldstart_bench PRIVATE
        CLINK_NODE_BINARY=\"$<TARGET_FILE:clink_node>\"
        CLINK_SUBMIT_BINARY=\"$<TARGET_FILE:clink_submit_job>\"
        CLINK_TWO_PHASE_COMMIT_JOB_PATH=\"$<TARGET_FILE:clink_two_phase_commit_job>\")
    add_dependencies(clink_failover_coldstart_bench
        clink_node
        clink_submit_job
        clink_two_phase_commit_job)
    clink_set_warnings(clink_failover_coldstart_bench)
    add_test(NAME FailoverColdStartBench COMMAND clink_failover_coldstart_bench)
    set_tests_properties(FailoverColdStartBench PROPERTIES TIMEOUT 300 LABELS benchmark)
endif()

# ---------------------------------------------------------------------------
# SQL frontend tests. Built when CLINK_BUILD_SQL is ON. Pure-parser
# and pure-planner tests live here; integration tests that drive a
# real Kafka/ClickHouse cluster live alongside the rest of the
# integration suite.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_SQL)
    add_executable(clink_sql_tests
        test_sql_parser.cpp
        test_sql_catalog.cpp
        test_sql_binder.cpp
        test_sql_physical.cpp
        test_sql_cli.cpp
        test_sql_row.cpp
        test_sql_runtime.cpp
        test_sql_metrics.cpp
        test_async_function_registry.cpp
        test_table_api.cpp
        test_sql_preparse.cpp
    )
    target_link_libraries(clink_sql_tests PRIVATE
        clink::sql
        GTest::gtest_main
    )
    target_compile_definitions(clink_sql_tests PRIVATE
        CLINK_SUBMIT_SQL_BINARY=\"$<TARGET_FILE:clink_submit_sql>\"
        CLINK_NODE_BINARY=\"$<TARGET_FILE:clink_node>\")
    add_dependencies(clink_sql_tests clink_submit_sql clink_node)
    clink_set_warnings(clink_sql_tests)
    gtest_discover_tests(clink_sql_tests
        DISCOVERY_MODE PRE_TEST
        PROPERTIES TIMEOUT 30 LABELS sql
    )
endif()

# SQL-on-disaggregated-state end-to-end. Needs both the SQL frontend and the
# S3 impl (which provides the remote-read:// state-backend scheme), so it only
# builds when the AWS SDK was found (clink::s3 exists). Live tests gate on
# CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET at runtime; skipped otherwise.
if(CLINK_BUILD_SQL AND TARGET clink::s3)
    add_executable(clink_sql_s3_e2e_tests test_sql_remote_read_e2e.cpp)
    target_link_libraries(clink_sql_s3_e2e_tests PRIVATE
        clink::sql
        clink::s3
        GTest::gtest_main
    )
    clink_set_warnings(clink_sql_s3_e2e_tests)
    gtest_discover_tests(clink_sql_s3_e2e_tests
        DISCOVERY_MODE PRE_TEST
        PROPERTIES TIMEOUT 120 LABELS "sql;s3"
    )
endif()
