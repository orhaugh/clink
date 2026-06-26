add_executable(clink_bench clink_bench.cpp)
target_link_libraries(clink_bench PRIVATE clink::core)
clink_set_warnings(clink_bench)

# Async-state throughput benchmark - async-vs-sync overlap of cold reads on a
# deferring (RemoteReadBackend) tier with a controlled-latency synthetic loader.
# Proves the per-key-gated async path overlaps remote reads the synchronous path
# serialises. Self-validating (hard-fails if it stops measuring real cold IO).
add_executable(clink_async_state_bench clink_async_state_bench.cpp)
target_link_libraries(clink_async_state_bench PRIVATE clink::core)
clink_set_warnings(clink_async_state_bench)

# Async-state SCALE benchmark - the per-key overlap win composed with
# share-nothing data parallelism via ShardedKeyedStage: each shard owns a
# private deferring backend, so async overlaps cold reads within each shard AND
# across S shards, while the sync baseline only gets the parallelism. Synthetic
# controlled-latency loader (the honest remote-read model); self-validating.
add_executable(clink_async_scale_bench clink_async_scale_bench.cpp)
target_link_libraries(clink_async_scale_bench PRIVATE clink::core)
clink_set_warnings(clink_async_scale_bench)

# M-Fast benchmark - quantifies the ASYNC-10 (read coalescing: N per-key
# round-trips collapse to 1 batched) + ASYNC-12 (deadline-aware resume: urgent
# reads served first within a completion batch) wins, clink-vs-clink A/B on a
# controlled-latency pool/parking backend. Reports the premise-free counter
# (round-trip count / emit position) alongside wall-clock; self-validating.
# (The ASYNC-9A io_threads fix is the io_threads sweep in clink_async_state_bench.)
add_executable(clink_mfast_bench clink_mfast_bench.cpp)
target_link_libraries(clink_mfast_bench PRIVATE clink::core)
clink_set_warnings(clink_mfast_bench)

# Serde benchmark - compares the legacy record-by-record Codec<T>
# wire encoding against the Arrow RecordBatch + IPC path. The Arrow
# path is the production path; the legacy path is benched as a
# reference. CLINK_HAS_ARROW is always defined on clink::core.
add_executable(clink_serde_bench clink_serde_bench.cpp)
target_link_libraries(clink_serde_bench PRIVATE clink::core)
clink_set_warnings(clink_serde_bench)

# Nexmark benchmark - the Nexmark streaming benchmark
# (github.com/nexmark/nexmark) as clink SQL jobs over a generated event stream
# (connector='nexmark') on an in-process JM/TM cluster, output discarded through
# a blackhole sink. Reports input-event throughput. Needs the SQL frontend.
if(TARGET clink::sql)
    add_executable(clink_nexmark_bench clink_nexmark_bench.cpp)
    target_link_libraries(clink_nexmark_bench PRIVATE clink::sql)
    clink_set_warnings(clink_nexmark_bench)

    # nexmark_compare/INC 2: shared-input producer. Runs the Nexmark generator
    # once and dumps the canonical dataset as per-type NDJSON for the
    # cross-engine harness to load into Kafka (both engines read identical bytes).
    add_executable(nexmark_dump nexmark_compare/nexmark_dump.cpp)
    target_link_libraries(nexmark_dump PRIVATE clink::sql)
    clink_set_warnings(nexmark_dump)

    # Columnar windowed-GROUP-BY benchmark - isolates the columnar-execution wave
    # (WS3 within-batch group-by + WS4 open-addressing state map) on a Parquet
    # (columnar-native) source that actually fires WindowRowOp::process_columnar,
    # which the JSON-sourced Nexmark harness does not. A/B via CLINK_DISABLE_COLUMNAR
    # (env) and -DCLINK_USE_FLAT_HASH_MAP (build); self-validating via the
    # batch_materialize_counter zero-decode proof.
    add_executable(clink_columnar_groupby_bench clink_columnar_groupby_bench.cpp)
    target_link_libraries(clink_columnar_groupby_bench PRIVATE clink::sql)
    clink_set_warnings(clink_columnar_groupby_bench)
endif()

# flink_compare/clink-job: CLINK_REGISTER_JOB .so for the
# clink-vs-Flink head-to-head harness. Requires the kafka impl
# headers for the message_source / message_sink helpers.
if(TARGET clink::kafka)
    add_library(clink_bench_canonical_pipeline MODULE
        flink_compare/clink-job/canonical_pipeline.cpp)
    target_link_libraries(clink_bench_canonical_pipeline PRIVATE
        clink::core clink::kafka)
    set_target_properties(clink_bench_canonical_pipeline PROPERTIES
        PREFIX ""
        OUTPUT_NAME "bench_canonical_pipeline")
    clink_set_warnings(clink_bench_canonical_pipeline)
endif()

# inproc_compare/clink-job: in-process bench that drives source + sink
# from inside the .so so we measure engine work (operator dispatch,
# RocksDB writes, checkpoint serde) instead of Kafka throughput.
#
# Links clink::rocksdb directly into the .so. The .so dlopens with
# RTLD_LOCAL so its StateBackendFactory::default_instance() singleton
# is private; the host's clink::rocksdb::install() registration never
# reaches it. Linking + calling install() from inside define_job
# closes that gap.
add_library(clink_inproc_bench_pipeline MODULE
    inproc_compare/clink-job/inproc_pipeline.cpp)
target_link_libraries(clink_inproc_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_inproc_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "inproc_bench_pipeline")
clink_set_warnings(clink_inproc_bench_pipeline)

# prod_compare/sliding_window: same shape as inproc but with sliding
# instead of tumbling - exercises the size/slide=4 fan-out path.
add_library(clink_sliding_bench_pipeline MODULE
    prod_compare/sliding_window/clink-job/sliding_pipeline.cpp)
target_link_libraries(clink_sliding_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_sliding_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "sliding_bench_pipeline")
clink_set_warnings(clink_sliding_bench_pipeline)

# prod_compare/process_function: KeyedProcessFunction with two state
# slots per key (ValueState + ListState). Emits one record per input
# record; exercises the read+update+write loop on multiple state slots.
add_library(clink_process_bench_pipeline MODULE
    prod_compare/process_function/clink-job/process_pipeline.cpp)
target_link_libraries(clink_process_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_process_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "process_bench_pipeline")
clink_set_warnings(clink_process_bench_pipeline)

# prod_compare/interval_join: two-stream connect + KeyedCoProcessFunction
# joining orders with payments via latest-per-key state. v1 has no time
# window enforcement; v2 will layer event-time bounds + broadcast state.
add_library(clink_join_bench_pipeline MODULE
    prod_compare/interval_join/clink-job/join_pipeline.cpp)
target_link_libraries(clink_join_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_join_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "join_bench_pipeline")
clink_set_warnings(clink_join_bench_pipeline)

# prod_compare/parallel_recovery: same workload as inproc_compare but
# at parallelism=N. Source shards the cursor across subtasks; key_by
# routes by extractor name so hash-partitioning is consistent. v1 has
# no induced-crash test; v2 will kill the TM mid-run.
add_library(clink_parallel_bench_pipeline MODULE
    prod_compare/parallel_recovery/clink-job/parallel_pipeline.cpp)
target_link_libraries(clink_parallel_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_parallel_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "parallel_bench_pipeline")
clink_set_warnings(clink_parallel_bench_pipeline)
