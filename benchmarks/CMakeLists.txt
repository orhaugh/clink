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

# Disaggregated-state property proof (PARITY-P3) - clink-vs-clink, controlled,
# in-process. Scenario 1: checkpoint cost scales with the delta (RemoteReadBackend
# + pool) not the total state (full Arrow-IPC snapshot). Scenario 2: a working set
# larger than the hot tier still runs correctly by spilling + cold-loading.
# Premise-free counters (committed entries/bytes, remote_loads) beside wall-clock;
# structural invariants hard-fail.
add_executable(clink_disagg_bench clink_disagg_bench.cpp)
target_link_libraries(clink_disagg_bench PRIVATE clink::core)
clink_set_warnings(clink_disagg_bench)

# Serde benchmark - compares the legacy record-by-record Codec<T>
# wire encoding against the Arrow RecordBatch + IPC path. The Arrow
# path is the production path; the legacy path is benched as a
# reference. CLINK_HAS_ARROW is always defined on clink::core.
add_executable(clink_serde_bench clink_serde_bench.cpp)
target_link_libraries(clink_serde_bench PRIVATE clink::core)
clink_set_warnings(clink_serde_bench)

# Nexmark benchmark - the Nexmark streaming benchmark
# (github.com/nexmark/nexmark) as clink SQL jobs over a generated event stream
# (connector='nexmark') on an in-process coordinator/worker cluster, output discarded through
# a blackhole sink. Reports input-event throughput. Needs the SQL frontend.
if(TARGET clink::sql)
    add_executable(clink_nexmark_bench clink_nexmark_bench.cpp)
    target_link_libraries(clink_nexmark_bench PRIVATE clink::sql)
    clink_set_warnings(clink_nexmark_bench)

    # Hot-path rig for the nexmark shapes, meant to be run under a sampling
    # profiler: the same operator chain in ONE process over pre-materialised
    # input, with no broker, network or deploy in the way. Not a throughput
    # claim - the containerised harness owns that - but the only way to see
    # where engine time actually goes.
    add_executable(clink_hotpath_bench clink_hotpath_bench.cpp)
    target_link_libraries(clink_hotpath_bench PRIVATE clink::sql)
    clink_set_warnings(clink_hotpath_bench)

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

# Operator FUSION A/B - the same three-stage chain as separate Dag operators (a
# thread and a BoundedChannel per boundary) vs fused into one ChainedOperator. The
# cloud rig measured an operator boundary at ~0.37us per record (what a blackhole
# sink that only increments a counter cost, 16% of all worker CPU on q0), and the
# cluster's chain dispatch stopped using ChainedOperator when it was generalised to
# DagBuilders - so "chained" ops now share a slot but still get a thread and a
# channel each. This measures the prize before the plumbing is rebuilt.
add_executable(clink_fusion_bench clink_fusion_bench.cpp)
target_link_libraries(clink_fusion_bench PRIVATE clink::sql)
clink_set_warnings(clink_fusion_bench)

# Per-group memory cost of a windowed aggregate, against the REAL operator via the
# embedded engine. The rig attributed 3.9 KB per group to a COUNT(*) keyed by an int64;
# this reproduces that locally so a fix can be iterated in seconds. The hot-path bench's
# q12 shape is a hand-written MODEL of the state and cannot validate an operator change.
if(TARGET clink::embed)
    add_executable(clink_window_state_bench clink_window_state_bench.cpp)
    target_link_libraries(clink_window_state_bench PRIVATE clink::embed clink::sql)
    clink_set_warnings(clink_window_state_bench)
endif()

# Keyed-shuffle split cost in isolation. The `hash` partitioner was 19.3% of all worker
# CPU on nexmark q12 - the largest non-library compute item in the engine, ahead of the
# window aggregation it feeds. This drives gather_columnar_by_target directly, since a
# whole-pipeline figure cannot attribute a change to it.
add_executable(clink_shuffle_split_bench clink_shuffle_split_bench.cpp)
target_link_libraries(clink_shuffle_split_bench PRIVATE clink::sql)
clink_set_warnings(clink_shuffle_split_bench)

# Kafka SOURCE read-rate bench. Isolates produce() - broker bytes to
# Batch<KafkaMessage>, no decode, no operators, no channels. The split cloud rig
# found the Kafka source to be the largest single consumer of worker CPU in every
# nexmark query measured (37.8% on q0, 27.3% on q12, ~0.9us per record against
# 0.05us for the projection), and a whole-pipeline number cannot attribute a
# change to the source. Needs a populated topic; see the file header.
if(TARGET clink::kafka)
    add_executable(clink_kafka_source_bench clink_kafka_source_bench.cpp)
    target_link_libraries(clink_kafka_source_bench PRIVATE clink::kafka)
    clink_set_warnings(clink_kafka_source_bench)
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
# The RocksDB-backed bench job plugins below all link clink::rocksdb, so they
# only build when the rocksdb impl was built (a lean CLINK_BUILD_IMPLS=OFF tree
# has no clink::rocksdb target).
if(TARGET clink::rocksdb)

# Links clink::rocksdb directly into the .so. The .so dlopens with
# RTLD_LOCAL so its StateBackendFactory::default_instance() singleton
# is private; the host's clink::rocksdb::install() registration never
# reaches it. Linking + calling install() from inside define_job
# closes that gap.
add_library(clink_inproc_bench_pipeline MODULE
    inproc_compare/clink-job/inproc_pipeline.cpp)
target_link_libraries(clink_inproc_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
# Optional ForSt backend for the same pipeline (CLINK_STATE_BACKEND=
# forst://<dir>): linked only when the opt-in impl was built; the
# CLINK_HAS_FORST define it propagates gates the install() call.
if(TARGET clink::forst)
    target_link_libraries(clink_inproc_bench_pipeline PRIVATE clink::forst)
endif()
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
# no induced-crash test; v2 will kill the worker mid-run.
add_library(clink_parallel_bench_pipeline MODULE
    prod_compare/parallel_recovery/clink-job/parallel_pipeline.cpp)
target_link_libraries(clink_parallel_bench_pipeline PRIVATE
    clink::core clink::rocksdb)
set_target_properties(clink_parallel_bench_pipeline PROPERTIES
    PREFIX ""
    OUTPUT_NAME "parallel_bench_pipeline")
clink_set_warnings(clink_parallel_bench_pipeline)

endif()  # TARGET clink::rocksdb
