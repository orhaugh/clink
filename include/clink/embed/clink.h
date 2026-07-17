#ifndef CLINK_EMBED_CLINK_H
#define CLINK_EMBED_CLINK_H

/* libclink - embed the clink engine in any process.
 *
 * A pure-C ABI over the embedded engine: open an engine (the whole
 * runtime, in this process, no daemons), execute SQL scripts, wait for or
 * cancel the submitted jobs, and read the rows a connector='collect'
 * table receives as typed Arrow batches through the Arrow C stream
 * interface (zero-copy into pyarrow, DuckDB, polars, or Arrow C++).
 *
 * Threading: one engine handle may be shared across threads; each collect
 * table allows exactly one consumer. Blocking calls (job waits, stream
 * get_next) are safe alongside calls on other threads.
 *
 * Errors: functions returning int use 0 for success. On failure,
 * clink_last_error(engine) returns a message valid until the next API
 * call on the same engine. clink_engine_open reports its failure through
 * clink_open_error() (thread-local) since there is no engine yet.
 *
 * Diagnostics that a CLI would print to stderr (statement errors, job
 * teardown notes) are captured per engine and surface through
 * clink_last_error, not stderr. Rows sent to a connector='print' table
 * still write to stdout by design.
 *
 * Bare SELECT statements are compiled onto a print (stdout) sink exactly
 * as `clink run` does; use a connector='collect' table to receive rows
 * programmatically instead.
 */

#include <stddef.h>
#include <stdint.h>

/* The Arrow C stream interface (canonical definitions from the Arrow
 * project's C data interface specification, reproduced verbatim so this
 * header stands alone for C consumers without Arrow headers). */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

#if defined(_WIN32)
#define CLINK_EMBED_API __declspec(dllexport)
#else
#define CLINK_EMBED_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define CLINK_EMBED_ABI_VERSION 1

/* Opaque engine handle. */
typedef struct clink_engine clink_engine;

typedef struct clink_engine_options {
    /* Uniform op parallelism for compiled jobs. 0 means 1. */
    uint32_t parallelism;
    /* Per-job state backend URI (e.g. "rocksdb:///tmp/state"). NULL keeps
     * the default: memory, or file when checkpoint_dir is set. */
    const char* state_backend_uri;
    /* Checkpoint root directory. NULL/empty disables checkpointing. */
    const char* checkpoint_dir;
    /* Periodic checkpoint cadence; 0 means the 10000 ms default. Only
     * used when checkpoint_dir is set. */
    int64_t checkpoint_interval_ms;
    /* Persistent catalog directory. NULL keeps a session-only catalog. */
    const char* catalog_dir;
} clink_engine_options;

/* The ABI version this library was built as. Compare against
 * CLINK_EMBED_ABI_VERSION before using anything else. */
CLINK_EMBED_API int32_t clink_abi_version(void);

/* Start an engine (in-process Coordinator + Worker). NULL `options`
 * uses defaults. Returns NULL on failure; see clink_open_error(). */
CLINK_EMBED_API clink_engine* clink_engine_open(const clink_engine_options* options);

/* Why the last clink_engine_open on THIS thread returned NULL. */
CLINK_EMBED_API const char* clink_open_error(void);

/* Stop every job, wake blocked collect streams (they see a cancelled
 * status), tear the engine down, and free the handle. Streams already
 * exported remain safe to drain and release after this returns. */
CLINK_EMBED_API void clink_engine_close(clink_engine* engine);

/* Execute a SQL script: DDL folds into the engine catalog, each compiled
 * job (INSERT / materialized view) is submitted immediately. Returns 0 on
 * success; on failure clink_last_error carries the statement diagnostic. */
CLINK_EMBED_API int clink_exec(clink_engine* engine, const char* sql);

/* Message for the most recent failed call on this engine ("" if none).
 * Valid until the next API call on the same engine. */
CLINK_EMBED_API const char* clink_last_error(clink_engine* engine);

/* Jobs submitted by this engine so far, in submission order. */
CLINK_EMBED_API size_t clink_job_count(clink_engine* engine);
/* The id of the index-th submitted job; 0 if index is out of range. */
CLINK_EMBED_API uint64_t clink_job_id_at(clink_engine* engine, size_t index);

/* Wait for one job to reach a terminal state (completed, failed, or
 * cancelled). timeout_ms < 0 waits forever. Returns 0 when terminal,
 * 1 on timeout, -1 on error. */
CLINK_EMBED_API int clink_job_wait(clink_engine* engine, uint64_t job_id, int64_t timeout_ms);

/* Request cancellation of one job (it still needs a wait to drain). */
CLINK_EMBED_API int clink_job_cancel(clink_engine* engine, uint64_t job_id);

/* Wait for every submitted job. timeout_ms < 0 waits forever. Returns 0
 * when all reached a terminal state with no errors, 1 on timeout (jobs
 * keep running), -1 when any job reported errors (clink_last_error
 * aggregates them). */
CLINK_EMBED_API int clink_await_all(clink_engine* engine, int64_t timeout_ms);

/* Request cancellation of every submitted job. */
CLINK_EMBED_API void clink_cancel_all(clink_engine* engine);

/* Export the typed Arrow stream of a connector='collect' table into
 * `out` (an uninitialised ArrowArrayStream). Exactly one consumer per
 * table. get_next blocks until a batch is available, signals end of
 * stream after the producing job's sinks close (completion, failure and
 * cancellation all close), and returns an error after the engine closes.
 * May be called before or after the producing job is submitted. The
 * stream must be released via its release callback; it remains valid to
 * drain after clink_engine_close. Returns 0 on success. */
CLINK_EMBED_API int clink_collect_stream(clink_engine* engine,
                                         const char* table,
                                         struct ArrowArrayStream* out);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* CLINK_EMBED_CLINK_H */
