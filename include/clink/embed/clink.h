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
 * Compatibility (design record 011). This header is the Stable C surface
 * for the 1.x line, and these are the rules it grows by:
 *
 *   - CLINK_EMBED_ABI_VERSION changes only for an incompatible change,
 *     which within 1.x is never. Compare clink_abi_version() against it
 *     once, before using anything else. clink_version() names the library
 *     release for logging; it is not a compatibility check.
 *   - Functions are only ever added. A function scheduled for removal at
 *     the next major release carries CLINK_DEPRECATED for at least one
 *     minor release first and keeps working until then.
 *   - clink_engine_options grows by appending fields, never by reordering
 *     or retyping one. struct_size tells the library how much of the
 *     struct the caller compiled: a field beyond the caller's size reads
 *     as its default, and a struct larger than the library knows is read
 *     only as far as the library knows. Initialise with
 *     CLINK_ENGINE_OPTIONS_INIT so struct_size is always right.
 *   - Return conventions are fixed: int-returning calls use 0 for success
 *     and the non-zero codes each one documents.
 *   - The exported symbol set is tracked in scripts/libclink-abi-symbols.txt
 *     and checked against this header and the built library.
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

/* Marks a function scheduled for removal at the next major release. It
 * keeps working for at least one minor release after the mark appears. */
#if defined(__GNUC__) || defined(__clang__)
#define CLINK_DEPRECATED(msg) __attribute__((deprecated(msg)))
#elif defined(_MSC_VER)
#define CLINK_DEPRECATED(msg) __declspec(deprecated(msg))
#else
#define CLINK_DEPRECATED(msg)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Version 2: clink_engine_options gained the leading struct_size field so
 * the struct can grow without another bump. Version 1 callers must be
 * recompiled. */
#define CLINK_EMBED_ABI_VERSION 2

/* Opaque engine handle. */
typedef struct clink_engine clink_engine;

typedef struct clink_engine_options {
    /* sizeof(clink_engine_options) as the CALLER compiled it. The library
     * reads only the fields this size covers; 0 is refused by name. Set by
     * CLINK_ENGINE_OPTIONS_INIT. */
    size_t struct_size;
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
    /* New options are appended here, never inserted above. */
} clink_engine_options;

/* Default options with struct_size filled in; every other field is zero
 * (its documented default). Usage:
 *
 *     clink_engine_options opts = CLINK_ENGINE_OPTIONS_INIT;
 *     opts.parallelism = 4;
 */
#define CLINK_ENGINE_OPTIONS_INIT {sizeof(clink_engine_options)}

/* The ABI version this library was built as. Compare against
 * CLINK_EMBED_ABI_VERSION before using anything else. */
CLINK_EMBED_API int32_t clink_abi_version(void);

/* The library's release version as a string ("1.2.0"). For logging and
 * diagnostics; the ABI question is answered by clink_abi_version(). The
 * pointer is static and valid for the life of the process. */
CLINK_EMBED_API const char* clink_version(void);

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
