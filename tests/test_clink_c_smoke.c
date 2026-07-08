/* Pure-C smoke for libclink: proves clink/embed/clink.h compiles as C and
 * the full open / exec / collect / close flow works with no C++ on the
 * consumer side. Drains the Arrow C stream through the raw callbacks.
 * Exits 0 on success; prints the first failure and exits 1 otherwise. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "clink/embed/clink.h"

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            fprintf(stderr, "FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__); \
            return 1;                                                       \
        }                                                                   \
    } while (0)

int main(void) {
    CHECK(clink_abi_version() == CLINK_EMBED_ABI_VERSION, "abi version");

    /* Input data. */
    const char* data_path = "/tmp/clink_c_smoke_orders.ndjson";
    FILE* f = fopen(data_path, "w");
    CHECK(f != NULL, "open data file");
    fputs("{\"user_id\":1,\"amount\":10}\n", f);
    fputs("{\"user_id\":2,\"amount\":32}\n", f);
    fclose(f);

    clink_engine* e = clink_engine_open(NULL);
    if (e == NULL) {
        fprintf(stderr, "FAIL: open: %s\n", clink_open_error());
        return 1;
    }

    /* Bad SQL reports through last_error and does not poison the engine. */
    CHECK(clink_exec(e, "NOT SQL AT ALL") != 0, "bad sql rejected");
    CHECK(strlen(clink_last_error(e)) > 0, "last_error populated");

    /* A full pipeline: file source -> collect sink. */
    char sql[1024];
    snprintf(sql, sizeof(sql),
             "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
             "WITH (connector='file', format='json', path='%s');"
             "CREATE TABLE results (user_id BIGINT, amount BIGINT) "
             "WITH (connector='collect');"
             "INSERT INTO results SELECT user_id, amount FROM orders",
             data_path);
    if (clink_exec(e, sql) != 0) {
        fprintf(stderr, "FAIL: exec: %s\n", clink_last_error(e));
        return 1;
    }
    CHECK(clink_job_count(e) == 1, "one job submitted");

    /* Drain the collect stream through the raw C callbacks. */
    struct ArrowArrayStream stream;
    if (clink_collect_stream(e, "results", &stream) != 0) {
        fprintf(stderr, "FAIL: collect_stream: %s\n", clink_last_error(e));
        return 1;
    }
    struct ArrowSchema schema;
    CHECK(stream.get_schema(&stream, &schema) == 0, "get_schema");
    CHECK(schema.n_children == 2, "two columns");
    schema.release(&schema);

    long rows = 0;
    for (;;) {
        struct ArrowArray array;
        CHECK(stream.get_next(&stream, &array) == 0, "get_next");
        if (array.release == NULL) {
            break; /* end of stream */
        }
        rows += (long)array.length;
        array.release(&array);
    }
    stream.release(&stream);
    CHECK(rows == 2, "row count");

    CHECK(clink_await_all(e, 30000) == 0, "await_all clean");
    clink_engine_close(e);
    remove(data_path);
    printf("clink C smoke: PASS (%ld rows)\n", rows);
    return 0;
}
