// Back-compat shim: `clink_submit_job` is now `clink run`. This
// binary exists so existing integration-test fixtures, scripts, and
// users with the old name in PATH keep working. New code should call
// `clink run` directly. The actual logic lives in the same
// clink_cmd_run() function the unified CLI dispatches to.
int clink_cmd_run(int argc, char** argv);
int main(int argc, char** argv) {
    return clink_cmd_run(argc, argv);
}
