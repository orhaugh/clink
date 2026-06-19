// Back-compat shim: `clink_cancel_job` is now `clink cancel`.
// See clink_submit_job_shim.cpp for rationale.
int clink_cmd_cancel(int argc, char** argv);
int main(int argc, char** argv) {
    return clink_cmd_cancel(argc, argv);
}
