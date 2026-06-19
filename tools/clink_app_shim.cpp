// Back-compat shim: `clink_app` is now `clink run-application`.
// See clink_submit_job_shim.cpp for rationale.
int clink_cmd_run_application(int argc, char** argv);
int main(int argc, char** argv) {
    return clink_cmd_run_application(argc, argv);
}
