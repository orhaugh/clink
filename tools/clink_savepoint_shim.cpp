// Back-compat shim: `clink_savepoint` is now `clink savepoint`.
// See clink_submit_job_shim.cpp for rationale.
int clink_cmd_savepoint(int argc, char** argv);
int main(int argc, char** argv) {
    return clink_cmd_savepoint(argc, argv);
}
