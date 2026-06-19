// Back-compat shim: `clink_rescale_job` is now `clink rescale`.
// See clink_submit_job_shim.cpp for rationale.
int clink_cmd_rescale(int argc, char** argv);
int main(int argc, char** argv) {
    return clink_cmd_rescale(argc, argv);
}
