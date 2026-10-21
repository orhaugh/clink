#pragma once

// Runs a docker command line and returns its exit status with everything it
// printed, so a helper that cannot start its container can say why: a failed
// `docker run` explains itself only on its standard error, which a helper
// that discards the output loses ("docker run failed" and nothing more).

#include <array>
#include <cstdio>
#include <string>

#include <sys/wait.h>

namespace clink::test {

struct DockerCommandResult {
    int status{-1};
    std::string out;
};

inline DockerCommandResult run_docker_command(const std::string& cmd) {
    DockerCommandResult r;
    FILE* pipe = ::popen((cmd + " 2>&1").c_str(), "r");
    if (pipe == nullptr) {
        r.out = "popen failed";
        return r;
    }
    std::array<char, 4096> buf{};
    std::size_t n = 0;
    while ((n = std::fread(buf.data(), 1, buf.size(), pipe)) > 0) {
        r.out.append(buf.data(), n);
    }
    const int status = ::pclose(pipe);
    r.status = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    return r;
}

}  // namespace clink::test
