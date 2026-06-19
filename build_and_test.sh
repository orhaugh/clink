#!/usr/bin/env bash
set -euo pipefail

# build_and_run_tests: configure, build and run CMake tests, then clean
# up on success.
# Optional $1 = mode: "asan" | "tsan" | "ubsan" | "coverage" | "" (normal).
build_and_run_tests() {
  local sanitizer="${1:-}"
  local build_dir="build"
  local -a cmake_extra_flags=()
  local test_env=""
  local src_dir
  src_dir="$(pwd)"

  case "$sanitizer" in
    asan)
      build_dir="build-asan"
      cmake_extra_flags=(
        "-DCMAKE_CXX_FLAGS=-fsanitize=address -fno-omit-frame-pointer"
        "-DCMAKE_C_FLAGS=-fsanitize=address -fno-omit-frame-pointer"
        "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address"
      )
      test_env="ASAN_OPTIONS=detect_leaks=1:halt_on_error=1"
      echo "▶ AddressSanitizer build"
      ;;
    tsan)
      build_dir="build-tsan"
      cmake_extra_flags=(
        "-DCMAKE_CXX_FLAGS=-fsanitize=thread"
        "-DCMAKE_C_FLAGS=-fsanitize=thread"
        "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread"
      )
      # `suppressions=...` ignores reports inside libraries we don't
      # compile with TSan (librdkafka, OpenSSL, libpq) - TSan can't
      # reason about their internal synchronisation and the reports
      # are noise. See tsan-suppressions.txt for the full list.
      test_env="TSAN_OPTIONS=halt_on_error=1:suppressions=${src_dir}/tsan-suppressions.txt"
      echo "▶ ThreadSanitizer build"
      ;;
    ubsan)
      build_dir="build-ubsan"
      cmake_extra_flags=(
        "-DCMAKE_CXX_FLAGS=-fsanitize=undefined -fno-omit-frame-pointer"
        "-DCMAKE_C_FLAGS=-fsanitize=undefined -fno-omit-frame-pointer"
        "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=undefined"
      )
      # `suppressions=...` ignores reports inside vendored third-party
      # libraries (RocksDB's ARM64 CRC32C, etc.) - upstream's problem,
      # surfaces as noise that masks real reports we should catch.
      # See ubsan-suppressions.txt for the full list.
      test_env="UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1:suppressions=${src_dir}/ubsan-suppressions.txt"
      echo "▶ UndefinedBehaviourSanitizer build"
      ;;
    coverage)
      build_dir="build-coverage"
      cmake_extra_flags=(
        "-DCMAKE_CXX_FLAGS=--coverage -fprofile-arcs -ftest-coverage"
        "-DCMAKE_C_FLAGS=--coverage -fprofile-arcs -ftest-coverage"
        "-DCMAKE_EXE_LINKER_FLAGS=--coverage"
      )
      echo "▶ Coverage build"
      ;;
    "")
      echo "▶ Normal build"
      ;;
    *)
      echo "▶ Unknown sanitizer: $sanitizer" >&2
      return 1
      ;;
  esac

  # Use an absolute build dir so we don't rely on shell CWD between
  # passes - bind-mounted Docker volumes on macOS occasionally lose
  # their kernel CWD after rapid rm/mkdir of the same name, which makes
  # cmake's getcwd() fail with "Current working directory cannot be
  # established."
  local abs_build_dir="${src_dir}/${build_dir}"

  if [[ -d "$abs_build_dir" ]]; then
    echo "Removing existing '$build_dir'..."
    rm -rf "$abs_build_dir"
  fi

  echo "Configuring with CMake..."
  cmake -S "$src_dir" -B "$abs_build_dir" -DCMAKE_BUILD_TYPE=Debug "${cmake_extra_flags[@]}" \
    || { echo "▶ CMake configuration failed"; return 1; }

  # Compile parallelism. Default cap at nproc/2 - heavy templated
  # headers (window operators, the join code) push cc1plus past 1 GB
  # resident on some TUs and full-nproc reliably OOM-kills compilers
  # in typical Docker environments. Override with BUILD_JOBS=N.
  local jobs
  if [[ -n "${BUILD_JOBS:-}" ]]; then
    jobs="$BUILD_JOBS"
  else
    if command -v nproc &>/dev/null; then
      jobs=$(( $(nproc) / 2 ))
    else
      # macOS fallback when running this script outside Docker.
      jobs=$(( $(sysctl -n hw.ncpu) / 2 ))
    fi
    [[ $jobs -lt 1 ]] && jobs=1
  fi

  echo "Building with CMake (jobs=$jobs)..."
  cmake --build "$abs_build_dir" -- -j"$jobs" \
    || { echo "▶ CMake build failed"; return 1; }

  # Test parallelism. Caps at nproc/2 under sanitizers and at min(8,
  # nproc) otherwise. Sanitizer instrumentation slows each test
  # significantly; under full-nproc parallelism the per-test ephemeral
  # TCP port lifetime extends and network-channel/distributed-bridge
  # tests collide on bind / hit kernel resource pressure. The
  # parallel-8 cap mirrors the host-side ctest convention (see
  # feedback_cap_build_parallelism).
  local test_jobs
  if command -v nproc &>/dev/null; then
    local hw_jobs="$(nproc)"
    if [[ -n "$sanitizer" && "$sanitizer" != "coverage" ]]; then
      test_jobs=$(( hw_jobs / 2 ))
      [[ $test_jobs -lt 1 ]] && test_jobs=1
    else
      test_jobs=$(( hw_jobs < 8 ? hw_jobs : 8 ))
    fi
  else
    test_jobs="$(sysctl -n hw.ncpu)"
  fi

  # Tests that exercise librdkafka's mock cluster trip TSan because
  # librdkafka itself isn't compiled with -fsanitize=thread; its
  # internal atomic/futex primitives bypass TSan's interception, and
  # TSan ends up corrupting librdkafka's state hard enough to crash.
  # Skipping the Kafka-using tests under TSan only - they're covered
  # by the normal/asan/ubsan passes. Same applies to PipelineConfig
  # which transitively uses Kafka.
  local ctest_exclude=""
  if [[ "$sanitizer" == "tsan" ]]; then
    ctest_exclude='--exclude-regex (Kafka\.|PipelineConfig\.)'
  fi

  echo "Running tests..."
  if [[ "$sanitizer" == "coverage" ]]; then
    # Coverage: run sequentially. Parallel writes to the same .gcda
    # files corrupt coverage data and the lcov/gcovr reports become
    # non-deterministic.
    ( cd "$abs_build_dir" && ctest --output-on-failure ) \
      || { echo "▶ Tests failed"; return 1; }
  else
    # All other modes parallelise via ctest. clink's tests are
    # process-isolated (each test gets its own KafkaMockCluster, its
    # own ephemeral ports, its own per-pid temp paths) so concurrent
    # workers don't collide. Sanitizer env (ASAN_OPTIONS, TSAN_OPTIONS,
    # UBSAN_OPTIONS) is propagated through `env` and inherited by each
    # ctest child process.
    ( cd "$abs_build_dir" && env $test_env ctest -j "$test_jobs" --output-on-failure $ctest_exclude ) \
      || { echo "▶ Tests failed"; return 1; }
  fi

  if [[ "$sanitizer" == "coverage" ]]; then
    echo "Generating coverage reports..."

    if ! command -v lcov &>/dev/null; then
      echo "  Installing lcov and gcovr..."
      apt-get update -qq && apt-get install -y -qq --no-install-recommends lcov gcovr >/dev/null 2>&1 || true
    fi

    local report_dir="${src_dir}/coverage-report"
    rm -rf "$report_dir"
    mkdir -p "$report_dir"

    # Capture all coverage data for the build dir, then strip third-
    # party / test code so the report is just our own sources.
    lcov --capture \
         --directory "$abs_build_dir" \
         --output-file "$report_dir/coverage.info" \
         --ignore-errors mismatch,mismatch,inconsistent,inconsistent,negative,negative,count,count \
         --quiet
    lcov --remove "$report_dir/coverage.info" \
         '*/build-coverage/_deps/*' \
         '*/tests/*' \
         '/usr/*' \
         --output-file "$report_dir/coverage.info" \
         --ignore-errors unused,unused,inconsistent,inconsistent,negative,negative,count,count \
         --quiet

    # HTML - for human review.
    genhtml "$report_dir/coverage.info" \
            --output-directory "$report_dir/html" \
            --ignore-errors inconsistent,inconsistent,negative,negative,count,count \
            --quiet
    echo "  HTML report: $report_dir/html/index.html"

    # SonarQube + Cobertura XML - for CI ingest.
    if command -v gcovr &>/dev/null; then
      gcovr --root "$src_dir" \
            "$abs_build_dir" \
            --filter 'src/' \
            --filter 'include/clink/' \
            --exclude '.*_deps.*' \
            --exclude '.*/tests/.*' \
            --sonarqube "$report_dir/sonarqube-coverage.xml" \
            --xml "$report_dir/cobertura-coverage.xml"
      echo "  SonarQube report: $report_dir/sonarqube-coverage.xml"
      echo "  Cobertura report: $report_dir/cobertura-coverage.xml"
    else
      echo "  gcovr not found - skipping SonarQube/Cobertura XML reports"
    fi

    lcov --summary "$report_dir/coverage.info" 2>&1 | grep -E "lines|functions"
    echo "✅ Coverage reports generated in '$report_dir/'."
    echo ""
    echo "To view: open $report_dir/html/index.html"
    echo "For SonarQube: set sonar.coverageReportPaths=$report_dir/sonarqube-coverage.xml"
  else
    echo "Tests succeeded; removing '$build_dir'..."
    rm -rf "$abs_build_dir"
  fi
  echo "✅ Done."
}

usage() {
  cat <<EOF
Usage: $0 [--image <docker-image>] [--sanitizer <asan|tsan|ubsan|coverage|all>] [--help]

  --image IMAGE        Run the build & test steps inside the specified Docker image.
  --sanitizer MODE     Build with sanitizer or coverage:
                         asan      AddressSanitizer (memory errors + leak detection)
                         tsan      ThreadSanitizer (data race detection)
                         ubsan     UndefinedBehaviourSanitizer (UB, overflow, misaligned access)
                         coverage  Code coverage (generates HTML + SonarQube reports)
                         all       Normal + ASan + TSan + UBSan (not coverage)
  --help               Show this help message and exit.

Examples:
  $0                                        # Normal build + test
  $0 --sanitizer asan                       # ASan build + test
  $0 --sanitizer coverage                   # Coverage build + report generation
  $0 --sanitizer all                        # Normal + ASan + TSan + UBSan
  $0 --image clink-build --sanitizer all  # Run all sanitizers in Docker
EOF
}

# --- option parsing ---
docker_image=""
sanitizer_mode=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      if [[ -z "${2-}" ]]; then
        echo "▶ Missing argument for --image" >&2
        exit 1
      fi
      docker_image="$2"
      shift 2
      ;;
    --sanitizer)
      if [[ -z "${2-}" ]]; then
        echo "▶ Missing argument for --sanitizer" >&2
        exit 1
      fi
      sanitizer_mode="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

# Re-invoke ourselves inside Docker if requested. The container mounts
# the repo at /workspace and runs this same script with IN_DOCKER=1 so
# the recursion guard skips the docker step.
if [[ -n "$docker_image" && -z "${IN_DOCKER-}" ]]; then
  echo "▶ Running inside Docker image: $docker_image"
  local_args=""
  [[ -n "$sanitizer_mode" ]] && local_args="--sanitizer $sanitizer_mode"
  # Coverage and sanitizer builds need root for apt-get (installing
  # lcov/gcovr in case the image is older than the current setup script).
  docker_user_flags=""
  if [[ "$sanitizer_mode" == "coverage" || "$sanitizer_mode" == "asan" || "$sanitizer_mode" == "tsan" || "$sanitizer_mode" == "ubsan" || "$sanitizer_mode" == "all" ]]; then
    docker_user_flags="--user root"
  fi
  docker run --rm \
    $docker_user_flags \
    -v "$(pwd)":/workspace \
    -w /workspace \
    -e IN_DOCKER=1 \
    "$docker_image" \
    bash -c "bash $(basename "$0") $local_args"
  exit $?
fi

# Run the function(s) when this script is executed directly.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  declare -a completed_steps=()
  declare -a failed_steps=()
  overall_start=$(date +%s)

  run_step() {
    local label="$1"
    shift
    local step_start
    step_start=$(date +%s)
    if "$@"; then
      local step_end
      step_end=$(date +%s)
      completed_steps+=("$label ($(( step_end - step_start ))s)")
    else
      local step_end
      step_end=$(date +%s)
      failed_steps+=("$label ($(( step_end - step_start ))s)")
      return 1
    fi
  }

  print_report() {
    local overall_end
    overall_end=$(date +%s)
    local total_secs=$(( overall_end - overall_start ))

    echo ""
    echo "======================================"
    echo "  Build & Test Report"
    echo "======================================"
    if [[ ${#completed_steps[@]} -gt 0 ]]; then
      for step in "${completed_steps[@]}"; do
        echo "  PASS  $step"
      done
    fi
    if [[ ${#failed_steps[@]} -gt 0 ]]; then
      for step in "${failed_steps[@]}"; do
        echo "  FAIL  $step"
      done
    fi
    echo "--------------------------------------"
    echo "  Total: $(( ${#completed_steps[@]} + ${#failed_steps[@]} )) step(s) in ${total_secs}s"
    echo "  Passed: ${#completed_steps[@]}, Failed: ${#failed_steps[@]}"
    echo "======================================"

    if [[ ${#failed_steps[@]} -gt 0 ]]; then
      return 1
    fi
  }

  rc=0
  case "$sanitizer_mode" in
    all)
      run_step "Normal build"  build_and_run_tests ""      || rc=1
      run_step "ASan build"    build_and_run_tests "asan"  || rc=1
      run_step "TSan build"    build_and_run_tests "tsan"  || rc=1
      run_step "UBSan build"   build_and_run_tests "ubsan" || rc=1
      ;;
    "")
      run_step "Normal build"  build_and_run_tests ""    || rc=1
      ;;
    *)
      run_step "$sanitizer_mode build" build_and_run_tests "$sanitizer_mode" || rc=1
      ;;
  esac

  print_report || true
  exit $rc
fi
