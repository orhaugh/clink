#!/usr/bin/env bash
# Run every showcase against a clink release candidate, before it is tagged.
#
# The showcases (docs/showcases/) live in their own repositories, pinned to a
# clink release, and each one's CI gates it against an independent model of
# what its outputs must hold. Against the pin alone, that finds what a clink
# change breaks only after the release that carries it. This script runs each
# showcase's CI against the candidate instead: it dry-runs release-binaries
# and runtime-image on main, which build the SDK tarball and the runtime image
# a release would publish, dispatches every showcase's ci.yml with the
# release-binaries run (input clink_sdk_run), waits for all of them and prints
# one table.
#
# Most showcases install that SDK. One that needs a connector the SDK leaves
# out (transfer-sentry's Kafka) builds the run's commit from source, and one
# that runs on Kubernetes (playhead) runs that commit's runtime image,
# ghcr.io/orhaugh/clink-runtime:sha-<first 12 of the commit>.
#
# The seventh, half-time, outgrows a hosted runner: it is run on one machine
# against the candidate by its evidence.sh, and the bundle that leaves is
# judged here by scripts/check-half-time-evidence.py, before anything is
# dispatched. A complete check needs that bundle, for exactly the candidate;
# --partial leaves half-time out and says so, and never reports every
# showcase passing.
#
#   scripts/check-showcases.sh --half-time-evidence <bundle>
#       dry-run release-binaries and runtime-image on main first (about an hour)
#   scripts/check-showcases.sh --sdk-run <id> --half-time-evidence <bundle>
#       use a finished release-binaries dry run; playhead runs only if its
#       commit's image exists
#   scripts/check-showcases.sh [--sdk-run <id>] --partial
#       the showcases that run in CI only
#
# Needs gh, signed in with permission to dispatch workflows in clink and in
# the showcase repositories, plus curl and python3. Exits 0 only when every
# showcase, half-time included, passes against the candidate; 1 when any
# fails or half-time's evidence does not pass; 2 for a usage error; and 3
# when a --partial check's showcases all passed, since that is not a
# complete release check.
set -euo pipefail

OWNER=orhaugh
SDK_SHOWCASES=(market-pulse card-sentry grounded coldchain transfer-sentry)
IMAGE_SHOWCASES=(playhead)
IMAGE="ghcr.io/${OWNER}/clink-runtime"

HERE="$(cd "$(dirname "$0")" && pwd)"
usage() {
    echo "usage: $0 [--sdk-run <release-binaries run id>] (--half-time-evidence <bundle> | --partial)" >&2
    exit 2
}
sdk_run=""
evidence=""
partial=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sdk-run) sdk_run="${2:-}"; [[ -n "${sdk_run}" ]] || usage; shift 2 ;;
        --half-time-evidence) evidence="${2:-}"; [[ -n "${evidence}" ]] || usage; shift 2 ;;
        --partial) partial=1; shift ;;
        *) usage ;;
    esac
done
if [[ -n "${evidence}" && ${partial} = 1 ]]; then
    echo "check-showcases: --half-time-evidence and --partial are a complete check and a partial one; give one" >&2
    exit 2
fi
if [[ -z "${evidence}" && ${partial} = 0 ]]; then
    echo "check-showcases: a complete check needs half-time's evidence for the candidate:" >&2
    echo "  run half-time's evidence.sh against it and pass --half-time-evidence <bundle>," >&2
    echo "  or pass --partial to check the other showcases and say that half-time was left out" >&2
    exit 2
fi

# Whether half-time's evidence shows it passing against the candidate. A
# complete check stops here, before anything is dispatched, when it does not.
half_time="not checked"
judge_half_time() {  # <candidate commit>
    [[ -n "${evidence}" ]] || return 0
    local verdict
    if verdict="$("${HERE}/check-half-time-evidence.py" "${evidence}" --candidate "$1")"; then
        half_time=success
        echo "   ${verdict}"
    else
        echo "${verdict}" >&2
        echo "check-showcases: half-time's evidence does not pass against clink ${1:0:12}; nothing was dispatched" >&2
        exit 1
    fi
}

# The id of the run a dispatch just created. gh prints its URL; for an older
# gh that does not, the newest dispatched run of the workflow with the given
# title (or, with no title, created since the dispatch) is the one.
dispatched_run() {  # <repo> <workflow> <title or ""> <since> <gh workflow run output>
    local repo=$1 workflow=$2 title=$3 since=$4 out=$5 id filter
    id="$(grep -oE '/actions/runs/[0-9]+' <<<"${out}" | grep -oE '[0-9]+$' | tail -1 || true)"
    if [[ -n "${title}" ]]; then
        filter="select(.displayTitle == \"${title}\")"
    else
        filter="select(.createdAt >= \"${since}\")"
    fi
    for _ in $(seq 1 30); do
        [[ -n "${id}" ]] && break
        id="$(gh run list -R "${repo}" --workflow "${workflow}" --event workflow_dispatch --limit 5 \
            --json databaseId,displayTitle,createdAt --jq "[.[] | ${filter}][0].databaseId // empty")"
        [[ -n "${id}" ]] || sleep 2
    done
    [[ -n "${id}" ]] || { echo "check-showcases: no run found for the dispatch of ${workflow} in ${repo}" >&2; exit 1; }
    echo "${id}"
}

dispatch_clink() {  # <workflow>; prints the run id
    local since out
    since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    out="$(gh workflow run "$1" -R "${OWNER}/clink" --ref main 2>&1)"
    dispatched_run "${OWNER}/clink" "$1" "" "${since}" "${out}"
}

# Whether the registry holds the runtime image for a commit (an anonymous pull
# token is enough for a public package).
image_exists() {  # <commit>
    local token
    token="$(curl -fsSL "https://ghcr.io/token?scope=repository:${OWNER}/clink-runtime:pull" |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')" || return 1
    curl -fsS -o /dev/null -H "Authorization: Bearer ${token}" \
        -H "Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json" \
        "https://ghcr.io/v2/${OWNER}/clink-runtime/manifests/sha-${1:0:12}"
}

image_run=""
main_commit=""
if [[ -z "${sdk_run}" ]]; then
    # The dry runs build main as it is when they are dispatched, so the
    # evidence is judged against that commit first, and the runs are held to
    # it below.
    main_commit="$(gh api "repos/${OWNER}/clink/commits/main" --jq .sha)"
    judge_half_time "${main_commit}"
    echo "== dry-running release-binaries and runtime-image on main"
    sdk_run="$(dispatch_clink release-binaries.yml)"
    image_run="$(dispatch_clink runtime-image.yml)"
    echo "   https://github.com/${OWNER}/clink/actions/runs/${sdk_run} (about twenty minutes)"
    echo "   https://github.com/${OWNER}/clink/actions/runs/${image_run} (about an hour)"
    if ! gh run watch "${sdk_run}" -R "${OWNER}/clink" --exit-status >/dev/null 2>&1; then
        echo "check-showcases: the release-binaries dry run failed; no showcase was run" >&2
        exit 1
    fi
fi
commit="$(gh api "repos/${OWNER}/clink/actions/runs/${sdk_run}" --jq .head_sha)"
echo "== candidate: clink ${commit:0:12}, SDK from release-binaries run ${sdk_run}"
if [[ -z "${main_commit}" ]]; then
    judge_half_time "${commit}"
elif [[ "${commit}" != "${main_commit}" ]]; then
    echo "check-showcases: release-binaries is building ${commit:0:12}, not ${main_commit:0:12}, whose half-time evidence was judged: main moved" >&2
    exit 1
fi
if [[ -n "${image_run}" ]]; then
    image_commit="$(gh api "repos/${OWNER}/clink/actions/runs/${image_run}" --jq .head_sha)"
    if [[ "${image_commit}" != "${commit}" ]]; then
        echo "check-showcases: runtime-image is building ${image_commit:0:12}, not the candidate ${commit:0:12}: main moved between the dispatches" >&2
        exit 1
    fi
fi

names=()
runs=()
dispatch_showcase() {  # <repo>
    local out
    out="$(gh workflow run ci.yml -R "${OWNER}/$1" -f clink_sdk_run="${sdk_run}" 2>&1)"
    names+=("$1")
    runs+=("$(dispatched_run "${OWNER}/$1" ci.yml "ci on clink SDK run ${sdk_run}" "" "${out}")")
    echo "   dispatched $1"
}
for repo in "${SDK_SHOWCASES[@]}"; do
    dispatch_showcase "${repo}"
done

skipped=()
if [[ -n "${image_run}" ]]; then
    gh run watch "${image_run}" -R "${OWNER}/clink" --exit-status >/dev/null 2>&1 ||
        echo "check-showcases: the runtime-image run failed" >&2
fi
for repo in "${IMAGE_SHOWCASES[@]}"; do
    if image_exists "${commit}"; then
        dispatch_showcase "${repo}"
    else
        skipped+=("${repo}")
        echo "   skipped ${repo}: no image ${IMAGE}:sha-${commit:0:12}"
    fi
done

echo "== waiting for ${#runs[@]} showcases"
failed=0
printf '\n%-16s %-10s %s\n' showcase result run
for i in "${!names[@]}"; do
    gh run watch "${runs[$i]}" -R "${OWNER}/${names[$i]}" >/dev/null 2>&1 || true
    result="$(gh run view "${runs[$i]}" -R "${OWNER}/${names[$i]}" --json conclusion --jq .conclusion)"
    [[ "${result}" == success ]] || failed=$((failed + 1))
    printf '%-16s %-10s %s\n' "${names[$i]}" "${result}" \
        "https://github.com/${OWNER}/${names[$i]}/actions/runs/${runs[$i]}"
done
for repo in ${skipped[@]+"${skipped[@]}"}; do
    printf '%-16s %-10s %s\n' "${repo}" skipped "no runtime image for the candidate"
done
if [[ ${partial} = 1 ]]; then
    printf '%-16s %-10s %s\n' half-time "not checked" "left out of this check (--partial)"
else
    printf '%-16s %-10s %s\n' half-time "${half_time}" "evidence ${evidence}"
fi
echo
ci_total=$(( ${#names[@]} + ${#skipped[@]} ))
if [[ ${failed} -gt 0 || ${#skipped[@]} -gt 0 ]]; then
    echo "check-showcases: ${failed} failed and ${#skipped[@]} skipped of the ${ci_total} showcases run in CI against clink ${commit:0:12}"
    exit 1
fi
if [[ ${partial} = 1 ]]; then
    echo "check-showcases: PARTIAL: the ${ci_total} showcases run in CI passed against clink ${commit:0:12}; half-time was left out, so this is not a complete release check"
    exit 3
fi
echo "check-showcases: all $(( ci_total + 1 )) showcases passed against clink ${commit:0:12}, half-time on its evidence"
