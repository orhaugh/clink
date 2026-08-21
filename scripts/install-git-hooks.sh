#!/usr/bin/env bash
# Install the git hooks as SHIMS that exec the tracked scripts.
#
# Why a shim rather than a copy. .git/hooks is not tracked, so the hook has to be
# installed by hand - and the obvious way to do that is to copy scripts/pre-commit
# into it. A copy goes stale the moment the tracked script changes, silently: the
# hook keeps passing, and the check that was just added never runs.
#
# That is not hypothetical. A bare-sleep check was added to scripts/pre-commit and
# committed; the commit's own hook output did not mention it, because .git/hooks
# still held a copy taken a day earlier. The check existed, was tested, was in the
# tree, and would not have run for anybody.
#
# A one-line shim cannot drift. Re-run this after cloning; running it again is
# harmless.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -d .git ]; then
    echo "install-git-hooks: no .git directory here" >&2
    exit 1
fi

mkdir -p .git/hooks

for hook in pre-commit commit-msg; do
    if [ ! -f "scripts/$hook" ]; then
        continue
    fi
    cat > ".git/hooks/$hook" <<SHIM
#!/usr/bin/env bash
# Shim installed by scripts/install-git-hooks.sh - do not edit.
# The real hook is scripts/$hook, which is tracked. This file only forwards to it
# so it can never go stale against the tracked version.
# bash explicitly, so a lost exec bit on the tracked script cannot break commits.
exec bash "\$(git rev-parse --show-toplevel)/scripts/$hook" "\$@"
SHIM
    chmod +x ".git/hooks/$hook"
    echo "installed .git/hooks/$hook -> scripts/$hook"
done
