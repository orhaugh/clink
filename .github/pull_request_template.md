## What and why

<!-- What changes, and the problem it solves. For anything beyond a small
     fix, link the issue where the approach was agreed. -->

## Checklist

- [ ] Tests cover the change (see [CONTRIBUTING.md](../CONTRIBUTING.md)).
- [ ] Every stateful operator touched has a stable `.uid("...")` / `set_uid("...")`.
- [ ] Affected docs updated in the same change (`docs/connectors/`, `docs/internals/`, the capability catalogue).
- [ ] Commit subjects follow the convention (`fix(cluster): ...`); `scripts/install-git-hooks.sh` installs the checks.
