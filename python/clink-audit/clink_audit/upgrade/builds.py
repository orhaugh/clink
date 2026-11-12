"""A build under test, named by explicit paths, and its identity.

A build is three files the operator names: `clink` (the CLI), `clink_node`
(the Coordinator and Worker) and the job module compiled against that build.
Nothing is taken from evidence: an evidence set records which build wrote it,
and `check` compares that record with the build it is given; it never runs
anything the record names.

Identity is the SHA-256 and size of each file, and what the build says of
itself through `clink --capabilities-json`: its version, the commit it was
built from and whether that tree was clean, and its capabilities. What a build
says of itself is labelled self-reported; the hashes are measured.
"""

import hashlib
import json
import os
import subprocess

FILES = ("clink", "clink_node", "job")


class BuildError(ValueError):
    pass


def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


class Build:
    """`tools=True` names a build used only for its clink: the one that
    exports, verifies and compares datasets, which is not under test."""

    def __init__(self, name, clink, clink_node=None, job=None, tools=False):
        self.name = name
        self.paths = {}
        wanted = (("clink", clink),) if tools else (("clink", clink), ("clink_node", clink_node),
                                                    ("job", job))
        for k, p in wanted:
            if not p:
                raise BuildError("--%s-%s is required" % (name, k.replace("_", "-")))
            p = os.path.abspath(p)
            if not os.path.isfile(p):
                raise BuildError("%s %s: %s is not a file" % (name, k, p))
            if k != "job" and not os.access(p, os.X_OK):
                raise BuildError("%s %s: %s is not executable" % (name, k, p))
            self.paths[k] = p
        self.identity = None

    @property
    def clink(self):
        return self.paths["clink"]

    @property
    def clink_node(self):
        return self.paths["clink_node"]

    @property
    def job(self):
        return self.paths["job"]

    def measure(self, wrap, timeout_s=30):
        """Hashes the files and asks the build for its manifest. Returns the
        identity; a manifest that cannot be read is recorded as unavailable,
        never guessed."""
        files = {k: {"path": p, "sha256": sha256_file(p), "bytes": os.path.getsize(p)}
                 for k, p in self.paths.items()}
        manifest, why = None, ""
        try:
            r = subprocess.run(wrap([self.clink, "--capabilities-json"]), capture_output=True,
                               text=True, timeout=timeout_s, env={"PATH": os.environ.get("PATH", ""),
                                                                   "CLINK_LOG_LEVEL": "off"})
            if r.returncode == 0:
                manifest = json.loads(r.stdout)
            else:
                why = "clink --capabilities-json exited %d" % r.returncode
        except (OSError, subprocess.SubprocessError, ValueError) as e:
            why = "clink --capabilities-json: %s" % e
        b = (manifest or {}).get("build", {}) if isinstance(manifest, dict) else {}
        self.identity = {
            "name": self.name,
            "files": files,
            "manifest_available": manifest is not None,
            "manifest_unavailable_because": why,
            "self_reported": dict(b, version=(manifest or {}).get("clink_version"),
                                  connectors=len((manifest or {}).get("connectors") or [])),
            "provenance": ("self-reported by the binary: built from %s%s" % (
                b.get("git_sha"), "" if b.get("git_clean") else " (tree not clean)")
                if b.get("git_sha") else "unavailable: the binary reports no commit"),
        }
        self.manifest = manifest
        return self.identity

    def same_files_as(self, recorded):
        """Which of this build's files differ from a recorded identity's."""
        diff = []
        for k in FILES:
            if k not in self.identity["files"]:
                continue
            mine = self.identity["files"][k]["sha256"]
            theirs = (recorded.get("files", {}).get(k) or {}).get("sha256")
            if mine != theirs:
                diff.append("%s: %s, the evidence's baseline %s" % (k, mine[:16], (theirs or "none")[:16]))
        return diff
