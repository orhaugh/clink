"""The evidence set, `clink-upgrade-evidence` version 1.

    evidence.json      what the set is: the test definition's SHA-256, the
                       baseline's identity, the savepoint (job, checkpoint,
                       the dataset's digest, the last watermark before it, the
                       baseline job's operators), the checks made when it was
                       recorded, and every other file's SHA-256 and size
    test.json          the test definition, byte for byte
    input/             every segment of the input
    checkpoint/        the savepoint as a checkpoint dataset
                       (`clink state-export --checkpoint-dir`)
    output/            what the baseline's sinks wrote before the savepoint
    reference.json     the independent reference's expected rows and states

The set's digest is the SHA-256 of evidence.json, recorded somewhere the set
is not. Verification needs that digest: every file is then bound through
evidence.json, a file it does not list is refused, and a link is never
followed. A set is never rewritten to suit a build: `check` reads it, copies
what a build needs, and hashes it again at the end.
"""

import hashlib
import json
import os
import stat

from . import EVIDENCE_FORMAT, EVIDENCE_VERSION
from .builds import sha256_file

INDEX = "evidence.json"


class EvidenceInvalid(Exception):
    """The set is not the one recorded, or not whole."""


class EvidenceUnsupported(Exception):
    """A set in a format or version this release does not read."""


def walk(root):
    """Every regular file under root, relative, refusing links and specials."""
    out = []
    for d, dirs, files in os.walk(root, followlinks=False):
        for name in list(dirs):
            if os.path.islink(os.path.join(d, name)):
                raise EvidenceInvalid("%s is a link, which evidence never holds"
                                      % os.path.relpath(os.path.join(d, name), root))
        for name in files:
            p = os.path.join(d, name)
            mode = os.lstat(p).st_mode
            if not stat.S_ISREG(mode):
                raise EvidenceInvalid("%s is not a regular file" % os.path.relpath(p, root))
            out.append(os.path.relpath(p, root))
    return sorted(out)


def file_table(root):
    return {rel: {"sha256": sha256_file(os.path.join(root, rel)),
                  "bytes": os.path.getsize(os.path.join(root, rel))}
            for rel in walk(root) if rel != INDEX}


def write_index(root, doc):
    doc = dict(doc)
    doc["format"] = EVIDENCE_FORMAT
    doc["version"] = EVIDENCE_VERSION
    doc["files"] = file_table(root)
    text = (json.dumps(doc, indent=1, sort_keys=True) + "\n").encode()
    with open(os.path.join(root, INDEX), "wb") as f:
        f.write(text)
    return hashlib.sha256(text).hexdigest()


def verify(root, digest):
    """Returns the index once every file is what it records. Raises
    EvidenceInvalid or EvidenceUnsupported with the reason."""
    if not digest or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise EvidenceInvalid("the evidence digest must be 64 lower-case hex characters")
    if os.path.islink(root) or not os.path.isdir(root):
        raise EvidenceInvalid("%s is not a directory (a link is not followed)" % root)
    index = os.path.join(root, INDEX)
    if os.path.islink(index) or not os.path.isfile(index):
        raise EvidenceInvalid("%s has no %s" % (root, INDEX))
    with open(index, "rb") as f:
        text = f.read()
    actual = hashlib.sha256(text).hexdigest()
    if actual != digest:
        raise EvidenceInvalid("%s has SHA-256 %s, not the recorded %s: this is not the evidence "
                              "that was recorded" % (INDEX, actual, digest))
    try:
        doc = json.loads(text)
    except ValueError as e:
        raise EvidenceInvalid("%s is not JSON: %s" % (INDEX, e))
    if doc.get("format") != EVIDENCE_FORMAT:
        raise EvidenceUnsupported("format %r, not %r" % (doc.get("format"), EVIDENCE_FORMAT))
    if doc.get("version") != EVIDENCE_VERSION:
        raise EvidenceUnsupported("version %r of %s; this release reads version %d"
                                  % (doc.get("version"), EVIDENCE_FORMAT, EVIDENCE_VERSION))
    listed = doc.get("files") or {}
    present = [r for r in walk(root) if r != INDEX]
    extra = sorted(set(present) - set(listed))
    if extra:
        raise EvidenceInvalid("%d file(s) the index does not list, first %s" % (len(extra), extra[0]))
    for rel, want in sorted(listed.items()):
        p = os.path.join(root, rel)
        if os.path.normpath(rel).startswith("..") or os.path.isabs(rel):
            raise EvidenceInvalid("the index lists %r outside the set" % rel)
        if not os.path.isfile(p):
            raise EvidenceInvalid("%s is listed and missing" % rel)
        size = os.path.getsize(p)
        if size != want.get("bytes"):
            raise EvidenceInvalid("%s has %d bytes, the index records %s (truncated or extended)"
                                  % (rel, size, want.get("bytes")))
        if sha256_file(p) != want.get("sha256"):
            raise EvidenceInvalid("%s does not hash to what the index records" % rel)
    ds = os.path.join(root, "checkpoint", "dataset.json")
    want = (doc.get("checkpoint") or {}).get("dataset_digest")
    if not os.path.isfile(ds) or sha256_file(ds) != want:
        raise EvidenceInvalid("the checkpoint dataset is not the one whose digest the index records")
    return doc


def fingerprint(root):
    """Every file's hash, evidence.json included, to show the set unchanged."""
    return {rel: sha256_file(os.path.join(root, rel)) for rel in walk(root)}
