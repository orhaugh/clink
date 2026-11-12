"""clink-audit upgrade: validate a candidate build against retained evidence.

    clink-audit upgrade record --test=FILE --out=DIR --tools-clink=PATH
        --baseline-clink=PATH --baseline-node=PATH --baseline-job=PATH

    clink-audit upgrade check --test=FILE --evidence=DIR --evidence-digest=SHA256
        --out=DIR --tools-clink=PATH
        --baseline-clink=PATH --baseline-node=PATH --baseline-job=PATH
        --candidate-clink=PATH --candidate-node=PATH --candidate-job=PATH

Every path is explicit: nothing is taken from the evidence, and nothing it
names is run. `record` prints the evidence's digest, which is kept somewhere
the evidence is not and given back to `check`.

Limits: --timeout-s bounds the whole command, --run-timeout-s each build's
run, --max-rss-mib the resident memory of a run's processes and
--max-output-mib the bytes a run writes. A limit reached stops the run and
leaves its checks unavailable, never passed.

Exit status: 0 every required check passed; 1 a check failed (record: a
record-time check failed, and the evidence is still written with it); 3 not
established (a required check could not run or is unsupported, or the run was
interrupted); 2 bad usage.
"""

import argparse
import os
import signal
import sys

from . import EXIT_NOT_ESTABLISHED, EXIT_USAGE
from . import definition, gate
from .builds import BuildError
from .isolate import NETNS_ENV, OUTSIDE_ENV, RunIsolation


def _parser():
    ap = argparse.ArgumentParser(prog="clink-audit upgrade", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    def common(p, candidate):
        p.add_argument("--test", required=True, help="the test definition (clink-upgrade-test)")
        p.add_argument("--out", required=True, help="a fresh directory")
        p.add_argument("--tools-clink", required=True,
                       help="the clink that exports, verifies and lists datasets (not under test)")
        names = ("baseline", "candidate") if candidate else ("baseline",)
        for b in names:
            p.add_argument("--%s-clink" % b, required=True)
            p.add_argument("--%s-node" % b, required=True, help="its clink_node")
            p.add_argument("--%s-job" % b, required=True, help="the job module built against it")
        p.add_argument("--timeout-s", type=float, default=900)
        p.add_argument("--run-timeout-s", type=float, default=180)
        p.add_argument("--max-rss-mib", type=int, default=4096)
        p.add_argument("--max-output-mib", type=float, default=256)

    p = sub.add_parser("record", help="record evidence with the baseline")
    common(p, False)
    p.add_argument("--work", help="where the baseline's run is kept (default <out>.work)")
    p = sub.add_parser("check", help="check a candidate against recorded evidence")
    common(p, True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--evidence-digest", required=True, help="the digest record printed")
    return ap


def main(argv=None, isolation=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(argv)
    iso = isolation or RunIsolation()
    if isolation is None and iso.needs_reexec():
        wrapped = iso.reexec_argv([sys.executable, "-m", "clink_audit", "upgrade"] + argv)
        if wrapped is None:
            print("clink-audit upgrade: no loopback-only namespace can be made here (unshare is "
                  "needed); nothing ran", file=sys.stderr)
            return EXIT_NOT_ESTABLISHED
        env = dict(os.environ)
        env[NETNS_ENV] = "1"
        env[OUTSIDE_ENV] = iso.outside_probe()
        package_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        env["PYTHONPATH"] = os.pathsep.join(p for p in (package_root, env.get("PYTHONPATH")) if p)
        os.execvpe(wrapped[0], wrapped, env)
    iso.establish()

    stop = {"why": ""}

    def on_signal(signum, _frame):
        stop["why"] = "signal %d" % signum

    old = {s: signal.signal(s, on_signal) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        if args.command == "record":
            return gate.record(args, iso, lambda: stop["why"])
        return gate.check(args, iso, lambda: stop["why"])
    except (gate.Usage, BuildError, definition.DefinitionError, OSError) as e:
        print("clink-audit upgrade: %s" % e, file=sys.stderr)
        return EXIT_USAGE
    finally:
        for s, h in old.items():
            signal.signal(s, h)
