"""clink-audit: repeated, bounded audits of a running clink job's checkpoints.

    clink-audit run --config=FILE            one cycle, for cron or another scheduler
    clink-audit poll --config=FILE           cycles in the foreground, every poll_interval_s
    clink-audit status --config=FILE         the status, from the state directory alone
    clink-audit acknowledge --config=FILE --audit=ID --note=TEXT
    clink-audit acknowledge --config=FILE --checkpoints=N[-M] --note=TEXT
    clink-audit check-config --config=FILE
    clink-audit upgrade record|check ...      upgrade validation (clink-audit upgrade --help)

Each cycle audits completed checkpoints of a Kafka -> clink -> ClickHouse
record-wise job with the existing workflow: incident-collect, incident-scope,
the operator's reference, incident-verify under network isolation, and
incident-reconcile with the reference, reading the destination read-only for
the scope's keys. It is repeated bounded auditing of the scopes it selects,
and its status says which were verified, which were not and why; it is not a
continuous proof of every result.

Exit status (run, poll, status): 0 healthy, 1 an unacknowledged discrepancy,
3 incomplete, behind, stale or stopped, 5 no eligible scope yet, 4 another
runner holds the state directory, 2 bad usage or configuration.
"""

import argparse
import json
import signal
import sys
import time

from . import config as config_mod
from . import status as status_mod
from .runner import Runner
from .state import Busy, StateDir

EXIT_BUSY = 4
EXIT_USAGE = 2


def _parser():
    ap = argparse.ArgumentParser(prog="clink-audit", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    def common(p, need_config=True):
        p.add_argument("--config", required=need_config, help="the runner's JSON configuration")
        p.add_argument("--state-dir", help="overrides the configuration's state_dir")
        p.add_argument("--json", action="store_true", help="print the status as JSON")

    p = sub.add_parser("run", help="one cycle: collect, audit and publish what is eligible, then exit")
    common(p)
    p = sub.add_parser("poll", help="cycles in the foreground until stopped (SIGINT or SIGTERM)")
    common(p)
    p.add_argument("--cycles", type=int, default=0, help="stop after this many cycles (0: run until stopped)")
    p = sub.add_parser("status", help="the status, read from the state directory")
    common(p, need_config=False)
    p = sub.add_parser("acknowledge", help="acknowledge a discrepancy or gap, which stays listed")
    common(p, need_config=False)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--audit", help="an audit's id, as its report names it")
    g.add_argument("--checkpoints", help="a checkpoint, or a range N-M, every result of which is acknowledged")
    p.add_argument("--note", required=True, help="what was found and done")
    p = sub.add_parser("check-config", help="load and check the configuration, then exit")
    p.add_argument("--config", required=True)
    p.add_argument("--state-dir")
    return ap


def _state_dir(args):
    if args.state_dir:
        return args.state_dir
    if args.config:
        try:
            raw = json.load(open(args.config))
        except (OSError, ValueError) as e:
            raise config_mod.ConfigError("cannot read %s: %s" % (args.config, e))
        if raw.get("state_dir"):
            return raw["state_dir"]
    raise config_mod.ConfigError("name the state directory (--state-dir, or state_dir in --config)")


def _print_status(state_dir, as_json):
    s = status_mod.write(state_dir)
    if as_json:
        print(json.dumps(s, indent=1, sort_keys=True))
    else:
        print(status_mod.render_text(s))
    return s["exit"]


def _cycle(cfg, isolation, interrupt):
    d = StateDir(cfg.state_dir)
    d.lock()
    try:
        Runner(cfg, isolation=isolation, interrupt=interrupt).cycle()
    finally:
        d.unlock()


def main(argv=None, isolation=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["upgrade"]:
        from .upgrade.cli import main as upgrade_main
        return upgrade_main(argv[1:])
    args = _parser().parse_args(argv)
    try:
        if args.command == "check-config":
            cfg = config_mod.load(args.config, args.state_dir)
            print("configuration %s: job %d at %s, state %s, fingerprint %s"
                  % (cfg.path, cfg.job_id, cfg.checkpoint_dir, cfg.state_dir, cfg.fingerprint[:16]))
            return 0
        if args.command == "status":
            return _print_status(_state_dir(args), args.json)
        if args.command == "acknowledge":
            d = StateDir(_state_dir(args))
            if args.audit:
                rec = d.acknowledge(args.note, audit_id=args.audit)
            else:
                lo, _, hi = args.checkpoints.partition("-")
                first, last = int(lo), int(hi or lo)
                if last < first:
                    raise config_mod.ConfigError("--checkpoints takes N or N-M with M at least N")
                rec = d.acknowledge(args.note, checkpoints=(first, last))
            print("acknowledged: %s" % json.dumps(rec, sort_keys=True))
            return 0
        cfg = config_mod.load(args.config, args.state_dir)
    except (config_mod.ConfigError, ValueError) as e:
        print("clink-audit: %s" % e, file=sys.stderr)
        return EXIT_USAGE

    stop = {"why": ""}

    def on_signal(signum, _frame):
        stop["why"] = "signal %d" % signum

    old = {s: signal.signal(s, on_signal) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        if args.command == "run":
            _cycle(cfg, isolation, lambda: stop["why"])
            return _print_status(cfg.state_dir, args.json)
        cycles = 0
        code = 0
        while not stop["why"]:
            started = time.monotonic()
            _cycle(cfg, isolation, lambda: stop["why"])
            code = _print_status(cfg.state_dir, args.json)
            cycles += 1
            if args.cycles and cycles >= args.cycles:
                break
            pause = cfg.limits["poll_interval_s"] - (time.monotonic() - started)
            end = time.monotonic() + max(0.0, pause)
            while not stop["why"] and time.monotonic() < end:
                time.sleep(min(0.2, end - time.monotonic()) if end > time.monotonic() else 0)
        return code
    except Busy as e:
        print("clink-audit: %s" % e, file=sys.stderr)
        return EXIT_BUSY
    except ValueError as e:
        print("clink-audit: %s" % e, file=sys.stderr)
        return EXIT_USAGE
    finally:
        for s, h in old.items():
            signal.signal(s, h)


if __name__ == "__main__":
    sys.exit(main())
