#!/usr/bin/env python3
"""QUAL-05's end-state oracle, against injected defects, with no container.

The oracle is the only thing standing between a rig run and a green page,
so it gets the same treatment the campaigns give the engine: every defect
class it claims to detect is injected and it must name that one and only
that one. QUAL-02's equivalent found a real defect in itself on its first
attempt, which is the standard here.

psycopg2 is stubbed rather than run, so this costs nothing and runs on
every invocation of run-all.sh instead of only under --with-docker.
"""
import io
import json
import pathlib
import sys
import tempfile
import types
from contextlib import redirect_stdout

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "qual01"))  # detspec, which endstate imports

SEED = 4242
PARTITIONS = 2
KEYS = 50
EPS = 100
BASE_MS = 1_700_000_000_000
KEY_EPOCH_MS = 10_000
PER_PARTITION = 3000  # 30s of event time: three key epochs


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._emit = False

    def execute(self, sql):
        self._emit = "FROM" in sql.upper()

    def __iter__(self):
        return iter(self._rows if self._emit else [])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.autocommit = False

    def cursor(self):
        return FakeCursor(self._rows)

    def close(self):
        pass


def install_fake_psycopg2(rows):
    mod = types.ModuleType("psycopg2")
    mod.connect = lambda *a, **k: FakeConn(rows)

    class Error(Exception):
        pass

    mod.Error = Error
    sys.modules["psycopg2"] = mod


def truth():
    """What the engine SHOULD hold: {key: count} over everything produced."""
    from detspec import Spec

    spec = Spec(SEED, PARTITIONS, KEYS, EPS, BASE_MS, 0, 10000, KEY_EPOCH_MS)
    out = {}
    for p in range(PARTITIONS):
        for seq in range(PER_PARTITION):
            key, _a, _t = spec.event(p, seq)
            out[key] = out.get(key, 0) + 1
    return out


def run_oracle(rows):
    """Run endstate.py against `rows` and return its key=value output."""
    install_fake_psycopg2(rows)
    for name in ("endstate",):
        sys.modules.pop(name, None)
    sys.path.insert(0, str(HERE))
    import endstate  # noqa: E402

    with tempfile.TemporaryDirectory() as tmp:
        prog = pathlib.Path(tmp) / "progress.json"
        prog.write_text(json.dumps(
            {"produced_high": {str(p): PER_PARTITION for p in range(PARTITIONS)}}))
        argv = [
            "endstate.py", "--dsn", "fake", "--table", "public.q5_out",
            "--progress", str(prog), "--seed", str(SEED),
            "--partitions", str(PARTITIONS), "--keys", str(KEYS),
            "--eps", str(EPS), "--base-ms", str(BASE_MS),
            "--key-epoch-ms", str(KEY_EPOCH_MS),
        ]
        old = sys.argv
        sys.argv = argv
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                endstate.main()
        finally:
            sys.argv = old
    return dict(
        line.split("=", 1) for line in buf.getvalue().splitlines() if "=" in line
    )


failures = []


def check(name, ok, detail=""):
    if ok:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}{': ' + detail if detail else ''}")
        failures.append(name)


EXPECTED = truth()
PRODUCED = PARTITIONS * PER_PARTITION
clean_rows = sorted(EXPECTED.items())

# The workload has to turn its key space over, or none of this is testing
# what the campaign measures.
check("the fixture's key space turns over",
      len(EXPECTED) > KEYS, f"only {len(EXPECTED)} distinct keys for a {KEYS}-key epoch block")

# --- a clean table ------------------------------------------------------------
r = run_oracle(clean_rows)
check("a correct table is reported exact",
      r["produced_total"] == str(PRODUCED) and r["sum_n"] == r["produced_total"],
      f"produced={r['produced_total']} sum_n={r['sum_n']}")
check("a correct table has no missing keys", r["keys_missing"] == "0", r["keys_missing"])
check("a correct table has no wrong counts", r["keys_wrong_n"] == "0", r["keys_wrong_n"])
check("a correct table has no fabricated keys", r["keys_fabricated"] == "0", r["keys_fabricated"])
check("a correct table judged every key",
      r["keys_checked"] == str(len(EXPECTED)), r["keys_checked"])

# --- a LOST key ----------------------------------------------------------------
r = run_oracle(clean_rows[1:])
check("a missing key is named as missing", r["keys_missing"] == "1", r["keys_missing"])
check("a missing key is not reported as fabricated", r["keys_fabricated"] == "0")
check("a missing key breaks exact accounting", r["sum_n"] != r["produced_total"])

# --- a key folded the WRONG number of times ------------------------------------
bad = list(clean_rows)
bad[3] = (bad[3][0], bad[3][1] + 1)
r = run_oracle(bad)
check("an over-counted key is named", r["keys_wrong_n"] == "1", r["keys_wrong_n"])
check("an over-counted key is not reported as missing", r["keys_missing"] == "0")
check("an over-counted key breaks exact accounting",
      int(r["sum_n"]) == PRODUCED + 1, r["sum_n"])

# --- a key the engine INVENTED ---------------------------------------------------
r = run_oracle(clean_rows + [(10 ** 12, 5)])
check("a fabricated key is named as fabricated", r["keys_fabricated"] == "1", r["keys_fabricated"])
check("a fabricated key is not reported as missing", r["keys_missing"] == "0")

# --- a NULL count, the QUAL-04 silent-NULL shape -----------------------------------
# An unaliased projection into a name-resolved sink writes NULL into every
# row. It cost QUAL-04 a round, and a `<>` gate swallowed it because NULL
# comparisons are NULL. Counted separately here so it can never be silent.
r = run_oracle([(clean_rows[0][0], None)] + clean_rows[1:])
check("a NULL count is counted", r["rows_with_null_n"] == "1", r["rows_with_null_n"])
check("a NULL count is also a missing key rather than a silent pass",
      r["keys_missing"] == "1", r["keys_missing"])

# --- an EMPTY table ----------------------------------------------------------------
r = run_oracle([])
check("an empty table is not mistaken for a clean one",
      r["keys_missing"] == str(len(EXPECTED)) and r["sum_n"] == "0",
      f"missing={r['keys_missing']} sum_n={r['sum_n']}")

print(f"\n{len(failures)} failed")
sys.exit(1 if failures else 0)
