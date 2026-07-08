"""pyclink end-to-end against a built libclink.

Point CLINK_LIB at the shared library (defaults to the conventional build
trees relative to the repo root). Runnable standalone (python3
test_pyclink.py) or under pytest.
"""

import json
import os
import pathlib
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "python"))

import pyclink  # noqa: E402


def _lib_path() -> str:
    env = os.environ.get("CLINK_LIB")
    if env:
        return env
    for tree in ("build-host", "build", "build-release"):
        for name in ("libclink.dylib", "libclink.so"):
            p = REPO / tree / name
            if p.exists():
                return str(p)
    raise RuntimeError("set CLINK_LIB to a built libclink")


def _write_orders(path: pathlib.Path) -> None:
    rows = [
        {"user_id": 1, "amount": 10},
        {"user_id": 2, "amount": 20},
        {"user_id": 1, "amount": 30},
        {"user_id": 2, "amount": 5},
        {"user_id": 1, "amount": 7},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_collect_end_to_end():
    import pyarrow as pa

    with tempfile.TemporaryDirectory() as d:
        data = pathlib.Path(d) / "orders.ndjson"
        _write_orders(data)
        with pyclink.Engine(lib_path=_lib_path()) as e:
            e.execute(
                "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
                f"WITH (connector='file', format='json', path='{data}');"
                "CREATE TABLE results (user_id BIGINT, amount BIGINT) "
                "WITH (connector='collect');"
                "INSERT INTO results SELECT user_id, amount FROM orders"
            )
            assert e.job_count == 1
            table = e.collect("results").read_all()
            e.await_all(timeout_ms=30_000)

        assert isinstance(table, pa.Table)
        assert table.schema.names == ["user_id", "amount"]
        assert table.schema.field("amount").type == pa.int64()
        assert table.num_rows == 5
        assert sum(table.column("amount").to_pylist()) == 72


def test_execute_error_raises():
    with pyclink.Engine(lib_path=_lib_path()) as e:
        try:
            e.execute("DEFINITELY NOT SQL")
        except pyclink.ClinkError as err:
            assert str(err)
        else:
            raise AssertionError("bad SQL must raise ClinkError")


def test_close_wakes_blocked_stream():
    with pyclink.Engine(lib_path=_lib_path()) as e:
        e.execute("CREATE TABLE never (a BIGINT) WITH (connector='collect')")
        reader = e.collect("never")
    # Engine closed with no producer: the read must error, not hang.
    try:
        reader.read_all()
    except Exception:
        pass
    else:
        raise AssertionError("read on an aborted stream must raise")


if __name__ == "__main__":
    test_collect_end_to_end()
    test_execute_error_raises()
    test_close_wakes_blocked_stream()
    print("pyclink tests: PASS")
