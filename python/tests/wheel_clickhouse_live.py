"""Live ClickHouse round trips over TLS through an installed pyclink wheel.

Run by the wheels workflow's verify job against a ClickHouse server with the
native protocol over TLS (docker/integration-services.yml, clickhouse-26-8-tls),
from outside the source tree and with CLINK_LIB unset, so the library under test
is the one the wheel bundles. The tables must exist, each (id Int64, v String);
the job counts their rows from outside with clickhouse-client afterwards.

Each case inserts ROWS rows from a file source through connector='clickhouse',
insert_format='native', secure='true' to host CLICKHOUSE_HOST on
CLICKHOUSE_TLS_PORT:

  ok       tls_ca_file is the CA that signed the server: lands in tls_ok.
  wrong    tls_ca_file is an unrelated CA: the job fails with
           clickhouse.tls_verify_failed and tls_wrong stays empty.
  default  no tls_ca_file, with SSL_CERT_FILE set to the CA by this script: lands
           in tls_default.
  system   no tls_ca_file and neither SSL_CERT_FILE nor SSL_CERT_DIR (refused if
           either is set): the CA must be found where the distribution keeps its
           bundle. Lands in tls_default.

Before any case, Python's own ssl module is loaded and a context created, so the
process holds the system OpenSSL beside the one linked into libclink.

    CLICKHOUSE_TLS_CA=ca.crt CLICKHOUSE_TLS_WRONG_CA=wrong-ca.crt \\
        python wheel_clickhouse_live.py ok wrong default

Environment: CLICKHOUSE_HOST (localhost), CLICKHOUSE_TLS_PORT (19458),
CLICKHOUSE_TLS_CA and CLICKHOUSE_TLS_WRONG_CA (paths, needed by the cases that
use them), ROWS (1000). Cases default to ok, wrong and default.
"""

from __future__ import annotations

import json
import os
import ssl
import sys
import tempfile

import pyclink

HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
PORT = os.environ.get("CLICKHOUSE_TLS_PORT", "19458")
ROWS = int(os.environ.get("ROWS", "1000"))
CASES = ("ok", "wrong", "default", "system")


def env_path(name: str) -> str:
    value = os.environ.get(name, "")
    if not value or not os.path.isfile(value):
        sys.exit(f"wheel_clickhouse_live: set {name} to an existing file")
    return value


def insert(rows_path: str, table: str, tls_ca_file: str | None) -> None:
    ca = f", tls_ca_file='{tls_ca_file}'" if tls_ca_file else ""
    with pyclink.Engine() as engine:
        engine.execute(f"""
            CREATE TABLE f (id BIGINT, v STRING) WITH (
              connector='file', format='json', path='{rows_path}');
            CREATE TABLE ch (id BIGINT, v STRING) WITH (
              connector='clickhouse', insert_format='native', host='{HOST}',
              port='{PORT}', secure='true'{ca}, table='{table}');
            INSERT INTO ch SELECT id, v FROM f
        """)
        engine.await_all(120000)


def expect_refused(rows_path: str, table: str, tls_ca_file: str) -> None:
    try:
        insert(rows_path, table, tls_ca_file)
    except pyclink.ClinkError as e:
        if "clickhouse.tls_verify_failed" not in str(e):
            sys.exit(f"wheel_clickhouse_live: wrong CA failed, but not as a verify failure: {e}")
        cause = next(line for line in str(e).splitlines() if "clickhouse.tls_verify_failed" in line)
        print(f"wrong: refused as expected: {cause.strip()[:300]}")
        return
    sys.exit("wheel_clickhouse_live: an unrelated CA was accepted")


def main() -> None:
    if os.environ.get("CLINK_LIB"):
        sys.exit("wheel_clickhouse_live: unset CLINK_LIB so the wheel's own library is tested")
    cases = sys.argv[1:] or ["ok", "wrong", "default"]
    unknown = [c for c in cases if c not in CASES]
    if unknown:
        sys.exit(f"wheel_clickhouse_live: unknown case(s) {unknown}; choose from {CASES}")
    if "system" in cases and (os.environ.get("SSL_CERT_FILE") or os.environ.get("SSL_CERT_DIR")):
        sys.exit("wheel_clickhouse_live: the system case needs SSL_CERT_FILE and SSL_CERT_DIR unset")
    if "system" in cases and "default" in cases:
        sys.exit("wheel_clickhouse_live: default and system both write tls_default; run one")

    # The system OpenSSL, through Python's ssl module, loaded and used first.
    ctx = ssl.create_default_context()
    print(f"python ssl: {ssl.OPENSSL_VERSION}, verify_mode={ctx.verify_mode.name}")

    with tempfile.TemporaryDirectory() as tmp:
        rows_path = os.path.join(tmp, "rows.ndjson")
        with open(rows_path, "w") as fh:
            for i in range(ROWS):
                fh.write(json.dumps({"id": i, "v": f"r{i}"}) + "\n")
        for case in cases:
            if case == "ok":
                insert(rows_path, "tls_ok", env_path("CLICKHOUSE_TLS_CA"))
                print(f"ok: {ROWS} rows sent to tls_ok with tls_ca_file")
            elif case == "wrong":
                expect_refused(rows_path, "tls_wrong", env_path("CLICKHOUSE_TLS_WRONG_CA"))
            elif case == "default":
                os.environ["SSL_CERT_FILE"] = env_path("CLICKHOUSE_TLS_CA")
                try:
                    insert(rows_path, "tls_default", None)
                finally:
                    del os.environ["SSL_CERT_FILE"]
                print(f"default: {ROWS} rows sent to tls_default through SSL_CERT_FILE")
            else:
                insert(rows_path, "tls_default", None)
                print(f"system: {ROWS} rows sent to tls_default through the system CA bundle")
    print("wheel_clickhouse_live OK")


if __name__ == "__main__":
    main()
