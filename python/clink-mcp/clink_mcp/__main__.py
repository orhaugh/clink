"""``python -m clink_mcp`` / ``clink-mcp``: serve clink's diagnostic surface over MCP on stdio."""

from __future__ import annotations

import argparse
import os
import shutil
import sys

from clink_mcp.server import Config, build_server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="clink-mcp",
        description=(
            "Serve clink's diagnostic surface as MCP tools over stdio. Read-only: the "
            "server inspects checkpoints, captures, lineage and live state, lints and "
            "explains, and never submits, cancels, rescales or commits anything. The one "
            "tool that writes, replay, writes only to the paths its caller names."
        ),
    )
    parser.add_argument(
        "--clink",
        default=os.environ.get("CLINK_BIN", "clink"),
        help="path to the clink CLI (default: $CLINK_BIN, else `clink` on PATH)",
    )
    parser.add_argument(
        "--coordinator",
        default=os.environ.get("CLINK_COORDINATOR_URL"),
        help=(
            "base URL of a coordinator's HTTP API, e.g. http://127.0.0.1:8081, for the "
            "live-cluster tools (default: $CLINK_COORDINATOR_URL; unset disables them)"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("CLINK_MCP_TIMEOUT_S", "300")),
        help="seconds a single CLI invocation or HTTP request may take (default 300)",
    )
    args = parser.parse_args(argv)

    resolved = shutil.which(args.clink) if os.sep not in args.clink else args.clink
    if resolved is None or not os.path.exists(resolved):
        print(
            f"clink-mcp: clink binary not found at '{args.clink}'; pass --clink or set CLINK_BIN",
            file=sys.stderr,
        )
        return 2

    server = build_server(
        Config(clink_bin=resolved, coordinator_url=args.coordinator, timeout_s=args.timeout)
    )
    server.run("stdio")
    return 0


if __name__ == "__main__":
    sys.exit(main())
