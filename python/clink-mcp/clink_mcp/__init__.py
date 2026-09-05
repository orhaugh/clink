"""clink-mcp: an MCP server over clink's diagnostic surface.

The server exposes the engine's existing diagnostic primitives - checkpoint
and savepoint inspection, the record-capture flight recorder and
deterministic replay, lineage, queryable state, configuration lint and
``EXPLAIN`` - as MCP tools any MCP client can call. It adds no engine
feature and changes nothing about a running job: every tool is read-only
except ``replay``, which writes only to the paths its caller names.

See ``python/clink-mcp/README.md`` and the published guide
``docs/guides/diagnosing-a-pipeline-with-an-agent.md``.
"""

from clink_mcp.server import Config, build_server

__version__ = "0.1.0"

__all__ = ["Config", "build_server", "__version__"]
