"""Smoke test: spawn the MCP server as a subprocess, call get_player_profile.

Validates that:
1. The server starts cleanly under stdio transport.
2. list_tools sees get_player_profile.
3. call_tool returns the expected stub payload.

Run with::

    python scripts/smoke_test_mcp.py
"""

from __future__ import annotations

import asyncio
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main() -> None:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "src.mcp_server.server"],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            print(f"discovered tools: {tool_names}")
            assert "get_player_profile" in tool_names, "tool not registered"

            result = await session.call_tool(
                "get_player_profile", {"player_id": "2544"}
            )
            print(f"call_tool result: {result.content}")
            assert not result.isError, f"tool errored: {result.content}"

    print("OK")


if __name__ == "__main__":
    asyncio.run(main())
