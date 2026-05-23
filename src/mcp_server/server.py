"""MCP server exposing NBA career-level player context.

Why a separate server (vs. another `@tool` in `src/tools.py`):

The existing tools in `src/tools.py` operate on the *current* game — box-score
stats, scoring runs, alerts. ``get_player_profile`` is a different shape: it
pulls career-level data (bio, career averages, career highs) that doesn't
change mid-game. Running it in its own process keeps that long-lived data
fetcher and its on-disk cache separate from the per-event tool surface, and
demonstrates MCP integration as a portfolio piece.

Transport is stdio: the agent spawns this module as a subprocess and talks to
it over the MCP protocol. No port, no separate terminal — one-command demo.

M1 (current): stub tool. Validates protocol wiring before nba_api is involved.
M2: real impl backed by CommonPlayerInfo + PlayerCareerStats with a
    write-through cache to ``data/player_profiles.json``.

Run standalone for testing::

    python -m src.mcp_server.server
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("nba-player-profile")


@mcp.tool()
def get_player_profile(player_id: str) -> dict:
    """Return career-level context for an NBA player.

    M1 stub — returns a placeholder so we can verify the MCP round-trip
    end-to-end before wiring in ``nba_api``. M2 will replace this body with
    a real CommonPlayerInfo + PlayerCareerStats fetch plus disk cache.

    Args:
        player_id: NBA player ID (e.g., "2544" for LeBron James).

    Returns:
        dict with at least: name, position, height, weight, draft info,
        seasons_played, career_averages, career_highs.
    """
    return {
        "player_id": player_id,
        "stub": True,
        "note": "M1 stub — real impl lands in M2",
    }


if __name__ == "__main__":
    mcp.run(transport="stdio")
