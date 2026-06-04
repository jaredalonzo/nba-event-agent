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

Cache strategy: write-through to ``data/player_profiles.json``. Career data
doesn't change mid-game (or really, more than once per offseason), so we
treat the disk file as authoritative for any player_id we've ever fetched.
Load on import, append on miss. No TTL — if you really need fresh data,
delete the file.

Run standalone for testing::

    python -m src.mcp_server.server
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats

mcp = FastMCP("nba-player-profile")

# Cache file lives in data/ next to insights.jsonl. We compute the path
# relative to this file so the server works regardless of cwd (the agent
# may spawn it from anywhere).
_CACHE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "player_profiles.json"


def _load_cache() -> dict[str, dict[str, Any]]:
    """Load the on-disk cache, or return an empty dict if missing/corrupt.

    A corrupt cache file shouldn't crash the server — we'd rather log and
    start fresh than refuse to serve. The next write will overwrite it.
    """
    if not _CACHE_PATH.exists():
        return {}
    try:
        with _CACHE_PATH.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _write_cache(cache: dict[str, dict[str, Any]]) -> None:
    """Persist the full cache to disk. Creates ``data/`` if needed."""
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _CACHE_PATH.open("w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)


# In-memory mirror of the on-disk cache. Reads/writes go through this dict
# and we flush to disk on every miss-then-fetch. For a single agent run
# this is fine; for a long-lived server we'd batch writes.
_cache: dict[str, dict[str, Any]] = _load_cache()


def _safe_int(v: Any) -> int | None:
    """Coerce nba_api's string-typed numerics to int, tolerating empties."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _fetch_profile(player_id: str) -> dict[str, Any]:
    """Hit nba_api and assemble the profile payload. Raises on unknown ID.

    Two API calls (CommonPlayerInfo + PlayerCareerStats) — both are cached
    on the disk file after this returns, so we only pay this cost once per
    player. Each endpoint is ~200-500ms over the wire.
    """
    try:
        info_rows = (
            commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            .get_normalized_dict()
            .get("CommonPlayerInfo", [])
        )
    except KeyError as e:
        # stats.nba.com returns a malformed payload (no "resultSet" key) for
        # unknown player IDs, and the nba_api library surfaces that as a raw
        # KeyError. Translate to a clean ValueError so the MCP client sees a
        # structured failure rather than a leaky implementation detail.
        if "resultSet" in str(e):
            raise ValueError(f"unknown player_id: {player_id}") from e
        raise
    if not info_rows:
        raise ValueError(f"unknown player_id: {player_id}")
    info = info_rows[0]

    career = playercareerstats.PlayerCareerStats(
        player_id=player_id
    ).get_normalized_dict()
    totals_list = career.get("CareerTotalsRegularSeason", [])
    totals = totals_list[0] if totals_list else {}

    # Career averages: totals / games played, rounded to one decimal so the
    # narrator gets PPG-style numbers it can read out loud.
    gp = totals.get("GP") or 0
    averages: dict[str, float] = {}
    if gp:
        averages = {
            "ppg": round((totals.get("PTS") or 0) / gp, 1),
            "rpg": round((totals.get("REB") or 0) / gp, 1),
            "apg": round((totals.get("AST") or 0) / gp, 1),
        }

    # Career highs come as one row per stat. Pull the headline three.
    # Naming note: nba_api's "CareerHighs" table is *postseason* highs only.
    # We name the field accordingly so the narrator doesn't conflate them
    # with regular-season highs (which nba_api doesn't expose cleanly —
    # would require walking every season's game log).
    highs_by_stat: dict[str, int] = {}
    for row in career.get("CareerHighs", []):
        stat = row.get("STAT")
        if stat in {"PTS", "REB", "AST"} and stat not in highs_by_stat:
            val = _safe_int(row.get("STAT_VALUE"))
            if val is not None:
                highs_by_stat[stat] = val
    career_high_playoffs = {
        "points": highs_by_stat.get("PTS"),
        "rebounds": highs_by_stat.get("REB"),
        "assists": highs_by_stat.get("AST"),
    }

    # Team history: walk seasons chronologically, collecting distinct
    # team abbreviations. Excludes the current team (already exposed
    # as current_team) but preserves the order of every prior stop —
    # crucially, this means a player who returned to a previous team
    # (e.g., LeBron CLE -> MIA -> CLE -> LAL) shows ["CLE", "MIA", "CLE"]
    # rather than a deduped set, which would lose the return narrative.
    current_team = info.get("TEAM_ABBREVIATION")
    previous_teams: list[str] = []
    for r in career.get("SeasonTotalsRegularSeason", []):
        abbr = r.get("TEAM_ABBREVIATION")
        if abbr and (not previous_teams or previous_teams[-1] != abbr):
            previous_teams.append(abbr)
    # Drop the current team from the tail if present (it usually is).
    if previous_teams and previous_teams[-1] == current_team:
        previous_teams.pop()

    return {
        "player_id": player_id,
        "name": info.get("DISPLAY_FIRST_LAST"),
        "position": info.get("POSITION"),
        "height": info.get("HEIGHT"),
        "weight": _safe_int(info.get("WEIGHT")),
        "country": info.get("COUNTRY"),
        # "school" is nba_api's catch-all for the player's last basketball
        # affiliation before the NBA — could be a college, a high school
        # (for pre-2006 prep-to-pro players like LeBron), or an
        # international club (e.g., Jokić's "Mega Basket").
        "school": info.get("SCHOOL"),
        "draft": {
            "year": _safe_int(info.get("DRAFT_YEAR")),
            "round": _safe_int(info.get("DRAFT_ROUND")),
            "pick": _safe_int(info.get("DRAFT_NUMBER")),
        },
        "seasons_played": info.get("SEASON_EXP"),
        "current_team": current_team,
        "previous_teams": previous_teams,
        "career_averages": averages,
        "career_high_playoffs": career_high_playoffs,
    }


@mcp.tool()
def get_player_profile(player_id: str) -> dict:
    """Return career-level context for an NBA player.

    Useful for milestone moments where the narrator benefits from longer-arc
    context: career highs, draft position, longevity, position. Not for
    in-game stats — use ``get_player_stats`` for those.

    The result is cached to ``data/player_profiles.json`` after the first
    fetch. Subsequent calls (same process or new) are free.

    Args:
        player_id: NBA player ID as a string (e.g., "2544" for LeBron James).

    Returns:
        dict with: name, position, height, weight, country, school
        (last pre-NBA affiliation — could be college, HS, or intl club),
        draft (year/round/pick), seasons_played, current_team,
        previous_teams (chronological, excludes current; preserves
        return-to-prior-team order), career_averages (ppg/rpg/apg),
        career_high_playoffs (points/rebounds/assists — postseason only;
        nba_api doesn't expose regular-season game highs cleanly).

    Raises:
        ValueError: if ``player_id`` is unknown to nba_api.
    """
    if player_id in _cache:
        name = _cache[player_id].get("name", player_id)
        print(f"[mcp] get_player_profile: cache hit — {name} ({player_id})", file=sys.stderr, flush=True)
        return _cache[player_id]
    print(f"[mcp] get_player_profile: fetching from nba_api — player_id={player_id}", file=sys.stderr, flush=True)
    profile = _fetch_profile(player_id)
    _cache[player_id] = profile
    _write_cache(_cache)
    print(f"[mcp] get_player_profile: cached — {profile.get('name', player_id)}", file=sys.stderr, flush=True)
    return profile


if __name__ == "__main__":
    mcp.run(transport="stdio")
