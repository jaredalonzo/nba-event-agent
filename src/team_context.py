"""Current-season team context provider: coach, record, seed, roster.

Unlike player_profiles.json (which caches forever), team context expires after
_TTL_HOURS because coaches, records, and rosters change mid-season.

Cache key is (team_tricode, game_date) so each game day gets its own snapshot —
no cross-day bleed and the file gives a per-game-day audit trail.

MCP boundary note: team context stays LOCAL (not behind MCP) because the
injection path needs it synchronously inside the graph, and the data is
session-current (TTL-bound), not permanent reference data.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from nba_api.stats.endpoints import commonteamroster, leaguestandingsv3
from nba_api.stats.static import teams as nba_teams_static

_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "team_context.json"
_TTL_HOURS = 24


def _load_cache() -> dict[str, Any]:
    if not _CACHE_PATH.exists():
        return {}
    try:
        with _CACHE_PATH.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _write_cache(cache: dict[str, Any]) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _CACHE_PATH.open("w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)


class TeamContextProvider:
    """Provides current-season team context (coach, record, seed, roster).

    One provider, two consumers:
      - Injection path in generate_insight (grounding floor, non-optional)
      - get_team_context tool in the classifier loop (opt-in depth)
    """

    def __init__(self) -> None:
        self._cache: dict[str, Any] = _load_cache()

    def get(self, team_tricode: str, game_date: str) -> dict:
        """Return team context for the given tricode and game date.

        Args:
            team_tricode: e.g. "LAL", "GSW"
            game_date: ISO date string "YYYY-MM-DD"

        Returns:
            dict with: coach, record, seed, roster {player_id: name}
        """
        if not team_tricode:
            return {}
        key = f"{team_tricode}::{game_date}"
        entry = self._cache.get(key)
        if entry and _is_fresh(entry):
            return entry

        data = self._fetch_from_api(team_tricode)
        data["fetched_at"] = datetime.now(tz=timezone.utc).isoformat()
        self._cache[key] = data
        _write_cache(self._cache)
        return data

    def _fetch_from_api(self, team_tricode: str) -> dict:
        """Fetch fresh team context from nba_api.

        Two calls:
          1. leaguestandingsv3 → Record + PlayoffRank for this team
          2. commonteamroster(team_id) → head coach (COACH_TYPE="Head Coach") + roster

        team_id resolution uses the static teams table — no extra API call.
        Note: IS_ASSISTANT is unreliable (=1 even for head coaches); filter
        by COACH_TYPE instead.
        """
        team_info = nba_teams_static.find_team_by_abbreviation(team_tricode)
        if not team_info:
            return {"coach": None, "record": None, "seed": None, "roster": {}}
        team_id = team_info["id"]

        # --- standings: record + seed ---
        standings = leaguestandingsv3.LeagueStandingsV3().get_normalized_dict().get("Standings", [])
        team_row = next((r for r in standings if r["TeamID"] == team_id), None)
        record = team_row["Record"] if team_row else None
        seed = team_row["PlayoffRank"] if team_row else None

        # --- roster + head coach ---
        roster_data = commonteamroster.CommonTeamRoster(team_id=str(team_id)).get_normalized_dict()

        roster = {
            str(p["PLAYER_ID"]): p["PLAYER"]
            for p in roster_data.get("CommonTeamRoster", [])
            if p.get("PLAYER_ID") and p.get("PLAYER")
        }

        coaches = roster_data.get("Coaches", [])
        head_coach = next((c for c in coaches if c.get("COACH_TYPE") == "Head Coach"), None)
        coach = head_coach["COACH_NAME"] if head_coach else None

        return {"coach": coach, "record": record, "seed": seed, "roster": roster}


def _is_fresh(entry: dict) -> bool:
    fetched_at = entry.get("fetched_at")
    if not fetched_at:
        return False
    try:
        ts = datetime.fromisoformat(fetched_at)
        return datetime.now(tz=timezone.utc) - ts < timedelta(hours=_TTL_HOURS)
    except ValueError:
        return False


# Module-level singleton shared by the injection path and get_team_context tool.
team_context_provider = TeamContextProvider()
