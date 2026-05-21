"""LangChain tools the agent can invoke.

Two data-fetch tools live here:
    get_player_stats — pulls a player's box-score line via nba_api.
    analyze_momentum — reads the last 5 scoring plays from the agent state.

A third placeholder (send_alert) is added in M5.

Both tools are pure from the LLM's perspective: they take typed args and
return a dict. analyze_momentum additionally receives the current AgentState
via LangGraph's InjectedState mechanism (transparent to the model).

Caveat: get_player_stats returns the *final* box-score line for the game,
not stats as-of-event. The nba_api does not expose a "stats at clock T"
endpoint, so for the historical-replay demo we accept that the agent sees
final-game stats while reasoning about an in-progress event. In a true live
deployment we'd compute deltas from a live box-score feed.
"""

from __future__ import annotations

import math
from typing import Annotated, Any

from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState
from nba_api.stats.endpoints import boxscoretraditionalv3


# Module-level cache: (game_id, player_id) -> stats dict. The demo replays a
# single historical game, so stats are static and we never need to refetch.
_stats_cache: dict[tuple[str, str], dict[str, Any]] = {}


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    # float(NaN) succeeds and returns NaN, which breaks downstream comparisons
    # and serializes as the non-standard "NaN" JSON literal. Coerce to default.
    if math.isnan(result):
        return default
    return result


@tool
def get_player_stats(player_id: str, game_id: str) -> dict:
    """Fetch a player's box-score line for the given game.

    Use this when you need to check whether a player is approaching a stat
    milestone (20 pts, 10 reb, 10 ast in-game), is in foul trouble, or is
    having a notably strong/weak performance.

    Args:
        player_id: The NBA personId of the player (as a numeric string).
        game_id: The NBA gameId of the game in progress.

    Returns:
        A dict with: name, team, position, minutes, points, rebounds,
        assists, steals, blocks, turnovers, fouls, plus_minus, fg_pct,
        three_pct. On error, returns {"error": "..."}.
    """
    cache_key = (game_id, player_id)
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    try:
        bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        df = bs.get_data_frames()[0]  # player-level box score
        row = df[df["personId"] == int(player_id)]
        if row.empty:
            return {"error": f"No box-score row for player_id={player_id}"}
        r = row.iloc[0]
        result = {
            "name": f"{r.get('firstName', '')} {r.get('familyName', '')}".strip(),
            "team": r.get("teamTricode", ""),
            "position": r.get("position", ""),
            "minutes": r.get("minutes", ""),
            "points": _coerce_int(r.get("points")),
            "rebounds": _coerce_int(r.get("reboundsTotal")),
            "assists": _coerce_int(r.get("assists")),
            "steals": _coerce_int(r.get("steals")),
            "blocks": _coerce_int(r.get("blocks")),
            "turnovers": _coerce_int(r.get("turnovers")),
            "fouls": _coerce_int(r.get("foulsPersonal")),
            "plus_minus": _coerce_int(r.get("plusMinusPoints")),
            "fg_pct": round(_coerce_float(r.get("fieldGoalsPercentage")), 3),
            "three_pct": round(_coerce_float(r.get("threePointersPercentage")), 3),
        }
        _stats_cache[cache_key] = result
        return result
    except Exception as e:  # noqa: BLE001 — surface any nba_api failure to the LLM
        return {"error": f"Failed to fetch player stats: {type(e).__name__}: {e}"}


@tool
def analyze_momentum(state: Annotated[dict, InjectedState]) -> dict:
    """Summarize the last 5 scoring plays for momentum context.

    Use this when you want to know which team has been on a run, or to see
    the recent scoring sequence before deciding whether an event is notable.
    Takes no LLM-supplied args — the current game context is injected
    automatically.

    Returns:
        A dict with: summary, home_team, away_team, home_team_plays,
        away_team_plays, current_score, plays (list of last 5 with team,
        player, description, score after each).
    """
    context = state.get("game_context", {}) if isinstance(state, dict) else {}
    plays = list(context.get("last_scoring_plays", []))
    home_team = context.get("home_team", "HOME")
    away_team = context.get("away_team", "AWAY")
    score_home = context.get("score_home", 0)
    score_away = context.get("score_away", 0)

    if not plays:
        return {
            "summary": "No scoring plays in the window yet.",
            "home_team": home_team,
            "away_team": away_team,
            "home_team_plays": 0,
            "away_team_plays": 0,
            "current_score": f"{home_team} {score_home} - {score_away} {away_team}",
            "plays": [],
        }

    home_count = sum(1 for p in plays if p.get("team") == home_team)
    away_count = sum(1 for p in plays if p.get("team") == away_team)

    if home_count >= 4:
        verdict = f"{home_team} has dominated the last {len(plays)} scoring plays ({home_count} of {len(plays)})."
    elif away_count >= 4:
        verdict = f"{away_team} has dominated the last {len(plays)} scoring plays ({away_count} of {len(plays)})."
    elif abs(home_count - away_count) <= 1:
        verdict = f"Recent scoring has been even: {home_team} {home_count}, {away_team} {away_count}."
    else:
        leader = home_team if home_count > away_count else away_team
        verdict = f"{leader} has had the edge recently: {home_team} {home_count}, {away_team} {away_count}."

    return {
        "summary": verdict,
        "home_team": home_team,
        "away_team": away_team,
        "home_team_plays": home_count,
        "away_team_plays": away_count,
        "current_score": f"{home_team} {score_home} - {score_away} {away_team}",
        "plays": plays,
    }


# Tools registered with the LangGraph ToolNode in agent.py. Order matters only
# for documentation / introspection — the LLM sees them by name and description.
AGENT_TOOLS = [get_player_stats, analyze_momentum]
