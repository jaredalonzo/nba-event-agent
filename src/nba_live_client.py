"""HTTP client for NBA's live JSON endpoints.

Why this exists: ``nba_api.live.nba.endpoints`` sends the same headers as the
historical ``stats.nba.com`` endpoints, and ``cdn.nba.com`` blocks those — every
call returns HTTP 403, which the library silently turns into a JSONDecodeError.

We bypass the library and hit the two URLs directly with browser-like headers:

    Scoreboard:    https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
    Play-by-play:  https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_<gameId>.json

Both endpoints are polling-only — there is no push / WebSocket option. The
caller is responsible for cadence (typically 5–10s between polls).

Game-status codes used by the scoreboard:
    1 = scheduled (not yet started)
    2 = in progress (live)
    3 = final

The play-by-play endpoint returns HTTP 403 for games that have not yet started
(status=1). We surface that as ``LiveGameNotStarted`` so callers can wait or
choose a different game.
"""

from __future__ import annotations

from typing import Any

import requests

SCOREBOARD_URL = (
    "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
)
PLAYBYPLAY_URL_TEMPLATE = (
    "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
)

# Browser-like headers. The CDN's WAF rejects the default `requests` UA and the
# `nba_api`-shipped headers. These three (UA + Origin + Referer) are the
# minimum that consistently get through.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}

DEFAULT_TIMEOUT = 10.0


class LiveClientError(RuntimeError):
    """Base for client-level failures (network, 4xx/5xx, bad JSON)."""


class LiveGameNotStarted(LiveClientError):
    """Raised when play-by-play is requested for a game that hasn't tipped off.

    The CDN returns 403 in this case (no PBP file exists yet). Callers can
    catch this specifically to wait for tipoff rather than crash.
    """


def _get_json(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> dict[str, Any]:
    """GET ``url`` and decode JSON, raising LiveClientError on any failure."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
    except requests.RequestException as e:
        raise LiveClientError(f"network error fetching {url}: {e}") from e
    if resp.status_code != 200:
        raise LiveClientError(
            f"HTTP {resp.status_code} from {url}: {resp.text[:200]!r}"
        )
    try:
        return resp.json()
    except ValueError as e:
        raise LiveClientError(f"non-JSON response from {url}: {e}") from e


def fetch_scoreboard(*, timeout: float = DEFAULT_TIMEOUT) -> list[dict[str, Any]]:
    """Return today's games from the live scoreboard.

    Each element has at minimum: ``gameId``, ``gameStatus`` (1/2/3),
    ``gameStatusText`` (e.g. "8:00 pm ET" or "Q3 5:14"), ``homeTeam``,
    ``awayTeam``. Returns ``[]`` if no games are scheduled today.
    """
    data = _get_json(SCOREBOARD_URL, timeout=timeout)
    return list(data.get("scoreboard", {}).get("games", []))


def find_live_game(*, timeout: float = DEFAULT_TIMEOUT) -> dict[str, Any] | None:
    """Return the first in-progress (gameStatus=2) game today, else None.

    Convenience helper for the producer's auto-discover mode.
    """
    for game in fetch_scoreboard(timeout=timeout):
        if game.get("gameStatus") == 2:
            return game
    return None


def fetch_playbyplay(
    game_id: str, *, timeout: float = DEFAULT_TIMEOUT
) -> dict[str, Any]:
    """Return the full PBP feed for ``game_id``.

    Top-level shape::

        {
          "meta":  {...},
          "game":  {
            "gameId": "...",
            "actions": [ {actionNumber, clock, period, ...}, ... ],
          }
        }

    Raises ``LiveGameNotStarted`` if the game exists in the scoreboard but
    hasn't yet tipped off (the CDN returns 403 for unstarted games). Raises
    ``LiveClientError`` for any other failure.
    """
    url = PLAYBYPLAY_URL_TEMPLATE.format(game_id=game_id)
    try:
        return _get_json(url, timeout=timeout)
    except LiveClientError as e:
        # The CDN returns 403 specifically for not-yet-started games. Surface
        # that as a distinct exception so callers can handle it specially
        # (typically: wait and retry).
        if "HTTP 403" in str(e):
            raise LiveGameNotStarted(
                f"game {game_id} has not started yet (no PBP file on CDN)"
            ) from e
        raise


def extract_actions(pbp_response: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Pull the gameId + actions list out of a fetch_playbyplay response.

    Returns ``(gameId, actions)``. Both default to sane empties if missing.
    """
    game = pbp_response.get("game", {}) or {}
    return game.get("gameId", ""), list(game.get("actions", []))


def extract_team_ids(
    scoreboard_game: dict[str, Any],
) -> tuple[int | None, int | None]:
    """Pull (home_team_id, away_team_id) out of a scoreboard game record.

    Used by the producer to attach a ``location`` field ("h"/"v") to each play
    so the downstream agent doesn't need to know about home/away in a new way.
    """
    home = (scoreboard_game.get("homeTeam") or {}).get("teamId")
    away = (scoreboard_game.get("awayTeam") or {}).get("teamId")
    return home, away
