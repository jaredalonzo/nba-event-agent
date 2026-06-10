"""Live Kafka producer for NBA play-by-play events.

The historical producer in ``src/producer.py`` fetches a completed game's PBP
once and replays it with an artificial delay to simulate a live stream. This
module is the real thing: it polls the NBA's live JSON CDN (via
``src/nba_live_client.py``), publishes new plays to Kafka as they appear, and
exits cleanly when the game ends.

Three modes for selecting which game to stream, in priority order:

    Explicit ID:  ``NBA_GAME_ID`` set to a game ID — in-progress or pre-tipoff;
                  waits for tipoff if status=1, raises if already final.
    By team:      ``NBA_TEAM`` set to a tricode (e.g. ``NYK``) — finds today's
                  game for that team; waits for tipoff if scheduled, raises if
                  final.
    Auto:         neither set — prefers the first in-progress game; falls back
                  to the next scheduled game and waits for tipoff; raises only
                  if today's slate has no in-progress or scheduled games.

Shape adapter: the live endpoint returns slightly different fields than the
historical ``PlayByPlayV3``. Notably it lacks ``location`` ("h"/"v"). We inject
that here so the agent's ``GameContextTracker`` works unchanged.

Run with::

    python -m src.producer_live
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv

from src.nba_live_client import (
    LiveClientError,
    LiveGameNotStarted,
    extract_actions,
    extract_team_ids,
    fetch_playbyplay,
    fetch_scoreboard,
    find_live_game,
)

# shell env wins over .env — lets inline overrides take effect
load_dotenv()

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
POLL_SECONDS = float(os.environ.get("LIVE_POLL_SECONDS", "5.0"))
# Empty string and unset both mean "fall through to the next mode".
EXPLICIT_GAME_ID = (os.environ.get("NBA_GAME_ID") or "").strip()
EXPLICIT_TEAM = (os.environ.get("NBA_TEAM") or "").strip().upper()


# --- Game resolution -------------------------------------------------------


def _team_matches(game: dict[str, Any], tricode: str) -> bool:
    """True if ``tricode`` is either side of ``game``. Case-insensitive."""
    home = ((game.get("homeTeam") or {}).get("teamTricode") or "").upper()
    away = ((game.get("awayTeam") or {}).get("teamTricode") or "").upper()
    return tricode == home or tricode == away


def _require_not_final(game: dict[str, Any], label: str) -> dict[str, Any]:
    """Return ``game`` if status=1 or 2; raise if final or unknown.

    Status=1 (scheduled) is allowed through — stream_game's poll loop already
    handles pre-tipoff by catching LiveGameNotStarted and sleeping until the
    PBP file appears on the CDN.
    """
    status = game.get("gameStatus")
    if status == 2:
        return game
    if status == 1:
        home = (game.get("homeTeam") or {}).get("teamTricode", "?")
        away = (game.get("awayTeam") or {}).get("teamTricode", "?")
        print(
            f"[producer] {label} hasn't started yet "
            f"({away} @ {home}, {game.get('gameStatusText')}); will wait for tipoff",
            flush=True,
        )
        return game
    if status == 3:
        raise RuntimeError(f"{label} is already final")
    raise RuntimeError(f"{label} has unexpected status: {status}")


def resolve_game() -> dict[str, Any]:
    """Pick a game to stream.

    Priority:
      1. ``NBA_GAME_ID`` — look up that specific game; waits for tipoff if
         status=1, raises if already final.
      2. ``NBA_TEAM``   — find today's game for that team tricode; waits for
         tipoff if scheduled, raises if final.
      3. Auto-discover — first in-progress game, then first scheduled game;
         raises only if today's slate has neither.

    Raises ``RuntimeError`` if no usable game can be found — callers should
    print the message and exit.
    """
    games = fetch_scoreboard()
    if not games:
        raise RuntimeError("scoreboard returned no games for today")

    if EXPLICIT_GAME_ID:
        for g in games:
            if g.get("gameId") == EXPLICIT_GAME_ID:
                return _require_not_final(g, f"game {EXPLICIT_GAME_ID}")
        raise RuntimeError(
            f"game {EXPLICIT_GAME_ID} not in today's scoreboard"
        )

    if EXPLICIT_TEAM:
        matches = [g for g in games if _team_matches(g, EXPLICIT_TEAM)]
        if not matches:
            raise RuntimeError(
                f"no game for team {EXPLICIT_TEAM} on today's slate"
            )
        # Same team could conceivably appear twice in a doubleheader scenario.
        # Prefer an in-progress game; else fall back to the first match so the
        # caller gets the status-aware error from _require_not_final.
        in_progress = next((g for g in matches if g.get("gameStatus") == 2), None)
        if in_progress is not None:
            return in_progress
        return _require_not_final(matches[0], f"team {EXPLICIT_TEAM}'s game")

    # Auto-discover: prefer in-progress, fall back to next scheduled game.
    live = find_live_game()
    if live is not None:
        return live
    scheduled = next(
        (g for g in games if g.get("gameStatus") == 1), None
    )
    if scheduled is not None:
        print("[producer] no live games on today's slate", flush=True)
        return _require_not_final(scheduled, "next scheduled game")
    raise RuntimeError(
        "no in-progress or scheduled games on today's slate"
    )


# --- Adapter ---------------------------------------------------------------


def adapt_action(
    action: dict[str, Any],
    *,
    game_id: str,
    home_team_id: int | None,
    away_team_id: int | None,
) -> dict[str, Any]:
    """Add the fields the agent expects but the live feed doesn't ship.

    The agent's GameContextTracker keys home/away off ``location`` ("h"/"v"),
    which the live endpoint doesn't provide. We derive it from teamId.
    Also injects ``gameId`` (which lives on the parent in the live response).
    """
    out = dict(action)
    out["gameId"] = game_id
    team_id = action.get("teamId")
    if team_id is not None and home_team_id is not None and team_id == home_team_id:
        out["location"] = "h"
    elif team_id is not None and away_team_id is not None and team_id == away_team_id:
        out["location"] = "v"
    else:
        out["location"] = ""
    return out


# --- Producer --------------------------------------------------------------


def _delivery_report(err, msg) -> None:
    """confluent_kafka Producer.produce callback. Logs delivery failures.

    Successful deliveries are quiet — we don't want to drown the live log in
    "delivered" lines (one per play, ~500 per game).
    """
    if err is not None:
        print(f"[producer] delivery failed for offset {msg.offset()}: {err}")


def stream_game(
    producer: Producer,
    game: dict[str, Any],
    *,
    poll_seconds: float = POLL_SECONDS,
    stop: threading.Event | None = None,
) -> int:
    """Poll the PBP feed for ``game`` and publish new plays.

    Returns the total number of plays published. Exits when:
        - the scoreboard reports gameStatus=3 (final), or
        - the caller sets the ``stop`` event (e.g. from a signal handler).

    Network errors during a single poll are logged and retried on the next
    cycle — we don't crash the whole producer on a transient hiccup.
    """
    if stop is None:
        stop = threading.Event()
    game_id = game["gameId"]
    home_id, away_id = extract_team_ids(game)
    home_tri = (game.get("homeTeam") or {}).get("teamTricode") or "HOME"
    away_tri = (game.get("awayTeam") or {}).get("teamTricode") or "AWAY"
    print(
        f"[producer] streaming {away_tri} @ {home_tri}  gameId={game_id}  "
        f"poll={poll_seconds:.1f}s  topic={TOPIC}",
        flush=True,
    )

    # Per-game state — bounded by ~500 actions for a single game. Resets to
    # empty when stream_game is called again. Do not reuse this set across games.
    seen_action_numbers: set[int] = set()
    published = 0

    # Retry/backoff state. consecutive_failures counts cycles in which any
    # LiveClientError fired (scoreboard or pbp — one cycle = one count, not
    # two). LiveGameNotStarted does NOT count as a failure (expected pre-tip).
    consecutive_failures = 0
    max_backoff = 60.0
    failure_budget = 60

    while not stop.is_set():
        cycle_failed = False

        # 1) Check scoreboard for game-end. Cheap (single small JSON file).
        try:
            current_games = fetch_scoreboard()
        except LiveClientError as e:
            cycle_failed = True
            print(
                f"[producer] scoreboard fetch failed "
                f"({consecutive_failures + 1} consecutive): {e}",
                flush=True,
            )
            current_games = []

        current_status = None
        for g in current_games:
            if g.get("gameId") == game_id:
                current_status = g.get("gameStatus")
                break

        # 2) Fetch latest PBP and publish only the new actions.
        try:
            pbp = fetch_playbyplay(game_id)
        except LiveGameNotStarted:
            # Pre-tipoff — neither a success nor a failure for the counter.
            print(
                "[producer] game not started yet; waiting for tipoff",
                flush=True,
            )
            if current_status == 3:
                print("[producer] game final; exiting", flush=True)
                break
            time.sleep(poll_seconds)
            continue
        except LiveClientError as e:
            cycle_failed = True
            consecutive_failures += 1
            print(
                f"[producer] pbp fetch failed "
                f"({consecutive_failures} consecutive): {e}",
                flush=True,
            )
            if consecutive_failures >= failure_budget:
                raise RuntimeError(
                    f"exceeded failure budget ({failure_budget} consecutive pbp failures)"
                )
            # Exponential backoff: poll_seconds * 2^(n-1), capped at max_backoff.
            backoff = min(poll_seconds * (2 ** (consecutive_failures - 1)), max_backoff)
            time.sleep(backoff)
            continue

        # pbp succeeded. If scoreboard also succeeded this cycle, reset the
        # counter. If only scoreboard failed, account for that one failure
        # (so the budget can still fire on pure-scoreboard outages).
        if cycle_failed:
            consecutive_failures += 1
            if consecutive_failures >= failure_budget:
                raise RuntimeError(
                    f"exceeded failure budget ({failure_budget} consecutive scoreboard failures)"
                )
        else:
            consecutive_failures = 0

        _, actions = extract_actions(pbp)
        new_actions = [
            a for a in actions if a.get("actionNumber") not in seen_action_numbers
        ]
        for action in new_actions:
            adapted = adapt_action(
                action,
                game_id=game_id,
                home_team_id=home_id,
                away_team_id=away_id,
            )
            payload = json.dumps(adapted, default=str).encode("utf-8")
            producer.produce(TOPIC, payload, callback=_delivery_report)
            producer.poll(0)  # service delivery callbacks promptly
            seen_action_numbers.add(action["actionNumber"])
            published += 1
        if new_actions:
            print(
                f"[producer] +{len(new_actions)} new plays "
                f"(total {published})  "
                f"last: #{new_actions[-1].get('actionNumber')} "
                f"{(new_actions[-1].get('description') or '')[:60]}",
                flush=True,
            )

        # 3) Check for game end.
        if current_status == 3:
            print("[producer] game final; exiting", flush=True)
            break

        if cycle_failed:
            # Scoreboard failed but pbp succeeded — apply backoff anyway
            # since we're in a degraded state.
            backoff = min(poll_seconds * (2 ** (consecutive_failures - 1)), max_backoff)
            time.sleep(backoff)
        else:
            time.sleep(poll_seconds)

    return published


def main() -> None:
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    try:
        game = resolve_game()
    except RuntimeError as e:
        print(f"[producer] {e}", flush=True)
        raise SystemExit(1)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        total = stream_game(producer, game, stop=stop)
    except RuntimeError as e:
        print(f"[producer] {e}", flush=True)
        raise SystemExit(1)
    finally:
        producer.flush(timeout=10)
    print(f"[producer] done. {total} plays published to {TOPIC}.", flush=True)


if __name__ == "__main__":
    main()
