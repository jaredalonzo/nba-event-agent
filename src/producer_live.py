"""Live Kafka producer for NBA play-by-play events.

The historical producer in ``src/producer.py`` fetches a completed game's PBP
once and replays it with an artificial delay to simulate a live stream. This
module is the real thing: it polls the NBA's live JSON CDN (via
``src/nba_live_client.py``), publishes new plays to Kafka as they appear, and
exits cleanly when the game ends.

Two modes for selecting which game to stream:

    Explicit: ``NBA_GAME_ID`` env var set to a game ID — must be in progress.
    Auto:     ``NBA_GAME_ID`` empty/unset — picks the first in-progress game
              from today's scoreboard; clear error if none are live.

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

load_dotenv(override=True)

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
POLL_SECONDS = float(os.environ.get("LIVE_POLL_SECONDS", "5.0"))
# Empty string and unset both mean "auto-discover from scoreboard".
EXPLICIT_GAME_ID = (os.environ.get("NBA_GAME_ID") or "").strip()


# --- Game resolution -------------------------------------------------------


def resolve_game() -> dict[str, Any]:
    """Pick a game to stream.

    If ``NBA_GAME_ID`` is set, fetch the scoreboard, look up that game, and
    verify it's actually in progress (status=2). If not set, return the first
    in-progress game on today's slate. Raises ``RuntimeError`` if no usable
    game can be found — callers should print the message and exit.
    """
    games = fetch_scoreboard()
    if not games:
        raise RuntimeError("scoreboard returned no games for today")

    if EXPLICIT_GAME_ID:
        for g in games:
            if g.get("gameId") == EXPLICIT_GAME_ID:
                status = g.get("gameStatus")
                if status == 2:
                    return g
                if status == 1:
                    raise RuntimeError(
                        f"game {EXPLICIT_GAME_ID} hasn't started yet "
                        f"(status={status}, {g.get('gameStatusText')})"
                    )
                if status == 3:
                    raise RuntimeError(
                        f"game {EXPLICIT_GAME_ID} is already final"
                    )
        raise RuntimeError(
            f"game {EXPLICIT_GAME_ID} not in today's scoreboard"
        )

    # Auto-discover
    live = find_live_game()
    if live is None:
        raise RuntimeError(
            "no in-progress games on today's slate; set NBA_GAME_ID to a "
            "specific in-progress game ID, or wait for tipoff"
        )
    return live


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


_running = True


def _handle_signal(signum, frame) -> None:
    """SIGINT/SIGTERM handler: ends the poll loop after the current cycle."""
    global _running
    _running = False


def stream_game(
    producer: Producer,
    game: dict[str, Any],
    *,
    poll_seconds: float = POLL_SECONDS,
) -> int:
    """Poll the PBP feed for ``game`` and publish new plays.

    Returns the total number of plays published. Exits when:
        - the scoreboard reports gameStatus=3 (final), or
        - a SIGINT/SIGTERM flips ``_running``.

    Network errors during a single poll are logged and retried on the next
    cycle — we don't crash the whole producer on a transient hiccup.
    """
    game_id = game["gameId"]
    home_id, away_id = extract_team_ids(game)
    home_tri = (game.get("homeTeam") or {}).get("teamTricode") or "HOME"
    away_tri = (game.get("awayTeam") or {}).get("teamTricode") or "AWAY"
    print(
        f"[producer] streaming {away_tri} @ {home_tri}  gameId={game_id}  "
        f"poll={poll_seconds:.1f}s  topic={TOPIC}",
        flush=True,
    )

    seen_action_numbers: set[int] = set()
    published = 0

    while _running:
        # 1) Check scoreboard for game-end. Cheap (single small JSON file).
        try:
            current_games = fetch_scoreboard()
        except LiveClientError as e:
            print(f"[producer] scoreboard fetch failed: {e}; retrying", flush=True)
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
            print(
                "[producer] game not started yet; waiting for tipoff",
                flush=True,
            )
            time.sleep(poll_seconds)
            continue
        except LiveClientError as e:
            print(f"[producer] pbp fetch failed: {e}; retrying", flush=True)
            time.sleep(poll_seconds)
            continue

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
            seen_action_numbers.add(action["actionNumber"])
            published += 1
        if new_actions:
            producer.poll(0)  # service delivery callbacks
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

        time.sleep(poll_seconds)

    producer.flush(timeout=10)
    return published


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        game = resolve_game()
    except RuntimeError as e:
        print(f"[producer] {e}", flush=True)
        raise SystemExit(1)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        total = stream_game(producer, game)
    finally:
        producer.flush(timeout=10)
    print(f"[producer] done. {total} plays published to {TOPIC}.", flush=True)


if __name__ == "__main__":
    main()
