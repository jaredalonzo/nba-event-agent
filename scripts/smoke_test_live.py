"""Smoke test: exercise the full live-producer data path without LLM costs.

Pulls play-by-play from a known completed game (Knicks Game 1, 0042500301),
runs every event through our adapt_action() function, and publishes 20 plays
to Kafka. Validates: live client (with browser headers), field adapter
(gameId + location injection), and Kafka publishing.

The agent is NOT started, so no Anthropic API calls. Run separately and
inspect the topic to confirm the bytes look right:

    python -m scripts.smoke_test_live
    docker exec nba-kafka kafka-console-consumer \\
        --bootstrap-server localhost:9092 --topic nba.plays \\
        --from-beginning --max-messages 20
"""

from __future__ import annotations

import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv

from src.nba_live_client import extract_actions, fetch_playbyplay
from src.producer_live import adapt_action

load_dotenv(override=True)


def main() -> None:
    # 1) Fetch a completed game's PBP via our live client. Proves the
    #    requests-based client with browser headers works against the real CDN.
    print("[smoke] fetching live PBP for 0042500301 ...")
    pbp = fetch_playbyplay("0042500301")
    game_id, actions = extract_actions(pbp)
    print(f"[smoke] got {len(actions)} actions for gameId={game_id}")

    # 2) Derive home/away team IDs from the actions themselves (we're skipping
    #    the scoreboard lookup since 0042500301 was yesterday).
    team_ids: list[int] = []
    for a in actions:
        tid = a.get("teamId")
        if tid and tid not in team_ids:
            team_ids.append(tid)
        if len(team_ids) >= 2:
            break
    home_id, away_id = team_ids[0], team_ids[1]
    print(f"[smoke] inferred teamIds: home={home_id} away={away_id}")

    # 3) Publish 20 sample plays through adapt_action + Kafka. Mix early /
    #    mid / late so we exercise different game states.
    sample_indices = (
        list(range(5))
        + [200, 250, 300, 400, 500]
        + list(range(len(actions) - 5, len(actions)))
    )
    sample = [actions[i] for i in sample_indices]

    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    delivered = {"ok": 0, "err": 0}

    def _on_delivery(err, msg):
        if err:
            delivered["err"] += 1
        else:
            delivered["ok"] += 1

    print(f"[smoke] publishing {len(sample)} plays to {os.environ['KAFKA_TOPIC']} ...")
    for a in sample:
        adapted = adapt_action(
            a, game_id=game_id, home_team_id=home_id, away_team_id=away_id
        )
        producer.produce(
            os.environ["KAFKA_TOPIC"],
            json.dumps(adapted, default=str).encode("utf-8"),
            callback=_on_delivery,
        )
    producer.flush(timeout=10)
    print(f"[smoke] delivered ok={delivered['ok']} err={delivered['err']}")

    # 4) Print a sample so we can see what the adapter produced.
    print("\n[smoke] sample adapted play (mid-game, idx=200):")
    sample_adapted = adapt_action(
        actions[200], game_id=game_id, home_team_id=home_id, away_team_id=away_id
    )
    print(json.dumps(sample_adapted, indent=2, default=str))


if __name__ == "__main__":
    main()
