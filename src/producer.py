"""Kafka producer that streams NBA play-by-play events from nba_api.

Fetches historical play-by-play for the configured game (default: 2016 Finals
Game 7) and publishes each play as a JSON message to the configured Kafka
topic, with a configurable delay between events to simulate a live stream.

Run after `docker-compose up -d`:
    python src/producer.py
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv
from nba_api.stats.endpoints import playbyplayv3

load_dotenv(override=True)

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
GAME_ID = os.environ["NBA_GAME_ID"]
DELAY_SECONDS = float(os.environ.get("PRODUCER_DELAY_SECONDS", "0.5"))


def delivery_report(err, msg) -> None:
    """Callback invoked once per produced message. Logs failures only."""
    if err is not None:
        print(f"[producer] delivery failed for key={msg.key()}: {err}", flush=True)


def fetch_plays(game_id: str) -> list[dict]:
    """Pull the full play-by-play frame for ``game_id`` from nba_api.

    Uses PlayByPlayV3 — the older PlayByPlay and PlayByPlayV2 endpoints are
    deprecated and stats.nba.com no longer returns data for them
    (see https://github.com/swar/nba_api/issues/591).
    """
    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
    df = pbp.get_data_frames()[0]  # df[1] is just video-availability metadata
    # Pandas NaN doesn't round-trip through json.dumps (emits non-standard `NaN`
    # literal). Convert to object dtype first so None sticks even in numeric
    # columns — in a plain float column, None gets silently re-cast to NaN.
    df = df.astype(object).where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def main() -> None:
    plays = fetch_plays(GAME_ID)
    print(
        f"[producer] fetched {len(plays)} plays for game {GAME_ID}; "
        f"publishing to {TOPIC} at {BOOTSTRAP_SERVERS} (delay={DELAY_SECONDS}s)",
        flush=True,
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for i, play in enumerate(plays):
        play["simulated_timestamp"] = datetime.now(timezone.utc).isoformat()
        # Key by actionNumber so events for the same game land on the same partition
        # (single-partition setup here, but keeps semantics correct if we scale).
        key = str(play.get("actionNumber", i))
        producer.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(play, default=str).encode("utf-8"),
            callback=delivery_report,
        )
        # Serve delivery callbacks so we don't queue up unbounded outstanding messages.
        producer.poll(0)

        if (i + 1) % 50 == 0:
            print(f"[producer] published {i + 1}/{len(plays)}", flush=True)

        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"[producer] done. {len(plays)} plays published to {TOPIC}.", flush=True)


if __name__ == "__main__":
    main()
