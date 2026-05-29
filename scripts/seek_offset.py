"""Inspect Kafka topic state and seed a starting offset for the agent.

The agent's default config gives every run a fresh consumer group, so it
replays from offset 0. That's the right behavior for deterministic demos and
cost benchmarking, but the wrong one when the agent got kicked out of the
group mid-stream during a live game and you just want to catch up.

This script handles two jobs:

    inspect: print the topic's offset range, optionally any committed offsets
             for a named group, plus the last actionNumber found in
             data/insights.jsonl (so you can pick a sensible target).

    seed:    commit a starting offset for a named consumer group. The agent,
             when started with KAFKA_RESUME=true and the same KAFKA_GROUP_ID,
             will pick up from that committed offset.

Examples
--------
    # see what's in the topic right now
    .venv/bin/python -m scripts.seek_offset inspect

    # jump to the tail — skip everything written so far
    .venv/bin/python -m scripts.seek_offset seed --group nba-agent-resume --latest

    # explicit offset (e.g. found via inspect)
    .venv/bin/python -m scripts.seek_offset seed --group nba-agent-resume --offset 237

    # everything written in the last 5 minutes (uses Kafka's offsets_for_times)
    .venv/bin/python -m scripts.seek_offset seed --group nba-agent-resume --minutes-ago 5

Then start the agent:

    KAFKA_RESUME=true KAFKA_GROUP_ID=nba-agent-resume .venv/bin/python -m src.agent
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv(override=True)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "nba.plays")
PARTITION = 0  # single-partition topic per docker-compose.yml
INSIGHTS_PATH = Path("data/insights.jsonl")


# --- helpers ---------------------------------------------------------------


def _watermarks(consumer: Consumer) -> tuple[int, int]:
    """(earliest, next-to-be-written) offsets for the topic partition."""
    tp = TopicPartition(TOPIC, PARTITION)
    low, high = consumer.get_watermark_offsets(tp, timeout=10)
    return low, high


def _last_action_from_insights() -> int | None:
    """Pull the actionNumber off the last line of data/insights.jsonl, or
    None if the file is missing/empty/unparseable. Best-effort — purely
    informational to help the operator pick a target offset."""
    if not INSIGHTS_PATH.exists():
        return None
    try:
        with INSIGHTS_PATH.open() as f:
            lines = [line for line in f if line.strip()]
        if not lines:
            return None
        record = json.loads(lines[-1])
        # The insight record shape varies — try a couple of likely paths.
        return (
            record.get("event", {}).get("actionNumber")
            or record.get("actionNumber")
        )
    except (json.JSONDecodeError, OSError):
        return None


def _committed_for_group(group: str) -> int | None:
    """Current committed offset for ``group`` on this topic partition, or
    None if the group has no committed offset (i.e., it's brand new)."""
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
    })
    try:
        result = c.committed([TopicPartition(TOPIC, PARTITION)], timeout=10)
        offset = result[0].offset
        # confluent_kafka returns -1001 (OFFSET_INVALID) for "no committed offset"
        return offset if offset >= 0 else None
    finally:
        c.close()


# --- commands --------------------------------------------------------------


def cmd_inspect(args: argparse.Namespace) -> int:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "seek-offset-inspect",
    })
    try:
        low, high = _watermarks(c)
    finally:
        c.close()

    print(f"topic:       {TOPIC} (partition {PARTITION})")
    print(f"broker:      {BOOTSTRAP}")
    print(f"earliest:    {low}")
    print(f"high water:  {high}  (next offset to be written)")
    print(f"available:   {high - low} events")

    last_action = _last_action_from_insights()
    if last_action is not None:
        print(
            f"\nlast actionNumber in {INSIGHTS_PATH}: {last_action}"
            f"  (≈ Kafka offset {last_action - 1} for a single-game stream)"
        )
    else:
        print(f"\n{INSIGHTS_PATH}: no usable entries")

    if args.group:
        committed = _committed_for_group(args.group)
        if committed is None:
            print(f"\ngroup {args.group!r}: no committed offset (fresh)")
        else:
            print(f"\ngroup {args.group!r}: committed offset = {committed}")

    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "enable.auto.commit": False,
    })
    try:
        low, high = _watermarks(c)

        # Resolve target offset from whichever flag was passed. The argparse
        # mutually-exclusive group guarantees exactly one is set.
        if args.latest:
            target = high
            chose = "--latest"
        elif args.offset is not None:
            target = args.offset
            chose = f"--offset {target}"
        elif args.minutes_ago is not None:
            ms = int((time.time() - args.minutes_ago * 60) * 1000)
            tp = TopicPartition(TOPIC, PARTITION, ms)
            result = c.offsets_for_times([tp], timeout=10)
            target = result[0].offset
            chose = f"--minutes-ago {args.minutes_ago}"
            if target < 0:
                # No message at or after that time — fall back to the tail
                # so the agent starts cleanly with new plays.
                print(
                    f"no events at or after {args.minutes_ago}m ago; "
                    f"falling back to high water ({high})",
                    file=sys.stderr,
                )
                target = high
        else:  # pragma: no cover - argparse enforces this
            print("internal error: no target chosen", file=sys.stderr)
            return 2

        # Sanity bounds. low and high define the legal range; the commit
        # itself would accept anything but a too-low offset becomes "earliest
        # available" silently and a too-high one stalls the consumer.
        if target < low or target > high:
            print(
                f"target offset {target} outside available range "
                f"[{low}, {high}]; refusing to commit",
                file=sys.stderr,
            )
            return 1

        tp = TopicPartition(TOPIC, PARTITION, target)
        if args.dry_run:
            print(
                f"DRY RUN: would commit offset {target} for group "
                f"{args.group!r} (chosen via {chose})"
            )
            return 0

        c.commit(offsets=[tp], asynchronous=False)
        print(
            f"seeded group {args.group!r} at offset {target} "
            f"(chosen via {chose})"
        )
        print(
            f"\nnext step:\n"
            f"  KAFKA_RESUME=true KAFKA_GROUP_ID={args.group} "
            f".venv/bin/python -m src.agent"
        )
    finally:
        c.close()
    return 0


# --- CLI -------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="seek_offset",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p_inspect = sub.add_parser("inspect", help="print topic + group state")
    p_inspect.add_argument(
        "--group",
        help="optional: also report the committed offset for this group",
    )
    p_inspect.set_defaults(func=cmd_inspect)

    p_seed = sub.add_parser(
        "seed",
        help="commit a starting offset for the named consumer group",
    )
    p_seed.add_argument(
        "--group", required=True, help="consumer group ID to commit against"
    )
    # Mutually exclusive target selection. Exactly one must be set.
    target = p_seed.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--latest",
        action="store_true",
        help="commit at the topic's high watermark (skip everything)",
    )
    target.add_argument(
        "--offset",
        type=int,
        help="commit at this explicit offset",
    )
    target.add_argument(
        "--minutes-ago",
        type=float,
        help="commit at the first offset written within the last N minutes",
    )
    p_seed.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be committed without committing",
    )
    p_seed.set_defaults(func=cmd_seed)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
