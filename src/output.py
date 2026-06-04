"""Insight persistence helpers.

Two responsibilities:
    1. Format an insight for the terminal (so a watcher can follow along live).
    2. Append a structured record to ``data/insights.jsonl`` for replay / review.

The JSONL file is the canonical artifact of a demo run. Each line is a single
JSON object so we can ``tail -f`` or load with ``pandas.read_json(lines=True)``.

The output directory is computed relative to the repo root rather than the
current working directory, so the agent writes to the same place regardless of
where it's launched from.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

# Resolve to <repo>/data/insights.jsonl. src/output.py lives one level below
# the repo root, so .parent.parent gets us back up.
_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INSIGHTS_PATH = _REPO_ROOT / "data" / "insights.jsonl"

# Stdout banner per severity. Keeping it ASCII for terminal compatibility.
_SEVERITY_BADGE = {
    "critical": "[!! CRITICAL]",
    "notable": "[** NOTABLE ]",
    "routine": "[   routine ]",
}


def _normalize_severity(severity: str | None) -> str:
    """Coerce free-form severity to one of the three known buckets."""
    if not severity:
        return "notable"
    s = severity.strip().lower()
    if s in _SEVERITY_BADGE:
        return s
    return "notable"


def log_insight(
    insight: str,
    severity: str,
    event: dict[str, Any],
    *,
    path: Path | str | None = None,
) -> dict[str, Any]:
    """Print the insight and append a JSON record to ``data/insights.jsonl``.

    Args:
        insight: The generated narrative (2–3 sentences).
        severity: One of ``routine`` / ``notable`` / ``critical`` — anything
            else is bucketed as ``notable``.
        event: The originating play-by-play event. We keep only a handful of
            useful fields in the persisted record (full event would bloat).
        path: Optional override for the output file. Mainly for tests.

    Returns:
        The record that was written (useful for tests / introspection).
    """
    sev = _normalize_severity(severity)
    target = Path(path) if path is not None else DEFAULT_INSIGHTS_PATH
    target.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "severity": sev,
        "insight": insight,
        # A compact projection of the event — enough to reconstruct context
        # without dragging the entire PlayByPlayV3 row along.
        "event": {
            "gameId": event.get("gameId"),
            "actionNumber": event.get("actionNumber"),
            "period": event.get("period"),
            "clock": event.get("clock"),
            "description": event.get("description"),
            "playerName": event.get("playerName"),
            "scoreHome": event.get("scoreHome"),
            "scoreAway": event.get("scoreAway"),
        },
    }

    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    badge = _SEVERITY_BADGE[sev]
    desc = event.get("description") or "(no description)"
    print(f"\n{badge}  {desc}\n            {insight}\n", flush=True)

    from src.bluesky_poster import post_insight
    post_insight(insight, sev, event)

    return record
