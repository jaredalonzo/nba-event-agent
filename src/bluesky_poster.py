"""Post critical NBA insights to Bluesky.

Credentials are optional — if BLUESKY_HANDLE or BLUESKY_APP_PASSWORD are
unset, all calls to post_insight are silent no-ops. Failures during login
or posting are logged to stdout but never raised, so the agent continues
uninterrupted regardless of Bluesky availability.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any

_client = None   # None = not yet attempted
_FAILED = object()  # sentinel: login was attempted and permanently failed
_last_post_time: float = 0.0  # monotonic timestamp of the last successful post

_CLOCK_RE = re.compile(r"PT(\d+)M(\d+(?:\.\d+)?)S")
_TAG = " #NBA #NBAsky #NBAfinals"
_LIMIT = 300
_MIN_POST_INTERVAL = 30.0  # seconds between posts; Bluesky allows ~1,666/hour


def _parse_clock(clock: str) -> str:
    """Convert ISO 8601 duration 'PT02M34.00S' to '2:34'."""
    m = _CLOCK_RE.match(clock)
    if not m:
        return clock
    minutes = int(m.group(1))
    seconds = int(float(m.group(2)))
    return f"{minutes}:{seconds:02d}"


def _format_post(insight: str, event: dict[str, Any]) -> str:
    """Build a Bluesky post string capped at _LIMIT characters."""
    period = event.get("period")
    clock_raw = event.get("clock", "")
    score_home = event.get("scoreHome", "")
    score_away = event.get("scoreAway", "")

    header_parts: list[str] = ["🏀"]
    if period and clock_raw:
        header_parts.append(f" Q{period} {_parse_clock(clock_raw)}")
    if score_home and score_away:
        header_parts.append(f" | {score_home}-{score_away}")
    header = "".join(header_parts) + "\n"

    max_body = _LIMIT - len(header) - len(_TAG)
    body = insight if len(insight) <= max_body else _truncate(insight, max_body)
    return header + body + _TAG


def _truncate(text: str, max_len: int) -> str:
    """Shorten text to max_len graphemes, preferring clean sentence breaks."""
    if len(text) <= max_len:
        return text
    # Try to end on a sentence boundary (keep the punctuation, drop trailing space)
    for punct in (". ", "! ", "? "):
        idx = text.rfind(punct, 0, max_len)
        if idx != -1:
            return text[: idx + 1]
    # Fall back to the last word boundary
    idx = text.rfind(" ", 0, max_len - 1)
    if idx != -1:
        return text[:idx] + "…"
    # Hard truncate as last resort
    return text[: max_len - 1] + "…"


def _get_client():
    """Return a logged-in atproto Client, or None if credentials are absent/failed."""
    global _client
    if _client is _FAILED:
        return None
    if _client is not None:
        return _client

    handle = os.getenv("BLUESKY_HANDLE", "").strip()
    password = os.getenv("BLUESKY_APP_PASSWORD", "").strip()
    if not handle or not password:
        return None

    try:
        from atproto import Client  # local import keeps atproto optional at import time

        c = Client()
        c.login(handle, password)
        _client = c
        return _client
    except Exception as exc:  # noqa: BLE001
        _client = _FAILED  # stop retrying for the lifetime of this process
        print(f"[bluesky] login failed: {exc}", flush=True)
        return None


def post_insight(insight: str, severity: str, event: dict[str, Any]) -> None:
    """Post a critical insight to Bluesky.

    No-ops silently when severity is not 'critical', credentials are absent,
    or the post call fails.
    """
    global _last_post_time

    if severity not in {"critical", "notable"}:
        return

    client = _get_client()
    if client is None:
        return

    elapsed = time.monotonic() - _last_post_time
    if elapsed < _MIN_POST_INTERVAL:
        print(
            f"[bluesky] rate limit: skipping post ({elapsed:.1f}s since last post, "
            f"min={_MIN_POST_INTERVAL}s)",
            flush=True,
        )
        return

    try:
        text = _format_post(insight, event)
        client.send_post(text=text)
        _last_post_time = time.monotonic()
    except Exception as exc:  # noqa: BLE001
        print(f"[bluesky] post failed: {exc}", flush=True)
