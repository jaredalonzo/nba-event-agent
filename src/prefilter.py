"""Deterministic pre-filter for routine NBA events.

Goal: skip the LLM entirely for events whose outcome is obvious from
field values alone. Saves classifier calls (~40-50% of events for a
typical game) without touching insight quality — the classifier itself
would have routed all of these to SKIP_* anyway.

Design principle: conservative. Every rule should be a near-zero-risk
skip; if a rule would silently drop even one narratable play per game,
it doesn't belong here. The classifier remains the last word on
anything ambiguous.

Returns an Action enum value (so the calling code can record the skip
reason consistently with classifier-driven skips), or None to mean
"let the classifier decide."
"""

from __future__ import annotations

from typing import Any

from src.state import Action


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

# Substitutions are always routine — no game-state implication, no
# narrative content. The classifier's system prompt already lists them
# as a skip case; we just save the round-trip.
_SUBSTITUTION = "substitution"

# Period start/end and game start/end markers are housekeeping, not
# plays. They typically have empty descriptions and no scoring/foul
# changes. PlayByPlayV3's actionType for these is "period" (lowercase)
# and "game"; we match case-insensitively.
_MARKER_ACTION_TYPES = {"period", "game"}

# Timeouts: always routine in Q1/Q2 (no game-state pressure yet).
# Q3 onward, timeouts can be strategic and we leave them to the
# classifier — e.g. a Q4 timeout after a momentum swing is narratable.
_TIMEOUT = "timeout"

# Free throw blowout threshold. In any score margin wider than this in
# Q1-Q3, a free throw is mechanically routine — the lead won't change
# meaningfully and no one's about to chase a comeback. In Q4 we always
# defer to the classifier regardless of margin (foul-trouble logic,
# intentional fouling, etc.).
_FREE_THROW = "free throw"
_BLOWOUT_MARGIN_POINTS = 15


def should_skip(event: dict[str, Any], snapshot: dict[str, Any]) -> Action | None:
    """Return an Action.SKIPPED_* if the event is obviously routine, else None.

    Args:
        event: the raw play-by-play event from Kafka. Field of interest
            is ``actionType``; we lowercase before matching.
        snapshot: the GameContextTracker snapshot. Fields of interest are
            ``period`` (int) and ``score_home`` / ``score_away`` (ints).

    Returns:
        An Action enum value indicating which skip bucket the event
        landed in, or None to let the classifier decide.
    """
    action_type = (event.get("actionType") or "").strip().lower()
    if not action_type:
        # No actionType means PlayByPlayV3 didn't tag this play — let
        # the classifier see it rather than guess.
        return None

    # Rule 1: substitutions — always SKIPPED_ROUTINE.
    if action_type == _SUBSTITUTION:
        return Action.SKIPPED_ROUTINE

    # Rule 2: period / game markers — housekeeping, SKIPPED_OTHER.
    if action_type in _MARKER_ACTION_TYPES:
        return Action.SKIPPED_OTHER

    period = _coerce_int(snapshot.get("period"))

    # Rule 3: timeouts in Q1 or Q2 — SKIPPED_EARLY_Q. Q3+ goes to the
    # classifier because timeouts there can carry narrative weight.
    if action_type == _TIMEOUT and period < 3:
        return Action.SKIPPED_EARLY_Q

    # Rule 4: free throws in a blowout in Q1-Q3. In Q4 we never skip
    # a free throw at this layer — clutch-time free throws are
    # narratively interesting regardless of margin (and the margin
    # might be closing exactly because of them).
    if action_type == _FREE_THROW and period < 4:
        margin = abs(
            _coerce_int(snapshot.get("score_home"))
            - _coerce_int(snapshot.get("score_away"))
        )
        if margin > _BLOWOUT_MARGIN_POINTS:
            return Action.SKIPPED_ROUTINE

    return None
