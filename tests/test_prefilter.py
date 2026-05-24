"""Tests for src/prefilter.py.

One positive (rule fires) and one negative (rule doesn't fire on the
close-game / late-period equivalent) per rule, plus sanity tests for
notable events the rules should NOT touch.
"""

from __future__ import annotations

from src.prefilter import should_skip
from src.state import Action


def _snapshot(*, period: int = 1, home: int = 50, away: int = 50) -> dict:
    """Build a minimal GameContextTracker-shaped snapshot."""
    return {"period": period, "score_home": home, "score_away": away}


def _event(action_type: str, **extra) -> dict:
    """Build a minimal play-by-play event."""
    out = {"actionType": action_type}
    out.update(extra)
    return out


# --- Rule 1: substitutions --------------------------------------------------


class TestSubstitution:
    def test_substitution_in_q1_skips_routine(self) -> None:
        assert should_skip(_event("Substitution"), _snapshot(period=1)) == Action.SKIPPED_ROUTINE

    def test_substitution_in_q4_still_skips(self) -> None:
        # Substitutions in Q4 are still substitutions — never narratable.
        assert should_skip(_event("Substitution"), _snapshot(period=4)) == Action.SKIPPED_ROUTINE

    def test_case_insensitive_match(self) -> None:
        # PlayByPlayV3 may emit "substitution" / "Substitution" inconsistently.
        assert should_skip(_event("SUBSTITUTION"), _snapshot()) == Action.SKIPPED_ROUTINE


# --- Rule 2: period / game markers ------------------------------------------


class TestMarkerEvents:
    def test_period_marker_skips_other(self) -> None:
        assert should_skip(_event("period"), _snapshot(period=2)) == Action.SKIPPED_OTHER

    def test_game_marker_skips_other(self) -> None:
        assert should_skip(_event("game"), _snapshot()) == Action.SKIPPED_OTHER

    def test_made_shot_is_not_a_marker(self) -> None:
        # Sanity: scoring plays are not housekeeping.
        assert should_skip(_event("Made Shot"), _snapshot()) is None


# --- Rule 3: early-quarter timeouts ----------------------------------------


class TestEarlyTimeouts:
    def test_q1_timeout_skips_early(self) -> None:
        assert should_skip(_event("Timeout"), _snapshot(period=1)) == Action.SKIPPED_EARLY_Q

    def test_q2_timeout_skips_early(self) -> None:
        assert should_skip(_event("Timeout"), _snapshot(period=2)) == Action.SKIPPED_EARLY_Q

    def test_q3_timeout_defers_to_classifier(self) -> None:
        # Q3 onwards: leave it to the classifier — could be strategic.
        assert should_skip(_event("Timeout"), _snapshot(period=3)) is None

    def test_q4_timeout_defers_to_classifier(self) -> None:
        # Especially in Q4 — clutch-time timeout, narrator-worthy.
        assert should_skip(_event("Timeout"), _snapshot(period=4)) is None


# --- Rule 4: free throws in blowout in early periods -----------------------


class TestBlowoutFreeThrows:
    def test_blowout_ft_in_q2_skips_routine(self) -> None:
        # 20-point margin in Q2 — FT is mechanical.
        snap = _snapshot(period=2, home=70, away=50)
        assert should_skip(_event("Free Throw"), snap) == Action.SKIPPED_ROUTINE

    def test_negative_margin_blowout_also_skips(self) -> None:
        # abs() — works in either direction.
        snap = _snapshot(period=3, home=60, away=85)
        assert should_skip(_event("Free Throw"), snap) == Action.SKIPPED_ROUTINE

    def test_close_game_ft_defers_to_classifier(self) -> None:
        # Same period, but margin = 4 — close game, classifier decides.
        snap = _snapshot(period=2, home=54, away=50)
        assert should_skip(_event("Free Throw"), snap) is None

    def test_q4_blowout_ft_still_defers(self) -> None:
        # Q4 always goes to the classifier regardless of margin — foul
        # trouble, intentional fouling, comeback dynamics all matter.
        snap = _snapshot(period=4, home=110, away=80)
        assert should_skip(_event("Free Throw"), snap) is None

    def test_threshold_at_15_does_not_skip(self) -> None:
        # The rule fires strictly above 15. Margin of 15 itself isn't a
        # blowout — could realistically close.
        snap = _snapshot(period=2, home=65, away=50)
        assert should_skip(_event("Free Throw"), snap) is None


# --- Sanity: notable events should never get pre-filtered ------------------


class TestDoesNotSkipNotable:
    def test_made_shot_clutch_time(self) -> None:
        # Q4, 1-point game — exactly the kind of event we WANT to analyze.
        snap = _snapshot(period=4, home=99, away=98)
        assert should_skip(_event("Made Shot"), snap) is None

    def test_foul_in_q4(self) -> None:
        # Foul-trouble logic only the classifier can evaluate.
        assert should_skip(_event("Foul"), _snapshot(period=4)) is None

    def test_block_in_overtime(self) -> None:
        # OT plays — let the classifier see them.
        assert should_skip(_event("Block"), _snapshot(period=5)) is None

    def test_missing_action_type_defers(self) -> None:
        # No actionType — defer rather than guess.
        assert should_skip({}, _snapshot()) is None
