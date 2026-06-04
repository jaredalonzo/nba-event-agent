"""Unit tests for src/bluesky_poster.py."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

import src.bluesky_poster as bp
from src.bluesky_poster import _format_post, _parse_clock, post_insight


@pytest.fixture(autouse=True)
def _reset_client() -> None:
    """Reset the module-level singleton before each test."""
    bp._client = None


def make_event(**overrides) -> dict:
    base = {
        "gameId": "0041500407",
        "period": 4,
        "clock": "PT02M34.00S",
        "scoreHome": "89",
        "scoreAway": "93",
        "description": "James 3pt Jump Shot (41 PTS)",
    }
    return {**base, **overrides}


# --- _parse_clock -------------------------------------------------------------


class TestParseClock:
    def test_standard_format(self) -> None:
        assert _parse_clock("PT02M34.00S") == "2:34"

    def test_zero_minutes(self) -> None:
        assert _parse_clock("PT00M07.00S") == "0:07"

    def test_double_digit_minutes(self) -> None:
        assert _parse_clock("PT12M00.00S") == "12:00"

    def test_fractional_seconds_truncated(self) -> None:
        assert _parse_clock("PT01M59.50S") == "1:59"

    def test_unrecognised_format_returned_as_is(self) -> None:
        assert _parse_clock("badclock") == "badclock"


# --- _format_post -------------------------------------------------------------


class TestFormatPost:
    def test_includes_period_clock_score(self) -> None:
        text = _format_post("Great play.", make_event())
        assert "Q4" in text
        assert "2:34" in text
        assert "89-93" in text

    def test_always_ends_with_nba_tag(self) -> None:
        text = _format_post("Great play.", make_event())
        assert text.endswith(" #NBA")

    def test_stays_within_300_chars(self) -> None:
        long_insight = "x" * 400
        text = _format_post(long_insight, make_event())
        assert len(text) <= 300

    def test_truncated_insight_ends_with_ellipsis(self) -> None:
        long_insight = "x" * 400
        text = _format_post(long_insight, make_event())
        # The body (between header and tag) should end with ellipsis
        body_and_tag = text.split("\n", 1)[1]
        assert "…" in body_and_tag

    def test_short_insight_not_truncated(self) -> None:
        insight = "Short insight."
        text = _format_post(insight, make_event())
        assert insight in text

    def test_missing_period_omits_quarter(self) -> None:
        text = _format_post("Play.", make_event(period=None, clock=""))
        assert "Q" not in text

    def test_missing_score_omits_score(self) -> None:
        text = _format_post("Play.", make_event(scoreHome="", scoreAway=""))
        assert "|" not in text

    def test_emoji_present(self) -> None:
        text = _format_post("Play.", make_event())
        assert "🏀" in text


# --- post_insight -------------------------------------------------------------


class TestPostInsight:
    def test_non_critical_severity_skips_post(self) -> None:
        with patch("src.bluesky_poster._get_client") as mock_get:
            post_insight("Insight.", "notable", make_event())
            post_insight("Insight.", "routine", make_event())
            mock_get.assert_not_called()

    def test_no_credentials_skips_post(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            with patch("atproto.Client") as mock_cls:
                post_insight("Insight.", "critical", make_event())
                mock_cls.assert_not_called()

    def test_critical_with_credentials_calls_send_post(self) -> None:
        mock_client = MagicMock()
        with patch.dict(
            "os.environ",
            {"BLUESKY_HANDLE": "test.bsky.social", "BLUESKY_APP_PASSWORD": "secret"},
        ):
            with patch("atproto.Client", return_value=mock_client):
                post_insight("Big play!", "critical", make_event())
                mock_client.send_post.assert_called_once()
                args, kwargs = mock_client.send_post.call_args
                text = kwargs.get("text") or (args[0] if args else "")
                assert "Big play!" in text
                assert "#NBA" in text

    def test_send_post_exception_does_not_raise(self, capsys) -> None:
        mock_client = MagicMock()
        mock_client.send_post.side_effect = RuntimeError("network error")
        with patch.dict(
            "os.environ",
            {"BLUESKY_HANDLE": "test.bsky.social", "BLUESKY_APP_PASSWORD": "secret"},
        ):
            with patch("atproto.Client", return_value=mock_client):
                post_insight("Big play!", "critical", make_event())  # must not raise
        captured = capsys.readouterr()
        assert "[bluesky] post failed" in captured.out


# --- _get_client --------------------------------------------------------------


class TestGetClient:
    def test_missing_handle_returns_none(self) -> None:
        with patch.dict("os.environ", {"BLUESKY_APP_PASSWORD": "secret"}, clear=True):
            assert bp._get_client() is None

    def test_missing_password_returns_none(self) -> None:
        with patch.dict("os.environ", {"BLUESKY_HANDLE": "test.bsky.social"}, clear=True):
            assert bp._get_client() is None

    def test_login_failure_returns_none_and_warns(self, capsys) -> None:
        mock_client = MagicMock()
        mock_client.login.side_effect = RuntimeError("bad password")
        with patch.dict(
            "os.environ",
            {"BLUESKY_HANDLE": "test.bsky.social", "BLUESKY_APP_PASSWORD": "wrong"},
        ):
            with patch("atproto.Client", return_value=mock_client):
                result = bp._get_client()
        assert result is None
        captured = capsys.readouterr()
        assert "[bluesky] login failed" in captured.out

    def test_second_call_reuses_cached_client(self) -> None:
        mock_client = MagicMock()
        with patch.dict(
            "os.environ",
            {"BLUESKY_HANDLE": "test.bsky.social", "BLUESKY_APP_PASSWORD": "secret"},
        ):
            with patch("atproto.Client", return_value=mock_client) as mock_cls:
                bp._get_client()
                bp._get_client()
                mock_cls.assert_called_once()
                mock_client.login.assert_called_once()
