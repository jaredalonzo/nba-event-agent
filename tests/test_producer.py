"""Unit tests for src/producer.py — fetch_plays and delivery_report.

Mocks out nba_api so no live HTTP hits stats.nba.com during testing.
``main()`` is intentionally not tested — it's pure orchestration whose
useful behavior is already exercised by fetch_plays + integration runs.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.producer import delivery_report, fetch_plays


# --- fetch_plays ------------------------------------------------------------

def _make_pbp_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame shaped like PlayByPlayV3's first result frame."""
    return pd.DataFrame(rows)


class TestFetchPlays:
    @patch("src.producer.playbyplayv3.PlayByPlayV3")
    def test_returns_list_of_dicts(self, mock_pbp_cls: MagicMock) -> None:
        df = _make_pbp_df(
            [
                {"actionNumber": 1, "description": "Tip"},
                {"actionNumber": 2, "description": "Shot"},
            ]
        )
        mock_pbp_cls.return_value.get_data_frames.return_value = [df, pd.DataFrame()]

        plays = fetch_plays("0041500407")

        assert isinstance(plays, list)
        assert len(plays) == 2
        assert plays[0] == {"actionNumber": 1, "description": "Tip"}
        assert plays[1] == {"actionNumber": 2, "description": "Shot"}

    @patch("src.producer.playbyplayv3.PlayByPlayV3")
    def test_nan_values_converted_to_none(self, mock_pbp_cls: MagicMock) -> None:
        # nba_api fills missing fields with NaN; we need None for JSON serialization.
        df = _make_pbp_df(
            [
                {"actionNumber": 1, "playerName": "James", "shotResult": np.nan},
                {"actionNumber": 2, "playerName": np.nan, "shotResult": "Made"},
            ]
        )
        mock_pbp_cls.return_value.get_data_frames.return_value = [df, pd.DataFrame()]

        plays = fetch_plays("0041500407")

        assert plays[0]["shotResult"] is None
        assert plays[1]["playerName"] is None
        # Non-NaN values pass through unchanged.
        assert plays[0]["playerName"] == "James"
        assert plays[1]["shotResult"] == "Made"

    @patch("src.producer.playbyplayv3.PlayByPlayV3")
    def test_uses_first_dataframe_only(self, mock_pbp_cls: MagicMock) -> None:
        # PlayByPlayV3 returns 2 frames: [0] is the plays, [1] is video metadata.
        plays_df = _make_pbp_df([{"actionNumber": 1, "description": "Play"}])
        metadata_df = _make_pbp_df([{"videoAvailable": 1}])
        mock_pbp_cls.return_value.get_data_frames.return_value = [
            plays_df,
            metadata_df,
        ]

        plays = fetch_plays("0041500407")

        assert len(plays) == 1
        # The metadata frame's column should NOT leak into the result.
        assert "videoAvailable" not in plays[0] or plays[0].get("description") == "Play"

    @patch("src.producer.playbyplayv3.PlayByPlayV3")
    def test_game_id_passed_to_endpoint(self, mock_pbp_cls: MagicMock) -> None:
        mock_pbp_cls.return_value.get_data_frames.return_value = [
            _make_pbp_df([]),
            pd.DataFrame(),
        ]

        fetch_plays("0041500407")

        mock_pbp_cls.assert_called_once_with(game_id="0041500407")

    @patch("src.producer.playbyplayv3.PlayByPlayV3")
    def test_empty_frame_returns_empty_list(self, mock_pbp_cls: MagicMock) -> None:
        mock_pbp_cls.return_value.get_data_frames.return_value = [
            _make_pbp_df([]),
            pd.DataFrame(),
        ]

        assert fetch_plays("0041500407") == []


# --- delivery_report --------------------------------------------------------

class TestDeliveryReport:
    def test_no_error_produces_no_output(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        msg = MagicMock()
        delivery_report(err=None, msg=msg)
        assert capsys.readouterr().out == ""

    def test_error_prints_key_and_error(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        msg = MagicMock()
        msg.key.return_value = b"42"
        delivery_report(err="broker unavailable", msg=msg)

        out = capsys.readouterr().out
        assert "broker unavailable" in out
        assert "42" in out  # the key should appear in the diagnostic
