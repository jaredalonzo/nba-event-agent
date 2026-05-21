"""Unit tests for src/tools.py.

Two tools to cover:
    get_player_stats — wraps BoxScoreTraditionalV3 (mocked).
    analyze_momentum — pure function over game_context (state injection).

Both tools are LangChain @tool-decorated objects; we invoke them via the
.invoke({...}) interface rather than calling the underlying function directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.tools import _stats_cache, analyze_momentum, get_player_stats


@pytest.fixture(autouse=True)
def _clear_stats_cache() -> None:
    """Reset the module-level cache before each test."""
    _stats_cache.clear()


# --- get_player_stats ------------------------------------------------------


class TestGetPlayerStats:
    @patch("src.tools.boxscoretraditionalv3.BoxScoreTraditionalV3")
    def test_returns_structured_line(self, mock_bs_cls: MagicMock) -> None:
        df = pd.DataFrame(
            [
                {
                    "personId": 2544,
                    "firstName": "LeBron",
                    "familyName": "James",
                    "teamTricode": "CLE",
                    "position": "F",
                    "minutes": "46:49",
                    "points": 27,
                    "reboundsTotal": 11,
                    "assists": 11,
                    "steals": 2,
                    "blocks": 3,
                    "turnovers": 5,
                    "foulsPersonal": 1,
                    "plusMinusPoints": 4.0,
                    "fieldGoalsPercentage": 0.375,
                    "threePointersPercentage": 0.2,
                }
            ]
        )
        mock_bs_cls.return_value.get_data_frames.return_value = [df]

        result = get_player_stats.invoke(
            {"player_id": "2544", "game_id": "0041500407"}
        )

        assert result["name"] == "LeBron James"
        assert result["team"] == "CLE"
        assert result["points"] == 27
        assert result["rebounds"] == 11
        assert result["assists"] == 11
        assert result["blocks"] == 3
        assert result["fg_pct"] == 0.375

    @patch("src.tools.boxscoretraditionalv3.BoxScoreTraditionalV3")
    def test_unknown_player_returns_error(self, mock_bs_cls: MagicMock) -> None:
        df = pd.DataFrame(
            [{"personId": 2544, "firstName": "LeBron", "familyName": "James"}]
        )
        mock_bs_cls.return_value.get_data_frames.return_value = [df]

        result = get_player_stats.invoke(
            {"player_id": "99999", "game_id": "0041500407"}
        )

        assert "error" in result
        assert "99999" in result["error"]

    @patch("src.tools.boxscoretraditionalv3.BoxScoreTraditionalV3")
    def test_nba_api_failure_returns_error_not_raise(
        self, mock_bs_cls: MagicMock
    ) -> None:
        mock_bs_cls.side_effect = RuntimeError("network down")
        result = get_player_stats.invoke(
            {"player_id": "2544", "game_id": "0041500407"}
        )
        # The tool should swallow the exception so the LLM gets a usable signal.
        assert "error" in result
        assert "network down" in result["error"]

    @patch("src.tools.boxscoretraditionalv3.BoxScoreTraditionalV3")
    def test_results_are_cached(self, mock_bs_cls: MagicMock) -> None:
        df = pd.DataFrame(
            [{"personId": 2544, "firstName": "L", "familyName": "J", "points": 27}]
        )
        mock_bs_cls.return_value.get_data_frames.return_value = [df]

        get_player_stats.invoke({"player_id": "2544", "game_id": "0041500407"})
        get_player_stats.invoke({"player_id": "2544", "game_id": "0041500407"})

        # nba_api should only be called once for the same (game, player) pair.
        assert mock_bs_cls.call_count == 1

    @patch("src.tools.boxscoretraditionalv3.BoxScoreTraditionalV3")
    def test_handles_nan_in_stats_gracefully(self, mock_bs_cls: MagicMock) -> None:
        df = pd.DataFrame(
            [
                {
                    "personId": 2544,
                    "firstName": "L",
                    "familyName": "J",
                    "points": 27,
                    "reboundsTotal": np.nan,  # missing
                    "fieldGoalsPercentage": np.nan,
                }
            ]
        )
        mock_bs_cls.return_value.get_data_frames.return_value = [df]

        result = get_player_stats.invoke(
            {"player_id": "2544", "game_id": "0041500407"}
        )

        # NaN should coerce to defaults rather than crash JSON serialization.
        assert result["rebounds"] == 0
        assert result["fg_pct"] == 0.0


# --- analyze_momentum ------------------------------------------------------


class TestAnalyzeMomentum:
    def _invoke(self, game_context: dict) -> dict:
        """Helper to invoke the InjectedState tool with a mock state."""
        return analyze_momentum.invoke(
            {"state": {"game_context": game_context}}
        )

    def test_no_plays_returns_empty_summary(self) -> None:
        result = self._invoke(
            {
                "home_team": "GSW",
                "away_team": "CLE",
                "score_home": 0,
                "score_away": 0,
                "last_scoring_plays": [],
            }
        )
        assert "No scoring plays" in result["summary"]
        assert result["plays"] == []

    def test_home_team_dominates(self) -> None:
        plays = [{"team": "GSW", "description": f"p{i}"} for i in range(5)]
        result = self._invoke(
            {
                "home_team": "GSW",
                "away_team": "CLE",
                "score_home": 10,
                "score_away": 0,
                "last_scoring_plays": plays,
            }
        )
        assert "GSW" in result["summary"]
        assert "dominated" in result["summary"]
        assert result["home_team_plays"] == 5
        assert result["away_team_plays"] == 0

    def test_away_team_dominates(self) -> None:
        plays = [{"team": "CLE", "description": f"p{i}"} for i in range(4)] + [
            {"team": "GSW", "description": "p4"}
        ]
        result = self._invoke(
            {
                "home_team": "GSW",
                "away_team": "CLE",
                "score_home": 2,
                "score_away": 10,
                "last_scoring_plays": plays,
            }
        )
        assert "CLE" in result["summary"]
        assert "dominated" in result["summary"]
        assert result["away_team_plays"] == 4

    def test_even_scoring(self) -> None:
        plays = [
            {"team": "GSW"},
            {"team": "CLE"},
            {"team": "GSW"},
            {"team": "CLE"},
        ]
        result = self._invoke(
            {
                "home_team": "GSW",
                "away_team": "CLE",
                "score_home": 6,
                "score_away": 6,
                "last_scoring_plays": plays,
            }
        )
        assert "even" in result["summary"].lower()
        assert result["home_team_plays"] == 2
        assert result["away_team_plays"] == 2

    def test_slight_edge_phrasing(self) -> None:
        # 3 vs 0 is enough of an edge that we don't say "even", but not 4+ so
        # not "dominated" either — should fall into the "edge" branch.
        plays = [{"team": "GSW"} for _ in range(3)]
        result = self._invoke(
            {
                "home_team": "GSW",
                "away_team": "CLE",
                "score_home": 6,
                "score_away": 0,
                "last_scoring_plays": plays,
            }
        )
        assert "edge" in result["summary"].lower()
        assert result["home_team_plays"] == 3

    def test_missing_state_doesnt_crash(self) -> None:
        # Defensive: if the consumer ever passes a malformed state.
        result = analyze_momentum.invoke({"state": {}})
        assert "summary" in result
        assert result["plays"] == []
